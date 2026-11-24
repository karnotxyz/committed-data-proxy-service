mod archive;
#[allow(dead_code)] // Keep Madara's pinned tree module intact for compatibility.
mod patricia;
mod recovery;
mod store;

use archive::Archive;
use axum::{
    body::Bytes,
    extract::{ConnectInfo, DefaultBodyLimit, State},
    http::{Method, StatusCode, Uri},
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use starknet_types_core::felt::Felt;
use std::{
    collections::HashMap,
    env,
    net::SocketAddr,
    path::Path,
    str::FromStr,
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use store::{Admission, Dataset, QueueEnqueue, QueuedCall, Store, Update, MAX_QUEUE_CAPACITY};
use tokio::sync::{oneshot, Mutex, Notify, Semaphore};

const COMBINED: &str = "madara_V0_1_0_importCommittedDataAndSubmitTransaction";
const IMPORT: &str = "madara_V0_1_0_importCommittedDataSet";
const UPDATE: &str = "madara_V0_1_0_updateCommittedDataSet";

struct App {
    store: Store,
    archive: Option<Archive>,
    client: reqwest::Client,
    forward: bool,
    require_c2_ack: bool,
    c1_url: Option<String>,
    c2_url: Option<String>,
    // ponytail: one process-wide admission lock; shard by transaction digest if throughput needs it.
    admission_lock: Mutex<()>,
    read_limit: Arc<Semaphore>,
    queue_capacity: u64,
    queue_notify: Notify,
    queue_waiters: StdMutex<HashMap<u64, Vec<oneshot::Sender<Value>>>>,
}

fn admission_id(transaction: &Value) -> String {
    hex::encode(Sha256::digest(
        serde_json::to_vec(transaction).unwrap_or_default(),
    ))
}

async fn persist(app: Arc<App>, dataset: Dataset) -> Result<String, String> {
    tokio::task::spawn_blocking(move || app.store.put(&dataset))
        .await
        .map_err(|e| e.to_string())?
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn rpc_error(id: Value, code: i32, message: &str) -> Json<Value> {
    Json(json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}}))
}

fn rpc_result(id: Value, result: Value) -> Json<Value> {
    Json(json!({"jsonrpc":"2.0","id":id,"result":result}))
}

fn params(value: &Value, fields: &[&str]) -> Result<Vec<Value>, String> {
    match value {
        Value::Array(array)
            if array.len() >= fields.len().saturating_sub(1) && array.len() <= fields.len() =>
        {
            Ok(array.clone())
        }
        Value::Object(object) if object.keys().all(|key| fields.contains(&key.as_str())) => {
            Ok(fields
                .iter()
                .map(|key| object.get(*key).cloned().unwrap_or(Value::Null))
                .collect())
        }
        _ => Err("Invalid params".into()),
    }
}

fn dataset_from_params(method: &str, value: &Value) -> Result<Dataset, String> {
    let fields: &[&str] = if method == UPDATE {
        &["update", "root", "entries", "signatures"]
    } else {
        &["root", "entries", "signatures"]
    };
    let mut args = params(value, fields)?;
    if args.len() == fields.len() - 1 {
        args.push(Value::Null);
    }
    let (update, root, entries, signatures) = if method == UPDATE {
        (
            Some(serde_json::from_value::<Update>(args[0].clone()).map_err(|e| e.to_string())?),
            args[1].clone(),
            args[2].clone(),
            args[3].clone(),
        )
    } else {
        (None, args[0].clone(), args[1].clone(), args[2].clone())
    };
    serde_json::from_value(json!({"update":update,"root":root,"entries":entries,
        "signatures":if signatures.is_null() {json!([])} else {signatures}}))
    .map_err(|e| e.to_string())
}

async fn call_c2(app: &App, ids: &[String]) -> Result<(), String> {
    let url = app.c2_url.as_ref().ok_or("C2 import URL is missing")?;
    for id in ids {
        let dataset = app.store.load_import(id)?;
        let (method, params) = if let Some(update) = dataset.update {
            (
                UPDATE,
                json!([update, dataset.root, dataset.entries, dataset.signatures]),
            )
        } else {
            (
                IMPORT,
                json!([dataset.root, dataset.entries, dataset.signatures]),
            )
        };
        let response: Value = app
            .client
            .post(url)
            .json(&json!({"jsonrpc":"2.0","id":id,
            "method":method,"params":params}))
            .send()
            .await
            .map_err(|e| e.to_string())?
            .error_for_status()
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        if response.get("error").is_some() || response.get("result").is_none() {
            return Err(format!(
                "C2 import rejected: {}",
                response.get("error").unwrap_or(&response)
            ));
        }
    }
    Ok(())
}

async fn call_c1(app: &App, transaction: &Value) -> Result<String, String> {
    let url = app.c1_url.as_ref().ok_or("C1 RPC URL is missing")?;
    let response: Value = app
        .client
        .post(url)
        .json(&json!({"jsonrpc":"2.0","id":1,
        "method":"starknet_addInvokeTransaction","params":{"invoke_transaction":transaction}}))
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    if response.get("error").is_some() {
        return Err(format!("C1 rejected: {}", response["error"]));
    }
    response["result"]["transaction_hash"]
        .as_str()
        .map(str::to_owned)
        .ok_or("C1 returned no transaction hash".into())
}

async fn combined(app: Arc<App>, value: &Value, id: Value) -> Json<Value> {
    let args = match params(value, &["datasets", "transaction"]) {
        Ok(args) if args.len() == 2 => args,
        _ => return rpc_error(id, -32602, "Invalid params"),
    };
    let datasets: Vec<Dataset> = match serde_json::from_value::<Vec<Dataset>>(args[0].clone()) {
        Ok(data) if (1..=4).contains(&data.len()) => data,
        _ => return rpc_error(id, -32602, "Invalid params"),
    };
    let transaction = &args[1];
    if !transaction.is_object() {
        return rpc_error(id, -32602, "Invalid params");
    }
    if app.forward && (transaction["type"] != "INVOKE" || transaction["is_query"] == true) {
        return rpc_error(id, -32602, "Expected signed non-query INVOKE");
    }
    let mut ids = Vec::new();
    for dataset in datasets {
        match persist(app.clone(), dataset).await {
            Ok(dataset_id) => ids.push(dataset_id),
            Err(error) => {
                eprintln!("committed-data store failed: {error}");
                return rpc_error(id, -32004, "Committed data could not be stored");
            }
        }
    }
    let payload = serde_json::to_vec(transaction).unwrap_or_default();
    if payload.len() > 1024 * 1024 {
        return rpc_error(id, -32602, "Transaction too large");
    }
    let admission_id = admission_id(transaction);
    {
        let _guard = app.admission_lock.lock().await;
        let existing = match app.store.admission(&admission_id) {
            Ok(existing) => existing,
            Err(error) => {
                eprintln!("admission read failed: {error}");
                return rpc_error(id, -32004, "Admission could not be stored");
            }
        };
        if let Some(existing) = existing {
            if existing.dataset_ids != ids || existing.transaction != *transaction {
                return rpc_error(id, -32004, "Conflicting transaction admission");
            }
        } else {
            let admission = Admission {
                dataset_ids: ids.clone(),
                transaction: transaction.clone(),
                status: "archived".into(),
                c2_delivered: false,
                transaction_hash: None,
                attempts: 0,
                next_attempt_ms: 0,
            };
            if let Err(error) = app.store.save_admission(&admission_id, &admission) {
                eprintln!("admission write failed: {error}");
                return rpc_error(id, -32004, "Admission could not be stored");
            }
        }
    }
    if !app.forward {
        return rpc_error(id, -32003, "Committed data import is not enabled");
    }
    if app.require_c2_ack {
        if let Err(error) = call_c2(&app, &ids).await {
            eprintln!("C2 import failed: {error}");
            return rpc_error(id, -32005, "C2 did not acknowledge committed data");
        }
        let _guard = app.admission_lock.lock().await;
        match app.store.admission(&admission_id) {
            Ok(Some(mut admission)) => {
                admission.c2_delivered = true;
                if let Err(error) = app.store.save_admission(&admission_id, &admission) {
                    eprintln!("C2 admission write failed: {error}");
                    return rpc_error(id, -32004, "C2 acknowledgment could not be stored");
                }
            }
            _ => return rpc_error(id, -32004, "Missing admission"),
        }
    }
    {
        let _guard = app.admission_lock.lock().await;
        let Ok(Some(mut admission)) = app.store.admission(&admission_id) else {
            return rpc_error(id, -32004, "Missing admission");
        };
        if admission.status == "submitted" {
            return rpc_result(id, json!({"transaction_hash":admission.transaction_hash}));
        }
        if admission.status != "archived" {
            return rpc_error(id, -32006, "C1 submission is pending reconciliation");
        }
        admission.status = "submitting".into();
        if let Err(error) = app.store.save_admission(&admission_id, &admission) {
            eprintln!("C1 admission claim failed: {error}");
            return rpc_error(id, -32004, "C1 admission claim failed");
        }
    }
    let result = call_c1(&app, transaction).await;
    let _guard = app.admission_lock.lock().await;
    let Ok(Some(mut admission)) = app.store.admission(&admission_id) else {
        return rpc_error(id, -32006, "C1 submission needs reconciliation");
    };
    match result {
        Ok(hash) => {
            admission.status = "submitted".into();
            admission.transaction_hash = Some(hash.clone());
            if let Err(error) = app.store.save_admission(&admission_id, &admission) {
                eprintln!("C1 result persistence failed: {error}");
                return rpc_error(id, -32006, "C1 submission needs reconciliation");
            }
            rpc_result(id, json!({"transaction_hash":hash}))
        }
        Err(error) => {
            eprintln!("C1 submission uncertain: {error}");
            admission.status = "uncertain".into();
            let _ = app.store.save_admission(&admission_id, &admission);
            rpc_error(id, -32006, "C1 submission needs reconciliation")
        }
    }
}

async fn queued_combined(app: Arc<App>, value: &Value, id: Value) -> Json<Value> {
    let args = match params(value, &["datasets", "transaction"]) {
        Ok(args) if args.len() == 2 => args,
        _ => return rpc_error(id, -32602, "Invalid params"),
    };
    let datasets = match serde_json::from_value::<Vec<Dataset>>(args[0].clone()) {
        Ok(datasets) if (1..=4).contains(&datasets.len()) => datasets,
        _ => return rpc_error(id, -32602, "Invalid params"),
    };
    let transaction = args[1].clone();
    if transaction["type"] != "INVOKE"
        || transaction["is_query"] == true
        || serde_json::to_vec(&transaction).is_ok_and(|bytes| bytes.len() > 1024 * 1024)
    {
        return rpc_error(id, -32602, "Expected signed non-query INVOKE");
    }
    let call = QueuedCall {
        admission_id: admission_id(&transaction),
        datasets,
        transaction,
    };
    let (sender, receiver) = oneshot::channel();
    let seq = {
        let mut waiters = app.queue_waiters.lock().unwrap();
        match app.store.enqueue(&call, app.queue_capacity) {
            Ok(QueueEnqueue::Pending(seq)) => {
                waiters.entry(seq).or_default().push(sender);
                seq
            }
            Ok(QueueEnqueue::Full) => return rpc_error(id, -32007, "Proxy queue is full"),
            Ok(QueueEnqueue::Conflict) => {
                return rpc_error(id, -32004, "Conflicting transaction admission")
            }
            Err(error) => {
                eprintln!("queue admission failed: {error}");
                return rpc_error(id, -32004, "Queue admission could not be stored");
            }
        }
    };
    app.queue_notify.notify_one();
    match receiver.await {
        Ok(mut response) => {
            response["id"] = id;
            Json(response)
        }
        Err(_) => rpc_error(
            id,
            -32006,
            &format!("Queue worker stopped while processing {seq}"),
        ),
    }
}

fn notify_queue_waiters(app: &App, seq: u64, result: Value) {
    if let Some(waiters) = app.queue_waiters.lock().unwrap().remove(&seq) {
        for waiter in waiters {
            let _ = waiter.send(result.clone());
        }
    }
}

async fn queue_worker(app: Arc<App>) {
    loop {
        let (seq, call) = match app.store.queue_head() {
            Ok(Some(head)) => head,
            Ok(None) => {
                app.queue_notify.notified().await;
                continue;
            }
            Err(error) => {
                eprintln!("queue read failed: {error}");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        let result = combined(
            app.clone(),
            &json!([call.datasets, call.transaction]),
            Value::Null,
        )
        .await
        .0;
        let error = result["error"]["code"].as_i64();
        if error == Some(-32005) {
            // Keep the head until C2 acknowledges it. Later calls cannot overtake it.
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }
        if error == Some(-32006) {
            // C1 may have accepted this transaction. Only manual reconciliation can unblock it.
            eprintln!("queue blocked at {seq}: C1 submission needs reconciliation");
            notify_queue_waiters(&app, seq, result);
            tokio::time::sleep(Duration::from_secs(30)).await;
            continue;
        }
        let mut waiters = app.queue_waiters.lock().unwrap();
        if let Err(error) = app.store.finish_queue_head(seq, &call.admission_id) {
            eprintln!("queue completion failed at {seq}: {error}");
            drop(waiters);
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }
        for waiter in waiters.remove(&seq).unwrap_or_default() {
            let _ = waiter.send(result.clone());
        }
    }
}

async fn rpc(State(app): State<Arc<App>>, body: Bytes) -> Json<Value> {
    let call: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return rpc_error(Value::Null, -32700, "Parse error"),
    };
    let id = call.get("id").cloned().unwrap_or(Value::Null);
    if call["jsonrpc"] != "2.0" {
        return rpc_error(id, -32600, "Invalid Request");
    }
    let Some(method) = call["method"].as_str() else {
        return rpc_error(id, -32600, "Invalid Request");
    };
    match method {
        COMBINED if app.forward => queued_combined(app.clone(), &call["params"], id).await,
        COMBINED => combined(app.clone(), &call["params"], id).await,
        IMPORT | UPDATE => {
            let dataset = match dataset_from_params(method, &call["params"]) {
                Ok(dataset) => dataset,
                Err(_) => return rpc_error(id, -32602, "Invalid params"),
            };
            match persist(app.clone(), dataset).await {
                Ok(_) => rpc_result(id, Value::Null),
                Err(error) => {
                    eprintln!("dataset import rejected: {error}");
                    rpc_error(
                        id,
                        if error == "unauthorized import" {
                            -32001
                        } else {
                            -32004
                        },
                        "Committed data could not be stored",
                    )
                }
            }
        }
        _ => rpc_error(id, -32601, "Method not found"),
    }
}

fn query(uri: &Uri) -> HashMap<String, String> {
    url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes())
        .into_owned()
        .collect()
}

fn felt_param(query: &HashMap<String, String>, name: &str) -> Result<Felt, String> {
    Felt::from_str(query.get(name).ok_or_else(|| format!("missing {name}"))?)
        .map_err(|e| e.to_string())
}

fn feeder_error(error: String) -> (StatusCode, Json<Value>) {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"code":"MALFORMED_REQUEST","message":error})),
    )
}

async fn feeder(
    State(app): State<Arc<App>>,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let q = query(&uri);
    let path = uri.path().to_owned();
    let permit = app.read_limit.clone().try_acquire_owned().map_err(|_| {
        (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"code":"RATE_LIMITED","message":"Committed-data reads are busy"})),
        )
    })?;
    let result: Result<Value, String> = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        (|| { match path.as_str() {
            "/feeder_gateway/get_committed_data_roots" if method == Method::GET => {
                let after = q.get("after").map(|s| Felt::from_str(s).map_err(|e| e.to_string())).transpose()?;
                Ok(json!(app.store.roots(after)?))
            }
            "/feeder_gateway/get_committed_data_versions" if method == Method::GET => {
                let after = if q.contains_key("application_id") || q.contains_key("dataset_id") || q.contains_key("root") {
                    Some((felt_param(&q, "application_id")?, felt_param(&q, "dataset_id")?, felt_param(&q, "root")?))
                } else { None };
                Ok(json!(app.store.versions(after)?.into_iter().map(|(application_id, dataset_id, root)|
                    json!({"application_id":application_id,"dataset_id":dataset_id,"root":root})).collect::<Vec<_>>()))
            }
            "/feeder_gateway/get_committed_data" if method == Method::GET => {
                let root = felt_param(&q, "root")?;
                let start = q.get("start").map(|s| s.parse::<usize>().map_err(|e| e.to_string()))
                    .transpose()?.unwrap_or(0);
                Ok(app.store.page(root, start)?.unwrap_or(Value::Null))
            }
            "/feeder_gateway/get_committed_data_witness" if method == Method::GET => {
                let root = felt_param(&q, "root")?;
                let key = felt_param(&q, "key")?;
                Ok(json!(app.store.witnesses(root, &[key])?.and_then(|mut proofs| proofs.pop())))
            }
            "/feeder_gateway/get_committed_data_witnesses" if method == Method::GET || method == Method::POST => {
                let (root, keys): (Felt, Vec<Felt>) = if method == Method::POST {
                    let value: Value = serde_json::from_slice(&body).map_err(|e| e.to_string())?;
                    (serde_json::from_value(value["root"].clone()).map_err(|e| e.to_string())?,
                        serde_json::from_value(value["keys"].clone()).map_err(|e| e.to_string())?)
                } else {
                    (felt_param(&q, "root")?, q.get("keys").ok_or("missing keys")?.split(',')
                        .map(|s| Felt::from_str(s).map_err(|e| e.to_string())).collect::<Result<_, _>>()?)
                };
                Ok(json!(app.store.witnesses(root, &keys)?))
            }
            _ => Err("not found".into()),
        } })()
    }).await.map_err(|error| feeder_error(error.to_string()))?;
    result.map(Json).map_err(feeder_error)
}

async fn health() -> &'static str {
    "OK"
}
async fn ready(State(app): State<Arc<App>>) -> (StatusCode, &'static str) {
    if app.store.roots(None).is_ok() {
        (StatusCode::OK, "OK")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "unavailable")
    }
}

async fn queue_status(State(app): State<Arc<App>>) -> (StatusCode, Json<Value>) {
    match app.store.queue_len() {
        Ok(pending) => (
            StatusCode::OK,
            Json(json!({"pending":pending,"capacity":app.queue_capacity})),
        ),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"queue unavailable"})),
        ),
    }
}

async fn smoke(
    State(app): State<Arc<App>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
) -> (StatusCode, Json<Value>) {
    if !peer.ip().is_loopback() {
        return (StatusCode::NOT_FOUND, Json(json!({"error":"not found"})));
    }
    match app.store.smoke_write() {
        Ok(id) => (StatusCode::OK, Json(json!({"id":id,"stored":true}))),
        Err(error) => {
            eprintln!("RocksDB smoke write failed: {error}");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error":"RocksDB write failed"})),
            )
        }
    }
}

async fn mongo_smoke(
    State(app): State<Arc<App>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
) -> (StatusCode, Json<Value>) {
    if !peer.ip().is_loopback() {
        return (StatusCode::NOT_FOUND, Json(json!({"error":"not found"})));
    }
    let Some(archive) = &app.archive else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"MongoDB is not configured"})),
        );
    };
    match archive.smoke_write().await {
        Ok(id) => (StatusCode::OK, Json(json!({"id":id,"stored":true}))),
        Err(error) => {
            eprintln!("MongoDB smoke write failed: {error}");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error":"MongoDB write failed"})),
            )
        }
    }
}

async fn archive_worker(app: Arc<App>) {
    let Some(archive) = &app.archive else {
        return;
    };
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    loop {
        interval.tick().await;
        let pending = match app.store.pending_recovery(16) {
            Ok(pending) => pending,
            Err(error) => {
                eprintln!("MongoDB recovery scan failed: {error}");
                continue;
            }
        };
        for (id, record) in pending {
            match export_recovery(&app, archive, &id, record).await {
                Ok(()) => {}
                Err(error) => eprintln!("MongoDB recovery export failed for {id}: {error}"),
            }
        }
    }
}

async fn export_recovery(
    app: &App,
    archive: &Archive,
    id: &str,
    record: recovery::RecoveryRecord,
) -> Result<(), String> {
    let mut path = vec![(id.to_owned(), record)];
    let mut seen = std::collections::HashSet::new();
    while let Some(base) = path
        .last()
        .and_then(|(_, record)| record.manifest.base.clone())
    {
        if !seen.insert(base.clone()) || path.len() > 4096 {
            return Err("recovery dependency cycle or excessive depth".into());
        }
        if archive.complete(&base).await? {
            break;
        }
        let parent = app
            .store
            .recovery_record(&base)?
            .ok_or("missing recovery base payload")?;
        if parent.manifest.id()? != base {
            return Err("corrupt recovery base identity".into());
        }
        path.push((base, parent));
    }
    for (id, record) in path.into_iter().rev() {
        archive.upload(&record).await?;
        if !archive.complete(&id).await? {
            return Err("recovery manifest not visible".into());
        }
        app.store.mark_recovery_exported(&id)?;
    }
    Ok(())
}

async fn delivery_worker(app: Arc<App>) {
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    loop {
        interval.tick().await;
        let pending = match app.store.pending_admissions() {
            Ok(pending) => pending,
            Err(error) => {
                eprintln!("delivery scan failed: {error}");
                continue;
            }
        };
        for (id, admission) in pending {
            if admission.next_attempt_ms > now_ms() {
                continue;
            }
            let result = call_c2(&app, &admission.dataset_ids).await;
            let _guard = app.admission_lock.lock().await;
            let Ok(Some(mut current)) = app.store.admission(&id) else {
                continue;
            };
            if current.c2_delivered {
                continue;
            }
            match result {
                Ok(()) => current.c2_delivered = true,
                Err(error) => {
                    eprintln!("C2 delivery failed: {error}");
                    current.attempts += 1;
                    current.next_attempt_ms = now_ms() + (30_000u64 << current.attempts.min(6));
                }
            }
            let _ = app.store.save_admission(&id, &current);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let chain_id = Felt::from_str(&env::var("CHAIN_ID")?)?;
    let allowed_signers = env::var("COMMITTED_DATA_SIGNERS")
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.is_empty())
        .map(Felt::from_str)
        .collect::<Result<Vec<_>, _>>()?;
    let forward = env::var("FORWARD_TRANSACTIONS").as_deref() == Ok("true");
    let require_c2_ack = env::var("REQUIRE_C2_ACK_BEFORE_C1").as_deref() != Ok("false");
    let c1_url = env::var("C1_RPC_URL").ok();
    let c2_url = env::var("C2_IMPORT_URL").ok();
    let mongo_uri = env::var("MONGO_URI").ok();
    if env::var("MONGO_REQUIRED").as_deref() == Ok("true") && mongo_uri.is_none() {
        return Err("MONGO_URI required".into());
    }
    let archive = if let Some(uri) = mongo_uri {
        let client = mongodb::Client::with_uri_str(uri).await?;
        let database =
            client.database(&env::var("MONGO_DATABASE").unwrap_or("committed_data".into()));
        Some(Archive::new(
            &database,
            &env::var("MONGO_COLLECTION_PREFIX").unwrap_or("mempool_transactions".into()),
        ))
    } else {
        None
    };
    if forward && (c1_url.is_none() || c2_url.is_none()) {
        return Err("forwarding URLs required".into());
    }
    let queue_capacity = env::var("QUEUE_CAPACITY")
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(MAX_QUEUE_CAPACITY);
    if !(1..=MAX_QUEUE_CAPACITY).contains(&queue_capacity) {
        return Err("QUEUE_CAPACITY must be between 1 and 10000".into());
    }
    let app = Arc::new(App {
        store: Store::open(
            Path::new(&env::var("ROCKSDB_PATH").unwrap_or("/data/rocksdb".into())),
            chain_id,
            allowed_signers,
        )
        .map_err(std::io::Error::other)?,
        archive,
        client: reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .build()?,
        forward,
        require_c2_ack,
        c1_url,
        c2_url,
        admission_lock: Mutex::new(()),
        read_limit: Arc::new(Semaphore::new(4)),
        queue_capacity,
        queue_notify: Notify::new(),
        queue_waiters: StdMutex::new(HashMap::new()),
    });
    if forward {
        tokio::spawn(queue_worker(app.clone()));
    }
    if forward && !require_c2_ack {
        tokio::spawn(delivery_worker(app.clone()));
    }
    if app.archive.is_some() {
        tokio::spawn(archive_worker(app.clone()));
    }
    let rpc = Router::new()
        .route("/", post(rpc))
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/__queue/status", get(queue_status))
        .route("/__test/rocksdb-write", post(smoke))
        .route("/__test/mongo-write", post(mongo_smoke))
        .layer(DefaultBodyLimit::max(128 * 1024 * 1024))
        .with_state(app.clone());
    let feeder = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/feeder_gateway/{*path}", get(feeder).post(feeder))
        .layer(DefaultBodyLimit::max(75 * 1024))
        .with_state(app);
    let rpc_listener =
        tokio::net::TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], 9945))).await?;
    let feeder_listener =
        tokio::net::TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], 8080))).await?;
    eprintln!("committed-data proxy listening on 9945 and 8080; forwarding={forward}");
    tokio::try_join!(
        axum::serve(
            rpc_listener,
            rpc.into_make_service_with_connect_info::<SocketAddr>()
        ),
        axum::serve(feeder_listener, feeder)
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;

    #[tokio::test]
    async fn import_rpc_exposes_matching_feeder_page_and_witness() {
        let dir = tempdir().unwrap();
        let app = Arc::new(App {
            store: Store::open(dir.path(), Felt::ONE, vec![]).unwrap(),
            archive: None,
            client: reqwest::Client::new(),
            forward: false,
            require_c2_ack: true,
            c1_url: None,
            c2_url: None,
            admission_lock: Mutex::new(()),
            read_limit: Arc::new(Semaphore::new(4)),
            queue_capacity: MAX_QUEUE_CAPACITY,
            queue_notify: Notify::new(),
            queue_waiters: StdMutex::new(HashMap::new()),
        });
        let root = Felt::from_dec_str(
            "3000094458541565324654730242423920536387969495385651240652446843684607154951",
        )
        .unwrap();
        let request = json!({"jsonrpc":"2.0","id":7,"method":IMPORT,
            "params":[root,[["0x2","0x64"]],[]]});
        assert_eq!(
            rpc(State(app.clone()), Bytes::from(request.to_string()))
                .await
                .0["result"],
            Value::Null
        );
        let uri = Uri::from_str(&format!(
            "/feeder_gateway/get_committed_data?root={root:#x}&start=0"
        ))
        .unwrap();
        let page = feeder(State(app.clone()), Method::GET, uri, Bytes::new())
            .await
            .unwrap()
            .0;
        assert_eq!(page["entries"], json!([["0x2", "0x64"]]));
        let uri = Uri::from_str(&format!(
            "/feeder_gateway/get_committed_data_witness?root={root:#x}&key=0x3"
        ))
        .unwrap();
        let proof: crate::patricia::CommittedDataWitness = serde_json::from_value(
            feeder(State(app), Method::GET, uri, Bytes::new())
                .await
                .unwrap()
                .0,
        )
        .unwrap();
        assert!(proof.verify());
        assert_eq!(proof.read_value(), Felt::ZERO);
    }

    #[tokio::test]
    async fn combined_rpc_imports_only_data_to_c2_and_submits_c1_once() {
        let c1_calls = Arc::new(AtomicUsize::new(0));
        let c2_calls = Arc::new(AtomicUsize::new(0));
        let c1 =
            {
                let calls = c1_calls.clone();
                Router::new().route("/", post(move |Json(call): Json<Value>| {
                let calls = calls.clone();
                async move {
                    assert_eq!(call["method"], "starknet_addInvokeTransaction");
                    calls.fetch_add(1, Ordering::SeqCst);
                    Json(json!({"jsonrpc":"2.0","id":1,"result":{"transaction_hash":"0x123"}}))
                }
            }))
            };
        let c2 = {
            let calls = c2_calls.clone();
            Router::new().route(
                "/",
                post(move |Json(call): Json<Value>| {
                    let calls = calls.clone();
                    async move {
                        assert_eq!(call["method"], IMPORT);
                        calls.fetch_add(1, Ordering::SeqCst);
                        Json(json!({"jsonrpc":"2.0","id":1,"result":null}))
                    }
                }),
            )
        };
        let c1_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let c2_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let c1_url = format!("http://{}", c1_listener.local_addr().unwrap());
        let c2_url = format!("http://{}", c2_listener.local_addr().unwrap());
        let c1_task = tokio::spawn(async move { axum::serve(c1_listener, c1).await.unwrap() });
        let c2_task = tokio::spawn(async move { axum::serve(c2_listener, c2).await.unwrap() });
        let dir = tempdir().unwrap();
        let app = Arc::new(App {
            store: Store::open(dir.path(), Felt::ONE, vec![]).unwrap(),
            archive: None,
            client: reqwest::Client::new(),
            forward: true,
            require_c2_ack: true,
            c1_url: Some(c1_url),
            c2_url: Some(c2_url),
            admission_lock: Mutex::new(()),
            read_limit: Arc::new(Semaphore::new(4)),
            queue_capacity: MAX_QUEUE_CAPACITY,
            queue_notify: Notify::new(),
            queue_waiters: StdMutex::new(HashMap::new()),
        });
        let root = Felt::from_dec_str(
            "3000094458541565324654730242423920536387969495385651240652446843684607154951",
        )
        .unwrap();
        let request = json!({"jsonrpc":"2.0","id":9,"method":COMBINED,
            "params":[[{"root":root,"entries":[["0x2","0x64"]]}],
                {"type":"INVOKE","nonce":"0x1"}]});
        let queue_task = tokio::spawn(queue_worker(app.clone()));
        for _ in 0..2 {
            let response = rpc(State(app.clone()), Bytes::from(request.to_string()))
                .await
                .0;
            assert_eq!(response["result"]["transaction_hash"], "0x123");
        }
        assert_eq!(c1_calls.load(Ordering::SeqCst), 1);
        assert_eq!(c2_calls.load(Ordering::SeqCst), 2);
        queue_task.abort();
        c1_task.abort();
        c2_task.abort();
    }
}
