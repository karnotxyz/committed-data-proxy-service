//! C2 owns committed-data validation, persistence, and feeder reads.
use crate::audit::Audit;
use axum::{
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
    http::{header, HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{any, get, post},
    Json, Router,
};
use mongodb::bson::oid::ObjectId;
use serde_json::{json, Value};
use std::{
    env,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime},
};

const COMBINED: &str = "madara_V0_1_0_importCommittedDataAndSubmitTransaction";
const IMPORT: &str = "madara_V0_1_0_importCommittedDataSet";
const UPDATE: &str = "madara_V0_1_0_updateCommittedDataSet";

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

struct App {
    audit: Audit,
    client: reqwest::Client,
    c1_url: Option<String>,
    c2_import_url: String,
    c2_feeder_url: String,
    forward_transactions: bool,
    require_c2_ack: bool,
}

const RETRY_DELAYS: [Duration; 5] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
    Duration::from_secs(16),
];

struct Upstream {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

fn retryable(status: StatusCode) -> bool {
    status.is_server_error()
        || status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::REQUEST_TIMEOUT
}

async fn send_with_retry(request: reqwest::RequestBuilder) -> Result<Upstream, String> {
    for attempt in 0..=RETRY_DELAYS.len() {
        let current = request
            .try_clone()
            .ok_or("Request body cannot be retried")?;
        let failure = match current.send().await {
            Ok(response) => {
                let status = response.status();
                let headers = response.headers().clone();
                match response.bytes().await {
                    Ok(body) if !retryable(status) || attempt == RETRY_DELAYS.len() => {
                        return Ok(Upstream {
                            status,
                            headers,
                            body,
                        });
                    }
                    Ok(_) => format!("upstream returned {status}"),
                    Err(error) => error.to_string(),
                }
            }
            Err(error) => error.to_string(),
        };
        if attempt == RETRY_DELAYS.len() {
            return Err(failure);
        }
        eprintln!("upstream request retry {}: {failure}", attempt + 1);
        tokio::time::sleep(RETRY_DELAYS[attempt]).await;
    }
    unreachable!()
}

fn gateway_error(message: &str) -> Response {
    (StatusCode::BAD_GATEWAY, Json(json!({"error": message}))).into_response()
}

fn relay_response(upstream: Upstream) -> Result<Response, String> {
    let mut response = Response::builder().status(upstream.status);
    for (name, value) in upstream.headers.iter() {
        if !matches!(
            name.as_str(),
            "connection" | "content-length" | "transfer-encoding" | "keep-alive" | "upgrade"
        ) {
            response = response.header(name, value);
        }
    }
    response
        .body(Body::from(upstream.body))
        .map_err(|error| error.to_string())
}

async fn relay_rpc(app: &App, body: Bytes) -> Result<Response, String> {
    let request = app
        .client
        .post(&app.c2_import_url)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body);
    relay_response(send_with_retry(request).await?)
}

async fn import_dataset(app: &App, dataset: Value, id: usize) -> Result<(), String> {
    let object = dataset.as_object().ok_or("Dataset must be an object")?;
    let root = object.get("root").ok_or("Dataset root is missing")?;
    let entries = object.get("entries").ok_or("Dataset entries are missing")?;
    let signatures = object
        .get("signatures")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let (method, params) = match object.get("update").filter(|update| !update.is_null()) {
        Some(update) => (UPDATE, json!([update, root, entries, signatures])),
        None => (IMPORT, json!([root, entries, signatures])),
    };
    let request = app
        .client
        .post(&app.c2_import_url)
        .json(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}));
    let response = send_with_retry(request).await?;
    if !response.status.is_success() {
        return Err(format!("C2 import returned {}", response.status));
    }
    let response: Value =
        serde_json::from_slice(&response.body).map_err(|error| error.to_string())?;
    if response["jsonrpc"] != "2.0"
        || response["id"] != json!(id)
        || response.get("result").is_none()
        || response.get("error").is_some()
    {
        return Err(format!("C2 import rejected: {response}"));
    }
    Ok(())
}

async fn submit_c1(app: &App, transaction: &Value) -> Result<String, String> {
    let request = app
        .client
        .post(app.c1_url.as_ref().ok_or("C1 RPC URL is missing")?)
        .json(&json!({"jsonrpc":"2.0","id":1,
            "method":"starknet_addInvokeTransaction",
            "params":{"invoke_transaction":transaction}}));
    let response = send_with_retry(request).await?;
    if !response.status.is_success() {
        return Err(format!("C1 returned {}", response.status));
    }
    let response: Value =
        serde_json::from_slice(&response.body).map_err(|error| error.to_string())?;
    if response.get("error").is_some() {
        return Err(format!("C1 rejected: {}", response["error"]));
    }
    response["result"]["transaction_hash"]
        .as_str()
        .map(str::to_owned)
        .ok_or("C1 returned no transaction hash".into())
}

async fn deliver_datasets(app: &App, datasets: Vec<Value>) -> Result<(), String> {
    for (index, dataset) in datasets.into_iter().enumerate() {
        import_dataset(app, dataset, index).await?;
    }
    Ok(())
}

async fn rpc(State(app): State<Arc<App>>, body: Bytes) -> Response {
    let received_at = SystemTime::now();
    let call_id = ObjectId::new();
    let request_id = call_id.to_hex();
    let call: Value = match serde_json::from_slice(&body) {
        Ok(call) => call,
        Err(_) => return rpc_error(Value::Null, -32700, "Parse error").into_response(),
    };
    let id = call.get("id").cloned().unwrap_or(Value::Null);
    if let Some(method @ (COMBINED | IMPORT | UPDATE)) = call["method"].as_str() {
        eprintln!(
            "request={request_id} received method={method} bytes={}",
            body.len()
        );
        if let Err(error) = app
            .audit
            .record(call_id, received_at, method, &id, &body)
            .await
        {
            eprintln!("request={request_id} MongoDB audit write failed: {error}");
            return rpc_error(id, -32007, "Audit log unavailable").into_response();
        }
        eprintln!("request={request_id} MongoDB audit saved method={method}");
    }
    if call["jsonrpc"] != "2.0" {
        return rpc_error(id, -32600, "Invalid Request").into_response();
    }
    match call["method"].as_str() {
        Some(method @ (IMPORT | UPDATE)) => {
            eprintln!("request={request_id} forwarding method={method} to C2");
            match relay_rpc(&app, body).await {
                Ok(response) => {
                    eprintln!(
                        "request={request_id} C2 returned method={method} http_status={}",
                        response.status()
                    );
                    response
                }
                Err(error) => {
                    eprintln!("request={request_id} C2 RPC relay failed: {error}");
                    rpc_error(id, -32005, "C2 did not acknowledge committed data").into_response()
                }
            }
        }
        Some(COMBINED) => {
            if !app.forward_transactions {
                return rpc_error(id, -32003, "Transaction forwarding is disabled").into_response();
            }
            let args = match params(&call["params"], &["datasets", "transaction"]) {
                Ok(args) if args.len() == 2 => args,
                _ => return rpc_error(id, -32602, "Invalid params").into_response(),
            };
            let datasets: Vec<Value> = match serde_json::from_value::<Vec<Value>>(args[0].clone()) {
                Ok(datasets) if (1..=4).contains(&datasets.len()) => datasets,
                _ => return rpc_error(id, -32602, "Invalid params").into_response(),
            };
            let transaction = &args[1];
            if !transaction.is_object()
                || transaction["type"] != "INVOKE"
                || transaction["is_query"] == true
            {
                return rpc_error(id, -32602, "Expected signed non-query INVOKE").into_response();
            }
            if serde_json::to_vec(transaction).is_ok_and(|bytes| bytes.len() > 1024 * 1024) {
                return rpc_error(id, -32602, "Transaction too large").into_response();
            }
            let dataset_count = datasets.len();
            eprintln!(
                "request={request_id} forwarding datasets={dataset_count} to C2 ack_before_c1={}",
                app.require_c2_ack
            );
            if app.require_c2_ack {
                if let Err(error) = deliver_datasets(&app, datasets).await {
                    eprintln!("request={request_id} C2 import failed: {error}");
                    return rpc_error(id, -32005, "C2 did not acknowledge committed data")
                        .into_response();
                }
                eprintln!("request={request_id} C2 import acknowledged datasets={dataset_count}");
            } else {
                let delivery_app = app.clone();
                let delivery_request_id = request_id.clone();
                tokio::spawn(async move {
                    if let Err(error) = deliver_datasets(&delivery_app, datasets).await {
                        eprintln!("request={delivery_request_id} C2 import exhausted retries after C1 submission: {error}");
                    } else {
                        eprintln!("request={delivery_request_id} C2 import acknowledged datasets={dataset_count}");
                    }
                });
            }
            eprintln!(
                "request={request_id} forwarding transaction_type=INVOKE nonce={} to C1",
                transaction["nonce"]
            );
            match submit_c1(&app, transaction).await {
                Ok(hash) => {
                    eprintln!("request={request_id} C1 accepted transaction_hash={hash}");
                    rpc_result(id, json!({"transaction_hash":hash})).into_response()
                }
                Err(error) => {
                    eprintln!("request={request_id} C1 submission uncertain: {error}");
                    rpc_error(id, -32006, "C1 submission needs reconciliation").into_response()
                }
            }
        }
        _ => rpc_error(id, -32601, "Method not found").into_response(),
    }
}

async fn feeder(
    State(app): State<Arc<App>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let url = format!("{}{}", app.c2_feeder_url.trim_end_matches('/'), uri);
    let mut request = app.client.request(method, url).body(body);
    for (name, value) in headers.iter() {
        if !matches!(
            name.as_str(),
            "host"
                | "connection"
                | "content-length"
                | "transfer-encoding"
                | "keep-alive"
                | "upgrade"
        ) {
            request = request.header(name, value);
        }
    }
    match send_with_retry(request).await {
        Ok(upstream) => match relay_response(upstream) {
            Ok(response) => response,
            Err(error) => {
                eprintln!("C2 feeder response failed: {error}");
                gateway_error("C2 feeder unavailable")
            }
        },
        Err(error) => {
            eprintln!("C2 feeder request failed: {error}");
            gateway_error("C2 feeder unavailable")
        }
    }
}

async fn ready(State(app): State<Arc<App>>) -> (StatusCode, &'static str) {
    let check = app
        .client
        .get(format!("{}/ready", app.c2_import_url.trim_end_matches('/')))
        .send();
    let dependencies = async { tokio::join!(check, app.audit.ready()) };
    match tokio::time::timeout(Duration::from_millis(800), dependencies).await {
        Ok((Ok(response), true)) if response.status().is_success() => (StatusCode::OK, "OK"),
        _ => (StatusCode::SERVICE_UNAVAILABLE, "C2 or MongoDB unavailable"),
    }
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let audit = Audit::connect(
        &env::var("MONGO_URI")?,
        &env::var("MONGO_DATABASE").unwrap_or_else(|_| "committed_data".into()),
        &env::var("MONGO_AUDIT_COLLECTION").unwrap_or_else(|_| "proxy_rpc_calls".into()),
    )
    .await?;
    let app = Arc::new(App {
        audit,
        client: reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .build()?,
        c1_url: env::var("C1_RPC_URL").ok(),
        c2_import_url: env::var("C2_IMPORT_URL")?,
        c2_feeder_url: env::var("C2_FEEDER_URL")?,
        forward_transactions: env::var("FORWARD_TRANSACTIONS").as_deref() == Ok("true"),
        require_c2_ack: env::var("REQUIRE_C2_ACK_BEFORE_C1").as_deref() != Ok("false"),
    });
    if app.forward_transactions && app.c1_url.is_none() {
        return Err("C1_RPC_URL required when forwarding transactions".into());
    }
    let rpc = Router::new()
        .route("/", post(rpc))
        .route("/health", get(|| async { "OK" }))
        .route("/ready", get(ready))
        .layer(DefaultBodyLimit::max(128 * 1024 * 1024))
        .with_state(app.clone());
    let feeder = Router::new()
        .route("/health", get(|| async { "OK" }))
        .route("/ready", get(ready))
        .fallback(any(feeder))
        .layer(DefaultBodyLimit::disable())
        .with_state(app.clone());
    let rpc_listener =
        tokio::net::TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], 9945))).await?;
    let feeder_listener =
        tokio::net::TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], 8080))).await?;
    eprintln!(
        "committed-data proxy listening on 9945 and 8080; stateless C2 passthrough, forwarding={}",
        app.forward_transactions
    );
    tokio::try_join!(
        axum::serve(rpc_listener, rpc),
        axum::serve(feeder_listener, feeder)
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::to_bytes, routing::post};
    use std::sync::Mutex;

    async fn json_body(response: Response) -> Value {
        serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap()).unwrap()
    }

    #[tokio::test]
    async fn c2_owns_datasets_and_feeder_while_c1_receives_only_invoke() {
        let calls = Arc::new(Mutex::new(Vec::<(String, Value)>::new()));
        let reject = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let transient_c2 = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let transient_c1 = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let transient_feeder = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let c2_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let c2_url = format!("http://{}", c2_listener.local_addr().unwrap());
        let c2_calls = calls.clone();
        let c2_reject = reject.clone();
        let c2_transient = transient_c2.clone();
        let feeder_transient = transient_feeder.clone();
        let c2_task = tokio::spawn(async move {
            axum::serve(
                c2_listener,
                Router::new()
                    .route("/ready", get(|| async { "OK" }))
                    .route(
                        "/",
                        post(move |Json(request): Json<Value>| {
                            let calls = c2_calls.clone();
                            let reject = c2_reject.clone();
                            let transient = c2_transient.clone();
                            async move {
                                calls.lock().unwrap().push(("c2".into(), request.clone()));
                                if transient.swap(false, std::sync::atomic::Ordering::SeqCst) {
                                    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error":"retry"})))
                                } else if reject.load(std::sync::atomic::Ordering::SeqCst) {
                                    (StatusCode::OK, Json(json!({"jsonrpc":"2.0","id":request["id"],
                                    "error":{"code":-32004,"message":"rejected"}})))
                                } else {
                                    (StatusCode::OK, Json(json!({"jsonrpc":"2.0","id":request["id"],"result":null})))
                                }
                            }
                        }),
                    )
                    .route(
                        "/feeder_gateway/{*path}",
                        any(move |method: Method, uri: Uri, body: Bytes| {
                            let transient = feeder_transient.clone();
                            async move {
                                if transient.swap(false, std::sync::atomic::Ordering::SeqCst) {
                                    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error":"retry"})))
                                } else {
                                    (StatusCode::ACCEPTED, Json(json!({"method":method.as_str(),
                                        "uri":uri.to_string(),"body":String::from_utf8_lossy(&body)})))
                                }
                            }
                        }),
                    ),
            )
            .await
            .unwrap();
        });
        let c1_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let c1_url = format!("http://{}", c1_listener.local_addr().unwrap());
        let c1_calls = calls.clone();
        let c1_transient = transient_c1.clone();
        let c1_task = tokio::spawn(async move {
            axum::serve(
                c1_listener,
                Router::new().route(
                    "/",
                    post(move |Json(request): Json<Value>| {
                        let calls = c1_calls.clone();
                        let transient = c1_transient.clone();
                        async move {
                            calls.lock().unwrap().push(("c1".into(), request.clone()));
                            if transient.swap(false, std::sync::atomic::Ordering::SeqCst) {
                                (
                                    StatusCode::SERVICE_UNAVAILABLE,
                                    Json(json!({"error":"retry"})),
                                )
                            } else {
                                (
                                    StatusCode::OK,
                                    Json(json!({"jsonrpc":"2.0","id":request["id"],
                                    "result":{"transaction_hash":"0x123"}})),
                                )
                            }
                        }
                    }),
                ),
            )
            .await
            .unwrap();
        });
        let app = Arc::new(App {
            audit: Audit::connect(
                &std::env::var("E2E_MONGO_URI").expect("set E2E_MONGO_URI"),
                "proxy_unit_audit",
                "calls",
            )
            .await
            .unwrap(),
            client: reqwest::Client::new(),
            c1_url: Some(c1_url),
            c2_import_url: c2_url.clone(),
            c2_feeder_url: c2_url,
            forward_transactions: true,
            require_c2_ack: true,
        });
        assert_eq!(ready(State(app.clone())).await.0, StatusCode::OK);
        let dataset = json!({"root":"0x1","entries":[["0x2","0x3"]],"signatures":[]});
        let request = json!({"jsonrpc":"2.0","id":7,"method":COMBINED,
            "params":{"datasets":[dataset],"transaction":{"type":"INVOKE","nonce":"0x1"}}});
        assert_eq!(
            json_body(rpc(State(app.clone()), Bytes::from(request.to_string())).await).await
                ["error"]["code"],
            -32005
        );
        assert!(calls
            .lock()
            .unwrap()
            .iter()
            .all(|(target, _)| target == "c2"));
        reject.store(false, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            json_body(rpc(State(app.clone()), Bytes::from(request.to_string())).await).await
                ["result"]["transaction_hash"],
            "0x123"
        );
        let recorded = calls.lock().unwrap().clone();
        assert_eq!(recorded.last().unwrap().0, "c1");
        assert_eq!(
            recorded.last().unwrap().1["method"],
            "starknet_addInvokeTransaction"
        );
        assert_eq!(recorded[recorded.len() - 2].1["method"], IMPORT);
        let standalone = json!({"jsonrpc":"2.0","id":9,"method":IMPORT,"params":["0x1",[],[]]});
        transient_c2.store(true, std::sync::atomic::Ordering::SeqCst);
        let c2_before = recorded.iter().filter(|(target, _)| target == "c2").count();
        assert_eq!(
            json_body(rpc(State(app.clone()), Bytes::from(standalone.to_string())).await).await
                ["result"],
            Value::Null
        );
        assert_eq!(
            calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(target, _)| target == "c2")
                .count(),
            c2_before + 2
        );
        assert_eq!(RETRY_DELAYS.map(|delay| delay.as_secs()), [1, 2, 4, 8, 16]);
        transient_c1.store(true, std::sync::atomic::Ordering::SeqCst);
        let c1_before = recorded.iter().filter(|(target, _)| target == "c1").count();
        assert_eq!(
            json_body(rpc(State(app.clone()), Bytes::from(request.to_string())).await).await
                ["result"]["transaction_hash"],
            "0x123"
        );
        assert_eq!(
            calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(target, _)| target == "c1")
                .count(),
            c1_before + 2
        );
        transient_feeder.store(true, std::sync::atomic::Ordering::SeqCst);
        let response = feeder(
            State(app.clone()),
            Method::POST,
            "/feeder_gateway/get_committed_data_witnesses?root=0x1"
                .parse()
                .unwrap(),
            HeaderMap::new(),
            Bytes::from_static(b"{\"keys\":[\"0x2\"]}"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = json_body(response).await;
        assert_eq!(body["method"], "POST");
        assert_eq!(
            body["uri"],
            "/feeder_gateway/get_committed_data_witnesses?root=0x1"
        );
        assert_eq!(body["body"], "{\"keys\":[\"0x2\"]}");
        reject.store(true, std::sync::atomic::Ordering::SeqCst);
        let no_ack = Arc::new(App {
            audit: app.audit.clone(),
            client: app.client.clone(),
            c1_url: app.c1_url.clone(),
            c2_import_url: app.c2_import_url.clone(),
            c2_feeder_url: app.c2_feeder_url.clone(),
            forward_transactions: true,
            require_c2_ack: false,
        });
        assert_eq!(
            json_body(rpc(State(no_ack), Bytes::from(request.to_string())).await).await["result"]
                ["transaction_hash"],
            "0x123",
            "C1 must not wait for a rejected C2 import when acknowledgement is disabled"
        );
        c1_task.abort();
        c2_task.abort();
    }
}
