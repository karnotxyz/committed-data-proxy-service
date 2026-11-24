//! Exercise the deployed HTTP surface, RocksDB restart, Mongo recovery archive, and forwarding order.
//! Run with E2E_MONGO_URI pointing at a disposable MongoDB instance.

use axum::{extract::State, routing::post, Json, Router};
use mongodb::bson::{doc, Document};
use serde_json::{json, Value};
use starknet_core::{crypto::ecdsa_sign, utils::starknet_keccak};
use starknet_types_core::{curve::AffinePoint, felt::Felt};
use std::{
    fs,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tempfile::TempDir;

const IMPORT: &str = "madara_V0_1_0_importCommittedDataSet";
const UPDATE: &str = "madara_V0_1_0_updateCommittedDataSet";
const COMBINED: &str = "madara_V0_1_0_importCommittedDataAndSubmitTransaction";
const ROOT_1: &str = "3000094458541565324654730242423920536387969495385651240652446843684607154951";
const ROOT_2: &str = "276299005619304478918798114286346347891379591787649727104272896463670221328";

#[derive(Default)]
struct MockCalls {
    reject_c2: AtomicBool,
    events: Mutex<Vec<(String, Value)>>,
}

async fn c2(State(calls): State<Arc<MockCalls>>, Json(request): Json<Value>) -> Json<Value> {
    calls
        .events
        .lock()
        .unwrap()
        .push(("c2".into(), request.clone()));
    if calls.reject_c2.load(Ordering::SeqCst) {
        Json(
            json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32004,"message":"not ready"}}),
        )
    } else {
        Json(json!({"jsonrpc":"2.0","id":request["id"],"result":null}))
    }
}

async fn c1(State(calls): State<Arc<MockCalls>>, Json(request): Json<Value>) -> Json<Value> {
    calls
        .events
        .lock()
        .unwrap()
        .push(("c1".into(), request.clone()));
    Json(json!({"jsonrpc":"2.0","id":request["id"],"result":{"transaction_hash":"0x123"}}))
}

struct Proxy {
    child: Child,
    log: PathBuf,
}

impl Drop for Proxy {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn start_proxy(
    dir: &TempDir,
    mongo_uri: &str,
    database: &str,
    c1_url: &str,
    c2_url: &str,
) -> Proxy {
    let log = dir.path().join("proxy.log");
    let output = fs::File::create(&log).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_committed-data-proxy-service"))
        .env("CHAIN_ID", "0x1")
        .env(
            "COMMITTED_DATA_SIGNERS",
            format!("{:#x}", AffinePoint::generator().x()),
        )
        .env("ROCKSDB_PATH", dir.path().join("rocksdb"))
        .env("MONGO_URI", mongo_uri)
        .env("MONGO_DATABASE", database)
        .env("MONGO_COLLECTION_PREFIX", "mempool_transactions")
        .env("MONGO_REQUIRED", "true")
        .env("FORWARD_TRANSACTIONS", "true")
        .env("REQUIRE_C2_ACK_BEFORE_C1", "true")
        .env("C1_RPC_URL", c1_url)
        .env("C2_IMPORT_URL", c2_url)
        .stdout(Stdio::from(output.try_clone().unwrap()))
        .stderr(Stdio::from(output))
        .spawn()
        .unwrap();
    let mut proxy = Proxy { child, log };
    let client = reqwest::Client::new();
    for _ in 0..100 {
        if client
            .get("http://127.0.0.1:9945/ready")
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
            && client
                .get("http://127.0.0.1:8080/ready")
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
        {
            return proxy;
        }
        if proxy.child.try_wait().unwrap().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "proxy did not become ready: {}",
        fs::read_to_string(&proxy.log).unwrap()
    );
}

fn signed(root: Felt, entries: Value, update: Option<Value>) -> Value {
    let mut preimage = if update.is_some() {
        b"MADARA_COMMITTED_DATA_UPDATE_V1".to_vec()
    } else {
        b"MADARA_COMMITTED_DATA_IMPORT_V1".to_vec()
    };
    preimage.extend(Felt::ONE.to_bytes_be());
    if let Some(update) = &update {
        preimage.extend(Felt::ONE.to_bytes_be()); // application_id
        preimage.extend(Felt::TWO.to_bytes_be()); // dataset_id
        let base = update["base_root"]
            .as_str()
            .map(|s| Felt::from_hex(s).unwrap());
        preimage.push(u8::from(base.is_some()));
        preimage.extend(base.unwrap_or(Felt::ZERO).to_bytes_be());
    }
    preimage.extend(root.to_bytes_be());
    preimage.extend((entries.as_array().unwrap().len() as u32).to_be_bytes());
    let signature = ecdsa_sign(&Felt::ONE, &starknet_keccak(&preimage)).unwrap();
    json!({"update":update,"root":root,"entries":entries,"signatures":[{
        "public_key":AffinePoint::generator().x(),"r":signature.r,"s":signature.s
    }]})
}

async fn rpc(client: &reqwest::Client, method: &str, params: Value) -> Value {
    let response = client
        .post("http://127.0.0.1:9945/")
        .json(&json!({"jsonrpc":"2.0","id":7,"method":method,"params":params}))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    response.json().await.unwrap()
}

async fn get(client: &reqwest::Client, path: &str) -> Value {
    let response = client
        .get(format!("http://127.0.0.1:8080{path}"))
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "{path}: {}",
        response.status()
    );
    response.json().await.unwrap()
}

#[tokio::test]
async fn committed_data_http_rocksdb_mongo_and_forwarding() {
    let mongo_uri =
        std::env::var("E2E_MONGO_URI").expect("set E2E_MONGO_URI to a disposable MongoDB");
    let mongo = mongodb::Client::with_uri_str(&mongo_uri).await.unwrap();
    let database = format!("proxy_e2e_{}", uuid::Uuid::new_v4().simple());
    let db = mongo.database(&database);
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let calls = Arc::new(MockCalls::default());
    let c1_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let c2_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let c1_url = format!("http://{}", c1_listener.local_addr().unwrap());
    let c2_url = format!("http://{}", c2_listener.local_addr().unwrap());
    let c1_calls = calls.clone();
    let c1_task = tokio::spawn(async move {
        axum::serve(
            c1_listener,
            Router::new().route("/", post(c1)).with_state(c1_calls),
        )
        .await
        .unwrap()
    });
    let c2_calls = calls.clone();
    let c2_task = tokio::spawn(async move {
        axum::serve(
            c2_listener,
            Router::new().route("/", post(c2)).with_state(c2_calls),
        )
        .await
        .unwrap()
    });
    let mut proxy = start_proxy(&dir, &mongo_uri, &database, &c1_url, &c2_url).await;

    for port in [9945, 8080] {
        for path in ["health", "ready"] {
            let response = client
                .get(format!("http://127.0.0.1:{port}/{path}"))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(response.text().await.unwrap(), "OK");
        }
    }

    let root1 = Felt::from_dec_str(ROOT_1).unwrap();
    let root2 = Felt::from_dec_str(ROOT_2).unwrap();
    let first = signed(root1, json!([["0x2", "0x64"]]), None);
    let initial_update = json!({"application_id":"0x1","dataset_id":"0x2","base_root":null});
    let initial = signed(
        root1,
        first["entries"].clone(),
        Some(initial_update.clone()),
    );
    let update = json!({"application_id":"0x1","dataset_id":"0x2","base_root":root1});
    let second = signed(
        root2,
        json!([["0x3", "0x7d"], ["0xa", "0xc8"]]),
        Some(update.clone()),
    );
    let mut invalid = first.clone();
    invalid["signatures"][0]["s"] = json!("0x0");
    assert_eq!(
        rpc(
            &client,
            IMPORT,
            json!([root1, first["entries"], invalid["signatures"]])
        )
        .await["error"]["code"],
        -32001
    );
    assert_eq!(
        rpc(
            &client,
            IMPORT,
            json!([root1, first["entries"], first["signatures"]])
        )
        .await["result"],
        Value::Null
    );
    assert_eq!(
        rpc(
            &client,
            UPDATE,
            json!([
                initial_update,
                root1,
                initial["entries"],
                initial["signatures"]
            ])
        )
        .await["result"],
        Value::Null
    );
    assert_eq!(
        rpc(
            &client,
            UPDATE,
            json!([update, root2, second["entries"], second["signatures"]])
        )
        .await["result"],
        Value::Null
    );
    assert!(
        calls.events.lock().unwrap().is_empty(),
        "standalone imports must not forward"
    );

    let roots = get(&client, "/feeder_gateway/get_committed_data_roots").await;
    assert_eq!(roots, json!([root2, root1]));
    let versions = get(&client, "/feeder_gateway/get_committed_data_versions").await;
    assert_eq!(versions.len(), 2);
    assert!(versions
        .as_array()
        .unwrap()
        .contains(&json!({"application_id":"0x1","dataset_id":"0x2","root":root2})));
    let page = get(
        &client,
        &format!("/feeder_gateway/get_committed_data?root={root2:#x}&start=0"),
    )
    .await;
    assert_eq!(
        page,
        json!({"root":root2,"start":0,"count":3,
        "entries":[["0x2","0x64"],["0x3","0x7d"],["0xa","0xc8"]]})
    );
    let witness = get(
        &client,
        &format!("/feeder_gateway/get_committed_data_witness?root={root2:#x}&key=0x3"),
    )
    .await;
    let batch_get = get(
        &client,
        &format!("/feeder_gateway/get_committed_data_witnesses?root={root2:#x}&keys=0x3,0x4"),
    )
    .await;
    let batch_post: Value = client
        .post("http://127.0.0.1:8080/feeder_gateway/get_committed_data_witnesses")
        .json(&json!({"root":root2,"keys":["0x3","0x4"]}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(batch_get, batch_post);
    assert_eq!(batch_get[0], witness);
    assert_eq!(batch_get[1]["key"], "0x4");
    assert!(batch_get[1]["terminal_key"].is_string());
    assert_eq!(
        client
            .get(format!(
                "http://127.0.0.1:8080/feeder_gateway/get_committed_data?root={root2:#x}&start=1"
            ))
            .send()
            .await
            .unwrap()
            .status(),
        400
    );

    calls.reject_c2.store(true, Ordering::SeqCst);
    let transaction = json!({"type":"INVOKE","nonce":"0x1"});
    let request = json!([[second], transaction]);
    assert_eq!(
        rpc(&client, COMBINED, request.clone()).await["error"]["code"],
        -32005
    );
    assert!(calls
        .events
        .lock()
        .unwrap()
        .iter()
        .all(|(target, _)| target == "c2"));
    calls.reject_c2.store(false, Ordering::SeqCst);
    assert_eq!(
        rpc(&client, COMBINED, request.clone()).await["result"]["transaction_hash"],
        "0x123"
    );
    assert_eq!(
        rpc(&client, COMBINED, request).await["result"]["transaction_hash"],
        "0x123"
    );
    let events = calls.events.lock().unwrap();
    let c1_events: Vec<_> = events.iter().filter(|(target, _)| target == "c1").collect();
    assert_eq!(c1_events.len(), 1, "retry must not submit C1 twice");
    assert_eq!(c1_events[0].1["method"], "starknet_addInvokeTransaction");
    assert_eq!(c1_events[0].1["params"]["invoke_transaction"], transaction);
    assert_eq!(
        events
            .iter()
            .filter(|(target, _)| target == "c2")
            .last()
            .unwrap()
            .1["method"],
        UPDATE
    );
    drop(events);

    let manifests = db.collection::<Document>("mempool_transactions_committed_manifests_v2");
    let chunks = db.collection::<Document>("mempool_transactions_committed_chunks_v2");
    let mut archived = false;
    for _ in 0..60 {
        if manifests.count_documents(doc! {}).await.unwrap_or(0) == 3
            && chunks.count_documents(doc! {}).await.unwrap_or(0) == 2
        {
            archived = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        archived,
        "three imports must be exported to Mongo: {}",
        fs::read_to_string(&proxy.log).unwrap()
    );
    let mut cursor = manifests.find(doc! {}).await.unwrap();
    let mut records = Vec::new();
    while cursor.advance().await.unwrap() {
        let doc = cursor.deserialize_current().unwrap();
        let payload: Value =
            serde_json::from_slice(doc.get_binary_generic("payload").unwrap()).unwrap();
        records.push((doc.get_str("_id").unwrap().to_owned(), payload));
    }
    let base = records
        .iter()
        .find(|(_, manifest)| manifest["root"] == json!(root1) && !manifest["update"].is_null())
        .unwrap();
    let patch = records
        .iter()
        .find(|(_, manifest)| manifest["root"] == json!(root2))
        .unwrap();
    assert_eq!(base.1["version"], 2);
    assert_eq!(base.1["chain_id"], "0x1");
    assert_eq!(base.1["entries"], 1);
    assert_eq!(patch.1["base"], base.0);
    assert_eq!(
        patch.1["entries"], 2,
        "Mongo stores upserts, while RocksDB stores the merged set"
    );
    for (_, manifest) in &records {
        let hash = manifest["chunks"][0].as_str().unwrap();
        let chunk = chunks.find_one(doc! {"_id":hash}).await.unwrap().unwrap();
        let raw = zstd::stream::decode_all(chunk.get_binary_generic("payload").unwrap()).unwrap();
        assert_eq!(
            raw.len(),
            manifest["entries"].as_u64().unwrap() as usize * 64
        );
    }

    drop(proxy);
    proxy = start_proxy(&dir, &mongo_uri, &database, &c1_url, &c2_url).await;
    assert_eq!(
        get(&client, "/feeder_gateway/get_committed_data_roots").await,
        roots
    );
    assert_eq!(
        get(&client, "/feeder_gateway/get_committed_data_versions").await,
        versions
    );
    assert_eq!(
        get(
            &client,
            &format!("/feeder_gateway/get_committed_data?root={root2:#x}&start=0")
        )
        .await,
        page
    );
    assert_eq!(
        get(
            &client,
            &format!("/feeder_gateway/get_committed_data_witness?root={root2:#x}&key=0x3")
        )
        .await,
        witness
    );
    drop(proxy);
    db.drop().await.unwrap();
    c1_task.abort();
    c2_task.abort();
}
