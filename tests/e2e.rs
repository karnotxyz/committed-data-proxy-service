use axum::{routing::post, Json, Router};
use mongodb::bson::{doc, Document};
use serde_json::{json, Value};
use std::{
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

const COMBINED: &str = "madara_V0_1_0_importCommittedDataAndSubmitTransaction";
const IMPORT: &str = "madara_V0_1_0_importCommittedDataSet";
const UPDATE: &str = "madara_V0_1_0_updateCommittedDataSet";

struct Proxy(Child);

impl Drop for Proxy {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn stateless_proxy_routes_dataset_reads_and_root_transaction() {
    let mongo_uri = std::env::var("E2E_MONGO_URI").expect("set E2E_MONGO_URI");
    let mongo = mongodb::Client::with_uri_str(&mongo_uri).await.unwrap();
    let database_name = format!("proxy_audit_e2e_{}", std::process::id());
    let database = mongo.database(&database_name);
    let calls = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
    let c2_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let c2_url = format!("http://{}", c2_listener.local_addr().unwrap());
    let c2_calls = calls.clone();
    let c2_task = tokio::spawn(async move {
        axum::serve(
            c2_listener,
            Router::new()
                .route("/ready", axum::routing::get(|| async { "OK" }))
                .route(
                    "/",
                    post(move |Json(request): Json<Value>| {
                        let calls = c2_calls.clone();
                        async move {
                            calls
                                .lock()
                                .unwrap()
                                .push(("c2".into(), request["method"].as_str().unwrap().into()));
                            Json(json!({"jsonrpc":"2.0","id":request["id"],"result":null}))
                        }
                    }),
                )
                .route(
                    "/feeder_gateway/get_committed_data_roots",
                    axum::routing::get(|| async { Json(json!(["0x1"])) }),
                ),
        )
        .await
        .unwrap();
    });
    let c1_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let c1_url = format!("http://{}", c1_listener.local_addr().unwrap());
    let c1_calls = calls.clone();
    let c1_task = tokio::spawn(async move {
        axum::serve(c1_listener, Router::new().route("/", post(move |Json(request): Json<Value>| {
            let calls = c1_calls.clone();
            async move {
                calls.lock().unwrap().push(("c1".into(), request["method"].as_str().unwrap().into()));
                Json(json!({"jsonrpc":"2.0","id":request["id"],"result":{"transaction_hash":"0x123"}}))
            }
        }))).await.unwrap();
    });
    let proxy = Proxy(
        Command::new(env!("CARGO_BIN_EXE_committed-data-proxy-service"))
            .env("C1_RPC_URL", c1_url)
            .env("C2_IMPORT_URL", &c2_url)
            .env("C2_FEEDER_URL", &c2_url)
            .env("FORWARD_TRANSACTIONS", "true")
            .env("REQUIRE_C2_ACK_BEFORE_C1", "true")
            .env("MONGO_URI", &mongo_uri)
            .env("MONGO_DATABASE", &database_name)
            .env("MONGO_AUDIT_COLLECTION", "calls")
            .env("ROCKSDB_PATH", "/nonexistent/read-only-path")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let client = reqwest::Client::new();
    let mut ready = false;
    for _ in 0..100 {
        if client
            .get("http://127.0.0.1:9945/ready")
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(ready, "proxy did not start without RocksDB");
    let standalone: Value = client
        .post("http://127.0.0.1:9945/")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":IMPORT,"params":["0x1",[],[]]}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(standalone["result"], Value::Null);
    let update: Value = client
        .post("http://127.0.0.1:9945/")
        .json(&json!({"jsonrpc":"2.0","id":3,"method":UPDATE,"params":[
            {"application_id":"0x1","dataset_id":"0x2","base_root":null},"0x1",[],[]]}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(update["result"], Value::Null);
    let roots: Value = client
        .get("http://127.0.0.1:8080/feeder_gateway/get_committed_data_roots")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(roots, json!(["0x1"]));
    let combined: Value = client
        .post("http://127.0.0.1:9945/")
        .json(&json!({"jsonrpc":"2.0","id":2,"method":COMBINED,"params":{
            "datasets":[{"root":"0x1","entries":[],"signatures":[]}],
            "transaction":{"type":"INVOKE","nonce":"0x1"}}}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(combined["result"]["transaction_hash"], "0x123");
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        &[
            ("c2".into(), IMPORT.into()),
            ("c2".into(), UPDATE.into()),
            ("c2".into(), IMPORT.into()),
            ("c1".into(), "starknet_addInvokeTransaction".into()),
        ]
    );
    let collection = database.collection::<Document>("calls");
    assert_eq!(collection.count_documents(doc! {}).await.unwrap(), 3);
    let mut cursor = collection
        .find(doc! {})
        .sort(doc! {"received_at_ns": 1})
        .await
        .unwrap();
    let mut methods = Vec::new();
    while cursor.advance().await.unwrap() {
        let entry = cursor.deserialize_current().unwrap();
        assert!(entry.get_datetime("received_at").is_ok());
        assert!(entry.get_i64("received_at_ns").unwrap() > 0);
        assert!(entry.get_i64("request_bytes").unwrap() > 0);
        methods.push(entry.get_str("method").unwrap().to_owned());
    }
    assert_eq!(methods, [IMPORT, UPDATE, COMBINED]);
    drop(proxy);
    database.drop().await.unwrap();
    c1_task.abort();
    c2_task.abort();
}
