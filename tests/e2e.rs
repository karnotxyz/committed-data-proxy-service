use axum::{extract::DefaultBodyLimit, routing::post, Json, Router};
use mongodb::bson::{doc, Document};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

static PROXY_PORTS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

async fn post_rpc(client: &reqwest::Client, request: Value) -> (Value, Vec<u8>) {
    let body = serde_json::to_vec(&request).unwrap();
    let response = client
        .post("http://127.0.0.1:9945/")
        .header("content-type", "application/json")
        .body(body.clone())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    (response, body)
}

#[tokio::test]
async fn proxy_archives_before_c1_and_routes_only_feeder_reads_to_c2() {
    let _ports = PROXY_PORTS.lock().await;
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
                .route("/health", axum::routing::get(|| async { "OK" }))
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
                )
                .layer(DefaultBodyLimit::max(32 * 1024 * 1024)),
        )
        .await
        .unwrap();
    });
    let c1_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let c1_url = format!("http://{}", c1_listener.local_addr().unwrap());
    let c1_calls = calls.clone();
    let c1_archive = database.collection::<Document>("calls");
    let c1_task = tokio::spawn(async move {
        axum::serve(c1_listener, Router::new().route("/", post(move |Json(request): Json<Value>| {
            let calls = c1_calls.clone();
            let archive = c1_archive.clone();
            async move {
                let manifest = archive.find_one(doc! {"method":COMBINED, "dataset_roots":"0x2"}).await.unwrap();
                assert!(manifest.is_some(), "C1 contacted before the root was indexed");
                assert_eq!(request["params"], json!({"invoke_transaction":{"type":"INVOKE","nonce":"0x1"}}));
                calls.lock().unwrap().push(("c1".into(), request["method"].as_str().unwrap().into()));
                Json(json!({"jsonrpc":"2.0","id":request["id"],"result":{"transaction_hash":"0x123"}}))
            }
        }))).await.unwrap();
    });
    let proxy = Proxy(
        Command::new(env!("CARGO_BIN_EXE_committed-data-proxy-service"))
            .env("C1_RPC_URL", c1_url)
            .env_remove("MONGO_FEEDER_ENABLED")
            .env("C2_FEEDER_URL", &c2_url)
            .env("FORWARD_TRANSACTIONS", "true")
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
    assert!(
        ready,
        "proxy did not start without a C2 write URL or RocksDB"
    );
    assert!(client
        .get("http://127.0.0.1:8080/ready")
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    let standalone_body = format!(
        "{{ \"id\": 1, \"method\": \"{IMPORT}\", \"params\": [\"0x1\", [], []], \"jsonrpc\": \"2.0\" }}\n"
    )
    .into_bytes();
    let standalone: Value = client
        .post("http://127.0.0.1:9945/")
        .header("content-type", "application/json")
        .body(standalone_body.clone())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(standalone["result"], Value::Null);
    let (update, update_body) = post_rpc(
        &client,
        json!({"jsonrpc":"2.0","id":3,"method":UPDATE,"params":[
            {"application_id":"0x1","dataset_id":"0x2","base_root":null},"0x1",[],[]]}),
    )
    .await;
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
    let (combined, combined_body) = post_rpc(
        &client,
        json!({"jsonrpc":"2.0","id":2,"method":COMBINED,"params":{
            "datasets":[{"root":"0x0002","entries":[],"signatures":[]},{"root":"0x3","entries":[],"signatures":[]}],
            "transaction":{"type":"INVOKE","nonce":"0x1"}}}),
    )
    .await;
    assert_eq!(combined["result"]["transaction_hash"], "0x123");
    let (large, large_body) = post_rpc(
        &client,
        json!({"jsonrpc":"2.0","id":4,"method":IMPORT,
            "params":["0x1",[["0x2","x".repeat(17 * 1024 * 1024)]],[]]}),
    )
    .await;
    assert_eq!(large["result"], Value::Null);
    assert!(large_body.len() > 16 * 1024 * 1024);
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        &[("c1".into(), "starknet_addInvokeTransaction".into())]
    );
    let collection = database.collection::<Document>("calls");
    let chunks = database.collection::<Document>("calls_chunks");
    let combined_manifest = collection
        .find_one(doc! {"method":COMBINED})
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        combined_manifest.get_array("dataset_roots").unwrap(),
        &vec!["0x2".into(), "0x3".into()]
    );
    let combined_id = combined_manifest.get_object_id("_id").unwrap();
    collection
        .update_one(
            doc! {"_id":combined_id},
            doc! {"$unset":{"dataset_roots":""}},
        )
        .await
        .unwrap();
    // Old proxy images did not include dataset_roots on their audit manifests.
    for expected in ["Indexed 1 archived requests", "Indexed 0 archived requests"] {
        let result = Command::new(env!("CARGO_BIN_EXE_committed-data-proxy-service"))
            .arg("--index-archive")
            .env("MONGO_URI", &mongo_uri)
            .env("MONGO_DATABASE", &database_name)
            .env("MONGO_AUDIT_COLLECTION", "calls")
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(String::from_utf8_lossy(&result.stderr).contains(expected));
    }
    assert!(collection
        .find_one(doc! {"_id":combined_id, "dataset_roots":"0x2"})
        .await
        .unwrap()
        .is_some());
    assert_eq!(collection.count_documents(doc! {}).await.unwrap(), 4);
    let mut cursor = collection
        .find(doc! {})
        .sort(doc! {"received_at_ns": 1})
        .await
        .unwrap();
    let mut methods = Vec::new();
    let expected_bodies = [standalone_body, update_body, combined_body, large_body];
    while cursor.advance().await.unwrap() {
        let entry = cursor.deserialize_current().unwrap();
        assert!(entry.get_datetime("received_at").is_ok());
        assert!(entry.get_i64("received_at_ns").unwrap() > 0);
        let index = methods.len();
        let mut body = Vec::new();
        for chunk_index in 0..entry.get_i32("chunk_count").unwrap() {
            let chunk = chunks
                .find_one(doc! {"_id": format!("{}:{chunk_index}",
                entry.get_object_id("_id").unwrap().to_hex())})
                .await
                .unwrap()
                .unwrap();
            body.extend_from_slice(chunk.get_binary_generic("payload").unwrap());
        }
        assert_eq!(body.len() as i64, entry.get_i64("request_bytes").unwrap());
        assert_eq!(
            Sha256::digest(&body).to_vec(),
            entry.get_binary_generic("payload_sha256").unwrap().clone()
        );
        assert_eq!(body, expected_bodies[index]);
        methods.push(entry.get_str("method").unwrap().to_owned());
    }
    assert_eq!(methods, [IMPORT, UPDATE, COMBINED, IMPORT]);
    // A valid request may not reach C1 if MongoDB rejects its manifest.
    database
        .run_command(doc! {
            "collMod":"calls",
            "validator":{"dataset_roots":{"$ne":"0x2"}},
        })
        .await
        .unwrap();
    let (failed, _) = post_rpc(
        &client,
        json!({
            "jsonrpc":"2.0","id":5,"method":COMBINED,"params":[
                [{"root":"0x2","entries":[],"signatures":[]}],
                {"type":"INVOKE","nonce":"0x1"}]
        }),
    )
    .await;
    assert_eq!(failed["error"]["code"], -32007);
    assert_eq!(calls.lock().unwrap().len(), 1);

    drop(proxy);
    database.drop().await.unwrap();
    c1_task.abort();
    c2_task.abort();
}

#[path = "../src/patricia.rs"]
mod patricia;
use starknet_core::{crypto::ecdsa_sign, utils::starknet_keccak};
use starknet_types_core::{curve::AffinePoint, felt::Felt};

fn signed_dataset(root: Felt, entries: Value, update: Option<Value>) -> Value {
    let mut bytes = if update.is_some() {
        b"MADARA_COMMITTED_DATA_UPDATE_V1".to_vec()
    } else {
        b"MADARA_COMMITTED_DATA_IMPORT_V1".to_vec()
    };
    bytes.extend(Felt::ONE.to_bytes_be());
    if let Some(update) = &update {
        for name in ["application_id", "dataset_id"] {
            bytes.extend(
                Felt::from_hex(update[name].as_str().unwrap())
                    .unwrap()
                    .to_bytes_be(),
            );
        }
        let base = update["base_root"]
            .as_str()
            .map(|v| Felt::from_hex(v).unwrap());
        bytes.push(u8::from(base.is_some()));
        bytes.extend(base.unwrap_or(Felt::ZERO).to_bytes_be());
    }
    bytes.extend(root.to_bytes_be());
    bytes.extend((entries.as_array().unwrap().len() as u32).to_be_bytes());
    let signature = ecdsa_sign(&Felt::ONE, &starknet_keccak(&bytes)).unwrap();
    json!({
        "root":root, "update":update, "entries":entries,
        "signatures":[{"public_key":AffinePoint::generator().x(), "r":signature.r, "s":signature.s}]
    })
}

async fn start_feeder_probe(command: &mut Command, client: &reqwest::Client) -> Proxy {
    let proxy = Proxy(
        command
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    for _ in 0..100 {
        if client
            .get("http://127.0.0.1:8080/ready")
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return proxy;
        }
        tokio::time::sleep(Duration::from_millis(75)).await;
    }
    panic!("feeder did not become ready");
}

async fn feeder_get(client: &reqwest::Client, query: &str) -> Value {
    client
        .get(format!("http://127.0.0.1:8080/feeder_gateway/{query}"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

#[tokio::test]
async fn mongo_feeder_materializes_updates_and_serves_them_after_restart() {
    let _ports = PROXY_PORTS.lock().await;
    let uri = std::env::var("E2E_MONGO_URI").expect("set E2E_MONGO_URI");
    let mongo = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let database_name = format!("proxy_mongo_feeder_{}", std::process::id());
    let database = mongo.database(&database_name);
    let datasets = database.collection::<Document>("committed_data_datasets");
    let manifests = database.collection::<Document>("calls");
    let client = reqwest::Client::new();

    let base_tree = patricia::CommittedDataSet::new(
        (1..=4097)
            .map(|index| (Felt::from(index as u64), Felt::ONE))
            .collect(),
    )
    .unwrap();
    let update_tree = patricia::CommittedDataSet::new(
        base_tree
            .entries()
            .iter()
            .map(|(key, value)| {
                (
                    *key,
                    if *key == Felt::from(4097u64) {
                        Felt::TWO
                    } else {
                        *value
                    },
                )
            })
            .collect(),
    )
    .unwrap();
    let base = base_tree.root();
    let root = update_tree.root();
    let namespace =
        |base: Option<Felt>| json!({"application_id":"0x1", "dataset_id":"0x2", "base_root":base});
    let init = signed_dataset(base, json!(base_tree.entries()), Some(namespace(None)));
    let patch = signed_dataset(
        root,
        json!([[Felt::from(4097u64), Felt::TWO]]),
        Some(namespace(Some(base))),
    );

    let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_url = format!("http://{}", upstream.local_addr().unwrap());
    let archived = manifests.clone();
    let materialized = datasets.clone();
    let task = tokio::spawn(async move {
        axum::serve(upstream, Router::new()
            .route("/health", axum::routing::get(|| async { "OK" }))
            .route("/", post(move |Json(call): Json<Value>| {
                let archived = archived.clone();
                let materialized = materialized.clone();
                async move {
                    assert_eq!(call["method"], "starknet_addInvokeTransaction");
                    assert!(archived.find_one(doc! {"method":COMBINED, "dataset_roots":format!("{root:#x}")}).await.unwrap().is_some());
                    assert!(materialized.find_one(doc! {"_id":format!("root/{}", hex::encode(root.to_bytes_be()))}).await.unwrap().is_some());
                    Json(json!({"jsonrpc":"2.0","id":call["id"],"result":{"transaction_hash":"0xabc"}}))
                }
            }))).await.unwrap();
    });

    let command = || {
        let mut bin = Command::new(env!("CARGO_BIN_EXE_committed-data-proxy-service"));
        bin.env("MONGO_URI", &uri)
            .env("MONGO_DATABASE", &database_name)
            .env("MONGO_AUDIT_COLLECTION", "calls")
            .env("CHAIN_ID", "0x1")
            .env(
                "COMMITTED_DATA_SIGNERS",
                format!("{:#x}", AffinePoint::generator().x()),
            )
            .env("FORWARD_TRANSACTIONS", "true")
            .env("C1_RPC_URL", &upstream_url)
            .env_remove("C2_FEEDER_URL")
            .env_remove("C2_IMPORT_URL")
            .env("MONGO_FEEDER_ENABLED", "true");
        bin
    };

    let previous = start_feeder_probe(
        command()
            .env("MONGO_FEEDER_ENABLED", "false")
            .env("C2_FEEDER_URL", &upstream_url),
        &client,
    )
    .await;
    let (standalone, _) = post_rpc(
        &client,
        json!({"jsonrpc":"2.0","id":10,"method":UPDATE,
        "params":[init["update"], init["root"], init["entries"], init["signatures"]]}),
    )
    .await;
    assert_eq!(standalone.get("result"), Some(&Value::Null));
    assert_eq!(datasets.count_documents(doc! {}).await.unwrap(), 0);
    drop(previous);

    for expected in [
        "Materialized 1 archived datasets",
        "Materialized 0 archived datasets",
    ] {
        let result = command().arg("--materialize-archive").output().unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(String::from_utf8_lossy(&result.stderr).contains(expected));
    }
    let proxy = start_feeder_probe(&mut command(), &client).await;
    let (result, _) = post_rpc(
        &client,
        json!({"jsonrpc":"2.0","id":11,"method":COMBINED,
        "params":[[patch], {"type":"INVOKE","nonce":"0x2"}]}),
    )
    .await;
    assert_eq!(result["result"]["transaction_hash"], "0xabc");

    let mut roots = vec![base, root];
    roots.sort();
    assert_eq!(
        feeder_get(&client, "get_committed_data_roots").await,
        json!(roots)
    );
    assert_eq!(
        feeder_get(
            &client,
            &format!("get_committed_data_roots?after={:#x}", roots[0])
        )
        .await,
        json!([roots[1]]),
    );
    let versions = feeder_get(&client, "get_committed_data_versions").await;
    assert_eq!(versions.as_array().unwrap().len(), 2);
    assert_eq!(
        versions[0],
        json!({"application_id":"0x1","dataset_id":"0x2","root":roots[0]})
    );
    assert_eq!(
        feeder_get(
            &client,
            &format!(
                "get_committed_data_versions?application_id=0x1&dataset_id=0x2&root={:#x}",
                roots[0]
            )
        )
        .await,
        json!([versions[1]]),
    );
    let range = doc! {"_id":{"$gt":"page/","$lt":"page/~"}};
    assert_eq!(
        datasets.count_documents(range).await.unwrap(),
        3,
        "unchanged first page is shared"
    );

    let mut unauthorized = patch.clone();
    unauthorized["signatures"] = json!([]);
    let (rejected, _) = post_rpc(
        &client,
        json!({"jsonrpc":"2.0","id":12,"method":COMBINED,
        "params":[[unauthorized], {"type":"INVOKE","nonce":"0x3"}]}),
    )
    .await;
    assert_eq!(rejected["error"]["code"], -32005);
    assert!(manifests
        .find_one(doc! {"rpc_id":"12"})
        .await
        .unwrap()
        .is_some());

    drop(proxy);
    let proxy = start_feeder_probe(&mut command(), &client).await;
    let page = feeder_get(
        &client,
        &format!("get_committed_data?root={root:#x}&start=4096"),
    )
    .await;
    assert_eq!(
        page,
        json!({"root":root,"count":4097,"start":4096,"entries":[["0x1001","0x2"]]})
    );
    let witness = feeder_get(
        &client,
        &format!("get_committed_data_witness?root={root:#x}&key=0x1001"),
    )
    .await;
    assert_eq!(
        witness,
        json!(update_tree.lookup_witness(Felt::from(4097u64)))
    );
    let batch = feeder_get(
        &client,
        &format!("get_committed_data_witnesses?root={root:#x}&keys=0x1001,0x1002"),
    )
    .await;
    let post: Value = client
        .post("http://127.0.0.1:8080/feeder_gateway/get_committed_data_witnesses")
        .json(&json!({"root":root,"keys":["0x1001","0x1002"]}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(batch, post);
    assert_eq!(batch[0], witness);
    assert!(batch[1]["terminal_key"].is_string());

    drop(proxy);
    task.abort();
    database.drop().await.unwrap();
}
