//! Stateless mode: C2 owns committed-data validation, persistence, and feeder reads.
use crate::{params, rpc_error, rpc_result, store::Dataset, COMBINED, IMPORT, UPDATE};
use axum::{
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
    http::{header, HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{any, get, post},
    Json, Router,
};
use serde_json::{json, Value};
use std::{env, net::SocketAddr, sync::Arc, time::Duration};

struct App {
    client: reqwest::Client,
    c1_url: Option<String>,
    c2_import_url: String,
    c2_feeder_url: String,
    forward_transactions: bool,
}

fn gateway_error(message: &str) -> Response {
    (StatusCode::BAD_GATEWAY, Json(json!({"error": message}))).into_response()
}

async fn relay_response(upstream: reqwest::Response) -> Result<Response, String> {
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let body = upstream.bytes().await.map_err(|error| error.to_string())?;
    let mut response = Response::builder().status(status);
    for (name, value) in headers.iter() {
        if !matches!(
            name.as_str(),
            "connection"
                | "content-length"
                | "transfer-encoding"
                | "content-encoding"
                | "keep-alive"
                | "upgrade"
        ) {
            response = response.header(name, value);
        }
    }
    response
        .body(Body::from(body))
        .map_err(|error| error.to_string())
}

async fn relay_rpc(app: &App, body: Bytes) -> Result<Response, String> {
    let upstream = app
        .client
        .post(&app.c2_import_url)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    relay_response(upstream).await
}

async fn import_dataset(app: &App, dataset: Dataset, id: usize) -> Result<(), String> {
    let (method, params) = match dataset.update {
        Some(update) => (
            UPDATE,
            json!([update, dataset.root, dataset.entries, dataset.signatures]),
        ),
        None => (
            IMPORT,
            json!([dataset.root, dataset.entries, dataset.signatures]),
        ),
    };
    let response = app
        .client
        .post(&app.c2_import_url)
        .json(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
        .send()
        .await
        .map_err(|error| error.to_string())?
        .error_for_status()
        .map_err(|error| error.to_string())?;
    let response: Value = response.json().await.map_err(|error| error.to_string())?;
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
    let response: Value = app
        .client
        .post(app.c1_url.as_ref().ok_or("C1 RPC URL is missing")?)
        .json(&json!({"jsonrpc":"2.0","id":1,
            "method":"starknet_addInvokeTransaction",
            "params":{"invoke_transaction":transaction}}))
        .send()
        .await
        .map_err(|error| error.to_string())?
        .error_for_status()
        .map_err(|error| error.to_string())?
        .json()
        .await
        .map_err(|error| error.to_string())?;
    if response.get("error").is_some() {
        return Err(format!("C1 rejected: {}", response["error"]));
    }
    response["result"]["transaction_hash"]
        .as_str()
        .map(str::to_owned)
        .ok_or("C1 returned no transaction hash".into())
}

async fn rpc(State(app): State<Arc<App>>, body: Bytes) -> Response {
    let call: Value = match serde_json::from_slice(&body) {
        Ok(call) => call,
        Err(_) => return rpc_error(Value::Null, -32700, "Parse error").into_response(),
    };
    let id = call.get("id").cloned().unwrap_or(Value::Null);
    if call["jsonrpc"] != "2.0" {
        return rpc_error(id, -32600, "Invalid Request").into_response();
    }
    match call["method"].as_str() {
        Some(IMPORT | UPDATE) => match relay_rpc(&app, body).await {
            Ok(response) => response,
            Err(error) => {
                eprintln!("C2 RPC relay failed: {error}");
                rpc_error(id, -32005, "C2 did not acknowledge committed data").into_response()
            }
        },
        Some(COMBINED) => {
            if !app.forward_transactions {
                return rpc_error(id, -32003, "Transaction forwarding is disabled").into_response();
            }
            let args = match params(&call["params"], &["datasets", "transaction"]) {
                Ok(args) if args.len() == 2 => args,
                _ => return rpc_error(id, -32602, "Invalid params").into_response(),
            };
            let datasets: Vec<Dataset> =
                match serde_json::from_value::<Vec<Dataset>>(args[0].clone()) {
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
            for (index, dataset) in datasets.into_iter().enumerate() {
                if let Err(error) = import_dataset(&app, dataset, index).await {
                    eprintln!("C2 import failed: {error}");
                    return rpc_error(id, -32005, "C2 did not acknowledge committed data")
                        .into_response();
                }
            }
            match submit_c1(&app, transaction).await {
                Ok(hash) => rpc_result(id, json!({"transaction_hash":hash})).into_response(),
                Err(error) => {
                    eprintln!("C1 submission uncertain: {error}");
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
    match request.send().await {
        Ok(upstream) => match relay_response(upstream).await {
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
    match tokio::time::timeout(Duration::from_millis(800), check).await {
        Ok(Ok(response)) if response.status().is_success() => (StatusCode::OK, "OK"),
        _ => (StatusCode::SERVICE_UNAVAILABLE, "C2 unavailable"),
    }
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let app = Arc::new(App {
        client: reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .build()?,
        c1_url: env::var("C1_RPC_URL").ok(),
        c2_import_url: env::var("C2_IMPORT_URL")?,
        c2_feeder_url: env::var("C2_FEEDER_URL")?,
        forward_transactions: env::var("FORWARD_TRANSACTIONS").as_deref() == Ok("true"),
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
        .route("/feeder_gateway/{*path}", any(feeder))
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
        let c2_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let c2_url = format!("http://{}", c2_listener.local_addr().unwrap());
        let c2_calls = calls.clone();
        let c2_reject = reject.clone();
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
                            async move {
                                calls.lock().unwrap().push(("c2".into(), request.clone()));
                                if reject.load(std::sync::atomic::Ordering::SeqCst) {
                                    Json(json!({"jsonrpc":"2.0","id":request["id"],
                                    "error":{"code":-32004,"message":"rejected"}}))
                                } else {
                                    Json(json!({"jsonrpc":"2.0","id":request["id"],"result":null}))
                                }
                            }
                        }),
                    )
                    .route(
                        "/feeder_gateway/{*path}",
                        any(|method: Method, uri: Uri, body: Bytes| async move {
                            (
                                StatusCode::ACCEPTED,
                                Json(json!({"method":method.as_str(),
                            "uri":uri.to_string(),"body":String::from_utf8_lossy(&body)})),
                            )
                        }),
                    ),
            )
            .await
            .unwrap();
        });
        let c1_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let c1_url = format!("http://{}", c1_listener.local_addr().unwrap());
        let c1_calls = calls.clone();
        let c1_task = tokio::spawn(async move {
            axum::serve(
                c1_listener,
                Router::new().route(
                    "/",
                    post(move |Json(request): Json<Value>| {
                        let calls = c1_calls.clone();
                        async move {
                            calls.lock().unwrap().push(("c1".into(), request.clone()));
                            Json(json!({"jsonrpc":"2.0","id":request["id"],
                        "result":{"transaction_hash":"0x123"}}))
                        }
                    }),
                ),
            )
            .await
            .unwrap();
        });
        let app = Arc::new(App {
            client: reqwest::Client::new(),
            c1_url: Some(c1_url),
            c2_import_url: c2_url.clone(),
            c2_feeder_url: c2_url,
            forward_transactions: true,
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
        let calls = calls.lock().unwrap().clone();
        assert_eq!(calls.last().unwrap().0, "c1");
        assert_eq!(
            calls.last().unwrap().1["method"],
            "starknet_addInvokeTransaction"
        );
        assert_eq!(calls[calls.len() - 2].1["method"], IMPORT);
        let standalone = json!({"jsonrpc":"2.0","id":9,"method":IMPORT,"params":["0x1",[],[]]});
        assert_eq!(
            json_body(rpc(State(app.clone()), Bytes::from(standalone.to_string())).await).await
                ["result"],
            Value::Null
        );
        let response = feeder(
            State(app),
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
        c1_task.abort();
        c2_task.abort();
    }
}
