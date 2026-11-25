//! Archive committed-data ingress and relay C1 submissions and C2 feeder reads.
use crate::{
    audit::{Audit, MAX_REQUEST_BYTES},
    request::{self, COMBINED, IMPORT, UPDATE},
};
use axum::{
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, Method, StatusCode, Uri},
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

fn rpc_error(id: Value, code: i32, message: &str) -> Json<Value> {
    Json(json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}}))
}

fn rpc_result(id: Value, result: Value) -> Json<Value> {
    Json(json!({"jsonrpc":"2.0","id":id,"result":result}))
}

struct App {
    audit: Audit,
    client: reqwest::Client,
    c1_url: Option<String>,
    c2_feeder_url: String,
    forward_transactions: bool,
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

async fn rpc(State(app): State<Arc<App>>, body: Bytes) -> Response {
    let received_at = SystemTime::now();
    let call_id = ObjectId::new();
    let request_id = call_id.to_hex();
    let call: Value = match serde_json::from_slice(&body) {
        Ok(call) => call,
        Err(_) => return rpc_error(Value::Null, -32700, "Parse error").into_response(),
    };
    let id = call.get("id").cloned().unwrap_or(Value::Null);
    let recognized = matches!(call["method"].as_str(), Some(COMBINED | IMPORT | UPDATE));
    let version_ok = call["jsonrpc"] == "2.0";
    let parsed = if version_ok && recognized {
        request::parse(&call)
    } else {
        Err("Invalid request")
    };
    if let Some(method @ (COMBINED | IMPORT | UPDATE)) = call["method"].as_str() {
        let roots = parsed
            .as_ref()
            .map(|request| request.roots.as_slice())
            .unwrap_or_default();
        eprintln!(
            "request={request_id} received method={method} bytes={}",
            body.len()
        );
        if let Err(error) = app
            .audit
            .record(call_id, received_at, method, &id, &body, roots)
            .await
        {
            eprintln!("request={request_id} MongoDB audit write failed: {error}");
            return rpc_error(id, -32007, "Audit log unavailable").into_response();
        }
        eprintln!(
            "request={request_id} MongoDB audit saved method={method} roots={}",
            roots.len()
        );
    }
    if !version_ok {
        return rpc_error(id, -32600, "Invalid Request").into_response();
    }
    if !recognized {
        return rpc_error(id, -32601, "Method not found").into_response();
    }
    let request = match parsed {
        Ok(request) => request,
        Err(message) => return rpc_error(id, -32602, message).into_response(),
    };
    let Some(transaction) = request.transaction else {
        return rpc_result(id, Value::Null).into_response();
    };
    if !app.forward_transactions {
        return rpc_error(id, -32003, "Transaction forwarding is disabled").into_response();
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

async fn write_ready(State(app): State<Arc<App>>) -> (StatusCode, &'static str) {
    match tokio::time::timeout(Duration::from_millis(800), app.audit.ready()).await {
        Ok(true) => (StatusCode::OK, "OK"),
        _ => (StatusCode::SERVICE_UNAVAILABLE, "MongoDB unavailable"),
    }
}

async fn feeder_ready(State(app): State<Arc<App>>) -> (StatusCode, &'static str) {
    let check = app
        .client
        .get(format!(
            "{}/health",
            app.c2_feeder_url.trim_end_matches('/')
        ))
        .send();
    match tokio::time::timeout(Duration::from_millis(800), check).await {
        Ok(Ok(response)) if response.status().is_success() => (StatusCode::OK, "OK"),
        _ => (StatusCode::SERVICE_UNAVAILABLE, "C2 feeder unavailable"),
    }
}

pub async fn run(audit: Audit) -> Result<(), Box<dyn std::error::Error>> {
    let app = Arc::new(App {
        audit,
        client: reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .build()?,
        c1_url: env::var("C1_RPC_URL").ok(),
        c2_feeder_url: env::var("C2_FEEDER_URL")?,
        forward_transactions: env::var("FORWARD_TRANSACTIONS").as_deref() == Ok("true"),
    });
    if app.forward_transactions && app.c1_url.is_none() {
        return Err("C1_RPC_URL required when forwarding transactions".into());
    }
    let rpc = Router::new()
        .route("/", post(rpc))
        .route("/health", get(|| async { "OK" }))
        .route("/ready", get(write_ready))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .with_state(app.clone());
    let feeder = Router::new()
        .route("/health", get(|| async { "OK" }))
        .route("/ready", get(feeder_ready))
        .fallback(any(feeder))
        .layer(DefaultBodyLimit::disable())
        .with_state(app.clone());
    let rpc_listener =
        tokio::net::TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], 9945))).await?;
    let feeder_listener =
        tokio::net::TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], 8080))).await?;
    eprintln!(
        "committed-data proxy listening on 9945 and 8080; MongoDB archive, C1 forwarding={}",
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
    use axum::body::to_bytes;

    async fn json_body(response: Response) -> Value {
        serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap()).unwrap()
    }

    #[tokio::test]
    async fn upstream_relay_retries_transient_http_errors() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let transient = Arc::new(AtomicBool::new(true));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let upstream = Router::new().fallback(any({
            let transient = transient.clone();
            move |method: Method, uri: Uri, body: Bytes| {
                let transient = transient.clone();
                async move {
                    if transient.swap(false, Ordering::SeqCst) {
                        (
                            StatusCode::SERVICE_UNAVAILABLE,
                            Json(json!({"error":"retry"})),
                        )
                    } else {
                        (
                            StatusCode::ACCEPTED,
                            Json(json!({
                                "method":method.as_str(),
                                "uri":uri.to_string(),
                                "body":String::from_utf8_lossy(&body),
                            })),
                        )
                    }
                }
            }
        }));
        let task = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
        let uri = "/feeder_gateway/get_committed_data_witnesses?root=0x1";
        let body = r#"{"keys":["0x2"]}"#;
        let result = send_with_retry(
            reqwest::Client::new()
                .post(format!("{url}{uri}"))
                .body(body),
        )
        .await
        .unwrap();
        let response = relay_response(result).unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(
            json_body(response).await,
            json!({"method":"POST","uri":uri,"body":body})
        );
        assert_eq!(RETRY_DELAYS.map(|delay| delay.as_secs()), [1, 2, 4, 8, 16]);
        task.abort();
    }
}
