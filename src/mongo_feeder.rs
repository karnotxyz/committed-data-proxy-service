use crate::{
    datasets::{DatasetStore, MAX_WITNESS_KEYS, VALUES_PER_PAGE},
    patricia::{valid_key, MAX_COMMITTED_DATA_VALUES},
};
use axum::{
    body::Bytes,
    extract::Query,
    http::{Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use starknet_types_core::felt::Felt;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Semaphore;

pub const MAX_BODY_BYTES: usize = 75 * 1024;
type Error = (StatusCode, Json<Value>);

fn error(status: StatusCode, code: &str, message: &str) -> Error {
    (status, Json(json!({"code":code, "message":message})))
}

fn malformed(message: &str) -> Error {
    error(StatusCode::BAD_REQUEST, "MALFORMED_REQUEST", message)
}

fn unavailable(cause: String) -> Error {
    eprintln!("MongoDB feeder request failed: {cause}");
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "INTERNAL_SERVER_ERROR",
        "Dataset unavailable",
    )
}

pub async fn serve(
    store: Arc<DatasetStore>,
    limit: Arc<Semaphore>,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> Response {
    let permit = match limit.try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return error(
                StatusCode::TOO_MANY_REQUESTS,
                "RATE_LIMITED",
                "Committed-data reads are busy",
            )
            .into_response()
        }
    };
    // Keep the permit until the request's MongoDB and proof work ends, including disconnects.
    match tokio::spawn(async move {
        let _permit = permit;
        read(&store, method, uri, body).await
    })
    .await
    {
        Ok(result) => result.map(Json).into_response(),
        Err(cause) => unavailable(cause.to_string()).into_response(),
    }
}

async fn read(store: &DatasetStore, method: Method, uri: Uri, body: Bytes) -> Result<Value, Error> {
    let Query(query) = Query::<HashMap<String, String>>::try_from_uri(&uri)
        .map_err(|_| malformed("Invalid query"))?;
    let parse =
        |value: &str| Felt::from_hex(value).map_err(|_| malformed("Invalid felt parameter"));
    let field = |name| {
        query
            .get(name)
            .ok_or_else(|| malformed("Missing parameter"))
            .and_then(|value| parse(value))
    };
    let data = match (uri.path(), method) {
        ("/feeder_gateway/get_committed_data_roots", Method::GET) => {
            let after = query.get("after").map(|value| parse(value)).transpose()?;
            json!(store.roots(after).await.map_err(unavailable)?)
        }
        ("/feeder_gateway/get_committed_data_versions", Method::GET) => {
            let names = ["application_id", "dataset_id", "root"];
            let after = if names.iter().any(|name| query.contains_key(*name)) {
                Some((field(names[0])?, field(names[1])?, field(names[2])?))
            } else {
                None
            };
            json!(store.versions(after).await.map_err(unavailable)?)
        }
        ("/feeder_gateway/get_committed_data", Method::GET) => {
            let root = field("root")?;
            let start = query
                .get("start")
                .map(|value| value.parse::<usize>())
                .transpose()
                .map_err(|_| malformed("Invalid dataset page offset"))?
                .unwrap_or(0);
            if !start.is_multiple_of(VALUES_PER_PAGE) || start >= MAX_COMMITTED_DATA_VALUES {
                return Err(malformed("Invalid dataset page offset"));
            }
            store.page(root, start).await.map_err(unavailable)?
        }
        ("/feeder_gateway/get_committed_data_witness", Method::GET) => {
            let root = field("root")?;
            let key = field("key")?;
            if !valid_key(key) {
                return Err(malformed("Invalid witness key"));
            }
            json!(store
                .witnesses(root, vec![key])
                .await
                .map_err(unavailable)?
                .and_then(|mut proofs| proofs.pop()))
        }
        ("/feeder_gateway/get_committed_data_witnesses", method @ (Method::GET | Method::POST)) => {
            #[derive(Deserialize)]
            struct WitnessQuery {
                root: Felt,
                keys: Vec<Felt>,
            }
            let WitnessQuery { root, keys } = if method == Method::POST {
                serde_json::from_slice(&body)
                    .map_err(|_| malformed("Invalid witness request body"))?
            } else {
                let keys = query
                    .get("keys")
                    .ok_or_else(|| malformed("Missing witness keys"))?;
                let keys = keys
                    .split(',')
                    .take(MAX_WITNESS_KEYS + 1)
                    .map(parse)
                    .collect::<Result<_, _>>()?;
                WitnessQuery {
                    root: field("root")?,
                    keys,
                }
            };
            if keys.is_empty()
                || keys.len() > MAX_WITNESS_KEYS
                || keys.iter().any(|key| !valid_key(*key))
            {
                return Err(malformed("Invalid witness keys"));
            }
            json!(store
                .witnesses(root, keys)
                .await
                .map_err(unavailable)?
                .ok_or_else(|| unavailable("Missing dataset".into()))?)
        }
        _ => {
            return Err(error(
                StatusCode::NOT_FOUND,
                "NOT_FOUND",
                "Unknown feeder route",
            ))
        }
    };
    Ok(data)
}
