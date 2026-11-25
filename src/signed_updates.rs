use crate::{
    audit::Audit,
    request::{canonical_felt, RawDataset},
};
use axum::{
    extract::Query,
    http::{header, HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use mongodb::bson::{doc, Document};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use starknet_types_core::felt::Felt;
use std::{env, sync::Arc};
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;

pub const PATH: &str = "/feeder_gateway/get_signed_committed_data_update";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Params {
    root: String,
    transaction_hash: Option<String>,
    application_id: Option<String>,
    dataset_id: Option<String>,
}

enum Selector {
    Transaction(String),
    Version(String),
}

pub struct Lookup {
    root: String,
    selector: Selector,
}

impl Lookup {
    fn parse(uri: &Uri) -> Option<Self> {
        let Query(params) = Query::<Params>::try_from_uri(uri).ok()?;
        let felt = |value| canonical_felt(&Value::String(value));
        let root = felt(params.root)?;
        let selector = match (
            params.transaction_hash,
            params.application_id,
            params.dataset_id,
        ) {
            (Some(hash), None, None) => Selector::Transaction(felt(hash)?),
            (None, Some(app), Some(dataset)) => {
                Selector::Version(format!("{}:{}:{root}", felt(app)?, felt(dataset)?))
            }
            _ => return None,
        };
        Some(Self { root, selector })
    }

    pub fn mongo_filter(&self) -> (Document, i32) {
        match &self.selector {
            Selector::Transaction(hash) => (
                doc! {"c1_transaction_hash":hash,"dataset_roots":&self.root},
                -1,
            ),
            Selector::Version(key) => (doc! {"dataset_versions":key}, 1),
        }
    }

    pub fn matches(&self, dataset: &RawDataset<'_>) -> bool {
        match &self.selector {
            Selector::Transaction(_) => canonical_felt(dataset.root).as_ref() == Some(&self.root),
            Selector::Version(key) => version_key(dataset).as_ref() == Some(key),
        }
    }
}

pub fn version_key(dataset: &RawDataset<'_>) -> Option<String> {
    let update = dataset.update?;
    let root = canonical_felt(dataset.root)?;
    // A no-op patch cannot provide the base needed to create this version.
    if canonical_felt(&update["base_root"]).as_ref() == Some(&root) {
        return None;
    }
    let app = canonical_felt(&update["application_id"])?;
    let dataset = canonical_felt(&update["dataset_id"])?;
    Some(format!("{app}:{dataset}:{root}"))
}

pub struct SignedUpdates {
    pub chain_id: Felt,
    token_digest: [u8; 32],
    limit: Arc<Semaphore>,
}

impl SignedUpdates {
    pub fn from_env() -> Result<Option<Self>, Box<dyn std::error::Error>> {
        let Ok(token) = env::var("COMMITTED_DATA_PROXY_TOKEN") else {
            return Ok(None);
        };
        if token.len() < 32 || !token.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err("COMMITTED_DATA_PROXY_TOKEN must contain at least 32 non-whitespace ASCII characters".into());
        }
        Ok(Some(Self {
            chain_id: Felt::from_hex(&env::var("CHAIN_ID")?)?,
            token_digest: Sha256::digest(token).into(),
            limit: Arc::new(Semaphore::new(2)),
        }))
    }

    pub async fn serve(&self, audit: Audit, uri: Uri, headers: HeaderMap) -> Response {
        let authorized = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .is_some_and(|token| {
                bool::from(self.token_digest.ct_eq(Sha256::digest(token).as_slice()))
            });
        if !authorized {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        let Some(lookup) = Lookup::parse(&uri) else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        let Ok(permit) = self.limit.clone().try_acquire_owned() else {
            return StatusCode::TOO_MANY_REQUESTS.into_response();
        };
        // Keep the permit until the lookup and JSON serialization end, even on disconnect.
        let task = tokio::spawn(async move {
            let _permit = permit;
            audit.signed_dataset(lookup).await
        });
        match task.await {
            Ok(Ok(Some(bytes))) => (
                [
                    (header::CONTENT_TYPE, "application/json"),
                    (header::CACHE_CONTROL, "no-store"),
                ],
                bytes,
            )
                .into_response(),
            Ok(Ok(None)) => StatusCode::NOT_FOUND.into_response(),
            Ok(Err(error)) => {
                eprintln!("Signed committed-data lookup failed: {error}");
                StatusCode::SERVICE_UNAVAILABLE.into_response()
            }
            _ => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        }
    }
}
