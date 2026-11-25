use serde::Serialize;
use serde_json::Value;

pub const COMBINED: &str = "madara_V0_1_0_importCommittedDataAndSubmitTransaction";
pub const IMPORT: &str = "madara_V0_1_0_importCommittedDataSet";
pub const UPDATE: &str = "madara_V0_1_0_updateCommittedDataSet";

pub struct ArchivedRequest<'a> {
    pub roots: Vec<String>,
    pub transaction: Option<&'a Value>,
    pub datasets: Vec<RawDataset<'a>>,
}

#[derive(Clone, Copy, Serialize)]
pub struct RawDataset<'a> {
    pub root: &'a Value,
    pub entries: &'a Value,
    pub signatures: &'a Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update: Option<&'a Value>,
}

fn params<'a, const N: usize>(value: &'a Value, fields: [&str; N]) -> Option<[&'a Value; N]> {
    match value {
        Value::Array(values) if (N - 1..=N).contains(&values.len()) => {
            Some(std::array::from_fn(|index| {
                values.get(index).unwrap_or(&Value::Null)
            }))
        }
        Value::Object(values) if values.keys().all(|key| fields.contains(&key.as_str())) => {
            Some(fields.map(|field| values.get(field).unwrap_or(&Value::Null)))
        }
        _ => None,
    }
}

pub fn canonical_felt(value: &Value) -> Option<String> {
    const MODULUS: &str = "800000000000011000000000000000000000000000000000000000000000001";
    let digits = value.as_str()?.strip_prefix("0x")?;
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    let normalized = digits.trim_start_matches('0').to_ascii_lowercase();
    if normalized.len() > MODULUS.len()
        || (normalized.len() == MODULUS.len() && normalized.as_str() >= MODULUS)
    {
        return None;
    }
    Some(format!(
        "0x{}",
        if normalized.is_empty() {
            "0"
        } else {
            &normalized
        }
    ))
}

fn dataset_root(root: &Value, entries: &Value, signatures: &Value) -> Result<String, &'static str> {
    if !entries.is_array() || (!signatures.is_null() && !signatures.is_array()) {
        return Err("Expected dataset entries and optional signatures");
    }
    canonical_felt(root).ok_or("Invalid dataset root")
}

fn validate_update(update: &Value) -> Result<(), &'static str> {
    if canonical_felt(&update["application_id"]).is_none()
        || canonical_felt(&update["dataset_id"]).is_none()
        || update.get("base_root").is_none()
        || (!update["base_root"].is_null() && canonical_felt(&update["base_root"]).is_none())
    {
        return Err("Invalid dataset update metadata");
    }
    Ok(())
}

pub fn parse(call: &Value) -> Result<ArchivedRequest<'_>, &'static str> {
    let input = &call["params"];
    let (datasets, transaction) = match call["method"].as_str() {
        Some(COMBINED) => {
            let [datasets, transaction] =
                params(input, ["datasets", "transaction"]).ok_or("Invalid params")?;
            let datasets = datasets
                .as_array()
                .filter(|datasets| (1..=4).contains(&datasets.len()))
                .ok_or("Expected one to four datasets")?;
            if transaction["type"] != "INVOKE" || transaction["is_query"] == true {
                return Err("Expected signed non-query INVOKE");
            }
            if serde_json::to_vec(transaction).is_ok_and(|bytes| bytes.len() > 1024 * 1024) {
                return Err("Transaction too large");
            }
            let datasets = datasets
                .iter()
                .map(|dataset| RawDataset {
                    root: &dataset["root"],
                    entries: &dataset["entries"],
                    signatures: &dataset["signatures"],
                    update: dataset.get("update").filter(|value| !value.is_null()),
                })
                .collect();
            (datasets, Some(transaction))
        }
        Some(IMPORT) => {
            let [root, entries, signatures] =
                params(input, ["root", "entries", "signatures"]).ok_or("Invalid params")?;
            (
                vec![RawDataset {
                    root,
                    entries,
                    signatures,
                    update: None,
                }],
                None,
            )
        }
        Some(UPDATE) => {
            let [update, root, entries, signatures] =
                params(input, ["update", "root", "entries", "signatures"])
                    .ok_or("Invalid params")?;
            (
                vec![RawDataset {
                    root,
                    entries,
                    signatures,
                    update: Some(update),
                }],
                None,
            )
        }
        _ => return Err("Method not found"),
    };
    let roots = datasets
        .iter()
        .map(|dataset| {
            if let Some(update) = dataset.update {
                validate_update(update)?;
            }
            dataset_root(dataset.root, dataset.entries, dataset.signatures)
        })
        .collect::<Result<_, _>>()?;
    Ok(ArchivedRequest {
        roots,
        transaction,
        datasets,
    })
}
