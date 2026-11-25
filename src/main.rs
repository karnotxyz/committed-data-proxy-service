mod audit;
mod datasets;
mod invoke_hash;
mod mongo_feeder;
mod passthrough;
mod patricia;
mod request;
mod retry;
mod signed_updates;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let audit = audit::Audit::from_env().await?;
    let command = std::env::args().nth(1);
    if command.as_deref() == Some("--index-archive") {
        let chain_id = std::env::var("CHAIN_ID")
            .ok()
            .map(|value| starknet_types_core::felt::Felt::from_hex(&value))
            .transpose()?;
        eprintln!(
            "Indexed {} archived requests",
            audit.index_archive(chain_id).await?
        );
        return Ok(());
    }
    let store = datasets::DatasetStore::from_env(audit.database())?;
    if command.as_deref() == Some("--materialize-archive") {
        let store = store.ok_or("--materialize-archive requires MONGO_FEEDER_ENABLED=true")?;
        eprintln!(
            "Materialized {} archived datasets",
            audit.materialize_archive(&store).await?
        );
        return Ok(());
    }
    passthrough::run(audit, store).await
}
