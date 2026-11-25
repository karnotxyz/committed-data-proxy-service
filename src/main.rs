mod audit;
mod datasets;
mod mongo_feeder;
mod passthrough;
mod patricia;
mod request;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let audit = audit::Audit::from_env().await?;
    let command = std::env::args().nth(1);
    if command.as_deref() == Some("--index-archive") {
        eprintln!("Indexed {} archived requests", audit.index_archive().await?);
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
