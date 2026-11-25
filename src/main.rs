mod audit;
mod passthrough;
mod request;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let audit = audit::Audit::from_env().await?;
    if std::env::args().nth(1).as_deref() == Some("--index-archive") {
        eprintln!("Indexed {} archived requests", audit.index_archive().await?);
        return Ok(());
    }
    passthrough::run(audit).await
}
