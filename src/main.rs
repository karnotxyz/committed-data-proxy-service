mod passthrough;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    passthrough::run().await
}
