#[tokio::main]
async fn main() -> anyhow::Result<()> {
    sporos::cli::run().await?;
    Ok(())
}
