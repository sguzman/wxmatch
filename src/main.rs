#[tokio::main]
async fn main() -> anyhow::Result<()> {
    wxmatch::run().await
}
