#[tokio::main]
async fn main() {
    let ports = hydroflow_plus::util::cli::init().await;

    hydroflow_plus::util::cli::launch_flow(
        flow::broadcast::broadcast_runtime!(&ports)
    ).await;
}