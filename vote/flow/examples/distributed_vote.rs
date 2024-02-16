use std::cell::RefCell;

use hydro_deploy::{Deployment, HydroflowCrate};
use hydroflow_plus_cli_integration::{DeployProcessSpec, DeployClusterSpec};

#[tokio::main]
async fn main() {
    let deployment = RefCell::new(Deployment::new());
    let localhost = deployment.borrow_mut().Localhost();

    let builder = hydroflow_plus::FlowBuilder::new();
    flow::distributed_vote::distributed_vote(
        &builder,
        &DeployProcessSpec::new(|| {
            let mut deployment = deployment.borrow_mut();
            deployment.add_service(
                HydroflowCrate::new(".", localhost.clone())
                    .bin("distributed_vote")
                    .profile("dev")
                    .display_name("leader"),
            )
        }),
        &DeployClusterSpec::new(|| {
            let mut deployment = deployment.borrow_mut();
            (0..3)
                .map(|idx| {
                    deployment.add_service(
                        HydroflowCrate::new(".", localhost.clone())
                            .bin("distributed_vote")
                            .profile("dev")
                            .display_name(format!("worker/{}", idx)),
                    )
                })
                .collect()
        }),
    );

    let mut deployment = deployment.into_inner();

    deployment.deploy().await.unwrap();

    deployment.start().await.unwrap();

    tokio::signal::ctrl_c().await.unwrap()
}