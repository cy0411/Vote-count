use hydroflow_plus::*;
use stageleft::*;

pub fn broadcast<'a, D: Deploy<'a>>(
    flow: &'a FlowBuilder<'a, D>,
    process_spec: &impl ProcessSpec<'a, D>,
    cluster_spec: &impl ClusterSpec<'a, D>
) {
    let leader = flow.process(process_spec);
    let workers = flow.cluster(cluster_spec);
    let data = leader.source_iter(q!(0..10));
data
    .broadcast_bincode(&workers)
    .for_each(q!(|n| println!("{}", n)));
}

use hydroflow_plus::util::cli::HydroCLI;
use hydroflow_plus_cli_integration::{CLIRuntime, HydroflowPlusMeta};

#[stageleft::entry]
pub fn broadcast_runtime<'a>(
    flow: &'a FlowBuilder<'a, CLIRuntime>,
    cli: RuntimeData<&'a HydroCLI<HydroflowPlusMeta>>,
) -> impl Quoted<'a, Hydroflow<'a>> {
    broadcast(flow, &cli, &cli);
    flow.build(q!(cli.meta.subgraph_id))
}