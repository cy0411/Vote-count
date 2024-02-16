use hydroflow_plus::*;
use stageleft::*;

pub fn distributed_vote<'a, D: Deploy<'a>>(
    flow: &'a FlowBuilder<'a, D>,
    process_spec: &impl ProcessSpec<'a, D>,
    cluster_spec: &impl ClusterSpec<'a, D>,
) {
    let process = flow.process(process_spec);
    let cluster = flow.cluster(cluster_spec);

    let numbers = process.source_iter(q!(1..7));
    let ids = process.source_iter(cluster.ids()).map(q!(|&id| id));

    ids.cross_product(&numbers)
        .map(q!(|(id, n)| (id, (id, n))))
        .demux_bincode(&cluster)
        .inspect(q!(|n| println!("cluster received: {:?}", n)))
        .map(q!(|(id, n)| (n, n % (id + 1) == 0)))
        .send_bincode_interleaved(&process)
        .tick_batch()
        .reduce_keyed(q!(|answer: &mut bool, reply : bool| *answer = *answer && reply))
        .for_each(q!(|(value, answer)| {if answer{println!("Aggreed on {:?}. Proceed!", value)} else {println!("Disagree on {:?}. Abort!", value)}}));
}


use hydroflow_plus::util::cli::HydroCLI;
use hydroflow_plus_cli_integration::{CLIRuntime, HydroflowPlusMeta};

#[stageleft::entry]
pub fn distributed_vote_runtime<'a>(
    flow: &'a FlowBuilder<'a, CLIRuntime>,
    cli: RuntimeData<&'a HydroCLI<HydroflowPlusMeta>>,
) -> impl Quoted<'a, Hydroflow<'a>> {
    distributed_vote(flow, &cli, &cli);
    flow.build(q!(cli.meta.subgraph_id))
}