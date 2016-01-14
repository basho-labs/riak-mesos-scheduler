-record(rms_cluster, {
          key :: mesos_scheduler_data:cluster_key(),

          riak_conf = "" :: string(),
          advanced_config = "" :: string(),

          status = requested :: mesos_scheduler_data:cluster_status(),
          nodes = [] :: [mesos_scheduler_data:node_key()]
}).

-record(rms_node, {
          key :: mesos_scheduler_data:node_key(),

          status :: mesos_scheduler_data:node_status(),

          cluster :: mesos_scheduler_data:cluster_key(),

          node_name :: node(),
          hostname :: string(),
          http_port :: integer(),
          pb_port :: integer(),
          disterl_port :: integer(),
          slave_id :: string(),
          container_path :: string(),
          persistence_id :: string()
}).
