-record(rms_cluster, {
          key :: mesos_scheduler_data:cluster_key(),
          status :: mesos_scheduler_data:cluster_status(),
          nodes = [] :: [mesos_scheduler_data:node_key()]
}).

-record(rms_node, {
          key :: mesos_scheduler_data:node_key(),
          status :: mesos_scheduler_data:node_status(),
          location :: term()
}).
