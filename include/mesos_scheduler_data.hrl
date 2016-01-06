-record(rms_cluster, {
          key :: key(),
          status :: cluster_status(),
          nodes = [] :: [key()]
}).

-record(rms_node, {
          key :: key(),
          status :: node_status(),
          location :: term()
}).
