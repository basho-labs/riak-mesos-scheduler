-record(rms_cluster, {key :: rms_cluster:key(),
                      status = requested :: rms_cluster:status(),
                      riak_conf = "" :: string(),
                      advanced_config = "" :: string(),
                      nodes = [] :: [rms_node:key()],
                      generation = 1 :: pos_integer()}).
