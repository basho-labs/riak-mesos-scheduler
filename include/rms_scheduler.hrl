-record(rms_scheduler, {framework_id :: undefined | erl_mesos:'FrameworkID'(),
                        cluster_keys = [] :: [rms_cluster:key()],
                        removed_cluster_keys = [] :: [rms_cluster:key()]}).
