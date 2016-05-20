# riak-mesos-scheduler
The Scheduler component of the Riak Mesos Framework.

## Usage

See the [RMF](https://github.com/basho-labs/riak-mesos-erlang) README for details on what, how and why to deploy the RMF.

### Development

To build a deployable artifact, simply run the following:

```
make tarball
```

### Testing

```
make && make test
```

#### Run the riak_mesos_scheduler application

For installing/running in a mesos cluster, see the [riak-mesos-erlang](https://github.com/basho-labs/riak-mesos-erlang) repository README.
