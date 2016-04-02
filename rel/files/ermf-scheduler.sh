#!/bin/bash

main() {
    echo "Running checks for proper environment:"
    echo "Checking that riak_mesos_scheduler directory exists"
    [ -d "riak_mesos_scheduler" ] || exit
    echo "Checking for riak_mesos_scheduler executable"
    [ -x "riak_mesos_scheduler/bin/riak_mesos_scheduler" ] || exit
    echo "Checking for required mesos env vars"
    echo "Checking if HOME is set..."
    if [ -z "$HOME" ]; then
        export HOME=`eval echo "~$WHOAMI"`
    fi
    echo "Setting riak_mesos_scheduler environment variables..."
    if [ -z "$PORT0" ]; then
        export PORT0=9090
    fi
    if [ -z "$RIAK_MESOS_PORT" ]; then
        export RIAK_MESOS_PORT=$PORT0
    fi
    if [ -z "$RIAK_MESOS_EXECUTOR_PKG"]; then
        export RIAK_MESOS_EXECUTOR_PKG=riak_mesos_executor.tar.gz
    fi
    if [ -z "$RIAK_MESOS_EXPLORER_PKG"]; then
        export RIAK_MESOS_EXPLORER_PKG=riak_explorer.tar.gz
    fi
    if [ -z "$RIAK_MESOS_RIAK_PKG"]; then
        export RIAK_MESOS_RIAK_PKG=riak.tar.gz
    fi

    mkdir -p artifacts
    mv riak_ts*gz artifacts/$RIAK_MESOS_RIAK_PKG  &> /dev/null
    mv riak-ts*gz artifacts/$RIAK_MESOS_RIAK_PKG  &> /dev/null
    mv riak_ee*gz artifacts/$RIAK_MESOS_RIAK_PKG  &> /dev/null
    mv riak-ee*gz artifacts/$RIAK_MESOS_RIAK_PKG  &> /dev/null
    mv riak_explorer*gz artifacts/$RIAK_MESOS_EXPLORER_PKG
    mv riak-explorer*gz artifacts/$RIAK_MESOS_EXPLORER_PKG
    mv riak_mesos_executor*gz artifacts/$RIAK_MESOS_EXECUTOR_PKG
    mv riak-mesos-executor*gz artifacts/$RIAK_MESOS_EXECUTOR_PKG
    rm riak-mesos-scheduler*gz
    rm riak_mesos_scheduler*gz
    mv riak*gz artifacts/$RIAK_MESOS_RIAK_PKG  &> /dev/null
    rm -rf root
    rm -rf riak_mesos_executor

    echo "Starting riak_mesos_scheduler..."
    riak_mesos_scheduler/bin/riak_mesos_scheduler console -noinput
}

main "$@"
