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

    mv riak-*.tar.gz riak.tar.gz  &> /dev/null
    mv riak_ts-*.tar.gz riak.tar.gz  &> /dev/null
    mv riak_ee-*.tar.gz riak.tar.gz  &> /dev/null
    mv riak_explorer-*.tar.gz riak_explorer.tar.gz
    mv riak_mesos_executor-*.tar.gz riak_mesos_executor.tar.gz
    rm -rf root
    rm -rf riak_mesos_executor

    if [ -z "$RIAK_MESOS_EXECUTOR_PKG"]; then
        export RIAK_MESOS_EXECUTOR_PKG=riak_mesos_executor.tar.gz
    fi
    if [ -z "$RIAK_MESOS_EXPLORER_PKG"]; then
        export RIAK_MESOS_EXPLORER_PKG=riak_explorer.tar.gz
    fi
    if [ -z "$RIAK_MESOS_RIAK_PKG"]; then
        export RIAK_MESOS_RIAK_PKG=riak.tar.gz
    fi

    echo "Starting riak_mesos_scheduler..."
    riak_mesos_scheduler/bin/riak_mesos_scheduler console -noinput
}

main "$@"
