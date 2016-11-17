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
    if [ -z "$RIAK_MESOS_NAME" ]; then
        export RIAK_MESOS_NAME=riak
    fi
    RUN_SCHEDULER="riak_mesos_scheduler/bin/riak_mesos_scheduler -noinput"
    if [ -n "$RIAK_MESOS_ATTACH" ]; then
        RUN_SCHEDULER="$RUN_SCHEDULER -sname '$RIAK_MESOS_NAME' -setcookie '$RIAK_MESOS_NAME'"
    fi
    echo "Starting riak_mesos_scheduler..."
    eval "$RUN_SCHEDULER"
}

main "$@"
