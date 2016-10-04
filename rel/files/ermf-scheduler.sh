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
    if [ -z "$RIAK_MESOS_SCHEDULER_PKG" ]; then
        export RIAK_MESOS_SCHEDULER_PKG=riak_mesos_scheduler.tar.gz
    fi
    if [ -z "$RIAK_MESOS_EXECUTOR_PKG" ]; then
        export RIAK_MESOS_EXECUTOR_PKG=riak_mesos_executor.tar.gz
    fi
    if [ -z "$RIAK_MESOS_EXPLORER_PKG" ]; then
        export RIAK_MESOS_EXPLORER_PKG=riak_explorer.tar.gz
    fi
    if [ -z "$RIAK_MESOS_RIAK_PKG" ]; then
        export RIAK_MESOS_RIAK_PKG=riak.tar.gz
    fi
    if [ -z "$RIAK_MESOS_PATCHES_PKG" ]; then
        export RIAK_MESOS_PATCHES_PKG=riak_erlpmd_patches.tar.gz
    fi

    if [ -z "$RIAK_MESOS_NAME" ]; then
        export RIAK_MESOS_NAME=riak
    fi

    mkdir -p artifacts
    mv $RIAK_MESOS_RIAK_PKG artifacts/$RIAK_MESOS_RIAK_PKG &> /dev/null
    mv $RIAK_MESOS_EXPLORER_PKG artifacts/$RIAK_MESOS_EXPLORER_PKG &> /dev/null
    mv $RIAK_MESOS_EXECUTOR_PKG artifacts/$RIAK_MESOS_EXECUTOR_PKG &> /dev/null
    mv $RIAK_MESOS_PATCHES_PKG artifacts/$RIAK_MESOS_PATCHES_PKG &> /dev/null
    rm $RIAK_MESOS_SCHEDULER_PKG
    rm -rf root
    rm -rf riak_mesos_executor

    RUN_SCHEDULER="riak_mesos_scheduler/bin/riak_mesos_scheduler -noinput"

    if [ -n "$RIAK_MESOS_ATTACH" ]; then
        RUN_SCHEDULER="$RUN_SCHEDULER -sname '$RIAK_MESOS_NAME' -setcookie '$RIAK_MESOS_NAME'"
    fi

    echo "Starting riak_mesos_scheduler..."
    eval "$RUN_SCHEDULER"
}

main "$@"
