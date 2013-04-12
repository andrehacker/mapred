#!/bin/bash

#set -x verbose

# This copies also the masters and slaves files to all nodes,
# which are only required by master.
# However, only start*.sh script read those files so they will
# be ignored on slaves

if [ "$HADOOP_PREFIX" = "" ]; then
    echo "Error: HADOOP_PREFIX is not set."
    exit 1  # 0 = success, non-zero is failure
fi

echo "copy config files to hadoop conf dir"
cp config-old-cluster/* $HADOOP_PREFIX/conf

echo "done"
