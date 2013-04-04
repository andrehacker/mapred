#!/bin/bash

#set -x verbose

if [ "$HADOOP_PREFIX" = "" ]; then
    echo "Error: HADOOP_PREFIX is not set."
    exit 1  # 0 = success, non-zero is failure
fi

DIR="config-cluster/$(hostname)"
if [ ! -d "$DIR" ]; then
    echo "Error: No config files for hostname $(hostname)"
    exit 1     # 0 = success, non-zero is failure
fi

echo "copy config files from ${DIR} to hadoop conf dir"
cp ${DIR}/* $HADOOP_PREFIX/conf

echo "done"
