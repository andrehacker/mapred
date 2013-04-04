#!/bin/bash

#set -x verbose

if [ "$HADOOP_PREFIX" = "" ]; then
    echo "Error: HADOOP_PREFIX is not set."
    exit 1  # 0 = success, non-zero is failure
fi
echo "copy config files to hadoop conf dir"
cp config-localhost/* $HADOOP_PREFIX/conf
echo "done"
