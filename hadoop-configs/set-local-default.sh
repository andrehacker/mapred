#!/bin/bash

#set -x verbose

if [ "$HADOOP_PREFIX" = "" ]; then
    echo "Error: JAVA_HOME is not set."
    exit 1  # 0 = success, non-zero is failure
fi
echo "copy config files to hadoop conf dir"
cp config-local/* $HADOOP_PREFIX/conf
echo "done"
