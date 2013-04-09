#!/bin/bash

#set -x verbose

if [ "$STRATOSPHERE_HOME" = "" ]; then
    echo "Error: STRATOSPHERE_HOME is not set."
    exit 1  # 0 = success, non-zero is failure
fi

echo "copy config files to stratosphere conf dir"
cp local/* $STRATOSPHERE_HOME/conf

echo "done"
