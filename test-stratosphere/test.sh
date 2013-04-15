#!/bin/bash

echo $0
exit

set -x verbose

DIR=`dirname "$0"`
DIR=`cd "$bin"; pwd`

echo $DIR
pwd
cd $DIR
pwd
