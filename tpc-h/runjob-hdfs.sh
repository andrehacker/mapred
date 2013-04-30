#!/bin/bash

# 8-10 min to load data to hdfs on desktop 

DIR=`dirname "$0"`
DIR=`cd "$bin"; pwd`
cd $DIR

INPUT_DIR=~/dev/tpc-h/
SCALE_FACTOR=1
SKIP_HDFS_COPY=1
#HDFS_NAMENODE=master:9000
HDFS_NAMENODE=desktop:9000 # if running dfs and stratosphere in local(host) mode
HDFS_PREFIX=/user/andre
HDFS_DIR=tpch-test
JAR_PATH=${STRATOSPHERE_HOME}/examples/pact/pact-examples-0.2-TPCHQuery3.jar


# Run task
set -x verbose

if [SKIP_HDFS_COPY -eq 1]; then
# Clean up
    ${HADOOP_PREFIX}/bin/hadoop fs -rmr $HDFS_DIR

# Copy input to hadoop
    ${HADOOP_PREFIX}/bin/hadoop fs -mkdir $HDFS_DIR/input
    ${HADOOP_PREFIX}/bin/hadoop fs -put $INPUT_DIR/*-${SCALE_FACTOR}.tbl $HDFS_DIR/input
fi;

ARG1=4    # noSubTasks
ARG2=hdfs://${HDFS_NAMENODE}${HDFS_PREFIX}/${HDFS_DIR}/input/orders-${SCALE_FACTOR}.tbl
ARG3=hdfs://${HDFS_NAMENODE}${HDFS_PREFIX}/${HDFS_DIR}/input/lineitem-${SCALE_FACTOR}.tbl
ARG4=hdfs://${HDFS_NAMENODE}${HDFS_PREFIX}/${HDFS_DIR}/output
$STRATOSPHERE_HOME/bin/pact-client.sh run -w -j $JAR_PATH -a $ARG1 $ARG2 $ARG3 $ARG4

# Show running tasks
# $STRATOSPHERE_HOME/bin/pact-client.sh list -r -s

# Show output
${HADOOP_PREFIX}/bin/hadoop fs -cat $HDFS_DIR/output/* | wc -l
