#!/bin/bash

DIR=`dirname "$0"`
DIR=`cd "$bin"; pwd`
cd $DIR

INPUT_DIR=tpch_2_15.0/dbgen/
#HDFS_NAMENODE=master:9000
HDFS_NAMENODE=localhost:9000 # if running dfs and stratosphere in local(host) mode
HDFS_PREFIX=/user/andre
HDFS_DIR=tpch-test
JAR_PATH=${STRATOSPHERE_HOME}/examples/pact/pact-examples-0.2-TPCHQuery3.jar

# Run task
set -x verbose

# Clean up
${HADOOP_PREFIX}/bin/hadoop fs -rmr $HDFS_DIR

# Copy input to hadoop
${HADOOP_PREFIX}/bin/hadoop fs -mkdir $HDFS_DIR/input
${HADOOP_PREFIX}/bin/hadoop fs -put $INPUT_DIR/*.tbl $HDFS_DIR/input

ARG1=4    # noSubTasks
ARG2=hdfs://${HDFS_NAMENODE}${HDFS_PREFIX}/${HDFS_DIR}/input/orders.tbl
ARG3=hdfs://${HDFS_NAMENODE}${HDFS_PREFIX}/${HDFS_DIR}/input/lineitem.tbl
ARG4=hdfs://${HDFS_NAMENODE}${HDFS_PREFIX}/${HDFS_DIR}/output
$STRATOSPHERE_HOME/bin/pact-client.sh run -w -j $JAR_PATH -a $ARG1 $ARG2 $ARG3 $ARG4

# Show running tasks
# $STRATOSPHERE_HOME/bin/pact-client.sh list -r -s

# Show output
${HADOOP_PREFIX}/bin/hadoop fs -cat $HDFS_DIR/output/* | wc -l
