#!/bin/bash

# TODO: FINISH AND TEST

DIR=`dirname "$0"`
DIR=`cd "$bin"; pwd`
cd $DIR

INPUT_DIR=../testdata/books
#HDFS_NAMENODE=desktop:9000
HDFS_NAMENODE=localhost:9000 # if running dfs and stratosphere in local(host) mode
HDFS_DIR=pact-test

# Run task
set -x verbose

# Clean up
${HADOOP_PREFIX}/bin/hadoop fs -rmr $HDFS_DIR

# Copy input to hadoop
${HADOOP_PREFIX}/bin/hadoop fs -mkdir $HDFS_DIR/input
${HADOOP_PREFIX}/bin/hadoop fs -put $INPUT_DIR/* $HDFS_DIR/input

$STRATOSPHERE_HOME/bin/pact-client.sh run -w -j $STRATOSPHERE_HOME/examples/pact/pact-examples-0.2-WordCount.jar -a 4 hdfs://${HDFS_NAMENODE}/user/andre/${HDFS_DIR}/input hdfs://${HDFS_NAMENODE}/user/andre/${HDFS_DIR}/output

# Show running tasks
# $STRATOSPHERE_HOME/bin/pact-client.sh list -r -s

# Show output
${HADOOP_PREFIX}/bin/hadoop fs -cat $HDFS_DIR/output/* | wc -l
