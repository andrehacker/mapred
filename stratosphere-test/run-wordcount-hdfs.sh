#!/bin/bash

# TODO: FINISH AND TEST

DIR=`dirname "$0"`
DIR=`cd "$bin"; pwd`
cd $DIR

OUTPUTDIR=wordcount-result

HDFS_DIR=pact-test
JAR_NAME=testandre.jar
CLASS_NAME=de.andrehacker.TestAndre
#INPUT_DIR=input
INPUT_DIR=input-big  # local dir containing the input

#remove output
rm -r $OUTPUTDIR

# Clean up
${HADOOP_PREFIX}/bin/hadoop fs -rmr $HDFS_DIR
${HADOOP_PREFIX}/bin/hadoop fs -mkdir $HDFS_DIR/$INPUT_DIR

# Copy input to hadoop
${HADOOP_PREFIX}/bin/hadoop fs -put $INPUT_DIR/* $HDFS_DIR/input

# Run task
set -x verbose

$STRATOSPHERE_HOME/bin/pact-client.sh run -j $STRATOSPHERE_HOME/examples/pact/pact-examples-0.2-WordCount.jar -a 4 hdfs://${HDFS_DIR}/input/hamlet.txt file://${DIR}/${OUTPUTDIR}

# Show running tasks
$STRATOSPHERE_HOME/bin/pact-client.sh list -r -s

