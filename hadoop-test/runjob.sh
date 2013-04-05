#!/bin/bash

set -x verbose

HDFS_DIR=testandre
JAR_NAME=testandre.jar
CLASS_NAME=de.andrehacker.TestAndre
#INPUT_DIR=input
INPUT_DIR=input-big  # local dir containing the input

NUM_MAPS=4      # This is just a hint, real value computed based on InputFormat. So we ignore it.
NUM_REDUCERS=8  # This is the real value

# Clean up
${HADOOP_PREFIX}/bin/hadoop fs -rmr $HDFS_DIR
${HADOOP_PREFIX}/bin/hadoop fs -mkdir $HDFS_DIR/$INPUT_DIR

# Copy input to hadoop
${HADOOP_PREFIX}/bin/hadoop fs -put $INPUT_DIR/* $HDFS_DIR/input

# Run task
# hadoop jar <jarfile> [Main class] args...
# For our tasks, the args are input and output directory
${HADOOP_PREFIX}/bin/hadoop jar $JAR_NAME $CLASS_NAME -D mapred.reduce.tasks=$NUM_REDUCERS $HDFS_DIR/input $HDFS_DIR/output

# Show output
${HADOOP_PREFIX}/bin/hadoop fs -cat $HDFS_DIR/output/*
