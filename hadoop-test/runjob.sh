#!/bin/bash

set -x verbose

HDFS_DIR=testandre
JAR_NAME=testandre.jar
CLASS_NAME=de.andrehacker.TestAndre

# Clean up
${HADOOP_PREFIX}/bin/hadoop fs -rmr $HDFS_DIR
${HADOOP_PREFIX}/bin/hadoop fs -mkdir $HDFS_DIR/input

# Copy input to hadoop
${HADOOP_PREFIX}/bin/hadoop fs -put input/* $HDFS_DIR/input

# Run task
# hadoop jar <jarfile> [Main class] args...
# For our tasks, the args are input and output directory
${HADOOP_PREFIX}/bin/hadoop jar $JAR_NAME $CLASS_NAME $HDFS_DIR/input $HDFS_DIR/output

# Show output
${HADOOP_PREFIX}/bin/hadoop fs -cat $HDFS_DIR/output/*
