OA#!/bin/bash

set -x verbose

# Clean up
${HADOOP_PREFIX}/bin/hadoop fs -rmr testandre
${HADOOP_PREFIX}/bin/hadoop fs -mkdir testandre/input

# Copy input to hadoop
${HADOOP_PREFIX}/bin/hadoop fs -put input/* testandre/input

# hadoop jar <jarfile> [Main class] args...
# For our tasks, the args are input and output directory
${HADOOP_PREFIX}/bin/hadoop jar testandre.jar de.andrehacker.TestAndre testandre/input testandre/output

# Show output
${HADOOP_PREFIX}/bin/hadoop fs -cat testandre/output/*
