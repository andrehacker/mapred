#!/bin/bash

set -x verbose
# hadoop jar <jarfile> [Main class] args...
# For our tasks, the args are input and output directory
${HADOOP_PREFIX}/bin/hadoop jar /home/andre/dev/mapred/testandre/testandre.jar de.andrehacker.TestAndre testandre/input testandre/output
