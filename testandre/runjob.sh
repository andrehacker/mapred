#!/bin/bash

set -x verbose
# add hadoop core library to classpath
${HADOOP_PREFIX}/bin/hadoop jar /home/andre/dev/mapred/testandre/testandre.jar de.andrehacker.TestAndre testandre/input testandre/output
