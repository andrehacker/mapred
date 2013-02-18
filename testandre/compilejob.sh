#!/bin/bash

set -x verbose
# add hadoop core library to classpath
mkdir -p testandre_classes
javac -classpath ${HADOOP_PREFIX}/hadoop-core-1.0.4.jar -d testandre_classes TestAndre.java

# -C changes to directory with classes, then we add all (folder ".")
# Otherwise the jar would contain testandre_classes as a folder
jar cvf testandre.jar -C testandre_classes/ .
