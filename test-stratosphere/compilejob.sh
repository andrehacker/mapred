#!/bin/bash

set -x verbose
# add hadoop core library to classpath
mkdir -p runner_classes
javac -classpath "${STRATOSPHERE_HOME}/lib/*" -d runner_classes Runner.java

# -C changes to directory with classes, then we add all (folder ".")
# Otherwise the jar would contain testandre_classes as a folder
jar cvf runner.jar -C runner_classes/ .
