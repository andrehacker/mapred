#!/bin/bash

java -cp "runner.jar:${STRATOSPHERE_HOME}/lib/*" Runner

# print output
cat wordcount-result/* | wc -l
