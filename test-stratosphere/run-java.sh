#!/bin/bash

SETTINGS_FILE=$1

java -cp "runner.jar:${STRATOSPHERE_HOME}/lib/*" Runner ${SETTINGS_FILE}

# print output
cat wordcount-result/* | wc -l
