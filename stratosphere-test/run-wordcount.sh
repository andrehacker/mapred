#!/bin/bash

# $0 returns relative path and filename of the script we are in.
# Relative means relative to the working directory (pwd)
DIR=`dirname "$0"`
DIR=`cd "$bin"; pwd`

OUTPUTDIR=wordcount-result

#remove output
rm -r $OUTPUTDIR

set -x verbose

# Run Job (submit only)
# pact-client.sh [ACTION] [GENERAL_OPTIONS] [ACTION_ARGUMENTS]
# action=run
# -a = pact program arguments
$STRATOSPHERE_HOME/bin/pact-client.sh run -j $STRATOSPHERE_HOME/examples/pact/pact-examples-0.2-WordCount.jar -a 4 file://${DIR}/input/hamlet.txt file://${DIR}/${OUTPUTDIR}

# List jobs:
$STRATOSPHERE_HOME/bin/pact-client.sh list -r -s
