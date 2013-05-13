#!/bin/bash

set -x verbose

INPUT_FILE=../testdata/mahout-csv/donut-test.csv
MODEL_FILE=donut.model

${MAHOUT_HOME}/bin/mahout org.apache.mahout.classifier.sgd.RunLogistic \
 --input ${INPUT_FILE} \
 --model ${MODEL_FILE} \
 --auc \
 --scores \
 --confusion