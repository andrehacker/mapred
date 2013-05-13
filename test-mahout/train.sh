#!/bin/bash

set -x verbose

INPUT_FILE=../testdata/mahout-csv/donut.csv
OUTPUT_FILE=donut.model

${MAHOUT_HOME}/bin/mahout trainlogistic \
 --input ${INPUT_FILE} \
 --output ${OUTPUT_FILE} \
 --target color \
 --categories 2 \
 --predictors x y xx xy yy a b c --types n n \
 --passes 100 \
 --rate 50 --lambda 0.001 \
 --features 21