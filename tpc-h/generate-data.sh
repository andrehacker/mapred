#!/bin/bash

cd tpch_2_15.0/dbgen

# Last parameter is scale factor (GB): 1, 10, 30, 100, 300, 1000, ...
./dbgen -T o -s 10
