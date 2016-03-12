#!/bin/bash
# $1 = edges file
# $2 = vertices file
# $3 = num iterations
source env.sh
START_TIME=`date +%s`
mkdir -p /tmp$1
make all
END_COMPILE_TIME=`date +%s`
COMPILE_TIME=`expr $END_COMPILE_TIME - $START_TIME`
echo "COMPILE TIME: "$COMPILE_TIME
./DataTransformer_bin copy_input $1 $2
./pagerank_bin file /tmp$1input niters $3 filetype edgelist membudget_mb 10000 execthreads 6
./DataTransformer_bin copy_output $1 $2
END_TIME=`date +%s`
RUN_TIME=`expr $END_TIME - $START_TIME`
echo "TOTAL TIME: "$RUN_TIME
hadoop fs -rm -r $2/output
make clean