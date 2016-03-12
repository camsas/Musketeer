#!/bin/bash
# $1 = edges file
# $2 = vertices file
# $3 = num iterations
# $4 = root dir
# $5 = musketeer dir
source env.sh
START_TIME=`date +%s`
make all
javac -classpath $5/ext/hadoop-core.jar:$5/ext/hadoop-common.jar:$5/ext/hadoop-hdfs.jar Intersection.java
jar cvf Intersection.jar Intersection*.class
END_COMPILE_TIME=`date +%s`
COMPILE_TIME=`expr $END_COMPILE_TIME - $START_TIME`
echo "COMPILE TIME: "$COMPILE_TIME
hadoop jar Intersection.jar Intersection $4/lj_edges/ $4/com_edges/ $4/int_edges/
mkdir -p /tmp$1
./DataTransformer_bin copy_input $1 $2
./pagerank_bin file /tmp$1input niters $3 filetype edgelist membudget_mb 13000 execthreads 4
./DataTransformer_bin copy_output $1 $2
END_TIME=`date +%s`
RUN_TIME=`expr $END_TIME - $START_TIME`
echo "TOTAL TIME: "$RUN_TIME
hadoop fs -rm $2output
hadoop fs -rm -r $4/int_edges/
make clean