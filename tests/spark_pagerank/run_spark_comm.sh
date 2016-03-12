#!/bin/bash
# $1 = graph_edges_dir
# $2 = com_edges_dir
# $3 = vertices_dir
# $4 = output_dir
# $5 = parallelism
hadoop fs -rm -r $4
cp comm_bs_pagerank.saved src/main/scala/pagerank.scala
sh clean_dir.sh
$SPARK_HOME/sbt/sbt clean
START_TIME=`date +%s`
$SPARK_HOME/sbt/sbt package
END_COMPILE_TIME=`date +%s`
COMPILE_TIME=`expr $END_COMPILE_TIME - $START_TIME`
echo "COMPILE TIME: "$COMPILE_TIME
$SPARK_HOME/sbt/sbt package "run-main PageRank $1 $3 $2 $4 $5"
END_TIME=`date +%s`
RUN_TIME=`expr $END_TIME - $START_TIME`
echo "TOTAL TIME: "$RUN_TIME
hadoop fs -rm -r $4