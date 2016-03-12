#!/bin/bash
# $1 = root_dir
# $2 = graph_edges_dir
# $3 = com_edges_dir
# $4 = vertices_dir
# $5 = output_dir
# $6 = parallelism
# $7 = musketeer_dir
hadoop fs -rm -r $1/int_edges/
hadoop fs -rm -r $5
sh clean_dir.sh
$SPARK_HOME/sbt/sbt clean
START_TIME=`date +%s`
javac -classpath $7/ext/hadoop-core.jar:$7/ext/hadoop-common.jar:$7/ext/hadoop-hdfs.jar Intersection.java
jar cvf Intersection.jar Intersection*.class
$SPARK_HOME/sbt/sbt package
END_COMPILE_TIME=`date +%s`
COMPILE_TIME=`expr $END_COMPILE_TIME - $START_TIME`
echo "COMPILE TIME: "$COMPILE_TIME
hadoop jar Intersection.jar Intersection $2 $3 $1/int_edges/
$SPARK_HOME/sbt/sbt package "run-main PageRank $1/int_edges/ $4 $5 $6"
END_TIME=`date +%s`
RUN_TIME=`expr $END_TIME - $START_TIME`
echo "TOTAL TIME: "$RUN_TIME
hadoop fs -rm -r $1/int_edges/
hadoop fs -rm -r $5