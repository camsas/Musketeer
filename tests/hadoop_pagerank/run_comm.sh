#!/bin/bash
# $1 = musketeer_dir
# $2 = root_dir
# $3 = edges_dir
# $4 = intersect_edges_dir
# $5 = vertices_dir
# $6 = local_vertices_file

if [ "$#" -neq 6 ]
  then
    echo "Please provide: musketeer_dir root_dir edges_dir intersect_edges_dir vertices_dir local_vertices_file"
    exit 1
fi

START_TIME=`date +%s`
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar Intersection.java
jar cvf Intersection.jar Intersection*.class
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar Phase1.java
jar cvf Phase1.jar Phase1*.class
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar Phase2.java
jar cvf Phase2.jar Phase2*.class
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar Phase3.java
jar cvf Phase3.jar Phase3*.class
END_COMPILE_TIME=`date +%s`
COMPILE_TIME=`expr $END_COMPILE_TIME - $START_TIME`
echo "COMPILE TIME: "$COMPILE_TIME

hadoop jar Intersection.jar Intersection $3 $4 $2/int_edges/
hadoop jar Phase1.jar Phase1 $2/int_edges/ $2/edges_cnt/
for it in {1..5}
do
  hadoop jar Phase2.jar Phase2 $2/edges_cnt/ $5/ $2/results1/
  hadoop jar Phase3.jar Phase3 $2/results1/ $2/results2/
  hadoop fs -rm -r $5/*
  hadoop fs -mv $2/results2/* $5/
  hadoop fs -rm -r $2/results1
  hadoop fs -rm -r $2/results2
done
END_TIME=`date +%s`
RUN_TIME=`expr $END_TIME - $START_TIME`
echo "TOTAL TIME: "$RUN_TIME
hadoop fs -rm -r $2/int_edges
hadoop fs -rm -r $2/edges_cnt
hadoop fs -rm -r $2/results1
hadoop fs -rm -r $2/results2
hadoop fs -rm -r $5/*
hadoop fs -put $6* $5/