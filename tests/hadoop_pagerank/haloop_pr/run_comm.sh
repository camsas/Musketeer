#!/bin/bash
# $1 = musketeer_dir
# $2 = root_dir
# $3 = hdfs graph egdes path
# $4 = hdfs graph edges path of the graph to intersect with

if [ "$#" -neq 5 ]
  then
    echo "Please provide: musketeer_dir root_dir hdfs_path_edges hdfs_path_edges_to_intersect_with"
    exit 1
fi

hadoop fs -rm -r $2/results/
hadoop fs -rm -r $2/int_edges/

START_TIME=`date +%s`
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar:./ TextPair.java
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar:./ GroupComparator.java
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar:./ KeyComparator.java
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar:./ FirstPartitioner.java
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar:./ NaivePageRank.java
jar cvf NaivePr.jar *.class
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar Intersection.java
jar cvf Intersection.jar Intersection*.class
END_COMPILE_TIME=`date +%s`
COMPILE_TIME=`expr $END_COMPILE_TIME - $START_TIME`
echo "COMPILE TIME: "$COMPILE_TIME
hadoop jar Intersection.jar Intersection $3 $4 $2/int_edges/
hadoop jar NaivePr.jar NaivePageRank $2/int_edges/ $2/results/ 5
END_TIME=`date +%s`
RUN_TIME=`expr $END_TIME - $START_TIME`
echo "TOTAL TIME: "$RUN_TIME
