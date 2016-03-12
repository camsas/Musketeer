#!/bin/bash
# $1 = musketeer_dir
# $2 = root_dir
# $3 = edges_hdfs_location

if [ "$#" -neq 3 ]
  then
    echo "Please provide: musketeer_dir root_dir edges_hdfs_location"
    exit 1
fi

hadoop fs -rm -r $2/results/

START_TIME=`date +%s`
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar:./ TextPair.java
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar:./ GroupComparator.java
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar:./ KeyComparator.java
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar:./ FirstPartitioner.java
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar:./ NaivePageRank.java
jar cvf NaivePr.jar *.class
END_COMPILE_TIME=`date +%s`
COMPILE_TIME=`expr $END_COMPILE_TIME - $START_TIME`
echo "COMPILE TIME: "$COMPILE_TIME
hadoop jar NaivePr.jar NaivePageRank $3 $2/results/ 5
END_TIME=`date +%s`
RUN_TIME=`expr $END_TIME - $START_TIME`
echo "TOTAL TIME: "$RUN_TIME
