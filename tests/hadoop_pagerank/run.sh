#!/bin/bash
# $1 = musketeer_dir
# $2 = root_dir
# $3 = edges_dir
# $4 = vertices_dir
# $5 = local_vertices_file

if [ "$#" -neq 5 ]
  then
    echo "Please provide: musketeer_dir root_dir edges_dir vertices_dir local_vertices_file"
    exit 1
fi
START_TIME=`date +%s`
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar Phase1.java
jar cvf Phase1.jar Phase1*.class
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar Phase2.java
jar cvf Phase2.jar Phase2*.class
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar Phase3.java
jar cvf Phase3.jar Phase3*.class
END_COMPILE_TIME=`date +%s`
COMPILE_TIME=`expr $END_COMPILE_TIME - $START_TIME`
echo "COMPILE TIME: "$COMPILE_TIME

hadoop jar Phase1.jar Phase1 $3 $2edges_cnt/
for it in {1..5}
do
  hadoop jar Phase2.jar Phase2 $2edges_cnt/ $4 $2results1/
  hadoop jar Phase3.jar Phase3 $2results1/ $2results2/
  hadoop fs -rm -r $4*
  hadoop fs -mv $2results2/* $4
  hadoop fs -rm -r $2results1
  hadoop fs -rm -r $2results2
done
END_TIME=`date +%s`
RUN_TIME=`expr $END_TIME - $START_TIME`
echo "TOTAL TIME: "$RUN_TIME
hadoop fs -rm -r $2edges_cnt/
hadoop fs -rm -r $4*
hadoop fs -put $5 $4