#!/bin/bash
# $1 = musketeer_dir
# $2 = root_dir
# $3 = ratings_dir
# $4 = movies_dir
# $5 = output_dir
# $6 = number of reducers

START_TIME=`date +%s`
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar JoinMovRat.java
jar cvf JoinMovRat.jar JoinMovRat*.class
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar JoinPrefTranspose.java
jar cvf JoinPrefTranspose.jar JoinPrefTranspose*.class
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar AggJob.java
jar cvf AggJob.jar AggJob*.class
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar JoinIntPref.java
jar cvf JoinIntPref.jar JoinIntPref*.class
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar AggJob.java
jar cvf AggJob.jar AggJob*.class
javac -classpath $1/ext/hadoop-core.jar:$1/ext/hadoop-common.jar:$1/ext/hadoop-hdfs.jar MaxJob.java
jar cvf MaxJob.jar MaxJob*.class
END_COMPILE_TIME=`date +%s`
COMPILE_TIME=`expr $END_COMPILE_TIME - $START_TIME`
echo "COMPILE TIME: "$COMPILE_TIME

hadoop jar JoinMovRat.jar JoinMovRat $4 $3 $2preferences/ $6
hadoop jar JoinPrefTranspose.jar JoinPrefTranspose $2preferences/ $2matmult/ $6
hadoop jar AggJob.jar AggJob $2matmult/ $2int_result/ $6
hadoop fs -rm -r $2matmult/
hadoop jar JoinIntPref.jar JoinIntPref $2int_result/ $2preferences/ $2matmultfinal/ $6
hadoop fs -rm -r $2int_result/
hadoop fs -rm -r $2preferences/
hadoop jar AggJob.jar AggJob $2matmultfinal/ $2predicted/ $6
hadoop fs -rm -r $2matmultfinal/
hadoop jar MaxJob.jar MaxJob $2predicted/ $5 $6
hadoop fs -rm -r $2predicted/

END_TIME=`date +%s`
RUN_TIME=`expr $END_TIME - $START_TIME`
echo "TOTAL TIME: "$RUN_TIME
hadoop fs -rm -r $5