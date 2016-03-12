#!/bin/bash
# $1 = ratings_dir
# $2 = movies_dir
# $3 = output_dir
# $4 = parallelism
# $5 = pre_year
$SPARK_HOME/sbt/sbt clean
START_TIME=`date +%s`
$SPARK_HOME/sbt/sbt package
END_COMPILE_TIME=`date +%s`
COMPILE_TIME=`expr $END_COMPILE_TIME - $START_TIME`
echo "COMPILE TIME: "$COMPILE_TIME
$SPARK_HOME/sbt/sbt package "run $1 $2 $3 $4 $5"
END_TIME=`date +%s`
RUN_TIME=`expr $END_TIME - $START_TIME`
echo "TOTAL TIME: "$RUN_TIME
hadoop fs -rm -r $3