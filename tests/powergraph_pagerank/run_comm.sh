#!/bin/bash
# $1 = source_file
# $2 = edges dir
# $3 = vertices dir
# $4 = powergraph dir
# $5 = output dir
# $6 = root dir
# $7 = musketeer dir
# $8 = comm graph edges dir
# $9 = num hosts
CUR_DIR=$PWD
START_TIME=`date +%s`
javac -classpath $7/ext/hadoop-core.jar:$7/ext/hadoop-common.jar:$7/ext/hadoop-hdfs.jar Intersection.java
jar cvf Intersection.jar Intersection*.class
mkdir -p $4/apps/pr_base
cp $1 $4/apps/pr_base/pr_base.cc
cp CMakeLists.txt $4/apps/pr_base/
cd $4 ; ./configure ; make -C $4release/apps/pr_base/
cd $4release/apps/pr_base/ ; $4/scripts/mpirsync
END_COMPILE_TIME=`date +%s`
COMPILE_TIME=`expr $END_COMPILE_TIME - $START_TIME`
cd $CUR_DIR
hadoop jar Intersection.jar Intersection $2 $8 $6/int_edges/
mpiexec.mpich2 -n $9 -hostfile ~/machines -envall $4release/apps/pr_base/pr_base --edges_dir hdfs://ec2-54-196-93-6.compute-1.amazonaws.com:8020$6/int_edges/ --vertices_dir hdfs://ec2-54-196-93-6.compute-1.amazonaws.com:8020$3 --saveprefix $5
END_TIME=`date +%s`
RUN_TIME=`expr $END_TIME - $START_TIME`
echo "TOTAL TIME: "$RUN_TIME
hadoop fs -rm -r $5
hadoop fs -rm -r $6/int_edges/
