#!/bin/bash
# $1 = source_file
# $2 = edges dir
# $3 = vertices dir
# $4 = powergraph dir
# $5 = output dir
# $6 = num hosts
CUR_DIR=$PWD
START_TIME=`date +%s`
mkdir -p $4/apps/pr_base
cp $1 $4/apps/pr_base/pr_base.cc
cp CMakeLists.txt $4/apps/pr_base/
cd $4 ; ./configure ; make -C $4release/apps/pr_base/
cd $4release/apps/pr_base/ ; $4/scripts/mpirsync
END_COMPILE_TIME=`date +%s`
COMPILE_TIME=`expr $END_COMPILE_TIME - $START_TIME`
echo "COMPILE TIME: "$COMPILE_TIME
mpiexec.mpich2 -n $6 -envall -hostfile ~/machines $4release/apps/pr_base/pr_base --edges_dir $2 --vertices_dir $3 --saveprefix $5
cd $CUR_DIR
END_TIME=`date +%s`
RUN_TIME=`expr $END_TIME - $START_TIME`
echo "TOTAL TIME: "$RUN_TIME
hadoop fs -rm -r $5