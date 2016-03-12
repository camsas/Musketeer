#!/bin/bash
# $1 = root_dir
# $2 = pr location
# $3 = iter location
# $4 = vertices_dir
if [ "$#" -eq 3 ]
  then
    hadoop fs -rm -r $4*
    hadoop fs -put $2 $4
    hadoop fs -rm -r $1/iter/*
    hadoop fs -put $3 $1/iter/
    hadoop fs -rm -r $1/int_edges/*
    hadoop fs -rm -r $1/edges_cnt/*
    hadoop fs -rm -r $1/results/*
    hadoop fs -rm -r $1/results1/*
    hadoop fs -rm -r $1/results2/*
  else
    echo "Please provide: root_dir pr_location iter_location hdfs_vertices_dir"
fi