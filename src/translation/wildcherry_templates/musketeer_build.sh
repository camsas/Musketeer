#! /bin/bash

cake_dir=/home/mpg39/Musketeer/ext/cake-git

variant="--variant=release"

#Build Wildcherry Operator
CFLAGS="-D_GNU_SOURCE -DIS_TEMPLATE -Ilibchaste -Werror -Wall -std=c11"
LINKFLAGS="-Llibchaste -lchaste -lrt"
$cake_dir/cake *.c --append-CFLAGS="$CFLAGS" --LINKFLAGS="$LINKFLAGS" $variant

#Build hdfs copier
JAVA_HOME="/usr/lib/jvm/java-7-openjdk-amd64"
CXXFLAGS="-Werror -Wall -I$JAVA_HOME/include -I$JAVA_HOME/include/linux/ -Wno-unused-result"
LINKFLAGS="-lrt -lhdfs -L$JAVA_HOME/jre/lib/amd64/server/ -ljvm -lz -lgomp"
$cake_dir/cake hdfs_copier.cc --append-CXXFLAGS="$CXXFLAGS" --append-CFLAGS="$CFLAGS" --LINKFLAGS="$LINKFLAGS" $variant 
