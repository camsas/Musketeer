#! /bin/bash

./quicktrans.py DataTransformer.cc DataTransformer.cpp
cake DataTransformer.cpp --append-CXXFLAGS="-I/usr/lib/jvm/java-7-openjdk-amd64/include -I/usr/lib/jvm/java-7-openjdk-amd64/include/linux" --append-LINKFLAGS="-L/usr/lib/hadoop/lib/native/ -l hdfs -lpthread -L /usr/lib/jvm/java-7-openjdk-amd64/jre/lib/amd64/server/ -l jvm"
