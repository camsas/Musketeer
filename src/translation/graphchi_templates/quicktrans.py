#! /usr/bin/python
import os
import sys

# Replace {{HDFS_MASTER}} with hdfs://freestyle
# Replace {{HDFS_PORT}} with 8020
# Replace {{EDGES_PATH}} and {{VERTICES_PATH}} with what you want to copy .e.g /home/icg27/lj_edges/  and /home/icg27/lj_pr/ . They are paths on hdfs.
# For testing you can comment out: ascii_to_binary("/tmp{{VERTICES_PATH}}input", ver_file);
# Then you can just make all; ./DataTransformer_bin copy_input. This should allow you to test it. I think it copies the data to /tmp{{VERTICES_PATH}} and /tmp{{EDGES_PATH}}.

trans = { "{{VERTEX_DATA_TYPE}}" : "double" ,
         "{{HDFS_MASTER}}"       : "freestyle.private.srg.cl.cam.ac.uk" ,
         "{{HDFS_PORT}}"         : "8020" ,
         "{{EDGES_PATH}}"        : "/home/icg27/lj_edges" ,
         "{{VERTICES_PATH}}"     : "/home/icg27/lj_pr/",
         "{{TMP_ROOT}}"          : "/tmp",
       }

out = open(sys.argv[2], "w")
for line in open(sys.argv[1]):
    for key in trans.keys():
        line = line.replace(key, trans[key])

    out.write(line)
