#!/usr/bin/env python
import sys, os
import random
import subprocess
from subprocess import call
from subprocess import check_output

if len(sys.argv) < 4:
    print "usage: gen_random_graph.py graph_edges graph_vert num_vertices vertex_deg"
    sys.exit(1)

graph_edges_loc = sys.argv[1]
graph_vert_loc = sys.argv[2]
num_ver = int(sys.argv[3])
ver_deg = int(sys.argv[4])

random.seed()
graph_edges = open(graph_edges_loc, 'w')
graph_vert = open(graph_vert_loc, 'w')

vert_id = 0
while vert_id < num_ver:
  graph_vert.write(str(vert_id) + ' 1.0\n')
  vert_id += 1
vert_id = 0
while vert_id < num_ver:
  vert_id_str = str(vert_id)
  cur_deg = 0
  while cur_deg < ver_deg:
    dst = random.randint(0, num_ver)
    graph_edges.write(vert_id_str + ' ' + str(dst) + '\n')
    cur_deg += 1
  vert_id += 1

graph_edges.close()
graph_vert.close()
