#!/usr/bin/env python
import sys, os
import random
import subprocess
from subprocess import call
from subprocess import check_output

if len(sys.argv) < 4:
    print "usage: gen_community_graph.py lj_graph lj_vertices com_graph percentage"
    sys.exit(1)

lj_graph_loc = sys.argv[1]
lj_vertices_loc = sys.argv[2]
com_graph_loc = sys.argv[3]
percentage = int(sys.argv[4])

random.seed()
lj_graph = open(lj_graph_loc, 'r')
lj_ver = open(lj_vertices_loc, 'r')
com_graph = open(com_graph_loc, 'w')
vertices = set()

for line in lj_ver.readlines():
    cols = line.rstrip().split(' ')
    vertex = int(cols[0])
    val = random.randint(0, 100)
    if val < percentage:
      vertices.add(vertex)

for line in lj_graph.readlines():
    cols = line.rstrip().split(' ')
    src = int(cols[0])
    dst = int(cols[1])
    if src in vertices and dst in vertices:
        com_graph.write(str(src) + ' ' + str(dst) + '\n')
# Number of vertices in the lj graph
lj_n_vertices = 4847571
lj_n_edges = 68993773
# How many vertices to add
new_n_vertices = lj_n_vertices - percentage * lj_n_vertices / 100
n_vertices = lj_n_vertices + new_n_vertices
print 'Vertices: ', n_vertices
edges_to_add = int(float(new_n_vertices) * float(lj_n_edges) / float(lj_n_vertices))
print 'Edges: ', (lj_n_edges + edges_to_add)
while edges_to_add > 0:
    src = random.randint(0, new_n_vertices) + lj_n_vertices
    dst = random.randint(0, n_vertices)
    if src != dst:
        com_graph.write(str(src) + ' ' + str(dst) + '\n')
        edges_to_add -= 1

lj_graph.close()
lj_ver.close()
com_graph.close()
