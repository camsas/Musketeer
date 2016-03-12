#!/usr/bin/env python
import sys, os
import random
import subprocess
from subprocess import call
from subprocess import check_output

if len(sys.argv) < 4:
    print "usage: gen_sssp.py edges_file_in edges_file_out ver_file_out num_vertices"
    sys.exit(1)

edges_file_in = open(sys.argv[1])
edges_file_out = open(sys.argv[2], 'w')
ver_file_out = open(sys.argv[3], 'w')
num_ver = int(sys.argv[4])

ver_id = 2
ver_file_out.write('0 100000000\n')
ver_file_out.write('1 0\n')
while ver_id < num_ver:
    ver_file_out.write(str(ver_id) + ' 100000000\n')
    ver_id += 1

with open(sys.argv[1]) as edges_file_in:
    for line in edges_file_in:
        edges_file_out.write(line.strip() + ' ' +
                             str(random.randint(0, 10000)) + '\n')

ver_file_out.close()
edges_file_out.close()
