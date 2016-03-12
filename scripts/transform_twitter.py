#!/usr/bin/env python
import sys, os
import subprocess
from subprocess import call
from subprocess import check_output
from subprocess import Popen, PIPE
from datetime import datetime

def main():
    in_file = open(sys.argv[1], 'r')
    out_file = open(sys.argv[2], 'w')
    index = 0
    new_val = dict()
    for line in in_file:
        left = long(line.split()[0])
        if not left in new_val:
            new_val[left] = index
            index += 1
    in_file.close()
    in_file = open(sys.argv[1], 'r')
    for line in in_file:
        right = long(line.split()[1])
        if not right in new_val:
            new_val[right] = index
            index += 1
    in_file.close()
    in_file = open(sys.argv[1], 'r')
    for line in in_file:
        els = line.split()
        left = long(els[0])
        right = long(els[1])
        out_file.write(str(new_val[left]) + ' ' + str(new_val[right]) + '\n')
    in_file.close()
    out_file.close()

if __name__ == "__main__":
    main()
