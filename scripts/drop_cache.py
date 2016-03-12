#!/usr/bin/python
import sys, os
import time
from datetime import datetime
from optparse import OptionParser
from sets import Set
from subprocess import call
from subprocess import check_output
from subprocess import Popen, PIPE
from threading import Thread
from time import sleep

def run_cmd_on_machine_thread(username, command, address):
        p = Popen(["ssh -o StrictHostKeyChecking=no " + username + "@" +
                   address + " \"" + command + "\""], shell=True, stdout=PIPE)
        (output,err) = p.communicate()
        print output, err

def run_cmd_on_machines(username, command, machines):
    print 'Run command on: ', machines
    index = 0
    threads = []
    for address in machines:
        threads.append(Thread(target = run_cmd_on_machine_thread,
                             args = (username, command, address)))
        threads[-1].start()
        index += 1
        if index % 10 == 0:
            for thread in threads:
                thread.join()
            threads = []
    for thread in threads:
        thread.join()

def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option("-w", "--workers_file", dest="workers_file",
                      type="string", help="workers file")
    (options, args) = parser.parse_args()
    if options.workers_file is None:
        parser.error("workers file not given")
    nodes = []
    if os.path.isfile(options.workers_file):
        for line in open(options.workers_file).readlines():
            fields = [x.strip() for x in line.split()]
            nodes.append(fields[0])
    # XXX: Assumes the username is the same as local
    run_cmd_on_machines(os.getlogin(),
                        "echo 3 | sudo tee /proc/sys/vm/drop_caches",
                        nodes)

if __name__ == "__main__":
    main()
