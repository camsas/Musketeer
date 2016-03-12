#!/usr/bin/python
import boto
import boto.ec2
import boto.iam
import sys, os
import time
from boto.ec2.blockdevicemapping import BlockDeviceType
from boto.ec2.blockdevicemapping import BlockDeviceMapping
from datetime import datetime
from optparse import OptionParser
from sets import Set
from subprocess import call
from subprocess import check_output
from subprocess import Popen, PIPE
from threading import Thread
from time import sleep

def get_connection(aws_access_key, aws_secret_key):
    return boto.ec2.connect_to_region(u'us-east-1',
                                      aws_access_key_id=aws_access_key,
                                      aws_secret_access_key=aws_secret_key)

def start_instances(conn, imageId, instanceType, num_instances, master=False):
    cur_instances = Set([])
    if os.path.isfile('musketeer_ec2_workers'):
        for line in open('musketeer_ec2_workers').readlines():
            fields = [x.strip() for x in line.split()]
            cur_instances.add(fields[0])
    if os.path.isfile('musketeer_ec2_master'):
        for line in open('musketeer_ec2_master').readlines():
            fields = [x.strip() for x in line.split()]
            cur_instances.add(fields[0])
    reservations = conn.get_all_reservations()
    for reservation in reservations:
        for instance in reservation.instances:
            instance.update()
            if instance.state == u'running':
                cur_instances.add(instance.id)
    if master is True:
        instances_file = open("musketeer_ec2_master", "w")
    else:
        instances_file = open("musketeer_ec2_workers", "a")
    instances = []
    bdm = BlockDeviceMapping()
    eph0 = BlockDeviceType()
    eph0.ephemeral_name = 'ephemeral0'
    bdm['/dev/sdb'] = eph0
    spot_request = conn.request_spot_instances(price="0.8",
                                               image_id=imageId,
                                               count=num_instances,
                                               launch_group="us-east-1a",
                                               availability_zone_group="us-east-1a",
                                               key_name="Musketeer",
                                               security_groups=["Musketeer"],
                                               instance_type=instanceType,
                                               block_device_map= bdm,
                                               user_data="")
    spot_request = conn.get_all_spot_instance_requests(request_ids=[spot_request[0].id])[0]

    # Wait until requests gets fulfilled
    open_request = True
    while open_request:
        time.sleep(5)
        open_request = False
        spot_requests = conn.get_all_spot_instance_requests(request_ids=[spot_request.id])
        for spot_request in spot_requests:
            print 'Spot request status: ', spot_request.state
            if spot_request.state == 'open':
                open_request = True

    # Get info about instances
    instance_num = 0
    reservations = conn.get_all_reservations()
    for reservation in reservations:
        for instance in reservation.instances:
            instance.update()
            while instance.state == u'pending':
                time.sleep(1)
                instance.update()
            if instance.state == u'running':
                if instance.id  not in cur_instances:
                    instance_num += 1
                    print 'Started instance %d: %s' % (instance_num, instance)
                    instances.append(instance.public_dns_name)
                    instances_file.write(instance.id + ' ' +
                                         instance.public_dns_name + '\n')
            else:
                print 'Could not start instance: ', instance
    instances_file.close()
    return instances

# TODO(ionel): An instance is not removed from the ec2 files when it is
# terminated. Need to removed it and update all the slaves and machines files
# on all the ec2 instances.
def terminate_instances(conn, instances):
    print 'Terminating instances: ', instances
    conn.terminate_instances(instances)

def terminate_instances(conn):
    print 'Terminate all instances'
    instances = []
    if os.path.isfile('musketeer_ec2_workers'):
        for line in open('musketeer_ec2_workers').readlines():
            fields = [x.strip() for x in line.split()]
            instances.append(fields[0])
    if os.path.isfile('musketeer_ec2_master'):
        for line in open('musketeer_ec2_master').readlines():
            fields = [x.strip() for x in line.split()]
            instances.append(fields[0])
    p = Popen(["rm musketeer_ec2_workers; rm musketeer_ec2_master; rm machines"],
              shell=True, stdout=PIPE)
    (output,err) = p.communicate()
    print "Removed ec2_files: ", output
    print "Removed ec2_files: ", err
    conn.terminate_instances(instances)

def generate_machines_file():
    machines_file = open("machines", "w")
    machines = []
    if os.path.isfile('musketeer_ec2_master'):
        for line in open('musketeer_ec2_master').readlines():
            fields = [x.strip() for x in line.split()]
            machines_file.write(fields[1] + '\n')
            machines.append(fields[1])
    if os.path.isfile('musketeer_ec2_workers'):
        for line in open('musketeer_ec2_workers').readlines():
            fields = [x.strip() for x in line.split()]
            machines_file.write(fields[1] + '\n')
            machines.append(fields[1])
    machines_file.close()
    return machines

def scp_machines_file(username, machines):
    print 'Scp machines file: ', machines
    for address in machines:
        p = Popen(["scp -o StrictHostKeyChecking=no machines " + username +
                   "@" + address + ":"], shell=True, stdout=PIPE)
        (output,err) = p.communicate()
        print address, output
        print address, err

def run_cmd_on_machine_thread(username, command, address):
        p = Popen(["ssh -o StrictHostKeyChecking=no " + username + "@" +
                   address + " \"" + command + "\""], shell=True, stdout=PIPE)
        (output,err) = p.communicate()

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

def get_master():
    for line in open('musketeer_ec2_master').readlines():
        fields = [x.strip() for x in line.split()]
        return fields[1]

def configure_spark_master(username, master):
    print 'Configure spark master: ', master
    p = Popen(["ssh -o StrictHostKeyChecking=no " + username + "@" +
               master + " \"sh configure_spark.sh " + master +
               " master\""], shell=True, stdout=PIPE)
    (output,err) = p.communicate()
    print master, output
    print master, err

def configure_spark_thread(username, address, master, start_spark):
    if start_spark is True:
        p = Popen(["ssh -o StrictHostKeyChecking=no " + username + "@" +
                   address + " \"sh configure_spark.sh " + master +
                   " slave " + address + "\""], shell=True, stdout=PIPE)
    else:
        p = Popen(["ssh -o StrictHostKeyChecking=no " + username + "@" +
                   address + " \"sh configure_spark.sh " + master +
                   " slave \""], shell=True, stdout=PIPE)
    (output,err) = p.communicate()


def configure_spark(username, machines, master, start_spark=False):
    print 'Configure spark: ', machines
    index = 0
    threads = []
    for address in machines:
        threads.append(Thread(target = configure_spark_thread,
                              args = (username, address, master, start_spark)))
        threads[-1].start()
        index += 1
        if index % 10 == 0:
            for thread in threads:
                thread.join()
            threads = []
    for thread in threads:
        thread.join()

def setup_master(username, master):
    print 'Setup master: ', master
    run_cmd_on_machines(username, "sh configure_hadoop.sh " + master +
                        " master", [master])

def setup_workers(username, master, workers):
    print 'Setup workers: ', workers
    run_cmd_on_machines(username, "sh configure_hadoop.sh " + master + " slave",
                        workers)

def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option("-m", "--start_master", dest="start_master",
                      action="store_true", default=False,
                      help="start cluster master")
    parser.add_option("-w", "--start_workers", dest="start_workers",
                      type="int", help="start workers")
    parser.add_option("-a", "--aws_access_key", dest="aws_access_key",
                      type="string", help="aws access key")
    parser.add_option("-s", "--aws_secret_key", dest="aws_secret_key",
                      type="string", help="aws secret key")
    parser.add_option("-z", "--terminate_instance", dest="terminate_instance",
                      type="string", help="terminate a given instance")
    parser.add_option("-t", "--terminate_all", action="store_true",
                      dest="terminate_all", default=False,
                      help="terminate all the instances")
    parser.add_option("-i", "--image_id", dest="image_id", type="string",
                      default="ami-691b1700", help="image id")
    parser.add_option("-n", "--instance_type", dest="instance_type",
                      type="string", default="m1.xlarge", help="instance type")
    parser.add_option("-u", "--user", dest="user", type="string",
                      default="icg27", help="username to use for running jobs")
    parser.add_option("-e", "--setup_spark", dest="setup_spark",
                      action="store_true", default=False,
                      help="start Spark cluster")
    parser.add_option("-g", "--setup_powergraph", dest="setup_powergraph",
                      action="store_true", default=False,
                      help="setup powergraph")
    parser.add_option("-x", "--add_workers", dest="add_workers", type="int",
                      help="spawn new ec2 worker instances")
    parser.add_option("-f", "--start_frameworks", dest="start_frameworks",
                      action="store_true", default=False,
                      help="true if Spark should be started on the newly added instances")
    (options, args) = parser.parse_args()
    if options.start_master and options.terminate_all:
        parser.error("options --start_master and --terminate_all are mutually exclusive")
    if options.start_workers and options.terminate_all:
        parser.error("options --start_workers and --terminate_all are mutually exclusive")
    if options.add_workers and options.terminate_all:
        parser.error("options --add_workers and --terminate_all are mutually exclusive")
    if options.setup_spark and options.terminate_all:
        parser.error("options --setup_spark and --terminate_all are mutually exclusive")
    if options.setup_powergraph and options.terminate_all:
        parser.error("options --setup_powergraph and --terminate_all are mutually exclusive")
    if options.aws_access_key is None:
        parser.error("aws_access_key not given")
    if options.aws_secret_key is None:
        parser.error("aws_secret_key not given")

    conn = get_connection(options.aws_access_key, options.aws_secret_key)

    if options.start_master is True:
        print "Starting master"
        instances = start_instances(conn, options.image_id,
                                    options.instance_type, 1, master=True)
        time.sleep(60)
        setup_master(options.user, instances[0])

    if options.start_workers is not None:
        print "Starting workers"
        instances = start_instances(conn, options.image_id,
                                    options.instance_type,
                                    options.start_workers)
        time.sleep(60)
        setup_workers(options.user, get_master(), instances)

    if options.setup_powergraph is True:
        print "Setup PowerGraph"
        machines = generate_machines_file()
        scp_machines_file(options.user, machines)

    if options.setup_spark is True:
        print "Starting Spark"
        machines = generate_machines_file()
        scp_machines_file(options.user, machines)
        master = get_master()
        configure_spark(options.user, machines, master)
        configure_spark_master(options.user, master)

    if options.add_workers is not None:
        print "Adding workers"
        instances = start_instances(conn, options.image_id,
                                    options.instance_type,
                                    options.add_workers)
        time.sleep(60)
        master = get_master()
        setup_workers(options.user, master, instances)
        machines = generate_machines_file()
        configure_spark(options.user, machines, master)
        if options.start_frameworks:
            configure_spark(options.user, instances, master, start_spark=True)

    if options.terminate_instance is not None:
        print "Terminating instance: ", options.terminate_instance
        terminate_instances(conn, [options.terminate_instance])

    if options.terminate_all is True:
        print "Terminating all instances"
        terminate_instances(conn)

if __name__ == "__main__":
    main()
