#!/usr/bin/env python
import sys, os
import random
import subprocess
from array import *
from subprocess import call
from subprocess import check_output

if len(sys.argv) < 4:
    print "usage: gen_shoppers.py shop_logs num_users num_countries num_prod"
    sys.exit(1)

shop_logs_loc = sys.argv[1]
num_users = int(sys.argv[2])
num_countries = int(sys.argv[3])
num_products = int(sys.argv[4])
shop_logs = open(shop_logs_loc, 'w')

random.seed()
pid_price = array('l', [])
cur_pid = 0
while cur_pid < num_products:
    price = int(random.gauss(300, 200))
    while price <= 0:
        price = int(random.gauss(300, 200))
    pid_price.append(price)
#    print cur_pid, pid_price[cur_pid]
    cur_pid += 1

num_user = 1
while num_user < num_users:
    if num_user % 2 == 0:
        # User from the US.
        num_prod = random.randint(1, 30)
        cur_prod = 0
        while cur_prod < num_prod:
            pid = random.randint(1, num_products)
            shop_logs.write(str(num_user) + ' 1 ' + str(pid) + ' ' +
                            str(pid_price[pid - 1]) + '\n')
            cur_prod += 1
    else:
        cid = random.randint(2, num_countries)
        num_prod = random.randint(1, 30)
        cur_prod = 0
        while cur_prod < num_prod:
            pid = random.randint(1, num_products)
            shop_logs.write(str(num_user) + ' ' + str(cid) + ' ' + str(pid) +
                            ' ' + str(pid_price[pid - 1]) + '\n')
            cur_prod += 1
    num_user += 1

shop_logs.close()
