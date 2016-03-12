#! /usr/bin/python

import sys
import os
import urllib2
import re

my_ip = "10.11.12.54" #TODO XXX: make this dynamic


#reverse_dns = { "54" : "uriel",
#                "59" : "michael",
#                "61" : "freestyle",
#                "63" : "backstroke",
#                "69" : "tigger-0",
#                "70" : "hammerthrow"   }


#redirect_handler = urllib2.HTTPRedirectHandler()

class MyHTTPRedirectHandler(urllib2.HTTPRedirectHandler):
    def http_error_302(self, req, fp, code, msg, headers):
#        print req
#        print fp
#        print code
#        print msg
        headers["location"] = headers["location"].replace(".cl.cam.ac.uk","") 
        return urllib2.HTTPRedirectHandler.http_error_302(self, req, fp, code, msg, headers)

    http_error_301 = http_error_303 = http_error_307 = http_error_302

opener = urllib2.build_opener(MyHTTPRedirectHandler)
urllib2.install_opener(opener)

file = sys.argv[1]
#file="right_input512"
path = "/home/icg27/%s/%s" % (file,file)
encoded = path.replace("/","%2F")

req = "http://hammerthrow:50075/browseDirectory.jsp?dir=%s&namenodeInfoPort=50070&nnaddr=10.11.12.61:8020" % (encoded)
response = urllib2.urlopen(req )
html = response.read()
#print html

p = re.compile(">(\d{1,2}\..*?\.\d{1,2}):.*?</a>")
matches_ips = p.findall(html)

p = re.compile( "<td>(.*?):</td>" )
matches_blocks = p.findall( html )
#print "*********************"
#print "Blocks=%i" % (len(matches_blocks))
#print "*********************"

#Make a copy of this as we'll need it later to put all the blocks together
block_list = list(matches_blocks)

block_to_ips = {}
for i in range(0,len(matches_blocks)):
    print matches_blocks[i], matches_ips[i*3 + 0], matches_ips[i*3 + 1], matches_ips[i*3 + 2]
    block_to_ips[matches_blocks[i]] = (matches_ips[i*3 + 0], matches_ips[i*3 + 1], matches_ips[i*3 + 2])    
#print "*********************"

ip_to_blocks = {}
local_blocks = []
for b in block_to_ips.keys():
    if my_ip in block_to_ips[b]:
        local_blocks.append(b)
        print "Local copy of block %s" % b

	del block_to_ips[b] #Remove the block 
        continue


for b in block_to_ips.keys():
    for ip in block_to_ips[b]:
        if ip not in  ip_to_blocks:
            ip_to_blocks[ip] = []

        ip_to_blocks[ip].append(b)


def sort_ip_blocks(item):
	return len(ip_to_blocks[item])

ips = ip_to_blocks.keys()

ips.sort(key=sort_ip_blocks)

for ip in ips:
    ip_to_blocks[ip].sort()
    print ip,"-->", ip_to_blocks[ip]

coppied = []
to_copy = []
import subprocess
dfs_root1 = "/var/lib/hadoop-hdfs/cache/hdfs/dfs/data"
dfs_root2 = "/mnt/datadisk"

pids= []
while len(ip_to_blocks) > 0:
    pids= []
    for ip in ips:
        #print "Working on ip=",ip,"len=",len(ip_to_blocks[ip]) 
        while True:
            block = None
            if ip_to_blocks[ip] == []:
                del ip_to_blocks[ip]
                ips.remove(ip)
                break

            block = ip_to_blocks[ip].pop(0)
            if block not in coppied:
                break

            #print "Dropping block", block
	
        if not block:
            continue

        #print "Pushing id=%s from %s" %(block,ip)
        coppied.append(block)

        pid = os.fork()
        if pid != 0:
            pids.append(pid)
            #print pids
            continue

        block_fmt = block
        if block_fmt[0] == '-':
            block_fmt = "\\" + block_fmt

        cmd = 'ssh r2d2@%s "sudo find %s" | grep "%s"' %(ip, dfs_root1, block_fmt)
        #print cmd 
        process = subprocess.Popen(cmd, shell=True,stdout=subprocess.PIPE)
        out, err = process.communicate()

        if out == "":
            cmd = 'ssh r2d2@%s "sudo find %s" | grep "%s"' %(ip, dfs_root2, block_fmt)
            #print cmd
            process = subprocess.Popen(cmd, shell=True,stdout=subprocess.PIPE)
            out, err = process.communicate()
            
        #ditch the meta-data file
        outs = out.split("\n")
        for o in outs:
            if "meta" not in o:
                out = o
                break
             
        print "Block ID=%s on %s is at %s :" % (block,ip,out)
        cmd = "time scp r2d2@%s:%s /mnt/tmp/%s" % (ip, out, block)
        #print cmd
        process = subprocess.Popen(cmd, shell=True,stdout=subprocess.PIPE)
        out, err = process.communicate()
        #print out 
        sys.exit(0) #Get out of this forked process

    #print "############################"
    #print pids
    #print "############################"
    print "Waiting for %s..." % str(pids),
    for pid in pids:
        os.waitpid(pid,0)
    print "Done."

found = {}
for block in local_blocks:
   
    block_fmt = block
    if block_fmt[0] == '-':
        block_fmt = "\\" + block
    cmd = 'sudo find %s | grep "%s"' %( dfs_root1, block_fmt)
#    print cmd 
    process = subprocess.Popen(cmd, shell=True,stdout=subprocess.PIPE)
    out, err = process.communicate()
    if out == "":
        cmd = 'sudo find %s | grep "%s"' %(dfs_root2, block_fmt)
#        print cmd
        process = subprocess.Popen(cmd, shell=True,stdout=subprocess.PIPE)
        out, err = process.communicate()

    outs = out.split("\n")
    for o in outs:
        if "meta" not in o:
            out = o
            break

    found[block] = out
#    print "Local block ID=%s is at %s" % (block,out)

if block_list == []:
    print "File not found"
    sys.exit(-1)


cmd = "cat "
for b in block_list:
    if b in coppied:
        cmd += "/mnt/tmp/%s " % b
    else:
        cmd += "%s " % found[b]
 
cmd += " > /mnt/tmp/%s" % file
print cmd
process = subprocess.Popen(cmd, shell=True,stdout=subprocess.PIPE)
out, err = process.communicate()

#print out
