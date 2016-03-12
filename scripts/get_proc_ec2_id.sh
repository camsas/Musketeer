#!/bin/bash
procid=`curl -s http://169.254.169.254/latest/meta-data/public-hostname | grep -n -f ~/machines | cut -d':' -f 1` ; procid=`expr $procid - 1` ; echo $procid