#!/bin/bash
procid=`grep -n \`hostname\` ~/machines | cut -d':' -f 1` ; procid=`expr $procid - 1` ; echo $procid