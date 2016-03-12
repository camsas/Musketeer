#!/bin/bash
# $1 = num hosts
parallel-ssh -h /home/${USER}/machines -t 10000 -p $1 -i 'PROCID=`./get_proc_id.sh` ; cd Naiad ; Examples/bin/Release/Examples.exe netflix -p $PROCID -n $1 -t 4 -h @/home/${USER}/machines --inlineserializer ratings/ratings.in movies/movies.in 1920'
