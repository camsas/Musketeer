#!/bin/bash
# $1 = server
# $2 = job_name (e.g. 201404211433_0438 from job_201404211433_0438)
# $3 = num_mappers
# $4 = num_reducers
# $5 = date to grep by (e.g. 1-May-2014)
for (( i=0 ; i<=$3; i++ ))
do
  mapper=`printf "%06i" $i`
  echo map $mapper $(curl -s http://$1:50030/taskdetails.jsp?tipid=task_$2_m_$mapper | grep "</table></td><td>" | awk -F"<td>" '{print $3}' | cut -d"." -f 1 | cut -d"/" -f 3 | sed 's:\:50060\">::g') $(curl -s http://$1:50030/taskdetails.jsp?tipid=task_$2_m_$mapper | grep "</table></td><td>" | awk 'BEGIN {FS="<td>"} {print $6 $7}') | sed 's/<td>//' | sed 's/<\/td>/ /' |  sed 's/<\/td>/ /'
done

for (( i=0 ; i<=$4; i++ ))
do
  mapper=`printf "%06i" $i`
  echo reduce $mapper $(curl -s http://$1:50030/taskdetails.jsp?tipid=task_$2_r_$mapper | grep "</table></td><td>" | awk -F"<td>" '{print $3}' | cut -d"." -f 1 | cut -d"/" -f 3 | sed 's:\:50060\">::g') $(curl -s http://$1:50030/taskdetails.jsp?tipid=task_$2_r_$mapper | grep "<td>$5" | sed '$!N;s/\n/ /' |  awk 'BEGIN {FS="<td>"} {print $6 $7 $8 $9}' | sed 's:<td>::g' | sed 's:<\/td>: :g')
done
