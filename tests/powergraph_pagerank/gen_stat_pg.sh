#!/bin/bash
# $1 = log file
# $2 = node to compute stat for
# $3 = stat file
COMPILE_TIME=`cat $1 | grep "COMPILE TIME:" | cut -d ":" -f 2 | tr -d ' '`
LOAD_TIME=`cat $1 | grep "LOADING DATA ON $2:" | cut -d ":" -f 2 | tr -d ' '`
RUN_TIME=`cat $1 | grep "RUN TIME ON $2:" | cut -d ":" -f 2 | tr -d ' '`
PUSHING_TIME=`cat $1 | grep "PUSHING DATA ON $2:" | cut -d ":" -f 2 | tr -d ' '`
TOTAL_TIME=`cat $1 | grep "TOTAL TIME:" | cut -d ":" -f 2 | tr -d ' '`
echo "# COMP LOAD RUN PUSH TOTAL" > $3
echo $COMPILE_TIME $LOAD_TIME $RUN_TIME $PUSHING_TIME $TOTAL_TIME >> $3