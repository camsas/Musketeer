#!/bin/bash
# $1 = log file
# $2 = stat file
COMPILE_TIME=`cat $1 | grep "COMPILE TIME:" | cut -d ":" -f 2 | tr -d ' '`
TOTAL_TIME=`cat $1 | grep "TOTAL TIME:" | cut -d ":" -f 2 | tr -d ' '`
echo "# COMP TOTAL" > $2
echo $COMPILE_TIME $TOTAL_TIME >> $2