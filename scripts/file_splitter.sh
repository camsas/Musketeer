#!/bin/bash
# $1 = file
# $2 = base_file
nrlines=`wc -l $1 | cut -d' ' -f 1`

mkdir $2_splits7
lines=$(($nrlines/7+1))
echo $nrlines
split -l $lines -da 2 $1 $2
for i in {0..6} ; do mv ${2}0$i $2$i.in ; done
mv $2*.in $2_splits7/

mkdir $2_splits16
lines=$(($nrlines/16+1))
echo $nrlines
split -l $lines -da 2 $1 $2
for i in {0..9} ; do mv ${2}0$i $2$i.in ; done
for i in {10..15} ; do mv ${2}$i $2$i.in ; done
mv $2*.in $2_splits16/

mkdir $2_splits32
lines=$(($nrlines/32+1))
split -l $lines -da 2 $1 $2
for i in {0..9} ; do mv ${2}0$i $2$i.in ; done
for i in {10..31} ; do mv ${2}$i $2$i.in ; done
mv $2*.in $2_splits32/

mkdir $2_splits64
lines=$(($nrlines/64+1))
split -l $lines -da 2 $1 $2
for i in {0..9} ; do mv ${2}0$i $2$i.in ; done
for i in {10..63} ; do mv ${2}$i $2$i.in ; done
mv $2*.in $2_splits64/

mkdir $2_splits100
lines=$(($nrlines/100+1))
split -l $lines -da 2 $1 $2
for i in {0..9} ; do mv ${2}0$i $2$i.in ; done
for i in {10..99} ; do mv ${2}$i $2$i.in ; done
mv $2*.in $2_splits100/
