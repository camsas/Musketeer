#!/bin/bash
GITHUB_USER="ms705"

cd ext
git clone https://github.com/${GITHUB_USER}/Metis.git metis
cd metis
./configure --enable-debug --enable-profile
make 
