#!/bin/python

import os
import sys

if (len(sys.argv)<2):
 print "Error Incorrect Format Specified"
 exit
directory = sys.argv[1]
dir = directory  + "/training_set"
print "Directory: " + directory
new_dir = dir + "_processed"
command = "mkdir " + new_dir
os.system(command)
new_file_name = "ratings.in"
with open(new_file_name, 'w') as outfile:
 for x in range (1,17771):
  mov_id = str(x).zfill(7)
  file_name = dir + "/" + "mv_" + mov_id + ".txt"
  print "Processing " + file_name
  with open(file_name) as in_file:
   for line in in_file:
    data = line.strip().replace(",", " " )
    outfile.write(str(x) + " " +  data + "\n")
movies = directory + "/movie_titles.txt"
movies_pro = directory + "/movie_titles_processed.txt"
with open(movies) as mov_file, open(movies_pro,'w') as mov_file_pro :
 for line in mov_file:
  dat  = line.strip().replace(" ", "-")
  dat = dat.replace(",", " ")
  dat = dat.replace("NULL", "1980")
  mov_file_pro.write(dat + "\n")
