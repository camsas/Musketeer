// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

#include <hdfs.h>
#include <jni.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>

using namespace std;

#define BUF_LEN (4096 * 4)

string get_local_path(string hdfs_path) {
    return  hdfs_path.substr(0, hdfs_path.size() -1 ) + ".in";
}

void copy_file_to_local(hdfsFS& distfs, string path_name) {
  string file_name = get_local_path(path_name);
  string make_dir = "mkdir -p " + path_name;
//    std::cout << "Creating local dir \"" << make_dir  << "\"" << std::endl;
//    system(make_dir.c_str());
  std::cout << "Creating local file \"" << file_name << "\"" << std::endl;
  FILE* localInFD = fopen(file_name.c_str(), "wb");

  if (!localInFD) {
    fprintf(stderr, "Could not open temporaty file!");
  }

  int num_entries = 0;
  hdfsFileInfo* hdfs_info = hdfsListDirectory(distfs, path_name.c_str(), &num_entries);

  for (int entry = 0; entry < num_entries; entry++) {
    if (hdfs_info[entry].mKind == kObjectKindDirectory) {
      continue;
    }

    hdfsFile hdfsInFD = hdfsOpenFile(distfs, hdfs_info[entry].mName, O_RDONLY, 0, 0, 0);
    if (!hdfsInFD) {
      fprintf(stderr, "Failed to open input file on HDFS!");
    }

    tSize rres = 1;
    size_t wres = 0;
    char buf[BUF_LEN];
    while (rres > 0) {
      rres = hdfsRead(distfs, hdfsInFD, &buf[0], BUF_LEN);
      wres = 0;
      while( wres < (size_t)rres) {
        wres += fwrite(&buf[wres], sizeof(char), rres - wres, localInFD);
      }
    }

    hdfsCloseFile(distfs, hdfsInFD);
  }
  fclose(localInFD);
}

void copy_file_to_hdfs(hdfsFS& localfs, hdfsFS& distfs, string local_file, string path_name) {
  string output_file = path_name + "output";
  hdfsCopy(localfs, local_file.c_str(), distfs, output_file.c_str());
}

int main(int argc, char *argv[]) {
  timeval start_time, end_time;
  gettimeofday(&start_time, NULL);
  hdfsFS distfs = hdfsConnect("freestyle.private.srg.cl.cam.ac.uk", 8020);  // XXX

  if(argc < 2) {
    goto usage1;
  }

  if (!strcmp(argv[1], "pull")) {
    if(argc < 3){
      goto usage2;
    }
    copy_file_to_local(distfs, argv[2]);
    gettimeofday(&end_time, NULL);
    long pulling_data = end_time.tv_sec - start_time.tv_sec;
    cout << "PULLING DATA: " << pulling_data << endl;
  } else if (!strcmp(argv[1], "push")) {
    if(argc < 4) {
      goto usage3;
    }

    hdfsFS localfs = hdfsConnect(NULL, 0);
    copy_file_to_hdfs(localfs, distfs, get_local_path(argv[2]), argv[3]);
    string rm_tmp_dirs = string("rm -r") + argv[2];
    system(rm_tmp_dirs.c_str());
    gettimeofday(&end_time, NULL);
    long pushing_data = end_time.tv_sec - start_time.tv_sec;
    cout << "PUSHING DATA: " << pushing_data << endl;
  } else {
    fprintf(stderr, "Uknown request!");
    hdfsDisconnect(distfs);
    return 1;
  }

  hdfsDisconnect(distfs);
  return 0;

  usage1:
    fprintf(stderr, "Usage: hdfs_coppier [ pull | push ] [ input path ] [ output path ]\n");
    return 1;

  usage2:
    fprintf(stderr, "Usage: hdfs_coppier pull <input path> -- No input path supplied\n");
    return 1;

  usage3:
    fprintf(stderr,
            "Usage: hdfs_coppier push <input path> <output path> -- No input or output supplied\n");
    return 1;
}
