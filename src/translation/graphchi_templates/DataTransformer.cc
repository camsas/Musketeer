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
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <sys/time.h>

//#define BUF_LEN 524288
#define BUF_LEN 16384
#define OUT_BUF_LEN 16384

using namespace std;

//void copy_file(hdfsFS& distfs, string path_name) {
//    string file_name = "{{TMP_ROOT}}" + path_name + "input";
//    string make_dir = "mkdir -p {{TMP_ROOT}}" + path_name;
//    system(make_dir.c_str());
//    FILE* localInFD = fopen(file_name.c_str(), "wb");
//    if (!localInFD) {
//        fprintf(stderr, "Could not open temporaty file!");
//    }
//    int num_entries = 0;
//    hdfsFileInfo* hdfs_info = hdfsListDirectory(distfs, path_name.c_str(),
//            &num_entries);
//    for (int entry = 0; entry < num_entries; entry++) {
//        if (hdfs_info[entry].mKind == kObjectKindDirectory) {
//            continue;
//        }
//        hdfsFile hdfsInFD = hdfsOpenFile(distfs, hdfs_info[entry].mName, O_RDONLY,
//                0, 0, 0);
//        if (!hdfsInFD) {
//            fprintf(stderr, "Failed to open input file on HDFS!");
//        }
//        tSize rres = 1;
//        size_t wres = NULL;
//        char buf[BUF_LEN];
//        while (rres > 0) {
//            rres = hdfsRead(distfs, hdfsInFD, &buf[0], BUF_LEN);
//            wres = fwrite(&buf[0], sizeof(char), rres, localInFD);
//        }
//        hdfsCloseFile(distfs, hdfsInFD);
//    }
//    fclose(localInFD);
//}

void copy_file(hdfsFS& distfs, const string& path_name) {
  string make_dir = "mkdir -p {{TMP_ROOT}}" + path_name;
  printf("Running \"%s\" ...\n", make_dir.c_str());
  system(make_dir.c_str());

  int num_entries = 0;
  hdfsFileInfo* hdfs_info = hdfsListDirectory(distfs, path_name.c_str(), &num_entries);
  string cat_cmd = "cat ";
  char* fname = NULL;
  for (int entry = 0; entry < num_entries; entry++) {
    if (hdfs_info[entry].mKind == kObjectKindDirectory) {
      continue;
    }

    unsigned i = 0;
    for (i = 0; i < strlen(hdfs_info[entry].mName); i++) {
      if (hdfs_info[entry].mName[i] == ':') {
        i++;
        break;
      }
    }

    for (; i < strlen(hdfs_info[entry].mName); i++) {
      if (hdfs_info[entry].mName[i] == ':') {
        break;
      }
    }

    for (; i < strlen(hdfs_info[entry].mName); i++) {
      if (hdfs_info[entry].mName[i] == '/') {
        i++;
        break;
      }
    }


    fname = hdfs_info[entry].mName + i;
    printf(" --> %s\n", fname);
    string cmd = string("curl http://{{HDFS_ADDRESS}}:50075/streamFile/") + fname +
      "?nnaddr={{HDFS_MASTER}}:8020 --silent -o {{TMP_ROOT}}/" + fname;
    cat_cmd +=  "{{TMP_ROOT}}/" + string(fname) + " ";
    printf("Running \"%s\" ...\n", cmd.c_str());
    system(cmd.c_str());
  }

  if (num_entries > 1) {
    cat_cmd += "> {{TMP_ROOT}}/" + path_name + "input";
    printf("Running \"%s\" ...\n", cat_cmd.c_str());
    system(cat_cmd.c_str() );
  } else{
    string mv_cmd = string("mv {{TMP_ROOT}}/") + fname + " " + "{{TMP_ROOT}}/" + path_name +
      "input";
    printf("Running \"%s\" ... \n", mv_cmd.c_str());
    system(mv_cmd.c_str());
  }
}

void ascii_to_binary(const char* in_filename, const char* out_filename) {
  {{VERTEX_DATA_TYPE}} value;
  ifstream infile(in_filename);
  ofstream outfile(out_filename, ios::out | ios::binary);
  while (infile >> value) {
    outfile.write(reinterpret_cast<const char*>(&value), sizeof(value));
  }
  infile.close();
  outfile.close();
}

void copy_file_append(hdfsFS& distfs, string path_name) {
  string file_name = "{{TMP_ROOT}}" + path_name + "input";
  string make_dir = "mkdir -p {{TMP_ROOT}}" + path_name;
  system(make_dir.c_str());
  FILE* localInFD = fopen(file_name.c_str(), "w");
  if (!localInFD) {
    fprintf(stderr, "Could not open temporaty file!");
  }
  int num_entries = 0;
  hdfsFileInfo* hdfs_info = hdfsListDirectory(distfs, path_name.c_str(),
                                              &num_entries);
  for (int entry = 0; entry < num_entries; entry++) {
    if (hdfs_info[entry].mKind == kObjectKindDirectory) {
      continue;
    }
    hdfsFile hdfsInFD = hdfsOpenFile(distfs, hdfs_info[entry].mName, O_RDONLY,
                                     0, 0, 0);
    if (!hdfsInFD) {
      fprintf(stderr, "Failed to open input file on HDFS!");
    }
    char buf[BUF_LEN];
    tSize rres = hdfsRead(distfs, hdfsInFD, buf, BUF_LEN);
    buf[rres] = '\0';
    char* line;
    char* last_line;
    while (rres > 0) {
      line = strtok(buf, "\n\r");
      bool appended = true;
      while (line != NULL) {
        last_line = line;
        line = strtok(NULL, "\n\r");
        if (line != NULL) {
          fprintf(localInFD, "%s 0\n", last_line);
        } else {
          appended = false;
          fprintf(localInFD, "%s", last_line);
        }
      }
      if (buf[rres - 1] == '\n' || buf[rres - 1] == '\r') {
        appended = true;
        fprintf(localInFD, " 0\n");
      }
      rres = hdfsRead(distfs, hdfsInFD, buf, BUF_LEN);
      buf[rres] = '\0';
      if (!appended && (rres == 0 || buf[0] == '\n' || buf[0] == '\r')) {
        fprintf(localInFD, " 0\n");
      }
    }
    hdfsCloseFile(distfs, hdfsInFD);
  }
  fclose(localInFD);
}

char* sprintf_vertex_val(char* tmp_buf, unsigned int index, double ver_val) {
  return tmp_buf + sprintf(tmp_buf, "%d %lf\n", index, ver_val) + 1;
}

char* sprintf_vertex_val(char* tmp_buf, unsigned int index, int ver_val) {
  return tmp_buf + sprintf(tmp_buf, "%d %d\n", index, ver_val) + 1;
}

void binary_to_ascii_hadoop(hdfsFS& distfs, const char* in_filename,
                            const char* out_filename) {
  FILE* localInFD = fopen(in_filename, "rb");
  hdfsFile hdfsOutFD = hdfsOpenFile(distfs, out_filename, O_WRONLY | O_CREAT,
                                    0, 0, 0);
  if (!hdfsOutFD) {
    fprintf(stderr, "Unable to open HDFS file!");
    return;
  }

  {{VERTEX_DATA_TYPE}} tmp_ver_val;
  int buflen = OUT_BUF_LEN * sizeof(tmp_ver_val);
  char buf[buflen];
  // 16 for vertex, 16 for value, 1 space, 1 EOL
  char out_buf[OUT_BUF_LEN * 34];
  unsigned int index = 0;

  while (!feof(localInFD)) {
    size_t n_read = fread(buf, sizeof(char), buflen, localInFD);
    char* tmp_buf = out_buf;
    for (unsigned int cur_index = 0; cur_index < n_read;
         cur_index += sizeof(tmp_ver_val), index++) {
      memcpy(&tmp_ver_val, &buf[cur_index], sizeof(tmp_ver_val));
      tmp_buf = sprintf_vertex_val(tmp_buf, index, tmp_ver_val);
    }
    hdfsWrite(distfs, hdfsOutFD, out_buf, (tmp_buf - out_buf));
  }
  fclose(localInFD);
  hdfsCloseFile(distfs, hdfsOutFD);
}

void cmd_append(string path_name) {
  string file_name = "{{TMP_ROOT}}" + path_name + "input";
  string append_cmd = "awk '{$3=\"0\"; print}' " + file_name + " > " +
    file_name + ".tmp";
  system(append_cmd.c_str());
  string mv_cmd = "mv " + file_name + ".tmp " + file_name;
  system(mv_cmd.c_str());
}

int main(int argc, char *argv[]) {
  timeval start_time, end_time;
  gettimeofday(&start_time, NULL);
  printf("Connecting...to %s:%i\n", "hdfs://{{HDFS_MASTER}}", {{HDFS_PORT}});
  hdfsFS distfs = hdfsConnect("{{HDFS_MASTER}}", {{HDFS_PORT}});
  char ver_file[128];
  sprintf(ver_file, "{{TMP_ROOT}}{{EDGES_PATH}}input.%luB.vout", sizeof({{VERTEX_DATA_TYPE}}));

  if (!strcmp(argv[1], "copy_input")) {
    printf("Copying...\n");
    copy_file(distfs, "{{EDGES_PATH}}");
    // cmd_append("{{EDGES_PATH}}");
    copy_file(distfs, "{{VERTICES_PATH}}");
    ascii_to_binary("{{TMP_ROOT}}{{VERTICES_PATH}}input", ver_file);
    gettimeofday(&end_time, NULL);
    long pulling_data = end_time.tv_sec - start_time.tv_sec;
    cout << "PULLING DATA: " << pulling_data << endl;
  } else if (!strcmp(argv[1], "copy_output")) {
    binary_to_ascii_hadoop(distfs, ver_file, "{{VERTICES_PATH}}output");
    string rm_tmp_dirs =
      "rm -r {{TMP_ROOT}}{{VERTICES_PATH}} ; rm -r {{TMP_ROOT}}{{EDGES_PATH}}";
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
}
