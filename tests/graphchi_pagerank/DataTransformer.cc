#include <hdfs.h>
#include <jni.h>
#include <stdio.h>
#include <string.h>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <sys/time.h>

#define BUF_LEN 524288
#define OUT_BUF_LEN 16384

using namespace std;

void copy_file(hdfsFS& distfs, string path_name) {
  string file_name = "/tmp" + path_name + "input";
  string make_dir = "mkdir -p /tmp" + path_name;
  system(make_dir.c_str());
  FILE* localInFD = fopen(file_name.c_str(), "wb");
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
    tSize rres = 1;
    size_t wres = NULL;
    char buf[BUF_LEN];
    while (rres > 0) {
      rres = hdfsRead(distfs, hdfsInFD, &buf[0], BUF_LEN);
      wres = fwrite(&buf[0], sizeof(char), rres, localInFD);
    }
    hdfsCloseFile(distfs, hdfsInFD);
  }
  fclose(localInFD);
}

void ascii_to_binary(const char* in_filename, const char* out_filename) {
  double value;
  ifstream infile(in_filename);
  ofstream outfile(out_filename, ios::out | ios::binary);
  while (infile >> value) {
    outfile.write(reinterpret_cast<const char*>(&value), sizeof(value));
  }
  infile.close();
  outfile.close();
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
  double tmp_ver_val;
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

int main(int argc, char *argv[]) {
  timeval start_time, end_time;
  gettimeofday(&start_time, NULL);
  hdfsFS distfs = hdfsConnect("hdfs://10.11.12.61", 8020);
  // argv[2] - edges path
  // argv[3] - vertices path
  string edges_path = string(argv[2]);
  string vertices_path = string(argv[3]);
  if (!strcmp(argv[1], "copy_input")) {
    copy_file(distfs, argv[2]);
    copy_file(distfs, argv[3]);
    string tmp_ver = "/tmp" + vertices_path + "input";
    string tmp_ver_bin = "/tmp" + edges_path + "input.8B.vout";
    ascii_to_binary(tmp_ver.c_str(), tmp_ver_bin.c_str());
    gettimeofday(&end_time, NULL);
    long pulling_data = end_time.tv_sec - start_time.tv_sec;
    cout << "PULLING DATA: " << pulling_data << endl;
  } else {
    if (!strcmp(argv[1], "copy_output")) {
      string tmp_ver_bin = "/tmp" + edges_path + "input.8B.vout";
      string ver_out = vertices_path + "output";
      binary_to_ascii_hadoop(distfs, tmp_ver_bin.c_str(), ver_out.c_str());
      string rm_tmp_dirs =
        "rm -r /tmp" + vertices_path + "; rm -r /tmp" + edges_path;
      system(rm_tmp_dirs.c_str());
      gettimeofday(&end_time, NULL);
      long pushing_data = end_time.tv_sec - start_time.tv_sec;
      cout << "PUSHING DATA: " << pushing_data << endl;
    } else {
      fprintf(stderr, "Unknown request!");
      hdfsDisconnect(distfs);
      return 1;
    }
  }
  hdfsDisconnect(distfs);
  return 0;
}
