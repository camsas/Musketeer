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

#include "base/hdfs_utils.h"

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>

#include "base/common.h"
#include "base/flags.h"

namespace musketeer {

  string GetHdfsRelValue(const string& relation) {
    string value = "";
    string cmd = "hadoop fs -get " + FLAGS_hdfs_input_dir + relation + " " +
      FLAGS_hdfs_input_dir;
    std::system(cmd.c_str());
    // Assumption about the file name.
    string file_name = FLAGS_hdfs_input_dir + relation + "/part-r-00000";
    ifstream rel_file(file_name.c_str());
    // Just read the first column.
    if (rel_file.is_open() && rel_file.good()) {
      getline(rel_file, value);
    }
    cmd = "rm -r " + FLAGS_hdfs_input_dir + relation;
    std::system(cmd.c_str());
    return value;
  }


  void removeHdfsDir(const string& path) {
    if (!FLAGS_dry_run) {
      string cmd = "hadoop fs -rm -r " + path;
      std::system(cmd.c_str());
    }
  }

  void renameHdfsDir(const string& src, const string& dst) {
    if (!FLAGS_dry_run) {
      string cmd = "hadoop fs -mv " + src + " " + dst;
      std::system(cmd.c_str());
    }
  }

  // Returns the size of a relation is KB.
  uint64_t GetRelationSize(string hdfs_location) {
    string cmd = "hadoop fs -du -s " + hdfs_location + " | awk '{print $1}'";
    const char* rel_size = Exec(cmd).c_str();
    uint64_t rel_size_uint;
    sscanf(rel_size, "%lu", &rel_size_uint);
    return rel_size_uint / 1024;
  }

  string Exec(string cmd) {
    FILE* pipe = popen(cmd.c_str(), "r");
    if (!pipe) {
      return "ERROR";
    }
    char buffer[128];
    string result = "";
    while (!feof(pipe)) {
      if (fgets(buffer, 128, pipe) != NULL) {
        result += buffer;
      }
    }
    pclose(pipe);
    return result;
  }

} // namespace musketeer
