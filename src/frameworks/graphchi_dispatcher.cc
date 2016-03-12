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

#include "frameworks/graphchi_dispatcher.h"

#include <cstdlib>
#include <iostream>
#include <string>

#include "base/common.h"
#include "base/flags.h"

namespace musketeer {
namespace framework {

  GraphChiDispatcher::GraphChiDispatcher() {
  }

  void GraphChiDispatcher::Execute(string job_path, string job_options) {
    // GraphChi doesn't have dynamic job options.
    LOG(INFO) << "graphChi run started for: " << job_path;
    string path = job_path.substr(0, job_path.rfind("/"));
    string copy_bin =  path + "/DataTransformer_bin";
    string source_env = path + "/env.sh";
    LOG(INFO) << "graphChi copy input started for: " << job_path;
    string copy_input_cmd = source_env + " ; " + copy_bin + " copy_input";
    system(copy_input_cmd.c_str());
    LOG(INFO) << "graphChi copy input ended for: " << job_path;
    string cmd = source_env + " ; export GRAPHCHI_ROOT=\"" +
      FLAGS_graphchi_dir + "\" ; " + job_path +
      " filetype edgelist membudget_mb 10000 execthreads 6";
    system(cmd.c_str());
    LOG(INFO) << "graphChi copy output started for: " << job_path;
    string copy_output_cmd = source_env + " ; " + copy_bin + " copy_output";
    system(copy_output_cmd.c_str());
    LOG(INFO) << "graphChi copy output ended for: " << job_path;
    LOG(INFO) << "graphChi run ended for: " << job_path;
  }

} // namespace framework
} // namespace musketeer
