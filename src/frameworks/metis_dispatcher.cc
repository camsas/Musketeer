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

#include "frameworks/metis_dispatcher.h"

#include <string>
#include <stdlib.h>

#include "base/common.h"
#include "base/flags.h"

namespace musketeer {
namespace framework {

  MetisDispatcher::MetisDispatcher() {
  }

  void MetisDispatcher::Execute(string job_path, string job_options) {
    string cmd;
    if (FLAGS_metis_heapprofile)
      cmd += "HEAP_PROFILE=/tmp/heapprofile ";
    if (FLAGS_metis_cpuprofile)
      cmd += "CPU_PROFILE=/tmp/cpuprofile ";
    if (atoi(FLAGS_metis_glog_v.c_str()) >= 0) {
      cmd += "GLOG_v=";
      cmd += FLAGS_metis_glog_v;
      cmd += " GLOG_logtostderr=1 ";
    }
    if (FLAGS_metis_debug_binaries)
      cmd += "gdb --args ";
    if (FLAGS_metis_strace_binaries)
      cmd += "strace ";
    cmd += (job_path + " " + job_options);
    LOG(INFO) << "Metis command: " << cmd;
    LOG(INFO) << "metis run started for: " << job_path;
    system(cmd.c_str());
    LOG(INFO) << "metis run ended for: " << job_path;
  }

} // namespace framework
} // namespace musketeer
