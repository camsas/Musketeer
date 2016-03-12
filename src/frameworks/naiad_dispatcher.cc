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
#include "frameworks/naiad_dispatcher.h"

#include <boost/lexical_cast.hpp>

#include <cstdlib>
#include <iostream>
#include <string>

#include "base/common.h"
#include "base/flags.h"

namespace musketeer {

  NaiadDispatcher::NaiadDispatcher() {
  }

  void NaiadDispatcher::Execute(string job_path, string job_options) {
    LOG(INFO) << "naiad run started for: " << job_path;
    LOG(INFO) << "naiad copy input started for: " << job_path;
    string copy_input_cmd = "parallel-ssh -h " + FLAGS_naiad_hosts_file +
      " -t 10000 -p 100 -i 'START_TIME=`date +%s` ; sh " +
      FLAGS_generated_code_dir + "Musketeer/hdfs_get.sh ; END_TIME=`date +%s` ; " +
      "PULL_TIME=`expr $END_TIME - $START_TIME` ; echo \"PULLING DATA: \"$PULL_TIME'";
    system(copy_input_cmd.c_str());
    LOG(INFO) << "naiad copy input ended for: " << job_path;
    string run_cmd = "parallel-ssh -h " + FLAGS_naiad_hosts_file +
      " -t 10000 -p 100 -i 'PROCID=`" + FLAGS_generated_code_dir +
      "Musketeer/get_proc_id.sh` ; cd " + FLAGS_generated_code_dir + " ; mono-sgen " +
      job_path + " -p $PROCID -n " +
      boost::lexical_cast<string>(FLAGS_naiad_num_workers) + " -t " +
      boost::lexical_cast<string>(FLAGS_naiad_num_threads) + " -h @" +
      FLAGS_naiad_hosts_file +
      " --inlineserializer ; START_TIME=`date +%s` ; sh Musketeer/hdfs_put.sh ; " +
      "END_TIME=`date +%s` ; PUSH_TIME=`expr $END_TIME - $START_TIME` ; " +
      "echo \"PUSHING DATA: \"$PUSH_TIME'";
    LOG(INFO) << "Running: " << run_cmd;
    system(run_cmd.c_str());
    LOG(INFO) << "naiad run ended for: " << job_path;
  }

} // namespace musketeer
