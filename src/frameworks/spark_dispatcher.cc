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

#include "frameworks/spark_dispatcher.h"

#include <stdlib.h>

#include <string>

#include "base/common.h"

namespace musketeer {
namespace framework {

  SparkDispatcher::SparkDispatcher() {
  }

  void SparkDispatcher::Execute(string job_path, string job_options) {
    string sbt = FLAGS_spark_dir + "sbt/sbt ";
    // change directory to be in the correct project directory then do sbt run
    string dir = FLAGS_generated_code_dir + "/" + job_options + "_code";
    string cmd = "export SPARK_MEM=8g ; cd " + dir + "&& " + sbt  + "run" +
      " " + job_options;
    LOG(INFO) << "spark run started for: " << job_path;
    system(cmd.c_str());
    LOG(INFO) << "spark run ended for: " << job_path;
  }

} // namespace framework
} // namespace musketeer
