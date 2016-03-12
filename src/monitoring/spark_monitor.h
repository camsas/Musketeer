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

#ifndef MUSKETEER_MONITORING_SPARK_MONITOR_H
#define MUSKETEER_MONITORING_SPARK_MONITOR_H

#include "monitoring/monitor_interface.h"

#include <jansson.h>
#include <stdio.h>
#include <stdlib.h>

#include <cstring>
#include <string>
#include <vector>

#include "base/common.h"

namespace musketeer {
namespace monitor {

static int writer(char* data, size_t size, size_t nmemb, string *writerData);

class SparkMonitor : public MonitorInterface {
 public:
  SparkMonitor();
  double CurrentUtilization();

 private:
  string formatJSON(const string& json);
  int ParseJSON(const string& json, double* utilization);
  static string HTTPLoad(const string& url);

  string master_name_;
  uint32_t master_port_;
};

} // namespace monitor
} // namespace musketeer
#endif  // MUSKETEER_MONITORING_SPARK_MONITOR_H
