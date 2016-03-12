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

#ifndef MUSKETEER_MONITORING_HADOOP_MONITOR_H
#define MUSKETEER_MONITORING_HADOOP_MONITOR_H

#include "monitoring/monitor_interface.h"

#include <string>
#include <vector>

#include "base/common.h"

namespace musketeer {
namespace monitor {

static size_t write_response(void *ptr, size_t size, size_t nmemb,
                             void *stream);

class HadoopMonitor : public MonitorInterface {
 public:
  HadoopMonitor();
  double CurrentUtilization() {
    string json_status = "";
    return PollStatusInformationFromHadoop(&json_status);
  }
  double PollStatusInformationFromHadoop(string* str);
 private:
  static string HTTPLoad(const string& url);
  double ParseJSONStatusString(const string& json_str);

  string jobtracker_hostname_;
  uint32_t jobtracker_port_;
};

struct curl_write_result {
  char* data;
  int64_t pos;
};

} // namespace monitor
} // namespace musketeer
#endif  // MUSKETEER_MONITORING_HADOOP_MONITOR_H
