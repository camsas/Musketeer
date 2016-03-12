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

#include "monitoring/hadoop_monitor.h"

#include <curl/curl.h>
#include <jansson.h>

#include <algorithm>

#include "base/common.h"
#include "base/flags.h"

namespace musketeer {
namespace monitor {

#define BUFFER_SIZE  (256 * 1024)
#define URL_FORMAT   "http://%s:%u/metrics?format=json"
#define URL_SIZE     256

HadoopMonitor::HadoopMonitor() :
  jobtracker_hostname_(FLAGS_hadoop_job_tracker_host),
  jobtracker_port_(FLAGS_hadoop_job_tracker_port) {
}

static size_t write_response(void *ptr, size_t size, size_t nmemb,
                             void *stream) {
  struct curl_write_result *result = (struct curl_write_result *)stream;

  if (result->pos + size * nmemb >= BUFFER_SIZE - 1) {
    LOG(ERROR) << "buffer too small";
    return 0;
  }

  memcpy(result->data + result->pos, ptr, size * nmemb);
  result->pos += size * nmemb;

  return size * nmemb;
}

string HadoopMonitor::HTTPLoad(const string& url) {
  CURL *curl;
  CURLcode status;
  char *data;
  int32_t code;

  curl = curl_easy_init();
  data = reinterpret_cast<char*>(malloc(BUFFER_SIZE));
  if (!curl || !data) {
    return NULL;
  }

  curl_write_result wr;
  wr.data = data;
  wr.pos = 0;

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_response);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &wr);

  status = curl_easy_perform(curl);
  if (status != 0) {
    LOG(ERROR) << "CURL error: unable to request data from " << url;
    LOG(ERROR) << curl_easy_strerror(status);
    return NULL;
  }

  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
  if (code != 200) {
    LOG(ERROR) << "HTTP error: server responded with code " << code;
    return NULL;
  }

  // XXX(malte): This is a bit ugly, as it copies the string
  data[wr.pos] = '\0';
  string result(data, wr.pos);

  curl_easy_cleanup(curl);
  curl_global_cleanup();

  return result;
}


double HadoopMonitor::ParseJSONStatusString(const string& json_str) {
  double utilization = 0;
  json_t *root;
  json_error_t error;
  root = json_loads(json_str.c_str(), 0, &error);
  LOG(INFO) << json_str;

  if (!root) {
    LOG(ERROR) << "Failed to parse Hadoop status info: on line " << error.line
               << ": " << error.text;
  }

  CHECK(json_is_object(root));
  json_t* mapred = json_object_get(root, "mapred");
  CHECK(json_is_object(mapred));
  json_t* jobtracker = json_object_get(mapred, "jobtracker");
  CHECK(json_is_array(jobtracker));
  json_t* jobtracker2 = json_array_get(jobtracker, 0);  // single-element array
  CHECK(json_is_array(jobtracker2));
  for (uint32_t i = 0; i < json_array_size(jobtracker2); i += 2) {
    // Job tracker hostname
    json_t* jt_meta = json_array_get(jobtracker2, i);
    CHECK(json_is_object(jt_meta));
    json_t* jt_hostname = json_object_get(jt_meta, "hostName");
    CHECK(json_is_string(jt_hostname));
    string jt_hostname_str = json_string_value(jt_hostname);
    // Stats
    json_t* jt_stats = json_array_get(jobtracker2, i+1);
    CHECK(json_is_object(jt_stats));
    uint64_t total_reduce_slots = json_integer_value(
        json_object_get(jt_stats, "reduce_slots"));
    uint64_t total_map_slots = json_integer_value(
        json_object_get(jt_stats, "map_slots"));
    uint64_t occup_reduce_slots = json_integer_value(
        json_object_get(jt_stats, "occupied_reduce_slots"));
    uint64_t occup_map_slots = json_integer_value(
        json_object_get(jt_stats, "occupied_map_slots"));
    utilization =
        max(1.0 - (static_cast<double>(total_reduce_slots - occup_reduce_slots)
                   / static_cast<double>(total_reduce_slots)),
            1.0 - (static_cast<double>(total_map_slots - occup_map_slots)
                   / static_cast<double>(total_map_slots)));
    LOG(INFO) << "Hadoop job tracker on " << jt_hostname_str << "'s utilization"
              << " is " << utilization;
  }

  // release resources
  json_decref(root);
  return utilization;
}

double HadoopMonitor::PollStatusInformationFromHadoop(string* json_str) {
  char url[URL_SIZE];

  snprintf(url, URL_SIZE, URL_FORMAT, jobtracker_hostname_.c_str(),
          jobtracker_port_);

  *json_str = HTTPLoad(url);

  if (json_str->empty())
    LOG(ERROR) << "Failed to poll Hadoop status from " << jobtracker_hostname_;

  return ParseJSONStatusString(*json_str);
}

} // namespace monitor
} // namespace musketeer
