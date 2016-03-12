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

#include "monitoring/spark_monitor.h"

#include <curl/curl.h>

#include "base/common.h"
#include "base/flags.h"

#define BUFFER_SIZE (256 * 1024)

namespace musketeer {
namespace monitor {

  SparkMonitor::SparkMonitor() : master_name_(FLAGS_spark_web_ui_host),
                                 master_port_(FLAGS_spark_web_ui_port) {
  }

  string SparkMonitor::HTTPLoad(const string& url) {
    CURL* conn;
    CURLcode errCode;
    string htmldata;

    curl_global_init(CURL_GLOBAL_DEFAULT);
    conn = curl_easy_init();
    if (conn) {
      curl_easy_setopt(conn, CURLOPT_URL, url.c_str());
      errCode = curl_easy_perform(conn);
      if (errCode !=CURLE_OK) {
        LOG(ERROR) << "Error accessing Spark Web UI Error: "
                   << curl_easy_strerror(errCode);
        curl_easy_cleanup(conn);
        return NULL;
      }
      htmldata = reinterpret_cast<char*>(malloc(BUFFER_SIZE));
      errCode = curl_easy_setopt(conn, CURLOPT_WRITEFUNCTION, writer);
      if (errCode!= CURLE_OK) {
        LOG(ERROR) << "Error accessing Spark UI Error: "
                   << curl_easy_strerror(errCode);
        return NULL;
      }
      errCode = curl_easy_setopt(conn, CURLOPT_WRITEDATA, &htmldata);
      if (errCode != CURLE_OK) {
        LOG(ERROR) << "Error accessing Spark UI Error: "
                   << curl_easy_strerror(errCode);
        return NULL;
      }
    } else {
      LOG(ERROR) << "Error accessing Spark Web UI";
    }
    errCode = curl_easy_perform(conn);
    if (errCode !=CURLE_OK) {
      LOG(ERROR) << "Error Accessing Spark UI Error: "
                 << curl_easy_strerror(errCode);
    }
    curl_easy_cleanup(conn);
    curl_global_cleanup();
    return htmldata;
  }

  static int writer(char* data, size_t size, size_t nmemb, string *writerData) {
    if (writerData == NULL) {
      LOG(ERROR) << "NULL data";
      return 0;
    } else {
      writerData->append(data, size*nmemb);
      return size * nmemb;
    }
  }

  double SparkMonitor::CurrentUtilization() {
    int ln = 128;
    char* url = reinterpret_cast<char*>(malloc(ln));
    double* util = NULL;
    snprintf(url, ln, "http://%s:%u?format=json", master_name_.c_str(), master_port_);
    string json = HTTPLoad(url);
    util = reinterpret_cast<double*>(malloc(sizeof(double)));
    int error = ParseJSON(json, util);
    if (error != 0)  {
      LOG(ERROR) << "Invalid JSON";
      return -1;
    } else {
      return *util;
    }
  }

  string SparkMonitor::formatJSON(const string& json) {
    char* pch;
    char* str_tmp;
    string parsed = "{";
    pch = strtok_r((char*)(json.c_str()), "\n ", &str_tmp); // NOLINT
    while (pch != NULL) {
      pch = strtok_r(NULL, "\n ", &str_tmp);
      if (pch != NULL) {
        parsed.append(pch);
      }
    }
    return parsed;
  }

  int SparkMonitor::ParseJSON(const string& json_str, double* utilisation) {
    json_t* rootNode = NULL;
    json_error_t error;
    json_t* url = NULL;
    json_t* workers = NULL;
    json_t* coresUsed = NULL;
    json_t* cores = NULL;
    json_t* memoryUsed = NULL;
    json_t* memory = NULL;

    string json = formatJSON(json_str);
    rootNode = json_loads(json.c_str(), 0, &error);

    if (!rootNode) {
      LOG(ERROR) << "Incorrect JSON root Line " << error.line << ": "
                 << error.text;
      return -1;
    }
    CHECK(json_is_object(rootNode));
    url = json_object_get(rootNode, "url");
    CHECK(json_is_string(url));
    workers = json_object_get(rootNode, "workers");
    CHECK(json_is_array(workers));
    coresUsed = json_object_get(rootNode, "coresused");
    CHECK(json_is_integer(coresUsed));
    cores = json_object_get(rootNode, "cores");
    CHECK(json_is_integer(cores));
    memoryUsed = json_object_get(rootNode, "memoryused");
    CHECK(json_is_integer(memoryUsed));
    memory = json_object_get(rootNode, "memory");
    CHECK(json_is_integer(memory));

    uint64_t coresUsedNb = json_integer_value(coresUsed);
    uint64_t coresNb = json_integer_value(cores);
    uint64_t memoryUsedNb = json_integer_value(memoryUsed);
    uint64_t memoryNb = json_integer_value(memory);

    LOG(INFO) << "Cores Used: " << coresUsedNb;
    LOG(INFO) << "Cores Available: " << coresNb;
    LOG(INFO) << "Memory Used: " << memoryUsedNb;
    LOG(INFO) << "Memory Available: " << memoryNb;

    if (coresNb == 0 || memoryNb == 0) {
      *utilisation = 0;
      return 0;
    }

    double coreUtilisation = static_cast<double>(coresUsedNb/coresNb);
    double memoryUtilisation = static_cast<double>(memoryUsedNb / memoryNb);

    LOG(INFO) << "Core Utilisation: " << coreUtilisation;
    LOG(INFO) << "Memory Utilisation: " << memoryUtilisation;

    *utilisation = (coreUtilisation < memoryUtilisation) ?
      memoryUtilisation : coreUtilisation;
    return 0;
  }

} // namespace monitor
} // namespace musketeer
