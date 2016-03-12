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

#include <stdio.h>
#include <boost/asio.hpp>

#include <cstdlib>

#include "base/common.h"
#include "base/job.pb.h"
#include "base/utils.h"

using musketeer::Job;

DEFINE_string(force_framework, "", "Force a framework for all operators in the workflow");
DEFINE_string(beer_query, "", "BEER DSL input file");
DEFINE_string(use_frameworks, "hadoop-spark-graphchi-naiad-powergraph",
              "Specifies the frameworks that can be used. Dash separated.");
DEFINE_bool(operator_merge, true, "Activates Operator Merge");
DEFINE_string(server_port, "20000", "Server Port");
DEFINE_string(server_ip, "", "Server IP");
DEFINE_bool(best_runtime, true, "Optimize for runtime or resource utilization");

inline void init(int argc, char *argv[]) {
  // Set up usage message
  string usage("Runs a job. Sample Usage:\nsubmit_job --beer_query test.rap");
  google::SetUsageMessage(usage);
  google::ParseCommandLineFlags(&argc, &argv, false);
  google::InitGoogleLogging(argv[0]);
}

Job* populateProtobuf() {
  LOG(INFO) << "Populate Protobuf";
  Job* job = new Job();
  job->set_force_framework(FLAGS_force_framework.c_str());
  job->set_frameworks(FLAGS_use_frameworks.c_str());
  if (FLAGS_operator_merge) {
    job->set_operator_merge("1");
  } else {
    job->set_operator_merge("0");
  }
  job->set_code(FLAGS_beer_query.c_str());
  return job;
}

int main(int argc, char* argv[]) {
  LOG(INFO) << "Begin Job Submission";
  init(argc, argv);
  FLAGS_logtostderr = true;
  FLAGS_stderrthreshold = 0;
  Job* job = populateProtobuf();
  LOG(INFO) << "Send Job";
  boost::asio::io_service io_service;
  boost::asio::ip::address serv_address =
    boost::asio::ip::address::from_string(FLAGS_server_ip.c_str());
  LOG(INFO) << FLAGS_server_ip;
  int port = atoi(FLAGS_server_port.c_str());
  LOG(INFO) << "Port : " << port;
  boost::asio::ip::tcp::endpoint endpoint(serv_address, port);
  LOG(INFO) << "Connect";
  boost::asio::ip::tcp::socket socket(io_service);
  socket.connect(endpoint);
  string buffer;
  job->SerializeToString(&buffer);
  boost::system::error_code ignored_error;
  boost::asio::write(socket, boost::asio::buffer(buffer.c_str(), buffer.size()),
                     boost::asio::transfer_at_least(buffer.size()),
                     ignored_error);
  LOG(INFO) << "Message sent";
  return 0;
}
