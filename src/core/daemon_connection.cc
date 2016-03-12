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

#include "core/daemon_connection.h"

namespace musketeer {
namespace core {

  void DaemonConnection::readJob() {
    LOG(INFO) << "Read Job";
    char* message_ = reinterpret_cast<char*>(malloc(sizeof(char) * 1024));
    boost::asio::async_read(*socket_, boost::asio::buffer(message_, 1024),
                     bind(&DaemonConnection::handleRead, shared_from_this(),
                          boost::asio::placeholders::error, message_));
  }

  void DaemonConnection::handleRead(const boost::system::error_code& error,
                                    char* message) {
    LOG(INFO) << "Handle Read";
    Job* job =  new Job();
    job->ParseFromString(string(message));
    scheduler_->AddJobToQueue(job);
  }

} // namespace core
} // namespace musketeer
