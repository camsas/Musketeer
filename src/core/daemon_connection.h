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

#ifndef MUSKETEER_DAEMON_CONNECTION_H
#define MUSKETEER_DAEMON_CONNECTION_H

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>

#include <string>

#include "base/common.h"
#include "base/job.pb.h"
#include "scheduling/operator_scheduler.h"

namespace musketeer {
namespace core {

using musketeer::scheduling::SchedulerInterface;

class DaemonConnection :
  public boost::enable_shared_from_this<DaemonConnection> {
 public:
  static shared_ptr<DaemonConnection> create(
      boost::asio::io_service* io_service, SchedulerInterface* scheduler) {
      return shared_ptr<DaemonConnection> (new DaemonConnection(io_service,
                                                                scheduler));
  }

  boost::asio::ip::tcp::socket& socket() {
    return *socket_;
  }

  void readJob();

 private:
  DaemonConnection(boost::asio::io_service* io_service,
                   SchedulerInterface* scheduler)
    : scheduler_(scheduler),
    socket_(new boost::asio::ip::tcp::socket(*io_service)) {
  }

  void handleRead(const boost::system::error_code& error, char* message);

  SchedulerInterface* scheduler_;
  boost::asio::ip::tcp::socket* socket_;
  std::string message_;
};

} // namespace core
} // namespace musketeer
#endif
