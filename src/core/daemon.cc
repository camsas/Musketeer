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

#include "core/daemon.h"

namespace musketeer {
namespace core {

  Daemon::Daemon(boost::asio::io_service* io_service, int port,
                 SchedulerInterface* scheduler):
    acceptor_(*io_service, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port)),
    scheduler_(scheduler) {
    LOG(INFO) << "Creating Daemon";
    startAccept();
  }

  void Daemon::startAccept() {
    LOG(INFO) << "startAccept()";
    shared_ptr<DaemonConnection> newConnection =
      DaemonConnection::create(&acceptor_.get_io_service(), scheduler_);
    LOG(INFO) << "New Connection Created ";
    acceptor_.async_accept(newConnection->socket(),
                           boost::bind(&Daemon::handleAccept, this,
                                       newConnection,
                                       boost::asio::placeholders::error));
    LOG(INFO) << "Accepted";
  }

  void Daemon::handleAccept(shared_ptr<DaemonConnection>  new_connection,
                            const boost::system::error_code& error) {
    LOG(INFO) << "handleAccept()";
    if (!error) {
      new_connection->readJob();
    }
    startAccept();
  }

} // namespace core
} // namespace musketeer
