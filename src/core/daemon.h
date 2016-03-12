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

#ifndef MUSKETEER_DAEMON_H
#define MUSKETEER_DAEMON_H

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>

#include "base/common.h"
#include "core/daemon_connection.h"

namespace musketeer {
namespace core {

class Daemon {
 public:
  Daemon(boost::asio::io_service* io_service, int port,
         SchedulerInterface* scheduler);

 private:
  void startAccept();
  void handleAccept(shared_ptr<DaemonConnection> new_connection,
                    const boost::system::error_code& error);

  boost::asio::ip::tcp::acceptor acceptor_;
  SchedulerInterface* scheduler_;
};

} // namespace core
} // namespace musketeer
#endif
