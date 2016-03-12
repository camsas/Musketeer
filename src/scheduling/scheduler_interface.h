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

#ifndef MUSKETEER_SCHEDULER_INTERFACE_H
#define MUSKETEER_SCHEDULER_INTERFACE_H

#include <boost/bind.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <stdlib.h>

#include <deque>
#include <map>
#include <string>
#include <vector>

#include "base/common.h"
#include "base/flags.h"
#include "base/job.pb.h"
#include "frameworks/framework_interface.h"
#include "frontends/operator_node.h"

namespace musketeer {
namespace scheduling {

using musketeer::framework::FrameworkInterface;

class SchedulerInterface {
 public:
  explicit SchedulerInterface(const map<string, FrameworkInterface*>& fmws_): fmws(fmws_) {
  }

  virtual void ScheduleDAG(const op_nodes& dag) = 0;
  virtual void DynamicScheduleDAG(const op_nodes& dag) = 0;
  void AddJobToQueue(Job* job);
  Job* GetJobFromQueue();

 protected:
  const map<string, musketeer::framework::FrameworkInterface*>& fmws;

 private:
  deque<Job*> queuedJobs_;
  boost::mutex mutexQ_;
  boost::condition_variable condQ_;
};

} // namespace scheduling
} // namespace musketeer
#endif
