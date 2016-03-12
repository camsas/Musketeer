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

#include "scheduling/scheduler_interface.h"

namespace musketeer {
namespace scheduling {

  void SchedulerInterface::AddJobToQueue(Job* job) {
    LOG(INFO) << "Add Job to Queue";
    boost::mutex::scoped_lock lock(mutexQ_);
    queuedJobs_.push_back(job);
    condQ_.notify_one();
  }

  Job* SchedulerInterface::GetJobFromQueue() {
    LOG(INFO) << "GetJobFromQueue";
    boost::mutex::scoped_lock lock(mutexQ_);
    while (queuedJobs_.size() == 0) {
      LOG(INFO) << "No more jobs to schedule";
      LOG(INFO) << "Sleeping";
      condQ_.wait(lock);
    }
    Job* job =  queuedJobs_.front();
    LOG(INFO) << "Queue Size " << queuedJobs_.size();
    LOG(INFO) << job->operator_merge();
    LOG(INFO) << job->code().c_str();
    queuedJobs_.pop_front();
    return job;
  }

} // namespace scheduling
} // namespace musketeer
