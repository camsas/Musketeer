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

#ifndef MUSKETEER_OPERATOR_SCHEDULER_H
#define MUSKETEER_OPERATOR_SCHEDULER_H

#include "scheduling/scheduler_interface.h"

#include <stdint.h>

#include <map>
#include <string>

#include "base/job.pb.h"
#include "base/utils.h"
#include "frameworks/framework_interface.h"
#include "frameworks/graphchi_framework.h"
#include "frameworks/hadoop_framework.h"
#include "frameworks/metis_framework.h"
#include "frameworks/naiad_framework.h"
#include "frameworks/powergraph_framework.h"
#include "frameworks/spark_framework.h"
#include "frontends/operator_node.h"
#include "ir/black_box_operator.h"
#include "ir/operator_interface.h"
#include "RLPlusLexer.h"
#include "RLPlusParser.h"

namespace musketeer {
namespace scheduling {

using musketeer::framework::FrameworkInterface;

class OperatorScheduler: public SchedulerInterface {
 public:
  explicit OperatorScheduler(const map<string, FrameworkInterface* >& fmws);

  void Schedule(shared_ptr<OperatorNode> node);
  void ScheduleAll(shared_ptr<OperatorNode> node);
  void ScheduleDAG(const op_nodes& dag);
  void DynamicScheduleDAG(const op_nodes& dag);
  void addJobToQueue(Job* job);
  Job* getJobFromQueue();

 private:
  void MoveOutputFromTmp(const string& output_relation,
                         OperatorInterface* op);
};

} // namespace scheduling
} // namespace musketeer
#endif
