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

#ifndef MUSKETEER_SCHEDULER_DYNAMIC_H
#define MUSKETEER_SCHEDULER_DYNAMIC_H

#include "scheduling/scheduler_interface.h"

#include <boost/shared_ptr.hpp>

#include <iostream>
#include <list>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/common.h"
#include "base/job.pb.h"
#include "base/utils.h"
#include "core/history_storage.h"
#include "frameworks/framework_interface.h"
#include "frameworks/graphchi_framework.h"
#include "frameworks/hadoop_framework.h"
#include "frameworks/metis_framework.h"
#include "frameworks/naiad_framework.h"
#include "frameworks/powergraph_framework.h"
#include "frameworks/powerlyra_framework.h"
#include "frameworks/spark_framework.h"
#include "frameworks/wildcherry_framework.h"
#include "frontends/operator_node.h"
#include "scheduling/scheduler_simulator.h"

namespace musketeer {
namespace scheduling {

using musketeer::framework::FrameworkInterface;
using musketeer::core::HistoryStorage;

typedef list<pair<op_nodes, FmwType> > bindings_lt;
typedef vector<pair<op_nodes, FmwType> > bindings_vt;
typedef set<shared_ptr<OperatorNode> > node_set;
typedef queue<shared_ptr<OperatorNode> > node_queue;

class SchedulerDynamic : public SchedulerInterface {
 public:
  SchedulerDynamic(const map<string, FrameworkInterface*>& fmws,
                   HistoryStorage* history)
    : SchedulerInterface(fmws), history_(history),
    rel_size_(new map<string, pair<uint64_t, uint64_t> >) {
    if (FLAGS_dry_run && FLAGS_dry_run_data_size_file.compare("")) {
      scheduler_simulator_.ReadDataSizeFile();
    }
  }

  void DynamicScheduleDAG(const op_nodes& dag);
  void ScheduleDAG(const op_nodes& dag);
  void TopologicalOrder(const op_nodes& dag, op_nodes* order);
  bindings_lt ComputeOptimal(const op_nodes& serial_dag);
  bindings_lt ComputeHeuristic(const op_nodes& serial_dag);

 private:
  void TopologicalOrderInternal(shared_ptr<OperatorNode> node,
      op_nodes* result, set<shared_ptr<OperatorNode> >* visited);
  void ConstructSubDAGWhile(shared_ptr<OperatorNode> cur_node,
                            const node_set& nodes, node_set* visited,
                            node_queue* to_visit);
  void ConstructSubDAGChildren(shared_ptr<OperatorNode> cur_node,
                               const node_set& nodes, node_set* visited,
                               node_queue* to_visit);
  void ConstructSubDAGChildren();
  op_nodes ConstructSubDAG(const op_nodes& ordered_nodes);
  void ClearBarriers(const op_nodes& ordered_nodes);
  string CheckForceFmwFlag(FmwType fmw);
  bindings_vt::size_type DetermineWhileBoundary(
      shared_ptr<OperatorNode> while_node, const bindings_vt& bindings,
      bindings_vt::size_type index);
  op_nodes::size_type DetermineWhileBoundary(
      shared_ptr<OperatorNode> while_node, const op_nodes& nodes,
      op_nodes::size_type index);
  void AvoidInputOverwrite(const op_nodes& ordered_nodes);
  void ReplaceWithTmp(const op_nodes& nodes);
  bool CheckInputRelOverwrite(OperatorInterface* op);
  string SwapRel(const op_nodes& binding);
  vector<pair<string, uint64_t> > DetermineInputsSize(const op_nodes& dag);
  op_nodes::size_type DynamicScheduleWhileBody(const op_nodes& nodes,
                                               const op_nodes& order,
                                               uint64_t* num_op_executed);
  bindings_vt::size_type ScheduleWhileBody(
      const op_nodes& nodes, const bindings_vt& bindings, bindings_vt::size_type index);
  void PopulateHistory(const op_nodes& nodes, const string& relation,
                       const string& framework, uint64_t run_time);
  void DispatchWithHistory(
      pair<op_nodes, FmwType> bind, const op_nodes& nodes, const string& relation);
  bindings_vt ScheduleNetflix(op_nodes order, FmwType fmw_type);
  bindings_vt SchedulePageRank(op_nodes order, FmwType fmw_type);
  bindings_vt SchedulePageRankHad(op_nodes order);
  bindings_vt ScheduleShopper(op_nodes order, FmwType fmw_type);
  bindings_vt ScheduleTriangle(op_nodes order, FmwType fmw_type);
  bindings_vt ScheduleSSSP(op_nodes order, FmwType fmw_type);
  bindings_vt ScheduleTPC(op_nodes order, FmwType fmw_type);

  void RefreshOutputSize(const op_nodes& nodes);
  bindings_lt BindOperators(const op_nodes& order);

  HistoryStorage* history_;
  map<string, pair<uint64_t, uint64_t> >* rel_size_;
  SchedulerSimulator scheduler_simulator_;
};

} // namespace scheduling
} // namespace musketeer
#endif
