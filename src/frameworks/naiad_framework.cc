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

#include "frameworks/naiad_framework.h"

#include <algorithm>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#define NAIAD_START_TIME 3.0

namespace musketeer {
namespace framework {

  using musketeer::translator::TranslatorNaiad;

  NaiadFramework::NaiadFramework(): FrameworkInterface() {
    dispatcher_ = new NaiadDispatcher();
  }

  string NaiadFramework::Translate(const op_nodes& dag,
                                   const string& relation) {
    TranslatorNaiad translator_naiad =
      translator::TranslatorNaiad(dag, relation);
    return translator_naiad.GenerateCode();
  }

  void NaiadFramework::Dispatch(const string& binary_file,
                                const string& relation) {
    if (!FLAGS_dry_run) {
      dispatcher_->Execute(binary_file, relation);
    }
  }

  uint32_t NaiadFramework::ScoreDAG(const node_list& nodes,
                                    const relation_size& rel_size) {
    node_set to_schedule;
    int32_t num_ops_to_schedule = 0;
    op_nodes input_nodes;
    // At the moment we only support one input DAGs in Hadoop.
    input_nodes.push_back(nodes.front());
    for (node_list::const_iterator it = nodes.begin(); it != nodes.end();
         ++it) {
      to_schedule.insert(*it);
      num_ops_to_schedule++;
    }
    if (CanMerge(input_nodes, to_schedule, num_ops_to_schedule)) {
      VLOG(2) << "Can merge in " << FrameworkToString(GetType());
      set<string> input_names;
      uint64_t input_data_size = GetDataSize(
          *DetermineInputs(input_nodes, &input_names), rel_size);
      uint64_t output_data_size =
        GetDataSize(DetermineFinalOutputs(input_nodes, nodes), rel_size);
      double time_compile_read_write = ScoreCompile() +
        ScoreLoad(input_data_size) + ScorePush(output_data_size);
      shared_ptr<OperatorNode> while_op = IsInWhileBody(nodes);
      if (while_op) {
        time_compile_read_write *=
          while_op->get_operator()->get_condition_tree()->getNumIterations();
      }
      if (FLAGS_best_runtime) {
        return min(static_cast<double>(FLAGS_max_scheduler_cost),
                   time_compile_read_write +
                   ScoreRuntime(input_data_size, nodes, rel_size));
      } else {
        return min(static_cast<double>(FLAGS_max_scheduler_cost),
                   (time_compile_read_write +
                    ScoreRuntime(input_data_size, nodes, rel_size)) *
                   FLAGS_naiad_num_workers);
      }
    } else {
      VLOG(2) << "Cannot merge in " << FrameworkToString(GetType());
      return numeric_limits<uint32_t>::max();
    }
  }

  bool NaiadFramework::CanMerge(const op_nodes& dag,
                                const node_set& to_schedule,
                                int32_t num_ops_to_schedule) {
    // Check if the entire while is scheduled in Spark.
    for (node_set::const_iterator it = to_schedule.begin();
         it != to_schedule.end(); ++it) {
      if ((*it)->get_operator()->get_type() == WHILE_OP) {
        if (to_schedule.size() == 1) {
          // We can schedule the WHILE op by itself.
          return true;
        }
        // Check that all the nodes in the body of the while are present.
        op_nodes children = (*it)->get_loop_children();
        if (!CheckChildrenInSet(children, to_schedule)) {
          return false;
        }
      }
    }
    return true;
  }

  FmwType NaiadFramework::GetType() {
    return FMW_NAIAD;
  }

  double NaiadFramework::ScoreCompile() {
    return 10.0 * FLAGS_time_to_cost;
  }

  double NaiadFramework::ScorePull(uint64_t data_size_kb) {
    // We pull from HDFS at about 10MB/s.
    return data_size_kb / FLAGS_naiad_num_workers / 10.0 / 1024.0 * FLAGS_time_to_cost;
  }

  double NaiadFramework::ScoreLoad(uint64_t data_size_kb) {
    // We load from local disk at about 18MB/s per worker.
    return NAIAD_START_TIME + data_size_kb / FLAGS_naiad_num_workers /
      18.0 / 1024.0 * FLAGS_time_to_cost;
  }

  double NaiadFramework::ScoreRuntime(uint64_t data_size_kb,
                                      const node_list& nodes,
                                      const relation_size& rel_size) {
    // data_size_kb is not used because we already account for it in
    // the PULL and LOAD scores.
    double cost = 0;
    // TODO(ionel): This doesn't reconsider state reuse yet/caching.
    int num_iterations_factor = 1;
    for (node_list::const_iterator it = nodes.begin(); it != nodes.end();
         ++it) {
      double op_cost = 0;
      if ((*it)->get_operator()->get_type() == WHILE_OP) {
        if (nodes.size() == 1) {
          op_cost = ScoreOperator(*it, rel_size);
        } else {
          num_iterations_factor =
            (*it)->get_operator()->get_condition_tree()->getNumIterations();
        }
      } else {
        op_cost = ScoreOperator((*it), rel_size);
      }
      // TODO(ionel): We may end up scaling operators that are not part of
      // the while body. FIX!
      cost += num_iterations_factor * op_cost;
    }
    return cost;
  }

  double NaiadFramework::ScorePush(uint64_t data_size_kb) {
    // We push to HDFS at about 25MB/s.
    return data_size_kb / FLAGS_naiad_num_workers / 25.0 / 1024.0 * FLAGS_time_to_cost;
  }

  double NaiadFramework::ScoreClusterState() {
    // TODO(ionel): IMPLEMENT!
    return 0.0;
  }

  double NaiadFramework::ScoreGroupByOperator(OperatorInterface* op,
                                              const relation_size& rel_size) {
    string input_rel = op->get_relations()[0]->get_name();
    map<string, pair<uint64_t, uint64_t> >::const_iterator it =
      rel_size.find(input_rel);
    if (it == rel_size.end()) {
      LOG(ERROR) << "Unknown relation size for: " << input_rel;
      return FLAGS_max_scheduler_cost;
    }
    uint64_t data_size = it->second.second;
    return data_size / FLAGS_naiad_num_workers / 40.0 / 1024.0 * FLAGS_time_to_cost;
  }

  double NaiadFramework::ScoreScanOperator(OperatorInterface* op,
                                           const relation_size& rel_size) {
    string input_rel = op->get_relations()[0]->get_name();
    map<string, pair<uint64_t, uint64_t> >::const_iterator it =
      rel_size.find(input_rel);
    if (it == rel_size.end()) {
      LOG(ERROR) << "Unknown relation size for: " << input_rel;
      return FLAGS_max_scheduler_cost;
    }
    uint64_t data_size = it->second.second;
    return data_size / FLAGS_naiad_num_workers / 100.0 / 1024.0 * FLAGS_time_to_cost;
  }

  double NaiadFramework::ScoreJoin(OperatorInterface* op,
                                   const relation_size& rel_size) {
    vector<Relation*> rels = op->get_relations();
    string left_input = rels[0]->get_name();
    string right_input = rels[1]->get_name();
    map<string, pair<uint64_t, uint64_t> >::const_iterator it =
      rel_size.find(left_input);
    if (it == rel_size.end()) {
      LOG(ERROR) << "Unknown relation size for: " << left_input;
      return FLAGS_max_scheduler_cost;
    }
    uint64_t left_size = it->second.second;
    it = rel_size.find(right_input);
    if (it == rel_size.end()) {
      LOG(ERROR) << "Unknown relation size for: " << right_input;
      return FLAGS_max_scheduler_cost;
    }
    uint64_t right_size = it->second.second;
    return SumNoOverflow(left_size, right_size) / FLAGS_naiad_num_workers /
      50.0 / 1024.0 * FLAGS_time_to_cost;
  }

  double NaiadFramework::ScoreOperator(shared_ptr<OperatorNode> op_node,
                                       const relation_size& rel_size) {
    OperatorInterface* op = op_node->get_operator();
    switch (op->get_type()) {
    case AGG_OP: {
      return ScoreGroupByOperator(op, rel_size);
    }
    case COUNT_OP: {
      return ScoreGroupByOperator(op, rel_size);
    }
    case DIFFERENCE_OP: {
      return ScoreScanOperator(op, rel_size);
    }
    case DISTINCT_OP: {
      return ScoreGroupByOperator(op, rel_size);
    }
    case DIV_OP: {
      return ScoreScanOperator(op, rel_size);
    }
    case INTERSECTION_OP: {
      return ScoreJoin(op, rel_size);
    }
    case JOIN_OP: {
      return ScoreJoin(op, rel_size);
    }
    case MAX_OP: {
      return ScoreGroupByOperator(op, rel_size);
    }
    case MIN_OP: {
      return ScoreGroupByOperator(op, rel_size);
    }
    case MUL_OP: {
      return ScoreGroupByOperator(op, rel_size);
    }
    case PROJECT_OP: {
      return ScoreScanOperator(op, rel_size);
    }
    case SELECT_OP: {
      return ScoreScanOperator(op, rel_size);
    }
    case SORT_OP: {
      // Not supported yet.
      return FLAGS_max_scheduler_cost;
    }
    case SUB_OP: {
      return ScoreScanOperator(op, rel_size);
    }
    case SUM_OP: {
      return ScoreScanOperator(op, rel_size);
    }
    case UDF_OP: {
      return 0.0;
    }
    case UNION_OP: {
      return ScoreScanOperator(op, rel_size);
    }
    case WHILE_OP: {
      int num_iterations = op->get_condition_tree()->getNumIterations();
      uint64_t input_data_size_kb = GetInputSizeOfWhile(op_node, rel_size);
      uint64_t output_data_size_kb = GetOutputSizeOfWhile(op_node, rel_size);
      double cost_per_iteration = ScorePull(input_data_size_kb) +
        ScoreLoad(input_data_size_kb) + ScorePush(output_data_size_kb);
      return num_iterations * cost_per_iteration;
    }
    default: {
      LOG(ERROR) << "Unexpected operator type: " << op->get_type();
      return FLAGS_max_scheduler_cost;
    }
    }
  }

} // namespace framework
} // namespace musketeer
