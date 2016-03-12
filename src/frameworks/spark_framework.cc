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

#include "frameworks/spark_framework.h"

#include <algorithm>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#define SPARK_START_TIME 3.0
#define NUM_SPARK_MACHINES 100

namespace musketeer {
namespace framework {

  using musketeer::translator::TranslatorSpark;
  using musketeer::monitor::SparkMonitor;

  SparkFramework::SparkFramework(): FrameworkInterface() {
    dispatcher_ = new SparkDispatcher();
    monitor_ = new SparkMonitor();
  }

  string SparkFramework::Translate(const op_nodes& dag,
                                   const string& relation) {
    TranslatorSpark translator_spark =
      translator::TranslatorSpark(dag, relation);
    return translator_spark.GenerateCode();
  }

  void SparkFramework::Dispatch(const string& binary_file,
                                const string& relation) {
    if (!FLAGS_dry_run) {
      dispatcher_->Execute(binary_file, relation);
    }
  }

  FmwType SparkFramework::GetType() {
    return FMW_SPARK;
  }

  double SparkFramework::ScoreMapOperator(OperatorInterface* op,
                                          const relation_size& rel_size) {
    string input_rel = op->get_relations()[0]->get_name();
    map<string, pair<uint64_t, uint64_t> >::const_iterator it =
      rel_size.find(input_rel);
    if (it == rel_size.end()) {
      LOG(ERROR) << "Unknown relation size for: " << input_rel;
      return FLAGS_max_scheduler_cost;
    }
    uint64_t data_size = it->second.second;
    // TODO(ionel): Update cost fuction to consider where clause.
    double time = data_size / NUM_SPARK_MACHINES / 80.0 / 1024.0;
    return time * FLAGS_time_to_cost;
  }

  double SparkFramework::ScoreJoinOperator(OperatorInterface* op,
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
    double time = SumNoOverflow(left_size, right_size) /
      NUM_SPARK_MACHINES / 35.0 / 1024.0;
    return time * FLAGS_time_to_cost;
  }

  double SparkFramework::ScoreMaxMinOperator(OperatorInterface* op,
                                             const relation_size& rel_size) {
    string input_rel = op->get_relations()[0]->get_name();
    map<string, pair<uint64_t, uint64_t> >::const_iterator it =
      rel_size.find(input_rel);
    if (it == rel_size.end()) {
      LOG(ERROR) << "Unknown relation size for: " << input_rel;
      return FLAGS_max_scheduler_cost;
    }
    uint64_t data_size = it->second.second;
    // TODO(ionel): This depends a lot on the group by clause.
    double time = data_size / NUM_SPARK_MACHINES / 35.0 / 1024.0;
    return time * FLAGS_time_to_cost;
  }

  double SparkFramework::ScoreOperator(shared_ptr<OperatorNode> op_node,
                                       const relation_size& rel_size) {
    OperatorInterface* op = op_node->get_operator();
    switch (op->get_type()) {
    case AGG_OP: {
      string input_rel = op->get_relations()[0]->get_name();
      map<string, pair<uint64_t, uint64_t> >::const_iterator it =
        rel_size.find(input_rel);
      if (it == rel_size.end()) {
        LOG(ERROR) << "Unknown relation size for: " << input_rel;
        return FLAGS_max_scheduler_cost;
      }
      uint64_t data_size = it->second.second;
      double time;
      if (op->hasGroupby()) {
        time = data_size / NUM_SPARK_MACHINES / 35.0 / 1024.0;
      } else {
        time = data_size / NUM_SPARK_MACHINES / 80.0 / 1024.0;
      }
      return time * FLAGS_time_to_cost;
    }
    case BLACK_BOX_OP: {
      return FLAGS_max_scheduler_cost;
    }
    case COUNT_OP: {
      string input_rel = op->get_relations()[0]->get_name();
      map<string, pair<uint64_t, uint64_t> >::const_iterator it =
        rel_size.find(input_rel);
      if (it == rel_size.end()) {
        LOG(ERROR) << "Unknown relation size for: " << input_rel;
        return FLAGS_max_scheduler_cost;
      }
      uint64_t data_size = it->second.second;
      double time;
      if (op->hasGroupby()) {
        time = data_size / NUM_SPARK_MACHINES / 35.0 / 1024.0;
      } else {
        time = data_size / NUM_SPARK_MACHINES / 80.0 / 1024.0;
      }
      return time * FLAGS_time_to_cost;
    }
    case DIFFERENCE_OP: {
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
      double time = SumNoOverflow(left_size, right_size) /
        NUM_SPARK_MACHINES / 35.0 / 1024.0;
      return time * FLAGS_time_to_cost;
    }
    case DISTINCT_OP: {
      // TODO(ionel): Refine value.
      string input_rel = op->get_relations()[0]->get_name();
      map<string, pair<uint64_t, uint64_t> >::const_iterator it =
        rel_size.find(input_rel);
      if (it == rel_size.end()) {
        LOG(ERROR) << "Unknown relation size for: " << input_rel;
        return FLAGS_max_scheduler_cost;
      }
      uint64_t data_size = it->second.second;
      double time = data_size / NUM_SPARK_MACHINES / 35.0 / 1024.0;
      return time * FLAGS_time_to_cost;
    }
    case DIV_OP: {
      return ScoreMapOperator(op, rel_size);
    }
    case INTERSECTION_OP: {
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
      double time = SumNoOverflow(left_size, right_size) /
        NUM_SPARK_MACHINES / 10.0 / 1024.0;
      return time * FLAGS_time_to_cost;
    }
    case JOIN_OP: {
      return ScoreJoinOperator(op, rel_size);
    }
    case MAX_OP: {
      return ScoreMaxMinOperator(op, rel_size);
    }
    case MIN_OP: {
      return ScoreMaxMinOperator(op, rel_size);
    }
    case MUL_OP: {
      return ScoreMapOperator(op, rel_size);
    }
    case PROJECT_OP: {
      return ScoreMapOperator(op, rel_size);
    }
    case SELECT_OP: {
      return ScoreMapOperator(op, rel_size);
    }
    case SORT_OP: {
      string input_rel = op->get_relations()[0]->get_name();
      map<string, pair<uint64_t, uint64_t> >::const_iterator it =
        rel_size.find(input_rel);
      if (it == rel_size.end()) {
        LOG(ERROR) << "Unknown relation size for: " << input_rel;
        return FLAGS_max_scheduler_cost;
      }
      uint64_t data_size = it->second.second;
      // TODO(ionel): Check why this value is so big!!!!
      double time = data_size / NUM_SPARK_MACHINES / 2.0 / 1024.0;
      return time * FLAGS_time_to_cost;
    }
    case SUB_OP: {
      return ScoreMapOperator(op, rel_size);
    }
    case SUM_OP: {
      return ScoreMapOperator(op, rel_size);
    }
    case UDF_OP: {
      return FLAGS_max_scheduler_cost;
    }
    case UNION_OP: {
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
      double time = SumNoOverflow(left_size, right_size) /
        NUM_SPARK_MACHINES / 60.0 / 1024.0;
      return time * FLAGS_time_to_cost;
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

  // In our current setup Spark always uses the entire cluster.
  // TODO(ionel): Fix!
  double SparkFramework::ScoreClusterState() {
    return monitor_->CurrentUtilization();
  }

  uint32_t SparkFramework::ScoreDAG(const node_list& nodes,
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
                   NUM_SPARK_MACHINES);
      }
    } else {
      VLOG(2) << "Cannot merge in " << FrameworkToString(GetType());
      return numeric_limits<uint32_t>::max();
    }
  }

  // Spark compile time takes around 20s.
  double SparkFramework::ScoreCompile() {
    return 18.0 * FLAGS_time_to_cost;
  }

  double SparkFramework::ScorePull(uint64_t data_size_kb) {
    // We can't approximate this value for Spark. It is included
    // in the cost of loading an operator.
    return 0.0;
  }

  double SparkFramework::ScoreLoad(uint64_t data_size_kb) {
    // Can read at about 50MB/s per machine.
    return SPARK_START_TIME + data_size_kb / NUM_SPARK_MACHINES / 50.0 / 1024.0;
  }

  double SparkFramework::ScoreRuntime(uint64_t data_size_kb,
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
        op_cost = ScoreOperator(*it, rel_size);
      }
      // TODO(ionel): We may end up scaling operators that are not part of
      // the while body. FIX!
      cost += num_iterations_factor * op_cost;
    }
    return cost;
  }

  double SparkFramework::ScorePush(uint64_t data_size_kb) {
    // Can write at about 20MB/s per machine.
    return data_size_kb / NUM_SPARK_MACHINES / 20.0 / 1024.0;
  }

  bool SparkFramework::CanMerge(const op_nodes& dag,
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

} // namespace framework
} // namespace musketeer
