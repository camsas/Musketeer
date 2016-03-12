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

#include "frameworks/metis_framework.h"

#include <algorithm>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace musketeer {
namespace framework {

  using musketeer::translator::TranslatorMetis;

  MetisFramework::MetisFramework(): FrameworkInterface() {
    dispatcher_ = new MetisDispatcher();
    monitor_ = NULL;
  }

  string MetisFramework::Translate(const op_nodes& dag,
                                   const string& relation) {
    TranslatorMetis translator_metis = TranslatorMetis(dag, relation);
    string binary_file = translator_metis.GenerateCode();
    return binary_file;
  }

  void MetisFramework::Dispatch(const string& binary_file,
                                const string& relation) {
    if (!FLAGS_dry_run) {
      dispatcher_->Execute(binary_file, relation);
    }
  }

  FmwType MetisFramework::GetType() {
    return FMW_METIS;
  }

  double MetisFramework::ScoreMapOnly(OperatorInterface* op,
                                      const relation_size& rel_size) {
    string input_rel = op->get_relations()[0]->get_name();
    map<string, pair<uint64_t, uint64_t> >::const_iterator it =
      rel_size.find(input_rel);
    if (it == rel_size.end()) {
      LOG(ERROR) << "Unknown relation size for: " << input_rel;
      return FLAGS_max_scheduler_cost;
    }
    uint64_t data_size = it->second.second;
    // TODO(ionel): Take into account machine type.
    double time = data_size / 93.0 / 1024.0;
    return time * FLAGS_time_to_cost;
  }

  double MetisFramework::ScoreMapRedNoGroup(OperatorInterface* op,
                                            const relation_size& rel_size) {
    string input_rel = op->get_relations()[0]->get_name();
    map<string, pair<uint64_t, uint64_t> >::const_iterator it =
      rel_size.find(input_rel);
    if (it == rel_size.end()) {
      LOG(ERROR) << "Unknown relation size for: " << input_rel;
      return FLAGS_max_scheduler_cost;
    }
    //    uint64_t data_size = it->second.second;
    // TODO(ionel): Plugin values.
    return FLAGS_max_scheduler_cost;
  }

  double MetisFramework::ScoreMapRedGroup(OperatorInterface* op,
                                          const relation_size& rel_size) {
    string input_rel = op->get_relations()[0]->get_name();
    map<string, pair<uint64_t, uint64_t> >::const_iterator it =
      rel_size.find(input_rel);
    if (it == rel_size.end()) {
      LOG(ERROR) << "Unknown relation size for: " << input_rel;
      return FLAGS_max_scheduler_cost;
    }
    //    uint64_t data_size = it->second.second;
    // TODO(ionel): Plugin values.
    return FLAGS_max_scheduler_cost;
  }

  double MetisFramework::ScoreMapRedTwoInputs(OperatorInterface* op,
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
    //    uint64_t left_data_size = it->second.second;
    it = rel_size.find(right_input);
    if (it == rel_size.end()) {
      LOG(ERROR) << "Unknown relation size for: " << right_input;
      return FLAGS_max_scheduler_cost;
    }
    //    uint64_t right_data_size = it->second.second;
    // TODO(ionel): Plugin values.
    return FLAGS_max_scheduler_cost;
  }

  double MetisFramework::ScoreJoin(OperatorInterface* op,
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
    //    uint64_t left_data_size = it->second.second;
    it = rel_size.find(right_input);
    if (it == rel_size.end()) {
      LOG(ERROR) << "Unknown relation size for: " << right_input;
      return FLAGS_max_scheduler_cost;
    }
    //    uint64_t right_data_size = it->second.second;
    // TODO(ionel): Plugin values.
    return FLAGS_max_scheduler_cost;
  }

  double MetisFramework::ScoreOperator(shared_ptr<OperatorNode> op_node,
                                       const relation_size& rel_size) {
    OperatorInterface* op = op_node->get_operator();
    if (op->get_type() == BLACK_BOX_OP || op->get_type() == UDF_OP) {
      return FLAGS_max_scheduler_cost;
    }
    if (op->mapOnly()) {
      return ScoreMapOnly(op, rel_size);
    }
    if (op->get_type() == JOIN_OP) {
      return ScoreJoin(op, rel_size);
    }
    if (op->get_type() == INTERSECTION_OP ||
        op->get_type() == DIFFERENCE_OP) {
      return ScoreMapRedTwoInputs(op, rel_size);
    }
    if (op->hasGroupby()) {
      return ScoreMapRedGroup(op, rel_size);
    } else {
      return ScoreMapRedNoGroup(op, rel_size);
    }
  }

  // The framework runs on a single machine => cluster state of 0.
  double MetisFramework::ScoreClusterState() {
    return 0.0;
  }

  // Compile time for a Metis job is ~1s.
  double MetisFramework::ScoreCompile() {
    return 1.0 * FLAGS_time_to_cost;
  }

  // This function is not used in the Metis case because an extra arguments
  // is needed.
  double MetisFramework::ScorePull(uint64_t data_size_kb) {
    return 0.0 * FLAGS_time_to_cost;
  }

  double MetisFramework::ScorePull(uint64_t data_size_kb, bool two_inputs) {
    // This represents the expected duration of the phase in seconds.
    if (two_inputs) {
      return data_size_kb / 25.0 / 1024.0 * FLAGS_time_to_cost;
    } else {
      return data_size_kb / 50.0 / 1024.0 * FLAGS_time_to_cost;
    }
  }

  // The load time is so low due to buffer cache and to the way nmap works.
  // (i.e. the pages are brought into memory when needed).
  double MetisFramework::ScoreLoad(uint64_t data_size_kb) {
    return 0.0 * FLAGS_time_to_cost;
  }

  double MetisFramework::ScoreRuntime(uint64_t data_size_kb,
                                      const node_list& nodes,
                                      const relation_size& rel_size) {
    // data_size_kb is not used because we already account for it in
    // the PULL and LOAD scores.
    double max_cost = 0;
    for (node_list::const_iterator it = nodes.begin(); it != nodes.end();
         ++it) {
      double op_cost = ScoreOperator((*it), rel_size);
      if (op_cost > max_cost) {
        max_cost = op_cost;
      }
    }
    // TODO(ionel): Approximate the overhead of running more than one operator.
    return max_cost * FLAGS_time_to_cost;
  }

  double MetisFramework::ScorePush(uint64_t data_size_kb) {
    // This represents the expected duration of the phase in seconds.
    return data_size_kb / 50.0 / 1024.0 * FLAGS_time_to_cost;
  }

  uint32_t MetisFramework::ScoreDAG(const node_list& nodes,
                                    const relation_size& rel_size) {
    node_set to_schedule;
    op_nodes input_nodes;
    int32_t num_ops_to_schedule = 0;
    // At the moment we only support one input DAGs in Metis.
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
      // TODO(ionel): FIX! ScorePush(output_data_size);
      return min(static_cast<double>(FLAGS_max_scheduler_cost),
                 ScoreCompile() + ScorePull(input_data_size) +
                 ScoreLoad(input_data_size) +
                 ScoreRuntime(input_data_size, nodes, rel_size) +
                 ScorePush(output_data_size));
    } else {
      VLOG(2) << "Cannot merge in " << FrameworkToString(GetType());
      return numeric_limits<uint32_t>::max();
    }
  }

  bool MetisFramework::CanMerge(const op_nodes& dag,
                                const node_set& to_schedule,
                                int32_t num_ops_to_schedule) {
    set<string> visited;
    // Can't merge DAGs that have more than 1 operator as input.
    if (dag.size() != 1) {
      return false;
    }
    bool got_reduce = false;
    shared_ptr<OperatorNode> cur_node = dag[0];
    op_nodes children = cur_node->get_children();
    for (; ; children = cur_node->get_children()) {
      // UDFs are currently non-mergeable.
      // WHILE is not mergeable in our current implementation.
      if (to_schedule.find(cur_node) == to_schedule.end()) {
        // The node is not part of the subDAG to be scheduled.
        return false;
      }
      visited.insert(
          cur_node->get_operator()->get_output_relation()->get_name());
      // Decrease number of operators to be visited.
      num_ops_to_schedule--;
      if (cur_node->get_operator()->get_type() == BLACK_BOX_OP ||
          cur_node->get_operator()->get_type() == UDF_OP ||
          cur_node->get_operator()->get_type() == WHILE_OP) {
        return false;
      }
      if (HasReduce(cur_node->get_operator())) {
        if (got_reduce) {
          // We can't merge two operators that need a reduce function.
          return false;
        } else {
          got_reduce = true;
        }
      }
      uint16_t num_children_to_schedule = 0;
      for (op_nodes::iterator c_it = children.begin(); c_it != children.end();
           ++c_it) {
        if (to_schedule.find(*c_it) != to_schedule.end()) {
          cur_node = *c_it;
          num_children_to_schedule++;
        }
      }
      if (num_children_to_schedule == 0) {
        break;
      } else if (num_children_to_schedule > 1) {
        // We can't merge nodes that have more than one children.
        return false;
      }
    }
    if (num_ops_to_schedule > 0) {
      // We haven't visited all the operators.
      return false;
    } else if (num_ops_to_schedule < 0) {
      LOG(ERROR) << "Merged an operators twice";
      return false;
    } else {
      return true;
    }
  }

  bool MetisFramework::HasReduce(OperatorInterface* op) {
    return op->get_type() == INTERSECTION_OP ||
      op->get_type() == DIFFERENCE_OP || op->get_type() == DISTINCT_OP ||
      op->get_type() == JOIN_OP || op->get_type() == AGG_OP ||
      op->get_type() == COUNT_OP || op->get_type() == MAX_OP ||
      op->get_type() == MIN_OP || op->get_type() == SORT_OP;
  }

} // namespace framework
} // namespace musketeer
