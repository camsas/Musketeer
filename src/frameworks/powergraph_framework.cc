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

#include "frameworks/powergraph_framework.h"

#include <algorithm>
#include <limits>
#include <vector>

namespace musketeer {
namespace framework {

  using musketeer::translator::TranslatorPowerGraph;
  using musketeer::ir::CountOperator;
  using musketeer::ir::JoinOperator;

  PowerGraphFramework::PowerGraphFramework(): FrameworkInterface() {
    dispatcher_ = new PowerGraphDispatcher();
    monitor_ = NULL;
    // TODO(malte): add PowerGraph Monitor
  }

  string PowerGraphFramework::Translate(const op_nodes& dag,
                                        const string& relation) {
    TranslatorPowerGraph translator_powergraph =
      translator::TranslatorPowerGraph(dag, relation);
    return translator_powergraph.GenerateCode();
  }

  void PowerGraphFramework::Dispatch(const string& binary_file,
                                     const string& relation) {
    if (!FLAGS_dry_run) {
      dispatcher_->Execute(binary_file, relation);
    }
  }

  FmwType PowerGraphFramework::GetType() {
    return FMW_POWER_GRAPH;
  }

  uint32_t PowerGraphFramework::ScoreDAG(
      const node_list& nodes, const relation_size& rel_size) {
    node_set to_schedule;
    int32_t num_ops_to_schedule = 0;
    op_nodes input_nodes;
    // At the moment we only support one input DAGs in PowerGraph.
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
      // TODO(ionel): This assumes that the output vertices relations is as big
      // as the input.
      // TODO(ionel): FIX! ScorePush(vertices_data_size)
      if (FLAGS_best_runtime) {
        return min(static_cast<double>(FLAGS_max_scheduler_cost),
                   ScoreCompile() + ScorePull(input_data_size) +
                   ScoreLoad(input_data_size) +
                   ScoreRuntime(input_data_size, nodes, rel_size) +
                   ScorePush(output_data_size));
      } else {
        return min(static_cast<double>(FLAGS_max_scheduler_cost),
                   ScoreCompile() +
                   (ScorePull(input_data_size) + ScoreLoad(input_data_size) +
                    ScoreRuntime(input_data_size, nodes, rel_size) +
                    ScorePush(output_data_size)) * FLAGS_powergraph_num_workers);
      }
    } else {
      VLOG(2) << "Cannot merge in " << FrameworkToString(GetType());
      return numeric_limits<uint32_t>::max();
    }
  }

  double PowerGraphFramework::ScoreOperator(shared_ptr<OperatorNode> op_node,
                                            const relation_size& rel_size) {
    return 0.0 * FLAGS_time_to_cost;
  }

  // The framework doesn't require a deployed cluster. We can create the cluster
  // at runtime => cluster state 0.
  double PowerGraphFramework::ScoreClusterState() {
    return 0.0 * FLAGS_time_to_cost;
  }

  // The compile time in PowerGraph takes around 50s.
  double PowerGraphFramework::ScoreCompile() {
    return 50.0 * FLAGS_time_to_cost;
  }

  double PowerGraphFramework::ScorePull(uint64_t data_size_kb) {
    // This represents the expected duration of the phase in seconds.
    return data_size_kb / FLAGS_powergraph_num_workers / 4.0
      / 1024.0 * FLAGS_time_to_cost;
  }

  double PowerGraphFramework::ScoreLoad(uint64_t data_size_kb) {
    // This represents the expected duration of the phase in seconds.
    // TODO(ionel): This may depend more on the structure of the graph. Check if
    // we can just use a cost based on data size.
    return data_size_kb / FLAGS_powergraph_num_workers / 7.0
      / 1024.0 * FLAGS_time_to_cost;
  }

  double PowerGraphFramework::ScoreRuntime(uint64_t data_size_kb,
                                           const node_list& nodes,
                                           const relation_size& rel_size) {
    // This is the expected runtime of the job. It is a very simple estimate
    // based on the size of the input.
    // To add: 1) differentiate between highly connected graphs and less
    //   connected ones.
    //         2) consider the amount of operators computed at each vertex.
    //         3) consider the number of machines and the overhead on
    //   communication.
    return data_size_kb / FLAGS_powergraph_num_workers / 8.0
      / 1024.0 * FLAGS_time_to_cost;
  }

  double PowerGraphFramework::ScorePush(uint64_t data_size_kb) {
    // This represents the expected duration of the phase in seconds.
    return data_size_kb / FLAGS_powergraph_num_workers / 1.5
      / 1024.0 * FLAGS_time_to_cost;
  }

  pair<bool, shared_ptr<OperatorNode> > PowerGraphFramework::CanMergePreWhile(
      shared_ptr<OperatorNode> cur_node, set<string>* visited,
      const node_set& to_schedule, int32_t* num_ops_to_schedule) {
    visited->insert(cur_node->get_operator()->get_output_relation()->get_name());
    // Decrease number of operators to be visited.
    (*num_ops_to_schedule)--;
    op_nodes children = cur_node->get_children();
    CountOperator* count_op =
      dynamic_cast<CountOperator*>(cur_node->get_operator());
    int32_t cnt_index = count_op->get_column()->get_index();
    vector<Column*> group_bys = count_op->get_group_bys();
    // If we count on source and group by on destination then the COUNT may be
    // pushed to the while loop.
    if (cnt_index != 1 || group_bys.size() != 1 ||
        group_bys[0]->get_index() != 0 || children.size() != 1 ||
        children[0]->get_operator()->get_type() != JOIN_OP) {
      return make_pair(false, cur_node);
    }
    if (to_schedule.find(children[0]) == to_schedule.end()) {
      // The JOIN operator is not part of the graph.
      return make_pair(false, cur_node);
    }
    JoinOperator* join_op =
      dynamic_cast<JoinOperator*>(children[0]->get_operator());
    if (join_op->get_col_left()->get_index() != 0 ||
        join_op->get_col_right()->get_index() != 0) {
      return make_pair(false, cur_node);
    }
    visited->insert(join_op->get_output_relation()->get_name());
    // Decrease number of operators to be visited.
    (*num_ops_to_schedule)--;
    children = children[0]->get_children();
    if (children.size() != 1 ||
        children[0]->get_operator()->get_type() != WHILE_OP) {
      return make_pair(false, cur_node);
    }
    return make_pair(true, children[0]);
  }

  // Check if the block before the group by operator respects the BSP pattern.
  // cur_node is the first node in the while loop.
  pair<bool, shared_ptr<OperatorNode> > PowerGraphFramework::CanMergePreGroup(
      shared_ptr<OperatorNode> cur_node, set<string>* visited,
      const node_set& to_schedule, int32_t* num_ops_to_schedule) {
    op_nodes children = cur_node->get_loop_children();
    // Check the first op in the loop is a JOIN and that the iter operator is
    // present.
    if (children.size() != 2 ||
        children[0]->get_operator()->get_type() != JOIN_OP ||
        (children[1]->get_operator()->get_type() != SUM_OP &&
         children[1]->get_operator()->get_type() != SUB_OP)) {
      return make_pair(false, cur_node);
    }
    cur_node = children[0];
    if (to_schedule.find(cur_node) == to_schedule.end()) {
      // The node is not part of the subDAG to be scheduled.
      return make_pair(false, cur_node);
    }
    JoinOperator* join_op =
      dynamic_cast<JoinOperator*>(cur_node->get_operator());
    vector<Relation*> join_relations = join_op->get_relations();
    // For the moment we're working with the assumption that the left column
    // is the one storing information about the edges.
    // TODO(ionel): FIX!
    vector<Column*> edges_columns = join_relations[0]->get_columns();
    vector<Column*> vertices_columns = join_relations[1]->get_columns();
    if (edges_columns.size() < 2 || vertices_columns.size() < 2) {
      return make_pair(false, cur_node);
    }
    if (join_op->get_col_left()->get_index() != 0 ||
        join_op->get_col_right()->get_index() != 0) {
      // The JOIN is not on source vertex id and vertex id.
      return make_pair(false, cur_node);
    }
    visited->insert(join_op->get_output_relation()->get_name());
    // Decrease number of operators to be visited.
    (*num_ops_to_schedule)--;
    children = cur_node->get_children();
    for (; ; cur_node = children[0], children = children[0]->get_children()) {
      // We don't support intermediate operators with more than one downstream
      // operator.
      if (children.size() != 1) {
        return make_pair(false, cur_node);
      }
      if (to_schedule.find(cur_node) == to_schedule.end()) {
        // Parts of the loop body are missing.
        return make_pair(false, cur_node);
      }
      visited->insert(
          cur_node->get_operator()->get_output_relation()->get_name());
      // Decrease number of operators to be visited.
      (*num_ops_to_schedule)--;
      if (children[0]->get_operator()->hasGroupby()) {
        break;
      }
    }
    return make_pair(true, children[0]);
  }

  // Check if the operators post the group by operators respect the required
  // BSP structure. cur_node is the node with the group by.
  pair<bool, shared_ptr<OperatorNode> > PowerGraphFramework::CanMergePostGroup(
      shared_ptr<OperatorNode> cur_node, set<string>* visited,
      const node_set& to_schedule, int32_t* num_ops_to_schedule) {
    op_nodes children = cur_node->get_children();
    for (; children.size() > 0;
         cur_node = children[0], children = children[0]->get_children()) {
      // We don't support intermediate operators with more than one downstream
      // operator.
      if (children.size() > 1) {
        return make_pair(false, cur_node);
      }
      if (to_schedule.find(cur_node) == to_schedule.end()) {
        // Parts of the loop body are missing.
        return make_pair(false, cur_node);
      }
      visited->insert(
          cur_node->get_operator()->get_output_relation()->get_name());
      // Decrease number of operators to be visited.
      (*num_ops_to_schedule)--;
    }
    return make_pair(true, cur_node);
  }

  bool PowerGraphFramework::CanMerge(const op_nodes& dag,
                                     const node_set& to_schedule,
                                     int32_t num_ops_to_schedule) {
    set<string> visited;
    // The dag can only start with a WHILE or a COUNT.
    if (dag.size() != 1) {
      return false;
    }
    shared_ptr<OperatorNode> cur_node = dag[0];
    if (cur_node->get_operator()->get_type() != COUNT_OP &&
        cur_node->get_operator()->get_type() != WHILE_OP) {
      return false;
    }
    if (cur_node->get_operator()->get_type() == COUNT_OP) {
      pair<bool, shared_ptr<OperatorNode> > merge_pair =
        CanMergePreWhile(cur_node, &visited, to_schedule, &num_ops_to_schedule);
      if (!merge_pair.first) {
        return false;
      }
      cur_node = merge_pair.second;
    }
    // Check if the while loop respects the graph algorithm structure.
    if (to_schedule.find(cur_node) == to_schedule.end()) {
      // The WHILE operator is not part of the subDAG.
      return false;
    }
    visited.insert(cur_node->get_operator()->get_output_relation()->get_name());
    // Decrease number of operators to be visited.
    num_ops_to_schedule--;
    // Check pre group by respects structure.
    pair<bool, shared_ptr<OperatorNode> > pre_group_merge =
      CanMergePreGroup(cur_node, &visited, to_schedule, &num_ops_to_schedule);
    if (!pre_group_merge.first) {
      return false;
    }
    cur_node = pre_group_merge.second;
    // TODO(ionel): Add check to make sure the group by is on the right columns.
    if (to_schedule.find(cur_node) == to_schedule.end()) {
      // The GroupBy operator is not part of the subDAG to be scheduled.
      return false;
    }
    visited.insert(cur_node->get_operator()->get_output_relation()->get_name());
    // Decrease number of operators to be visited.
    num_ops_to_schedule--;
    // Check post group by respects structure.
    pair<bool, shared_ptr<OperatorNode> > post_group_merge =
      CanMergePostGroup(cur_node, &visited, to_schedule, &num_ops_to_schedule);
    if (!pre_group_merge.first) {
      return false;
    }
    if (num_ops_to_schedule > 0) {
      // We haven't visited all the operators.
      return false;
    } else if (num_ops_to_schedule < 0) {
      LOG(ERROR) << "Merged an operator twice";
      return false;
    } else {
      return true;
    }
  }

} // namespace framework
} // namespace musketeer
