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

#include "frameworks/hadoop_framework.h"

#include <algorithm>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#define HADOOP_START_TIME 15.0
#define NUM_HADOOP_MACHINES 100

namespace musketeer {
namespace framework {

  using musketeer::translator::TranslatorHadoop;
  using musketeer::monitor::HadoopMonitor;

  HadoopFramework::HadoopFramework(): FrameworkInterface() {
    dispatcher_ = new HadoopDispatcher();
    monitor_ = new HadoopMonitor();
  }

  string HadoopFramework::Translate(const op_nodes& dag,
                                    const string& relation) {
    TranslatorHadoop translator_hadoop =
      translator::TranslatorHadoop(dag, relation);
    return translator_hadoop.GenerateCode();
  }

  void HadoopFramework::Dispatch(const string& binary_file,
                                 const string& relation) {
    if (!FLAGS_dry_run) {
      dispatcher_->Execute(binary_file, relation);
    }
  }

  FmwType HadoopFramework::GetType() {
    return FMW_HADOOP;
  }

  uint32_t HadoopFramework::ScoreDAG(const node_list& nodes,
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
      // TODO(ionel): FIXX!!! This is nasty. What is the real cost of running
      // while in Hadoop?
      if (to_schedule.size() == 1 &&
          input_nodes[0]->get_operator()->get_type() == WHILE_OP) {
        int num_iterations =
          input_nodes[0]->get_operator()->get_condition_tree()->getNumIterations();
        uint64_t input_data_size_kb = GetInputSizeOfWhile(input_nodes[0], rel_size);
        uint64_t output_data_size_kb = GetOutputSizeOfWhile(input_nodes[0], rel_size);
        double cost_per_iteration = ScorePull(input_data_size_kb) +
          ScoreLoad(input_data_size_kb) + ScorePush(output_data_size_kb);
        return num_iterations * cost_per_iteration;
      }
      if (FLAGS_best_runtime) {
        return min(static_cast<double>(FLAGS_max_scheduler_cost),
                   HADOOP_START_TIME + ScoreCompile() + ScoreLoad(input_data_size) +
                   ScoreRuntime(input_data_size, nodes, rel_size) +
                   ScorePush(output_data_size));
      } else {
        return min(static_cast<double>(FLAGS_max_scheduler_cost),
                   (HADOOP_START_TIME + ScoreCompile() + ScoreLoad(input_data_size) +
                    ScoreRuntime(input_data_size, nodes, rel_size) +
                    ScorePush(output_data_size)) *
                   NUM_HADOOP_MACHINES);
      }
    } else {
      VLOG(2) << "Cannot merge in " << FrameworkToString(GetType());
      return numeric_limits<uint32_t>::max();
    }
  }

  double HadoopFramework::ScoreMapOnly(OperatorInterface* op,
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
    double time = data_size / NUM_HADOOP_MACHINES / 150.0 / 1024.0;
    return time * FLAGS_time_to_cost;
  }

  double HadoopFramework::ScoreMapRedNoGroup(OperatorInterface* op,
                                             const relation_size& rel_size) {
    string input_rel = op->get_relations()[0]->get_name();
    map<string, pair<uint64_t, uint64_t> >::const_iterator it =
      rel_size.find(input_rel);
    if (it == rel_size.end()) {
      LOG(ERROR) << "Unknown relation size for: " << input_rel;
      return FLAGS_max_scheduler_cost;
    }
    uint64_t data_size = it->second.second;
    // The value is similar to map-only operators because the amount of data
    // shuffled is equal to the number of mappers => small.
    double time = data_size / NUM_HADOOP_MACHINES / 125.0 / 1024.0;
    return time * FLAGS_time_to_cost;
  }

  double HadoopFramework::ScoreMapRedGroup(OperatorInterface* op,
                                           const relation_size& rel_size) {
    string input_rel = op->get_relations()[0]->get_name();
    map<string, pair<uint64_t, uint64_t> >::const_iterator it =
      rel_size.find(input_rel);
    if (it == rel_size.end()) {
      LOG(ERROR) << "Unknown relation size for: " << input_rel;
      return FLAGS_max_scheduler_cost;
    }
    uint64_t data_size = it->second.second;
    double time = data_size / NUM_HADOOP_MACHINES / 10.0 / 1024.0;
    return time * FLAGS_time_to_cost;
  }

  double HadoopFramework::ScoreMapRedTwoInputs(OperatorInterface* op,
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
    uint64_t left_data_size = it->second.second;
    it = rel_size.find(right_input);
    if (it == rel_size.end()) {
      LOG(ERROR) << "Unknown relation size for: " << right_input;
      return FLAGS_max_scheduler_cost;
    }
    uint64_t right_data_size = it->second.second;
    // 15 seconds is the overhead of starting a Hadoop job.
    // TODO(ionel): Must include structure of data as well.
    double time = SumNoOverflow(left_data_size, right_data_size) /
      NUM_HADOOP_MACHINES / 30.0 / 1024.0;
    return time * FLAGS_time_to_cost;
  }

  double HadoopFramework::ScoreJoin(OperatorInterface* op,
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
    uint64_t left_data_size = it->second.second;
    it = rel_size.find(right_input);
    if (it == rel_size.end()) {
      LOG(ERROR) << "Unknown relation size for: " << right_input;
      return FLAGS_max_scheduler_cost;
    }
    uint64_t right_data_size = it->second.second;
    // TODO(ionel): Must include structure of data as well.
    // TODO(ionel): Must include number of machines as well.
    // Use the estimated output size if we have one.
    uint64_t expected_output_size =
      MulNoOverflow(left_data_size, right_data_size);
    string output_rel_name = op->get_output_relation()->get_name();
    it = rel_size.find(output_rel_name);
    if (it != rel_size.end()) {
      expected_output_size = it->second.second;
    }
    double time = expected_output_size / NUM_HADOOP_MACHINES / 7.0 / 1024.0;
    return time * FLAGS_time_to_cost;
  }

  double HadoopFramework::ScoreOperator(shared_ptr<OperatorNode> op_node,
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

  double HadoopFramework::ScoreClusterState() {
    // CurrentUtilizaion returns a value tha represents the max between
    // the percentage of map slots and the percentage of reduce slots occupied.
    return monitor_->CurrentUtilization();
  }

  // The compile time in Hadoop takes around 5s.
  double HadoopFramework::ScoreCompile() {
    return 5.0 * FLAGS_time_to_cost;
  }

  double HadoopFramework::ScorePull(uint64_t data_size_kb) {
    // We can't approximate this value for Hadoop. It is included
    // in the cost of loading an operator.
    return 0.0;
  }

  double HadoopFramework::ScoreLoad(uint64_t data_size_kb) {
    // Can read at about 50MB/s per machine.
    return data_size_kb / NUM_HADOOP_MACHINES / 50.0 / 1024.0;
  }

  // The data_size parameter is 0.
  double HadoopFramework::ScoreRuntime(uint64_t data_size_kb,
                                       const node_list& nodes,
                                       const relation_size& rel_size) {
    // data_size_kb is not used because we already account for it in
    // the PULL and LOAD scores.
    double cur_cost = 0;
    for (node_list::const_iterator it = nodes.begin(); it != nodes.end();
         ++it) {
      double op_cost = ScoreOperator((*it), rel_size);
      cur_cost += op_cost;
    }
    // TODO(ionel): Improve the approximation of the overhead of running more
    // than one operator.
    return cur_cost;
  }

  double HadoopFramework::ScorePush(uint64_t data_size_kb) {
    // Can write at about 20MB/s per machine.
    return data_size_kb / NUM_HADOOP_MACHINES / 20.0 / 1024.0;
  }

  bool HadoopFramework::CanMerge(const op_nodes& dag,
                                 const node_set& to_schedule,
                                 int32_t num_ops_to_schedule) {
    set<string> visited;
    // Can't merge DAGs that have more than 1 operator as input.
    if (dag.size() != 1) {
      return false;
    }
    // Can run a single while operator
    if (to_schedule.size() == 1 &&
        dag[0]->get_operator()->get_type() == WHILE_OP) {
      return true;
    }
    bool got_reduce = false;
    set<string> input_remaining_dag;
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
        } else {
          vector<Relation*> input_rels =
            (*c_it)->get_operator()->get_relations();
          for (vector<Relation*>::iterator r_it = input_rels.begin();
               r_it != input_rels.end(); ++r_it) {
            input_remaining_dag.insert((*r_it)->get_name());
          }
        }
      }
      if (num_children_to_schedule == 0) {
        break;
      } else if (num_children_to_schedule > 1) {
        // We can't merge nodes that have more than one children.
        return false;
      }
    }
    // Intersect the set of output rels of the DAG with the set of input rels
    // of the remaining DAG. If the intersect has more than 1 element then it
    // means that the subDAG has 2 outputs => can't be merged.
    set<string> intersect_rels;
    set_intersection(input_remaining_dag.begin(), input_remaining_dag.end(),
                     visited.begin(), visited.end(),
                     inserter(intersect_rels, intersect_rels.begin()));
    if (intersect_rels.size() > 1) {
      return false;
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

  bool HadoopFramework::HasReduce(OperatorInterface* op) {
    return op->get_type() == INTERSECTION_OP ||
      op->get_type() == CROSS_JOIN_OP || op->get_type() == DIFFERENCE_OP ||
      op->get_type() == DISTINCT_OP || op->get_type() == JOIN_OP ||
      op->get_type() == AGG_OP || op->get_type() == COUNT_OP ||
      op->get_type() == MAX_OP || op->get_type() == MIN_OP ||
      op->get_type() == SORT_OP;
  }

} // namespace framework
} // namespace musketeer
