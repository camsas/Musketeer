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

#include "scheduling/scheduler_dynamic.h"

#include <bitset>
#include <limits>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <utility>

#include "base/common.h"
#include "base/hdfs_utils.h"
#include "ir/while_operator.h"

namespace musketeer {
namespace scheduling {

  using musketeer::core::JobRun;
  using musketeer::ir::WhileOperator;

  // Construct subDAG for a while operator node.
  void SchedulerDynamic::ConstructSubDAGWhile(
      shared_ptr<OperatorNode> cur_node, const node_set& nodes, node_set* visited,
      node_queue* to_visit) {
    op_nodes loop_children = cur_node->get_loop_children();
    op_nodes barrier_loop_children = op_nodes();
    for (op_nodes::iterator lc_it = loop_children.begin();
         lc_it != loop_children.end(); ++lc_it) {
      if (nodes.find(*lc_it) != nodes.end()) {
        barrier_loop_children.push_back(*lc_it);
        if (visited->insert(*lc_it).second) {
          to_visit->push(*lc_it);
        }
      }
    }
    cur_node->set_barrier_children_loop(barrier_loop_children);
    if (barrier_loop_children.size() != loop_children.size()) {
      cur_node->set_barrier(true);
    }
  }

  // Add children to be visited and set barrier.
  void SchedulerDynamic::ConstructSubDAGChildren(
      shared_ptr<OperatorNode> cur_node, const node_set& nodes, node_set* visited,
      node_queue* to_visit) {
    op_nodes barrier_children = op_nodes();
    op_nodes children = cur_node->get_children();
    for (op_nodes::iterator c_it = children.begin(); c_it != children.end();
         ++c_it) {
      if (nodes.find(*c_it) != nodes.end()) {
        barrier_children.push_back(*c_it);
        if (visited->insert(*c_it).second) {
          to_visit->push(*c_it);
        }
      }
    }
    // Not all the children are included in the subDAG => set barrier.
    if (barrier_children.size() != children.size()) {
      cur_node->set_barrier_children(barrier_children);
      cur_node->set_barrier(true);
    }
  }

  // Takes a vector of topologically sorted nodes and constructs a DAG.
  op_nodes SchedulerDynamic::ConstructSubDAG(const op_nodes& ordered_nodes) {
    op_nodes dag;
    node_set nodes(ordered_nodes.begin(), ordered_nodes.end());
    node_set visited;
    node_queue to_visit;
    // Add the input nodes to the BFS queue.
    for (op_nodes::const_iterator it = ordered_nodes.begin();
         it != ordered_nodes.end(); ++it) {
      to_visit.push(*it);
      visited.insert(*it);
    }
    while (!to_visit.empty()) {
      shared_ptr<OperatorNode> cur_node = to_visit.front();
      to_visit.pop();
      // Get the children and the parents before we set any barrier. Otherwise,
      // the return values will not be correct for parents.
      op_nodes parents = cur_node->get_parents();
      if (cur_node->get_operator()->get_type() == WHILE_OP) {
        ConstructSubDAGWhile(cur_node, nodes, &visited, &to_visit);
      }
      ConstructSubDAGChildren(cur_node, nodes, &visited, &to_visit);
      op_nodes barrier_parents = op_nodes();
      for (op_nodes::iterator p_it = parents.begin(); p_it != parents.end();
           ++p_it) {
        if (nodes.find(*p_it) != nodes.end()) {
          barrier_parents.push_back(*p_it);
        }
      }
      if (barrier_parents.size() == 0) {
        dag.push_back(cur_node);
      }
      if (cur_node->HasBarrier()) {
        cur_node->set_barrier_parents(barrier_parents);
      }
    }
    return dag;
  }

  void SchedulerDynamic::ClearBarriers(const op_nodes& ordered_nodes) {
    for (op_nodes::const_iterator it = ordered_nodes.begin();
         it != ordered_nodes.end(); ++it) {
      (*it)->set_barrier(false);
    }
  }

  void SchedulerDynamic::AvoidInputOverwrite(const op_nodes& ordered_nodes) {
    for (op_nodes::const_iterator it = ordered_nodes.begin();
         it != ordered_nodes.end(); ++it) {
      OperatorInterface* op = (*it)->get_operator();
      if (CheckInputRelOverwrite(op)) {
        op->set_rename(true);
      }
    }
  }

  bool SchedulerDynamic::CheckInputRelOverwrite(OperatorInterface* op) {
    vector<Relation*> rels = op->get_relations();
    string output_rel = op->get_output_relation()->get_name();
    for (vector<Relation*>::iterator it = rels.begin(); it != rels.end();
         ++it) {
      if (!output_rel.compare((*it)->get_name())) {
        return true;
      }
    }
    return false;
  }

  void SchedulerDynamic::ReplaceWithTmp(const op_nodes& nodes) {
    for (op_nodes::const_iterator it = nodes.begin(); it != nodes.end(); ++it) {
      OperatorInterface* op = (*it)->get_operator();
      if (op->get_rename()) {
        string tmp_rel_dir = FLAGS_hdfs_input_dir +
          op->get_output_relation()->get_name() + "/";
        op->set_rename(false);
        string rel_dir = FLAGS_hdfs_input_dir +
          op->get_output_relation()->get_name() + "/";
        // Remove input.
        removeHdfsDir(rel_dir + "*");
        // Move output.
        renameHdfsDir(tmp_rel_dir + "*", rel_dir);
        removeHdfsDir(tmp_rel_dir);
      }
    }
  }

  vector<pair<string, uint64_t> > SchedulerDynamic::DetermineInputsSize(
      const op_nodes& dag) {
    LOG(INFO) << "Determine inputs size for DAG";
    vector<pair<string, uint64_t> > input_size;
    set<string> rels_inputs;
    vector<Relation*> rels = (*DetermineInputs(dag, &rels_inputs));
    if (!FLAGS_dry_run) {
      for (vector<Relation*>::iterator it = rels.begin(); it != rels.end();
           ++it) {
        string rel_dir = FLAGS_hdfs_input_dir + (*it)->get_name() + "/";
        uint64_t input_rel_size = GetRelationSize(rel_dir);
        (*rel_size_)[(*it)->get_name()] = make_pair(input_rel_size, input_rel_size);
        input_size.push_back(make_pair((*it)->get_name(), input_rel_size));
        LOG(INFO) << "Size of: " << (*it)->get_name() << " is: "
                  << input_rel_size;
      }
    } else {
      // Update the size of the inputs.
      for (set<string>::iterator it = rels_inputs.begin();
           it != rels_inputs.end(); ++it) {
        scheduler_simulator_.UpdateOutputSize(*it);
      }
      rel_size_ = scheduler_simulator_.GetCurrentRelSize();
      for (vector<Relation*>::iterator it = rels.begin(); it != rels.end();
           ++it) {
        string input_rel = (*it)->get_name();
        input_size.push_back(make_pair(input_rel, (*rel_size_)[input_rel].second));
        LOG(INFO) << "Size of: " << input_rel << " is: "
                  << (*rel_size_)[input_rel].second;
      }
    }
    return input_size;
  }

  op_nodes::size_type SchedulerDynamic::DynamicScheduleWhileBody(
      const op_nodes& nodes, const op_nodes& order, uint64_t* num_op_executed) {
    WhileOperator* while_op =
      dynamic_cast<WhileOperator*>(nodes[0]->get_operator());
    // TODO(ionel): FIX! We increase iter in the scheduler code.
    op_nodes::size_type while_boundary =
      DetermineWhileBoundary(nodes[0], order, 0);
    int iter = 0;
    bool first_iteration = true;
    while (while_op->get_condition_tree()->checkCondition(iter)) {
      op_nodes order_body(order.begin() + 1,
                          order.begin() + while_boundary + 1);
      while (order_body.size() > 0) {
        RefreshOutputSize(order_body);
        bindings_lt l_bindings = BindOperators(order_body);
        pair<op_nodes, FmwType> bind = l_bindings.front();
        string relation = SwapRel(bind.first);
        op_nodes body_nodes = ConstructSubDAG(bind.first);
        DispatchWithHistory(bind, body_nodes, relation);
        if (first_iteration) {
          LOG(INFO) << "Running operators " << (*num_op_executed + 1) << " "
                    << (*num_op_executed + bind.first.size()) << " on "
                    << CheckForceFmwFlag(bind.second);
          *num_op_executed += bind.first.size();
          first_iteration = false;
        }
        ReplaceWithTmp(bind.first);
        ClearBarriers(bind.first);
        order_body.erase(order_body.begin(),
                         order_body.begin() + bind.first.size());
      }
      ++iter;
    }
    return while_boundary;
  }

  void SchedulerDynamic::DynamicScheduleDAG(const op_nodes& dag) {
    DetermineInputsSize(dag);
    LOG(INFO) << "DynamicSchedule DAG";
    op_nodes order = op_nodes();
    //    optimiser_->optimiseDAG(dag);
    TopologicalOrder(dag, &order);
    PrintNodesVector("Node order after optimisation: ", order);
    if (FLAGS_populate_history) {
      vector<string> rel_names;
      for (op_nodes::const_iterator it = order.begin(); it != order.end();
           ++it) {
        OperatorInterface* op = (*it)->get_operator();
        scheduler_simulator_.UpdateOutputSize(
            op->get_output_relation()->get_name());
      }
      rel_size_ = scheduler_simulator_.GetCurrentRelSize();
    }

    uint64_t num_op_executed = 0;
    while (order.size() > 0) {
      RefreshOutputSize(order);
      bindings_lt l_bindings = BindOperators(order);
      bindings_vt bindings(l_bindings.begin(), l_bindings.end());
      pair<op_nodes, FmwType> bind = bindings[0];
      // LOG(INFO) << "Running job " << index << " on framework " <<
      //   CheckForceFmwFlag(bind.second);
      uint64_t num_op_scheduled = 0;
      // Check if we're trying to schedyle WHILE OPERATOR individually.
      if (bind.first[0]->get_operator()->get_type() == WHILE_OP &&
          bind.first.size() == 1) {
        num_op_scheduled++;
        num_op_executed++;
        LOG(INFO) << "Running operators " <<  num_op_executed << " "
                  << num_op_executed << " on "
                  << CheckForceFmwFlag(bind.second);
        num_op_scheduled +=
          DynamicScheduleWhileBody(bind.first, order, &num_op_executed);
      } else {
        // Executed DAG in the other frameworks.
        // The name of the job is the one of the last output relation.
        // TODO(ionel): Fix this.
        string relation = SwapRel(bind.first);
        op_nodes nodes = ConstructSubDAG(bind.first);
        DispatchWithHistory(bind, nodes, relation);
        num_op_scheduled = bind.first.size();
        LOG(INFO) << "Running operators " << (num_op_executed + 1) << " "
                  << (num_op_executed + num_op_scheduled) << " on "
                  << CheckForceFmwFlag(bind.second);
        num_op_executed += num_op_scheduled;
      }

      LOG(INFO) << "Number of operators scheduled: " << num_op_scheduled;
      ReplaceWithTmp(bind.first);
      ClearBarriers(bind.first);
      // Remove the operators that have already been executed.
      order.erase(order.begin(), order.begin() + num_op_scheduled);
    }
  }

  void SchedulerDynamic::DispatchWithHistory(
      pair<op_nodes, FmwType> bind, const op_nodes& nodes, const string& relation) {
    string fmw_name = CheckForceFmwFlag(bind.second);
    LOG(INFO) << "Dispatching relation " << relation << " in framework "
              << fmw_name;
    FrameworkInterface* fmw = fmws.find(fmw_name)->second;
    timeval start_make_span;
    gettimeofday(&start_make_span, NULL);
    string binary_file = fmw->Translate(nodes, relation);
    fmw->Dispatch(binary_file, relation);
    timeval end_make_span;
    gettimeofday(&end_make_span, NULL);
    PopulateHistory(nodes, relation, fmw_name,
                    end_make_span.tv_sec - start_make_span.tv_sec);
  }

  bindings_vt::size_type SchedulerDynamic::ScheduleWhileBody(
      const op_nodes& nodes, const bindings_vt& bindings, bindings_vt::size_type index) {
    WhileOperator* while_op =
      dynamic_cast<WhileOperator*>(nodes[0]->get_operator());
    bindings_vt::size_type while_boundary =
      DetermineWhileBoundary(nodes[0], bindings, index);
    // TODO(ionel): FIX! We increase iter in the scheduler code.
    int iter = 0;
    while (while_op->get_condition_tree()->checkCondition(iter)) {
      // NOTE: it only goes to < while_boundary because at the
      // while_boundary we have the iter sum operator.
      for (bindings_vt::size_type while_index = index + 1;
           while_index < while_boundary; ++while_index) {
        pair<op_nodes, FmwType> while_bind = bindings[while_index];
        string while_relation = SwapRel(while_bind.first);
        op_nodes while_nodes = ConstructSubDAG(while_bind.first);
        DispatchWithHistory(while_bind, while_nodes, while_relation);
        ReplaceWithTmp(while_bind.first);
        ClearBarriers(while_bind.first);
      }
      ++iter;
    }
    return while_boundary;
  }

  void SchedulerDynamic::RefreshOutputSize(const op_nodes& nodes) {
    for (op_nodes::const_iterator it = nodes.begin(); it != nodes.end(); ++it) {
      // Trigger output size update.
      uint64_t r_size = (*it)->get_operator()->get_output_size(rel_size_).second;
      LOG(INFO) << "Refresh rel size of "
                << (*it)->get_operator()->get_output_relation()->get_name()
                << " is " << r_size;
    }
  }

  bindings_lt SchedulerDynamic::BindOperators(const op_nodes& order) {
    bindings_lt bindings;
    timeval start_scheduler;
    gettimeofday(&start_scheduler, NULL);
    if (FLAGS_use_heuristic) {
      bindings = ComputeHeuristic(order);
    } else {
      bindings = ComputeOptimal(order);
    }
    timeval end_scheduler;
    gettimeofday(&end_scheduler, NULL);
    double scheduler_time = (end_scheduler.tv_sec - start_scheduler.tv_sec);
    scheduler_time +=
      (end_scheduler.tv_usec - start_scheduler.tv_usec) / 1000000.0;
    cout << "SCHEDULER TIME: " << scheduler_time << endl;
    return bindings;
  }

  void SchedulerDynamic::ScheduleDAG(const op_nodes& dag) {
    DetermineInputsSize(dag);
    LOG(INFO) << "Schedule DAG";
    op_nodes order = op_nodes();
    //    optimiser_->optimiseDAG(dag);
    TopologicalOrder(dag, &order);
    PrintNodesVector("Node order after optimisation: ", order);
    RefreshOutputSize(order);
    // Comment the next 2 lines if you're uncommenting the next block.
    bindings_lt l_bindings = BindOperators(order);
    bindings_vt bindings(l_bindings.begin(), l_bindings.end());

    // Uncomment to merge all operators of PageRank.
    //bindings_vt bindings = ScheduleNetflix(order, FMW_HADOOP);
    //bindings_vt bindings = SchedulePageRank(order, FMW_SPARK);
    //bindings_vt bindings = ScheduleShopper(order, FMW_GRAPH_CHI);
    //bindings_vt bindings = SchedulePageRankHad(order);
    //bindings_vt bindings = ScheduleTriangle(order, FMW_GRAPH_CHI);
    //bindings_vt bindings = ScheduleSSSP(order, FMW_HADOOP);
    //bindings_vt bindings = ScheduleTPC(order, FMW_SPARK);

    uint16_t rem_index = 0;
    for (bindings_vt::size_type index = 0; index != bindings.size(); index++) {
      pair<op_nodes, FmwType> bind = bindings[index];
      LOG(INFO) << "Running job " << index << " on framework " <<
        CheckForceFmwFlag(bind.second);
      // The name of the job is the one of the last output relation.
      // TODO(ionel): Fix this.
      // Check if we're trying to schedyle WHILE OPERATOR individually.
      if (bind.first[0]->get_operator()->get_type() == WHILE_OP &&
          bind.first.size() == 1) {
        index = ScheduleWhileBody(bind.first, bindings, index);
      } else {
        string relation = SwapRel(bind.first);
        op_nodes nodes = ConstructSubDAG(bind.first);
        DispatchWithHistory(bind, nodes, relation);
      }
      ReplaceWithTmp(bind.first);
      ClearBarriers(bind.first);
      rem_index++;
    }
  }

  string SchedulerDynamic::SwapRel(const op_nodes& binding) {
    OperatorInterface* last_op = binding.back()->get_operator();
    string relation = last_op->get_output_relation()->get_name();
    if (CheckInputRelOverwrite(last_op)) {
      relation += "_tmp";
    } else {
      removeHdfsDir(last_op->get_output_path());
    }
    AvoidInputOverwrite(binding);
    return relation;
  }

  // Topological sorts a DAG. The second variable will contain the sorted nodes.
  void SchedulerDynamic::TopologicalOrder(const op_nodes& dag,
                                          op_nodes* order) {
    node_set visited = node_set();
    for (op_nodes::const_iterator it = dag.begin(); it != dag.end(); ++it) {
      TopologicalOrderInternal(*it, order, &visited);
    }
  }

  void SchedulerDynamic::TopologicalOrderInternal(shared_ptr<OperatorNode> node,
      op_nodes* order, node_set* visited) {
    op_nodes children_non_loop = node->get_children();
    op_nodes children = node->get_loop_children();
    // Append non_loop children so that all children can be visited in a loop.
    children.insert(children.end(), children_non_loop.begin(),
                    children_non_loop.end());
    order->push_back(node);
    visited->insert(node);
    LOG(INFO) << "Topological order: "
              << node->get_operator()->get_output_relation()->get_name();
    for (op_nodes::iterator it = children.begin(); it != children.end(); ++it) {
      bool dependency_satisfied = true;
      op_nodes parents = (*it)->get_parents();
      for (op_nodes::iterator p_it = parents.begin(); p_it != parents.end();
           ++p_it) {
        if (visited->find(*p_it) == visited->end()) {
          dependency_satisfied = false;
          break;
        }
      }
      if (dependency_satisfied && visited->find(*it) == visited->end()) {
        TopologicalOrderInternal(*it, order, visited);
      }
    }
  }

  // Computes a mapping of subDAG to frameworks of minimal cost. The search
  // is exhaustive and the complexity is exponential.
  bindings_lt SchedulerDynamic::ComputeOptimal(const op_nodes& serial_dag) {
    // TODO(ionel): Implement the dynamic version of the optimal scheduler.
    uint32_t num_ops = serial_dag.size();
    bool **cost = new bool*[FLAGS_max_scheduler_cost + 1];
    uint32_t **parent = new uint32_t*[FLAGS_max_scheduler_cost + 1];
    uint32_t **parent_cost = new uint32_t*[FLAGS_max_scheduler_cost + 1];
    FmwType **scheduled_fmw = new FmwType*[FLAGS_max_scheduler_cost + 1];
    uint32_t max_op_val = 1 << num_ops;
    uint32_t *min_cost = new uint32_t[max_op_val];
    // Job cost stores the min cost of running a subDAG as a single job. It is
    // important for the dynamic because otherwise the job boundaries will not
    // be set corretly. We can not use min_cost, because min_cost represents
    // the minimum cost of running a subDAG. It may involve multiple jobs.
    uint32_t *job_cost = new uint32_t[max_op_val];
    FmwType *min_fmw = new FmwType[max_op_val];
    uint32_t all_ops_ran = max_op_val - 1;
    for (uint32_t ops_ran = 1; ops_ran < max_op_val; ++ops_ran) {
      min_cost[ops_ran] = numeric_limits<uint32_t>::max();
      job_cost[ops_ran] = numeric_limits<uint32_t>::max();
    }
    min_cost[0] = 0;
    job_cost[0] = 0;
    for (uint32_t cur_cost = 0; cur_cost <= FLAGS_max_scheduler_cost;
         ++cur_cost) {
      cost[cur_cost] = new bool[max_op_val];
      parent[cur_cost] = new uint32_t[max_op_val];
      parent_cost[cur_cost] = new uint32_t[max_op_val];
      scheduled_fmw[cur_cost] = new FmwType[max_op_val];
      for (uint32_t op_val = 0; op_val < max_op_val; ++op_val) {
        cost[cur_cost][op_val] = false;
        parent[cur_cost][op_val] = 0;
        parent_cost[cur_cost][op_val] = 0;
        scheduled_fmw[cur_cost][op_val] = FMW_HADOOP;
      }
    }
    vector<string> rel_names;
    for (op_nodes::const_iterator it = serial_dag.begin();
         it != serial_dag.end(); ++it) {
      OperatorInterface* op = (*it)->get_operator();
      rel_names.push_back(op->get_output_relation()->get_name());
    }
    // Initialize jobs costs.
    list<shared_ptr<OperatorNode> > merge_nodes;
    for (uint32_t jobs_merged = 1; jobs_merged <= all_ops_ran; ++jobs_merged) {
      merge_nodes.clear();
      for (uint32_t num_op = 0; num_op < num_ops; ++num_op) {
        if ((1 << num_op) & jobs_merged) {
          merge_nodes.push_back(serial_dag[num_op]);
        }
      }
      for (map<string, FrameworkInterface*>::const_iterator it = fmws.begin();
           it != fmws.end(); ++it) {
        // NOTE: The vector of nodes passed doesn't stricly represent a
        // dag. It is a list of nodes selected from the topological
        // order.
        if (!FLAGS_force_framework.compare("") ||
            !it->first.compare(FLAGS_force_framework)) {
          uint32_t cost_dag =
            ClampCost(it->second->ScoreDAG(merge_nodes, *rel_size_));
          if (cost_dag < min_cost[jobs_merged] && cost_dag < FLAGS_max_scheduler_cost) {
            min_cost[jobs_merged] = cost_dag;
            job_cost[jobs_merged] = cost_dag;
            min_fmw[jobs_merged] = it->second->GetType();
          }
        }
      }
    }
    cost[0][0] = true;
    uint32_t cur_cost = 0;
    for (; cur_cost <= FLAGS_max_scheduler_cost; ++cur_cost) {
      //      LOG(INFO) << "Current cost: " << cur_cost;
      if (cost[cur_cost][all_ops_ran]) {
        // The best solution has been reached.
        break;
      }
      for (uint32_t jobs_exec = 0; jobs_exec < all_ops_ran; ++jobs_exec) {
        // Check if there's a possibility to reach this state.
        if (cost[cur_cost][jobs_exec] && cur_cost <= min_cost[jobs_exec]) {
          for (uint32_t jobs_to_merge = jobs_exec + 1;
               jobs_to_merge <= all_ops_ran; ++jobs_to_merge) {
            uint32_t jobs_merged = (jobs_to_merge & jobs_exec) ^ jobs_to_merge;
            uint32_t next_jobs_exec = jobs_merged | jobs_exec;
            uint32_t next_cost =
              ClampCost(SumNoOverflow(cur_cost, job_cost[jobs_merged]));
            if (!cost[next_cost][next_jobs_exec] &&
                next_cost < FLAGS_max_scheduler_cost) {
              cost[next_cost][next_jobs_exec] = true;
              parent[next_cost][next_jobs_exec] = jobs_exec;
              parent_cost[next_cost][next_jobs_exec] = cur_cost;
              if (next_cost < min_cost[next_jobs_exec]) {
                min_cost[next_jobs_exec] = next_cost;
                min_fmw[next_jobs_exec] = min_fmw[jobs_merged];
              }
              scheduled_fmw[next_cost][next_jobs_exec] = min_fmw[jobs_merged];
            }
          }
        }
      }
    }
    LOG(INFO) << "The minimum cost of running the DAG: " << cur_cost;
    bindings_lt output;
    for (uint32_t cur_jobs_exec = all_ops_ran; cur_jobs_exec > 0; ) {
      op_nodes nodes;
      LOG(INFO) << "Cur cost: " << cur_cost;
      if (cur_cost >= FLAGS_max_scheduler_cost) {
        LOG(FATAL) << "At least one operator could not be scheduled on any of"
                   << "the available execution engines!";
      }
      CHECK(parent[cur_cost] != NULL);
      uint32_t prev_jobs_exec = parent[cur_cost][cur_jobs_exec];
      uint32_t jobs_merged = prev_jobs_exec ^ cur_jobs_exec;
      LOG(INFO) << "---------- Job boundary ----------";
      for (uint32_t num_op = 0; num_op < num_ops; ++num_op) {
        if ((1 << num_op) & jobs_merged) {
          nodes.push_back(serial_dag[num_op]);
          LOG(INFO) << serial_dag[num_op]->get_operator()->get_output_relation()->get_name();
        }
      }
      output.push_front(make_pair(nodes,
                                  scheduled_fmw[cur_cost][cur_jobs_exec]));
      uint32_t tmp_jobs_exec = cur_jobs_exec;
      cur_jobs_exec = parent[cur_cost][cur_jobs_exec];
      cur_cost = parent_cost[cur_cost][tmp_jobs_exec];
    }
    for (uint32_t cur_cost = 0; cur_cost <= FLAGS_max_scheduler_cost;
         ++cur_cost) {
      delete [] cost[cur_cost];
      delete [] parent[cur_cost];
      delete [] parent_cost[cur_cost];
      delete [] scheduled_fmw[cur_cost];
    }
    delete [] cost;
    delete [] parent;
    delete [] parent_cost;
    delete [] scheduled_fmw;
    delete [] min_cost;
    delete [] min_fmw;
    return output;
  }

  // Greedy to compute a sensible split of the DAG into subDAGs. The complexity
  // is O(NUM_JOBS^3 * NUM_FMWS * COST_DAG_COMP).
  bindings_lt SchedulerDynamic::ComputeHeuristic(const op_nodes& serial_dag) {
    LOG(INFO) << "ComputeHeuristic";
    uint32_t num_ops = serial_dag.size();
    uint32_t cost[num_ops + 1][num_ops + 1];
    uint32_t parent[num_ops + 1][num_ops + 1];
    FmwType scheduled_fmw[num_ops + 1][num_ops + 1];
    list<shared_ptr<OperatorNode> > merge_nodes;
    // Initialize matrix.
    for (uint32_t ops_used = 0; ops_used <= num_ops; ++ops_used) {
      for (uint32_t num_jobs = 0; num_jobs <= num_ops; ++num_jobs) {
        cost[ops_used][num_jobs] = numeric_limits<uint32_t>::max();
      }
    }
    cost[0][0] = 0;
    for (uint32_t ops_used = 1; ops_used <= num_ops; ++ops_used) {
      for (uint32_t num_jobs = 1; num_jobs <= ops_used; ++num_jobs) {
        merge_nodes.clear();
        for (uint32_t num_merge = 1; num_merge <= ops_used - num_jobs + 1;
             ++num_merge) {
          merge_nodes.push_front(serial_dag[ops_used - num_merge]);
          for (map<string, FrameworkInterface*>::const_iterator it = fmws.begin();
               it != fmws.end(); ++it) {
            if (!FLAGS_force_framework.compare("") ||
                !it->first.compare(FLAGS_force_framework)) {
              uint32_t cost_dag = it->second->ScoreDAG(merge_nodes, *rel_size_);
              // LOG(INFO) << "Cost of DAG [" << ops_used - num_merge + 1 << ", "
              //           << ops_used << "] in framework: " << it->second->GetType()
              //           << " is: " << cost_dag;
              uint32_t new_cost;
              if (cost[ops_used - num_merge][num_jobs - 1] >
                  numeric_limits<uint32_t>::max() - cost_dag) {
                new_cost = numeric_limits<uint32_t>::max();
              } else {
                new_cost = cost[ops_used - num_merge][num_jobs - 1] + cost_dag;
              }
              if (new_cost <= cost[ops_used][num_jobs]) {
                cost[ops_used][num_jobs] = new_cost;
                parent[ops_used][num_jobs] = ops_used - num_merge;
                scheduled_fmw[ops_used][num_jobs] = it->second->GetType();
              }
            }
          }
        }
      }
    }
    for (uint32_t ops_used = 1; ops_used <= num_ops; ++ops_used) {
      for (uint32_t num_jobs = 1; num_jobs <= ops_used; ++num_jobs) {
        LOG(INFO) << ops_used << " " << num_jobs << " "
                  << cost[ops_used][num_jobs] << " " << parent[ops_used][num_jobs];
      }
    }
    uint32_t optimal_num_jobs = num_ops;
    uint32_t schedulable_ops = num_ops;
    uint32_t min_cost = FLAGS_max_scheduler_cost;
    for (schedulable_ops = num_ops;
         schedulable_ops > 0 && min_cost == FLAGS_max_scheduler_cost;
         --schedulable_ops) {
      for (uint32_t num_jobs = 1; num_jobs <= schedulable_ops; ++num_jobs) {
        if (cost[schedulable_ops][num_jobs] < min_cost) {
          min_cost = cost[schedulable_ops][num_jobs];
          optimal_num_jobs = num_jobs;
        }
      }
    }
    if (min_cost == FLAGS_max_scheduler_cost) {
      // All the costs are maximum.
      optimal_num_jobs = 1;
      LOG(ERROR) << "The cost of running the operators is maximum! This can "
                 << "happen when the operators' input bounds are too big. "
                 << "Defaulting to running in Spark.";
      scheduled_fmw[1][1] = FMW_SPARK;
    }
    schedulable_ops++;
    LOG(INFO) << "Schedulable operators: [1, " << schedulable_ops << "]";
    bindings_lt output;
    for (uint32_t cur_num_op = schedulable_ops;
         cur_num_op > 0 && optimal_num_jobs > 0; --optimal_num_jobs) {
      op_nodes nodes;
      uint32_t prev_num_op = parent[cur_num_op][optimal_num_jobs];
      for (uint32_t num_op = prev_num_op + 1; num_op <= cur_num_op;
           ++num_op) {
        nodes.push_back(serial_dag[num_op - 1]);
      }
      output.push_front(
          make_pair(nodes, scheduled_fmw[cur_num_op][optimal_num_jobs]));
      cur_num_op = prev_num_op;
    }
    return output;
  }

  // If FLAGS_force_framework is "" use fmw, otherwise use the forced fmw.
  string SchedulerDynamic::CheckForceFmwFlag(FmwType fmw) {
    if (!FLAGS_force_framework.compare("")) {
      return FrameworkToString(fmw);
    } else {
      return FLAGS_force_framework;
    }
  }

  // NOTE: Works with the assumption that the end of the loop is not located
  // within the same binding. Having the end of the loop in the same binding
  // as the while_op does shows there's a bug in the canMerge logic.
  bindings_vt::size_type SchedulerDynamic::DetermineWhileBoundary(
      shared_ptr<OperatorNode> while_node, const bindings_vt& bindings,
      bindings_vt::size_type index) {
    op_nodes children = while_node->get_children();
    node_set non_loop_children(children.begin(), children.end());
    for (bindings_vt::size_type w_index = index + 1; w_index < bindings.size();
         ++w_index) {
      pair<op_nodes, FmwType > bind = bindings[w_index];
      for (op_nodes::iterator it = bind.first.begin(); it != bind.first.end();
           ++it) {
        if (non_loop_children.find(*it) != non_loop_children.end()) {
          return w_index - 1;
        }
      }
    }
    return bindings.size() - 1;
  }

  op_nodes::size_type SchedulerDynamic::DetermineWhileBoundary(
       shared_ptr<OperatorNode> while_node, const op_nodes& nodes,
       op_nodes::size_type index) {
    op_nodes children = while_node->get_children();
    node_set non_loop_children(children.begin(), children.end());
    for (op_nodes::size_type w_index = index + 1; w_index < nodes.size();
         ++w_index) {
      if (non_loop_children.find(nodes[w_index]) != non_loop_children.end()) {
        return w_index - 1;
      }
    }
    return nodes.size() - 1;
  }

  void SchedulerDynamic::PopulateHistory(
      const op_nodes& nodes, const string& relation, const string& framework,
      uint64_t make_span) {
    vector<pair<string, uint64_t> > input_rels_size =
      DetermineInputsSize(nodes);
    vector<string> output_rels = GetDagOutputs(nodes);
    vector<pair<string, uint64_t> > output_rels_size;
    if (!FLAGS_dry_run) {
      for (vector<string>::iterator it = output_rels.begin();
           it != output_rels.end(); ++it) {
        string output_rel = FLAGS_hdfs_input_dir + (*it) + "/";
        uint64_t output_rel_size = GetRelationSize(output_rel);
        output_rels_size.push_back(make_pair(*it, output_rel_size));
        (*rel_size_)[output_rel] = make_pair(output_rel_size, output_rel_size);
        LOG(INFO) << "Size of output: " << *it << " is: "
                  << output_rel_size;
      }
    } else {
      // Update the size of the outputs
      for (vector<string>::iterator it = output_rels.begin();
           it != output_rels.end(); ++it) {
        scheduler_simulator_.UpdateOutputSize(*it);
      }
      rel_size_ = scheduler_simulator_.GetCurrentRelSize();
      for (vector<string>::iterator it = output_rels.begin();
           it != output_rels.end(); ++it) {
        output_rels_size.push_back(make_pair(*it, (*rel_size_)[*it].second));
        LOG(INFO) << "Size of output: " << *it << " is: "
                  << (*rel_size_)[*it].second;
      }
    }
    history_->AddRun(new JobRun(relation, framework, make_span,
                                input_rels_size, output_rels_size));
  }

  bindings_vt SchedulerDynamic::ScheduleNetflix(op_nodes order,
                                                FmwType fmw_type) {
    bindings_vt bindings = bindings_vt();
    op_nodes b_nodes = op_nodes();
    b_nodes.push_back(order[0]);
    bindings.push_back(make_pair(b_nodes, fmw_type));
    b_nodes.clear();
    b_nodes.push_back(order[1]);
    b_nodes.push_back(order[2]);
    bindings.push_back(make_pair(b_nodes, fmw_type));
    b_nodes.clear();
    b_nodes.push_back(order[3]);
    b_nodes.push_back(order[4]);
    b_nodes.push_back(order[5]);
    b_nodes.push_back(order[6]);
    bindings.push_back(make_pair(b_nodes, fmw_type));
    b_nodes.clear();
    b_nodes.push_back(order[7]);
    bindings.push_back(make_pair(b_nodes, fmw_type));
    b_nodes.clear();
    b_nodes.push_back(order[8]);
    b_nodes.push_back(order[9]);
    b_nodes.push_back(order[10]);
    bindings.push_back(make_pair(b_nodes, fmw_type));
    b_nodes.clear();
    b_nodes.push_back(order[11]);
    bindings.push_back(make_pair(b_nodes, fmw_type));
    b_nodes.clear();
    b_nodes.push_back(order[12]);
    bindings.push_back(make_pair(b_nodes, fmw_type));
    return bindings;
  }

  bindings_vt SchedulerDynamic::SchedulePageRank(op_nodes order,
                                                 FmwType fmw_type) {
    bindings_vt bindings = bindings_vt();
    op_nodes b_nodes = op_nodes();
    b_nodes.push_back(order[0]);
    b_nodes.push_back(order[1]);
    b_nodes.push_back(order[2]);
    b_nodes.push_back(order[3]);
    b_nodes.push_back(order[4]);
    b_nodes.push_back(order[5]);
    b_nodes.push_back(order[6]);
    b_nodes.push_back(order[7]);
    b_nodes.push_back(order[8]);
    b_nodes.push_back(order[9]);
    bindings.push_back(make_pair(b_nodes, fmw_type));
    return bindings;
  }

  bindings_vt SchedulerDynamic::ScheduleShopper(op_nodes order,
                                                FmwType fmw_type) {
    bindings_vt bindings = bindings_vt();
    op_nodes b_nodes = op_nodes();
    b_nodes.push_back(order[0]);
    bindings.push_back(make_pair(b_nodes, fmw_type));
    b_nodes.clear();
    b_nodes.push_back(order[1]);
    bindings.push_back(make_pair(b_nodes, fmw_type));
    b_nodes.clear();
    b_nodes.push_back(order[2]);
    bindings.push_back(make_pair(b_nodes, fmw_type));
    return bindings;
  }

  bindings_vt SchedulerDynamic::ScheduleTriangle(op_nodes order,
                                                 FmwType fmw_type) {
    bindings_vt bindings = bindings_vt();
    op_nodes b_nodes = op_nodes();
    b_nodes.push_back(order[0]);
    bindings.push_back(make_pair(b_nodes, FMW_HADOOP));
    b_nodes.clear();
    b_nodes.push_back(order[1]);
    b_nodes.push_back(order[2]);
    b_nodes.push_back(order[3]);
    b_nodes.push_back(order[4]);
    b_nodes.push_back(order[5]);
    bindings.push_back(make_pair(b_nodes, fmw_type));
    b_nodes.clear();
    b_nodes.push_back(order[6]);
    bindings.push_back(make_pair(b_nodes, FMW_HADOOP));
    return bindings;
  }

  bindings_vt SchedulerDynamic::ScheduleSSSP(op_nodes order,
                                             FmwType fmw_type) {
    bindings_vt bindings = bindings_vt();
    op_nodes b_nodes = op_nodes();
    b_nodes.push_back(order[0]);
    bindings.push_back(make_pair(b_nodes, fmw_type));
    b_nodes.clear();
    b_nodes.push_back(order[1]);
    b_nodes.push_back(order[2]);
    b_nodes.push_back(order[3]);
    bindings.push_back(make_pair(b_nodes, fmw_type));
    b_nodes.clear();
    b_nodes.push_back(order[4]);
    bindings.push_back(make_pair(b_nodes, fmw_type));
    b_nodes.clear();
    b_nodes.push_back(order[5]);
    bindings.push_back(make_pair(b_nodes, fmw_type));
    b_nodes.clear();
    b_nodes.push_back(order[6]);
    bindings.push_back(make_pair(b_nodes, fmw_type));
    return bindings;
  }

  bindings_vt SchedulerDynamic::ScheduleTPC(op_nodes order,
                                            FmwType fmw_type) {
    bindings_vt bindings = bindings_vt();
    op_nodes b_nodes = op_nodes();
    b_nodes.push_back(order[0]);
    b_nodes.push_back(order[1]);
    b_nodes.push_back(order[2]);
    b_nodes.push_back(order[3]);
    b_nodes.push_back(order[4]);
    b_nodes.push_back(order[5]);
    b_nodes.push_back(order[6]);
    b_nodes.push_back(order[7]);
    b_nodes.push_back(order[8]);
    b_nodes.push_back(order[9]);
    bindings.push_back(make_pair(b_nodes, fmw_type));
    return bindings;
  }

  bindings_vt SchedulerDynamic::SchedulePageRankHad(op_nodes order) {
    bindings_vt bindings = bindings_vt();
    op_nodes b_nodes = op_nodes();
    b_nodes.push_back(order[0]);
    bindings.push_back(make_pair(b_nodes, FMW_HADOOP));
    b_nodes.clear();
    b_nodes.push_back(order[1]);
    bindings.push_back(make_pair(b_nodes, FMW_HADOOP));
    b_nodes.clear();
    b_nodes.push_back(order[2]);
    bindings.push_back(make_pair(b_nodes, FMW_HADOOP));
    b_nodes.clear();
    b_nodes.push_back(order[3]);
    b_nodes.push_back(order[4]);
    b_nodes.push_back(order[5]);
    bindings.push_back(make_pair(b_nodes, FMW_HADOOP));
    b_nodes.clear();
    b_nodes.push_back(order[6]);
    b_nodes.push_back(order[7]);
    b_nodes.push_back(order[8]);
    bindings.push_back(make_pair(b_nodes, FMW_HADOOP));
    b_nodes.clear();
    b_nodes.push_back(order[9]);
    bindings.push_back(make_pair(b_nodes, FMW_HADOOP));
    return bindings;
  }

} // namespace scheduling
} // namespace musketeer
