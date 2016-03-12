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

#include "base/utils.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <limits>
#include <queue>
#include <set>
#include <string>

#include "base/common.h"
#include "base/flags.h"
#include "frameworks/graphchi_framework.h"
#include "frameworks/hadoop_framework.h"
#include "frameworks/metis_framework.h"
#include "frameworks/naiad_framework.h"
#include "frameworks/powergraph_framework.h"
#include "frameworks/spark_framework.h"
#include "RLPlusLexer.h"
#include "RLPlusParser.h"

namespace musketeer {

  // Checks if all the nodes in the dag are in the nodes set.
  bool CheckChildrenInSet(const op_nodes& dag, const node_set& nodes) {
    set<shared_ptr<OperatorNode> > visited;
    queue<shared_ptr<OperatorNode> > to_visit;
    for (op_nodes::const_iterator it = dag.begin(); it != dag.end(); ++it) {
      to_visit.push(*it);
      visited.insert(*it);
      if (nodes.find(*it) == nodes.end()) {
        // Node is not in the nodes set.
        return false;
      }
    }
    while (!to_visit.empty()) {
      shared_ptr<OperatorNode> cur_node = to_visit.front();
      to_visit.pop();
      op_nodes non_loop_children = cur_node->get_children();
      op_nodes children = cur_node->get_loop_children();
      children.insert(children.end(), non_loop_children.begin(),
                      non_loop_children.end());
      for (op_nodes::iterator it = children.begin(); it != children.end();
           ++it) {
        if (visited.insert(*it).second) {
          if (nodes.find(*it) == nodes.end()) {
            // Node is not in the nodes set.
            return false;
          }
          to_visit.push(*it);
        }
      }
    }
    return true;
  }

  // TODO(ionel): This is very hacky. Fix it!
  string ExecCmd(const string& cmd) {
    FILE* pipe = popen(cmd.c_str(), "r");
    if (!pipe) {
      return "ERROR";
    }
    char buffer[128];
    string result = "";
    while (!feof(pipe)) {
      if (fgets(buffer, 128, pipe) != NULL) {
        result += buffer;
      }
    }
    pclose(pipe);
    return result;
  }

  void PrintDag(op_nodes dag) {
    set<shared_ptr<OperatorNode> > visited;
    queue<shared_ptr<OperatorNode> > to_visit;
    for (op_nodes::iterator it = dag.begin(); it != dag.end(); ++it) {
      to_visit.push(*it);
      visited.insert(*it);
      LOG(INFO) << "DAG input node: "
                << (*it)->get_operator()->get_output_relation()->get_name()
                << endl;
    }
    while (!to_visit.empty()) {
      shared_ptr<OperatorNode> cur_node = to_visit.front();
      to_visit.pop();
      if (!cur_node->IsLeaf()) {
        op_nodes children = cur_node->get_loop_children();
        op_nodes non_loop_children = cur_node->get_children();
        children.insert(children.end(), non_loop_children.begin(),
                        non_loop_children.end());
        for (op_nodes::iterator it = children.begin(); it != children.end();
             ++it) {
          LOG(INFO) << "DAG edge: "
                    << cur_node->get_operator()->get_output_relation()->get_name()
                    << " "
                    << (*it)->get_operator()->get_output_relation()->get_name()
                    << endl;
          if (visited.insert(*it).second) {
            to_visit.push(*it);
          }
        }
      }
    }
  }

  void PrintDagGV(op_nodes dag) {
    set<shared_ptr<OperatorNode> > visited;
    queue<shared_ptr<OperatorNode> > to_visit;
    cout << "digraph OpDAG {" << endl;
    cout << "node [shape=box]; ";
    for (op_nodes::iterator it = dag.begin(); it != dag.end(); ++it) {
      to_visit.push(*it);
      visited.insert(*it);
      cout << (*it)->get_operator()->get_output_relation()->get_name()
           << " [label=\"" << (*it)->get_operator()->get_type() << "\"]"
           << "; ";
    }
    cout << endl;
    while (!to_visit.empty()) {
      shared_ptr<OperatorNode> cur_node = to_visit.front();
      to_visit.pop();
      if (!cur_node->IsLeaf()) {
        op_nodes children = cur_node->get_loop_children();
        op_nodes non_loop_children = cur_node->get_children();
        children.insert(children.end(), non_loop_children.begin(),
                        non_loop_children.end());
        for (op_nodes::iterator it = children.begin(); it != children.end();
             ++it) {
          cout << cur_node->get_operator()->get_output_relation()->get_name()
               << "->"
               << (*it)->get_operator()->get_output_relation()->get_name()
               << " [label=\"" <<
            cur_node->get_operator()->get_output_relation()->get_name()
               << "\"];" << endl;
          if (visited.insert(*it).second) {
            to_visit.push(*it);
          }
        }
      }
    }
    cout << "}" << endl;
  }

  vector<string> GetDagOutputs(op_nodes dag) {
    vector<string> output_rels;
    set<shared_ptr<OperatorNode> > visited;
    queue<shared_ptr<OperatorNode> > to_visit;
    for (op_nodes::iterator it = dag.begin(); it != dag.end(); ++it) {
      to_visit.push(*it);
      visited.insert(*it);
    }
    while (!to_visit.empty()) {
      shared_ptr<OperatorNode> cur_node = to_visit.front();
      to_visit.pop();
      op_nodes non_loop_children = cur_node->get_children();
      op_nodes children = cur_node->get_loop_children();
      children.insert(children.end(), non_loop_children.begin(),
                      non_loop_children.end());
      if (children.size() == 0) {
        output_rels.push_back(
            cur_node->get_operator()->get_output_relation()->get_name());
      }
      for (op_nodes::iterator it = children.begin(); it != children.end();
           ++it) {
        if (visited.insert(*it).second) {
          to_visit.push(*it);
        }
      }
    }
    return output_rels;
  }

  bool CheckRenameRequired(OperatorInterface* op) {
    string output_relation = op->get_output_relation()->get_name();
    vector<Relation*> relations = op->get_relations();
    for (vector<Relation*>::iterator rel_it = relations.begin();
         rel_it != relations.end(); ++rel_it) {
      if (!output_relation.compare((*rel_it)->get_name())) {
        return true;
      }
    }
    return false;
  }

  string FmwToString(uint8_t fmw) {
    switch (fmw) {
    case GRAPH_CHI:
      return "graphchi";
    case HADOOP:
      return "hadoop";
    case METIS:
      return "metis";
    case POWER_GRAPH:
      return "powergraph";
    case POWER_LYRA:
      return "powerlyra";
    case SPARK:
      return "spark";
    case WILD_CHERRY:
      return "wildcherry";
    default: {
      LOG(ERROR) << "Unknown framework: " << fmw;
      return NULL;
    }
    }
  }

  string FrameworkToString(FmwType fmw) {
    switch (fmw) {
    case FMW_GRAPH_CHI:
      return "graphchi";
    case FMW_HADOOP:
      return "hadoop";
    case FMW_METIS:
      return "metis";
    case FMW_NAIAD:
      return "naiad";
    case FMW_POWER_GRAPH:
      return "powergraph";
    case FMW_POWER_LYRA:
      return "powerlyra";
    case FMW_SPARK:
      return "spark";
    case FMW_WILD_CHERRY:
      return "wildcherry";
    default: {
      LOG(ERROR) << "Unknown framework: " << fmw;
      return NULL;
    }
    }
  }

  void PrintNodesVector(const string& message, const op_nodes& nodes) {
    for (op_nodes::const_iterator it = nodes.begin(); it != nodes.end(); ++it) {
      LOG(INFO) << message
                << (*it)->get_operator()->get_output_relation()->get_name();
    }
  }

  void PrintRelationVector(const string& message,
                           const vector<Relation*>& nodes) {
    for (vector<Relation*>::const_iterator it = nodes.begin();
         it != nodes.end(); ++it) {
      LOG(INFO) << message << " " << (*it)->get_name();
    }
    if (nodes.size() == 0) {
      LOG(INFO) << message << " Empty ";
    }
  }

  void PrintStringVector(const string& message, const vector<string>& nodes) {
    for (vector<string>::const_iterator it = nodes.begin(); it != nodes.end();
         ++it) {
      LOG(INFO) << message << " " << (*it);
    }
    if (nodes.size() == 0) {
      LOG(INFO) << message << " Empty ";
    }
  }

  void PrintStringSet(const string& message, const set<string>& nodes) {
    for (set<string>::const_iterator it = nodes.begin(); it != nodes.end();
         ++it) {
      LOG(INFO) << message << " " << (*it);
    }
    if (nodes.size() == 0) {
      LOG(INFO) << message << " Empty ";
    }
  }

  void PrintRelationOpNodes(const string& message,  const op_nodes& nodes) {
    for (op_nodes::const_iterator it = nodes.begin(); it != nodes.end(); ++it) {
      LOG(INFO) << message << " "
                << (*it)->get_operator()->get_output_relation()->get_name();
    }
    if (nodes.size() == 0) {
      LOG(INFO) << message << " Empty ";
    }
  }

  bool IsGeneratedByOp(const string& rel_name,
                       const op_nodes& parents) {
    for (op_nodes::const_iterator it = parents.begin(); it != parents.end();
         ++it) {
      if (!rel_name.compare((*it)->get_operator()->get_output_relation()->get_name())) {
        return true;
      }
    }
    return false;
  }

  vector<Relation*>* DetermineInputs(const op_nodes& dag, set<string>* inputs) {
    set<string> visited = set<string>();
    return DetermineInputs(dag, inputs, &visited);
  }

  shared_ptr<OperatorNode> IsInWhileBody(const node_list& nodes) {
    queue<shared_ptr<OperatorNode> > to_visit;
    set<string> visited;
    for (node_list::const_iterator it = nodes.begin(); it != nodes.end(); ++it) {
      to_visit.push(*it);
      visited.insert((*it)->get_operator()->get_output_relation()->get_name());
    }
    while (!to_visit.empty()) {
      shared_ptr<OperatorNode> node = to_visit.front();
      to_visit.pop();
      op_nodes parents = node->get_parents();
      for (op_nodes::iterator it = parents.begin(); it != parents.end(); ++it) {
        if (visited.insert(
                (*it)->get_operator()->get_output_relation()->get_name()).second) {
          if ((*it)->get_operator()->get_type() == WHILE_OP) {
            return *it;
          }
          to_visit.push(*it);
        }
      }
    }
    return shared_ptr<OperatorNode>();
  }

  vector<Relation*>* DetermineInputs(const op_nodes& dag, set<string>* inputs,
                                     set<string>* visited) {
    vector<Relation*> *input_rels_out = new vector<Relation*>;
    queue<shared_ptr<OperatorNode> > to_visit;
    for (op_nodes::const_iterator it = dag.begin(); it != dag.end(); ++it) {
      to_visit.push(*it);
      visited->insert((*it)->get_operator()->get_output_relation()->get_name());
    }
    while (!to_visit.empty()) {
      shared_ptr<OperatorNode> node = to_visit.front();
      to_visit.pop();
      OperatorInterface* op = node->get_operator();
      vector<Relation*> input_rels = op->get_relations();
      for (vector<Relation*>::iterator it = input_rels.begin();
           it != input_rels.end(); ++it) {
        // Rel is an input if rel not visited or doesn't have parents and
        // not already marked as an input.
        if ((node->get_parents().size() == 0 ||
             !IsGeneratedByOp((*it)->get_name(), node->get_parents()) ||
             (node->get_children().size() == 0 && node->get_parents().size() == 1 &&
              node->get_parents()[0]->get_operator()->get_type() == WHILE_OP)) &&
            ((visited->find((*it)->get_name()) == visited->end() &&
              inputs->find((*it)->get_name()) == inputs->end()) ||
             !op->get_output_relation()->get_name().compare((*it)->get_name()))) {
          VLOG(2) << (*it)->get_name() << " is an input";
          inputs->insert((*it)->get_name());
          input_rels_out->push_back(*it);
        } else {
          VLOG(2) << (*it)->get_name() << " is not an input";
        }
      }
      op_nodes non_loop_children = node->get_children();
      op_nodes children = node->get_loop_children();
      children.insert(children.end(), non_loop_children.begin(),
                      non_loop_children.end());
      for (op_nodes::iterator it = children.begin(); it != children.end();
           ++it) {
        bool can_add = true;
        vector<Relation*> input_rels = (*it)->get_operator()->get_relations();
        for (vector<Relation*>::iterator input_it = input_rels.begin();
             input_it != input_rels.end(); ++input_it) {
          if (visited->find((*input_it)->get_name()) == visited->end() &&
              inputs->find((*input_it)->get_name()) == inputs->end() &&
              IsGeneratedByOp((*input_it)->get_name(), node->get_parents())) {
            can_add = false;
            break;
          }
        }
        if ((can_add || op->get_type() == WHILE_OP ||
             (*it)->get_operator()->get_type() == WHILE_OP) &&
            visited->insert((*it)->get_operator()->get_output_relation()->get_name()).second) {
          to_visit.push(*it);
        }
      }
    }
    return input_rels_out;
  }

  void DetermineAllRelOutputs(const op_nodes& dag, set<string>* outputs) {
    queue<shared_ptr<OperatorNode> > to_visit;
    for (op_nodes::const_iterator it = dag.begin(); it != dag.end(); ++it) {
      to_visit.push(*it);
      outputs->insert((*it)->get_operator()->get_output_relation()->get_name());
    }
    while (!to_visit.empty()) {
      shared_ptr<OperatorNode> node = to_visit.front();
      to_visit.pop();
      op_nodes non_loop_children = node->get_children();
      op_nodes children = node->get_loop_children();
      children.insert(children.end(), non_loop_children.begin(),
                      non_loop_children.end());
      for (op_nodes::iterator it = children.begin(); it != children.end();
           ++it) {
        if (outputs->insert(
                (*it)->get_operator()->get_output_relation()->get_name()).second) {
          to_visit.push(*it);
        }
      }
    }
  }

  vector<string> DetermineFinalOutputs(const op_nodes& input_nodes,
                                       const node_list& all_nodes) {
    vector<string> output_rels_name;
    set<shared_ptr<OperatorNode> > visited;
    set<shared_ptr<OperatorNode> > nodes_to_consider;
    queue<shared_ptr<OperatorNode> > to_visit;
    // Build set of out all nodes.
    for (node_list::const_iterator it = all_nodes.begin(); it != all_nodes.end();
         ++it) {
      nodes_to_consider.insert(*it);
    }
    for (op_nodes::const_iterator it = input_nodes.begin(); it != input_nodes.end();
         ++it) {
      to_visit.push(*it);
      visited.insert(*it);
    }
    while (!to_visit.empty()) {
      shared_ptr<OperatorNode> node = to_visit.front();
      to_visit.pop();
      op_nodes non_loop_children = node->get_children();
      op_nodes children = node->get_loop_children();
      children.insert(children.end(), non_loop_children.begin(),
                      non_loop_children.end());
      bool has_children_in_cur_job = false;
      for (op_nodes::iterator it = children.begin(); it != children.end();
           ++it) {
        if (nodes_to_consider.find(*it) != nodes_to_consider.end()) {
          has_children_in_cur_job = true;
          if (visited.insert(*it).second) {
            to_visit.push(*it);
          }
        }
      }
      if (!has_children_in_cur_job) {
        output_rels_name.push_back(
            node->get_operator()->get_output_relation()->get_name());
      }
    }
    return output_rels_name;
  }

  // Add the two numbers. If overflow then returns numeric limit.
  uint64_t SumNoOverflow(uint64_t a, uint64_t b) {
    if (b > numeric_limits<uint64_t>::max() - a) {
      return numeric_limits<uint64_t>::max();
    }
    return a + b;
  }

  uint32_t SumNoOverflow(uint32_t a, uint32_t b) {
    if (b > numeric_limits<uint32_t>::max() - a) {
      return numeric_limits<uint32_t>::max();
    }
    return a + b;
  }

  // Mul the two numbers. If overflow then returns numeric limit.
  uint64_t MulNoOverflow(uint64_t a, uint64_t b) {
    if (a == 0) {
      return 0;
    }
    if (b > numeric_limits<uint64_t>::max() / a) {
      return numeric_limits<uint64_t>::max();
    }
    return a * b;
  }

  uint32_t ClampCost(uint32_t cost) {
    if (cost > FLAGS_max_scheduler_cost) {
      return FLAGS_max_scheduler_cost;
    }
    return cost;
  }

} // namespace musketeer
