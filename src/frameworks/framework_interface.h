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

#ifndef MUSKETEER_FRAMEWORK_INTERFACE_H
#define MUSKETEER_FRAMEWORK_INTERFACE_H

#include <algorithm>
#include <list>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/common.h"
#include "base/utils.h"
#include "frameworks/dispatcher_interface.h"
#include "monitoring/monitor_interface.h"
#include "translation/translator_interface.h"

namespace musketeer {
namespace framework {

using monitor::MonitorInterface;

typedef map<string, pair<uint64_t, uint64_t> > relation_size;
typedef set<shared_ptr<OperatorNode> > node_set;
typedef list<shared_ptr<OperatorNode> > node_list;

class FrameworkInterface {
 public:
  virtual uint32_t ScoreDAG(const node_list& dag,
                            const relation_size& rel_size) = 0;
  virtual string Translate(const op_nodes& dag, const string& relation) = 0;
  virtual void Dispatch(const string& binary, const string& relation) = 0;
  virtual FmwType GetType() = 0;

 protected:
  MonitorInterface* monitor_;
  DispatcherInterface* dispatcher_;

  virtual double ScoreClusterState() = 0;
  virtual double ScoreOperator(shared_ptr<OperatorNode> op_node,
                               const relation_size& rel_size) = 0;
  virtual double ScoreCompile() = 0;
  virtual double ScorePull(uint64_t data_size_kb) = 0;
  virtual double ScoreLoad(uint64_t data_size_kb) = 0;
  virtual double ScoreRuntime(uint64_t data_size_kb, const node_list& nodes,
                              const relation_size& rel_size) = 0;
  virtual double ScorePush(uint64_t data_size_kb) = 0;
  virtual bool CanMerge(const op_nodes& dag, const node_set& to_schedule,
                        int32_t num_ops_to_merge) = 0;

  uint64_t GetDataSize(
      const vector<Relation*>& rels, const relation_size& rel_size) {
    uint64_t data_size = 0;
    for (vector<Relation*>::const_iterator it = rels.begin(); it != rels.end();
         ++it) {
      // At the moment we're only using the max bound on the size of the
      // relation.
      // TODO(ionel): Fix this. What should we use here?
      uint64_t upper_bound_size =
        rel_size.find((*it)->get_name())->second.second;
      data_size = SumNoOverflow(data_size, upper_bound_size);
    }
    return data_size;
  }

  uint64_t GetDataSize(vector<string> rels, const relation_size& rel_size) {
    uint64_t data_size = 0;
    for (vector<string>::iterator it = rels.begin(); it != rels.end();
         ++it) {
      uint64_t upper_bound_size = rel_size.find(*it)->second.second;
      data_size = SumNoOverflow(data_size, upper_bound_size);
    }
    return data_size;
  }

  uint64_t GetInputSizeOfWhile(shared_ptr<OperatorNode> op_node,
                               const relation_size& rel_size) {
    set<string> input_names;
    op_nodes input_nodes;
    input_nodes.push_back(op_node);
    uint64_t input_data_size = GetDataSize(
        *DetermineInputs(input_nodes, &input_names), rel_size);
    return input_data_size;
  }

  uint64_t GetOutputSizeOfWhile(shared_ptr<OperatorNode> op_node,
                                const relation_size& rel_size) {
    uint64_t data_size = 0;
    set<shared_ptr<OperatorNode> > visited;
    queue<shared_ptr<OperatorNode> > to_visit;
    // Start from the loop children of the while operator.
    op_nodes loop_children = op_node->get_loop_children();
    for (op_nodes::const_iterator it = loop_children.begin();
         it != loop_children.end(); ++it) {
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
      for (op_nodes::iterator it = children.begin(); it != children.end();
           ++it) {
        if (visited.insert(*it).second) {
          to_visit.push(*it);
        }
      }
      if (children.size() == 0) {
        string output_rel_name =
          node->get_operator()->get_output_relation()->get_name();
        uint64_t upper_bound_size = rel_size.find(output_rel_name)->second.second;
        data_size = SumNoOverflow(data_size, upper_bound_size);
      }
    }
    return data_size;
  }

  void UpdateOpSize(const op_nodes& dag, relation_size* rel_size) {
    node_set visited;
    queue<shared_ptr<OperatorNode> > to_visit;
    for (op_nodes::const_iterator it = dag.begin(); it != dag.end(); ++it) {
      to_visit.push(*it);
      visited.insert(*it);
      // Update the predicted output size.
      (*it)->get_operator()->get_output_size(rel_size);
    }
    while (!to_visit.empty()) {
      shared_ptr<OperatorNode> cur_node = to_visit.front();
      // Update the predicted output size.
      cur_node->get_operator()->get_output_size(rel_size);
      to_visit.pop();
      if (!cur_node->IsLeaf()) {
        op_nodes children = cur_node->get_loop_children();
        op_nodes non_loop_children = cur_node->get_children();
        children.insert(children.end(), non_loop_children.begin(),
                        non_loop_children.end());
        for (op_nodes::iterator it = children.begin(); it != children.end();
             ++it) {
          if (visited.insert(*it).second) {
            to_visit.push(*it);
          }
        }
      }
    }
  }
};

} // namespace framework
} // namespace musketeer
#endif
