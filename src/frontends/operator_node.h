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

#ifndef MUSKETEER_OPERATOR_NODE_H
#define MUSKETEER_OPERATOR_NODE_H

#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>
#include <vector>

#include "ir/operator_interface.h"

namespace musketeer {

using ir::OperatorInterface;

class OperatorNode {
 public:
  explicit OperatorNode(OperatorInterface* node_operator_):
    node_operator(node_operator_), has_barrier(false) {
  }

  OperatorNode(OperatorInterface* node_operator_,
               vector<shared_ptr<OperatorNode> > parents_):
    node_operator(node_operator_), parents(parents_), has_barrier(false) {
  }

  OperatorNode(OperatorInterface* node_operator_,
               vector<shared_ptr<OperatorNode> > parents_,
               vector<shared_ptr<OperatorNode> > children_):
    node_operator(node_operator_), parents(parents_), children(children_),
      has_barrier(false) {
  }

  /* ~OperatorNode() { */
  /*   delete node_operator; */
  /* } */

  OperatorInterface* get_operator();
  void AddChild(shared_ptr<OperatorNode> child);
  void AddParent(shared_ptr<OperatorNode> parent);
  void AddLoopChild(shared_ptr<OperatorNode> loopChild);
  vector<shared_ptr<OperatorNode> > get_parents();
  vector<shared_ptr<OperatorNode> > get_children();
  vector<shared_ptr<OperatorNode> > get_loop_children();
  void set_parents(vector<shared_ptr<OperatorNode> > parents);
  void set_children(vector<shared_ptr<OperatorNode> > children);
  void set_children_loop(vector<shared_ptr<OperatorNode> > children_loop);
  void set_barrier_parents(vector<shared_ptr<OperatorNode> > parents);
  void set_barrier_children(vector<shared_ptr<OperatorNode> > children);
  void set_barrier_children_loop(vector<shared_ptr<OperatorNode> > children);
  void set_barrier(bool has_barrier_);
  bool HasBarrier();
  bool IsLeaf();

 private:
  OperatorInterface* node_operator;
  vector<shared_ptr<OperatorNode> > parents;
  vector<shared_ptr<OperatorNode> > barrier_parents;
  vector<shared_ptr<OperatorNode> > children;
  vector<shared_ptr<OperatorNode> > barrier_children;
  vector<shared_ptr<OperatorNode> > loop_children;
  vector<shared_ptr<OperatorNode> > barrier_loop_children;
  bool has_barrier;
};

} // namespace musketeer
#endif
