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

#include "frontends/operator_node.h"

namespace musketeer {

  OperatorInterface* OperatorNode::get_operator() {
    return node_operator;
  }

  void OperatorNode::AddChild(shared_ptr<OperatorNode> child) {
    children.push_back(child);
  }

  void OperatorNode::AddLoopChild(shared_ptr<OperatorNode> child) {
    loop_children.push_back(child);
  }

  void OperatorNode::AddParent(shared_ptr<OperatorNode> parent) {
    parents.push_back(parent);
  }

  vector<shared_ptr<OperatorNode> > OperatorNode::get_parents() {
    if (has_barrier) {
      return barrier_parents;
    } else {
      return parents;
    }
  }

  bool OperatorNode::HasBarrier() {
    return has_barrier;
  }

  bool OperatorNode::IsLeaf() {
    return (children.size() == 0 && loop_children.size() == 0) ||
      (has_barrier && barrier_children.size() == 0 &&
       barrier_loop_children.size() == 0);
  }

  void OperatorNode::set_barrier(bool has_barrier_) {
    has_barrier = has_barrier_;
  }

  vector<shared_ptr<OperatorNode> > OperatorNode::get_children() {
    if (has_barrier) {
      return barrier_children;
    } else {
      return children;
    }
  }

  vector<shared_ptr<OperatorNode> > OperatorNode::get_loop_children() {
    if (has_barrier) {
      return barrier_loop_children;
    } else {
      return loop_children;
    }
  }

  void OperatorNode::set_parents(vector<shared_ptr<OperatorNode> > parents_) {
    parents =  parents_;
  }

  void OperatorNode::set_barrier_parents(
      vector<shared_ptr<OperatorNode> > barrier_parents_) {
    barrier_parents =  barrier_parents_;
  }

  void OperatorNode::set_children(vector<shared_ptr<OperatorNode > > children_) {
    children = children_;
  }

  void OperatorNode::set_barrier_children(
      vector<shared_ptr<OperatorNode> > barrier_children_) {
    barrier_children = barrier_children_;
  }

  void OperatorNode::set_children_loop(
      vector<shared_ptr<OperatorNode> > children_loop_) {
    loop_children = children_loop_;
  }

  void OperatorNode::set_barrier_children_loop(
      vector<shared_ptr<OperatorNode> > barrier_children_loop_) {
    barrier_loop_children = barrier_children_loop_;
  }

} // namespace musketeer
