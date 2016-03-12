// Copyright (c) 2015 Natacha Crooks <ncrooks@mpi-sws.org>

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

#ifndef MUSKETEER_QUERY_OPTIMISER_H
#define MUSKETEER_QUERY_OPTIMISER_H

#include <boost/shared_ptr.hpp>
#include <stdio.h>
#include <string.h>

#include <cstring>
#include <functional>
#include <string>
#include <vector>

#include "base/common.h"
#include "base/utils.h"
#include "core/history_storage.h"
#include "frontends/operator_node.h"
#include "ir/operator_interface.h"

namespace musketeer {

const int kNumberOperators = 18;

class QueryOptimiser {
 public:
  QueryOptimiser(bool active, HistoryStorage* history);
  void optimiseDAG(op_nodes* dag);

 private:
  bool canReorder(shared_ptr<OperatorNode> op1, shared_ptr<OperatorNode> op2);
  bool doCommute(shared_ptr<OperatorNode> op1, shared_ptr<OperatorNode> op2);
  bool areIndependent(shared_ptr<OperatorNode> op1,
                      shared_ptr<OperatorNode> op2);

  HistoryStorage* history_;
  bool active_;

  // TODO(tach): Pick better value
  static const int kSizeFactor = 100;

  /* Rules */
  bool filterMerge(shared_ptr<OperatorNode> child,
                   shared_ptr<OperatorNode> parent,
                   op_nodes* new_roots);
  bool filterPushUp(shared_ptr<OperatorNode> child,
                    shared_ptr<OperatorNode> parent,
                    op_nodes* new_roots);
  bool intersectionPushUp(shared_ptr<OperatorNode> child,
                          shared_ptr<OperatorNode> parent,
                          op_nodes* new_roots);
  bool differencePushUp(shared_ptr<OperatorNode> child,
                        shared_ptr<OperatorNode> parent,
                        op_nodes* new_roots);
  bool sortPushDown(shared_ptr<OperatorNode> child,
                    shared_ptr<OperatorNode> parent,
                    op_nodes* new_roots);
  /* These two make use of the history */
  bool optimiseJoin(shared_ptr<OperatorNode> child,
                    shared_ptr<OperatorNode> parent,
                    op_nodes* new_roots);
  bool optimiseProject(shared_ptr<OperatorNode> child,
                       shared_ptr<OperatorNode> parent,
                       op_nodes* new_roots);
  bool unionPushDown(shared_ptr<OperatorNode> child,
                     shared_ptr<OperatorNode> parent,
                     op_nodes* new_roots);
  bool applyRules(shared_ptr<OperatorNode> op1,
                  shared_ptr<OperatorNode> op2,
                  op_nodes* new_roots);
  bool navigateTree(op_nodes* dag);
  bool navigateTreeInternal(shared_ptr<OperatorNode > node, op_nodes* new_roots);
  shared_ptr<OperatorNode> copyNodes(shared_ptr<OperatorNode> node);
  void renameNodes(shared_ptr<OperatorNode> child,
                   shared_ptr<OperatorNode> parent);
  void deleteNode(shared_ptr<OperatorNode> child,
                  shared_ptr<OperatorNode> parent);
  void switchNodes(shared_ptr<OperatorNode> child,
                   shared_ptr<OperatorNode> parent,
                   op_nodes* new_roots);
  void swapOperators(shared_ptr<OperatorNode> child,
                     shared_ptr<OperatorNode> parent);
  void rewriteCondition(string relation, pANTLR3_BASE_TREE condition_tree);
  void initialiseCommutativityMatrix();
  void initialiseRules(bool active);
  void updateOperator(OperatorInterface* op);
  bool commutativity_matrix_[kNumberOperators][kNumberOperators];
};

typedef bool (QueryOptimiser::*rule_t)(shared_ptr<OperatorNode>,
                                       shared_ptr<OperatorNode>);

} // namespace musketeer
#endif
