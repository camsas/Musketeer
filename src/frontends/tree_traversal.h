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

#ifndef MUSKETEER_TREE_TRAVERSAL_H
#define MUSKETEER_TREE_TRAVERSAL_H

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "base/common.h"
#include "base/flags.h"
#include "base/utils.h"
#include "frontends/operator_node.h"
#include "frontends/relations_type.h"
#include "RLPlusLexer.h"
#include "RLPlusParser.h"
#include "scheduling/operator_scheduler.h"

namespace musketeer {

typedef map<string, shared_ptr<OperatorNode> > rel_node;

class TreeTraversal {
 public:
  explicit TreeTraversal(pANTLR3_BASE_TREE tree);
  ~TreeTraversal();
  op_nodes Traverse();

 private:
  pANTLR3_BASE_TREE tree;

  shared_ptr<OperatorNode> Traverse(pANTLR3_BASE_TREE tree, rel_node* op_map);
  Relation* ConstructRelation(string relation);
  bool HandleCreateRelation(pANTLR3_BASE_TREE tree);
  shared_ptr<OperatorNode> HandleAgg(pANTLR3_BASE_TREE tree, rel_node* op_map);
  shared_ptr<OperatorNode> HandleBlackBox(pANTLR3_BASE_TREE tree, rel_node* op_map);
  shared_ptr<OperatorNode> HandleCount(pANTLR3_BASE_TREE tree, rel_node* op_map);
  shared_ptr<OperatorNode> HandleCrossJoin(pANTLR3_BASE_TREE tree, rel_node* op_map);
  shared_ptr<OperatorNode> HandleDifference(pANTLR3_BASE_TREE tree, rel_node* op_map);
  shared_ptr<OperatorNode> HandleDistinct(pANTLR3_BASE_TREE tree, rel_node* op_map);
  shared_ptr<OperatorNode> HandleDiv(pANTLR3_BASE_TREE tree, rel_node* op_map);
  shared_ptr<OperatorNode> HandleIntersection(pANTLR3_BASE_TREE tree, rel_node* op_map);
  shared_ptr<OperatorNode> HandleJoin(pANTLR3_BASE_TREE tree, rel_node* op_map);
  shared_ptr<OperatorNode> HandleMax(pANTLR3_BASE_TREE tree, rel_node* op_map);
  shared_ptr<OperatorNode> HandleMin(pANTLR3_BASE_TREE tree, rel_node* op_map);
  shared_ptr<OperatorNode> HandleMul(pANTLR3_BASE_TREE tree, rel_node* op_map);
  shared_ptr<OperatorNode> HandleProject(pANTLR3_BASE_TREE tree, rel_node* op_map);
  shared_ptr<OperatorNode> HandleSelect(pANTLR3_BASE_TREE tree, rel_node* op_map);
  shared_ptr<OperatorNode> HandleSort(pANTLR3_BASE_TREE tree, rel_node* op_map);
  shared_ptr<OperatorNode> HandleSub(pANTLR3_BASE_TREE tree, rel_node* op_map);
  shared_ptr<OperatorNode> HandleSum(pANTLR3_BASE_TREE tree, rel_node* op_map);
  shared_ptr<OperatorNode> HandleUdf(pANTLR3_BASE_TREE tree, rel_node* op_map);
  shared_ptr<OperatorNode> HandleUnion(pANTLR3_BASE_TREE tree, rel_node* op_map);
  shared_ptr<OperatorNode> HandleWhile(pANTLR3_BASE_TREE tree, rel_node* op_map);
  uint32_t LoopExpr(pANTLR3_BASE_TREE tree, uint32_t left, uint32_t right,
                    vector<Relation*>* relations, op_nodes* children, rel_node* op_map);
  void GenGroupBys(pANTLR3_BASE_TREE tree, uint32_t left, uint32_t right,
                   vector<Column*>* group_bys, vector<uint16_t>* col_types);
  void HandleExpr(pANTLR3_BASE_TREE tree, vector<Relation*>* relations,
                  op_nodes* children, rel_node* op_map);
  Column* GenColumn(const string& attribute);
  shared_ptr<OperatorNode> AddChild(shared_ptr<OperatorNode> child);
  shared_ptr<OperatorNode> AddParent(shared_ptr<OperatorNode> parent);
  op_nodes GetInputNodes(op_nodes nodes);
};

} // namespace musketeer
#endif
