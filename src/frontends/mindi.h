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

#ifndef MUSKETEER_MINDI_H
#define MUSKETEER_MINDI_H

#include <string>
#include <vector>

#include "frontends/operator_node.h"
#include "ir/condition_tree.h"
#include "ir/relation.h"

namespace musketeer {

using ir::ConditionTree;

typedef enum {
  MIN_GROUP,
  MAX_GROUP,
  COUNT_GROUP,
  PLUS_GROUP,
  MINUS_GROUP,
  DIVIDE_GROUP,
  MULTIPLY_GROUP
} GroupByType;

class Mindi {
 public:
  shared_ptr<OperatorNode> Select(shared_ptr<OperatorNode> op_node,
                                  const vector<Column*>& sel_cols,
                                  const string& rel_out_name) const;

  shared_ptr<OperatorNode> Select(shared_ptr<OperatorNode> op_node,
                                  const vector<Column*>& sel_cols,
                                  ConditionTree* cond_tree,
                                  const string& rel_out_name) const;

  shared_ptr<OperatorNode> Where(shared_ptr<OperatorNode> op_node,
                                 ConditionTree* cond_tree,
                                 const string& rel_out_name) const;

  shared_ptr<OperatorNode> SelectMany(shared_ptr<OperatorNode> op_node,
                                      ConditionTree* cond_tree,
                                      const string& rel_out_name) const;

  shared_ptr<OperatorNode> Concat(shared_ptr<OperatorNode> op_node,
                                  const string& rel_out_name,
                                  shared_ptr<OperatorNode> other_op_node) const;

  shared_ptr<OperatorNode> GroupBy(shared_ptr<OperatorNode> op_node,
                                   const vector<Column*>& group_bys, // key selector
                                   const GroupByType group_reducer,
                                   Column* key_col,
                                   const string& rel_out_name) const;

  shared_ptr<OperatorNode> Join(shared_ptr<OperatorNode> op_node,
                                const string& rel_out_name,
                                shared_ptr<OperatorNode> other_op_node,
                                vector<Column*> left_cols,
                                vector<Column*> right_cols) const;

  shared_ptr<OperatorNode> Distinct(shared_ptr<OperatorNode> op_node,
                                    const string& rel_out_name) const;

  shared_ptr<OperatorNode> Union(shared_ptr<OperatorNode> op_node,
                                 const string& rel_out_name,
                                 shared_ptr<OperatorNode> other_op_node) const;

  shared_ptr<OperatorNode> Intersect(shared_ptr<OperatorNode> op_node,
                                     const string& rel_out_name,
                                     shared_ptr<OperatorNode> other_op_node) const;

  shared_ptr<OperatorNode> Except(shared_ptr<OperatorNode> op_node,
                                  const string& rel_out_name,
                                  shared_ptr<OperatorNode> other_op_node) const;

  shared_ptr<OperatorNode> Count(shared_ptr<OperatorNode> op_node,
                                 Column* column,
                                 const string& rel_out_name) const;

  shared_ptr<OperatorNode> Min(shared_ptr<OperatorNode> op_node,
                               Column* key_col,
                               const vector<Column*>& val_cols,
                               const string& rel_out_name) const;

  shared_ptr<OperatorNode> Max(shared_ptr<OperatorNode> op_node,
                               Column* key_col,
                               const vector<Column*>& val_cols,
                               const string& rel_out_name) const;

  // Non-standard LINQ methods
  shared_ptr<OperatorNode> Iterate(shared_ptr<OperatorNode> op_node,
                                   ConditionTree* cond_tree,
                                   const string& rel_out_name) const;
};

} // namespace musketeer
#endif
