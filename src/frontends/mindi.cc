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

#include "frontends/mindi.h"

#include "base/common.h"
#include "base/flags.h"
#include "ir/agg_operator.h"
#include "ir/count_operator.h"
#include "ir/difference_operator.h"
#include "ir/distinct_operator.h"
#include "ir/div_operator.h"
#include "ir/input_operator.h"
#include "ir/intersection_operator.h"
#include "ir/join_operator.h"
#include "ir/max_operator.h"
#include "ir/min_operator.h"
#include "ir/mul_operator.h"
#include "ir/select_operator.h"
#include "ir/sub_operator.h"
#include "ir/sum_operator.h"
#include "ir/union_operator.h"
#include "ir/while_operator.h"

namespace musketeer {

  using ir::AggOperator;
  using ir::CountOperator;
  using ir::DifferenceOperator;
  using ir::DistinctOperator;
  using ir::DivOperator;
  using ir::InputOperator;
  using ir::IntersectionOperator;
  using ir::JoinOperator;
  using ir::MaxOperator;
  using ir::MinOperator;
  using ir::MulOperator;
  using ir::SelectOperator;
  using ir::SubOperator;
  using ir::SumOperator;
  using ir::UnionOperator;
  using ir::WhileOperator;

  shared_ptr<OperatorNode> Mindi::Select(shared_ptr<OperatorNode> op_node,
                                         const vector<Column*>& sel_cols,
                                         const string& rel_out_name) const {
    vector<Relation*> relations;
    relations.push_back(op_node->get_operator()->get_output_relation());
    vector<Column*> out_sel_cols;
    uint32_t index = 0;
    for (vector<Column*>::const_iterator it = sel_cols.begin();
         it != sel_cols.end(); ++it) {
      out_sel_cols.push_back(new Column(rel_out_name, index,
                                        (*it)->get_type()));
      ++index;
    }

    Relation* output_rel = new Relation(rel_out_name, out_sel_cols);
    ConditionTree* cond_tree =
      new ConditionTree(new Value("true", BOOLEAN_TYPE));
    SelectOperator* select_op =
      new SelectOperator(FLAGS_hdfs_input_dir, cond_tree, sel_cols, relations,
                         output_rel);
    vector<shared_ptr<OperatorNode> > parents;
    if (dynamic_cast<InputOperator*>(op_node->get_operator()) == NULL) {
      parents.push_back(op_node);
    }
    shared_ptr<OperatorNode> select_node =
      shared_ptr<OperatorNode>(new OperatorNode(select_op, parents));
    op_node->AddChild(select_node);
    return select_node;
  }

  shared_ptr<OperatorNode> Mindi::Select(shared_ptr<OperatorNode> op_node,
                                         const vector<Column*>& sel_cols,
                                         ConditionTree* cond_tree,
                                         const string& rel_out_name) const {
    ConditionTree* empty_cond_tree =
      new ConditionTree(new Value("true", BOOLEAN_TYPE));
    // TODO(ionel): Assumes that the passed in condition tree only does one
    // arithmetic operation. Extend it to handle the other cases as well.
    vector<Value*> values;
    values.push_back(cond_tree->get_left()->get_column());
    values.push_back(cond_tree->get_right()->get_column());
    vector<Column*> in_cols =
      op_node->get_operator()->get_output_relation()->get_columns();
    vector<Column*> out_cols;
    uint32_t index = 0;
    for (vector<Column*>::iterator it = in_cols.begin(); it != in_cols.end();
         ++it) {
      out_cols.push_back(new Column(rel_out_name + "_math", index,
                                    (*it)->get_type()));
      index++;
    }
    Relation* math_output_rel = new Relation(rel_out_name + "_math", out_cols);
    OperatorInterface* math_op = NULL;
    vector<Relation*> math_relations;
    math_relations.push_back(op_node->get_operator()->get_output_relation());
    string math_op_type = cond_tree->get_cond_operator()->toString();
    if (!math_op_type.compare("+")) {
      math_op = new SumOperator(FLAGS_hdfs_input_dir, empty_cond_tree, math_relations,
                                values, math_output_rel);
    } else if (!math_op_type.compare("-")) {
      math_op = new SubOperator(FLAGS_hdfs_input_dir, empty_cond_tree, math_relations,
                                values, math_output_rel);
    } else if (!math_op_type.compare("/")) {
      math_op = new DivOperator(FLAGS_hdfs_input_dir, empty_cond_tree, math_relations,
                                values, math_output_rel);
    } else if (!math_op_type.compare("*")) {
      math_op = new MulOperator(FLAGS_hdfs_input_dir, empty_cond_tree, math_relations,
                                values, math_output_rel);
    } else {
      LOG(ERROR) << "Unexpected math op type: " << math_op_type;
    }
    vector<shared_ptr<OperatorNode> > parents;
    if (dynamic_cast<InputOperator*>(op_node->get_operator()) == NULL) {
      parents.push_back(op_node);
    }
    shared_ptr<OperatorNode> math_node =
      shared_ptr<OperatorNode>(new OperatorNode(math_op, parents));
    op_node->AddChild(math_node);


    vector<Relation*> relations;
    relations.push_back(math_output_rel);
    vector<Column*> math_sel_cols;
    vector<Column*> sel_out_cols;
    index = 0;
    for (vector<Column*>::const_iterator it = sel_cols.begin();
         it != sel_cols.end(); ++it) {
      math_sel_cols.push_back(new Column(rel_out_name + "_math",
                                         (*it)->get_index(),
                                         (*it)->get_type()));
      sel_out_cols.push_back(new Column(rel_out_name, index,
                                        (*it)->get_type()));
      index++;
    }
    Relation* output_rel = new Relation(rel_out_name, sel_out_cols);
    ConditionTree* cond_tree_sel =
      new ConditionTree(new Value("true", BOOLEAN_TYPE));
    SelectOperator* select_op =
      new SelectOperator(FLAGS_hdfs_input_dir, cond_tree_sel, math_sel_cols,
                         relations, output_rel);
    vector<shared_ptr<OperatorNode> > parents_select;
    parents_select.push_back(math_node);
    shared_ptr<OperatorNode> select_node =
      shared_ptr<OperatorNode>(new OperatorNode(select_op, parents_select));
    math_node->AddChild(select_node);
    return select_node;
  }

  shared_ptr<OperatorNode> Mindi::Where(shared_ptr<OperatorNode> op_node,
                                        ConditionTree* cond_tree,
                                        const string& rel_out_name) const {
    // TODO(ionel): This can be improved. At the moment we create a new select
    // operator. However, we could just append it to the current operator. If
    // the operator already has a condition tree then we'll have to merge them.
    // We need to make sure that the operator already supports condition tree
    // generation, otherwise we can just fallback to adding a new select
    // operator.
    vector<Column*> in_columns =
      op_node->get_operator()->get_output_relation()->get_columns();
    vector<Column*> columns;
    uint32_t index = 0;
    for (vector<Column*>::iterator it = in_columns.begin();
         it != in_columns.end(); ++it, ++index) {
      columns.push_back(new Column(rel_out_name, index, (*it)->get_type()));
    }
    vector<Relation*> relations;
    relations.push_back(op_node->get_operator()->get_output_relation());
    Relation* output_rel = new Relation(rel_out_name, columns);
    SelectOperator* select_op =
      new SelectOperator(FLAGS_hdfs_input_dir, cond_tree, columns, relations,
                         output_rel);
    vector<shared_ptr<OperatorNode> > parents;
    if (dynamic_cast<InputOperator*>(op_node->get_operator()) == NULL) {
      parents.push_back(op_node);
    }
    shared_ptr<OperatorNode> select_node =
      shared_ptr<OperatorNode>(new OperatorNode(select_op, parents));
    op_node->AddChild(select_node);
    return select_node;
  }

  shared_ptr<OperatorNode> Mindi::SelectMany(shared_ptr<OperatorNode> op_node,
                                             ConditionTree* cond_tree,
                                             const string& rel_out_name) const {
    // TODO(ionel): SelectMany is used with nested collections to flatten them.
    // We don't support at the moment nested collections.
    // TODO(ionel): Implement!
    return op_node;
  }

  shared_ptr<OperatorNode> Mindi::Concat(shared_ptr<OperatorNode> op_node,
                                         const string& rel_out_name,
                                         shared_ptr<OperatorNode> other_op_node) const {
    vector<Relation*> relations;
    vector<Column*> in_columns =
      op_node->get_operator()->get_output_relation()->get_columns();
    vector<Column*> columns;
    uint32_t index = 0;
    for (vector<Column*>::iterator it = in_columns.begin();
         it != in_columns.end(); ++it, ++index) {
      columns.push_back(new Column(rel_out_name, index, (*it)->get_type()));
    }
    relations.push_back(op_node->get_operator()->get_output_relation());
    relations.push_back(other_op_node->get_operator()->get_output_relation());
    Relation* output_rel = new Relation(rel_out_name, columns);
    UnionOperator* union_op =
      new UnionOperator(FLAGS_hdfs_input_dir, relations, output_rel);
    vector<shared_ptr<OperatorNode> > parents;
    if (dynamic_cast<InputOperator*>(op_node->get_operator()) == NULL) {
      parents.push_back(shared_ptr<OperatorNode>(op_node));
    }
    if (dynamic_cast<InputOperator*>(other_op_node->get_operator()) == NULL) {
      parents.push_back(shared_ptr<OperatorNode>(other_op_node));
    }
    shared_ptr<OperatorNode> union_node =
      shared_ptr<OperatorNode>(new OperatorNode(union_op, parents));
    op_node->AddChild(union_node);
    other_op_node->AddChild(union_node);
    return union_node;
  }

  shared_ptr<OperatorNode> Mindi::GroupBy(shared_ptr<OperatorNode> op_node,
                                          const vector<Column*>& group_bys,
                                          const GroupByType reducer,
                                          Column* key_col,
                                          const string& rel_out_name) const {
    vector<Relation*> relations;
    relations.push_back(op_node->get_operator()->get_output_relation());
    vector<Column*> columns;
    uint32_t index = 0;
    for (vector<Column*>::const_iterator it = group_bys.begin();
         it != group_bys.end(); it++, index++) {
      columns.push_back(new Column(rel_out_name, index, (*it)->get_type()));
    }
    ConditionTree* cond_tree =
      new ConditionTree(new Value("true", BOOLEAN_TYPE));
    OperatorInterface* group_by_op;
    vector<Column*> key_cols;
    key_cols.push_back(key_col);
    columns.push_back(new Column(rel_out_name, index, key_col->get_type()));
    Relation* output_rel = new Relation(rel_out_name, columns);
    switch (reducer) {
    case PLUS_GROUP: {
      group_by_op = new AggOperator(FLAGS_hdfs_input_dir, cond_tree, group_bys,
                                    "+", relations, key_cols, output_rel);
      break;
    }
    case MINUS_GROUP: {
      group_by_op = new AggOperator(FLAGS_hdfs_input_dir, cond_tree, group_bys,
                                    "-", relations, key_cols, output_rel);
      break;
    }
    case DIVIDE_GROUP: {
      group_by_op = new AggOperator(FLAGS_hdfs_input_dir, cond_tree, group_bys,
                                    "/", relations, key_cols, output_rel);

      break;
    }
    case MULTIPLY_GROUP: {
      group_by_op = new AggOperator(FLAGS_hdfs_input_dir, cond_tree, group_bys,
                                    "*", relations, key_cols, output_rel);
      break;
    }
    case COUNT_GROUP: {
      group_by_op = new CountOperator(FLAGS_hdfs_input_dir, cond_tree, group_bys,
                                      relations, key_col, output_rel);
      break;
    }
    case MIN_GROUP: {
      vector<Column*> val_cols;
      group_by_op = new MinOperator(FLAGS_hdfs_input_dir, cond_tree, group_bys,
                                    relations, val_cols, key_col, output_rel);
      break;
    }
    case MAX_GROUP: {
      vector<Column*> val_cols;
      group_by_op = new MaxOperator(FLAGS_hdfs_input_dir, cond_tree, group_bys,
                                    relations, val_cols, key_col, output_rel);
      break;
    }
    default: {
      LOG(ERROR) << "Unexpected GroupBy operator type";
      return op_node;
    }
    }
    vector<shared_ptr<OperatorNode> > parents;
    if (dynamic_cast<InputOperator*>(op_node->get_operator()) == NULL) {
      parents.push_back(op_node);
    }
    shared_ptr<OperatorNode> group_by_node =
      shared_ptr<OperatorNode>(new OperatorNode(group_by_op, parents));
    op_node->AddChild(group_by_node);
    return group_by_node;
  }

  shared_ptr<OperatorNode> Mindi::Join(shared_ptr<OperatorNode> op_node,
                                       const string& rel_out_name,
                                       shared_ptr<OperatorNode> other_op_node,
                                       vector<Column*> left_cols,
                                       vector<Column*> right_cols) const {
    vector<Relation*> relations;
    vector<Column*> columns;
    vector<Column*> out_left_cols =
      op_node->get_operator()->get_output_relation()->get_columns();
    uint32_t index = 0;
    for (vector<Column*>::iterator it = out_left_cols.begin();
         it != out_left_cols.end(); ++it, ++index) {
      columns.push_back(new Column(rel_out_name, index, (*it)->get_type()));
    }

    set<int32_t> join_right_cols_indices;
    for (vector<Column*>::iterator it = right_cols.begin(); it != right_cols.end(); ++it) {
      join_right_cols_indices.insert((*it)->get_index());
    }

    vector<Column*> out_right_cols =
      other_op_node->get_operator()->get_output_relation()->get_columns();
    for (vector<Column*>::iterator it = out_right_cols.begin();
         it != out_right_cols.end(); ++it) {
      if (join_right_cols_indices.find((*it)->get_index()) == join_right_cols_indices.end()) {
        columns.push_back(new Column(rel_out_name, index, (*it)->get_type()));
        ++index;
      }
    }
    relations.push_back(op_node->get_operator()->get_output_relation());
    relations.push_back(other_op_node->get_operator()->get_output_relation());
    Relation* output_rel = new Relation(rel_out_name, columns);
    JoinOperator* join_op =
      new JoinOperator(FLAGS_hdfs_input_dir, relations, left_cols, right_cols, output_rel);
    vector<shared_ptr<OperatorNode> > parents;
    if (dynamic_cast<InputOperator*>(op_node->get_operator()) == NULL) {
      parents.push_back(shared_ptr<OperatorNode>(op_node));
    }
    if (dynamic_cast<InputOperator*>(other_op_node->get_operator()) == NULL) {
      parents.push_back(shared_ptr<OperatorNode>(other_op_node));
    }
    shared_ptr<OperatorNode> join_node =
      shared_ptr<OperatorNode>(new OperatorNode(join_op, parents));
    op_node->AddChild(join_node);
    other_op_node->AddChild(join_node);
    return join_node;
  }

  shared_ptr<OperatorNode> Mindi::Distinct(shared_ptr<OperatorNode> op_node,
                                           const string& rel_out_name) const {
    vector<Relation*> relations;
    vector<Column*> in_columns =
      op_node->get_operator()->get_output_relation()->get_columns();
    vector<Column*> columns;
    uint32_t index = 0;
    for (vector<Column*>::iterator it = in_columns.begin();
         it != in_columns.end(); ++it, ++index) {
      columns.push_back(new Column(rel_out_name, index, (*it)->get_type()));
    }
    relations.push_back(op_node->get_operator()->get_output_relation());
    Relation* output_rel = new Relation(rel_out_name, columns);
    DistinctOperator* distinct_op =
      new DistinctOperator(FLAGS_hdfs_input_dir, relations, output_rel);
    vector<shared_ptr<OperatorNode> > parents;
    if (dynamic_cast<InputOperator*>(op_node->get_operator()) == NULL) {
      parents.push_back(op_node);
    }
    shared_ptr<OperatorNode> distinct_node =
      shared_ptr<OperatorNode>(new OperatorNode(distinct_op, parents));
    op_node->AddChild(distinct_node);
    return distinct_node;
  }

  shared_ptr<OperatorNode> Mindi::Union(shared_ptr<OperatorNode> op_node,
                                        const string& rel_out_name,
                                        shared_ptr<OperatorNode> other_op_node) const {
    return Distinct(Concat(op_node, rel_out_name + "concat", other_op_node),
                    rel_out_name);
  }

  shared_ptr<OperatorNode> Mindi::Intersect(shared_ptr<OperatorNode> op_node,
                                            const string& rel_out_name,
                                            shared_ptr<OperatorNode> other_op_node) const {
    vector<Relation*> relations;
    vector<Column*> in_columns =
      op_node->get_operator()->get_output_relation()->get_columns();
    vector<Column*> columns;
    uint32_t index = 0;
    for (vector<Column*>::iterator it = in_columns.begin();
         it != in_columns.end(); ++it, ++index) {
      columns.push_back(new Column(rel_out_name, index, (*it)->get_type()));
    }
    relations.push_back(op_node->get_operator()->get_output_relation());
    relations.push_back(other_op_node->get_operator()->get_output_relation());
    Relation* output_rel = new Relation(rel_out_name, columns);
    IntersectionOperator* intersect_op =
      new IntersectionOperator(FLAGS_hdfs_input_dir, relations, output_rel);
    vector<shared_ptr<OperatorNode> > parents;
    if (dynamic_cast<InputOperator*>(op_node->get_operator()) == NULL) {
      parents.push_back(shared_ptr<OperatorNode>(op_node));
    }
    if (dynamic_cast<InputOperator*>(other_op_node->get_operator()) == NULL) {
      parents.push_back(shared_ptr<OperatorNode>(other_op_node));
    }
    shared_ptr<OperatorNode> intersect_node =
      shared_ptr<OperatorNode>(new OperatorNode(intersect_op, parents));
    op_node->AddChild(intersect_node);
    other_op_node->AddChild(intersect_node);
    return intersect_node;
  }

  shared_ptr<OperatorNode> Mindi::Except(shared_ptr<OperatorNode> op_node,
                                         const string& rel_out_name,
                                         shared_ptr<OperatorNode> other_op_node) const {
    vector<Relation*> relations;
    vector<Column*> in_columns =
      op_node->get_operator()->get_output_relation()->get_columns();
    vector<Column*> columns;
    uint32_t index = 0;
    for (vector<Column*>::iterator it = in_columns.begin();
         it != in_columns.end(); ++it, ++index) {
      columns.push_back(new Column(rel_out_name, index, (*it)->get_type()));
    }
    relations.push_back(op_node->get_operator()->get_output_relation());
    relations.push_back(other_op_node->get_operator()->get_output_relation());
    Relation* output_rel = new Relation(rel_out_name, columns);
    DifferenceOperator* difference_op =
      new DifferenceOperator(FLAGS_hdfs_input_dir, relations, output_rel);
    vector<shared_ptr<OperatorNode> > parents;
    if (dynamic_cast<InputOperator*>(op_node->get_operator()) == NULL) {
      parents.push_back(shared_ptr<OperatorNode>(op_node));
    }
    if (dynamic_cast<InputOperator*>(other_op_node->get_operator()) == NULL) {
      parents.push_back(shared_ptr<OperatorNode>(other_op_node));
    }
    shared_ptr<OperatorNode> difference_node =
      shared_ptr<OperatorNode>(new OperatorNode(difference_op, parents));
    op_node->AddChild(difference_node);
    other_op_node->AddChild(difference_node);
    return difference_node;
  }

  shared_ptr<OperatorNode> Mindi::Count(shared_ptr<OperatorNode> op_node,
                                        Column* column,
                                        const string& rel_out_name) const {
    vector<Relation*> relations;
    relations.push_back(op_node->get_operator()->get_output_relation());
    vector<Column*> columns;
    columns.push_back(new Column(rel_out_name, 0, INTEGER_TYPE));
    Relation* output_rel = new Relation(rel_out_name, columns);
    vector<Column*> group_bys;
    ConditionTree* cond_tree =
      new ConditionTree(new Value("true", BOOLEAN_TYPE));
    CountOperator* count_op =
      new CountOperator(FLAGS_hdfs_input_dir, cond_tree, group_bys, relations,
                        column, output_rel);
    vector<shared_ptr<OperatorNode> > parents;
    if (dynamic_cast<InputOperator*>(op_node->get_operator()) == NULL) {
      parents.push_back(op_node);
    }
    shared_ptr<OperatorNode> count_node =
      shared_ptr<OperatorNode>(new OperatorNode(count_op, parents));
    op_node->AddChild(count_node);
    return count_node;
  }

  shared_ptr<OperatorNode> Mindi::Min(shared_ptr<OperatorNode> op_node,
                                      Column* key_col,
                                      const vector<Column*>& val_cols,
                                      const string& rel_out_name) const {
    vector<Relation*> relations;
    relations.push_back(op_node->get_operator()->get_output_relation());
    vector<Column*> columns;
    columns.push_back(new Column(rel_out_name, 0, key_col->get_type()));
    uint32_t index = 1;
    for (vector<Column*>::const_iterator it = val_cols.begin();
         it != val_cols.end(); it++, index++) {
      columns.push_back(new Column(rel_out_name, index, (*it)->get_type()));
    }
    Relation* output_rel = new Relation(rel_out_name, columns);
    vector<Column*> group_bys;
    ConditionTree* cond_tree =
      new ConditionTree(new Value("true", BOOLEAN_TYPE));
    MinOperator* min_operator =
      new MinOperator(FLAGS_hdfs_input_dir, cond_tree, group_bys, relations,
                      val_cols, key_col, output_rel);
    vector<shared_ptr<OperatorNode> > parents;
    if (dynamic_cast<InputOperator*>(op_node->get_operator()) == NULL) {
      parents.push_back(op_node);
    }
    shared_ptr<OperatorNode> min_node =
      shared_ptr<OperatorNode>(new OperatorNode(min_operator, parents));
    op_node->AddChild(min_node);
    return min_node;
  }

  shared_ptr<OperatorNode> Mindi::Max(shared_ptr<OperatorNode> op_node,
                                      Column* key_col,
                                      const vector<Column*>& val_cols,
                                      const string& rel_out_name) const {
    vector<Relation*> relations;
    relations.push_back(op_node->get_operator()->get_output_relation());
    vector<Column*> columns;
    columns.push_back(new Column(rel_out_name, 0, key_col->get_type()));
    uint32_t index = 1;
    for (vector<Column*>::const_iterator it = val_cols.begin();
         it != val_cols.end(); it++, index++) {
      columns.push_back(new Column(rel_out_name, index, (*it)->get_type()));
    }
    Relation* output_rel = new Relation(rel_out_name, columns);
    vector<Column*> group_bys;
    ConditionTree* cond_tree =
      new ConditionTree(new Value("true", BOOLEAN_TYPE));
    MaxOperator* max_operator =
      new MaxOperator(FLAGS_hdfs_input_dir, cond_tree, group_bys, relations,
                      val_cols, key_col, output_rel);
    vector<shared_ptr<OperatorNode> > parents;
    if (dynamic_cast<InputOperator*>(op_node->get_operator()) == NULL) {
      parents.push_back(op_node);
    }
    shared_ptr<OperatorNode> max_node =
      shared_ptr<OperatorNode>(new OperatorNode(max_operator, parents));
    op_node->AddChild(max_node);
    return max_node;
  }

  shared_ptr<OperatorNode> Mindi::Iterate(shared_ptr<OperatorNode> op_node,
                                          ConditionTree* cond_tree,
                                          const string& rel_out_name) const {
    vector<Relation*> relations;
    relations.push_back(op_node->get_operator()->get_output_relation());
    vector<Column*> columns;
    Relation* output_rel = new Relation(rel_out_name, columns);
    WhileOperator* while_op = new WhileOperator(FLAGS_hdfs_input_dir, cond_tree,
                                                relations, output_rel);
    vector<shared_ptr<OperatorNode> > parents;
    if (dynamic_cast<InputOperator*>(op_node->get_operator()) == NULL) {
      parents.push_back(op_node);
    }
    shared_ptr<OperatorNode> while_node =
      shared_ptr<OperatorNode>(new OperatorNode(while_op, parents));
    op_node->AddChild(while_node);
    return while_node;
  }

} // namespace musketeer
