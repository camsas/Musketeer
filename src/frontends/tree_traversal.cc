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

#include "frontends/tree_traversal.h"

#include <iostream>
#include <queue>
#include <set>
#include <string>

#include "base/common.h"
#include "ir/agg_operator.h"
#include "ir/black_box_operator.h"
#include "ir/count_operator.h"
#include "ir/condition_tree.h"
#include "ir/cross_join_operator.h"
#include "ir/difference_operator.h"
#include "ir/distinct_operator.h"
#include "ir/div_operator.h"
#include "ir/intersection_operator.h"
#include "ir/join_operator.h"
#include "ir/max_operator.h"
#include "ir/min_operator.h"
#include "ir/mul_operator.h"
#include "ir/project_operator.h"
#include "ir/select_operator.h"
#include "ir/sort_operator.h"
#include "ir/sub_operator.h"
#include "ir/sum_operator.h"
#include "ir/udf_operator.h"
#include "ir/union_operator.h"
#include "ir/while_operator.h"

namespace musketeer {

  using namespace musketeer::ir; // NOLINT

  TreeTraversal::TreeTraversal(pANTLR3_BASE_TREE tree) {
    this->tree = tree;
  }

  TreeTraversal::~TreeTraversal() {
  }

  void PrintNode(pANTLR3_BASE_TREE tree) {
    const unsigned char* text = tree->getText(tree)->chars;
    LOG(INFO) << text;
  }

  op_nodes TreeTraversal::Traverse() {
    rel_node* op_map = new rel_node();
    op_nodes dag;
    if (!tree->getToken(tree)) {
      uint32_t cnt = tree->getChildCount(tree);
      for (uint32_t child_num = 0; child_num < cnt; child_num++) {
        pANTLR3_BASE_TREE node =
          (pANTLR3_BASE_TREE)tree->getChild(tree, child_num);
        if (node->getToken(node)->type == CREATE_RELATION) {
          if (!HandleCreateRelation(node)) {
            LOG(ERROR) << "Relation: " << node->getText(node)->chars
                       << " already exists";
          }
        } else {
          uint32_t cnt = node->getChildCount(node);
          pANTLR3_BASE_TREE outputNode =
            (pANTLR3_BASE_TREE)node->getChild(node, cnt - 1);
          string relation = string(reinterpret_cast<char*>
                                   (outputNode->getText(outputNode)->chars));
          if (op_map->find(relation) == op_map->end()) {
            shared_ptr<OperatorNode> op_node = Traverse(node, op_map);
            if (!op_node->get_parents().size()) {
              dag.push_back(op_node);
            }
          }
        }
      }
    } else {
      dag.push_back(Traverse(tree, op_map));
    }
    delete op_map;
    return dag;
  }

  shared_ptr<OperatorNode> TreeTraversal::Traverse(pANTLR3_BASE_TREE tree,
                                                   rel_node* op_map) {
    pANTLR3_COMMON_TOKEN tok = tree->getToken(tree);
    PrintNode(tree);
    if (tok) {
      switch (tok->type) {
      case AGG: {
        return HandleAgg(tree, op_map);
      }
      case BLACK_BOX: {
        return HandleBlackBox(tree, op_map);
      }
      case COUNT: {
        return HandleCount(tree, op_map);
      }
      case CROSS_JOIN: {
        return HandleCrossJoin(tree, op_map);
      }
      case DIFFERENCE: {
        return HandleDifference(tree, op_map);
      }
      case DISTINCT: {
        return HandleDistinct(tree, op_map);
      }
      case DIV: {
        return HandleDiv(tree, op_map);
      }
      case INTERSECT: {
        return HandleIntersection(tree, op_map);
      }
      case JOIN: {
        return HandleJoin(tree, op_map);
      }
      case MAXIMUM: {
        return HandleMax(tree, op_map);
      }
      case MINIMUM: {
        return HandleMin(tree, op_map);
      }
      case MUL: {
        return HandleMul(tree, op_map);
      }
      case PROJECT: {
        return HandleProject(tree, op_map);
      }
      case RENAME: {
        // TODO(ionel): Implement.
        LOG(ERROR) << "Unimplemented operator: " << tok->type;
      }
      case SELECT: {
        return HandleSelect(tree, op_map);
      }
      case SORT_INC: {
        return HandleSort(tree, op_map);
      }
      case SORT_DEC: {
        return HandleSort(tree, op_map);
      }
      case SUB: {
        return HandleSub(tree, op_map);
      }
      case SUM: {
        return HandleSum(tree, op_map);
      }
      case UDF: {
        return HandleUdf(tree, op_map);
      }
      case UNION: {
        return HandleUnion(tree, op_map);
      }
      case WHILE: {
        return HandleWhile(tree, op_map);
      }
      default:
        LOG(ERROR) << "Handling unknown AST node: " << tok->type;
      }
    } else {
      // This should not be reached.
      LOG(ERROR) << "AST node is not a token";
    }
    return shared_ptr<OperatorNode>();
  }

  Column* TreeTraversal::GenColumn(const string& attribute) {
    uint32_t pos = attribute.rfind("_");
    string relation = attribute.substr(0, pos);
    uint32_t index = atoi(attribute.substr(pos + 1, string::npos).c_str());
    unordered_map<string, vector<uint16_t> >::iterator rel_it =
      RelationsType::relations_type.find(relation);
    uint16_t col_type = INTEGER_TYPE;
    if (rel_it != RelationsType::relations_type.end()) {
      col_type = rel_it->second[index];
    } else {
      LOG(ERROR) << "Relation: " << relation << " is undefined";
    }
    return new Column(relation, index, col_type);
  }

  // Iterates over children in [left, right). Expects them to be either
  // ATTRIBUTE or expr. If DELIMITER node is found then the iteration is
  // interrupted. The returned value is the last node which was unprocessed
  // (e.g. right if no DELIMITER exists)
  uint32_t TreeTraversal::LoopExpr(
      pANTLR3_BASE_TREE tree, uint32_t left, uint32_t right,
      vector<Relation*>* relations, op_nodes* parents, rel_node* op_map) {
    for (uint32_t child_num = left; child_num < right; child_num++) {
      pANTLR3_BASE_TREE expr_node =
        (pANTLR3_BASE_TREE)tree->getChild(tree, child_num);
      PrintNode(expr_node);
      pANTLR3_COMMON_TOKEN expr_token = expr_node->getToken(expr_node);
      if (expr_token->type == ATTRIBUTE) {
        string relation = string(reinterpret_cast<char*>(
            expr_node->getText(expr_node)->chars));
        relations->push_back(ConstructRelation(relation));
        if (op_map->find(relation) != op_map->end()) {
          parents->push_back((*op_map)[relation]);
        }
      } else {
        if (expr_token->type == DELIMITER) {
          return child_num;
        } else {
          // Run subquery.
          shared_ptr<OperatorNode> parent = Traverse(expr_node, op_map);
          parents->push_back(parent);
          relations->push_back(parent->get_operator()->get_output_relation());
        }
      }
    }
    return right;
  }

  void TreeTraversal::GenGroupBys(pANTLR3_BASE_TREE tree, uint32_t left,
                                  uint32_t right, vector<Column*>* group_bys,
                                  vector<uint16_t>* col_types) {
    for (uint32_t child_num = left; child_num < right; child_num++) {
      pANTLR3_BASE_TREE group_by_node =
        (pANTLR3_BASE_TREE)tree->getChild(tree, child_num);
      PrintNode(group_by_node);
      Column* group_by_column;
      if (group_by_node->getToken(group_by_node)->type == ATTRIBUTE) {
        group_by_column = GenColumn(string(reinterpret_cast<char*>(
          group_by_node->getText(group_by_node)->chars)));
      } else {
        // Empty Column.
        group_by_column = new Column("", -1, STRING_TYPE);
      }
      group_bys->push_back(group_by_column);
      col_types->push_back(group_by_column->get_type());
    }
  }

  bool TreeTraversal::HandleCreateRelation(pANTLR3_BASE_TREE tree) {
    vector<uint16_t> col_types;
    uint32_t cnt = tree->getChildCount(tree);
    pANTLR3_BASE_TREE rel_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 0);
    string rel_name =
      string(reinterpret_cast<char*>(rel_node->getText(rel_node)->chars));
    for (uint32_t child_num = 1; child_num < cnt; ++child_num) {
      pANTLR3_BASE_TREE type_node =
        (pANTLR3_BASE_TREE)tree->getChild(tree, child_num);
      switch (type_node->getToken(type_node)->type) {
      case INTEGER: {
        col_types.push_back(INTEGER_TYPE);
        break;
      }
      case DOUBLE: {
        col_types.push_back(DOUBLE_TYPE);
        break;
      }
      case STRING: {
        col_types.push_back(STRING_TYPE);
        break;
      }
      case BOOLEAN: {
        col_types.push_back(BOOLEAN_TYPE);
        break;
      }
      default: {
        LOG(ERROR) << "Column has unexpected type";
        return false;
      }
      }
    }

    if (!RelationsType::relations_type.insert(
          pair<string, vector<uint16_t> >(rel_name, col_types)).second) {
      LOG(ERROR) << "Relation: " << rel_name << " already defined";
      return false;
    }
    return true;
  }

  // Populates the relations and parents vector.
  void TreeTraversal::HandleExpr(pANTLR3_BASE_TREE tree,
      vector<Relation*>* relations, op_nodes* parents, rel_node* op_map) {
    PrintNode(tree);
    pANTLR3_COMMON_TOKEN token = tree->getToken(tree);
    if (token->type != ATTRIBUTE) {
      shared_ptr<OperatorNode> parent = Traverse(tree, op_map);
      parents->push_back(parent);
      relations->push_back(parent->get_operator()->get_output_relation());
    } else {
      string relation =
        string(reinterpret_cast<char*>(tree->getText(tree)->chars));
      relations->push_back(ConstructRelation(relation));
      if (op_map->find(relation) != op_map->end()) {
        parents->push_back((*op_map)[relation]);
      }
    }
  }

  shared_ptr<OperatorNode> TreeTraversal::HandleAgg(pANTLR3_BASE_TREE tree,
                                                    rel_node* op_map) {
    vector<uint16_t> col_types;
    vector<Relation*> relations;
    op_nodes parents;
    vector<Column*> group_bys;
    vector<Column*> agg_columns;
    uint32_t cnt = tree->getChildCount(tree);
    uint32_t child_num;
    for (child_num = 0; child_num < cnt - 1; child_num++) {
      pANTLR3_BASE_TREE expr_node =
        (pANTLR3_BASE_TREE)tree->getChild(tree, child_num);
      pANTLR3_COMMON_TOKEN expr_token = expr_node->getToken(expr_node);
      if (expr_token->type == ENDCOLS) {
        // We've reached the columns-relations delimiter.
        break;
      } else {
        if (expr_token->type == ATTRIBUTE) {
          PrintNode(expr_node);
          agg_columns.push_back(GenColumn(string(reinterpret_cast<char*>(
              expr_node->getText(expr_node)->chars))));
        } else {
          LOG(ERROR) << "Unexpected column in AGG: " << expr_token->type;
          return shared_ptr<OperatorNode>();
        }
      }
    }
    pANTLR3_BASE_TREE math_op_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, ++child_num);
    PrintNode(math_op_node);
    string math_op = string(reinterpret_cast<char*>(
      math_op_node->getText(math_op_node)->chars));
    pANTLR3_BASE_TREE condition_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, ++child_num);
    PrintNode(condition_node);
    child_num = LoopExpr(tree, ++child_num, cnt - 2, &relations, &parents, op_map);
    GenGroupBys(tree, child_num + 1, cnt - 1, &group_bys, &col_types);
    for (vector<Column*>::iterator it = agg_columns.begin();
         it != agg_columns.end(); ++it) {
      col_types.push_back((*it)->get_type());
    }
    pANTLR3_BASE_TREE output_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, cnt - 1);
    PrintNode(output_node);
    string output_relation = string(reinterpret_cast<char*>(
        output_node->getText(output_node)->chars));
    RelationsType::relations_type[output_relation] = col_types;
    AggOperator* agg_operator =
      new AggOperator(FLAGS_hdfs_input_dir, condition_node, group_bys, math_op,
                      relations, agg_columns,
                      ConstructRelation(output_relation));
    shared_ptr<OperatorNode> op_node = AddChild(shared_ptr<OperatorNode> (
        new OperatorNode(agg_operator, parents)));
    (*op_map)[output_relation] = op_node;
    return op_node;
  }

  shared_ptr<OperatorNode> TreeTraversal::HandleBlackBox(
      pANTLR3_BASE_TREE tree, rel_node* op_map) {
    vector<uint16_t> col_types;
    vector<Relation*> relations;
    op_nodes parents;
    uint32_t cnt = tree->getChildCount(tree);
    pANTLR3_BASE_TREE fmw_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 0);
    uint8_t fmw = fmw_node->getToken(fmw_node)->type;
    PrintNode(fmw_node);
    pANTLR3_BASE_TREE path_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 1);
    string binary_path = string(reinterpret_cast<char*>(
        path_node->getText(path_node)->chars));
    PrintNode(path_node);
    pANTLR3_BASE_TREE output_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 2);
    PrintNode(output_node);
    string output_relation = string(reinterpret_cast<char*>(
        output_node->getText(output_node)->chars));
    for (uint32_t child_num = 3; child_num < cnt; ++child_num) {
      pANTLR3_BASE_TREE type_node =
        (pANTLR3_BASE_TREE)tree->getChild(tree, child_num);
      col_types.push_back(type_node->getToken(type_node)->type);
    }
    RelationsType::relations_type[output_relation] = col_types;
    BlackBoxOperator* black_box_operator =
      new BlackBoxOperator(FLAGS_hdfs_input_dir, relations,
                           ConstructRelation(output_relation),
                           fmw, binary_path);
    shared_ptr<OperatorNode> op_node = AddChild(shared_ptr<OperatorNode> (
        new OperatorNode(black_box_operator, parents)));
    (*op_map)[output_relation] = op_node;
    return op_node;
  }

  shared_ptr<OperatorNode> TreeTraversal::HandleCount(pANTLR3_BASE_TREE tree,
                                                      rel_node* op_map) {
    vector<uint16_t> col_types;
    vector<Relation*> relations;
    op_nodes parents;
    vector<Column*> group_bys;
    uint32_t cnt = tree->getChildCount(tree);
    pANTLR3_BASE_TREE column_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 0);
    PrintNode(column_node);
    Column* count_column =
      GenColumn(string(reinterpret_cast<char*>(
          column_node->getText(column_node)->chars)));
    pANTLR3_BASE_TREE condition_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 1);
    PrintNode(condition_node);
    uint32_t child_num = LoopExpr(tree, 2, cnt - 2, &relations, &parents, op_map);
    GenGroupBys(tree, child_num + 1, cnt - 1, &group_bys, &col_types);
    col_types.push_back(count_column->get_type());
    pANTLR3_BASE_TREE output_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, cnt - 1);
    PrintNode(output_node);
    string output_relation = string(reinterpret_cast<char*>(
        output_node->getText(output_node)->chars));
    RelationsType::relations_type[output_relation] = col_types;
    CountOperator* count_operator =
      new CountOperator(FLAGS_hdfs_input_dir, condition_node, group_bys, relations,
                        count_column, ConstructRelation(output_relation));
    shared_ptr<OperatorNode> op_node = AddChild(shared_ptr<OperatorNode> (
        new OperatorNode(count_operator, parents)));
    (*op_map)[output_relation] = op_node;
    return op_node;
  }

  shared_ptr<OperatorNode> TreeTraversal::HandleCrossJoin(pANTLR3_BASE_TREE tree,
      rel_node* op_map) {
    uint32_t cnt = tree->getChildCount(tree);
    if (cnt == 3) {
      vector<uint16_t> col_types;
      vector<Relation*> relations;
      op_nodes parents;
      HandleExpr((pANTLR3_BASE_TREE)tree->getChild(tree, 0),
                 &relations, &parents, op_map);
      HandleExpr((pANTLR3_BASE_TREE)tree->getChild(tree, 1),
                 &relations, &parents, op_map);
      // All the columns from the left relation are included in the join.
      vector<Column*> left_rel_cols = relations[0]->get_columns();
      for (vector<Column*>::iterator it = left_rel_cols.begin();
           it != left_rel_cols.end(); ++it) {
        col_types.push_back((*it)->get_type());
      }
      // All the columns from the right relation are included in the join.
      vector<Column*> right_rel_cols = relations[1]->get_columns();
      for (vector<Column*>::iterator it = right_rel_cols.begin();
           it != right_rel_cols.end(); ++it) {
        col_types.push_back((*it)->get_type());
      }
      pANTLR3_BASE_TREE output_node =
        (pANTLR3_BASE_TREE)tree->getChild(tree, cnt - 1);
      PrintNode(output_node);
      string output_relation = string(reinterpret_cast<char*>(
          output_node->getText(output_node)->chars));
      RelationsType::relations_type[output_relation] = col_types;
      CrossJoinOperator* cross_join_operator =
        new CrossJoinOperator(FLAGS_hdfs_input_dir, relations,
                              ConstructRelation(output_relation));
      shared_ptr<OperatorNode> op_node = AddChild(shared_ptr<OperatorNode> (
        new OperatorNode(cross_join_operator, parents)));
      (*op_map)[output_relation] = op_node;
      return op_node;
    } else {
      LOG(ERROR) << "Unexpected number of children in CROSS JOIN";
    }
    return shared_ptr<OperatorNode>();
  }

  shared_ptr<OperatorNode> TreeTraversal::HandleDifference(
      pANTLR3_BASE_TREE tree, rel_node* op_map) {
    uint32_t cnt = tree->getChildCount(tree);
    if (cnt == 3) {
      vector<uint16_t> col_types;
      vector<Relation*> relations;
      op_nodes parents;
      HandleExpr((pANTLR3_BASE_TREE)tree->getChild(tree, 0),
                 &relations, &parents, op_map);
      HandleExpr((pANTLR3_BASE_TREE)tree->getChild(tree, 1),
                 &relations, &parents, op_map);
      // Left rel and right rel have the same types. The ouput will have the
      // same types as well.
      vector<Column*> left_rel_cols = relations[0]->get_columns();
      for (vector<Column*>::iterator it = left_rel_cols.begin();
           it != left_rel_cols.end(); ++it) {
        col_types.push_back((*it)->get_type());
      }
      pANTLR3_BASE_TREE output_node =
        (pANTLR3_BASE_TREE)tree->getChild(tree, cnt - 1);
      PrintNode(output_node);
      string output_relation = string(reinterpret_cast<char*>(
          output_node->getText(output_node)->chars));
      RelationsType::relations_type[output_relation] = col_types;
      DifferenceOperator* difference_operator =
        new DifferenceOperator(FLAGS_hdfs_input_dir, relations,
                               ConstructRelation(output_relation));
      shared_ptr<OperatorNode> op_node = AddChild(shared_ptr<OperatorNode> (
          new OperatorNode(difference_operator, parents)));
      (*op_map)[output_relation] = op_node;
      return op_node;
    } else {
      LOG(ERROR) << "Unexpected number of children in DIFFERENCE";
    }
    return shared_ptr<OperatorNode>();
  }

  shared_ptr<OperatorNode> TreeTraversal::HandleDistinct(pANTLR3_BASE_TREE tree,
                                                         rel_node* op_map) {
    uint32_t cnt = tree->getChildCount(tree);
    if (cnt == 2) {
      vector<uint16_t> col_types;
      vector<Relation*> relations;
      op_nodes parents;
      HandleExpr((pANTLR3_BASE_TREE)tree->getChild(tree, 0),
                 &relations, &parents, op_map);
      vector<Column*> input_cols = relations[0]->get_columns();
      for (vector<Column*>::iterator it = input_cols.begin();
           it != input_cols.end(); ++it) {
        col_types.push_back((*it)->get_type());
      }
      pANTLR3_BASE_TREE output_node =
        (pANTLR3_BASE_TREE)tree->getChild(tree, cnt - 1);
      PrintNode(output_node);
      string output_relation = string(reinterpret_cast<char*>(
          output_node->getText(output_node)->chars));
      RelationsType::relations_type[output_relation] = col_types;
      DistinctOperator* distinct_operator =
        new DistinctOperator(FLAGS_hdfs_input_dir, relations,
                             ConstructRelation(output_relation));
      shared_ptr<OperatorNode> op_node = AddChild(shared_ptr<OperatorNode> (
          new OperatorNode(distinct_operator, parents)));
      (*op_map)[output_relation] = op_node;
      return op_node;
    } else {
      LOG(ERROR) << "Unexpected number of children in DISTINCT";
    }
    return shared_ptr<OperatorNode>();
  }

  shared_ptr<OperatorNode> TreeTraversal::HandleDiv(pANTLR3_BASE_TREE tree,
                                                    rel_node* op_map) {
    uint32_t cnt = tree->getChildCount(tree);
    vector<Value*> values;
    vector<Relation*> relations;
    op_nodes parents;
    pANTLR3_BASE_TREE left_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 0);
    PrintNode(left_node);
    pANTLR3_COMMON_TOKEN token = left_node->getToken(left_node);
    Column* left_column = NULL;
    Column* right_column = NULL;
    if (token->type == ATTRIBUTE) {
      // Check if column.
      left_column = GenColumn(string(reinterpret_cast<char*>(
        left_node->getText(left_node)->chars)));
      values.push_back(left_column);
    } else {
      if (token->type == INT_VALUE) {
        values.push_back(new Value(
          string(reinterpret_cast<char*>(
            left_node->getText(left_node)->chars)), INTEGER_TYPE));
      } else if (token->type == DOUBLE_VALUE) {
        values.push_back(new Value(
          string(reinterpret_cast<char*>(
            left_node->getText(left_node)->chars)), DOUBLE_TYPE));
      } else {
        LOG(ERROR) << "DIV only operates on columns or constans.";
        return shared_ptr<OperatorNode>();
      }
    }
    pANTLR3_BASE_TREE right_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 1);
    PrintNode(right_node);
    token = right_node->getToken(right_node);
    if (token->type == ATTRIBUTE) {
      right_column = GenColumn(string(reinterpret_cast<char*>(
        right_node->getText(right_node)->chars)));
      values.push_back(right_column);
    } else {
      if (token->type == INT_VALUE) {
        values.push_back(new Value(
          string(reinterpret_cast<char*>(
            right_node->getText(right_node)->chars)), INTEGER_TYPE));
      } else if (token->type == DOUBLE_VALUE) {
        values.push_back(new Value(
          string(reinterpret_cast<char*>(
            right_node->getText(right_node)->chars)), DOUBLE_TYPE));
      } else {
        LOG(ERROR) << "DIV only operates on columns or constans.";
        return shared_ptr<OperatorNode>();
      }
    }
    pANTLR3_BASE_TREE condition_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 2);
    LoopExpr(tree, 3, cnt - 1, &relations, &parents, op_map);
    PrintNode(condition_node);
    pANTLR3_BASE_TREE output_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, cnt - 1);
    PrintNode(output_node);
    string output_relation = string(reinterpret_cast<char*>(
      output_node->getText(output_node)->chars));
    // DIV only works on one relation. The column types of the output
    // are the same as of the input.
    // TODO(ionel): Support the case when we're dividing by a float and the type
    // of a column must be updated.
    RelationsType::relations_type[output_relation] =
      RelationsType::relations_type[relations[0]->get_name()];
    if (values[1]->get_type() == DOUBLE_TYPE && left_column != NULL) {
      values[0]->set_type(DOUBLE_TYPE);
      RelationsType::relations_type[output_relation][left_column->get_index()] =
        values[0]->get_type();
    }
    DivOperator* div_operator =
      new DivOperator(FLAGS_hdfs_input_dir, condition_node, relations, values,
                      ConstructRelation(output_relation));
    shared_ptr<OperatorNode> op_node = AddChild(shared_ptr<OperatorNode> (
        new OperatorNode(div_operator, parents)));
    (*op_map)[output_relation] = op_node;
    return op_node;
  }

  shared_ptr<OperatorNode> TreeTraversal::HandleIntersection(
      pANTLR3_BASE_TREE tree, rel_node* op_map) {
    uint32_t cnt = tree->getChildCount(tree);
    if (cnt == 3) {
      vector<uint16_t> col_types;
      vector<Relation*> relations;
      op_nodes parents;
      HandleExpr((pANTLR3_BASE_TREE)tree->getChild(tree, 0),
                 &relations, &parents, op_map);
      HandleExpr((pANTLR3_BASE_TREE)tree->getChild(tree, 1),
                 &relations, &parents, op_map);
      // Left rel and right rel have the same types. The ouput will have the
      // same types as well.
      vector<Column*> left_rel_cols = relations[0]->get_columns();
      for (vector<Column*>::iterator it = left_rel_cols.begin();
           it != left_rel_cols.end(); ++it) {
        col_types.push_back((*it)->get_type());
      }
      pANTLR3_BASE_TREE output_node =
        (pANTLR3_BASE_TREE)tree->getChild(tree, cnt - 1);
      PrintNode(output_node);
      string output_relation = string(reinterpret_cast<char*>(
          output_node->getText(output_node)->chars));
      RelationsType::relations_type[output_relation] = col_types;
      IntersectionOperator* intersection_operator =
        new IntersectionOperator(FLAGS_hdfs_input_dir, relations,
                                 ConstructRelation(output_relation));
      shared_ptr<OperatorNode> op_node = AddChild(shared_ptr<OperatorNode> (
          new OperatorNode(intersection_operator, parents)));
      (*op_map)[output_relation] = op_node;
      return op_node;
    } else {
      LOG(ERROR) << "Unexpected number of children in INTERSECTION";
    }
    return shared_ptr<OperatorNode>();
  }

  shared_ptr<OperatorNode> TreeTraversal::HandleJoin(pANTLR3_BASE_TREE tree,
      rel_node* op_map) {
    vector<uint16_t> col_types;
    vector<Relation*> relations;
    op_nodes parents;
    uint32_t cnt = tree->getChildCount(tree);
    HandleExpr((pANTLR3_BASE_TREE)tree->getChild(tree, 0),
               &relations, &parents, op_map);
    HandleExpr((pANTLR3_BASE_TREE)tree->getChild(tree, 1),
               &relations, &parents, op_map);
    uint32_t child_num;
    vector<Column*> left_cols;
    for (child_num = 2; child_num < cnt; child_num++) {
      pANTLR3_BASE_TREE col_node = (pANTLR3_BASE_TREE)tree->getChild(tree, child_num);
      pANTLR3_COMMON_TOKEN col_token = col_node->getToken(col_node);
      if (col_token->type == ENDCOLS) {
        // Reached the end of the left columns.
        break;
      } else if (col_token->type == ATTRIBUTE) {
        PrintNode(col_node);
        left_cols.push_back(
            GenColumn(string(reinterpret_cast<char*>(col_node->getText(col_node)->chars))));
      } else {
        LOG(FATAL) << "Unexpected column on the left side of JOIN";
      }
    }

    if (child_num == cnt) {
      LOG(FATAL) << "There are no columns on the right side of JOIN";
    }

    vector<Column*> right_cols;
    set<int32_t> right_join_col_indices;
    for (++child_num; child_num < cnt - 1; child_num++) {
      pANTLR3_BASE_TREE col_node = (pANTLR3_BASE_TREE)tree->getChild(tree, child_num);
      pANTLR3_COMMON_TOKEN col_token = col_node->getToken(col_node);
      if (col_token->type == ENDCOLS) {
        // Reached the end of the right columns.
        break;
      } else if (col_token->type == ATTRIBUTE) {
        PrintNode(col_node);
        Column* col =
          GenColumn(string(reinterpret_cast<char*>(col_node->getText(col_node)->chars)));
        right_join_col_indices.insert(col->get_index());
        right_cols.push_back(col);
      } else {
        LOG(FATAL) << "Unexpected column on the right side of JOIN";
      }
    }

    if (left_cols.size() != right_cols.size()) {
      LOG(FATAL) << "The numbers of columns on which we are joining are not equal";
    }

    // All the columns from the left relation are included in the join.
    vector<Column*> left_rel_cols = relations[0]->get_columns();
    for (vector<Column*>::iterator it = left_rel_cols.begin();
         it != left_rel_cols.end(); ++it) {
      col_types.push_back((*it)->get_type());
    }
    // All the columns from the right relation, except the ones used for join,
    // are included in the join.
    vector<Column*> right_rel_cols = relations[1]->get_columns();
    for (vector<Column*>::iterator it = right_rel_cols.begin();
         it != right_rel_cols.end(); ++it) {
      if (right_join_col_indices.find((*it)->get_index()) == right_join_col_indices.end()) {
        col_types.push_back((*it)->get_type());
      }
    }

    pANTLR3_BASE_TREE output_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, cnt - 1);
    PrintNode(output_node);
    string output_relation =
      string(reinterpret_cast<char*>(output_node->getText(output_node)->chars));
    RelationsType::relations_type[output_relation] = col_types;
    JoinOperator* join_operator =
      new JoinOperator(FLAGS_hdfs_input_dir, relations, left_cols, right_cols,
                       ConstructRelation(output_relation));
    shared_ptr<OperatorNode> op_node =
      AddChild(shared_ptr<OperatorNode>(new OperatorNode(join_operator, parents)));
    (*op_map)[output_relation] = op_node;
    return op_node;
  }

  shared_ptr<OperatorNode> TreeTraversal::HandleMax(pANTLR3_BASE_TREE tree,
                                                    rel_node* op_map) {
    vector<uint16_t> col_types;
    vector<Relation*> relations;
    op_nodes parents;
    vector<Column*> group_bys;
    uint32_t cnt = tree->getChildCount(tree);
    uint32_t child_num;
    vector<Column*> selected_columns;
    Column* max_column;
    for (child_num = 0; child_num < cnt - 1; child_num++) {
      pANTLR3_BASE_TREE expr_node =
        (pANTLR3_BASE_TREE)tree->getChild(tree, child_num);
      pANTLR3_COMMON_TOKEN expr_token = expr_node->getToken(expr_node);
      if (expr_token->type == ENDCOLS) {
        // We've reached the columns-relations delimiter.
        break;
      } else {
        if (expr_token->type == ATTRIBUTE) {
          PrintNode(expr_node);
          if (child_num == 0) {
            max_column = GenColumn(string(reinterpret_cast<char*>(
                expr_node->getText(expr_node)->chars)));
          } else {
            Column* column = GenColumn(string(reinterpret_cast<char*>(
                expr_node->getText(expr_node)->chars)));
            selected_columns.push_back(column);
          }
        } else {
          LOG(ERROR) << "Unexpected column in MIN: " << expr_token->type;
          return shared_ptr<OperatorNode>();
        }
      }
    }
    pANTLR3_BASE_TREE condition_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, ++child_num);
    PrintNode(condition_node);
    child_num = LoopExpr(tree, ++child_num, cnt - 2, &relations, &parents, op_map);
    GenGroupBys(tree, child_num + 1, cnt - 1, &group_bys, &col_types);
    col_types.push_back(max_column->get_type());
    // Add selected columns types.
    for (vector<Column*>::iterator it = selected_columns.begin();
         it != selected_columns.end(); ++it) {
      col_types.push_back((*it)->get_type());
    }
    pANTLR3_BASE_TREE output_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, cnt - 1);
    PrintNode(output_node);
    string output_relation = string(reinterpret_cast<char*>(
        output_node->getText(output_node)->chars));
    RelationsType::relations_type[output_relation] = col_types;
    MaxOperator* max_operator =
      new MaxOperator(FLAGS_hdfs_input_dir, condition_node, group_bys, relations,
                      selected_columns, max_column,
                      ConstructRelation(output_relation));
    shared_ptr<OperatorNode> op_node = AddChild(shared_ptr<OperatorNode> (
        new OperatorNode(max_operator, parents)));
    (*op_map)[output_relation] = op_node;
    return op_node;
  }

  shared_ptr<OperatorNode> TreeTraversal::HandleMin(pANTLR3_BASE_TREE tree,
                                                    rel_node* op_map) {
    vector<uint16_t> col_types;
    vector<Relation*> relations;
    op_nodes parents;
    vector<Column*> group_bys;
    uint32_t cnt = tree->getChildCount(tree);
    uint32_t child_num;
    vector<Column*> selected_columns;
    Column* min_column;
    for (child_num = 0; child_num < cnt - 1; child_num++) {
      pANTLR3_BASE_TREE expr_node =
        (pANTLR3_BASE_TREE)tree->getChild(tree, child_num);
      pANTLR3_COMMON_TOKEN expr_token = expr_node->getToken(expr_node);
      if (expr_token->type == ENDCOLS) {
        // We've reached the columns-relations delimiter.
        break;
      } else {
        if (expr_token->type == ATTRIBUTE) {
          PrintNode(expr_node);
          if (child_num == 0) {
            min_column = GenColumn(string(reinterpret_cast<char*>(
                expr_node->getText(expr_node)->chars)));
          } else {
            Column* column = GenColumn(string(reinterpret_cast<char*>(
                expr_node->getText(expr_node)->chars)));
            selected_columns.push_back(column);
          }
        } else {
          LOG(ERROR) << "Unexpected column in MIN: " << expr_token->type;
          return shared_ptr<OperatorNode>();
        }
      }
    }
    pANTLR3_BASE_TREE condition_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, ++child_num);
    PrintNode(condition_node);
    child_num = LoopExpr(tree, ++child_num, cnt - 2, &relations, &parents, op_map);
    GenGroupBys(tree, child_num + 1, cnt - 1, &group_bys, &col_types);
    col_types.push_back(min_column->get_type());
    // Add selected columns types.
    for (vector<Column*>::iterator it = selected_columns.begin();
         it != selected_columns.end(); ++it) {
      col_types.push_back((*it)->get_type());
    }
    pANTLR3_BASE_TREE output_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, cnt - 1);
    PrintNode(output_node);
    string output_relation = string(reinterpret_cast<char*>(
        output_node->getText(output_node)->chars));
    RelationsType::relations_type[output_relation] = col_types;
    MinOperator* min_operator =
      new MinOperator(FLAGS_hdfs_input_dir, condition_node, group_bys, relations,
                      selected_columns, min_column,
                      ConstructRelation(output_relation));
    shared_ptr<OperatorNode> op_node = AddChild(shared_ptr<OperatorNode> (
        new OperatorNode(min_operator, parents)));
    (*op_map)[output_relation] = op_node;
    return op_node;
  }

  shared_ptr<OperatorNode> TreeTraversal::HandleProject(pANTLR3_BASE_TREE tree,
                                                        rel_node* op_map) {
    vector<uint16_t> col_types;
    vector<Column*> columns;
    vector<Relation*> relations;
    op_nodes parents;
    uint32_t cnt = tree->getChildCount(tree);
    uint32_t child_num;
    for (child_num = 0; child_num < cnt - 1; child_num++) {
      pANTLR3_BASE_TREE expr_node =
        (pANTLR3_BASE_TREE)tree->getChild(tree, child_num);
      pANTLR3_COMMON_TOKEN expr_token = expr_node->getToken(expr_node);
      if (expr_token->type == ENDCOLS) {
        // We've reached the columns-relations delimiter.
        break;
      } else {
        if (expr_token->type == ATTRIBUTE) {
          PrintNode(expr_node);
          Column* column = GenColumn(string(reinterpret_cast<char*>(
            expr_node->getText(expr_node)->chars)));
          columns.push_back(column);
        } else {
          LOG(ERROR) << "Unexpected column in PROJECT: " << expr_token->type;
          return shared_ptr<OperatorNode>();
        }
      }
    }
    pANTLR3_BASE_TREE condition_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, ++child_num);
    PrintNode(condition_node);
    LoopExpr(tree, ++child_num, cnt - 1, &relations, &parents, op_map);
    for (vector<Column*>::iterator it = columns.begin();
         it != columns.end(); ++it) {
      col_types.push_back((*it)->get_type());
    }
    pANTLR3_BASE_TREE output_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, cnt - 1);
    PrintNode(output_node);
    string output_relation = string(reinterpret_cast<char*>(
        output_node->getText(output_node)->chars));
    RelationsType::relations_type[output_relation] = col_types;
    ProjectOperator* project_operator =
      new ProjectOperator(FLAGS_hdfs_input_dir, condition_node, relations, columns,
                          ConstructRelation(output_relation));
    shared_ptr<OperatorNode> op_node = AddChild(shared_ptr<OperatorNode> (
        new OperatorNode(project_operator, parents)));
    (*op_map)[output_relation] = op_node;
    return op_node;
  }

  shared_ptr<OperatorNode> TreeTraversal::HandleMul(pANTLR3_BASE_TREE tree,
                                                    rel_node* op_map) {
    uint32_t cnt = tree->getChildCount(tree);
    vector<Value*> values;
    vector<Relation*> relations;
    op_nodes parents;
    pANTLR3_BASE_TREE left_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 0);
    PrintNode(left_node);
    pANTLR3_COMMON_TOKEN token = left_node->getToken(left_node);
    Column* left_column = NULL;
    Column* right_column = NULL;
    if (token->type == ATTRIBUTE) {
      // Check if column.
      left_column = GenColumn(string(reinterpret_cast<char*>(
        left_node->getText(left_node)->chars)));
      values.push_back(left_column);
    } else {
      if (token->type == INT_VALUE) {
        values.push_back(new Value(
          string(reinterpret_cast<char*>(
            left_node->getText(left_node)->chars)), INTEGER_TYPE));
      } else if (token->type == DOUBLE_VALUE) {
        values.push_back(new Value(
          string(reinterpret_cast<char*>(
            left_node->getText(left_node)->chars)), DOUBLE_TYPE));
      } else {
        LOG(ERROR) << "MUL only operates on columns or constans.";
        return shared_ptr<OperatorNode>();
      }
    }
    pANTLR3_BASE_TREE right_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 1);
    PrintNode(right_node);
    token = right_node->getToken(right_node);
    if (token->type == ATTRIBUTE) {
      right_column = GenColumn(string(reinterpret_cast<char*>(
        right_node->getText(right_node)->chars)));
      values.push_back(right_column);
    } else {
      if (token->type == INT_VALUE) {
        values.push_back(new Value(
          string(reinterpret_cast<char*>(
            right_node->getText(right_node)->chars)), INTEGER_TYPE));
      } else if (token->type == DOUBLE_VALUE) {
        values.push_back(new Value(
          string(reinterpret_cast<char*>(
            right_node->getText(right_node)->chars)), DOUBLE_TYPE));
      } else {
        LOG(ERROR) << "MUL only operates on columns or constans.";
        return shared_ptr<OperatorNode>();
      }
    }
    pANTLR3_BASE_TREE condition_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 2);
    LoopExpr(tree, 3, cnt - 1, &relations, &parents, op_map);
    PrintNode(condition_node);
    pANTLR3_BASE_TREE output_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, cnt - 1);
    PrintNode(output_node);
    string output_relation = string(reinterpret_cast<char*>(
      output_node->getText(output_node)->chars));
    // MUL only works on one relation. The column types of the output
    // are the same as of the input.
    // TODO(ionel): Support the case when we're multiplying by a float and the
    // type of a column must be updated.
    RelationsType::relations_type[output_relation] =
      RelationsType::relations_type[relations[0]->get_name()];
    if (values[1]->get_type() == DOUBLE_TYPE && left_column != NULL) {
      values[0]->set_type(DOUBLE_TYPE);
      RelationsType::relations_type[output_relation][left_column->get_index()] =
        values[0]->get_type();
    }
    MulOperator* mul_operator =
      new MulOperator(FLAGS_hdfs_input_dir, condition_node, relations, values,
                      ConstructRelation(output_relation));
    shared_ptr<OperatorNode> op_node = AddChild(shared_ptr<OperatorNode> (
        new OperatorNode(mul_operator, parents)));
    (*op_map)[output_relation] = op_node;
    return op_node;
  }

  shared_ptr<OperatorNode> TreeTraversal::HandleSelect(pANTLR3_BASE_TREE tree,
                                                       rel_node* op_map) {
    vector<Relation*> relations;
    vector<Column*> columns_proj;
    op_nodes parents;
    uint32_t cnt = tree->getChildCount(tree);
    uint32_t child_num;
    // Get columns.
    for (child_num = 0; child_num < cnt - 1; child_num++) {
      pANTLR3_BASE_TREE expr_node =
        (pANTLR3_BASE_TREE)tree->getChild(tree, child_num);
      pANTLR3_COMMON_TOKEN expr_token = expr_node->getToken(expr_node);
      if (expr_token->type == ENDCOLS) {
        // We've reached the columns-relations delimiter.
        break;
      } else {
        if (expr_token->type == ATTRIBUTE) {
          PrintNode(expr_node);
          Column* column = GenColumn(string(reinterpret_cast<char*>(
            expr_node->getText(expr_node)->chars)));
          columns_proj.push_back(column);
        } else {
            LOG(ERROR) << "Unexpected column in SELECT: " << expr_token->type;
            return shared_ptr<OperatorNode>();
        }
      }
    }
    pANTLR3_BASE_TREE condition_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, ++child_num);
    PrintNode(condition_node);
    LoopExpr(tree, ++child_num, cnt - 1, &relations, &parents, op_map);
    vector<uint16_t> col_types;
    for (vector<Column*>::iterator it = columns_proj.begin();
         it != columns_proj.end(); ++it) {
      col_types.push_back((*it)->get_type());
    }
    pANTLR3_BASE_TREE output_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, cnt - 1);
    PrintNode(output_node);
    string output_relation = string(reinterpret_cast<char*>(
        output_node->getText(output_node)->chars));
    RelationsType::relations_type[output_relation] = col_types;
    SelectOperator* select_operator =
      new SelectOperator(FLAGS_hdfs_input_dir, condition_node, columns_proj,
                         relations, ConstructRelation(output_relation));
    shared_ptr<OperatorNode> op_node = AddChild(shared_ptr<OperatorNode> (
      new OperatorNode(select_operator, parents)));
    (*op_map)[output_relation] = op_node;
    return op_node;
  }

  shared_ptr<OperatorNode> TreeTraversal::HandleSort(pANTLR3_BASE_TREE tree,
                                                     rel_node* op_map) {
    vector<Relation*> relations;
    op_nodes parents;
    uint32_t cnt = tree->getChildCount(tree);
    pANTLR3_BASE_TREE column_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 0);
    PrintNode(column_node);
    Column* sort_column = GenColumn(string(reinterpret_cast<char*>(
        column_node->getText(column_node)->chars)));
    pANTLR3_BASE_TREE condition_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 1);
    LoopExpr(tree, 2, cnt - 1, &relations, &parents, op_map);
    pANTLR3_BASE_TREE output_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, cnt - 1);
    PrintNode(output_node);
    string output_relation = string(reinterpret_cast<char*>(
        output_node->getText(output_node)->chars));
    // SORT should only operate on a relation. The types of the output are the
    // same as the types of the relation on which it operates.
    RelationsType::relations_type[output_relation] =
      RelationsType::relations_type[relations[0]->get_name()];
    bool increasing = tree->getToken(tree)->type == SORT_INC ? true : false;
    SortOperator* sort_operator =
      new SortOperator(FLAGS_hdfs_input_dir, condition_node, relations, sort_column,
                       increasing, ConstructRelation(output_relation));
    shared_ptr<OperatorNode> op_node = AddChild(shared_ptr<OperatorNode> (
        new OperatorNode(sort_operator, parents)));
    (*op_map)[output_relation] = op_node;
    return op_node;
  }

  shared_ptr<OperatorNode> TreeTraversal::HandleSub(pANTLR3_BASE_TREE tree,
                                                    rel_node* op_map) {
    uint32_t cnt = tree->getChildCount(tree);
    vector<Value*> values;
    vector<Relation*> relations;
    op_nodes parents;
    pANTLR3_BASE_TREE left_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 0);
    PrintNode(left_node);
    pANTLR3_COMMON_TOKEN token = left_node->getToken(left_node);
    Column* left_column = NULL;
    Column* right_column = NULL;
    if (token->type == ATTRIBUTE) {
      // Check if column.
      left_column = GenColumn(string(reinterpret_cast<char*>(
        left_node->getText(left_node)->chars)));
      values.push_back(left_column);
    } else {
      if (token->type == INT_VALUE) {
        values.push_back(new Value(
          string(reinterpret_cast<char*>(
            left_node->getText(left_node)->chars)), INTEGER_TYPE));
      } else if (token->type == DOUBLE_VALUE) {
        values.push_back(new Value(
          string(reinterpret_cast<char*>(
            left_node->getText(left_node)->chars)), DOUBLE_TYPE));
      } else {
        LOG(ERROR) << "SUB only operates on columns or constans.";
        return shared_ptr<OperatorNode>();
      }
    }
    pANTLR3_BASE_TREE right_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 1);
    PrintNode(right_node);
    token = right_node->getToken(right_node);
    if (token->type == ATTRIBUTE) {
      right_column = GenColumn(string(reinterpret_cast<char*>(
        right_node->getText(right_node)->chars)));
      values.push_back(right_column);
    } else {
      if (token->type == INT_VALUE) {
        values.push_back(new Value(
          string(reinterpret_cast<char*>(
            right_node->getText(right_node)->chars)), INTEGER_TYPE));
      } else if (token->type == DOUBLE_VALUE) {
        values.push_back(new Value(
          string(reinterpret_cast<char*>(
            right_node->getText(right_node)->chars)), DOUBLE_TYPE));
      } else {
        LOG(ERROR) << "SUB only operates on columns or constans.";
        return shared_ptr<OperatorNode>();
      }
    }
    pANTLR3_BASE_TREE condition_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 2);
    LoopExpr(tree, 3, cnt - 1, &relations, &parents, op_map);
    PrintNode(condition_node);
    pANTLR3_BASE_TREE output_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, cnt - 1);
    PrintNode(output_node);
    string output_relation = string(reinterpret_cast<char*>(
      output_node->getText(output_node)->chars));
    // SUB only works on one relation. The column types of the output
    // are the same as of the input.
    // TODO(ionel): Support the case when we're subtrating a float and the
    // type of a column must be updated.
    RelationsType::relations_type[output_relation] =
      RelationsType::relations_type[relations[0]->get_name()];
    if (values[1]->get_type() == DOUBLE_TYPE && left_column != NULL) {
      values[0]->set_type(DOUBLE_TYPE);
      RelationsType::relations_type[output_relation][left_column->get_index()] =
        values[0]->get_type();
    }
    SubOperator* sub_operator =
      new SubOperator(FLAGS_hdfs_input_dir, condition_node, relations, values,
                      ConstructRelation(output_relation));
    shared_ptr<OperatorNode> op_node = AddChild(shared_ptr<OperatorNode> (
        new OperatorNode(sub_operator, parents)));
    (*op_map)[output_relation] = op_node;
    return op_node;
  }

  shared_ptr<OperatorNode> TreeTraversal::HandleSum(pANTLR3_BASE_TREE tree,
                                                    rel_node* op_map) {
    uint32_t cnt = tree->getChildCount(tree);
    vector<Value*> values;
    vector<Relation*> relations;
    op_nodes parents;
    vector<uint16_t> col_types;
    pANTLR3_BASE_TREE left_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 0);
    PrintNode(left_node);
    pANTLR3_COMMON_TOKEN token = left_node->getToken(left_node);
    Column* left_column = NULL;
    Column* right_column = NULL;
    if (token->type == ATTRIBUTE) {
      // Check if column.
      left_column = GenColumn(string(reinterpret_cast<char*>(
        left_node->getText(left_node)->chars)));
      values.push_back(left_column);
    } else {
      if (token->type == INT_VALUE) {
        values.push_back(new Value(
          string(reinterpret_cast<char*>(
            left_node->getText(left_node)->chars)), INTEGER_TYPE));
      } else if (token->type == DOUBLE_VALUE) {
        values.push_back(new Value(
          string(reinterpret_cast<char*>(
            left_node->getText(left_node)->chars)), DOUBLE_TYPE));
      } else {
        LOG(ERROR) << "SUM only operates on columns or constans.";
        return shared_ptr<OperatorNode>();
      }
    }
    pANTLR3_BASE_TREE right_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 1);
    PrintNode(right_node);
    token = right_node->getToken(right_node);
    if (token->type == ATTRIBUTE) {
      right_column = GenColumn(string(reinterpret_cast<char*>(
        right_node->getText(right_node)->chars)));
      values.push_back(right_column);
    } else {
      if (token->type == INT_VALUE) {
        values.push_back(new Value(
          string(reinterpret_cast<char*>(
            right_node->getText(right_node)->chars)), INTEGER_TYPE));
      } else if (token->type == DOUBLE_VALUE) {
        values.push_back(new Value(
          string(reinterpret_cast<char*>(
            right_node->getText(right_node)->chars)), DOUBLE_TYPE));
      } else {
        LOG(ERROR) << "SUM only operates on columns or constans.";
        return shared_ptr<OperatorNode>();
      }
    }
    pANTLR3_BASE_TREE condition_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 2);
    // Iterate until penultimate node or until DELIMITER found.
    LoopExpr(tree, 3, cnt - 1, &relations, &parents, op_map);
    PrintNode(condition_node);
    pANTLR3_BASE_TREE output_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, cnt - 1);
    PrintNode(output_node);
    string output_relation = string(reinterpret_cast<char*>(
      output_node->getText(output_node)->chars));
    // SUM only works on one relation. The column types of the output
    // are the same as of the input.
    // TODO(ionel): Support the case when we're subtrating a float and the
    // type of a column must be updated.
    RelationsType::relations_type[output_relation] =
      RelationsType::relations_type[relations[0]->get_name()];
    if (values[1]->get_type() == DOUBLE_TYPE && left_column != NULL) {
      values[0]->set_type(DOUBLE_TYPE);
      RelationsType::relations_type[output_relation][left_column->get_index()] =
        values[0]->get_type();
    }
    SumOperator* sum_operator =
      new SumOperator(FLAGS_hdfs_input_dir, condition_node, relations, values,
                      ConstructRelation(output_relation));
    shared_ptr<OperatorNode> op_node = AddChild(shared_ptr<OperatorNode> (
        new OperatorNode(sum_operator, parents)));
    (*op_map)[output_relation] = op_node;
    return op_node;
  }

  shared_ptr<OperatorNode> TreeTraversal::HandleUdf(pANTLR3_BASE_TREE tree,
                                                    rel_node* op_map) {
    vector<uint16_t> col_types;
    vector<Relation*> relations;
    op_nodes parents;
    uint32_t cnt = tree->getChildCount(tree);
    pANTLR3_BASE_TREE path_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 0);
    PrintNode(path_node);
    string source_path = string(reinterpret_cast<char*>(
      path_node->getText(path_node)->chars));
    pANTLR3_BASE_TREE name_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 1);
    PrintNode(name_node);
    string udf_name = string(reinterpret_cast<char*>(
      name_node->getText(name_node)->chars));
    pANTLR3_BASE_TREE input_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 2);
    PrintNode(input_node);
    string input_relation = string(reinterpret_cast<char*>(
      input_node->getText(input_node)->chars));
    relations.push_back(ConstructRelation(input_relation));
    pANTLR3_BASE_TREE output_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 3);
    PrintNode(output_node);
    string output_relation = string(reinterpret_cast<char*>(
      output_node->getText(output_node)->chars));
    for (uint32_t child_num = 4; child_num < cnt; ++child_num) {
      pANTLR3_BASE_TREE type_node =
        (pANTLR3_BASE_TREE)tree->getChild(tree, child_num);
      col_types.push_back(type_node->getToken(type_node)->type);
    }
    RelationsType::relations_type[output_relation] = col_types;
    UdfOperator* udf_operator =
      new UdfOperator(FLAGS_hdfs_input_dir, relations,
                      ConstructRelation(output_relation), source_path,
                      udf_name);
    shared_ptr<OperatorNode> op_node = AddChild(shared_ptr<OperatorNode> (
        new OperatorNode(udf_operator, parents)));
    (*op_map)[output_relation] = op_node;
    return op_node;
  }

  shared_ptr<OperatorNode> TreeTraversal::HandleUnion(pANTLR3_BASE_TREE tree,
                                                      rel_node* op_map) {
    uint32_t cnt = tree->getChildCount(tree);
    if (cnt == 3) {
      vector<uint16_t> col_types;
      vector<Relation*> relations;
      op_nodes parents;
      HandleExpr((pANTLR3_BASE_TREE)tree->getChild(tree, 0),
                 &relations, &parents, op_map);
      HandleExpr((pANTLR3_BASE_TREE)tree->getChild(tree, 1),
                 &relations, &parents, op_map);
      // Left rel and right rel have the same types. The ouput will have the
      // same types as well.
      vector<Column*> left_rel_cols = relations[0]->get_columns();
      for (vector<Column*>::iterator it = left_rel_cols.begin();
           it != left_rel_cols.end(); ++it) {
        col_types.push_back((*it)->get_type());
      }
      pANTLR3_BASE_TREE output_node =
        (pANTLR3_BASE_TREE)tree->getChild(tree, cnt - 1);
      PrintNode(output_node);
      string output_relation = string(reinterpret_cast<char*>(
          output_node->getText(output_node)->chars));
      RelationsType::relations_type[output_relation] = col_types;
      UnionOperator* union_operator =
        new UnionOperator(FLAGS_hdfs_input_dir, relations,
                          ConstructRelation(output_relation));
      shared_ptr<OperatorNode> op_node = AddChild(shared_ptr<OperatorNode> (
          new OperatorNode(union_operator, parents)));
      (*op_map)[output_relation] = op_node;
      return op_node;
    } else {
      LOG(ERROR) << "Unexpected number of children in UNION";
    }
    return shared_ptr<OperatorNode>();
  }

  shared_ptr<OperatorNode> TreeTraversal::HandleWhile(pANTLR3_BASE_TREE tree,
                                                      rel_node* op_map) {
    vector<Relation*> relations;
    op_nodes children;
    op_nodes body_nodes;
    op_nodes parents;
    uint32_t cnt = tree->getChildCount(tree);
    pANTLR3_BASE_TREE condition_node =
      (pANTLR3_BASE_TREE)tree->getChild(tree, 0);
    PrintNode(condition_node);
    set<string> input_rels;
    // Create new rel_map so that the nodes within the while loop
    // are not connected to nodes outside of the body.
    rel_node* while_op_map = new rel_node();
    for (uint32_t child_num = 1; child_num < cnt; child_num++) {
      pANTLR3_BASE_TREE expr_node =
        (pANTLR3_BASE_TREE)tree->getChild(tree, child_num);
      PrintNode(expr_node);
      // Traverse subquery.
      shared_ptr<OperatorNode> body_node = Traverse(expr_node, while_op_map);
      // Detect the inputs of the while loop.
      string output_rel_name =
        body_node->get_operator()->get_output_relation()->get_name();
      vector<Relation*> op_relations =
        body_node->get_operator()->get_relations();
      for (vector<Relation*>::iterator rel_it = op_relations.begin();
           rel_it != op_relations.end(); ++rel_it) {
        if (!output_rel_name.compare((*rel_it)->get_name())) {
          if (input_rels.find(output_rel_name) == input_rels.end()) {
            relations.push_back(*rel_it);
          }
        }
      }
      input_rels.insert(output_rel_name);
      body_nodes.push_back(body_node);
    }
    // XXX(ionel): HACK! It doesn't have an output_relation. It can have
    // more than one output and they are implicit, in the loop.
    // NOTE: output_while is a fake relation.
    children = GetInputNodes(body_nodes);
    set<string> input_rel_names = set<string>();
    relations = *DetermineInputs(body_nodes, &input_rel_names);
    for (vector<Relation*>::iterator it = relations.begin();
         it != relations.end(); ++it) {
      if (op_map->find((*it)->get_name()) != op_map->end()) {
        parents.push_back((*op_map)[(*it)->get_name()]);
      }
    }
    // TODO(ionel): FIX! Generate unique relation name. Otherwise, we
    // can't support multiple loops in a workflow.
    WhileOperator* while_operator =
      new WhileOperator(FLAGS_hdfs_input_dir, condition_node, relations,
                        ConstructRelation("output_while"));
    shared_ptr<OperatorNode> while_node =
      shared_ptr<OperatorNode>(new OperatorNode(while_operator, parents));
    while_node->set_children_loop(children);
    (*op_map)["output_while"] = while_node;
    set<string> output_rel_names = set<string>();
    DetermineAllRelOutputs(body_nodes, &output_rel_names);
    for (set<string>::iterator it = output_rel_names.begin();
         it != output_rel_names.end(); ++it) {
      (*op_map)[*it] = while_node;
    }
    AddChild(while_node);
    delete while_op_map;
    return AddParent(while_node);
  }

  shared_ptr<OperatorNode> TreeTraversal::AddChild(
      shared_ptr<OperatorNode> child) {
    op_nodes parents = child->get_parents();
    for (op_nodes::iterator it = parents.begin(); it != parents.end(); ++it) {
      (*it)->AddChild(child);
    }
    return child;
  }

  shared_ptr<OperatorNode> TreeTraversal::AddParent(
      shared_ptr<OperatorNode> parent) {
    op_nodes children = parent->get_children();
    for (op_nodes::iterator it = children.begin(); it != children.end(); ++it) {
      (*it)->AddParent(parent);
    }
    op_nodes loop_children =
      parent->get_loop_children();
    for (op_nodes::iterator it = loop_children.begin();
         it != loop_children.end(); ++it) {
      (*it)->AddParent(parent);
    }
    return parent;
  }

  op_nodes TreeTraversal::GetInputNodes(op_nodes nodes) {
    op_nodes input_nodes;
    set<shared_ptr<OperatorNode> > visited;
    queue<shared_ptr<OperatorNode> > to_visit;
    for (op_nodes::iterator it = nodes.begin(); it != nodes.end(); ++it) {
      to_visit.push(*it);
      visited.insert(*it);
    }
    while (!to_visit.empty()) {
      shared_ptr<OperatorNode> cur_node = to_visit.front();
      to_visit.pop();
      op_nodes parents = cur_node->get_parents();
      if (parents.size() == 0) {
        input_nodes.push_back(cur_node);
      }
      for (op_nodes::iterator it = parents.begin(); it != parents.end(); ++it) {
        if (visited.insert(*it).second) {
          to_visit.push(*it);
        }
      }
    }
    return input_nodes;
  }

  Relation* TreeTraversal::ConstructRelation(string relation) {
    unordered_map<string, vector<uint16_t> >::iterator rel_it =
      RelationsType::relations_type.find(relation);
    vector<Column*> columns;
    if (rel_it != RelationsType::relations_type.end()) {
      // Create the columns with their types.
      uint32_t index = 0;
      for (vector<uint16_t>::iterator type_it = rel_it->second.begin();
           type_it != rel_it->second.end(); ++type_it, ++index) {
        columns.push_back(new Column(relation, index, *type_it));
      }
      return new Relation(relation, columns);
    } else {
      LOG(ERROR) << "Relation: " << relation << " is not defined";
      // TODO(ionel): Handle this case (e.g. kill workflow).
      return new Relation(relation, columns);
    }
  }

} // namespace musketeer
