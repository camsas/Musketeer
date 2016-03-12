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

#include "ir/condition_tree.h"

#include "base/hdfs_utils.h"

namespace musketeer {
namespace ir {

  bool ConditionTree::isValue() {
    return value != NULL;
  }

  bool ConditionTree::isColumn() {
    return column != NULL;
  }

  bool ConditionTree::isUnary() {
    return !isValue() && !isColumn() && left != NULL && right == NULL;
  }

  bool ConditionTree::isBinary() {
    return left != NULL && right != NULL;
  }

  ConditionTree* ConditionTree::get_left() {
    return left;
  }

  ConditionTree* ConditionTree::get_right() {
    return right;
  }

  Value* ConditionTree::get_value() {
    return value;
  }

  Column* ConditionTree::get_column() {
    return column;
  }

  CondOperator* ConditionTree::get_cond_operator() {
    return cond_operator;
  }

  void ConditionTree::createTree(const vector<pANTLR3_BASE_TREE>& cond_trees) {
    if (cond_trees.size() == 1) {
      createTree(cond_trees[0]);
    } else {
      ConditionTree* prev_tree = NULL;
      for (vector<pANTLR3_BASE_TREE>::size_type index = 0;
           index != cond_trees.size() - 1; index++) {
        if (prev_tree == NULL) {
          prev_tree = new ConditionTree(cond_trees[index]);
        } else {
          prev_tree = new ConditionTree(new CondOperator("&&"), prev_tree,
                                        new ConditionTree(cond_trees[index]));
        }
      }
      cond_operator = new CondOperator("&&");
      left = prev_tree;
      right = new ConditionTree(cond_trees.back());
    }
  }

  void ConditionTree::createTree(const pANTLR3_BASE_TREE cond_tree) {
    uint32_t child_cnt = cond_tree->getChildCount(cond_tree);
    pANTLR3_COMMON_TOKEN token = cond_tree->getToken(cond_tree);
    switch (child_cnt) {
    case 0: {
      switch (token->type) {
      case ATTRIBUTE: {
        string attribute =
          string(reinterpret_cast<char*>(cond_tree->getText(cond_tree)->chars));
        uint32_t pos = attribute.rfind("_");
        string rel_name = attribute.substr(0, pos);
        pos = atoi(attribute.substr(pos + 1, string::npos).c_str());
        unordered_map<string, vector<uint16_t> >::iterator rel_it =
          RelationsType::relations_type.find(rel_name);
        // TODO(ionel): DO NOT ACCESS relations_type directly.
        if (rel_it == RelationsType::relations_type.end()) {
          LOG(ERROR) << "Relation: " << rel_name << " is undefined";
          return;
        }
        this->column = new Column(rel_name, pos, rel_it->second[pos]);
        break;
      }
      case INT_VALUE: {
        this->value =
          new Value(string(reinterpret_cast<char*>(cond_tree->getText(cond_tree)->chars)),
                    INTEGER_TYPE);
        break;
      }
      case DOUBLE_VALUE: {
        this->value =
          new Value(string(reinterpret_cast<char*>(cond_tree->getText(cond_tree)->chars)),
                    DOUBLE_TYPE);
        break;
      }
      case STRING_VALUE: {
        this->value =
          new Value(string(reinterpret_cast<char*>(cond_tree->getText(cond_tree)->chars)),
                    STRING_TYPE);
        break;
      }
      case EMPTY_NODE: {
        this->value = new Value("true", BOOLEAN_TYPE);
        break;
      }
      default: {
        LOG(ERROR) << "Unexpected AST node type: " << token->type;
        return;
      }
      }
      break;
    }
    case 1: {
      // The not case
      this->cond_operator = new CondOperator(cond_tree);
      this->left =
        new ConditionTree((pANTLR3_BASE_TREE)cond_tree->getChild(cond_tree, 0));
      break;
    }
    case 2: {
      this->left =
        new ConditionTree((pANTLR3_BASE_TREE)cond_tree->getChild(cond_tree, 0));
      this->right =
        new ConditionTree((pANTLR3_BASE_TREE)cond_tree->getChild(cond_tree, 1));
      this->cond_operator = new CondOperator(cond_tree);
      break;
    }
    default: {
      LOG(ERROR) << "Unexpected number of children in AST condition node: "
                 << token->type;
      return;
    }
    }
  }

  string ConditionTree::toString(const string& fmw) {
    if (isValue()) {
      return value->get_value();
    }
    if (isColumn()) {
      return column->toString(fmw);
    }
    if (isUnary()) {
      return " (" + cond_operator->toString() + left->toString(fmw) + ") ";
    }
    if (!cond_operator->toString().compare("==")) {
      if (!fmw.compare("hadoop") || !fmw.compare("spark")) {
        return " (" + left->toString(fmw) + ".equals(" +
          right->toString(fmw) + ")) ";
      }
      if (!fmw.compare("naiad")) {
        return " (" + left->toString(fmw) + " == " + right->toString(fmw) + ")";
      }
      if (!fmw.compare("graphchi") || !fmw.compare("metis") ||
          !fmw.compare("powergraph") || !fmw.compare("wildcherry")) {
        if ((left->isValue() && left->value->get_type() == STRING_TYPE) ||
            (left->isColumn() && left->column->get_type() == STRING_TYPE)) {
          return " (" + left->toString(fmw) + ".compare(" +
            right->toString(fmw) + ")) ";
        } else {
          return " (" + left->toString(fmw) + " " + cond_operator->toString() +
            " " + right->toString(fmw) + ") ";
        }
      }
      LOG(ERROR) << "Unexpected framework argument: " << fmw;
      return NULL;
    }
    return " (" + left->toString(fmw) + " " + cond_operator->toString() + " " +
      right->toString(fmw) + ") ";
  }

  bool ConditionTree::checkCondition(Value* value) {
    string value_str = value->get_value();
    if (value->get_type() == BOOLEAN_TYPE) {
      return value_str.compare("true") == 0 ? true : false;
    }
    if (value->get_type() == INTEGER_TYPE) {
      return value_str.compare("0") == 0 ? true : false;
    }
    return false;
  }

  // TODO(ionel): Extend. At the moment it assumens that the relation
  // only has one column.
  bool ConditionTree::checkCondition(Column* column) {
    string value_str = GetHdfsRelValue(column->get_relation());
    return checkCondition(new Value(value_str, column->get_type()));
  }

  // TODO(ionel): Add support for double.
  bool ConditionTree::checkConditionArithmetic() {
    int left_val;
    int right_val;
    if (left->isValue()) {
      left_val = atoi(left->get_value()->get_value().c_str());
    } else if (left->isColumn()) {
      left_val = atoi(GetHdfsRelValue(left->get_column()->get_relation()).c_str());
    } else {
      LOG(ERROR) << "Unexpected type of left condition subtree";
      return false;
    }
    if (right->isValue()) {
      right_val = atoi(right->get_value()->get_value().c_str());
    } else if (right->isColumn()) {
      right_val = atoi(GetHdfsRelValue(right->get_column()->get_relation()).c_str());
    } else {
      LOG(ERROR) << "Unexpected type of right condition subtree";
      return false;
    }
    string op = cond_operator->toString();
    if (!op.compare("<")) {
      return left_val < right_val;
    }
    if (!op.compare(">")) {
      return left_val > right_val;
    }
    if (!op.compare(">=")) {
      return left_val >= right_val;
    }
    if (!op.compare("<=")) {
      return left_val <= right_val;
    }
    if (!op.compare("==")) {
      return left_val == right_val;
    }
    if (!op.compare("!=")) {
      return left_val != right_val;
    }
    LOG(ERROR) << "Unexpected type of operator";
    return false;
  }

  bool ConditionTree::checkCondition() {
    if (isValue()) {
      return checkCondition(value);
    }
    if (isColumn()) {
      return checkCondition(column);
    }
    string op = cond_operator->toString();
    if (!op.compare("&&")) {
      return left->checkCondition() && right->checkCondition();
    }
    if (!op.compare("||")) {
      return left->checkCondition() || right->checkCondition();
    }
    return checkConditionArithmetic();
  }

  // TODO(ionel): FIX! This is an ugly hack just to get iter in the scheduler.
  // It only checks how the argument compare against the constant in the
  // condition.
  bool ConditionTree::checkCondition(int iter) {
    int left_val;
    int right_val;
    if (left->isValue()) {
      left_val = atoi(left->get_value()->get_value().c_str());
      right_val = iter;
    } else {
      right_val = atoi(right->get_value()->get_value().c_str());
      left_val = iter;
    }
    string op = cond_operator->toString();
    if (!op.compare("<")) {
      return left_val < right_val;
    }
    if (!op.compare(">")) {
      return left_val > right_val;
    }
    if (!op.compare(">=")) {
      return left_val >= right_val;
    }
    if (!op.compare("<=")) {
      return left_val <= right_val;
    }
    if (!op.compare("=")) {
      return left_val == right_val;
    }
    if (!op.compare("<>")) {
      return left_val != right_val;
    }
    LOG(ERROR) << "Unexpected type of operator";
    return false;
  }

  int ConditionTree::getNumIterations() {
    int iter = 0;
    while (checkCondition(iter)) {
      iter++;
    }
    return iter;
  }

  void ConditionTree::getRelNames(set<string>* rel_names) {
    if (isValue()) {
      return;
    }
    if (isColumn()) {
      rel_names->insert(column->get_relation());
      return;
    }
    left->getRelNames(rel_names);
    if (isBinary()) {
      right->getRelNames(rel_names);
    }
  }

} // namespace ir
} // namespace musketeer
