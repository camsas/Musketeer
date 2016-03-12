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

#ifndef MUSKETEER_CONDITION_TREE_H
#define MUSKETEER_CONDITION_TREE_H

#include <set>
#include <string>
#include <vector>

#include "base/common.h"
#include "frontends/relations_type.h"
#include "ir/column.h"
#include "ir/value.h"
#include "RLPlusLexer.h"
#include "RLPlusParser.h"

namespace musketeer {
namespace ir {

class CondOperator {
 public:
  explicit CondOperator(string cond_operator_): cond_operator(cond_operator_) {
  }

  explicit CondOperator(const pANTLR3_BASE_TREE node) {
    pANTLR3_COMMON_TOKEN token = node->getToken(node);
    switch (token->type) {
    case AND: {
      cond_operator = "&&";
      break;
    }
    case OR: {
      cond_operator = "||";
      break;
    }
    case NOT: {
      cond_operator = "!";
      break;
    }
    case EQUAL: {
      cond_operator = "==";
      break;
    }
    case NOT_EQUAL: {
      cond_operator = "!=";
      break;
    }
    default: {
      // For the other tokens the text is the same as the Java/C++ syntax.
      cond_operator = reinterpret_cast<char*>(node->getText(node)->chars);
    }
    }
  }

  string toString() {
    return cond_operator;
  }

 private:
  string cond_operator;
};

class ConditionTree {
 public:
  explicit ConditionTree(Value* value_): cond_operator(NULL), left(NULL), right(NULL),
      column(NULL), value(value_) {
  }

  explicit ConditionTree(Column* column_): cond_operator(NULL), left(NULL), right(NULL),
      column(column_), value(NULL) {
  }

  explicit ConditionTree(CondOperator* cond_operator_, ConditionTree* left_cond,
                         ConditionTree* right_cond): cond_operator(cond_operator_),
      left(left_cond), right(right_cond), column(NULL), value(NULL) {
  }

  explicit ConditionTree(const vector<pANTLR3_BASE_TREE>& cond_trees): cond_operator(NULL),
      left(NULL), right(NULL), column(NULL), value(NULL) {
      createTree(cond_trees);
  }

  explicit ConditionTree(const pANTLR3_BASE_TREE cond_tree): cond_operator(NULL),
      left(NULL), right(NULL), column(NULL), value(NULL) {
      createTree(cond_tree);
  }

  ~ConditionTree() {
    if (cond_operator != NULL) {
      delete cond_operator;
    }
    if (left != NULL) {
      delete left;
    }
    if (right != NULL) {
      delete right;
    }
    if (value != NULL) {
      delete value;
    }
    if (column != NULL) {
      delete column;
    }
  }

  bool isValue();
  bool isColumn();
  bool isUnary();
  bool isBinary();
  ConditionTree* get_left();
  ConditionTree* get_right();
  CondOperator* get_cond_operator();
  Value* get_value();
  Column* get_column();
  bool checkCondition();
  bool checkCondition(int iter);
  bool checkCondition(Value* value);
  bool checkCondition(Column* column);
  bool checkConditionArithmetic();
  int getNumIterations();
  void getRelNames(set<string>* rel_names);

  // Returns a string that will be plugged in the template for filtering out
  // rows. If the type of the node is EMPTY_NODE then the operator doesn't
  // have a condition node. Hence, just return the appropriate empty
  // condition. (e.g. True for Java code).
  string toString(const string& fmw);

 private:
  void createTree(const vector<pANTLR3_BASE_TREE>& cond_trees);
  void createTree(const pANTLR3_BASE_TREE cond_tree);

  CondOperator* cond_operator;
  ConditionTree* left;
  ConditionTree* right;
  Column* column;
  Value* value;
};

} // namespace ir
} // namespace musketeer
#endif
