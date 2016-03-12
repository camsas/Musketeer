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

#ifndef MUSKETEER_MIN_OPERATOR_H
#define MUSKETEER_MIN_OPERATOR_H

#include "ir/operator_interface.h"

#include <stdint.h>

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "base/common.h"
#include "ir/column.h"
#include "ir/condition_tree.h"
#include "ir/relation.h"
#include "RLPlusLexer.h"
#include "RLPlusParser.h"

namespace musketeer {
namespace ir {

class MinOperator : public OperatorInterface {
 public:
  MinOperator(const string& input_dir, const pANTLR3_BASE_TREE condition_tree,
              const vector<Column*>& group_bys_,
              const vector<Relation*>& relations,
              const vector<Column*>& selected_columns_,
              Column* column_, Relation* output_rel):
    OperatorInterface(input_dir, relations, output_rel,
                      new ConditionTree(condition_tree)),
      group_bys(group_bys_), selected_columns(selected_columns_),
      column(column_) {
  }

  MinOperator(const string& input_dir,
              const vector<pANTLR3_BASE_TREE>& condition_tree,
              const vector<Column*>& group_bys_,
              const vector<Relation*>& relations,
              const vector<Column*>& selected_columns_,
              Column* column_, Relation* output_rel):
    OperatorInterface(input_dir, relations, output_rel,
                      new ConditionTree(condition_tree)),
      group_bys(group_bys_), selected_columns(selected_columns_),
      column(column_) {
  }

  MinOperator(const string& input_dir, ConditionTree* condition_tree,
              const vector<Column*>& group_bys_,
              const vector<Relation*>& relations,
              const vector<Column*>& selected_columns_,
              Column* column_, Relation* output_rel):
    OperatorInterface(input_dir, relations, output_rel, condition_tree),
      group_bys(group_bys_), selected_columns(selected_columns_),
      column(column_) {
  }

  ~MinOperator() {
    for (vector<Column*>::iterator it = group_bys.begin();
         it != group_bys.end(); ++it) {
      delete *it;
    }
    group_bys.clear();
    for (vector<Column*>::iterator it = selected_columns.begin();
         it != selected_columns.end(); ++it) {
      delete *it;
    }
    selected_columns.clear();
    delete column;
  }

  vector<Column*> get_group_bys();
  Column* get_column();
  vector<Column*> get_selected_columns();
  OperatorType get_type();
  bool hasAction();
  bool hasGroupby();
  bool mapOnly();
  pair<uint64_t, uint64_t> get_output_size(
      map<string, pair<uint64_t, uint64_t> >* rel_size);
  OperatorInterface* clone();

 private:
  vector<Column*> group_bys;
  vector<Column*> selected_columns;
  Column* column;
};

} // namespace ir
} // namespace musketeer
#endif
