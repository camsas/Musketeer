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

#ifndef MUSKETEER_SUB_OPERATOR_H
#define MUSKETEER_SUB_OPERATOR_H

#include "ir/operator_interface.h"

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "base/common.h"
#include "ir/column.h"
#include "ir/condition_tree.h"
#include "ir/relation.h"
#include "ir/value.h"
#include "RLPlusLexer.h"
#include "RLPlusParser.h"

namespace musketeer {
namespace ir {

class SubOperator : public OperatorInterface {
 public:
  SubOperator(const string& input_dir, const pANTLR3_BASE_TREE condition_tree,
              const vector<Relation*>& relations, const vector<Value*>& values_,
              Relation* output_rel):
    OperatorInterface(input_dir, relations, output_rel,
                      new ConditionTree(condition_tree)), values(values_) {
  }

  SubOperator(const string& input_dir,
              const vector<pANTLR3_BASE_TREE>& condition_tree,
              const vector<Relation*>& relations, const vector<Value*>& values_,
              Relation* output_rel):
    OperatorInterface(input_dir, relations, output_rel,
                      new ConditionTree(condition_tree)), values(values_) {
  }

  SubOperator(const string& input_dir, ConditionTree* condition_tree,
              const vector<Relation*>& relations, const vector<Value*>& values_,
              Relation* output_rel):
    OperatorInterface(input_dir, relations, output_rel, condition_tree),
      values(values_) {
  }

  ~SubOperator() {
    for (vector<Value*>::iterator it = values.begin();
         it != values.end(); ++it) {
      delete *it;
    }
    values.clear();
  }

  vector<Value*> get_values();
  OperatorType get_type();
  bool mapOnly();
  pair<uint64_t, uint64_t> get_output_size(
      map<string, pair<uint64_t, uint64_t> >* rel_size);
  OperatorInterface* clone();

 private:
  vector<Value*> values;
};

} // namespace ir
} // namespace musketeer
#endif
