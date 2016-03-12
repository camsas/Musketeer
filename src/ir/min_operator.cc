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

#include "ir/min_operator.h"

#include <limits>
#include <map>
#include <utility>
#include <vector>

namespace musketeer {
namespace ir {

  vector<Column*> MinOperator::get_group_bys() {
    return group_bys;
  }

  Column* MinOperator::get_column() {
    return column;
  }

  vector<Column*> MinOperator::get_selected_columns() {
    return selected_columns;
  }

  OperatorType MinOperator::get_type() {
    return MIN_OP;
  }

  bool MinOperator::hasGroupby() {
    if ((group_bys.size() == 1 && group_bys[0]->get_relation().empty()) ||
        group_bys.size() == 0) {
      return false;
    } else {
      return true;
    }
  }

  pair<uint64_t, uint64_t> MinOperator::get_output_size(
      map<string, pair<uint64_t, uint64_t> >* rel_size) {
    vector<Relation*> rels = get_relations();
    string input_rel = rels[0]->get_name();
    pair<uint64_t, uint64_t> min_rel_size;
    if (rel_size->find(input_rel) != rel_size->end()) {
      min_rel_size = make_pair(1, (*rel_size)[input_rel].second);
    } else {
      // This should not happen.
      LOG(INFO) << "Called out of order";
      min_rel_size = make_pair(1, numeric_limits<uint64_t>::max());
    }
    return UpdateIfSmaller(get_output_relation()->get_name(), min_rel_size,
                           rel_size);
  }

  bool MinOperator::hasAction() {
    return !hasGroupby();
  }

  bool MinOperator::mapOnly() {
    return false;
  }

  OperatorInterface* MinOperator::clone() {
    return new MinOperator(get_input_dir(), get_condition_tree(), group_bys,
                           get_relations(), get_selected_columns(),
                           column, get_output_relation());
  }

} // namespace ir
} // namespace musketeer
