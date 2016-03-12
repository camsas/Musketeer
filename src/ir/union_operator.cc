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

#include "ir/union_operator.h"

namespace musketeer {
namespace ir {

  OperatorType UnionOperator::get_type() {
    return UNION_OP;
  }

  pair<uint64_t, uint64_t> UnionOperator::get_output_size(
      map<string, pair<uint64_t, uint64_t> >* rel_size) {
    vector<Relation*> rels = get_relations();
    uint64_t left_max_size = 0;
    uint64_t right_max_size = 0;
    string left_input_rel = rels[0]->get_name();
    string right_input_rel = rels[1]->get_name();
    if (rel_size->find(left_input_rel) != rel_size->end()) {
      left_max_size = (*rel_size)[left_input_rel].second;
    } else {
      // This should not happen.
      LOG(INFO) << "Called out of order";
      left_max_size = numeric_limits<uint64_t>::max();
    }
    if (rel_size->find(right_input_rel) != rel_size->end()) {
      right_max_size = (*rel_size)[right_input_rel].second;
    } else {
      // This should not happen.
      LOG(INFO) << "Called out of order";
      right_max_size = numeric_limits<uint64_t>::max();
    }
    if (left_max_size == numeric_limits<uint64_t>::max() ||
        right_max_size == numeric_limits<uint64_t>::max()) {
      pair<uint64_t, uint64_t> union_rel_size =
        make_pair(0, numeric_limits<uint64_t>::max());
      return UpdateIfSmaller(get_output_relation()->get_name(), union_rel_size,
                             rel_size);
    } else {
      uint64_t max_size = SumNoOverflow(left_max_size, right_max_size);
      pair<uint64_t, uint64_t> union_rel_size = make_pair(0, max_size);
      return UpdateIfSmaller(get_output_relation()->get_name(), union_rel_size,
                             rel_size);
    }
  }

  bool UnionOperator::mapOnly() {
    return true;
  }

  OperatorInterface* UnionOperator::clone() {
    return new UnionOperator(get_input_dir(), get_relations(),
                             get_output_relation());
  }

} // namespace ir
} // namespace musketeer
