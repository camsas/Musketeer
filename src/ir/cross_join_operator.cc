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

#include "ir/cross_join_operator.h"

#include <limits>
#include <map>
#include <utility>
#include <vector>

namespace musketeer {
namespace ir {

  OperatorType CrossJoinOperator::get_type() {
    return CROSS_JOIN_OP;
  }

  pair<uint64_t, uint64_t> CrossJoinOperator::get_output_size(
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
      pair<uint64_t, uint64_t> join_rel_size =
        make_pair(numeric_limits<uint64_t>::max(),
                  numeric_limits<uint64_t>::max());
      return UpdateIfSmaller(get_output_relation()->get_name(), join_rel_size,
                             rel_size);
    } else {
      uint64_t max_size = MulNoOverflow(left_max_size, right_max_size);
      pair<uint64_t, uint64_t> join_rel_size = make_pair(max_size, max_size);
      return UpdateIfSmaller(get_output_relation()->get_name(), join_rel_size,
                             rel_size);
    }
  }


  bool CrossJoinOperator::mapOnly() {
    return false;
  }

  OperatorInterface* CrossJoinOperator::clone() {
    return new CrossJoinOperator(get_input_dir(), get_relations(),
                                 get_output_relation());
  }

} // namespace ir
} // namespace musketeer
