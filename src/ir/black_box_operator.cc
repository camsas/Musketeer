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

#include "ir/black_box_operator.h"

#include <limits>

namespace musketeer {
namespace ir {

  uint8_t BlackBoxOperator::get_fmw() {
    return fmw;
  }

  string BlackBoxOperator::get_binary_path() {
    return binary_path;
  }

  OperatorType BlackBoxOperator::get_type() {
    return BLACK_BOX_OP;
  }

  pair<uint64_t, uint64_t> BlackBoxOperator::get_output_size(
      map<string, pair<uint64_t, uint64_t> >* rel_size) {
    pair<uint64_t, uint64_t> bbox_rel_size =
      make_pair(1, numeric_limits<uint64_t>::max());
    (*rel_size)[get_output_relation()->get_name()] = bbox_rel_size;
    return bbox_rel_size;
  }

  bool BlackBoxOperator::hasAction() {
    return true;
  }

  bool BlackBoxOperator::mapOnly() {
    return false;
  }

  OperatorInterface* BlackBoxOperator::clone() {
    return new BlackBoxOperator(get_input_dir(), get_relations(),
                                get_output_relation(), fmw, binary_path);
  }

} // namespace ir
} // namespace musketeer
