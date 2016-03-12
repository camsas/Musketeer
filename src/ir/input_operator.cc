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

#include "ir/input_operator.h"

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "base/common.h"
#include "base/flags.h"
#include "base/hdfs_utils.h"
#include "base/utils.h"

namespace musketeer {
namespace ir {

  OperatorType InputOperator::get_type() {
    return INPUT_OP;
  }

  bool InputOperator::mapOnly() {
    return true;
  }

  pair<uint64_t, uint64_t> InputOperator::get_output_size(
    map<string, pair<uint64_t, uint64_t> >* rel_size) {
    vector<Relation*> relations = get_relations();
    if (relations.size() != 1) {
      LOG(ERROR) << "Input operator has more than one relation";
      return make_pair(0, 0);
    }
    string rel_dir = FLAGS_hdfs_input_dir + relations[0]->get_name() + "/";
    uint64_t input_rel_size = GetRelationSize(rel_dir);
    return make_pair(input_rel_size, input_rel_size);
  }

  OperatorInterface* InputOperator::clone() {
    return new InputOperator(get_input_dir(), get_relations(),
                             get_output_relation());
  }

} // namespace ir
} // namespace musketeer
