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

#include "ir/udf_operator.h"

#include <limits>
#include <map>
#include <string>
#include <utility>

namespace musketeer {
namespace ir {

  string UdfOperator::get_udf_source_path() {
    return udf_source_path;
  }

  string UdfOperator::get_udf_name() {
    return udf_name;
  }

  OperatorType UdfOperator::get_type() {
    return UDF_OP;
  }

  pair<uint64_t, uint64_t> UdfOperator::get_output_size(
      map<string, pair<uint64_t, uint64_t> >* rel_size) {
    pair<uint64_t, uint64_t> udf_rel_size =
      make_pair(1, numeric_limits<uint64_t>::max());
    (*rel_size)[get_output_relation()->get_name()] = udf_rel_size;
    return udf_rel_size;
  }

  bool UdfOperator::mapOnly() {
    return false;
  }

  OperatorInterface* UdfOperator::clone() {
    return new UdfOperator(get_input_dir(), get_relations(),
                           get_output_relation(), udf_source_path, udf_name);
  }

} // namespace ir
} // namespace musketeer
