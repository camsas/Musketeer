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

#include "translation/mapreduce_job_code.h"

#include "base/common.h"

namespace musketeer {
namespace translator {

  string MapReduceJobCode::get_code() {
    return code;
  }

  string MapReduceJobCode::get_map_variables_code() {
    return map_variables_code;
  }

  void MapReduceJobCode::set_map_variables_code(string map_variables_code_) {
    map_variables_code = map_variables_code_;
  }

  bool MapReduceJobCode::HasMapVariablesCode() {
    return map_variables_code.compare("");
  }

  string MapReduceJobCode::get_setup_code() {
    return setup_code;
  }

  void MapReduceJobCode::set_setup_code(string setup_code_) {
    setup_code = setup_code_;
  }

  bool MapReduceJobCode::HasSetupCode() {
    return setup_code.compare("");
  }

  string MapReduceJobCode::get_map_code() {
    return map_code;
  }

  void MapReduceJobCode::set_map_code(string map_code_) {
    map_code = map_code_;
  }

  bool MapReduceJobCode::HasMapCode() {
    return map_code.compare("");
  }

  string MapReduceJobCode::get_cleanup_code() {
    return cleanup_code;
  }

  void MapReduceJobCode::set_cleanup_code(string cleanup_code_) {
    cleanup_code = cleanup_code_;
  }

  bool MapReduceJobCode::HasCleanupCode() {
    return cleanup_code.compare("");
  }

  string MapReduceJobCode::get_reduce_code() {
    return reduce_code;
  }

  void MapReduceJobCode::set_reduce_code(string reduce_code_) {
    reduce_code = reduce_code_;
  }

  bool MapReduceJobCode::HasReduceCode() {
    return reduce_code.compare("");
  }

  void MapReduceJobCode::set_map_key_type(string type) {
    map_key_type = type;
  }

  void MapReduceJobCode::set_map_value_type(string type) {
    map_value_type = type;
  }

  void MapReduceJobCode::set_reduce_key_type(string type) {
    reduce_key_type = type;
  }

  void MapReduceJobCode::set_reduce_value_type(string type) {
    reduce_value_type = type;
  }

  string MapReduceJobCode::get_map_key_type() {
    return map_key_type;
  }

  string MapReduceJobCode::get_map_value_type() {
    return map_value_type;
  }

  string MapReduceJobCode::get_reduce_key_type() {
    return reduce_key_type;
  }

  string MapReduceJobCode::get_reduce_value_type() {
    return reduce_value_type;
  }

} // namespace translator
} // namespace musketeer
