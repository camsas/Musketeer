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

#include "translation/naiad_job_code.h"

#include "base/common.h"

namespace musketeer {
namespace translator {

  void NaiadJobCode::set_agg_fun_code(string agg_fun_code_) {
    agg_fun_code = agg_fun_code_;
  }

  string NaiadJobCode::get_agg_fun_code() {
    return agg_fun_code;
  }

  void NaiadJobCode::set_compactor_code(string compactor_code_) {
    compactor_code = compactor_code_;
  }

  string NaiadJobCode::get_compactor_code() {
    return compactor_code;
  }

  void NaiadJobCode::set_out_fun_code(string out_fun_code_) {
    out_fun_code = out_fun_code_;
  }

  string NaiadJobCode::get_out_fun_code() {
    return out_fun_code;
  }

  void NaiadJobCode::set_rel_out_name(string rel_out_name_) {
    rel_out_name = rel_out_name_;
  }

  string NaiadJobCode::get_rel_out_name() {
    return rel_out_name;
  }

  void NaiadJobCode::set_operator_code(string operator_code_) {
    operator_code = operator_code_;
  }

  string NaiadJobCode::get_operator_code() {
    return operator_code;
  }

} // namespace translator
} // namespace musketeer
