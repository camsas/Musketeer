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

#ifndef MUSKETEER_NAIAD_JOB_CODE_H
#define MUSKETEER_NAIAD_JOB_CODE_H

#include "translation/job_code.h"

#include <string>

#include "base/common.h"
#include "ir/operator_interface.h"

namespace musketeer {
namespace translator {

using ir::OperatorInterface;

class NaiadJobCode : public JobCode {
 public:
  NaiadJobCode(OperatorInterface* op, const string& code): JobCode(op, code) {
  }

  void set_agg_fun_code(string agg_fun_code_);
  string get_agg_fun_code();
  void set_compactor_code(string compactor_code_);
  string get_compactor_code();
  void set_out_fun_code(string out_fun_code_);
  string get_out_fun_code();
  void set_rel_out_name(string rel_out_name_);
  string get_rel_out_name();
  void set_operator_code(string operator_code_);
  string get_operator_code();

 private:
  string agg_fun_code;
  string compactor_code;
  string out_fun_code;
  string rel_out_name;
  string operator_code;
};

} // namespace translator
} // namespace musketeer
#endif
