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

#ifndef MUSKETEER_MAPREDUCE_JOB_CODE_H
#define MUSKETEER_MAPREDUCE_JOB_CODE_H

#include "translation/job_code.h"

#include <string>
#include <vector>

#include "base/common.h"
#include "ir/operator_interface.h"

namespace musketeer {
namespace translator {

using ir::OperatorInterface;

class MapReduceJobCode : public JobCode {
 public:
  MapReduceJobCode(OperatorInterface* op, const string& map_variables_code_,
                   const string& setup_code_, const string& map_code_,
                   const string& cleanup_code_, const string& reduce_code_)
     : JobCode(op, ""), map_key_type(""), map_value_type(""),
       reduce_key_type(""), reduce_value_type(""),
       map_variables_code(map_variables_code_),
       setup_code(setup_code_), map_code(map_code_),
       cleanup_code(cleanup_code_),
       reduce_code(reduce_code_) {
  }

  MapReduceJobCode(OperatorInterface* op, const string& code)
    : JobCode(op, code) {
  }

  string get_code();
  string get_map_variables_code();
  void set_map_variables_code(string map_variables_code_);
  bool HasMapVariablesCode();
  string get_setup_code();
  void set_setup_code(string setup_code_);
  bool HasSetupCode();
  string get_map_code();
  void set_map_code(string map_code_);
  bool HasMapCode();
  string get_cleanup_code();
  void set_cleanup_code(string cleanup_code_);
  bool HasCleanupCode();
  string get_reduce_code();
  void set_reduce_code(string reduce_code_);
  bool HasReduceCode();
  void set_map_key_type(string type);
  void set_map_value_type(string type);
  void set_reduce_key_type(string type);
  void set_reduce_value_type(string type);
  string get_map_key_type();
  string get_map_value_type();
  string get_reduce_key_type();
  string get_reduce_value_type();

 private:
  string map_key_type;
  string map_value_type;
  string reduce_key_type;
  string reduce_value_type;
  string map_variables_code;
  string setup_code;
  string map_code;
  string cleanup_code;
  string reduce_code;
};

} // namespace translator
} // namespace musketeer
#endif
