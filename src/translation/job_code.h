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

#ifndef MUSKETEER_JOB_CODE_H
#define MUSKETEER_JOB_CODE_H

#include <string>
#include <vector>

#include "base/common.h"
#include "ir/operator_interface.h"

namespace musketeer {
namespace translator {

using ir::OperatorInterface;

class JobCode {
 public:
  JobCode(OperatorInterface* op_, const string& code_): code(code_), op(op_) {
  }

  OperatorInterface* get_operator() {
    return op;
  }

  virtual bool hasCode() {
    return code.compare("");
  }

  virtual string get_code() {
    return code;
  }

 protected:
  string code;

 private:
  OperatorInterface* op;
};

} // namespace translator
} // namespace musketeer
#endif
