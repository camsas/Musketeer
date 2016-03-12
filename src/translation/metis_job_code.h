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

#ifndef MUSKETEER_METIS_JOB_CODE_H
#define MUSKETEER_METIS_JOB_CODE_H

#include "translation/mapreduce_job_code.h"

#include <string>
#include <vector>

#include "base/common.h"
#include "ir/operator_interface.h"

namespace musketeer {
namespace translator {

using musketeer::ir::OperatorInterface;

class MetisJobCode : public MapReduceJobCode {
 public:
  MetisJobCode(OperatorInterface* op, const string& map_variables_code_,
               const string& setup_code_, const string& map_code_,
               const string& cleanup_code_, const string& reduce_code_)
    : MapReduceJobCode(op, map_variables_code_, setup_code_, map_code_,
                       cleanup_code_, reduce_code_) {
  }

  MetisJobCode(OperatorInterface* op, const string& code):
    MapReduceJobCode(op, code) {
  }
};

} // namespace translator
} // namespace musketeer
#endif
