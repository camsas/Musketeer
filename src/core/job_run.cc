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

#include "core/job_run.h"

#include <utility>

namespace musketeer {
namespace core {

  string JobRun::get_name() {
    return job_name;
  }

  string JobRun::get_framework() {
    return framework;
  }

  uint64_t JobRun::get_make_span() {
    return make_span;
  }

  vector<pair<string, uint64_t> > JobRun::get_input_rels_size() {
    return input_rels_size;
  }

  vector<pair<string, uint64_t> > JobRun::get_output_rels_size() {
    return output_rels_size;
  }

} // namespace core
} // namespace musketeer
