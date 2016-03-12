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

#ifndef MUSKETEER_JOB_RUN_H
#define MUSKETEER_JOB_RUN_H

#include <stdint.h>

#include <string>
#include <utility>
#include <vector>

#include "base/common.h"

namespace musketeer {
namespace core {

class JobRun {
 public:
  JobRun(string job_name_, string framework_, uint64_t make_span_,
         vector<pair<string, uint64_t> > input_rels_size_,
         vector<pair<string, uint64_t> > output_rels_size_):
  job_name(job_name_), framework(framework_), make_span(make_span_),
    input_rels_size(input_rels_size_), output_rels_size(output_rels_size_) {
  }
  string get_name();
  string get_framework();
  uint64_t get_make_span();
  vector<pair<string, uint64_t> > get_input_rels_size();
  vector<pair<string, uint64_t> > get_output_rels_size();

 private:
  string job_name;
  string framework;
  uint64_t make_span;
  vector<pair<string, uint64_t> > input_rels_size;
  vector<pair<string, uint64_t> > output_rels_size;
};

} // namespace core
} // namespace musketeer
#endif
