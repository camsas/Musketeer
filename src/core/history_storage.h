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

#ifndef MUSKETEER_HISTORY_STORAGE_H
#define MUSKETEER_HISTORY_STORAGE_H

#include <iostream>
#include <list>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "base/common.h"
#include "core/job_run.h"

namespace musketeer {
namespace core {

// ((job_name, fmw), list<JobRun>)
typedef map<pair<string, string>, list<JobRun*> > job_history_map;
// TODO(ionel): This is not thread safe.
typedef map<string, vector<pair<string, uint64_t> > > avg_out_size_map;
typedef map<string, uint16_t> job_num_runs_map;

class HistoryStorage {
 public:
  HistoryStorage() {
  }

  ~HistoryStorage() {
  }

  void AddRun(JobRun* job_run);
  list<JobRun*> get_history(pair<string, string> job_fmw);
  vector<pair<string, uint64_t> > get_expected_data_size(string job_name);

 private:
  job_history_map job_history;
  avg_out_size_map avg_out_size;
  job_num_runs_map job_num_runs;
};

} // namespace core
} // namespace musketeer
#endif
