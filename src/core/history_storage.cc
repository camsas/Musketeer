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

#include "core/history_storage.h"

#include <utility>
#include <vector>

namespace musketeer {
namespace core {

  void HistoryStorage::AddRun(JobRun* job_run) {
    pair<string, string> key =
      make_pair(job_run->get_name(), job_run->get_framework());
    if (job_history.find(key) != job_history.end()) {
      job_history[key].push_front(job_run);
    } else {
      list<JobRun*> hist_list;
      hist_list.push_front(job_run);
      job_history[key] = hist_list;
    }
    string job_name = job_run->get_name();
    if (job_num_runs.find(job_run->get_name()) != job_num_runs.end()) {
      uint64_t cur_num_runs = job_num_runs[job_name];
      job_num_runs[job_name]++;
      vector<pair<string, uint64_t> > cur_out_size = avg_out_size[job_name];
      vector<pair<string, uint64_t> > out_size =
        job_run->get_output_rels_size();
      for (vector<pair<string, uint64_t> >::size_type i = 0;
           i < cur_out_size.size(); i++) {
        // TODO(ionel): This may overflow.
        long double tmp = cur_out_size[i].second * cur_num_runs /
          (cur_num_runs + 1) + out_size[i].second / (cur_num_runs + 1);
        cur_out_size[i].second = tmp;
      }
      avg_out_size[job_name] = cur_out_size;
    } else {
      job_num_runs[job_name] = 1;
      avg_out_size[job_name] = job_run->get_output_rels_size();
    }
  }

  list<JobRun*> HistoryStorage::get_history(pair<string, string> key) {
    if (job_history.find(key) != job_history.end()) {
      return job_history[key];
    } else {
      list<JobRun*> hist_list;
      return hist_list;
    }
  }

  vector<pair<string, uint64_t> > HistoryStorage::get_expected_data_size(
      string job_name) {
    if (avg_out_size.find(job_name) != avg_out_size.end()) {
      return avg_out_size[job_name];
    } else {
      vector<pair<string, uint64_t> > data_size;
      return data_size;
    }
  }

} // namespace core
} // namespace musketeer
