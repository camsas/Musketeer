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

#include "scheduling/scheduler_simulator.h"

#include <iostream>
#include <fstream>
#include <utility>

#include "base/common.h"
#include "base/flags.h"

namespace musketeer {
namespace scheduling {

  map<string, pair<uint64_t, uint64_t> >* SchedulerSimulator::GetAllRelSize() {
    return all_rel_size_;
  }

  map<string, pair<uint64_t, uint64_t> >* SchedulerSimulator::GetCurrentRelSize() {
    return current_rel_size_;
  }

  void SchedulerSimulator::ReadDataSizeFile() {
    string file_location = FLAGS_dry_run_data_size_file;
    ifstream in_file(file_location.c_str());
    string rel_name;
    uint64_t relation_size;
    while (in_file >> rel_name >> relation_size) {
      LOG(INFO) << "Relation " << rel_name << " is of size: " << relation_size;
      (*all_rel_size_)[rel_name] = make_pair(relation_size, relation_size);
    }
    in_file.close();
  }

  void SchedulerSimulator::UpdateOutputSize(const string& rel_name) {
    (*current_rel_size_)[rel_name] = (*all_rel_size_)[rel_name];
  }

} // namespace scheduling
} // namespace musketeer
