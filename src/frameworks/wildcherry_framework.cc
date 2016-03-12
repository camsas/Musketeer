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

#include "frameworks/wildcherry_framework.h"

#include <limits>
#include <string>

namespace musketeer {
namespace framework {

  using musketeer::translator::TranslatorWildCherry;

  WildCherryFramework::WildCherryFramework(): FrameworkInterface() {
    dispatcher_ = new WildCherryDispatcher();
  }

  string WildCherryFramework::Translate(const op_nodes& dag,
                                        const string& relation) {
    TranslatorWildCherry translator_wildcherry = translator::TranslatorWildCherry(dag, relation);
    string result = translator_wildcherry.GenerateCode();
    return result;
  }

  void WildCherryFramework::Dispatch(const string& binary_file,
                                     const string& relation) {
    if (!FLAGS_dry_run) {
      dispatcher_->Execute(binary_file, relation);
    }
  }

  FmwType WildCherryFramework::GetType() {
    return FMW_WILD_CHERRY;
  }

  uint32_t WildCherryFramework::ScoreDAG(const node_list& nodes,
                                         const relation_size& rel_size) {
    node_set to_schedule;
    int32_t num_ops_to_schedule = 0;
    op_nodes input_nodes;
    // At the moment we only support one input DAGs in WildCherry.
    input_nodes.push_back(nodes.front());
    for (node_list::const_iterator it = nodes.begin(); it != nodes.end();
         ++it) {
      to_schedule.insert(*it);
      num_ops_to_schedule++;
    }
    if (CanMerge(input_nodes, to_schedule, num_ops_to_schedule)) {
      // TODO(ionel): Implement.
      VLOG(2) << "Can merge in " << FrameworkToString(GetType());
      return FLAGS_max_scheduler_cost;
    } else {
      VLOG(2) << "Cannot merge in " << FrameworkToString(GetType());
      return numeric_limits<uint32_t>::max();
    }
  }

  double WildCherryFramework::ScoreOperator(shared_ptr<OperatorNode> op_node,
                                            const relation_size& rel_size) {
    return 0.0;
  }

  double WildCherryFramework::ScoreClusterState() {
    return 0.0;
  }

  double WildCherryFramework::ScoreCompile() {
    return 0.0;
  }

  double WildCherryFramework::ScorePull(uint64_t data_size_kb) {
    return 0.0;
  }

  double WildCherryFramework::ScoreLoad(uint64_t data_size_kb) {
    return 0.0;
  }

  double WildCherryFramework::ScoreRuntime(uint64_t data_size_kb,
                                           const node_list& nodes,
                                           const relation_size& rel_size) {
    return 0.0;
  }

  double WildCherryFramework::ScorePush(uint64_t data_size_kb) {
    return 0.0;
  }

  bool WildCherryFramework::CanMerge(const op_nodes& dag,
                                     const node_set& to_schedule,
                                     int32_t num_ops_to_schedule) {
    return true;
  }

} // namespace framework
} // namespace musketeer
