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

#include "frameworks/powerlyra_framework.h"

#include "frameworks/powerlyra_dispatcher.h"

namespace musketeer {
namespace framework {

  PowerLyraFramework::PowerLyraFramework() : PowerGraphFramework() {
    dispatcher_ = new PowerLyraDispatcher();
    monitor_ = NULL;
    // TODO(ionel): Add PowerLyra monitor.
  }

  string PowerLyraFramework::Translate(const op_nodes& dag,
                                       const string& relation) {
    // TODO(ionel): Implement.
    return "";
  }

  void PowerLyraFramework::Dispatch(const string& binary_file,
                                    const string& relation) {
    if (!FLAGS_dry_run) {
      dispatcher_->Execute(binary_file, relation);
    }
  }

  FmwType PowerLyraFramework::GetType() {
    return FMW_POWER_LYRA;
  }

  uint32_t PowerLyraFramework::ScoreDAG(const node_list& nodes, const relation_size& rel_size) {
    return numeric_limits<uint32_t>::max();
  }

  double PowerLyraFramework::ScoreOperator(shared_ptr<OperatorNode> op_node,
                                           const relation_size& rel_size) {
    return 0.0 * FLAGS_time_to_cost;
  }

  // The framework doesn't require a deployed cluster. We can create the cluster
  // at runtime => cluster state 0.
  double PowerLyraFramework::ScoreClusterState() {
    return 0.0 * FLAGS_time_to_cost;
  }

  // The compile time in PowerLyra takes around 50s.
  double PowerLyraFramework::ScoreCompile() {
    return 50.0 * FLAGS_time_to_cost;
  }

  double PowerLyraFramework::ScorePull(uint64_t data_size_kb) {
    // TODO(ionel): Implement.
    return 0.0;
  }

  double PowerLyraFramework::ScoreLoad(uint64_t data_size_kb) {
    // TODO(ionel): Implement.
    return 0.0;
  }

  double PowerLyraFramework::ScoreRuntime(uint64_t data_size_kb,
                                          const node_list& nodes,
                                          const relation_size& rel_size) {
    // TODO(ionel): Implement.
    return 0.0;
  }

  double PowerLyraFramework::ScorePush(uint64_t data_size_kb) {
    // TODO(ionel): Implement.
    return 0.0;
  }
} // namespace framework
} // namespace musketeer
