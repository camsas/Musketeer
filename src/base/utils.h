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

#ifndef MUSKETEER_UTILS_H
#define MUSKETEER_UTILS_H

#include <antlr3.h>
#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>

#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "base/common.h"
#include "frontends/operator_node.h"

namespace musketeer {

  typedef list<shared_ptr<OperatorNode> > node_list;
  typedef vector<shared_ptr<OperatorNode> > op_nodes;
  typedef set<shared_ptr<OperatorNode> > node_set;

  enum FmwType {FMW_SPARK, FMW_GRAPH_CHI, FMW_HADOOP, FMW_METIS,
                FMW_NAIAD, FMW_POWER_GRAPH, FMW_POWER_LYRA, FMW_WILD_CHERRY};

  bool CheckChildrenInSet(const op_nodes& dag, const node_set& nodes);
  string ExecCmd(const string& cmd);
  void PrintDag(op_nodes dag);
  void PrintDagGV(op_nodes dag);
  vector<string> GetDagOutputs(op_nodes dag);
  bool CheckRenameRequired(OperatorInterface* op);
  string FmwToString(uint8_t fmw);
  string FrameworkToString(FmwType fmw);
  shared_ptr<OperatorNode> IsInWhileBody(const node_list& nodes);
  void PrintNodesVector(const string& message, const op_nodes& nodes);
  void PrintStringVector(const string& message, const vector<string>& nodes);
  void PrintStringSet(const string& message,  const set<string>& nodes);
  void PrintRelationVector(const string& message,
                           const vector<Relation*>& nodes);
  void PrintRelationOpNodes(const string& message, const op_nodes& nodes);
  bool IsGeneratedByOp(const string& rel_name, const op_nodes& parents);
  vector<Relation*>* DetermineInputs(const op_nodes& dag, set<string>* inputs);
  vector<Relation*>* DetermineInputs(const op_nodes& dag, set<string>* inputs,
                                     set<string>* visited);
  void DetermineAllRelOutputs(
      const op_nodes& input_nodes, set<string>* outputs);
  vector<string> DetermineFinalOutputs(const op_nodes& input_nodes,
                                       const node_list& all_nodes);
  uint64_t SumNoOverflow(uint64_t a, uint64_t b);
  uint32_t SumNoOverflow(uint32_t a, uint32_t b);
  uint64_t MulNoOverflow(uint64_t a, uint64_t b);
  uint32_t ClampCost(uint32_t cost);

} // namespace musketeer
#endif
