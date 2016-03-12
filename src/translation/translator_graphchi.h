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

#ifndef MUSKETEER_TRANSLATOR_GRAPHCHI_H
#define MUSKETEER_TRANSLATOR_GRAPHCHI_H

#include "translation/translator_graph_interface.h"

#include <string>
#include <vector>

#include "base/common.h"
#include "translation/graphchi_job_code.h"

namespace musketeer {
namespace translator {

using ctemplate::TemplateDictionary;
using namespace musketeer::ir; // NOLINT

class TranslatorGraphChi : public TranslatorGraphInterface {
 public:
  TranslatorGraphChi(const op_nodes& dag, const string& class_name);
  string GenerateCode();

 private:
  string HandleVerticesEdgesJoin(
      unordered_map<string, vector<string> >* rel_var_names,
      TemplateDictionary* dict, shared_ptr<OperatorNode> join_node,
      bool has_count);
  shared_ptr<OperatorNode> IgnoreIfCountEdgesPresent(const op_nodes& dag);
  string GetBinaryPath(OperatorInterface* op);
  string GetSourcePath(OperatorInterface* op);
  void GenMakeFile(OperatorInterface* op,
                   const TemplateDictionary& dict);
  string TranslateSuperType(uint16_t e_cost, uint16_t ver_val);
  string GenAndCompile(OperatorInterface* op, const string& op_code);
  string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                   AggOperator* op);
  string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                   CountOperator* op);
  string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                   CrossJoinOperator* op);
  string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                   DifferenceOperator* op);
  string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                   DistinctOperator* op);
  string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                   DivOperator* op);
  string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                   IntersectionOperator* op);
  string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                   JoinOperator* op);
  string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                   MaxOperator* op);
  string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                   MinOperator* op);
  string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                   MulOperator* op);
  string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                   ProjectOperator* op);
  string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                   SelectOperator* op);
  string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                   SortOperator* op);
  string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                   SubOperator* op);
  string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                   SumOperator* op);
  string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                   UnionOperator* op);
  string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                   WhileOperator* op);
  string UpdateGroupVertexVal(const string& var_to_update,
      unordered_map<string, vector<string> >* rel_var_names, AggOperator* op);
  string UpdateGroupVertexVal(const string& var_to_update,
      unordered_map<string, vector<string> >* rel_var_names,
      CountOperator* op);
  string UpdateGroupVertexVal(const string& var_to_update,
      unordered_map<string, vector<string> >* rel_var_names, MaxOperator* op);
  string UpdateGroupVertexVal(const string& var_to_update,
      unordered_map<string, vector<string> >* rel_var_names, MinOperator* op);
};

} // namespace translator
} // namespace musketeer
#endif
