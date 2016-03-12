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

#ifndef MUSKETEER_TRANSLATOR_HADOOP_H
#define MUSKETEER_TRANSLATOR_HADOOP_H

#include "translation/translator_interface.h"

#include <ctemplate/template.h>

#include <cstring>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "base/common.h"
#include "ir/column.h"
#include "translation/hadoop_job_code.h"

namespace musketeer {
namespace translator {

using ctemplate::TemplateDictionary;
using namespace musketeer::ir; // NOLINT

class TranslatorHadoop : public TranslatorInterface {
 public:
  TranslatorHadoop(const op_nodes& dag, const string& class_name);
  string GenerateCode();

 private:
  HadoopJobCode* Translate(AggOperator* op);
  HadoopJobCode* Translate(CountOperator* op);
  HadoopJobCode* Translate(CrossJoinOperator* op);
  HadoopJobCode* Translate(DifferenceOperator* op);
  HadoopJobCode* Translate(DistinctOperator* op);
  HadoopJobCode* Translate(DivOperator* op);
  HadoopJobCode* Translate(IntersectionOperator* op);
  HadoopJobCode* Translate(JoinOperator* op);
  HadoopJobCode* Translate(MaxOperator* op);
  HadoopJobCode* Translate(MinOperator* op);
  HadoopJobCode* Translate(MulOperator* op);
  HadoopJobCode* Translate(ProjectOperator* op);
  HadoopJobCode* Translate(SelectOperator* op);
  HadoopJobCode* Translate(SortOperator* op);
  HadoopJobCode* Translate(SubOperator* op);
  HadoopJobCode* Translate(SumOperator* op);
  HadoopJobCode* Translate(UdfOperator* op);
  HadoopJobCode* Translate(UnionOperator* op);
  string GenerateGroupByKey(const vector<Column*>& group_bys);
  string GetBinaryPath(OperatorInterface* op);
  string GetSourcePath(OperatorInterface* op);
  string GenAndCompile(OperatorInterface* op, const string& op_code);
  pair<string, string> GetInputPathsAndRelationsCode(const op_nodes& dag);
  bool HasReduce(OperatorInterface* op);
  void UpdateDAGCode(const string& code, HadoopJobCode* child_code,
                     bool add_to_reduce, HadoopJobCode* dag_code);
  void PopulateEndDAG(OperatorInterface* op, HadoopJobCode* dag_code,
                      TemplateDictionary* dict,
                      pair<string, string> input_rel_code);

  bool single_input;
};

} // namespace translator
} // namespace musketeer
#endif
