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

#ifndef MUSKETEER_TRANSLATOR_METIS_H
#define MUSKETEER_TRANSLATOR_METIS_H

#include "translation/translator_interface.h"

#include <stdint.h>

#include <string>
#include <utility>
#include <vector>

#include "base/common.h"
#include "translation/metis_job_code.h"

namespace musketeer {
namespace translator {

using ctemplate::TemplateDictionary;
using namespace musketeer::ir; // NOLINT

class TranslatorMetis : public TranslatorInterface {
 public:
  TranslatorMetis(const op_nodes& dag, const string& class_name);
  string GenerateCode();

 private:
  void GenerateMakefile(OperatorInterface* op, const string& rel_name,
                        const string& make_file_name);
  MetisJobCode* Translate(AggOperator* op);
  MetisJobCode* Translate(CountOperator* op);
  MetisJobCode* Translate(CrossJoinOperator* op);
  MetisJobCode* Translate(DifferenceOperator* op);
  MetisJobCode* Translate(DistinctOperator* op);
  MetisJobCode* Translate(DivOperator* op);
  MetisJobCode* Translate(IntersectionOperator* op);
  MetisJobCode* Translate(JoinOperator* op);
  MetisJobCode* Translate(MaxOperator* op);
  MetisJobCode* Translate(MinOperator* op);
  MetisJobCode* Translate(MulOperator* op);
  MetisJobCode* Translate(ProjectOperator* op);
  MetisJobCode* Translate(SelectOperator* op);
  MetisJobCode* Translate(SortOperator* op);
  MetisJobCode* Translate(SubOperator* op);
  MetisJobCode* Translate(SumOperator* op);
  MetisJobCode* Translate(UdfOperator* op);
  MetisJobCode* Translate(UnionOperator* op);
  string GetBinaryPath(OperatorInterface* op);
  string GetSourcePath(OperatorInterface* op);
  string GenAndCompile(OperatorInterface* op, const string& op_code);
  void GetInputPathsAndRelations(const op_nodes& dag,
                                 vector<string>* input_paths,
                                 vector<Relation*>* input_rels);
  pair<string, string> GetInputPathsAndRelationsCode(
      const vector<string>& input_paths,
      const vector<Relation*>& input_rels);
  bool HasReduce(OperatorInterface* op);
  void UpdateDAGCode(const string& code, MetisJobCode* child_code,
                     bool add_to_reduce, MetisJobCode* dag_code);
  void PopulateCommonValues(OperatorInterface* op, TemplateDictionary* dict);
  void PopulateEndDAG(OperatorInterface* op, MetisJobCode* dag_code,
                      TemplateDictionary* dict);
  void PrepareCodeDirectory(OperatorInterface* op);

  bool use_mergable_operators_;
};

} // namespace translator
} // namespace musketeer
#endif  // MUSKETEER_TRANSLATOR_METIS_H
