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

#ifndef MUSKETEER_TRANSLATOR_NAIAD_H
#define MUSKETEER_TRANSLATOR_NAIAD_H

#include "translation/translator_interface.h"

#include <ctemplate/template.h>

#include <iostream>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/common.h"
#include "ir/column.h"
#include "ir/operator_interface.h"
#include "translation/naiad_job_code.h"

namespace musketeer {
namespace translator {

using ctemplate::TemplateDictionary;
using namespace musketeer::ir; // NOLINT

class TranslatorNaiad : public TranslatorInterface {
 public:
  TranslatorNaiad(const op_nodes& dag, const string& class_name);

  ~TranslatorNaiad() {
    //TODO(Tach): Implement
  }
  string GenerateCode();

 private:
  NaiadJobCode* Translate(AggOperator* op);
  NaiadJobCode* Translate(CountOperator* op);
  NaiadJobCode* Translate(CrossJoinOperator* op);
  NaiadJobCode* Translate(DifferenceOperator* op);
  NaiadJobCode* Translate(DistinctOperator* op);
  NaiadJobCode* Translate(DivOperator* op);
  NaiadJobCode* Translate(IntersectionOperator* op);
  NaiadJobCode* Translate(JoinOperator* op);
  NaiadJobCode* Translate(MaxOperator* op);
  NaiadJobCode* Translate(MinOperator* op);
  NaiadJobCode* Translate(MulOperator* op);
  NaiadJobCode* Translate(ProjectOperator* op);
  NaiadJobCode* Translate(SelectOperator* op);
  NaiadJobCode* Translate(SortOperator* op);
  NaiadJobCode* Translate(SubOperator* op);
  NaiadJobCode* Translate(SumOperator* op);
  NaiadJobCode* Translate(UdfOperator* op);
  NaiadJobCode* Translate(UnionOperator* op);
  NaiadJobCode* Translate(WhileOperator* op);
  NaiadJobCode* TranslateMathOp(OperatorInterface* op, vector<Value*> values,
                                ConditionTree* condition_tree,
                                string math_op);
  void TranslateDAG(string* code, string* fun_code, string* compactor_code,
                    const op_nodes& dag,
                    set<shared_ptr<OperatorNode> >* leaves,
                    set<string>* processed);
  bool CanSchedule(OperatorInterface* op, set<string>* processed);
  void MarkImmutable(const op_nodes& dag, set<string>* processed,
                     map<string, Relation*>* name_rel);
  void MarkImmutable(shared_ptr<OperatorNode> node, set<string>* processed,
                     map<string, Relation*>* name_rel);
  void MarkImmutableWhile(shared_ptr<OperatorNode> node,
                          set<string>* processed,
                          map<string, Relation*>* name_rel);
  pair<uint16_t, uint16_t> TranslateType(const vector<Value*>& values);
  void GenerateFastJoin(TemplateDictionary* dict, JoinOperator* op);
  string GenerateReducerClass(TemplateDictionary* dict, const string& reducer_name, string op,
                              vector<Column*> group_bys, vector<Column*> sel_cols);
  string GenerateType(Relation* relation);
  string GenerateType(const vector<uint16_t>& types);
  string GenerateType(const vector<Column*>& cols);
  string GenerateKeyOut(uint32_t num_cols);
  string GenerateSelectCols(string rel_name, const vector<Column*>& cols);
  string GenerateOutput(string rel_name, const vector<int32_t>& indexes_selected);
  string GenerateOutput(string rel_name, OperatorInterface* op,
                        const vector<Value*>& values, string math_op,
                        bool single_value);
  string DeclareVariables(vector<Relation*> input_rels,
                          const set<string>& processed,
                          const map<string, Relation*>& name_rel);
  string GetBinaryPath(OperatorInterface* op);
  string GetBinaryPath();
  string GetSourcePath(OperatorInterface* op);
  string GetSourcePath();
  string Compile(const string& code, const vector<Relation*>& input_rels,
                 const set<shared_ptr<OperatorNode> >& leaves);
  string GenerateReadCode(const vector<Relation*>& input_relations);
  string GenerateStructsCode();
  pair<string, string> GenerateOutputCode(const set<shared_ptr<OperatorNode> >& leaves);
  bool SameColumns(const vector<Column*>& in_cols, const vector<Column*>& cols);
  bool MustGenerateCode(shared_ptr<OperatorNode> node);

  NaiadJobCode* cur_job_code;
  unordered_map<string, vector<uint16_t> > unique_generated_types_;
};

} // namespace translator
} // namespace musketeer
#endif
