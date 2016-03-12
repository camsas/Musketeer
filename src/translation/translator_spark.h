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

#ifndef MUSKETEER_TRANSLATOR_SPARK_H
#define MUSKETEER_TRANSLATOR_SPARK_H

#include "translation/translator_interface.h"

#include <ctemplate/template.h>

#include <iostream>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <vector>

#include "base/common.h"
#include "ir/column.h"
#include "translation/spark_job_code.h"

namespace musketeer {
namespace translator {

using ctemplate::TemplateDictionary;
using namespace musketeer::ir; // NOLINT

/* Defines different possible types of RDD */
typedef enum {
SIMPLE,
  KEY,
  SEQUENCE, // result of certain accum calls
  MAP, // result of count by key
  LO, // resul of
  PAIR, // result of join (K,(W,V))
  BASE  // String initial type
  } SparkResultType;


class TranslatorSpark : public TranslatorInterface {
 public:
  TranslatorSpark(const op_nodes& dag, const string& class_name);

  ~TranslatorSpark() {
    //TODO(Tach): Implement
  }
  string GenerateCode();

  /* State used to identify format of relations */
  static map<string, Relation*> relations;

 private:
  SparkJobCode* Translate(AggOperator* op);
  SparkJobCode* Translate(CountOperator* op);
  SparkJobCode* Translate(CrossJoinOperator* op);
  SparkJobCode* Translate(DifferenceOperator* op);
  SparkJobCode* Translate(DistinctOperator* op);
  SparkJobCode* Translate(DivOperator* op);
  SparkJobCode* Translate(IntersectionOperator* op);
  SparkJobCode* Translate(JoinOperator* op);
  SparkJobCode* Translate(MaxOperator* op);
  SparkJobCode* Translate(MinOperator* op);
  SparkJobCode* Translate(MulOperator* op);
  SparkJobCode* Translate(ProjectOperator* op);
  SparkJobCode* Translate(SelectOperator* op);
  SparkJobCode* Translate(SortOperator* op);
  SparkJobCode* Translate(SubOperator* op);
  SparkJobCode* Translate(SumOperator* op);
  SparkJobCode* Translate(UdfOperator* op);
  SparkJobCode* Translate(UnionOperator* op);
  SparkJobCode* Translate(WhileOperator* op);
  string TranslateHeader(const string& class_name,
                         const string& bin_name,
                         const set<string>& inputs,
                         const set<string>& processed,
                         const string& root_dir);
  void TranslateDAG(string* code, const op_nodes& dag,
                    set<shared_ptr<OperatorNode> >* leafs,
                    set<string>* processed);
  string TranslateTail(const set<shared_ptr<OperatorNode> >& leaf_nodes,
                       const string& output_path);
  SparkJobCode* TranslateMathOp(OperatorInterface* op, vector<Value*> values,
                                ConditionTree* condition_tree, string math_op);
  string GetOutputPath(OperatorInterface* op);
  string GetBinaryPath(OperatorInterface* op);
  string GetSourcePath(OperatorInterface* op);
  string GetOutputPath(const string& class_name, const string& root_dir);
  string GetBinaryPath(const string& class_name, const string& root_dir);
  string GetSourcePath(const string& class_name, const string& root_dir);
  string Compile(const string& code, const string& root_dir);
  void PopulateCommonValues(OperatorInterface* op,
                            TemplateDictionary* dict);
  void PopulateCondition(string condition,
                         string input_rel_name,
                         TemplateDictionary* dict);
  string GenConditionInput(const set<string>& cond_names);
  string GenUpdateConditionInput(const set<string>& cond_names);
  bool CanSchedule(OperatorInterface* op, set<string>* processed);
  string GenTypedRDD(Relation* relation);
  string GenMinMaxTupleType(Column* col, const vector<Column*>& sel_cols);
  string GenTupleType(const vector<Column*>& columns);
  string GenTupleType(Relation* rel);
  string GenJoinTupleType(const vector<Column*>& columns, const vector<Column*>& group_bys);
  string GenTupleVar(const string& input_name, Relation* rel, const vector<Column*>& key_cols);
  string GenFlatJoin(const string& intput_rel_name, uint16_t col_join_type, int32_t col1,
                     int32_t col2, Relation* rel1, Relation* rel2);
  string GenFlatMappingConvert(const string& input_name, const vector<Column*>& columns);
  string OutputDefaultForType(uint16_t type);
  string OutputMapping(Relation* rel);
  vector<Relation*>* DetermineInputsSpark(const op_nodes& dags, set<string>* inputs,
                                          set<string>* visited);
  string GenerateAggGroupBy(const string& input_name, Relation* input_rel,
                            const vector<Column*>& group_bys,
                            const vector<Column*>& selected_cols);
  string GenerateCountGroupBy(const string& input_name, Relation* input_rel,
                              const vector<Column*>& group_bys);
  string GenerateMinMaxGroupBy(const string& input_name, int32_t col_index, Relation* input_rel,
                               const vector<Column*>& group_bys,
                               const vector<Column*>& selected_cols);
  string GenerateKeyRDD(const string& input_name, Relation* input_rel,
                        const vector<Column*>& key_cols, int32_t col_index);
  string GenOutputCols(const string& name, const vector<Column*>& cols);
  string GenOutputCols(const string& name, int32_t agg_col_index,
                       const vector<Column*>& cols);
  string GenerateFlatMapping(const string& input_name, int32_t col_index,
                             const vector<Column*>& selected_cols,
                             const vector<Column*>& groupby_cols,
                             const vector<Column*>& columns);
  string GenerateProject(const string& input_name, vector<Column*> proj_cols);
  string GenerateMaths(const string& input_name, const string& op, Relation* rel, Value* left_val,
                       Value* right_val, Relation*  output_type);
  string GenCrossJoin(Relation* rel1, Relation* rel2);
  string GenerateAgg(const string& op, const vector<Column*>& agg_cols);
  string GenerateAggShuffle(const string& op, const vector<Column*>& agg_cols,
                            uint32_t num_input_cols);
  bool MustGenerateCode(shared_ptr<OperatorNode> node);

  /* State used to determine when to cache */
  map<string, bool> to_cache;
  SparkJobCode* cur_job_code;
};

} // namespace translator
} // namespace musketeer
#endif
