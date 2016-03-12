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

#ifndef MUSKETEER_TRANSLATOR_GRAPH_INTERFACE_H
#define MUSKETEER_TRANSLATOR_GRAPH_INTERFACE_H

#include "translation/translator_interface.h"

#include <string>
#include <vector>

#include "base/common.h"

namespace musketeer {
namespace translator {

using namespace musketeer::ir; // NOLINT

class TranslatorGraphInterface : public TranslatorInterface {
 public:
  TranslatorGraphInterface(const op_nodes& dag, const string& class_name):
    TranslatorInterface(dag, class_name) {
  }

 protected:
  // Returns the number of iterations.
  // TODO(ionel): This makes the following assumptions:
  //  1. the condition will be smt_0 < VAL
  //  2. smt_0 is initialized to 0 => we want to do VAL iterations
  string GetNumIters(WhileOperator* op) {
    ConditionTree* tree = op->get_condition_tree();
    if (tree->isBinary() && tree->get_right()->isValue()) {
      return tree->get_right()->get_value()->get_value();
    } else {
      LOG(ERROR) << "Unsupported while loop condition";
      return NULL;
    }
  }

  string GenOperatorCode(
      unordered_map<string, vector<string> >* rel_var_names,
      OperatorInterface* op) {
    switch (op->get_type()) {
    case AGG_OP:
      return GenOpCode(rel_var_names, dynamic_cast<AggOperator*>(op));
    case COUNT_OP:
      return GenOpCode(rel_var_names, dynamic_cast<CountOperator*>(op));
    case DIFFERENCE_OP:
      return GenOpCode(rel_var_names, dynamic_cast<DifferenceOperator*>(op));
    case DIV_OP:
      return GenOpCode(rel_var_names, dynamic_cast<DivOperator*>(op));
    case INTERSECTION_OP:
      return GenOpCode(rel_var_names,
                       dynamic_cast<IntersectionOperator*>(op));
    case JOIN_OP:
      return GenOpCode(rel_var_names, dynamic_cast<JoinOperator*>(op));
    case MAX_OP:
      return GenOpCode(rel_var_names, dynamic_cast<MaxOperator*>(op));
    case MIN_OP:
      return GenOpCode(rel_var_names, dynamic_cast<MinOperator*>(op));
    case MUL_OP:
      return GenOpCode(rel_var_names, dynamic_cast<MulOperator*>(op));
    case PROJECT_OP:
      return GenOpCode(rel_var_names, dynamic_cast<ProjectOperator*>(op));
    case SELECT_OP:
      return GenOpCode(rel_var_names, dynamic_cast<SelectOperator*>(op));
    case SORT_OP:
      return GenOpCode(rel_var_names, dynamic_cast<SortOperator*>(op));
    case SUB_OP:
      return GenOpCode(rel_var_names, dynamic_cast<SubOperator*>(op));
    case SUM_OP:
      return GenOpCode(rel_var_names, dynamic_cast<SumOperator*>(op));
    case UNION_OP:
      return GenOpCode(rel_var_names, dynamic_cast<UnionOperator*>(op));
    case WHILE_OP:
      return GenOpCode(rel_var_names, dynamic_cast<WhileOperator*>(op));
    default: {
      LOG(ERROR) << "Unexpected gen operator type!";
      return NULL;
    }
    }
  }

  virtual string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                           AggOperator* op) = 0;
  virtual string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                           CountOperator* op) = 0;
  virtual string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                           DifferenceOperator* op) = 0;
  virtual string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                           DivOperator* op) = 0;
  virtual string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                           IntersectionOperator* op) = 0;
  virtual string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                           JoinOperator* op) = 0;
  virtual string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                           MaxOperator* op) = 0;
  virtual string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                           MinOperator* op) = 0;
  virtual string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                           MulOperator* op) = 0;
  virtual string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                           ProjectOperator* op) = 0;
  virtual string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                           SelectOperator* op) = 0;
  virtual string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                           SortOperator* op) = 0;
  virtual string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                           SubOperator* op) = 0;
  virtual string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                           SumOperator* op) = 0;
  virtual string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                           UnionOperator* op) = 0;
  virtual string GenOpCode(unordered_map<string, vector<string> >* rel_var_names,
                           WhileOperator* op) = 0;

  string UpdateGroupVertexValue(const string& var_to_update,
      unordered_map<string, vector<string> >* rel_var_names,
      OperatorInterface* op) {
    switch (op->get_type()) {
    case AGG_OP:
      return UpdateGroupVertexVal(var_to_update, rel_var_names,
                                  dynamic_cast<AggOperator*>(op));
    case COUNT_OP:
      return UpdateGroupVertexVal(var_to_update, rel_var_names,
                                  dynamic_cast<CountOperator*>(op));
    case MAX_OP:
      return UpdateGroupVertexVal(var_to_update, rel_var_names,
                                  dynamic_cast<MaxOperator*>(op));
    case MIN_OP:
      return UpdateGroupVertexVal(var_to_update, rel_var_names,
                                  dynamic_cast<MinOperator*>(op));
    default: {
      LOG(ERROR) << "Unexpected update group vertex operator type!";
      return NULL;
    }
    }
  }

  virtual string UpdateGroupVertexVal(
      const string& var_to_update,
      unordered_map<string, vector<string> >* rel_var_names,
      AggOperator* op) {
    LOG(FATAL) << "Unsupported group operator";
    return NULL;
  }
  virtual string UpdateGroupVertexVal(
      const string& var_to_update,
      unordered_map<string, vector<string> >* rel_var_names,
      CountOperator* op) {
    LOG(FATAL) << "Unsupported group operator";
    return NULL;
  }
  virtual string UpdateGroupVertexVal(
      const string& var_to_update,
      unordered_map<string, vector<string> >* rel_var_names,
      MaxOperator* op) {
    LOG(FATAL) << "Unsupported group operator";
    return NULL;
  }
  virtual string UpdateGroupVertexVal(
      const string& var_to_update,
      unordered_map<string, vector<string> >* rel_var_names,
      MinOperator* op) {
    LOG(FATAL) << "Unsupported group operator";
    return NULL;
  }
};

} // namespace translator
} // namespace musketeer
#endif
