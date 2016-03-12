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

#ifndef MUSKETEER_TRANSLATOR_INTERFACE_H
#define MUSKETEER_TRANSLATOR_INTERFACE_H

#include <ctemplate/template.h>
#include <stdint.h>

#include <string>
#include <vector>

#include "base/common.h"
#include "base/flags.h"
#include "base/utils.h"
#include "frontends/operator_node.h"
#include "ir/operator_interface.h"
#include "ir/agg_operator.h"
#include "ir/count_operator.h"
#include "ir/cross_join_operator.h"
#include "ir/difference_operator.h"
#include "ir/distinct_operator.h"
#include "ir/div_operator.h"
#include "ir/intersection_operator.h"
#include "ir/join_operator.h"
#include "ir/max_operator.h"
#include "ir/min_operator.h"
#include "ir/mul_operator.h"
#include "ir/project_operator.h"
#include "ir/select_operator.h"
#include "ir/sort_operator.h"
#include "ir/sub_operator.h"
#include "ir/sum_operator.h"
#include "ir/udf_operator.h"
#include "ir/union_operator.h"
#include "ir/while_operator.h"
#include "translation/job_code.h"

namespace musketeer {
namespace translator {

using namespace musketeer::ir; // NOLINT

typedef vector<shared_ptr<OperatorNode> > op_nodes;

class TranslatorInterface {
 public:
  TranslatorInterface(const op_nodes& dag_, const string& class_name_):
    dag(dag_), class_name(class_name_) {
  }

  const vector<shared_ptr<OperatorNode> >& get_dag() {
    return dag;
  }

  // It will generate the code and then compile it. It returns the path to the
  // binary/jar.
  virtual string GenerateCode() = 0;

 protected:
  JobCode* TranslateOperator(OperatorInterface *op) {
    switch (op->get_type()) {
    case AGG_OP:
      return Translate(dynamic_cast<AggOperator*>(op));
    case COUNT_OP:
      return Translate(dynamic_cast<CountOperator*>(op));
    case CROSS_JOIN_OP:
      return Translate(dynamic_cast<CrossJoinOperator*>(op));
    case DIFFERENCE_OP:
      return Translate(dynamic_cast<DifferenceOperator*>(op));
    case DISTINCT_OP:
      return Translate(dynamic_cast<DistinctOperator*>(op));
    case DIV_OP:
      return Translate(dynamic_cast<DivOperator*>(op));
    case INTERSECTION_OP:
      return Translate(dynamic_cast<IntersectionOperator*>(op));
    case JOIN_OP:
      return Translate(dynamic_cast<JoinOperator*>(op));
    case MAX_OP:
      return Translate(dynamic_cast<MaxOperator*>(op));
    case MIN_OP:
      return Translate(dynamic_cast<MinOperator*>(op));
    case MUL_OP:
      return Translate(dynamic_cast<MulOperator*>(op));
    case PROJECT_OP:
      return Translate(dynamic_cast<ProjectOperator*>(op));
    case SELECT_OP:
      return Translate(dynamic_cast<SelectOperator*>(op));
    case SORT_OP:
      return Translate(dynamic_cast<SortOperator*>(op));
    case SUB_OP:
      return Translate(dynamic_cast<SubOperator*>(op));
    case SUM_OP:
      return Translate(dynamic_cast<SumOperator*>(op));
    case UDF_OP:
      return Translate(dynamic_cast<UdfOperator*>(op));
    case UNION_OP:
      return Translate(dynamic_cast<UnionOperator*>(op));
    case WHILE_OP:
      return Translate(dynamic_cast<WhileOperator*>(op));
    default:
      LOG(ERROR) << "Unexpected operator type: " << op->get_type();
    }
    return NULL;
  }

  // Returns path to the binary/archive file. (e.g. /home/rufus/Select.jar.
  virtual JobCode* Translate(AggOperator* op) {
    // Only frameworks that support AGG must implement it.
    LOG(FATAL) << "AGG operator not supported by framework!";
    return NULL;
  }

  virtual JobCode* Translate(CountOperator* op) {
    // Only frameworks that support COUNT must implement it.
    LOG(FATAL) << "COUNT operator not supported by framework!";
    return NULL;
  }

  virtual JobCode* Translate(CrossJoinOperator* op) {
    // Only frameworks that support CROSS_JOIN must implement it.
    LOG(FATAL) << "CROSS JOIN operator not supported by framework!";
    return NULL;
  }

  virtual JobCode* Translate(DifferenceOperator* op) {
    // Only frameworks that support DIFFERENCE must implement it.
    LOG(FATAL) << "DIFFERENCE operator not supported by framework!";
    return NULL;
  }

  virtual JobCode* Translate(DistinctOperator* op) {
    // Only frameworks that support DISTINCT must implement it.
    LOG(FATAL) << "DISTINCT operator not supported by framework!";
    return NULL;
  }

  virtual JobCode* Translate(DivOperator* op) {
    // Only frameworks that support DIV must implement it.
    LOG(FATAL) << "DIV operator not supported by framework!";
    return NULL;
  }

  virtual JobCode* Translate(IntersectionOperator* op) {
    // Only frameworks that support INTERSECTION must implement it.
    LOG(FATAL) << "INTERSECTION operator not supported by framework!";
    return NULL;
  }

  virtual JobCode* Translate(JoinOperator* op) {
    // Only frameworks that support JOIN must implement it.
    LOG(FATAL) << "JOIN operator not supported by framework!";
    return NULL;
  }

  virtual JobCode* Translate(MaxOperator* op) {
    // Only frameworks that support MAX must implement it.
    LOG(FATAL) << "MAX operator not supported by framework!";
    return NULL;
  }

  virtual JobCode* Translate(MinOperator* op) {
    // Only frameworks that support MIN must implement it.
    LOG(FATAL) << "MIN operator not supported by framework!";
    return NULL;
  }

  virtual JobCode* Translate(MulOperator* op) {
    // Only frameworks that support MUL must implement it.
    LOG(FATAL) << "MUL operator not supported by framework!";
    return NULL;
  }

  virtual JobCode* Translate(ProjectOperator* op) {
    // Only frameworks that support PROJECT must implement it.
    LOG(FATAL) << "PROJECT operator not supported by framework!";
    return NULL;
  }

  virtual JobCode* Translate(SelectOperator* op) {
    // Only frameworks that support SELECT must implement it.
    LOG(FATAL) << "SELECT operator not supported by framework!";
    return NULL;
  }

  virtual JobCode* Translate(SortOperator* op) {
    // Only frameworks that support SORT must implement it.
    LOG(FATAL) << "SORT operator not supported by framework!";
    return NULL;
  }

  virtual JobCode* Translate(SubOperator* op) {
    // Only frameworks that support SUB must implement it.
    LOG(FATAL) << "SUB operator not supported by framework!";
    return NULL;
  }

  virtual JobCode* Translate(SumOperator* op) {
    // Only frameworks that support SUM must implement it.
    LOG(FATAL) << "SUM operator not supported by framework!";
    return NULL;
  }

  virtual JobCode* Translate(UdfOperator* op) {
    // Only frameworks that support UDF must implement it.
    LOG(FATAL) << "UDF operator not supported by framework!";
    return NULL;
  }

  virtual JobCode* Translate(UnionOperator* op) {
    // Only frameworks that support UNION must implement it.
    LOG(FATAL) << "UNION operator not supported by framework!";
    return NULL;
  }
  virtual JobCode* Translate(WhileOperator* op) {
    // Only frameworks that support while loops must implement it.
    LOG(FATAL) << "WHILE loops not supported by framework!";
    return NULL;
  }
  // Returns the path including file name to the binary.
  virtual string GetBinaryPath(OperatorInterface* op) = 0;
  // Returns the path including file name to the source code.
  virtual string GetSourcePath(OperatorInterface* op) = 0;

  string AvoidNameClash(OperatorInterface* op, uint32_t index) {
    string output_relation = op->get_output_relation()->get_name();
    if (!op->get_relations()[index]->get_name().compare(output_relation)) {
      return output_relation + "_input";
    }
    return op->get_relations()[index]->get_name();
  }

  vector<shared_ptr<OperatorNode> > dag;
  string class_name;
};

} // namespace translator
} // namespace musketeer
#endif
