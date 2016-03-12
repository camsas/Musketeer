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

#ifndef MUSKETEER_TRANSLATOR_WILDCHERRY_H
#define MUSKETEER_TRANSLATOR_WILDCHERRY_H

#include "translation/translator_interface.h"

#include <ctemplate/template.h>

#include <cstring>
#include <iostream>
#include <queue>
#include <set>
#include <string>
#include <vector>

#include "base/common.h"
#include "ir/column.h"
#include "translation/job_code.h"

namespace musketeer {
namespace translator {

  using namespace musketeer::ir;  // NOLINT

  class TranslatorWildCherry : public TranslatorInterface {

  public:
    TranslatorWildCherry(const op_nodes& dag, const string& class_name);
    string GenerateCode();

  private:
    JobCode* Translate(AggOperator* op);
    JobCode* Translate(CountOperator* op);
    JobCode* Translate(CrossJoinOperator* op);
    JobCode* Translate(DifferenceOperator* op);
    JobCode* Translate(DistinctOperator* op);
    JobCode* Translate(DivOperator* op);
    JobCode* Translate(IntersectionOperator* op);
    JobCode* Translate(JoinOperator* op);
    JobCode* Translate(MaxOperator* op);
    JobCode* Translate(MinOperator* op);
    JobCode* Translate(MulOperator* op);
    JobCode* Translate(ProjectOperator* op);
    JobCode* Translate(SelectOperator* op);
    JobCode* Translate(SortOperator* op);
    JobCode* Translate(SubOperator* op);
    JobCode* Translate(SumOperator* op);
    JobCode* Translate(UdfOperator* op);
    JobCode* Translate(UnionOperator* op);
    JobCode* Translate(WhileOperator* op);
    string GenerateTmpPath(const string& path);
    string GenerateGroupByKey(const vector<Column*>& group_bys);
    string GetBinaryPath(OperatorInterface* op);
    string GetPath(OperatorInterface* op);
    string GetSourcePath(OperatorInterface* op);
    string GetSourceFile(OperatorInterface* op);
    void CompileAll(OperatorInterface* op);
    void PrepareCodeDirectory(OperatorInterface* op);
    void print_op_node(shared_ptr<OperatorNode>  cur_node, int indent);
    string GenerateOp(int indent );
    string IterateOps(vector<shared_ptr<OperatorNode> >& nodes, int indent );


    queue<shared_ptr<OperatorNode> > _to_visit;
    set<shared_ptr<OperatorNode> > _visited;
    set<string> _hdfs_inputs;
    set<string> _hdfs_outputs;
    set<string> _intermediates;
    string _bash_script;

  };

} // translator
} // musketeer
#endif
