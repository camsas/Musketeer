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

#include "scheduling/operator_scheduler.h"

#include <iostream>
#include <map>
#include <string>

#include "base/hdfs_utils.h"
#include "frameworks/graphchi_dispatcher.h"
#include "frameworks/hadoop_dispatcher.h"
#include "frameworks/metis_dispatcher.h"
#include "frameworks/spark_dispatcher.h"
#include "ir/operator_interface.h"
#include "ir/while_operator.h"
#include "translation/translator_graphchi.h"
#include "translation/translator_hadoop.h"
#include "translation/translator_metis.h"
#include "translation/translator_spark.h"

namespace musketeer {
namespace scheduling {

  using musketeer::ir::BlackBoxOperator;
  using musketeer::ir::WhileOperator;

  OperatorScheduler::OperatorScheduler(const map<string, FrameworkInterface*>& fmws):
    SchedulerInterface(fmws) {
  }

  void OperatorScheduler::Schedule(shared_ptr<OperatorNode> node) {
    if (node->get_operator()->get_type() == WHILE_OP) {
      // IF a while operator is encountered then handle it in the scheduler.
      WhileOperator* while_op =
        dynamic_cast<WhileOperator*>(node->get_operator());
      while (while_op->get_condition_tree()->checkCondition()) {
        op_nodes children = node->get_children();
        for (op_nodes::iterator child_it = children.begin();
             child_it != children.end(); ++child_it) {
          Schedule(*child_it);
        }
      }
    } else {
      op_nodes children = node->get_children();
      for (op_nodes::iterator child_it = children.begin();
           child_it != children.end(); ++child_it) {
        Schedule(*child_it);
      }
      ScheduleAll(node);
    }
  }

  void OperatorScheduler::DynamicScheduleDAG(const op_nodes& dag) {
    ScheduleDAG(dag);
  }

  void OperatorScheduler::ScheduleDAG(const op_nodes& dag) {
    string output_relation = GetDagOutputs(dag)[0];
    // TODO(tach): FIX!
    //    optimiser_->optimiseDAG(dag);
    FrameworkInterface* fmw = fmws.find(FLAGS_force_framework)->second;
    LOG(INFO) << "Begin Schedule DAG";
    string binary_file = fmw->Translate(dag, output_relation);
    LOG(INFO) << "-------> *** ";
    LOG(INFO) << "End Schedule DAG";
    fmw->Dispatch(binary_file, output_relation);
  }

  void OperatorScheduler::ScheduleAll(shared_ptr<OperatorNode> node) {
    string output_relation = "";
    OperatorInterface* op = node->get_operator();
    op_nodes dag = op_nodes();
    dag.push_back(node);
    // The black box operator is executed directly.
    if (op->get_type() == BLACK_BOX_OP) {
      BlackBoxOperator* box_op = dynamic_cast<BlackBoxOperator*>(op);
      FrameworkInterface* fmw = fmws.find(FmwToString(box_op->get_fmw()))->second;
      fmw->Dispatch(box_op->get_binary_path(),
                    box_op->get_output_relation()->get_name());
      return;
    }
    if (CheckRenameRequired(op)) {
      // TODO(ionel): Generate unique relation name.
      Relation* relation = op->get_output_relation();
      output_relation = relation->get_name();
      relation->set_name(output_relation + "_tmp");
    } else {
      // XXX(ionel): HACK! Removing the output_relation just in case it already
      // exists.
      removeHdfsDir(op->get_output_path());
    }
    FrameworkInterface* fmw = fmws.find(FLAGS_force_framework)->second;
    LOG(INFO) << "-------> Begin Schedule ALL ";
    string binary_file =
      fmw->Translate(dag, op->get_output_relation()->get_name());
    LOG(INFO) << "-------> ";
    fmw->Dispatch(binary_file, output_relation);
    LOG(INFO) << "-------> End Schedule ALL ";

    if (output_relation.compare("")) {
      // If output_relation is different from "" then the operator has been
      // changed.
      MoveOutputFromTmp(output_relation, op);
    }
  }

  void OperatorScheduler::MoveOutputFromTmp(const string& output_relation,
                                            OperatorInterface* op) {
    op->get_output_relation()->set_name(output_relation);
    string rel_dir = FLAGS_hdfs_input_dir + output_relation + "/";
    string tmp_rel_dir = FLAGS_hdfs_input_dir + output_relation + "_tmp/";
    // Remove input.
    removeHdfsDir(rel_dir + "*");
    // Move output.
    renameHdfsDir(tmp_rel_dir + "*", rel_dir);
    removeHdfsDir(tmp_rel_dir);
  }

} // namespace scheduling
} // namespace musketeer
