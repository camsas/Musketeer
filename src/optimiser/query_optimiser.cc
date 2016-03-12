// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
// Copyright (c) 2015 Natacha Crooks <ncrooks@mpi-sws.org>

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

#include "optimiser/query_optimiser.h"

namespace musketeer {

  QueryOptimiser::QueryOptimiser(bool active, HistoryStorage* history):
    history_(history), active_(active) {
    LOG(INFO) << "Starting Query Optimiser";
    initialiseCommutativityMatrix();
    initialiseRules(active);
  }

  void QueryOptimiser::optimiseDAG(op_nodes* dag) {
    bool changed = true;
    LOG(INFO) << "optimiseDAG";
    /* Keep applying rules until dag can no longer be further optimised */
    // TODO(Tach): someone keep all intermediate DAGs
    /* In certain cases, may add to the roots of the tree
       need to explicitly add those nodes */
    while (changed) {
      changed = navigateTree(dag);
    }
  }

  shared_ptr<OperatorNode> QueryOptimiser::copyNodes(
      shared_ptr<OperatorNode> obj) {
    LOG(INFO) << "copyNodes";
    OperatorInterface* copy_op = obj->get_operator()->clone();
    shared_ptr<OperatorNode> copy_node(new OperatorNode(copy_op));
    copy_node->set_children(obj->get_children());
    copy_node->set_parents(obj->get_parents());
    copy_node->set_children_loop(obj->get_loop_children());
    return copy_node;
  }

  /* op1 would go above op2 */
  bool QueryOptimiser::canReorder(shared_ptr<OperatorNode> op1,
                                  shared_ptr<OperatorNode> op2) {
    LOG(INFO) << "canReorder";
    return doCommute(op1, op2) || areIndependent(op1, op2) ||
      (op2->get_children().size() == 0);
  }

  bool QueryOptimiser::doCommute(shared_ptr<OperatorNode> op_left,
                                 shared_ptr<OperatorNode> op_right) {
    LOG(INFO) << "doCommute";
    OperatorType op1 = op_left->get_operator()->get_type();
    OperatorType op2 = op_right->get_operator()->get_type();
    return commutativity_matrix_[op1][op2];
  }

  bool QueryOptimiser::areIndependent(shared_ptr<OperatorNode> op1,
                                      shared_ptr<OperatorNode> op2) {
    LOG(INFO) << "areIndependent";
    bool independent = true;
    string output_op1 = op1->get_operator()->get_output_relation()->get_name();
    string output_op2 = op2->get_operator()->get_output_relation()->get_name();
    //TODO(Tach): inefficient, optimise
    vector<Relation*> input_relations_op1 =
      op1->get_operator()->get_relations();
    vector<Relation*> input_relations_op2 =
      op2->get_operator()->get_relations();
    for (vector<Relation*>::iterator it = input_relations_op1.begin() ;
         it != input_relations_op1.end(); ++it) {
      string name = (*it)->get_name();
      if (!strcmp(output_op1.c_str(), name.c_str())) {
        return false;
      }
    }
    // TODO(tach): currently can make no assumptions as to which one is parent
    // of the other
    for (vector<Relation*>::iterator it = input_relations_op2.begin();
         it != input_relations_op2.end(); ++it) {
      string name = (*it)->get_name();
      if (!strcmp(output_op2.c_str(), name.c_str())) {
        LOG(INFO) << "Depend on " << output_op2 << " and " << name;
        return false;
      }
    }
    return true;
  }

  bool QueryOptimiser::applyRules(shared_ptr<OperatorNode> child,
                                  shared_ptr<OperatorNode> parent,
                                  op_nodes* new_roots) {
    bool changed = false;
    LOG(INFO) << "applyRules";
    // for(vector<rule_t>::iterator it = rules_.begin() ;
    //  it!=rules_.end();
    //  ++it) {
    //   rule_t fn = **it ;
    //   changed = changed && fn(child,parent);
    // }
    if (active_) {
      changed = false // || filterMerge(child,parent, new_roots)
        || filterPushUp(child, parent, new_roots);
      //  || intersectionPushUp(child,parent, new_roots)
      //  || differencePushUp(child,parent,new_roots) || sortPushDown(child,parent,new_roots)
      //  || optimiseJoin(child,parent,new_roots)
    } else {
      LOG(INFO) << "Optimisations deactivated";
    }
    LOG(INFO) << "Changed " << changed;
    return changed;
  }

  bool QueryOptimiser::navigateTree(op_nodes* dag) {
    bool changed = false;
    LOG(INFO) << "navigateTree DAG size " << dag->size();
    for (op_nodes::iterator it = dag->begin(); it != dag->end(); ++it) {
      shared_ptr<OperatorNode> node = *it;
      bool opt = navigateTreeInternal(node, dag);
      changed = changed || opt;
    }
    return changed;
  }

  bool QueryOptimiser::navigateTreeInternal(shared_ptr<OperatorNode> node,
                                            op_nodes* dag) {
    op_nodes children = node->get_children();
    int size = children.size();
    bool changed = false;
    if (size == 0) {
      return false;
    } else {
     for (op_nodes::iterator it = children.begin(); it != children.end(); ++it) {
       shared_ptr<OperatorNode> kid = *it;
       bool chan = applyRules(kid, node, dag);
       if (chan) {
         navigateTreeInternal(node, dag);
       } else {
         navigateTreeInternal(kid, dag);
       }
       changed = changed && chan;
     }
    }
    return changed;
  }

  void QueryOptimiser::deleteNode(shared_ptr<OperatorNode> child,
                                  shared_ptr<OperatorNode> parent) {
    LOG(INFO) << "deleteNode";
    op_nodes::iterator it = find(parent->get_children().begin(),
                                 parent->get_children().end(), child);
    parent->get_children().erase(it);
    parent->get_children().insert(parent->get_children().begin(),
                                  child->get_children().begin(),
                                  child->get_children().end());
    for (op_nodes::iterator it = child->get_children().begin();
         it != child->get_children().end(); ++it ) {
      shared_ptr<OperatorNode> chi = *it;
       /* Find  parent */
      op_nodes::iterator it_par =
        find(chi->get_parents().begin(), chi->get_parents().end(), parent);
      *it_par = parent;
    }
  }

  void QueryOptimiser::switchNodes(shared_ptr<OperatorNode> child,
                                   shared_ptr<OperatorNode> parent,
                                   op_nodes* dag) {
    LOG(INFO) << "switchNodes";
    op_nodes parents_of_child = child->get_parents();
    op_nodes parents_of_parent = parent->get_parents();
    op_nodes childs_of_child = child->get_children();
    op_nodes childs_of_parent = parent->get_children();
    shared_ptr<OperatorNode> dup_node;
    OperatorInterface* op_inter_child = child->get_operator();
    OperatorInterface* op_inter_parent = parent->get_operator();

    /* Determine whether there is a need to duplicate */
    // TODO(tach): for now: always duplicate, but possible to optimise
    if ((op_inter_child->get_relations().size() == 1) &&
        (op_inter_parent->get_relations().size() == 1)) {
      LOG(INFO) << "Simple case, inverse nodes";
      /* Do renaming here */
      if (parent->get_parents().size() == 0) {
        /* If Parent was a root, remove it */
        op_nodes::iterator it = find(dag->begin(), dag->end(), parent);
        if (it != dag->end()) {
          dag->erase(it);
        }
      }
      swapOperators(child, parent);
      if (child->get_parents().size() == 0) {
        /* If Child has no parent, add it to roots */
        dag->push_back(child);
      }
    } else if ((op_inter_child->get_relations().size() == 1) &&
               (op_inter_parent->get_relations().size() == 2)) {
      LOG(INFO) << "Requires duplication";
      Relation* left = op_inter_parent->get_relations()[0];
      Relation* right = op_inter_parent->get_relations()[1];
      swapOperators(child, parent);

      dup_node = copyNodes(child);

      /* Dealing with operators */
      left = op_inter_child->get_relations()[0];
      right = op_inter_child->get_relations()[1];
      vector<Relation*> relations_left = vector<Relation*>();
      relations_left.push_back(left);
      vector<Relation*> relations_right = vector<Relation*>();
      relations_right.push_back(right);
      child->get_operator()->set_relations(relations_left);
      dup_node->get_operator()->set_relations(relations_right);

      //TODO(HACK,HACK,HACK)
      string left_name = left->get_name() + "reorder";
      string right_name = right->get_name() + "reorder";

      Relation* relation_left_filter = left->copy(left_name);
      Relation* relation_right_filter = right->copy(right_name);

      child->get_operator()->set_output_relation(relation_left_filter);
      dup_node->get_operator()->set_output_relation(relation_right_filter);
      dup_node->get_operator()->set_offset(
          child->get_operator()->get_relations()[0]->get_columns(). size());

      vector<Relation*> inputs = vector<Relation*>();
      inputs.push_back(relation_left_filter);
      inputs.push_back(relation_right_filter);
      op_inter_parent->set_relations(inputs);

      /* Dealing with nodes */
      op_nodes left_children = op_nodes();
      op_nodes right_children = op_nodes();
      left_children.push_back(parent);
      right_children.push_back(parent);
      child->set_children(left_children);
      dup_node->set_children(right_children);
      op_nodes parents = op_nodes();
      parents.push_back(child);
      parents.push_back(dup_node);
      parent->set_parents(parents);
      updateOperator(parent->get_operator());
      updateOperator(child->get_operator());
      if (dup_node) updateOperator(dup_node->get_operator());
      if (dup_node && (dup_node->get_parents().size() == 0)) {
        LOG(INFO) << "Adding Dup Node";
        dag->push_back(dup_node);
      }
     if (child->get_parents().size() == 0) {
        replaceInVector(dag, parent, child);
      }
    } else {
      LOG(FATAL) << "Unimplemented";
    }
  }


  void QueryOptimiser::updateOperator(OperatorInterface* op) {
    switch (op->get_type()) {
    case AGG_OP: {
      AggOperator* opCast = (dynamic_cast<AggOperator*>(op));
      string relation = opCast->get_relations()[0]->get_name();
      vector<Column*> cols = opCast->get_columns();
      for (vector<Column*>::iterator it = cols.begin(); it != cols.end();
           ++it) {
        (*it)->rename(relation);
      }
      vector<Column*> group_bys = opCast->get_group_bys();
      for (vector<Column*>::iterator it = group_bys.begin();
           it != group_bys.end(); ++it) {
        (*it)->rename(relation);
      }
      break;
    }
    case COUNT_OP: {
      CountOperator* opCast = (dynamic_cast<CountOperator*>(op));
      string relation = opCast->get_relations()[0]->get_name();
      opCast->get_column()->rename(relation);
      vector<Column*> group_bys = opCast->get_group_bys();
      for (vector<Column*>::iterator it = group_bys.begin();
           it != group_bys.end(); ++it) {
        (*it)->rename(relation);
      }
      break;
    }
    case DIFFERENCE_OP: {
      DifferenceOperator* opCast = dynamic_cast<DifferenceOperator*>(op);
      break;
    }
    case DIV_OP: {
      DivOperator* opCast = (dynamic_cast<DivOperator*>(op));
      break;
    }
    case INTERSECTION_OP: {
      IntersectionOperator* opCast = (dynamic_cast<IntersectionOperator*>(op));
      break;
    }
    case JOIN_OP: {
      JoinOperator* opCast = (dynamic_cast<JoinOperator*>(op));
      string left = opCast->get_relations()[0]->get_name();
      string right = opCast->get_relations()[1]->get_name();
      opCast->get_col_left()->rename(left);
      opCast->get_col_left()->rename(right);
      break;
    }
    case MAX_OP: {
      MaxOperator* opCast = (dynamic_cast<MaxOperator*>(op));
      string relation = opCast->get_relations()[0]->get_name();
      opCast->get_column()->rename(relation);
      vector<Column*> group_bys = opCast->get_group_bys();
      for (vector<Column*>::iterator it = group_bys.begin();
           it != group_bys.end(); ++it) {
        (*it)->rename(relation);
      }
      break;
    }
    case MIN_OP: {
      MinOperator* opCast = (dynamic_cast<MinOperator*>(op));
      string relation = opCast->get_relations()[0]->get_name();
      opCast->get_column()->rename(relation);
      vector<Column*> group_bys = opCast->get_group_bys();
      for (vector<Column*>::iterator it = group_bys.begin();
           it != group_bys.end(); ++it) {
        (*it)->rename(relation);
      }
      break;
    }
    case MUL_OP: {
      MulOperator* opCast = (dynamic_cast<MulOperator*>(op));
      break;
    }
    case PROJECT_OP: {
      ProjectOperator* opCast = (dynamic_cast<ProjectOperator*>(op));
      vector<Column*> columns = opCast->get_columns();
      string relation = opCast->get_relations()[0]->get_name();
      for (vector<Column*>::iterator it = columns.begin();
           it != columns.end(); ++it) {
        (*it)->rename(relation);
      }
      break;
    }
    case SELECT_OP: {
      SelectOperator* opCast = (dynamic_cast<SelectOperator*>(op));
      vector<Column*> columns = opCast->get_columns();
      string relation = opCast->get_relations()[0]->get_name();
      for (vector<Column*>::iterator it = columns.begin();
           it != columns.end(); ++it) {
        (*it)->rename(relation);
      }
      break;
    }
    case SORT_OP: {
      SortOperator* opCast = (dynamic_cast<SortOperator*>(op));
      string relation = opCast->get_relations()[0]->get_name();
      opCast->get_column()->rename(relation);
      break;
    }
    case SUB_OP: {
      SubOperator* opCast = (dynamic_cast<SubOperator*>(op));
      break;
    }
    case SUM_OP: {
      SumOperator* opCast = (dynamic_cast<SumOperator*>(op));
      break;
    }
    case UDF_OP: {
      UdfOperator* opCast = (dynamic_cast<UdfOperator*>(op));
      break;
    }
    case UNION_OP: {
      UnionOperator* opCast = (dynamic_cast<UnionOperator*>(op));
      break;
    }
    case WHILE_OP: {
      WhileOperator* opCast = (dynamic_cast<WhileOperator*>(op));
      break;
    }
    default:
      LOG(ERROR) << "Unexpected operator type: " << op->get_type();
    }
  }

  void QueryOptimiser::rewriteCondition(string relation,
                                        pANTLR3_BASE_TREE condition_tree) {
    LOG(INFO) << "rewriteCondition";
  }

  void QueryOptimiser::swapOperators(shared_ptr<OperatorNode> child,
                                     shared_ptr<OperatorNode> parent) {
    LOG(INFO) << "swapOperators";
    op_nodes parents_of_child = child->get_parents();
    op_nodes parents_of_parent = parent->get_parents();
    op_nodes childs_of_child = child->get_children();
    op_nodes childs_of_parent = parent->get_children();
    /* Handling Operators */
    OperatorInterface* op_inter_child = child->get_operator();
    OperatorInterface* op_inter_parent = parent->get_operator();

    Relation* swap = op_inter_parent->get_output_relation();
    op_inter_parent->set_output_relation(op_inter_child->get_output_relation());
    op_inter_child->set_output_relation(swap);
    vector<Relation*> swaps = op_inter_child->get_relations();
    if (op_inter_parent->get_relations().size() != op_inter_parent->get_relations().size()) {
      LOG(ERROR) << "ERROR: Not Implemented. Logic wrong";
      //TODO(Tach): fix
    }
    op_inter_child->set_relations(op_inter_parent->get_relations());
    vector<Relation*> relations = vector<Relation*>();
    relations.push_back(swaps.front());
    op_inter_parent->set_relations(relations);

    /* Handling Nodes */
    for (op_nodes::iterator it = childs_of_parent.begin();
         it != childs_of_parent.end(); ++it ) {
      shared_ptr<OperatorNode> chi = *it;
      if (chi != child) {
        op_nodes::iterator it_par = find(chi->get_parents().begin(),
                                         chi->get_parents().end(), parent);
        *it_par = child;
      }
    }
    for (op_nodes::iterator it = parents_of_parent.begin();
         it != parents_of_parent.end(); ++it ) {
      shared_ptr<OperatorNode> par = *it;
      op_nodes::iterator it_par = find(par->get_children().begin(),
                                       par->get_children().end(), parent);
      *it_par = child;
    }
    for (op_nodes::iterator it = childs_of_child.begin();
         it != childs_of_child.end(); ++it) {
      shared_ptr<OperatorNode> chi = *it;
      op_nodes::iterator it_chi = find(chi->get_parents().begin(),
                                       chi->get_parents().end(), child);
      *it_chi = parent;
    }
    op_nodes::iterator r = find(parents_of_child.begin(),
                                parents_of_child.end(),
                                parent);
    if (r != parents_of_child.end()) {
      parents_of_child.erase(r);
    }

    op_nodes sw = childs_of_child;
    childs_of_child = childs_of_parent;
    childs_of_parent = sw;
    sw = parents_of_parent;
    parents_of_parent = parents_of_child;
    parents_of_parent.push_back(child);
    parents_of_child = sw;
    parent->set_parents(parents_of_parent);
    child->set_parents(parents_of_child);
    op_nodes::iterator it;
    if (childs_of_parent.size() > 0) {
      it = find(childs_of_parent.begin(), childs_of_parent.end(), child);
      if (it != childs_of_parent.end()) {
        childs_of_parent.erase(it);
      }
    }
    parent->set_children(childs_of_parent);

    if (childs_of_child.size() > 0)  {
      it = find(childs_of_child.begin(),
                childs_of_child.end(), child);
      if (it != childs_of_child.end()) {
        childs_of_child.erase(it);
      }
    }
    childs_of_child.push_back(parent);
    child->set_children(childs_of_child);

    op_nodes nodes = child->get_parents();
    it = nodes.begin();
    if (nodes.size() > 0) {
      it = find(nodes.begin(), nodes.end(), parent);
      if (it != nodes.end()) {
        nodes.erase(it);
      }
    }
    child->set_parents(nodes);
  }

  bool QueryOptimiser::filterMerge(shared_ptr<OperatorNode> child,
                                   shared_ptr<OperatorNode> parent,
                                   op_nodes* dag) {
    LOG(INFO) << "filterMerge";
    bool changed = false;
    if ((child->get_operator()->get_type() == SELECT_OP) &&
        (parent->get_operator()->get_type() == SELECT_OP))  {
      vector<pANTLR3_BASE_TREE> conditions =
        static_cast<SelectOperator*>(child->get_operator())->get_condition_tree();
      for (vector<pANTLR3_BASE_TREE>::iterator it = conditions.begin();
           it != conditions.end(); ++it) {
        (static_cast<SelectOperator*> (parent->get_operator()))->addCondition(*it);
      }
      deleteNode(child, parent);
      return true;
    } else {
      LOG(INFO) << "Rule Cannot be Applied";
    }
    return false;
  }

  bool QueryOptimiser::filterPushUp(shared_ptr<OperatorNode> child,
                                    shared_ptr<OperatorNode> parent,
                                    op_nodes* dag) {
    LOG(INFO) << "filterPushUp";
    if ((child == NULL || parent == NULL)) {
      return false;
    }
    if ((child->get_operator()->get_type() == SELECT_OP)) {
      if (canReorder(child, parent)) {
        switchNodes(child, parent, dag);
        return true;
      } else {
        LOG(INFO) << "Cannot be reodered";
        return false;
      }
    } else {
      LOG(INFO) << "Rule Cannot Be Applied";
      return false;
    }
  }

  bool QueryOptimiser::intersectionPushUp(shared_ptr<OperatorNode> child,
                                          shared_ptr<OperatorNode> parent,
                                          op_nodes* dag) {
    LOG(INFO) << "intersectionPushUp";
    if ((child->get_operator()->get_type() == INTERSECTION_OP)) {
      if (canReorder(child, parent)) {
        switchNodes(child, parent, dag);
        return true;
      }
    } else {
      LOG(INFO) << "Rule Cannot Be Applied";
      return false;
    }
    return false;
  }

  bool QueryOptimiser::differencePushUp(shared_ptr<OperatorNode> child,
                                        shared_ptr<OperatorNode> parent,
                                        op_nodes* new_root) {
    LOG(INFO) << "differencePushUp";
    if ((child->get_operator()->get_type() == DIFFERENCE_OP)) {
      if (canReorder(child, parent)) {
        switchNodes(child, parent, new_root);
        return true;
      }
    } else {
      LOG(INFO) << "Rule Cannot Be Applied";
      return false;
    }
    return false;
  }

  bool QueryOptimiser::optimiseJoin(shared_ptr<OperatorNode> child,
                                    shared_ptr<OperatorNode> parent,
                                    op_nodes* dag) {
    LOG(INFO) << "optimiseJoin";
    bool changed = false;
    if ((child->get_operator()->get_type() == JOIN_OP)) {
      /* It is only worth reordering a join if there
         is a significant asymetry in size */
      Relation* relA = child->get_operator()->get_relations().front();
      Relation* relB = child->get_operator()->get_relations().back();
      // TODO(tach/ionel): Fix this. It only uses the first output of a job.
      uint64_t sizA = history_->get_expected_data_size(relA->get_name())[0].second;
      uint64_t sizB = history_->get_expected_data_size(relB->get_name())[0].second;
      // TODO(tach/ionel): Check for overflow.
      if (sizA != 0 && sizB != 0 &&
          ((sizA > (kSizeFactor * sizB)) || (sizB > (kSizeFactor* sizA)))) {
        if (canReorder(child, parent)) {
          switchNodes(child, parent, dag);
          return true;
        }
        return false;
      }
    } else {
      LOG(INFO) << "Rule Cannot Be Applied";
      return false;
    }
    return false;
  }

  bool QueryOptimiser::unionPushDown(shared_ptr<OperatorNode> child,
                                     shared_ptr<OperatorNode> parent,
                                     op_nodes* dag) {
    LOG(INFO) << "unionPushDown";
    if ((parent->get_operator()->get_type() == UNION_OP)) {
      if (canReorder(child, parent)) {
        switchNodes(parent, child, dag);
        return true;
      }
    } else {
      LOG(INFO) << "Rule Cannot Be Applied";
      return false;
    }
    return false;
  }

  bool QueryOptimiser::sortPushDown(shared_ptr<OperatorNode> child,
                                    shared_ptr<OperatorNode> parent,
                                    op_nodes* dag) {
    LOG(INFO) << "sortPushDown";
    if ((parent->get_operator()->get_type() == SORT_OP)) {
      if (canReorder(child, parent)) {
        switchNodes(parent, child, dag);
        return true;
      }
    } else {
      LOG(INFO) << "Rule Cannot Be Applied";
      return false;
    }
    return false;
  }

  bool QueryOptimiser::optimiseProject(shared_ptr<OperatorNode> child,
                                       shared_ptr<OperatorNode> parent,
                                       op_nodes* dag)  {
    LOG(INFO) << "optimiseProject";
    bool changed = false;
    //TODO(implement)
    return changed;
  }

  void QueryOptimiser::initialiseCommutativityMatrix() {
    LOG(INFO) << "initialiseCommutativityMatrix";
    for (int i = 0; i < kNumberOperators; i++) {
      commutativity_matrix_[AGG_OP][i] = false;
      commutativity_matrix_[i][AGG_OP] = false;
      commutativity_matrix_[BLACK_BOX_OP][i] = false;
      commutativity_matrix_[i][BLACK_BOX_OP] = false;
      commutativity_matrix_[i][WHILE_OP] = false;
      commutativity_matrix_[WHILE_OP][i] = false;
      commutativity_matrix_[SORT_OP][i] = true;
      commutativity_matrix_[i][SORT_OP] = true;
      commutativity_matrix_[UDF_OP][i] = false;
      commutativity_matrix_[i][UDF_OP] = false;
      commutativity_matrix_[COUNT_OP][i] = false;
      commutativity_matrix_[i][COUNT_OP] = false;
    }
    commutativity_matrix_[DIFFERENCE_OP][DIFFERENCE_OP] = false;
    commutativity_matrix_[DIFFERENCE_OP][DIV_OP] = false;
    commutativity_matrix_[DIFFERENCE_OP][INTERSECTION_OP] = false;
    commutativity_matrix_[DIFFERENCE_OP][JOIN_OP] = true;
    commutativity_matrix_[DIFFERENCE_OP][MAX_OP] = false;
    commutativity_matrix_[DIFFERENCE_OP][MIN_OP] = false;
    commutativity_matrix_[DIFFERENCE_OP][MUL_OP] = false;
    commutativity_matrix_[DIFFERENCE_OP][PROJECT_OP] = false;
    commutativity_matrix_[DIFFERENCE_OP][SELECT_OP] = true;
    commutativity_matrix_[DIFFERENCE_OP][SUB_OP] = false;
    commutativity_matrix_[DIFFERENCE_OP][SUM_OP] = false;
    commutativity_matrix_[DIFFERENCE_OP][UNION_OP] = false;
    commutativity_matrix_[DIV_OP][COUNT_OP] = false;
    commutativity_matrix_[DIV_OP][DIFFERENCE_OP] = false;
    commutativity_matrix_[DIV_OP][DIV_OP] = false;
    commutativity_matrix_[DIV_OP][INTERSECTION_OP] = true;
    commutativity_matrix_[DIV_OP][JOIN_OP] = true;
    commutativity_matrix_[DIV_OP][MAX_OP] = true;
    commutativity_matrix_[DIV_OP][MIN_OP] = true;
    commutativity_matrix_[DIV_OP][MUL_OP] = false;
    commutativity_matrix_[DIV_OP][PROJECT_OP] = false;
    commutativity_matrix_[DIV_OP][SELECT_OP] = false;
    commutativity_matrix_[DIV_OP][SUB_OP] = false;
    commutativity_matrix_[DIV_OP][SUM_OP] = false;
    commutativity_matrix_[DIV_OP][UNION_OP] = true;
    commutativity_matrix_[INTERSECTION_OP][COUNT_OP] = false;
    commutativity_matrix_[INTERSECTION_OP][DIFFERENCE_OP] = false;
    commutativity_matrix_[INTERSECTION_OP][DIV_OP] = true;
    commutativity_matrix_[INTERSECTION_OP][INTERSECTION_OP] = true;
    commutativity_matrix_[INTERSECTION_OP][JOIN_OP] = true;
    commutativity_matrix_[INTERSECTION_OP][MAX_OP] = true;
    commutativity_matrix_[INTERSECTION_OP][MIN_OP] = true;
    commutativity_matrix_[INTERSECTION_OP][MUL_OP] = false;
    commutativity_matrix_[INTERSECTION_OP][PROJECT_OP] = true;
    commutativity_matrix_[INTERSECTION_OP][SUB_OP] = false;
    commutativity_matrix_[INTERSECTION_OP][SUM_OP] = false;
    commutativity_matrix_[INTERSECTION_OP][UNION_OP] = false;
    commutativity_matrix_[JOIN_OP][COUNT_OP] = false;
    commutativity_matrix_[JOIN_OP][DIFFERENCE_OP] = false;
    commutativity_matrix_[JOIN_OP][DIV_OP] = false;
    commutativity_matrix_[JOIN_OP][INTERSECTION_OP] = true;
    commutativity_matrix_[JOIN_OP][JOIN_OP] = true;
    commutativity_matrix_[JOIN_OP][MAX_OP] = false;
    commutativity_matrix_[JOIN_OP][MIN_OP] = false;
    commutativity_matrix_[JOIN_OP][MUL_OP] = false;
    commutativity_matrix_[JOIN_OP][PROJECT_OP] = false;
    commutativity_matrix_[JOIN_OP][SELECT_OP] = true;
    commutativity_matrix_[JOIN_OP][SUB_OP] = false;
    commutativity_matrix_[JOIN_OP][SUM_OP] = false;
    commutativity_matrix_[JOIN_OP][UNION_OP] = false;
    commutativity_matrix_[MAX_OP][COUNT_OP] = false;
    commutativity_matrix_[MAX_OP][DIFFERENCE_OP] = false;
    commutativity_matrix_[MAX_OP][DIV_OP] = false;
    commutativity_matrix_[MAX_OP][INTERSECTION_OP] = false;
    commutativity_matrix_[MAX_OP][JOIN_OP] = false;
    commutativity_matrix_[MAX_OP][MAX_OP] = false;
    commutativity_matrix_[MAX_OP][MIN_OP] = false;
    commutativity_matrix_[MAX_OP][MUL_OP] =  false;
    commutativity_matrix_[MAX_OP][PROJECT_OP] = false;
    commutativity_matrix_[MAX_OP][SELECT_OP] = false;
    commutativity_matrix_[MAX_OP][SUB_OP] = false;
    commutativity_matrix_[MAX_OP][SUM_OP] = false;
    commutativity_matrix_[MAX_OP][UNION_OP] = false;
    commutativity_matrix_[MIN_OP][COUNT_OP] = false;
    commutativity_matrix_[MIN_OP][DIFFERENCE_OP] = false;
    commutativity_matrix_[MIN_OP][DIV_OP] = false;
    commutativity_matrix_[MIN_OP][INTERSECTION_OP] = false;
    commutativity_matrix_[MIN_OP][JOIN_OP] = false;
    commutativity_matrix_[MIN_OP][MAX_OP] = false;
    commutativity_matrix_[MIN_OP][MIN_OP] = false;
    commutativity_matrix_[MIN_OP][MUL_OP] = false;
    commutativity_matrix_[MIN_OP][PROJECT_OP] = false;
    commutativity_matrix_[MIN_OP][SELECT_OP] = false;
    commutativity_matrix_[MIN_OP][SUB_OP] = false;
    commutativity_matrix_[MIN_OP][SUM_OP] = false;
    commutativity_matrix_[MIN_OP][UNION_OP] = false;
    commutativity_matrix_[MUL_OP][COUNT_OP] = false;
    commutativity_matrix_[MUL_OP][DIFFERENCE_OP] = true;
    commutativity_matrix_[MUL_OP][DIV_OP] = false;
    commutativity_matrix_[MUL_OP][INTERSECTION_OP] = true;
    commutativity_matrix_[MUL_OP][JOIN_OP] = true;
    commutativity_matrix_[MUL_OP][MAX_OP] = false;
    commutativity_matrix_[MUL_OP][MIN_OP] = false;
    commutativity_matrix_[MUL_OP][MUL_OP] = true;
    commutativity_matrix_[MUL_OP][PROJECT_OP] = false;
    commutativity_matrix_[MUL_OP][SELECT_OP] = true;
    commutativity_matrix_[MUL_OP][SUB_OP] = false;
    commutativity_matrix_[MUL_OP][SUM_OP] = false;
    commutativity_matrix_[MUL_OP][UNION_OP] = true;
    commutativity_matrix_[PROJECT_OP][COUNT_OP] = false;
    commutativity_matrix_[PROJECT_OP][DIFFERENCE_OP] = false;
    commutativity_matrix_[PROJECT_OP][DIV_OP] = false;
    commutativity_matrix_[PROJECT_OP][INTERSECTION_OP] = false;
    commutativity_matrix_[PROJECT_OP][JOIN_OP] = false;
    commutativity_matrix_[PROJECT_OP][MAX_OP] = false;
    commutativity_matrix_[PROJECT_OP][MIN_OP] = false;
    commutativity_matrix_[PROJECT_OP][MUL_OP] = false;
    commutativity_matrix_[PROJECT_OP][PROJECT_OP] = false;
    commutativity_matrix_[PROJECT_OP][SELECT_OP] = false;
    commutativity_matrix_[PROJECT_OP][SUB_OP] = false;
    commutativity_matrix_[PROJECT_OP][SUM_OP] = false;
    commutativity_matrix_[PROJECT_OP][UNION_OP] = false;
    commutativity_matrix_[SELECT_OP][COUNT_OP] = true;
    commutativity_matrix_[SELECT_OP][DIFFERENCE_OP] = true;
    commutativity_matrix_[SELECT_OP][DIV_OP] = true;
    commutativity_matrix_[SELECT_OP][INTERSECTION_OP] = true;
    commutativity_matrix_[SELECT_OP][JOIN_OP] = true;
    commutativity_matrix_[SELECT_OP][MAX_OP] = true;
    commutativity_matrix_[SELECT_OP][MIN_OP] = true;
    commutativity_matrix_[SELECT_OP][MUL_OP] = true;
    commutativity_matrix_[SELECT_OP][PROJECT_OP] = false;
    commutativity_matrix_[SELECT_OP][SELECT_OP] = true;
    commutativity_matrix_[SELECT_OP][SUB_OP] = true;
    commutativity_matrix_[SELECT_OP][SUM_OP] = true;
    commutativity_matrix_[SELECT_OP][UNION_OP] = true;
    commutativity_matrix_[SUB_OP][COUNT_OP] = false;
    commutativity_matrix_[SUB_OP][DIFFERENCE_OP] = false;
    commutativity_matrix_[SUB_OP][DIV_OP] = false;
    commutativity_matrix_[SUB_OP][INTERSECTION_OP] = true;
    commutativity_matrix_[SUB_OP][JOIN_OP] = false;
    commutativity_matrix_[SUB_OP][MAX_OP] = false;
    commutativity_matrix_[SUB_OP][MIN_OP] = false;
    commutativity_matrix_[SUB_OP][MUL_OP] = false;
    commutativity_matrix_[SUB_OP][PROJECT_OP] = true;
    commutativity_matrix_[SUB_OP][SELECT_OP] = false;
    commutativity_matrix_[SUB_OP][SUB_OP] = true;
    commutativity_matrix_[SUB_OP][SUM_OP] = true;
    commutativity_matrix_[SUB_OP][UNION_OP] = true;
    commutativity_matrix_[SUM_OP][COUNT_OP] = false;
    commutativity_matrix_[SUM_OP][DIFFERENCE_OP] = false;
    commutativity_matrix_[SUM_OP][DIV_OP] = false;
    commutativity_matrix_[SUM_OP][INTERSECTION_OP] = true;
    commutativity_matrix_[SUM_OP][JOIN_OP] = false;
    commutativity_matrix_[SUM_OP][MAX_OP] = false;
    commutativity_matrix_[SUM_OP][MIN_OP] =  false;
    commutativity_matrix_[SUM_OP][MUL_OP] = false;
    commutativity_matrix_[SUM_OP][PROJECT_OP] = true;
    commutativity_matrix_[SUM_OP][SELECT_OP] = true;
    commutativity_matrix_[SUM_OP][SUB_OP] = true;
    commutativity_matrix_[SUM_OP][SUM_OP] = true;
    commutativity_matrix_[SUM_OP][UNION_OP] = true;
    commutativity_matrix_[MUL_OP][COUNT_OP] = false;
    commutativity_matrix_[UNION_OP][DIFFERENCE_OP] = false;
    commutativity_matrix_[UNION_OP][DIV_OP] = true;
    commutativity_matrix_[UNION_OP][INTERSECTION_OP] = true;
    commutativity_matrix_[UNION_OP][JOIN_OP] = false;
    commutativity_matrix_[UNION_OP][MAX_OP] = false;
    commutativity_matrix_[UNION_OP][MIN_OP] = false;
    commutativity_matrix_[UNION_OP][MUL_OP] =  false;
    commutativity_matrix_[UNION_OP][PROJECT_OP] = true;
    commutativity_matrix_[UNION_OP][SELECT_OP] = false;
    commutativity_matrix_[UNION_OP][SUB_OP] = false;
    commutativity_matrix_[UNION_OP][SUM_OP] = false;
    commutativity_matrix_[UNION_OP][UNION_OP] = true;
  }

  void QueryOptimiser::initialiseRules(bool active) {
    // LOG(INFO) << "initialiseRules" ;
    // rules_ = vector<rule_t >();
    // if (active ) {

    //   rules_.push_back(&QueryOptimiser::filterMerge);
      // rules_.push_back((rule_t)QueryOptimiser::filterPushUp);
      // rules_.push_back((rule_t)QueryOptimiser::intersectionPushUp);
      // rules_.push_back((rule_t)QueryOptimiser::differencePushUp);
      // rules_.push_back((rule_t)QueryOptimiser::sortPushDown);
      // rules_.push_back((rule_t)QueryOptimiser::optimiseJoin);
      // rules_.push_back((rule_t)QueryOptimiser::unionPushDown);
    // }
  }

} // namespace musketeer
