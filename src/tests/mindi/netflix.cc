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

#include "tests/mindi/netflix.h"

#include <vector>

#include "base/common.h"
#include "base/flags.h"
#include "ir/column.h"
#include "ir/condition_tree.h"
#include "ir/input_operator.h"

namespace musketeer {
namespace tests {
namespace mindi {

  using ir::CondOperator;
  using ir::InputOperator;

  shared_ptr<OperatorNode> Netflix::Run() {
    Mindi* mindi = new Mindi();
    vector<Column*> rating_cols;
    rating_cols.push_back(new Column("rat_small", 0, INTEGER_TYPE));
    rating_cols.push_back(new Column("rat_small", 1, INTEGER_TYPE));
    rating_cols.push_back(new Column("rat_small", 2, INTEGER_TYPE));
    rating_cols.push_back(new Column("rat_small", 3, STRING_TYPE));
    vector<Column*> movie_cols;
    movie_cols.push_back(new Column("mov_small", 0, INTEGER_TYPE));
    movie_cols.push_back(new Column("mov_small", 1, INTEGER_TYPE));
    movie_cols.push_back(new Column("mov_small", 2, STRING_TYPE));
    Relation* rating_rel = new Relation("rat_small", rating_cols);
    Relation* movie_rel = new Relation("mov_small", movie_cols);
    vector<Relation*> rating_rels;
    rating_rels.push_back(rating_rel);
    vector<Relation*> movie_rels;
    movie_rels.push_back(movie_rel);
    OperatorInterface* rating_op =
      new InputOperator(FLAGS_hdfs_input_dir, rating_rels, rating_rel);
    OperatorInterface* movie_op =
      new InputOperator(FLAGS_hdfs_input_dir, movie_rels, movie_rel);

    vector<shared_ptr<OperatorNode> > parents;

    ConditionTree* movies_sel_cond_tree =
      new ConditionTree(new CondOperator("<"),
                        new ConditionTree(movie_cols[1]->clone()),
                        new ConditionTree(new Value("1970", INTEGER_TYPE)));
    shared_ptr<OperatorNode> movies_sel =
      mindi->Where(shared_ptr<OperatorNode>(new OperatorNode(movie_op, parents)),
                   movies_sel_cond_tree,
                   "movies_sel");

    shared_ptr<OperatorNode> rating_op_node =
      shared_ptr<OperatorNode>(new OperatorNode(rating_op, parents));
    shared_ptr<OperatorNode> rating_sel =
      mindi->Join(rating_op_node, "rating_rel", movies_sel,
                  rating_op->get_output_relation()->get_columns(),
                  movies_sel->get_operator()->get_output_relation()->get_columns());

    vector<Column*> rating_sel_cols =
      rating_sel->get_operator()->get_output_relation()->get_columns();
    vector<Column*> preferences_sel_cols;
    preferences_sel_cols.push_back(rating_sel_cols[0]->clone());
    preferences_sel_cols.push_back(rating_sel_cols[1]->clone());
    preferences_sel_cols.push_back(rating_sel_cols[2]->clone());
    shared_ptr<OperatorNode> preferences =
      mindi->Select(rating_sel, preferences_sel_cols, "preferences");

    vector<Column*> transpose_sel_cols;
    transpose_sel_cols.push_back(rating_sel_cols[1]->clone());
    transpose_sel_cols.push_back(rating_sel_cols[0]->clone());
    transpose_sel_cols.push_back(rating_sel_cols[2]->clone());
    shared_ptr<OperatorNode> transpose =
      mindi->Select(rating_sel, transpose_sel_cols, "transpose");

    shared_ptr<OperatorNode> mat =
      mindi->Join(preferences, "mat", transpose,
                  preferences->get_operator()->get_output_relation()->get_columns(),
                  transpose->get_operator()->get_output_relation()->get_columns());

    vector<Column*> mat_cols =
      mat->get_operator()->get_output_relation()->get_columns();

    ConditionTree* matmult_arith_tree =
      new ConditionTree(new CondOperator("*"),
                        new ConditionTree(mat_cols[2]->clone()),
                        new ConditionTree(mat_cols[4]->clone()));
    vector<Column*> mat_sel_cols;
    mat_sel_cols.push_back(mat_cols[0]->clone());
    mat_sel_cols.push_back(mat_cols[2]->clone());
    mat_sel_cols.push_back(mat_cols[3]->clone());
    shared_ptr<OperatorNode> matmult_prj =
      mindi->Select(mat, mat_sel_cols, matmult_arith_tree, "matmult_prj");

    vector<Column*> matmult_prj_cols =
      matmult_prj->get_operator()->get_output_relation()->get_columns();
    vector<Column*> int_result_group_by;
    int_result_group_by.push_back(matmult_prj_cols[0]->clone());
    int_result_group_by.push_back(matmult_prj_cols[2]->clone());
    shared_ptr<OperatorNode> int_result =
      mindi->GroupBy(matmult_prj, int_result_group_by, PLUS_GROUP,
                     matmult_prj_cols[1], "int_result");

    shared_ptr<OperatorNode> matfinal =
      mindi->Join(int_result, "matfinal", preferences,
                  int_result->get_operator()->get_output_relation()->get_columns(),
                  preferences->get_operator()->get_output_relation()->get_columns());

    vector<Column*> matfinal_cols =
      matfinal->get_operator()->get_output_relation()->get_columns();
    ConditionTree* matmultfinal_arith_tree =
      new ConditionTree(new CondOperator("*"),
                        new ConditionTree(matfinal_cols[2]->clone()),
                        new ConditionTree(matfinal_cols[4]->clone()));
    vector<Column*> matfinal_sel_cols;
    matfinal_sel_cols.push_back(matfinal_cols[0]->clone());
    matfinal_sel_cols.push_back(matfinal_cols[2]->clone());
    matfinal_sel_cols.push_back(matfinal_cols[3]->clone());
    shared_ptr<OperatorNode> matmultfinal_prj =
      mindi->Select(matfinal, matfinal_sel_cols, matmultfinal_arith_tree,
                    "matmultfinal_prj");

    vector<Column*> matmultfinal_prj_cols =
      matmultfinal_prj->get_operator()->get_output_relation()->get_columns();
    vector<Column*> predicted_group_by;
    predicted_group_by.push_back(matmultfinal_prj_cols[0]->clone());
    predicted_group_by.push_back(matmultfinal_prj_cols[2]->clone());
    shared_ptr<OperatorNode> predicted =
      mindi->GroupBy(matmultfinal_prj, predicted_group_by, PLUS_GROUP,
                     matmultfinal_prj_cols[1]->clone(), "predicted");

    // vector<Column*> predicted_cols =
    //   predicted->get_operator()->get_output_relation()->get_columns();
    // vector<Column*> predicted_val_cols;
    // predicted_val_cols.push_back(predicted_cols[2]->clone());
    // shared_ptr<OperatorNode> prediction =
    //   mindi->Max(predicted, predicted_cols[1]->clone(), predicted_val_cols, "prediction");

    return movies_sel;
  }

} // namespace mindi
} // namespace tests
} // namespace musketeer
