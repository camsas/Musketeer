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

#include "translation/translator_graphchi.h"

#include <boost/lexical_cast.hpp>
#include <ctemplate/template.h>
#include <sys/time.h>

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <string>
#include <utility>

#include "base/common.h"
#include "ir/column.h"

namespace musketeer {
namespace translator {

  TranslatorGraphChi::TranslatorGraphChi(const op_nodes& dag,
                                         const string& class_name):
    TranslatorGraphInterface(dag, class_name) {
  }

  string TranslatorGraphChi::GetBinaryPath(OperatorInterface* op) {
    return op->get_code_dir() + class_name + "_code/" + class_name + "_bin";
  }

  string TranslatorGraphChi::GetSourcePath(OperatorInterface* op) {
    return op->get_code_dir() + class_name + "_code/" + class_name + ".cc";
  }

  string TranslatorGraphChi::HandleVerticesEdgesJoin(
      unordered_map<string, vector<string> >* rel_var_names,
      TemplateDictionary* dict, shared_ptr<OperatorNode> join_node,
      bool has_count) {
    JoinOperator* join_op =
      dynamic_cast<JoinOperator*>(join_node->get_operator());
    vector<string> input_paths = join_op->get_input_paths();
    if (has_count) {
      // Set the edges path to the first JOIN relation.
      OperatorInterface* join_cnt =
        join_node->get_parents()[0]->get_parents()[0]->get_operator();
      dict->SetValue("EDGES_PATH", join_cnt->get_input_paths()[0]);
    } else {
      dict->SetValue("EDGES_PATH", input_paths[0]);
    }
    dict->SetValue("VERTICES_PATH", input_paths[1]);
    vector<Relation*> join_relations = join_op->get_relations();
    vector<Column*> edges_columns = join_relations[0]->get_columns();
    vector<string> edges_names;
    edges_names.push_back("v_src");
    edges_names.push_back("v_dst");
    vector<Column*> vertices_columns = join_relations[1]->get_columns();
    vector<string> vertices_names;
    vertices_names.push_back("ver_id");
    vertices_names.push_back("ver_val");
    string pre_group_vertex = "";
    // Check if we're handling cost as well.
    if (edges_columns.size() > 2) {
      edges_names.push_back("n_out_edges");
      dict->SetValue("EDGE_DATA_TYPE",
                    TranslateSuperType(edges_columns[2]->get_type(),
                                       vertices_columns[1]->get_type()));
    } else {
      dict->SetValue("EDGE_DATA_TYPE",
                    vertices_columns[1]->translateTypeC());
    }
    dict->SetValue("VERTEX_DATA_TYPE",
                  vertices_columns[1]->translateTypeC());
    rel_var_names->insert(pair<string, vector<string> >(
                              join_relations[0]->get_name(), edges_names));
    rel_var_names->insert(pair<string, vector<string> >(
                              join_relations[1]->get_name(), vertices_names));
    Relation* join_output_rel = join_op->get_output_relation();
    vector<string> join_col_names(edges_names.begin(), edges_names.end());
    // Ver_id is the one on which the join is conducted.
    join_col_names.push_back("in_edge_val");
    rel_var_names->insert(pair<string, vector<string> >(
                              join_output_rel->get_name(),
                              join_col_names));
    return pre_group_vertex;
  }

  shared_ptr<OperatorNode> TranslatorGraphChi::IgnoreIfCountEdgesPresent(
      const op_nodes& dag) {
    if (dag[0]->get_operator()->get_type() == COUNT_OP) {
      op_nodes children = dag[0]->get_children();
      CountOperator* count_op =
        dynamic_cast<CountOperator*>(dag[0]->get_operator());
      int32_t cnt_index = count_op->get_column()->get_index();
      vector<Column*> group_bys = count_op->get_group_bys();
      // If we count on source and group by on destination then the count may be
      // pushed to the while loop.
      if (cnt_index == 1 && group_bys.size() == 1 &&
          group_bys[0]->get_index() == 0 && children.size() == 1 &&
          children[0]->get_operator()->get_type() == JOIN_OP) {
        JoinOperator* join_op =
          dynamic_cast<JoinOperator*>(children[0]->get_operator());
        if (join_op->get_col_left()->get_index() == 0 &&
            join_op->get_col_right()->get_index() == 0) {
          return children[0]->get_children()[0];
        }
      }
    }
    return dag[0];
  }

  string TranslatorGraphChi::GenerateCode() {
    TemplateDictionary dict("graphchi");
    // Stores var names for each column of each relation.
    unordered_map<string, vector<string> > rel_var_names;
    shared_ptr<OperatorNode> while_node = IgnoreIfCountEdgesPresent(dag);
    OperatorInterface* op = while_node->get_operator();
    string binary_file = GetBinaryPath(op);
    string num_iters = GetNumIters(dynamic_cast<WhileOperator*>(op));
    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("HDFS_MASTER", "hdfs://" + FLAGS_hdfs_master);
    dict.SetValue("HDFS_PORT", FLAGS_hdfs_port);
    dict.SetValue("GRAPHCHI_DIR", FLAGS_graphchi_dir);
    dict.SetValue("N_ITERS", num_iters);
    op_nodes children = while_node->get_loop_children();
    if (children.size() != 2) {
      LOG(ERROR) << "WHILE has more than 2 children nodes";
      return NULL;
    }
    // Assumes that the first operator is JOIN and the second operator is
    // SUM (i.e. the one incrementing the iterator).
    shared_ptr<OperatorNode> join_node = children[0];
    string pre_group_vertex =
      HandleVerticesEdgesJoin(&rel_var_names, &dict, join_node,
                              dag[0] != while_node);

    children = join_node->get_children();
    for (; ; children = children[0]->get_children()) {
      if (children.size() != 1) {
        LOG(ERROR) << "Unsupported merge!";
        return NULL;
      }
      OperatorInterface* child_op = children[0]->get_operator();
      if (child_op->hasGroupby()) {
        break;
      }
      pre_group_vertex += GenOperatorCode(&rel_var_names, child_op) + "\n";
    }
    dict.SetValue("PRE_GROUP_VERTEX_VAL", pre_group_vertex);
    // Generating code for the operator with group by.
    string group_vertex_val =
      UpdateGroupVertexValue("ver_val", &rel_var_names,
                             children[0]->get_operator());
    dict.SetValue("GROUP_VERTEX_VAL", group_vertex_val);
    string post_group_vertex = "";
    // Check if the parent is UNION
    if (children[0]->get_parents()[0]->get_operator()->get_type() == UNION_OP) {
      if (children[0]->get_operator()->get_type() == COUNT_OP) {
        dict.SetValue("INIT_VER_VAL", "1");
      } else {
        dict.SetValue("INIT_VER_VAL", "v.get_data()");
      }
    } else {
      // TODO(ionel): FIX! This doesn't work if ver_val is of type string.
      dict.SetValue("INIT_VER_VAL", "0");
    }
    children = children[0]->get_children();

    for (; children.size() > 0; children = children[0]->get_children()) {
      if (children.size() > 1) {
        LOG(ERROR) << "Unsupported merge!";
        return NULL;
      }
      OperatorInterface* child_op = children[0]->get_operator();
      post_group_vertex += GenOperatorCode(&rel_var_names, child_op) + "\n";
    }
    dict.SetValue("POST_GROUP_VERTEX_VAL", post_group_vertex);
    dict.SetValue("TMP_ROOT", FLAGS_tmp_data_dir);
    dict.SetValue("HDFS_ADDRESS", FLAGS_hdfs_master);
    GenMakeFile(op, dict);
    string code;
    ExpandTemplate(FLAGS_graphchi_templates_dir + "JobTemplate.cc",
                   ctemplate::DO_NOT_STRIP, &dict, &code);
    GraphChiJobCode job_code(op, code);
    return GenAndCompile(op, job_code.get_code());
  }

  string TranslatorGraphChi::TranslateSuperType(uint16_t e_cost,
                                                uint16_t ver_val) {
    if (e_cost == STRING || ver_val == STRING) {
      return "string";
    }
    if (e_cost == DOUBLE || ver_val == DOUBLE) {
      return "double";
    }
    return "int";
  }

  string TranslatorGraphChi::GenOpCode(
      unordered_map<string, vector<string> >* rel_var_names, AggOperator* op) {
    // TODO(ionel): Implement.
    return "";
  }

  string TranslatorGraphChi::GenOpCode(
      unordered_map<string, vector<string> >* rel_var_names,
      CountOperator* op) {
    // TODO(ionel): Implement.
    return "";
  }

  string TranslatorGraphChi::GenOpCode(
      unordered_map<string, vector<string> >* rel_var_names,
      CrossJoinOperator* op) {
    // TODO(ionel): Implement.
    return "";
  }

  string TranslatorGraphChi::GenOpCode(
      unordered_map<string, vector<string> >* rel_var_names,
      DifferenceOperator* op) {
    // TODO(ionel): Implement.
    return "";
  }

  string TranslatorGraphChi::GenOpCode(
      unordered_map<string, vector<string> >* rel_var_names,
      IntersectionOperator* op) {
    // TODO(ionel): Implement.
    return "";
  }

  string TranslatorGraphChi::GenOpCode(
      unordered_map<string, vector<string> >* rel_var_names, JoinOperator* op) {
    // TODO(ionel): Implement.
    return "";
  }

  string TranslatorGraphChi::GenOpCode(
      unordered_map<string, vector<string> >* rel_var_names, MaxOperator* op) {
    // TODO(ionel): Implement.
    return "";
  }

  string TranslatorGraphChi::GenOpCode(
      unordered_map<string, vector<string> >* rel_var_names, MinOperator* op) {
    // TODO(ionel): Implement.
    return "";
  }

  string TranslatorGraphChi::GenOpCode(
      unordered_map<string, vector<string> >* rel_var_names, SortOperator* op) {
    // TODO(ionel): Implement.
    return "";
  }

  string TranslatorGraphChi::GenOpCode(
      unordered_map<string, vector<string> >* rel_var_names,
      UnionOperator* op) {
    vector<Relation*> rels = op->get_relations();
    vector<string> input_left_names = (*rel_var_names)[rels[0]->get_name()];
    vector<string> input_right_names = (*rel_var_names)[rels[1]->get_name()];
    string output_rel_name = op->get_output_relation()->get_name();
    vector<string> output_col_names(input_right_names.begin(),
                                    input_right_names.end());
    rel_var_names->insert(pair<string, vector<string> >(output_rel_name, output_col_names));
    // TODO(ionel): Implement.
    return "";
  }

  string TranslatorGraphChi::GenOpCode(
      unordered_map<string, vector<string> >* rel_var_names, WhileOperator* op) {
    // TODO(ionel): Implement.
    return "";
  }

  string TranslatorGraphChi::GenOpCode(
      unordered_map<string, vector<string> >* rel_var_names,
      ProjectOperator* op) {
    string input_rel_name = op->get_relations()[0]->get_name();
    string output_rel_name = op->get_output_relation()->get_name();
    vector<string> input_col_names = (*rel_var_names)[input_rel_name];
    vector<Column*> input_cols = op->get_columns();
    vector<string> output_col_names;
    for (vector<Column*>::iterator it = input_cols.begin();
         it != input_cols.end(); ++it) {
      output_col_names.push_back(input_col_names[(*it)->get_index()]);
    }
    rel_var_names->insert(pair<string, vector<string> >(output_rel_name, output_col_names));
    // No code must be generated for a project.
    return "";
  }

  string TranslatorGraphChi::GenOpCode(
      unordered_map<string, vector<string> >* rel_var_names,
      SelectOperator* op) {
    string input_rel_name = op->get_relations()[0]->get_name();
    string output_rel_name = op->get_output_relation()->get_name();
    vector<string> input_col_names = (*rel_var_names)[input_rel_name];
    vector<Column*> input_cols = op->get_columns();
    vector<string> output_col_names;
    for (vector<Column*>::iterator it = input_cols.begin();
         it != input_cols.end(); ++it) {
      output_col_names.push_back(input_col_names[(*it)->get_index()]);
    }
    rel_var_names->insert(pair<string, vector<string> >(output_rel_name, output_col_names));
    // No code must be generated for a select.
    return "";
  }

  string TranslatorGraphChi::GenOpCode(
      unordered_map<string, vector<string> >* rel_var_names,
      DistinctOperator* op) {
    // TODO(ionel): Implement.
    return "";
  }

  string TranslatorGraphChi::GenOpCode(
      unordered_map<string, vector<string> >* rel_var_names, DivOperator* op) {
    vector<Relation*> rels = op->get_relations();
    vector<Value*> values = op->get_values();
    Column* left_column = dynamic_cast<Column*>(values[0]);
    Column* right_column = dynamic_cast<Column*>(values[1]);
    string left_side;
    bool swap = false;
    if (left_column != NULL) {
      // Column.
      string left_col_name = left_column->get_relation();
      left_side = (*rel_var_names)[left_col_name][left_column->get_index()];
    } else {
      // Value.
      left_side = values[0]->get_value();
      swap = true;
    }
    string right_side;
    if (right_column != NULL) {
      // Column.
      string right_col_name = right_column->get_relation();
      right_side = (*rel_var_names)[right_col_name][right_column->get_index()];
    } else {
      // Value.
      right_side = values[1]->get_value();
    }
    string output_rel_name = op->get_output_relation()->get_name();
    vector<string> input_rel_cols = (*rel_var_names)[rels[0]->get_name()];
    vector<string> output_col_names(input_rel_cols.begin(),
                                    input_rel_cols.end());
    rel_var_names->insert(pair<string, vector<string> >(output_rel_name, output_col_names));
    if (swap) {
      return right_side + " /= " + left_side + ";";
    } else {
      return left_side + " /= " + right_side + ";";
    }
  }

  string TranslatorGraphChi::GenOpCode(
      unordered_map<string, vector<string> >* rel_var_names, MulOperator* op) {
    vector<Relation*> rels = op->get_relations();
    vector<Value*> values = op->get_values();
    Column* left_column = dynamic_cast<Column*>(values[0]);
    Column* right_column = dynamic_cast<Column*>(values[1]);
    string left_side;
    bool swap = false;
    if (left_column != NULL) {
      // Column.
      string left_col_name = left_column->get_relation();
      left_side = (*rel_var_names)[left_col_name][left_column->get_index()];
    } else {
      // Value.
      left_side = values[0]->get_value();
      swap = true;
    }
    string right_side;
    if (right_column != NULL) {
      // Column.
      string right_col_name = right_column->get_relation();
      vector<string> rel_names = (*rel_var_names)[right_col_name];
      right_side = (*rel_var_names)[right_col_name][right_column->get_index()];
    } else {
      // Value.
      right_side = values[1]->get_value();
    }
    string output_rel_name = op->get_output_relation()->get_name();
    vector<string> input_rel_cols = (*rel_var_names)[rels[0]->get_name()];
    vector<string> output_col_names(input_rel_cols.begin(),
                                    input_rel_cols.end());
    rel_var_names->insert(pair<string, vector<string> >(output_rel_name, output_col_names));
    if (swap) {
      return right_side + " *= " + left_side + ";";
    } else {
      return left_side + " *= " + right_side + ";";
    }
  }

  string TranslatorGraphChi::GenOpCode(
      unordered_map<string, vector<string> >* rel_var_names, SubOperator* op) {
    vector<Relation*> rels = op->get_relations();
    vector<Value*> values = op->get_values();
    Column* left_column = dynamic_cast<Column*>(values[0]);
    Column* right_column = dynamic_cast<Column*>(values[1]);
    string left_side;
    bool swap = false;
    if (left_column != NULL) {
      // Column.
      string left_col_name = left_column->get_relation();
      left_side = (*rel_var_names)[left_col_name][left_column->get_index()];
    } else {
      // Value.
      left_side = values[0]->get_value();
      swap = true;
    }
    string right_side;
    if (right_column != NULL) {
      // Column.
      string right_col_name = right_column->get_relation();
      right_side = (*rel_var_names)[right_col_name][right_column->get_index()];
    } else {
      // Value.
      right_side = values[1]->get_value();
    }
    string output_rel_name = op->get_output_relation()->get_name();
    vector<string> input_rel_cols = (*rel_var_names)[rels[0]->get_name()];
    vector<string> output_col_names(input_rel_cols.begin(),
                                    input_rel_cols.end());
    rel_var_names->insert(pair<string, vector<string> >(output_rel_name, output_col_names));
    if (swap) {
      return right_side + " -= " + left_side + ";";
    } else {
      return left_side + " -= " + right_side + ";";
    }
  }

  string TranslatorGraphChi::GenOpCode(
      unordered_map<string, vector<string> >* rel_var_names, SumOperator* op) {
    vector<Relation*> rels = op->get_relations();
    vector<Value*> values = op->get_values();
    Column* left_column = dynamic_cast<Column*>(values[0]);
    Column* right_column = dynamic_cast<Column*>(values[1]);
    string left_side;
    bool swap = false;
    if (left_column != NULL) {
      // Column.
      string left_col_name = left_column->get_relation();
      left_side = (*rel_var_names)[left_col_name][left_column->get_index()];
    } else {
      // Value.
      left_side = values[0]->get_value();
      swap = true;
    }
    string right_side;
    if (right_column != NULL) {
      // Column.
      string right_col_name = right_column->get_relation();
      right_side = (*rel_var_names)[right_col_name][right_column->get_index()];
    } else {
      // Value.
      right_side = values[1]->get_value();
    }
    string output_rel_name = op->get_output_relation()->get_name();
    vector<string> input_rel_cols = (*rel_var_names)[rels[0]->get_name()];
    vector<string> output_col_names(input_rel_cols.begin(),
                                    input_rel_cols.end());
    rel_var_names->insert(pair<string, vector<string> >(output_rel_name, output_col_names));
    if (swap) {
      return right_side + " += " + left_side + ";";
    } else {
      return left_side + " += " + right_side + ";";
    }
  }

  string TranslatorGraphChi::UpdateGroupVertexVal(
      const string& var_to_update,
      unordered_map<string, vector<string> >* rel_var_names, AggOperator* op) {
    vector<Column*> group_bys = op->get_group_bys();
    vector<string> input_rel_cols =
      (*rel_var_names)[op->get_relations()[0]->get_name()];
    vector<string> output_names;
    for (vector<Column*>::iterator it = group_bys.begin();
         it != group_bys.end(); ++it) {
      output_names.push_back(input_rel_cols[(*it)->get_index()]);
    }
    // TODO(ionel): Add support for multiple columns agg.
    // Add the aggregated column.
    string agg_col_name = input_rel_cols[op->get_columns()[0]->get_index()];
    // Pushing the var name used to agg.
    output_names.push_back(var_to_update);
    string output_rel_name = op->get_output_relation()->get_name();
    rel_var_names->insert(pair<string, vector<string> >(output_rel_name, output_names));
    return var_to_update + " " + op->get_operator() + "= " + agg_col_name + ";";
  }

  string TranslatorGraphChi::UpdateGroupVertexVal(
      const string& var_to_update,
      unordered_map<string, vector<string> >* rel_var_names,
      CountOperator* op) {
    vector<Column*> group_bys = op->get_group_bys();
    vector<string> input_rel_cols =
      (*rel_var_names)[op->get_relations()[0]->get_name()];
    vector<string> output_names;
    for (vector<Column*>::iterator it = group_bys.begin();
         it != group_bys.end(); ++it) {
      output_names.push_back(input_rel_cols[(*it)->get_index()]);
    }
    // Add the aggregated column.
    //    output_names.push_back(input_rel_cols[op->get_column()->get_index()]);
    output_names.push_back(var_to_update);
    string output_rel_name = op->get_output_relation()->get_name();
    rel_var_names->insert(pair<string, vector<string> >(output_rel_name, output_names));
    return var_to_update + " += 1;";
  }

  string TranslatorGraphChi::UpdateGroupVertexVal(
      const string& var_to_update,
      unordered_map<string, vector<string> >* rel_var_names, MaxOperator* op) {
    vector<Column*> group_bys = op->get_group_bys();
    vector<string> input_rel_cols =
      (*rel_var_names)[op->get_relations()[0]->get_name()];
    vector<string> output_names;
    for (vector<Column*>::iterator it = group_bys.begin();
         it != group_bys.end(); ++it) {
      output_names.push_back(input_rel_cols[(*it)->get_index()]);
    }
    // Add the aggregated column.
    string max_col_name = input_rel_cols[op->get_column()->get_index()];
    //    output_names.push_back(max_col_name);
    output_names.push_back(var_to_update);
    string output_rel_name = op->get_output_relation()->get_name();
    rel_var_names->insert(pair<string, vector<string> >(output_rel_name, output_names));
    return "if (" + max_col_name + " > " + var_to_update + ") { " +
      var_to_update + " = " + max_col_name + "; }";
  }

  string TranslatorGraphChi::UpdateGroupVertexVal(
      const string& var_to_update,
      unordered_map<string, vector<string> >* rel_var_names, MinOperator* op) {
    vector<Column*> group_bys = op->get_group_bys();
    vector<string> input_rel_cols =
      (*rel_var_names)[op->get_relations()[0]->get_name()];
    vector<string> output_names;
    for (vector<Column*>::iterator it = group_bys.begin();
         it != group_bys.end(); ++it) {
      output_names.push_back(input_rel_cols[(*it)->get_index()]);
    }
    // Add the aggregated column.
    string min_col_name = input_rel_cols[op->get_column()->get_index()];
    //    output_names.push_back(min_col_name);
    output_names.push_back(var_to_update);
    string output_rel_name = op->get_output_relation()->get_name();
    rel_var_names->insert(pair<string, vector<string> >(output_rel_name, output_names));
    return "if (" + min_col_name + " < " + var_to_update + ") { " +
      var_to_update + " = " + min_col_name + "; }";
  }

  void TranslatorGraphChi::GenMakeFile(OperatorInterface* op,
                                       const TemplateDictionary& dict) {
    string path = op->get_code_dir() + class_name + "_code/";
    string create_dir = "mkdir -p " + path;
    std::system(create_dir.c_str());
    // Fill in the make template.
    ofstream make_file_stream;
    string make_file = path + "Makefile";
    string make_code;
    ExpandTemplate(FLAGS_graphchi_templates_dir + "Makefile", ctemplate::DO_NOT_STRIP,
                   &dict, &make_code);
    make_file_stream.open(make_file.c_str());
    make_file_stream << make_code;
    make_file_stream.close();
    ofstream data_transformer_stream;
    string data_transformer_file = path + "DataTransformer.cc";
    string data_transformer_code;
    ExpandTemplate(FLAGS_graphchi_templates_dir + "DataTransformer.cc",
                   ctemplate::DO_NOT_STRIP, &dict, &data_transformer_code);
    data_transformer_stream.open(data_transformer_file.c_str());
    data_transformer_stream << data_transformer_code;
    data_transformer_stream.close();
    // Copy env file.
    string copy_env = "cp " + FLAGS_graphchi_templates_dir + "env.sh " + path;
    std::system(copy_env.c_str());
  }

  string TranslatorGraphChi::GenAndCompile(OperatorInterface* op,
                                           const string& op_code) {
    string path = op->get_code_dir() + class_name + "_code/";
    ofstream job_file;
    string source_file = GetSourcePath(op);
    string binary_file = GetBinaryPath(op);
    job_file.open(source_file.c_str());
    job_file << op_code;
    job_file.close();
    LOG(INFO) << "graphChi build started for: " << class_name;
    timeval start_compile;
    gettimeofday(&start_compile, NULL);
    // Compile the job.
    string compile_cmd = "make -C " + path;
    std::system(compile_cmd.c_str());
    LOG(INFO) << "graphChi build ended for: " << class_name;
    timeval end_compile;
    gettimeofday(&end_compile, NULL);
    uint32_t compile_time = end_compile.tv_sec - start_compile.tv_sec;
    cout << "COMPILE TIME: " << compile_time << endl;
    return binary_file;
  }

} // namespace translator
} // namespace musketeer
