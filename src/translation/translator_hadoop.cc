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

#include "translation/translator_hadoop.h"

#include <boost/lexical_cast.hpp>
#include <ctemplate/template.h>
#include <sys/time.h>

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <queue>
#include <set>
#include <string>

#include "base/common.h"
#include "ir/column.h"
#include "ir/condition_tree.h"

namespace musketeer {
namespace translator {

  using ctemplate::mutable_default_template_cache;

  TranslatorHadoop::TranslatorHadoop(const op_nodes& dag,
                                     const string& class_name):
    TranslatorInterface(dag, class_name) {
    single_input = true;
  }

  string TranslatorHadoop::GetBinaryPath(OperatorInterface* op) {
    return op->get_code_dir() + class_name + "_code/" + class_name + ".jar";
  }

  string TranslatorHadoop::GetSourcePath(OperatorInterface* op) {
    return op->get_code_dir() + class_name + "_code/" + class_name + ".java";
  }

  // TODO(ionel): There is a bug here. If an operator that uses same input and
  // output relation is present together with another operator which takes the
  // relation as input there will be a name clash in the generated code.
  pair<string, string> TranslatorHadoop::GetInputPathsAndRelationsCode(
      const op_nodes& dag) {
    queue<shared_ptr<OperatorNode> > to_visit;
    set<shared_ptr<OperatorNode> > visited;
    set<string> known_rels;
    set<string> input_paths;
    set<string> input_rels;
    for (op_nodes::const_iterator it = dag.begin(); it != dag.end(); ++it) {
      to_visit.push(*it);
      visited.insert(*it);
    }
    while (!to_visit.empty()) {
      shared_ptr<OperatorNode> cur_node = to_visit.front();
      to_visit.pop();
      OperatorInterface* op = cur_node->get_operator();
      vector<Relation*> relations = op->get_relations();
      string output_relation = op->get_output_relation()->get_name();
      for (vector<Relation*>::iterator rel_it = relations.begin();
           rel_it != relations.end(); ++rel_it) {
        string rel_name = (*rel_it)->get_name();
        if (known_rels.insert(rel_name).second) {
          if (!output_relation.compare(rel_name)) {
            input_rels.insert(rel_name + "_input");
          } else {
            input_rels.insert(rel_name);
          }
          input_paths.insert(op->CreateInputPath(*rel_it));
        }
      }
      known_rels.insert(output_relation);
      if (!cur_node->IsLeaf()) {
        op_nodes children = cur_node->get_loop_children();
        op_nodes non_loop_children = cur_node->get_children();
        children.insert(children.end(), non_loop_children.begin(),
                        non_loop_children.end());
        for (op_nodes::iterator it = children.begin(); it != children.end();
             ++it) {
          if (visited.insert(*it).second) {
            to_visit.push(*it);
          }
        }
      }
    }
    string input_paths_code = "";
    // IMPORTANT! HACK! This has side effects on which version of join is used.
    if (input_paths.size() > 1) {
      single_input = false;
    }
    for (set<string>::iterator it = input_paths.begin();
         it != input_paths.end(); ++it) {
      LOG(INFO) << "Job input: " << *it;
      input_paths_code += "FileInputFormat.addInputPath(job, new Path(\"" +
        *it + "\"));\n";
    }
    string input_rels_code = "";
    for (set<string>::iterator it = input_rels.begin(); it != input_rels.end();
         ++it) {
      input_rels_code += "String[] " + *it +
        " = value.toString().trim().split(\" \");\n";
    }
    return make_pair(input_paths_code, input_rels_code);
  }

  bool TranslatorHadoop::HasReduce(OperatorInterface* op) {
    return op->get_type() == AGG_OP || op->get_type() == COUNT_OP ||
      op->get_type() == CROSS_JOIN_OP || op->get_type() == DIFFERENCE_OP ||
      op->get_type() == INTERSECTION_OP || op->get_type() == JOIN_OP ||
      op->get_type() == MAX_OP || op->get_type() == MIN_OP ||
      op->get_type() == SORT_OP;
  }

  void TranslatorHadoop::UpdateDAGCode(
       const string& code, HadoopJobCode* child_code, bool add_to_reduce,
       HadoopJobCode* dag_code) {
    if (add_to_reduce) {
      dag_code->set_reduce_code(code);
    } else {
      if (child_code->HasMapVariablesCode()) {
        dag_code->set_map_variables_code(child_code->get_map_variables_code());
      }
      if (child_code->HasSetupCode()) {
        dag_code->set_setup_code(child_code->get_setup_code());
      }
      if (child_code->HasCleanupCode()) {
        dag_code->set_cleanup_code(child_code->get_cleanup_code());
      }
      dag_code->set_map_code(code);
    }
  }

  void TranslatorHadoop::PopulateEndDAG(
      OperatorInterface* op, HadoopJobCode* dag_code,
      TemplateDictionary* dict, pair<string, string> input_rel_code) {
    dict->SetValue("OUTPUT_REL", op->get_output_relation()->get_name());
    mutable_default_template_cache()->ClearCache();
    if (!HasReduce(op)) {
      string output_code = "";
      ExpandTemplate(FLAGS_hadoop_templates_dir + "OutputCode.java",
                     ctemplate::DO_NOT_STRIP, dict, &output_code);
      dict->SetValue("OUTPUT_CODE", output_code);
    }
    StringToTemplateCache("dag", dag_code->get_map_code(), ctemplate::DO_NOT_STRIP);
    // No more operators to add.
    dict->SetValue("NEXT_OPERATOR", "");
    string dag_map_code = "";
    ExpandTemplate("dag", ctemplate::DO_NOT_STRIP, dict, &dag_map_code);
    dag_code->set_map_code(dag_map_code);
    string output_code = "";
    mutable_default_template_cache()->ClearCache();
    ExpandTemplate(FLAGS_hadoop_templates_dir + "OutputCode.java",
                   ctemplate::DO_NOT_STRIP, dict, &output_code);
    dict->SetValue("OUTPUT_CODE", output_code);
    mutable_default_template_cache()->ClearCache();
    StringToTemplateCache("dag", dag_code->get_reduce_code(), ctemplate::DO_NOT_STRIP);
    string dag_reduce_code = "";
    ExpandTemplate("dag", ctemplate::DO_NOT_STRIP, dict, &dag_reduce_code);
    dag_code->set_reduce_code(dag_reduce_code);
    dict->SetValue("CLASS_NAME", class_name);
    dict->SetValue("MAP_VARIABLES_CODE", dag_code->get_map_variables_code());
    dict->SetValue("SETUP_CODE", dag_code->get_setup_code());
    dict->SetValue("MAP_CODE", dag_map_code);
    dict->SetValue("CLEANUP_CODE", dag_code->get_cleanup_code());
    dict->SetValue("INPUT_PATHS", input_rel_code.first);
    dict->SetValue("INPUT_CODE", input_rel_code.second);
    dict->SetValue("OUTPUT_PATH", op->get_output_path());
    dict->SetValue("MAP_KEY_TYPE", dag_code->get_map_key_type());
    dict->SetValue("MAP_VALUE_TYPE", dag_code->get_map_value_type());
    dict->SetValue("REDUCE_KEY_TYPE", dag_code->get_reduce_key_type());
    dict->SetValue("REDUCE_VALUE_TYPE", dag_code->get_reduce_value_type());
    dict->SetValue("OUTPUT_REL", op->get_output_relation()->get_name());
  }

  // Map only operators: Div, Mul, Sub, Sum, Select, Project, Union
  // Map and reduce operators: Intersect, Difference. The map output key is the
  // entire row.
  // Map and reduce operators: Join. The map is used to select the joining
  // columns and to detect from which relation the row is coming.
  // Map and reduce operators: Agg, Count, Min, Max. The map is used to select
  // the group by columns or to do partial aggregation when no groupby clause is
  // present.
  string TranslatorHadoop::GenerateCode() {
    // XXX(ionel): At the moment we only support a DAG in which each node has
    // only one children.
    LOG(INFO) << "Hadoop generate code";
    // If false the code should be added to the map function, otherwise to the
    // reduce.
    bool add_to_reduce = false;
    shared_ptr<OperatorNode> op_node = dag[0];
    OperatorInterface* op = op_node->get_operator();
    LOG(INFO) << "Merging node: " << op->get_output_relation()->get_name();
    TemplateDictionary dict("op");
    pair<string, string> input_rel_code = GetInputPathsAndRelationsCode(dag);
    HadoopJobCode* dag_code =
      dynamic_cast<HadoopJobCode*>(TranslateOperator(op));
    while (!op_node->IsLeaf()) {
      // If op is one of the following then we can only add the code to the
      // reduce step.
      if (HasReduce(op)) {
        if (add_to_reduce) {
          // We can't merge two operators that have reduce function.
          LOG(ERROR) << "Can not merge two operators with reduce function";
          return NULL;
        }
        add_to_reduce = true;
      }
      mutable_default_template_cache()->ClearCache();
      // Decide if we're caching the map or the reduce function.
      if (add_to_reduce) {
        StringToTemplateCache("dag", dag_code->get_reduce_code(),
                              ctemplate::DO_NOT_STRIP);
      } else {
        StringToTemplateCache("dag", dag_code->get_map_code(), ctemplate::DO_NOT_STRIP);
      }
      op_nodes children = op_node->get_children();
      if (children.size() > 1) {
        LOG(ERROR) << "Can not merge operator with two dependant operators";
        return NULL;
      }
      // This works with the assumption that there is only one children.
      for (op_nodes::iterator it = children.begin(); it != children.end();
           ++it) {
        HadoopJobCode* child_code = dynamic_cast<HadoopJobCode*>(
            TranslateOperator((*it)->get_operator()));
        // We don't have to set the reduce code, because we can never merge it
        // a reduce with another one. The intermediate step is required.
        dict.SetValue("NEXT_OPERATOR", child_code->get_map_code());
        string code = "";
        ExpandTemplate("dag", ctemplate::DO_NOT_STRIP, &dict, &code);
        UpdateDAGCode(code, child_code, add_to_reduce, dag_code);
        if (HasReduce((*it)->get_operator())) {
          dag_code->set_reduce_code(child_code->get_reduce_code());
          dag_code->set_reduce_key_type(child_code->get_reduce_key_type());
          dag_code->set_reduce_value_type(child_code->get_reduce_value_type());
          dag_code->set_map_key_type(child_code->get_map_key_type());
          dag_code->set_map_value_type(child_code->get_map_value_type());
        }
        // Set op to the next one to be processed by the while loop.
        op_node = *it;
        op = op_node->get_operator();
      }
      LOG(INFO) << "Merging node: " << op->get_output_relation()->get_name();
    }
    PopulateEndDAG(op, dag_code, &dict, input_rel_code);
    string template_location;
    if (dag_code->HasReduceCode()) {
      dict.SetValue("REDUCE_CODE", dag_code->get_reduce_code());
      template_location = FLAGS_hadoop_templates_dir +
        "JobMapReduceTemplate.java";
    } else {
      template_location = FLAGS_hadoop_templates_dir + "JobMapTemplate.java";
    }
    string gen_code = "";
    mutable_default_template_cache()->ClearCache();
    ExpandTemplate(template_location, ctemplate::DO_NOT_STRIP, &dict, &gen_code);
    // This works because we assume that we only output to one relation.
    LOG(INFO) << "Job name: " << op->get_output_relation()->get_name();
    return GenAndCompile(op, gen_code);
  }

  string TranslatorHadoop::GenerateGroupByKey(
      const vector<Column*>& group_bys) {
    string result = "";
    if (group_bys.size() == 1 && group_bys[0]->get_relation().empty()) {
      return "";
    }
    for (vector<Column*>::const_iterator it = group_bys.begin();
         it != group_bys.end(); ++it) {
      result += (*it)->get_relation() + "[" +
        boost::lexical_cast<string>((*it)->get_index()) + "] + \" \" + ";
    }
    if (!result.empty()) {
      // This is to remove the trailing " + " " +"
      return result.substr(0, result.size() - 9);
    }
    return "\"\"";
  }

  HadoopJobCode* TranslatorHadoop::Translate(AggOperator* op) {
    string input_path = op->get_input_paths()[0];
    TemplateDictionary dict("agg");
    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("INPUT_PATH", input_path);
    dict.SetValue("OUTPUT_PATH", op->get_output_path());
    dict.SetValue("REL_NAME", AvoidNameClash(op, 0));
    dict.SetValue("CONDITION", op->get_condition_tree()->toString("hadoop"));
    dict.SetValue("GROUP_BY",
      boost::lexical_cast<string>(op->get_group_bys()[0]->get_index()));
    dict.SetValue("GROUP_BY_KEY", GenerateGroupByKey(op->get_group_bys()));
    dict.SetValue("NUM_AGG_COLS",
                  boost::lexical_cast<string>(op->get_columns().size()));
    vector<Column*> columns = op->get_columns();
    string index_array = boost::lexical_cast<string>(columns[0]->get_index());
    for (vector<Column*>::size_type i = 1; i != columns.size(); i++) {
      index_array += ", " +
        boost::lexical_cast<string>(columns[i]->get_index());
    }
    dict.SetValue("INDEX_ARRAY", index_array);
    // TODO(ionel): Add support for multiple columns agg.
    dict.SetValue("COL_INDEX",
                  boost::lexical_cast<string>(op->get_columns()[0]->get_index()));
    string math_op = op->get_operator();
    if (math_op.compare("+") && math_op.compare("-")) {
      dict.SetValue("INIT_VAL", "1");
    } else {
      dict.SetValue("INIT_VAL", "0");
    }
    dict.SetValue("OPERATOR", op->get_operator());
    // TODO(ionel): Add support for multiple columns agg.
    dict.SetValue("TYPE", op->get_columns()[0]->translateTypeJava());
    dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    dict.SetValue("OUTPUT_REL", op->get_output_relation()->get_name());
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string op_map_variables;
    string op_map;
    string op_cleanup;
    string op_reduce;
    ExpandTemplate(FLAGS_hadoop_templates_dir + "AggMapVariables.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_map_variables);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "AggMap.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_map);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "AggCleanup.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_cleanup);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "AggReduce.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_reduce);
    HadoopJobCode* job_code =
      new HadoopJobCode(op, op_map_variables, "", op_map, op_cleanup,
                        op_reduce);
    job_code->set_map_key_type("Text");
    job_code->set_map_value_type("Text");
    job_code->set_reduce_key_type("NullWritable");
    job_code->set_reduce_value_type("Text");
    return job_code;
  }

  HadoopJobCode* TranslatorHadoop::Translate(CountOperator* op) {
    // TODO(ionel): Handle multiple input paths. Handle multiple relations.
    string input_path = op->get_input_paths()[0];
    TemplateDictionary dict("count");
    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("INPUT_PATH", input_path);
    dict.SetValue("OUTPUT_PATH", op->get_output_path());
    dict.SetValue("REL_NAME", AvoidNameClash(op, 0));
    dict.SetValue("CONDITION", op->get_condition_tree()->toString("hadoop"));
    dict.SetValue("GROUP_BY",
      boost::lexical_cast<string>(op->get_group_bys()[0]->get_index()));
    dict.SetValue("GROUP_BY_KEY", GenerateGroupByKey(op->get_group_bys()));
    dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    dict.SetValue("OUTPUT_REL", op->get_output_relation()->get_name());
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string op_map_variables;
    string op_map;
    string op_cleanup;
    string op_reduce;
    ExpandTemplate(FLAGS_hadoop_templates_dir + "CountMapVariables.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_map_variables);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "CountMap.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_map);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "CountCleanup.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_cleanup);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "CountReduce.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_reduce);
    HadoopJobCode* job_code =
      new HadoopJobCode(op, op_map_variables, "", op_map, op_cleanup,
                        op_reduce);
    job_code->set_map_key_type("Text");
    job_code->set_map_value_type("Text");
    job_code->set_reduce_key_type("NullWritable");
    job_code->set_reduce_value_type("Text");
    return job_code;
  }

  HadoopJobCode* TranslatorHadoop::Translate(CrossJoinOperator* op) {
    vector<string> input_paths = op->get_input_paths();
    TemplateDictionary dict("cross_join");
    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("LEFT_REL", AvoidNameClash(op, 0));
    dict.SetValue("RIGHT_REL", AvoidNameClash(op, 1));
    dict.SetValue("LEFT_PATH", input_paths[0]);
    dict.SetValue("RIGHT_PATH", input_paths[1]);
    dict.SetValue("OUTPUT_PATH", op->get_output_path());
    dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    dict.SetValue("OUTPUT_REL", op->get_output_relation()->get_name());
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string op_map_variables;
    string op_setup;
    string op_map;
    string op_reduce;
    ExpandTemplate(FLAGS_hadoop_templates_dir + "CrossJoinMapVariables.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_map_variables);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "CrossJoinSetup.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_setup);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "CrossJoinMap.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_map);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "CrossJoinReduce.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_reduce);
    HadoopJobCode* job_code =
      new HadoopJobCode(op, op_map_variables, op_setup, op_map, "", op_reduce);
    job_code->set_map_key_type("Text");
    job_code->set_map_value_type("Text");
    job_code->set_reduce_key_type("NullWritable");
    job_code->set_reduce_value_type("Text");
    return job_code;
  }

  HadoopJobCode* TranslatorHadoop::Translate(DifferenceOperator* op) {
    vector<string> input_paths = op->get_input_paths();
    TemplateDictionary dict("difference");
    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("LEFT_PATH", input_paths[0]);
    dict.SetValue("RIGHT_PATH", input_paths[1]);
    dict.SetValue("OUTPUT_PATH", op->get_output_path());
    dict.SetValue("LEFT_REL", AvoidNameClash(op, 0));
    dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    dict.SetValue("OUTPUT_REL", op->get_output_relation()->get_name());
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string op_map_variables;
    string op_setup;
    string op_map;
    string op_reduce;
    ExpandTemplate(FLAGS_hadoop_templates_dir + "DifferenceMapVariables.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_map_variables);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "DifferenceSetup.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_setup);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "DifferenceMap.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_map);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "DifferenceReduce.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_reduce);
    HadoopJobCode* job_code =
      new HadoopJobCode(op, op_map_variables, op_setup, op_map, "",
                        op_reduce);
    job_code->set_map_key_type("Text");
    job_code->set_map_value_type("BooleanWritable");
    job_code->set_reduce_key_type("NullWritable");
    job_code->set_reduce_value_type("Text");
    return job_code;
  }

  HadoopJobCode* TranslatorHadoop::Translate(DistinctOperator* op) {
    TemplateDictionary dict("distinct");
    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("INPUT_PATH", op->get_input_paths()[0]);
    dict.SetValue("OUTPUT_PATH", op->get_output_path());
    dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    dict.SetValue("OUTPUT_REL", op->get_output_relation()->get_name());
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string op_map;
    string op_reduce;
    ExpandTemplate(FLAGS_hadoop_templates_dir + "DistinctMap.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_map);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "DistinctReduce.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_reduce);
    HadoopJobCode* job_code =
      new HadoopJobCode(op, "", "", op_map, "", op_reduce);
    job_code->set_map_key_type("Text");
    job_code->set_map_value_type("NullWritable");
    job_code->set_reduce_key_type("NullWritable");
    job_code->set_reduce_value_type("Text");
    return job_code;
  }

  HadoopJobCode* TranslatorHadoop::Translate(DivOperator* op) {
    string input_path = op->get_input_paths()[0];
    TemplateDictionary dict("div");
    vector<Value*> values = op->get_values();
    Column* left_column = dynamic_cast<Column*>(values[0]);
    // Set default values so that the generated code compiles.
    dict.SetValue("LEFT_TYPE", Column::stringTypeJava(INTEGER_TYPE));
    dict.SetValue("LEFT_COL_INDEX", "-1");
    dict.SetValue("RIGHT_TYPE", Column::stringTypeJava(INTEGER_TYPE));
    dict.SetValue("RIGHT_COL_INDEX", "-1");
    dict.SetValue("VALUE", "1");
    if (left_column != NULL) {
      dict.SetValue("LEFT_COL_INDEX",
                    boost::lexical_cast<string>(left_column->get_index()));
      dict.SetValue("LEFT_TYPE", left_column->translateTypeJava());
    } else {
      dict.SetValue("VALUE", values[0]->get_value());
    }
    Column* right_column = dynamic_cast<Column*>(values[1]);
    if (right_column != NULL) {
      dict.SetValue("RIGHT_COL_INDEX",
                    boost::lexical_cast<string>(right_column->get_index()));
      dict.SetValue("RIGHT_TYPE", right_column->translateTypeJava());
    } else {
      dict.SetValue("VALUE", values[1]->get_value());
    }
    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("INPUT_PATH", input_path);
    dict.SetValue("OUTPUT_PATH", op->get_output_path());
    dict.SetValue("REL_NAME", AvoidNameClash(op, 0));
    dict.SetValue("CONDITION", op->get_condition_tree()->toString("hadoop"));
    dict.SetValue("OUTPUT_REL", op->get_output_relation()->get_name());
    dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string map_code;
    ExpandTemplate(FLAGS_hadoop_templates_dir + "DivMap.java",
                   ctemplate::DO_NOT_STRIP, &dict, &map_code);
    HadoopJobCode* job_code = new HadoopJobCode(op, "", "", map_code, "", "");
    job_code->set_map_key_type("NullWritable");
    job_code->set_map_value_type("Text");
    return job_code;
  }

  HadoopJobCode* TranslatorHadoop::Translate(IntersectionOperator* op) {
    vector<string> input_paths = op->get_input_paths();
    TemplateDictionary dict("intersection");
    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("LEFT_PATH", input_paths[0]);
    dict.SetValue("RIGHT_PATH", input_paths[1]);
    dict.SetValue("OUTPUT_PATH", op->get_output_path());
    dict.SetValue("LEFT_REL", AvoidNameClash(op, 0));
    dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    dict.SetValue("OUTPUT_REL", op->get_output_relation()->get_name());
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string op_map_variables;
    string op_setup;
    string op_map;
    string op_reduce;
    ExpandTemplate(FLAGS_hadoop_templates_dir + "IntersectionMapVariables.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_map_variables);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "IntersectionSetup.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_setup);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "IntersectionMap.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_map);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "IntersectionReduce.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_reduce);
    HadoopJobCode* job_code =
      new HadoopJobCode(op, op_map_variables, op_setup, op_map, "",
                        op_reduce);
    job_code->set_map_key_type("Text");
    job_code->set_map_value_type("BooleanWritable");
    job_code->set_reduce_key_type("NullWritable");
    job_code->set_reduce_value_type("Text");
    return job_code;
  }

  HadoopJobCode* TranslatorHadoop::Translate(JoinOperator* op) {
    vector<string> input_paths = op->get_input_paths();
    TemplateDictionary dict("join");
    // Set default values so that the generated code compiles.
    dict.SetValue("CLASS_NAME", class_name);
    string left_rel_name = AvoidNameClash(op, 0);
    dict.SetValue("LEFT_REL", left_rel_name);

    vector<Column*> left_cols = op->get_left_cols();
    for (vector<Column*>::iterator it = left_cols.begin(); it != left_cols.end(); ++it) {
      TemplateDictionary* left_indices = dict.AddSectionDictionary("LEFT_INDICES");
      left_indices->SetValue("CHECK_INDEX",
                             left_rel_name + "_i == " +
                             boost::lexical_cast<string>((*it)->get_index()));
    }

    string right_rel_name = AvoidNameClash(op, 1);
    dict.SetValue("RIGHT_REL", right_rel_name);

    vector<Column*> right_cols = op->get_right_cols();
    for (vector<Column*>::iterator it = right_cols.begin(); it != right_cols.end(); ++it) {
      TemplateDictionary* right_indices = dict.AddSectionDictionary("RIGHT_INDICES");
      right_indices->SetValue("CHECK_NOT_INDEX",
                              right_rel_name + "_i != " +
                              boost::lexical_cast<string>((*it)->get_index()));
    }

    dict.SetValue("LEFT_PATH", input_paths[0]);
    dict.SetValue("RIGHT_PATH", input_paths[1]);
    dict.SetValue("OUTPUT_PATH", op->get_output_path());
    dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    dict.SetValue("OUTPUT_REL", op->get_output_relation()->get_name());
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string op_map_variables;
    string op_setup;
    string op_map;
    string op_reduce;
    ExpandTemplate(FLAGS_hadoop_templates_dir + "JoinMapVariables.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_map_variables);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "JoinSetup.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_setup);
    if (single_input) {
      ExpandTemplate(FLAGS_hadoop_templates_dir + "JoinMapSingleInput.java",
                     ctemplate::DO_NOT_STRIP, &dict, &op_map);
    } else {
      ExpandTemplate(FLAGS_hadoop_templates_dir + "JoinMap.java",
                     ctemplate::DO_NOT_STRIP, &dict, &op_map);
    }
    ExpandTemplate(FLAGS_hadoop_templates_dir + "JoinReduce.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_reduce);
    HadoopJobCode* job_code =
      new HadoopJobCode(op, op_map_variables, op_setup, op_map, "", op_reduce);
    job_code->set_map_key_type("Text");
    job_code->set_map_value_type("Text");
    job_code->set_reduce_key_type("NullWritable");
    job_code->set_reduce_value_type("Text");
    return job_code;
  }

  HadoopJobCode* TranslatorHadoop::Translate(MaxOperator* op) {
    // TODO(ionel): Handle multiple input paths. Handle multiple relations.
    string input_path = op->get_input_paths()[0];
    TemplateDictionary dict("max");
    string col_type = op->get_column()->translateTypeJava();
    dict.SetValue("COL_TYPE", col_type);
    string rel_name = AvoidNameClash(op, 0);
    dict.SetValue("REL_NAME", rel_name);
    if (col_type == "String") {
      dict.SetValue("MAX_VALUE", "String maxValue = \"\";");
    } else {
      dict.SetValue("MAX_VALUE", col_type + " maxValue = " + col_type +
                    ".MIN_VALUE;");
    }
    if (op->get_selected_columns().size() != 0) {
      vector<Column*> selected_columns = op->get_selected_columns();
      string selected_cols = "selectedCols = \" \"";
      for (vector<Column*>::iterator it = selected_columns.begin();
           it != selected_columns.end(); ++it) {
        selected_cols += " + " + rel_name + "[" +
          boost::lexical_cast<string>((*it)->get_index()) + "]";
      }
      selected_cols += ";";
      dict.SetValue("GEN_SELECTED_COLS", selected_cols);
    } else {
      dict.SetValue("GEN_SELECTED_COLS", "selectedCols = \"\";");
    }
    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("INPUT_PATH", input_path);
    dict.SetValue("OUTPUT_PATH", op->get_output_path());
    dict.SetValue("CONDITION", op->get_condition_tree()->toString("hadoop"));
    dict.SetValue("COL_INDEX",
                  boost::lexical_cast<string>(op->get_column()->get_index()));
    dict.SetValue("GROUP_BY",
      boost::lexical_cast<string>(op->get_group_bys()[0]->get_index()));
    dict.SetValue("GROUP_BY_KEY", GenerateGroupByKey(op->get_group_bys()));
    dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    dict.SetValue("OUTPUT_REL", op->get_output_relation()->get_name());
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string op_map_variables;
    string op_map;
    string op_cleanup;
    string op_reduce;
    ExpandTemplate(FLAGS_hadoop_templates_dir + "MaxMapVariables.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_map_variables);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "MaxMap.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_map);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "MaxCleanup.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_cleanup);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "MaxReduce.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_reduce);
    HadoopJobCode* job_code =
      new HadoopJobCode(op, op_map_variables, "", op_map, op_cleanup,
                        op_reduce);
    job_code->set_map_key_type("Text");
    job_code->set_map_value_type("Text");
    job_code->set_reduce_key_type("NullWritable");
    job_code->set_reduce_value_type("Text");
    return job_code;
  }

  HadoopJobCode* TranslatorHadoop::Translate(MinOperator* op) {
    // TODO(ionel): Handle multiple input paths. Handle multiple relations.
    string input_path = op->get_input_paths()[0];
    TemplateDictionary dict("min");
    string col_type = op->get_column()->translateTypeJava();
    dict.SetValue("COL_TYPE", col_type);
    string rel_name = AvoidNameClash(op, 0);
    dict.SetValue("REL_NAME", rel_name);
    if (col_type == "String") {
      dict.SetValue("MIN_VALUE", "String minValue = \"\";");
    } else {
      dict.SetValue("MIN_VALUE", col_type + " minValue = " + col_type +
                    ".MAX_VALUE;");
    }
    if (op->get_selected_columns().size() != 0) {
      vector<Column*> selected_columns = op->get_selected_columns();
      string selected_cols = "selectedCols = \" \"";
      for (vector<Column*>::iterator it = selected_columns.begin();
           it != selected_columns.end(); ++it) {
        selected_cols += " + " + rel_name + "[" +
          boost::lexical_cast<string>((*it)->get_index()) + "]";
      }
      selected_cols += ";";
      dict.SetValue("GEN_SELECTED_COLS", selected_cols);
    } else {
      dict.SetValue("GEN_SELECTED_COLS", "selectedCols = \"\";");
    }
    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("INPUT_PATH", input_path);
    dict.SetValue("OUTPUT_PATH", op->get_output_path());
    dict.SetValue("CONDITION", op->get_condition_tree()->toString("hadoop"));
    dict.SetValue("COL_INDEX",
                  boost::lexical_cast<string>(op->get_column()->get_index()));
    dict.SetValue("GROUP_BY",
      boost::lexical_cast<string>(op->get_group_bys()[0]->get_index()));
    dict.SetValue("GROUP_BY_KEY", GenerateGroupByKey(op->get_group_bys()));
    dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    dict.SetValue("OUTPUT_REL", op->get_output_relation()->get_name());
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string op_map_variables;
    string op_map;
    string op_cleanup;
    string op_reduce;
    ExpandTemplate(FLAGS_hadoop_templates_dir + "MinMapVariables.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_map_variables);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "MinMap.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_map);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "MinCleanup.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_cleanup);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "MinReduce.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_reduce);
    HadoopJobCode* job_code =
      new HadoopJobCode(op, op_map_variables, "", op_map, op_cleanup,
                        op_reduce);
    job_code->set_map_key_type("Text");
    job_code->set_map_value_type("Text");
    job_code->set_reduce_key_type("NullWritable");
    job_code->set_reduce_value_type("Text");
    return job_code;
  }

  HadoopJobCode* TranslatorHadoop::Translate(MulOperator* op) {
    string input_path = op->get_input_paths()[0];
    TemplateDictionary dict("mul");
    vector<Value*> values = op->get_values();
    Column* left_column = dynamic_cast<Column*>(values[0]);
    // Set default values so that the generated code compiles.
    dict.SetValue("LEFT_TYPE", Column::stringTypeJava(INTEGER_TYPE));
    dict.SetValue("LEFT_COL_INDEX", "-1");
    dict.SetValue("RIGHT_TYPE", Column::stringTypeJava(INTEGER_TYPE));
    dict.SetValue("RIGHT_COL_INDEX", "-1");
    dict.SetValue("VALUE", "1");
    if (left_column != NULL) {
      dict.SetValue("LEFT_COL_INDEX",
                    boost::lexical_cast<string>(left_column->get_index()));
      dict.SetValue("LEFT_TYPE", left_column->translateTypeJava());
    } else {
      dict.SetValue("VALUE", values[0]->get_value());
    }
    Column* right_column = dynamic_cast<Column*>(values[1]);
    if (right_column != NULL) {
      dict.SetValue("RIGHT_COL_INDEX",
                    boost::lexical_cast<string>(right_column->get_index()));
      dict.SetValue("RIGHT_TYPE", right_column->translateTypeJava());
    } else {
      dict.SetValue("VALUE", values[1]->get_value());
    }
    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("INPUT_PATH", input_path);
    dict.SetValue("OUTPUT_PATH", op->get_output_path());
    dict.SetValue("REL_NAME", AvoidNameClash(op, 0));
    dict.SetValue("CONDITION", op->get_condition_tree()->toString("hadoop"));
    dict.SetValue("OUTPUT_REL", op->get_output_relation()->get_name());
    dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string map_code;
    ExpandTemplate(FLAGS_hadoop_templates_dir + "MulMap.java",
                   ctemplate::DO_NOT_STRIP, &dict, &map_code);
    HadoopJobCode* job_code =  new HadoopJobCode(op, "", "", map_code, "", "");
    job_code->set_map_key_type("NullWritable");
    job_code->set_map_value_type("Text");
    return job_code;
  }

  HadoopJobCode* TranslatorHadoop::Translate(SelectOperator* op) {
    // TODO(ionel): Handle multiple input paths. Handle multiple relations.
    string input_path = op->get_input_paths()[0];
    string rel_name = AvoidNameClash(op, 0);
    TemplateDictionary dict("select");
    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("INPUT_PATH", input_path);
    dict.SetValue("OUTPUT_PATH", op->get_output_path());
    dict.SetValue("REL_NAME", rel_name);
    dict.SetValue("CONDITION", op->get_condition_tree()->toString("hadoop"));
    vector<Column*> columns_proj = op->get_columns();
    string output_rel = op->get_output_relation()->get_name();
    uint32_t index = 0;
    string get_columns = "";
    for (vector<Column*>::iterator it = columns_proj.begin();
         it != columns_proj.end(); ++it) {
      get_columns += "      " + output_rel + "[" +
        boost::lexical_cast<string>(index) + "] = " + rel_name +
        "[" + boost::lexical_cast<string>((*it)->get_index()) + "];\n";
      index++;
    }
    dict.SetValue("COL_SIZE", boost::lexical_cast<string>(columns_proj.size()));
    dict.SetValue("GET_COLUMNS", get_columns);
    dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    dict.SetValue("OUTPUT_REL", output_rel);
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string op_map;
    ExpandTemplate(FLAGS_hadoop_templates_dir + "SelectMap.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_map);
    HadoopJobCode* job_code =  new HadoopJobCode(op, "", "", op_map, "", "");
    job_code->set_map_key_type("NullWritable");
    job_code->set_map_value_type("Text");
    return job_code;
  }

  // Todo(ionel): Plugin arguments/columns
  HadoopJobCode* TranslatorHadoop::Translate(ProjectOperator* op) {
    // TODO(ionel): Handle multiple input paths.
    string input_path = op->get_input_paths()[0];
    string rel_name = AvoidNameClash(op, 0);
    // At the moment we assume all the columns come from a single relation.
    vector<Column*> columns = op->get_columns();
    TemplateDictionary dict("project");
    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("INPUT_PATH", input_path);
    dict.SetValue("OUTPUT_PATH", op->get_output_path());
    dict.SetValue("REL_NAME", rel_name);
    dict.SetValue("CONDITION", op->get_condition_tree()->toString("hadoop"));
    string output_rel = op->get_output_relation()->get_name();
    uint32_t index = 0;
    string get_columns = "";
    for (vector<Column*>::iterator it = columns.begin(); it != columns.end();
         ++it) {
      get_columns += "      " + output_rel + "[" +
        boost::lexical_cast<string>(index) + "] = " + rel_name +
        "[" + boost::lexical_cast<string>((*it)->get_index()) + "];\n";
      index++;
    }
    dict.SetValue("GET_COLUMNS", get_columns);
    dict.SetValue("COL_SIZE", boost::lexical_cast<string>(columns.size()));
    dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    dict.SetValue("OUTPUT_REL", output_rel);
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string op_map;
    ExpandTemplate(FLAGS_hadoop_templates_dir + "ProjectMap.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_map);
    HadoopJobCode* job_code = new HadoopJobCode(op, "", "", op_map, "", "");
    job_code->set_map_key_type("NullWritable");
    job_code->set_map_value_type("Text");
    return job_code;
  }

  HadoopJobCode* TranslatorHadoop::Translate(SortOperator* op) {
    string input_path = op->get_input_paths()[0];
    TemplateDictionary dict("sort");
    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("INPUT_PATH", input_path);
    dict.SetValue("OUTPUT_PATH", op->get_output_path());
    dict.SetValue("REL_NAME", AvoidNameClash(op, 0));
    dict.SetValue("CONDITION", op->get_condition_tree()->toString("hadoop"));
    dict.SetValue("COL_INDEX",
                  boost::lexical_cast<string>(op->get_column()->get_index()));
    uint16_t col_type = op->get_column()->get_type();
    dict.SetValue("COL_TYPE", op->get_column()->translateTypeJava());
    dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    dict.SetValue("OUTPUT_REL", op->get_output_relation()->get_name());
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string col_type_string;
    switch (col_type) {
    case INTEGER_TYPE: {
      col_type_string = "IntWritable";
      dict.SetValue("HADOOP_COL_TYPE", "IntWritable");
      break;
    }
    case STRING_TYPE: {
      col_type_string = "Text";
      dict.SetValue("HADOOP_COL_TYPE", "Text");
      break;
    }
    case DOUBLE_TYPE: {
      col_type_string = "DoubleWritable";
      dict.SetValue("HADOOP_COL_TYPE", "DoubleWritable");
      break;
    }
    case BOOLEAN_TYPE: {
      col_type_string = "BoolWritable";
      dict.SetValue("HADOOP_COL_TYPE", "BoolWritable");
      break;
    }
    default: {
      LOG(ERROR) << "Column has unexpected type";
    }
    }
    string op_map;
    string op_reduce;
    ExpandTemplate(FLAGS_hadoop_templates_dir + "SortMap.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_map);
    ExpandTemplate(FLAGS_hadoop_templates_dir + "SortReduce.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_reduce);
    HadoopJobCode* job_code =
      new HadoopJobCode(op, "", "", op_map, "", op_reduce);
    job_code->set_map_key_type(col_type_string);
    job_code->set_map_value_type("Text");
    job_code->set_reduce_key_type("NullWritable");
    job_code->set_reduce_value_type("Text");
    return job_code;
  }

  HadoopJobCode* TranslatorHadoop::Translate(SubOperator* op) {
    string input_path = op->get_input_paths()[0];
    TemplateDictionary dict("sub");
    vector<Value*> values = op->get_values();
    Column* left_column = dynamic_cast<Column*>(values[0]);
    // Set default values so that the generated code compiles.
    dict.SetValue("LEFT_TYPE", Column::stringTypeJava(INTEGER_TYPE));
    dict.SetValue("LEFT_COL_INDEX", "-1");
    dict.SetValue("RIGHT_TYPE", Column::stringTypeJava(INTEGER_TYPE));
    dict.SetValue("RIGHT_COL_INDEX", "-1");
    dict.SetValue("VALUE", "0");
    if (left_column != NULL) {
      dict.SetValue("LEFT_COL_INDEX",
                    boost::lexical_cast<string>(left_column->get_index()));
      dict.SetValue("LEFT_TYPE", left_column->translateTypeJava());
    } else {
      dict.SetValue("VALUE", values[0]->get_value());
    }
    Column* right_column = dynamic_cast<Column*>(values[1]);
    if (right_column != NULL) {
      dict.SetValue("RIGHT_COL_INDEX",
                    boost::lexical_cast<string>(right_column->get_index()));
      dict.SetValue("RIGHT_TYPE", right_column->translateTypeJava());
    } else {
      dict.SetValue("VALUE", values[1]->get_value());
    }
    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("INPUT_PATH", input_path);
    dict.SetValue("OUTPUT_PATH", op->get_output_path());
    dict.SetValue("REL_NAME", AvoidNameClash(op, 0));
    dict.SetValue("CONDITION", op->get_condition_tree()->toString("hadoop"));
    dict.SetValue("OUTPUT_REL", op->get_output_relation()->get_name());
    dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string map_code;
    ExpandTemplate(FLAGS_hadoop_templates_dir + "SubMap.java",
                   ctemplate::DO_NOT_STRIP, &dict, &map_code);
    HadoopJobCode* job_code =  new HadoopJobCode(op, "", "", map_code, "", "");
    job_code->set_map_key_type("NullWritable");
    job_code->set_map_value_type("Text");
    return job_code;
  }

  HadoopJobCode* TranslatorHadoop::Translate(SumOperator* op) {
    // TODO(ionel): Handle multiple input paths. Handle multiple relations.
    string input_path = op->get_input_paths()[0];
    TemplateDictionary dict("sum");
    vector<Value*> values = op->get_values();
    Column* left_column = dynamic_cast<Column*>(values[0]);
    // Set default values so that the generated code compiles.
    dict.SetValue("LEFT_TYPE", Column::stringTypeJava(INTEGER_TYPE));
    dict.SetValue("LEFT_COL_INDEX", "-1");
    dict.SetValue("RIGHT_TYPE", Column::stringTypeJava(INTEGER_TYPE));
    dict.SetValue("RIGHT_COL_INDEX", "-1");
    dict.SetValue("VALUE", "0");
    if (left_column != NULL) {
      dict.SetValue("LEFT_COL_INDEX",
                    boost::lexical_cast<string>(left_column->get_index()));
      dict.SetValue("LEFT_TYPE", left_column->translateTypeJava());
    } else {
      dict.SetValue("VALUE", values[0]->get_value());
    }
    Column* right_column = dynamic_cast<Column*>(values[1]);
    if (right_column != NULL) {
      dict.SetValue("RIGHT_COL_INDEX",
                    boost::lexical_cast<string>(right_column->get_index()));
      dict.SetValue("RIGHT_TYPE", right_column->translateTypeJava());
    } else {
      dict.SetValue("VALUE", values[1]->get_value());
    }
    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("INPUT_PATH", input_path);
    dict.SetValue("OUTPUT_PATH", op->get_output_path());
    dict.SetValue("REL_NAME", AvoidNameClash(op, 0));
    dict.SetValue("CONDITION", op->get_condition_tree()->toString("hadoop"));
    dict.SetValue("OUTPUT_REL", op->get_output_relation()->get_name());
    dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string map_code;
    ExpandTemplate(FLAGS_hadoop_templates_dir + "SumMap.java",
                   ctemplate::DO_NOT_STRIP, &dict, &map_code);
    HadoopJobCode* job_code = new HadoopJobCode(op, "", "", map_code, "", "");
    job_code->set_map_key_type("NullWritable");
    job_code->set_map_value_type("Text");
    return job_code;
  }

  HadoopJobCode* TranslatorHadoop::Translate(UdfOperator* op) {
    string input_path = op->get_input_paths()[0];
    TemplateDictionary dict("udf");
    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("INPUT_PATH", input_path);
    dict.SetValue("OUTPUT_PATH", op->get_output_path());
    dict.SetValue("UDF_CLASS", op->get_udf_name());
    string op_map;
    ExpandTemplate(FLAGS_hadoop_templates_dir + "UdfMap.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_map);
    HadoopJobCode* job_code = new  HadoopJobCode(op, "", "", op_map, "", "");
    job_code->set_map_key_type("NullWritable");
    job_code->set_map_value_type("Text");
    return job_code;
  }

  HadoopJobCode* TranslatorHadoop::Translate(UnionOperator* op) {
    vector<string> input_paths = op->get_input_paths();
    TemplateDictionary dict("union");
    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("LEFT_PATH", input_paths[0]);
    dict.SetValue("RIGHT_PATH", input_paths[1]);
    dict.SetValue("OUTPUT_PATH", op->get_output_path());
    dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    dict.SetValue("OUTPUT_REL", op->get_output_relation()->get_name());
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string op_map;
    ExpandTemplate(FLAGS_hadoop_templates_dir + "UnionMap.java",
                   ctemplate::DO_NOT_STRIP, &dict, &op_map);
    HadoopJobCode* job_code = new HadoopJobCode(op, "", "", op_map, "", "");
    job_code->set_map_key_type("NullWritable");
    job_code->set_map_value_type("Text");
    return job_code;
  }

  string TranslatorHadoop::GenAndCompile(OperatorInterface* op,
                                         const string& op_code) {
    ofstream job_file;
    string source_file = GetSourcePath(op);
    string binary_file = GetBinaryPath(op);
    string path = op->get_code_dir() + op->get_output_relation()->get_name() +
      "_code/";
    string create_dir = "mkdir -p " + path;
    std::system(create_dir.c_str());
    job_file.open(source_file.c_str());
    job_file << op_code;
    job_file.close();
    // Compile the generated operator code.
    LOG(INFO) << "hadoop build started for: " << class_name;
    timeval start_compile;
    gettimeofday(&start_compile, NULL);
    if (op->get_type() == UDF_OP) {
      UdfOperator* udf_op = dynamic_cast<UdfOperator*>(op);
      string copy_cmd = "cp " + udf_op->get_udf_source_path() + " " + path;
      std::system(copy_cmd.c_str());
      string compile_cmd =
        "javac -cp ext/hadoop-common.jar:ext/hadoop-core.jar:ext/hadoop-hdfs.jar " +
        path + "*.java";
      std::system(compile_cmd.c_str());
    } else {
      string compile_cmd =
        "javac -cp ext/hadoop-common.jar:ext/hadoop-core.jar:ext/hadoop-hdfs.jar " +
        source_file;
      std::system(compile_cmd.c_str());
    }
    string cur_dir = ExecCmd("pwd");
    string jar_cmd = "cd " + path + "; jar cf " + binary_file +  " " +
      class_name + "*.class";
    std::system(jar_cmd.c_str());
    string chd_dir = "cd " + cur_dir;
    std::system(chd_dir.c_str());
    LOG(INFO) << "hadoop build ended for: " << class_name;
    timeval end_compile;
    gettimeofday(&end_compile, NULL);
    uint32_t compile_time = end_compile.tv_sec - start_compile.tv_sec;
    cout << "COMPILE TIME: " << compile_time << endl;
    return binary_file;
  }

} // namespace translator
} // namespace musketeer
