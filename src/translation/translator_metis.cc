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

#include "translation/translator_metis.h"

#include <boost/lexical_cast.hpp>
#include <ctemplate/template.h>
#include <sys/time.h>

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <utility>

#include "base/common.h"
#include "ir/column.h"
#include "ir/relation.h"

namespace musketeer {
namespace translator {

  using ctemplate::mutable_default_template_cache;

  TranslatorMetis::TranslatorMetis(const op_nodes& dag,
                                   const string& class_name):
    TranslatorInterface(dag, class_name),
    use_mergable_operators_(false) {
  }

  string TranslatorMetis::GetBinaryPath(OperatorInterface* op) {
    return op->get_code_dir() + class_name + "_code/" + class_name + "_bin";
  }

  string TranslatorMetis::GetSourcePath(OperatorInterface* op) {
    return op->get_code_dir() + class_name + "_code/" + class_name + ".cc";
  }

  // Map only operators: Div, Mul, Sub, Sum, Select, Project, Union
  // Map and reduce operators: Intersect, Difference. The map output key is the
  // entire row.
  // Map and reduce operators: Join. The map is used to select the joining
  // columns and to detect from which relation the row is coming.
  // Map and reduce operators: Agg, Count, Min, Max. The map is used to select
  // the group by columns or to do partial aggregation when no groupby clause is
  // present.
  string TranslatorMetis::GenerateCode() {
    // XXX(ionel): At the moment we only support a DAG in which each node has
    // only one child.
    LOG(INFO) << "Metis code generation starting";
    // If false the code should be added to the map function, otherwise to the
    // reduce.
    bool add_to_reduce = false;
    shared_ptr<OperatorNode> op_node = dag[0];
    OperatorInterface* op = op_node->get_operator();
    LOG(INFO) << "Merging node: " << op->get_output_relation()->get_name();
    TemplateDictionary dict("op");
    if (op_node->IsLeaf()) {
      VLOG(1) << "Have singular Metis operators, so using non-mergeable "
              << "implementation.";
      use_mergable_operators_ = false;
    } else {
      VLOG(1) << "Have multiple Metis operators, so using mergeable "
              << "implementations.";
      use_mergable_operators_ = true;
    }
    MetisJobCode* dag_code =
      dynamic_cast<MetisJobCode*>(TranslateOperator(op));
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
        StringToTemplateCache("dag", dag_code->get_reduce_code(), ctemplate::DO_NOT_STRIP);
      } else {
        StringToTemplateCache("dag", dag_code->get_map_code(), ctemplate::DO_NOT_STRIP);
      }
      op_nodes children = op_node->get_children();
      if (children.size() > 1) {
        LOG(ERROR) << "Can not merge operator with two dependent operators";
        return NULL;
      }
      // This works with the assumption that there is only one child.
      for (op_nodes::iterator it = children.begin(); it != children.end();
           ++it) {
        MetisJobCode* child_code = dynamic_cast<MetisJobCode*>(
            TranslateOperator((*it)->get_operator()));
        // We don't have to set the reduce code, because we can never merge it
        // a reduce with another one. The intermediate step is required.
        dict.SetValue("NEXT_OPERATOR", child_code->get_map_code());
        string code = "";
        ExpandTemplate("dag", ctemplate::DO_NOT_STRIP, &dict, &code);
        UpdateDAGCode(code, child_code, add_to_reduce, dag_code);
        // Set op to the next one to be processed by the while loop.
        op_node = *it;
        op = op_node->get_operator();
      }
      LOG(INFO) << "Merging node: " << op->get_output_relation()->get_name();
    }
    PopulateEndDAG(op, dag_code, &dict);
    string template_location;
    if (dag_code->HasReduceCode()) {
      dict.SetValue("REDUCE_CODE", dag_code->get_reduce_code());
      template_location = FLAGS_metis_templates_dir +
        "job_map_reduce_template.cc";
    } else {
      template_location =
        FLAGS_metis_templates_dir + "job_map_only_template.cc";
    }
    string gen_code = "";
    mutable_default_template_cache()->ClearCache();
    ExpandTemplate(template_location, ctemplate::DO_NOT_STRIP, &dict, &gen_code);
    // This works because we assume that we only output to one relation.
    LOG(INFO) << "Job name: " << op->get_output_relation()->get_name();
    return GenAndCompile(op, gen_code);
  }

  void TranslatorMetis::GenerateMakefile(OperatorInterface* op,
                                         const string& rel_name,
                                         const string& make_file_name) {
    ofstream make_file;
    // Generate Makefile for this operator
    TemplateDictionary make_file_dict("makefile");
    PopulateCommonValues(op, &make_file_dict);
    make_file_dict.SetValue("REL_NAME", rel_name);
    string difference = "";
    // Differentiate between local-only and HDFS-enabled Makefile
    string makefile_filename;
    if (FLAGS_metis_use_hdfs)
      makefile_filename = "Makefile_hdfs";
    else
      makefile_filename = "Makefile_localonly";
    ExpandTemplate(FLAGS_metis_templates_dir + makefile_filename,
                   ctemplate::DO_NOT_STRIP, &make_file_dict, &difference);
    make_file.open(make_file_name.c_str());
    make_file << difference;
    make_file.close();
  }

  bool TranslatorMetis::HasReduce(OperatorInterface* op) {
    return op->get_type() == INTERSECTION_OP ||
      op->get_type() == DIFFERENCE_OP || op->get_type() == JOIN_OP ||
      op->get_type() == AGG_OP || op->get_type() == COUNT_OP ||
      op->get_type() == MAX_OP || op->get_type() == MIN_OP ||
      op->get_type() == SORT_OP || op->get_type() == CROSS_JOIN_OP;
  }

  void TranslatorMetis::PopulateCommonValues(OperatorInterface* op,
                                             TemplateDictionary* dict) {
    string binary_file = GetBinaryPath(op);
    string output_path = op->get_output_path();
    string class_name =  op->get_output_relation()->get_name();
    dict->SetValue("OUTPUT", class_name);
    dict->SetValue("CLASS_NAME", class_name);
    dict->SetValue("OUTPUT_PATH", output_path);
    dict->SetValue("HDFS_MASTER", "hdfs://" + FLAGS_hdfs_master + ":" + FLAGS_hdfs_port);
    dict->SetValue("METIS_ROOT", FLAGS_metis_dir);
    dict->SetValue("BIN_NAME", binary_file);
  }

  void TranslatorMetis::PopulateEndDAG(
      OperatorInterface* op, MetisJobCode* dag_code,
      TemplateDictionary* dict) {
    dict->SetValue("OUTPUT_REL", op->get_output_relation()->get_name());
    mutable_default_template_cache()->ClearCache();
    if (!HasReduce(op)) {
      string output_code = "";
      ExpandTemplate(FLAGS_metis_templates_dir + "map_output_code.cc",
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
    ExpandTemplate(FLAGS_metis_templates_dir + "reduce_output_code.cc",
                   ctemplate::DO_NOT_STRIP, dict, &output_code);
    dict->SetValue("OUTPUT_CODE", output_code);
    mutable_default_template_cache()->ClearCache();
    StringToTemplateCache("dag", dag_code->get_reduce_code(), ctemplate::DO_NOT_STRIP);
    string dag_reduce_code = "";
    ExpandTemplate("dag", ctemplate::DO_NOT_STRIP, dict, &dag_reduce_code);
    dag_code->set_reduce_code(dag_reduce_code);
    // Extract input relations
    vector<Relation*> input_rels;
    vector<string> input_paths;
    GetInputPathsAndRelations(dag, &input_paths, &input_rels);
    pair<string, string> input_rel_code =
        GetInputPathsAndRelationsCode(input_paths, input_rels);
    // Set up input relation section
    uint32_t i = 0;
    for (vector<Relation*>::iterator it = input_rels.begin();
         it != input_rels.end();
         ++it) {
      TemplateDictionary* sub_dict =
        dict->AddSectionDictionary("INPUT_RELATIONS");
      sub_dict->SetValue("REL_NAME", (*it)->get_name());
      sub_dict->SetIntValue("REL_ID", i);
      sub_dict->SetIntValue("NUM_COLS", (*it)->get_columns().size());
      ++i;
    }
    // Set all remaining global variables
    dict->SetValue("CLASS_NAME", class_name);
    dict->SetValue("MAP_VARIABLES_CODE", dag_code->get_map_variables_code());
    dict->SetValue("SETUP_CODE", dag_code->get_setup_code());
    dict->SetValue("MAP_CODE", dag_map_code);
    dict->SetValue("CLEANUP_CODE", dag_code->get_cleanup_code());
    dict->SetValue("INPUT_PATH", input_rel_code.first);
    dict->SetValue("INPUT_CODE", input_rel_code.second);
    dict->SetValue("OUTPUT_PATH", op->get_output_path());
    dict->SetValue("MAP_KEY_TYPE", dag_code->get_map_key_type());
    dict->SetValue("MAP_VALUE_TYPE", dag_code->get_map_value_type());
    dict->SetValue("REDUCE_KEY_TYPE", dag_code->get_reduce_key_type());
    dict->SetValue("REDUCE_VALUE_TYPE", dag_code->get_reduce_value_type());
    dict->SetValue("OUTPUT_REL", op->get_output_relation()->get_name());
  }

  // TODO(ionel): There is a bug here. If an operator that uses same input and
  // output relation is present together with another operator which takes the
  // relation as input there will be a name clash in the generated code.
  void TranslatorMetis::GetInputPathsAndRelations(
      const op_nodes& dag, vector<string>* input_paths,
      vector<Relation*>* input_rels) {
    queue<shared_ptr<OperatorNode> > to_visit;
    set<shared_ptr<OperatorNode> > visited;
    set<string> known_rels;
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
          // XXX(malte): Need to fix the name swizzling if necessary
          /*if (!output_relation.compare(rel_name)) {
            input_rels->push_back(rel_name + "_input");
          } else {
            input_rels->push_back(rel_name);
          }*/
          input_rels->push_back(*rel_it);
          input_paths->push_back(op->CreateInputPath(*rel_it));
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
  }

  pair<string, string> TranslatorMetis::GetInputPathsAndRelationsCode(
       const vector<string>& input_paths, const vector<Relation*>& input_rels) {
    string input_paths_code = "";
    // XXX(malte): need correct support for multiple inputs here.
    for (vector<string>::const_iterator it = input_paths.begin();
         it != input_paths.end(); ++it) {
      LOG(INFO) << "Job input: " << *it;
      input_paths_code += "input_paths.push_back(\"" + *it + "\");\n";
    }
    string input_rels_code = "";
    TemplateDictionary dict("tmp_input_dict");
    uint32_t i = 0;
    for (vector<Relation*>::const_iterator it = input_rels.begin();
         it != input_rels.end();
         ++it) {
      TemplateDictionary* sub_dict =
        dict.AddSectionDictionary("INPUT_RELATIONS");
      sub_dict->SetValue("REL_NAME", (*it)->get_name());
      sub_dict->SetIntValue("REL_ID", i);
      sub_dict->SetIntValue("NUM_COLS", (*it)->get_columns().size());
      ++i;
    }
    if (use_mergable_operators_) {
      ExpandTemplate(FLAGS_metis_templates_dir + "input_code.cc",
                     ctemplate::DO_NOT_STRIP, &dict, &input_rels_code);
    }
    return make_pair(input_paths_code, input_rels_code);
  }


  void TranslatorMetis::PrepareCodeDirectory(OperatorInterface* op) {
    // Create directory
    string output_path = op->get_output_path();
    string path = op->get_code_dir() + op->get_output_relation()->get_name() +
      "_code/";
    string make_file_name = path + "Makefile";
    string create_dir = "mkdir -p " + path;
    std::system(create_dir.c_str());

    // Populate compilation directory with utility headers
    string copy_cmd =
      "cp " + FLAGS_metis_templates_dir + "/utils.h " + path + "; ";
    copy_cmd += "cp " + FLAGS_metis_templates_dir + "/hdfs_utils.h " + path;
    std::system(copy_cmd.c_str());
  }

  void TranslatorMetis::UpdateDAGCode(
       const string& code, MetisJobCode* child_code, bool add_to_reduce,
       MetisJobCode* dag_code) {
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

  MetisJobCode* TranslatorMetis::Translate(AggOperator* op) {
    LOG(ERROR) << "AggOperator [" << op << "] can't be translated: "
               << "unimplemented!";
    return new MetisJobCode(op, "");
  }

  MetisJobCode* TranslatorMetis::Translate(CountOperator* op) {
    LOG(ERROR) << "CountOperator [" << op << "] can't be translated: "
               << "unimplemented!";
    return new MetisJobCode(op, "");
  }

  MetisJobCode* TranslatorMetis::Translate(CrossJoinOperator* op) {
    LOG(ERROR) << "CrossJoinOperator [" << op << "] can't be translated: "
               << "unimplemented!";
    return new MetisJobCode(op, "");
  }

  MetisJobCode* TranslatorMetis::Translate(DifferenceOperator* op) {
    LOG(ERROR) << "DifferenceOperator [" << op << "] can't be translated: "
               << "unimplemented!";
    return new MetisJobCode(op, "");
  }

  MetisJobCode* TranslatorMetis::Translate(DistinctOperator* op) {
    LOG(ERROR) << "DistinctOperator [" << op << "] can't be translated: "
               << "unimplemented!";
    return new MetisJobCode(op, "");
  }

  MetisJobCode* TranslatorMetis::Translate(DivOperator* op) {
    LOG(ERROR) << "DivOperator [" << op << "] can't be translated: "
               << "unimplemented!";
    return new MetisJobCode(op, "");
  }

  MetisJobCode* TranslatorMetis::Translate(IntersectionOperator* op) {
    LOG(ERROR) << "IntersectionOperator [" << op << "] can't be translated: "
               << "unimplemented!";
    return new MetisJobCode(op, "");
  }

  MetisJobCode* TranslatorMetis::Translate(JoinOperator* op) {
    vector<string> input_paths = op->get_input_paths();
    TemplateDictionary dict("join");
    // Set default values so that the generated code compiles.
    PopulateCommonValues(op, &dict);
    // Generate .cc file (with most code)
    dict.SetValue("LEFT_COL_TYPE", op->get_col_left()->translateTypeC());
    dict.SetValue("RIGHT_COL_TYPE", op->get_col_right()->translateTypeC());

    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("LEFT_REL", AvoidNameClash(op, 0));
    dict.SetValue("LEFT_INDEX",
                  boost::lexical_cast<string>(op->get_col_left()->get_index()));
    dict.SetValue("RIGHT_REL", AvoidNameClash(op, 1));
    dict.SetValue("RIGHT_INDEX",
                  boost::lexical_cast<string>(
                      op->get_col_right()->get_index()));
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
    ExpandTemplate(FLAGS_metis_templates_dir + "join_map_variables.cc",
                   ctemplate::DO_NOT_STRIP, &dict, &op_map_variables);
    ExpandTemplate(FLAGS_metis_templates_dir + "join_setup.cc",
                   ctemplate::DO_NOT_STRIP, &dict, &op_setup);
    if (use_mergable_operators_) {
      ExpandTemplate(FLAGS_metis_templates_dir + "join_map.cc",
                     ctemplate::DO_NOT_STRIP, &dict, &op_map);
      ExpandTemplate(FLAGS_metis_templates_dir + "join_reduce.cc",
                     ctemplate::DO_NOT_STRIP, &dict, &op_reduce);
    } else {
      ExpandTemplate(FLAGS_metis_templates_dir + "join_map_simple.cc",
                     ctemplate::DO_NOT_STRIP, &dict, &op_map);
      ExpandTemplate(FLAGS_metis_templates_dir + "join_reduce_simple.cc",
                     ctemplate::DO_NOT_STRIP, &dict, &op_reduce);
    }
    MetisJobCode* job_code =
      new MetisJobCode(op, op_map_variables, op_setup, op_map, "",
                       op_reduce);
    job_code->set_map_key_type("char*");
    job_code->set_map_value_type("char*");
    job_code->set_reduce_key_type("void*");
    job_code->set_reduce_value_type("char*");
    return job_code;
  }

  MetisJobCode* TranslatorMetis::Translate(MaxOperator* op) {
    LOG(ERROR) << "MaxOperator [" << op << "] can't be translated: "
               << "unimplemented!";
    return new MetisJobCode(op, "");
  }

  MetisJobCode* TranslatorMetis::Translate(MinOperator* op) {
    LOG(ERROR) << "MinOperator [" << op << "] can't be translated: "
               << "unimplemented!";
    return new MetisJobCode(op, "");
  }

  MetisJobCode* TranslatorMetis::Translate(MulOperator* op) {
    LOG(ERROR) << "MulOperator [" << op << "] can't be translated: "
               << "unimplemented!";
    return new MetisJobCode(op, "");
  }

  MetisJobCode* TranslatorMetis::Translate(ProjectOperator* op) {
    // TODO(malte): Handle multiple input paths?
    // Project currently only supports a single input
    string input_path = op->get_input_paths()[0];
    string rel_name = AvoidNameClash(op, 0);
    // At the moment we assume all the columns come from a single relation.
    vector<Column*> columns = op->get_columns();
    // Figure out column bitmask
    // N.B. this limits us to 64 columns!
    uint64_t column_bitmap = 0;
    for (vector<Column*>::iterator it = columns.begin();
         it != columns.end(); ++it) {
      column_bitmap |= (1 << (*it)->get_index());
    }
    VLOG(1) << "column bitmap is " << hex << column_bitmap;
    // Generate .cc file (with most code)
    TemplateDictionary dict("project");
    PopulateCommonValues(op, &dict);
    dict.SetIntValue("COLUMN_MASK", column_bitmap);
    dict.SetValue("INPUT_PATH", input_path);
    dict.SetValue("REL_NAME", rel_name);
    dict.SetValue("CONDITION", op->get_condition_tree()->toString("metis"));
    string output_rel = op->get_output_relation()->get_name();
    dict.SetValue("COL_SIZE", boost::lexical_cast<string>(columns.size()));
    dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    dict.SetValue("OUTPUT_REL", output_rel);
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string op_map;
    if (use_mergable_operators_) {
      ExpandTemplate(FLAGS_metis_templates_dir + "project_map.cc",
                     ctemplate::DO_NOT_STRIP, &dict, &op_map);
    } else {
      ExpandTemplate(FLAGS_metis_templates_dir + "project_simple.cc",
                     ctemplate::DO_NOT_STRIP, &dict, &op_map);
    }
    MetisJobCode* job_code = new MetisJobCode(op, "", "", op_map, "", "");
    job_code->set_map_key_type("char*");
    job_code->set_map_value_type("char*");
    return job_code;
  }

  MetisJobCode* TranslatorMetis::Translate(SelectOperator* op) {
    // TODO(malte): Handle multiple input paths?
    // Project currently only supports a single input
    string input_path = op->get_input_paths()[0];
    string rel_name = AvoidNameClash(op, 0);
    // At the moment we assume all the columns come from a single relation.
    vector<Column*> columns = op->get_columns();
    // Figure out column bitmask
    // N.B. this limits us to 64 columns!
    uint64_t column_bitmap = 0;
    for (vector<Column*>::iterator it = columns.begin();
         it != columns.end(); ++it) {
      column_bitmap |= (1 << (*it)->get_index());
    }
    VLOG(1) << "column bitmap is " << hex << column_bitmap;
    // Generate .cc file (with most code)
    TemplateDictionary dict("project");
    PopulateCommonValues(op, &dict);
    dict.SetIntValue("COLUMN_MASK", column_bitmap);
    dict.SetValue("INPUT_PATH", input_path);
    dict.SetValue("REL_NAME", rel_name);
    dict.SetValue("CONDITION", op->get_condition_tree()->toString("metis"));
    string output_rel = op->get_output_relation()->get_name();
    dict.SetValue("COL_SIZE", boost::lexical_cast<string>(columns.size()));
    dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    dict.SetValue("OUTPUT_REL", output_rel);
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string op_map;
    if (use_mergable_operators_) {
      ExpandTemplate(FLAGS_metis_templates_dir + "select_map.cc",
                     ctemplate::DO_NOT_STRIP, &dict, &op_map);
    } else {
      ExpandTemplate(FLAGS_metis_templates_dir + "select_simple.cc",
                     ctemplate::DO_NOT_STRIP, &dict, &op_map);
    }
    MetisJobCode* job_code = new MetisJobCode(op, "", "", op_map, "", "");
    job_code->set_map_key_type("char*");
    job_code->set_map_value_type("char*");
    return job_code;
  }

  MetisJobCode* TranslatorMetis::Translate(SortOperator* op) {
    ofstream make_file;
    vector<string> input_paths = op->get_input_paths();
    string output_path = op->get_output_path();
    string path = op->get_code_dir() + op->get_output_relation()->get_name() +
      "_code/";
    string make_file_name = path + "Makefile";
    string create_dir = "mkdir -p " + path;
    std::system(create_dir.c_str());
    string input_path = op->get_input_paths()[0];
    string rel_name = op->get_relations()[0]->get_name();
    // Generate .cc file (with most code)
    TemplateDictionary dict("sort");
    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("INPUT_PATH", input_path);
    dict.SetValue("OUTPUT_PATH", output_path);
    dict.SetValue("REL_NAME", rel_name);
    // Generate Makefile for this operator
    TemplateDictionary make_file_dict("sort_make");
    make_file_dict.SetValue("METIS_ROOT", FLAGS_metis_dir);
    make_file_dict.SetValue("CLASS_NAME", class_name);
    make_file_dict.SetValue("REL_NAME", rel_name);
    string difference = "";
    ExpandTemplate(FLAGS_metis_templates_dir + "Makefile",
                   ctemplate::DO_NOT_STRIP, &make_file_dict, &difference);
    make_file.open(make_file_name.c_str());
    make_file << difference;
    make_file.close();
    string op_code;
    ExpandTemplate(FLAGS_metis_templates_dir + "sort.cc", ctemplate::DO_NOT_STRIP, &dict,
                   &op_code);
    return new MetisJobCode(op, op_code);
  }

  MetisJobCode* TranslatorMetis::Translate(SubOperator* op) {
    LOG(ERROR) << "SubOperator [" << op << "] can't be translated: "
               << "unimplemented!";
    return new MetisJobCode(op, "");
  }

  MetisJobCode* TranslatorMetis::Translate(SumOperator* op) {
    LOG(ERROR) << "SumOperator [" << op << "] can't be translated: "
               << "unimplemented!";
    return new MetisJobCode(op, "");
  }

  MetisJobCode* TranslatorMetis::Translate(UdfOperator* op) {
    LOG(ERROR) << "UdfOperator [" << op << "] can't be translated: "
               << "unimplemented!";
    return new MetisJobCode(op, "");
  }

  MetisJobCode* TranslatorMetis::Translate(UnionOperator* op) {
    vector<string> input_paths = op->get_input_paths();
    TemplateDictionary dict("union");
    // Set default values so that the generated code compiles.
    PopulateCommonValues(op, &dict);
    // Generate .cc file (with most code)
    PopulateCommonValues(op, &dict);
    dict.SetValue("LEFT_PATH", input_paths[0]);
    dict.SetValue("RIGHT_PATH", input_paths[1]);
    dict.SetValue("OUTPUT_PATH", op->get_output_path());
    dict.SetValue("LEFT_REL", AvoidNameClash(op, 0));
    dict.SetValue("RIGHT_REL", AvoidNameClash(op, 1));
    string output_rel = op->get_output_relation()->get_name();
    dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    dict.SetValue("OUTPUT_REL", output_rel);
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string op_map;
    if (use_mergable_operators_) {
      ExpandTemplate(FLAGS_metis_templates_dir + "union_map.cc",
                     ctemplate::DO_NOT_STRIP, &dict, &op_map);
    } else {
      ExpandTemplate(FLAGS_metis_templates_dir + "union_simple.cc",
                     ctemplate::DO_NOT_STRIP, &dict, &op_map);
    }
    MetisJobCode* job_code = new MetisJobCode(op, "", "", op_map, "", "");
    job_code->set_map_key_type("char*");
    job_code->set_map_value_type("char*");
    return job_code;
  }

  string TranslatorMetis::GenAndCompile(OperatorInterface* op,
                                        const string& op_code) {
    PrepareCodeDirectory(op);
    string source_file = GetSourcePath(op);
    string binary_file = GetBinaryPath(op);
    string path = op->get_code_dir() + op->get_output_relation()->get_name() + "_code/";
    ofstream job_file;
    job_file.open(source_file.c_str());
    job_file << op_code;
    job_file.close();

    // Now generate a Makefile for the whole thing
    string output_path = op->get_output_path();
    string make_file_name = path + "Makefile";
    GenerateMakefile(op, op->get_output_relation()->get_name(), make_file_name);

    //string cur_dir = ExecCmd("pwd");
    //string cur_dir = path;
    //string chd_dir = "cd " + cur_dir;
    //system(chd_dir.c_str());
    LOG(INFO) << "metis build started for: " << class_name;
    timeval start_time, end_time;
    gettimeofday(&start_time, NULL);
    string compile_cmd = "make -C " + path;
    std::system(compile_cmd.c_str());
    gettimeofday(&end_time, NULL);
    uint64_t compile_time = end_time.tv_sec - start_time.tv_sec;
    cout << "COMPILE TIME: " << compile_time << endl;
    LOG(INFO) << "metis build ended for: " << class_name;
    return binary_file;
  }

} // namespace translator
} // namespace musketeer
