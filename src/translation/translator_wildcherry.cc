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

#include "translation/translator_wildcherry.h"

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

#include "base/common.h"
#include "ir/column.h"

namespace musketeer {
namespace translator {

  using ctemplate::TemplateDictionary;

  TranslatorWildCherry::TranslatorWildCherry(const op_nodes& dag,
                                             const string& class_name) :
    TranslatorInterface(dag, class_name) {
  }

  string TranslatorWildCherry::GetPath(OperatorInterface* op) {
    return op->get_code_dir() + "wildcherry_code/";
  }

  string TranslatorWildCherry::GetBinaryPath(OperatorInterface* op) {
    return GetPath(op) + "bin/" + op->get_type_string() +
      op->get_output_relation()->get_name();
  }

  string TranslatorWildCherry::GetSourceFile(OperatorInterface* op) {
    return op->get_type_string() + op->get_output_relation()->get_name() + ".c";
  }

  string TranslatorWildCherry::GetSourcePath(OperatorInterface* op) {
    return GetPath(op) + GetSourceFile(op);
  }

  void TranslatorWildCherry::print_op_node(shared_ptr<OperatorNode> cur_node,
                                           int indent) {
    for (int i = 0; i < indent; i++) {
        cout << " ";
    }

    vector<string> inputs = cur_node->get_operator()->get_input_paths();
    cout << "[";
    for (vector<string>::iterator it = inputs.begin(); it != inputs.end(); ++it) {
      cout << (*it);
      if (it != --inputs.end()) {
        cout << ",";
      }
    }
    cout << "]";
    cout << " --> " << cur_node->get_operator()->get_type_string() << " --> ["
         << cur_node->get_operator()->get_output_path() << "]";
    if (cur_node->get_operator()->get_rename()) {
        cout << "[renamed]";
    }
    cout << endl;
  }

  string TranslatorWildCherry::IterateOps(vector<shared_ptr<OperatorNode> >& nodes,
                                          int indent) {
    string result = "";
    //Iterate over the DAG
    for (op_nodes::iterator it = nodes.begin(); it != nodes.end(); ++it) {
        //This is a nicer alias
        shared_ptr<OperatorNode> node = (*it);
        //Figure out the outputs
        string output_path = node->get_operator()->get_output_path();

        if (node->IsLeaf()) {
            _hdfs_outputs.insert(output_path);
        } else {
            _intermediates.insert(output_path);
        }

        //Figure out the inputs
        std::vector<std::string> input_paths =
          node->get_operator()->get_input_paths();
        for (vector<string>::iterator it2 = input_paths.begin();
             it2 != input_paths.end(); ++it2) {
          // If this input is not an intermediate, and it's not an output,
          //then it must be a fresh inpuit
          if (_hdfs_outputs.find(*it2) == _hdfs_outputs.end() &&
             _intermediates.find(*it2) == _intermediates.end()) {
            _hdfs_inputs.insert(*it2);
          }
        }
        if (_visited.insert(*it).second) {
            _to_visit.push(*it);
        }
        result += GenerateOp(indent);
    }
    return result;
}

  string TranslatorWildCherry::GenerateOp(int indent) {
    string result = "";

    //Traverse the DAG
    while (!_to_visit.empty()) {
      shared_ptr<OperatorNode> cur_node = _to_visit.front();
      _to_visit.pop();

      //Translate the operator
      print_op_node(cur_node, indent);
      JobCode* jc = TranslateOperator(cur_node->get_operator());
      string source_file = GetSourcePath(cur_node->get_operator());

      //While loops are special as they are part of the bash script
      if (cur_node->get_operator()->get_type() == WHILE_OP) {
        result += jc->get_code();
      } else {
        //Dump the translated operator out to the source file
        //LOG(INFO) << "Writing source out to " << source_file << " ...";
        ofstream job_file;
        job_file.open(source_file.c_str());
        job_file << jc->get_code();
        job_file.close();

        // Include the source file in the build script and the add the binary
        // path to the run script
        result += "echo \"" + GetBinaryPath(cur_node->get_operator()) + "\";" +
          "time " + GetBinaryPath(cur_node->get_operator()) + " \n";
        // If the output was renamed, we need to move it back
        if (cur_node->get_operator()->get_rename()) {
          string output_path_tmp =
            GenerateTmpPath(cur_node->get_operator()->get_output_path());
          string output_path =
            output_path_tmp.substr(0, output_path_tmp.size() - 7) + ".in";
          result += "mv " + output_path_tmp + " " + output_path + "\n";
        }
      }

      if (!cur_node->IsLeaf()) {
        op_nodes loop_children = cur_node->get_loop_children();
        if (loop_children.size() > 0) {
          //If there are children, we must be in a while loop
          result += "; do \n";
          result += IterateOps(loop_children, indent + 4);
          result += "let COUNTER=COUNTER+1 \n  done;\n";
          //result += "done;\n";
        }
        op_nodes children = cur_node->get_children();
        result += IterateOps(children, indent);
      }
    }
    return result;
  }

  string TranslatorWildCherry::GenerateCode() {
    LOG(INFO) << "Wildcherry generate code";
    OperatorInterface* hack_op = dag[0]->get_operator();  //XXX HACK!

    //Get the code directory ready for some operators
    PrepareCodeDirectory(hack_op); //XXX HACK!

    _bash_script = "#! /bin/bash \n\n#Pulling data in from HDFS\n";
    _hdfs_inputs.clear();
    _hdfs_outputs.clear();
    _intermediates.clear();
    _visited.clear();

    //Travese the DAG
    std::cout << "<-------------------------------------------------->" << endl;
    string to_run = IterateOps(dag, 0);
    std::cout << "<-------------------------------------------------->" << endl;

    //Add inputs to the bash script
    for (set<string>::iterator it = _hdfs_inputs.begin();
        it != _hdfs_inputs.end(); ++it) {
      //_bash_script += "time " +  GetPath(hack_op) + "bin/hdfs_copier pull " +
      // (*it) + "\n"; //XXX HACK!
      _bash_script += "echo hadoop fs -get " + (*it) + " " +
        hack_op->get_input_dir() + "\n"; //XXX HACK!
      _bash_script += "time hadoop fs -get " + (*it) + " " +
        hack_op->get_input_dir() + "\n"; //XXX HACK!
      _bash_script += "echo mv " + (*it) + "*.in " +
        hack_op->get_input_dir() + "\n"; //XXX HACK!
      _bash_script += "time mv " + (*it) + "*.in " + hack_op->get_input_dir() +
        "\n"; //XXX HACK!
    }

    _bash_script += "\n#Running operators\n";
    _bash_script += to_run;

    //    cout << "Intermediates for local disk:\n";
    //    for(set<string>::iterator it = _intermediates.begin(); it != _intermediates.end(); ++it) {
    //        cout << *it << ",";
    //    }
    //    cout << endl;

    _bash_script += "\n#Pushing data back to HDFS\n";
    for (set<string>::iterator it = _hdfs_outputs.begin();
        it != _hdfs_outputs.end(); ++it) {
      // _bash_script += "time " +  GetPath(hack_op) + "bin/hdfs_copier push " +
      // GenerateTmpPath(*it) + " " +  (*it) + "\n";
      _bash_script += "echo time hadoop fs -mkdir " + *it + "\n";
      _bash_script += "time hadoop fs -mkdir " + *it + "\n";
      _bash_script += "echo hadoop fs -put " + GenerateTmpPath(*it) + " " +
        (*it) + "\n";
      _bash_script += "time hadoop fs -put " + GenerateTmpPath(*it) + " " +
        (*it) + "\n";
    }

    CompileAll(hack_op); //XXX HACK!

    string bash_out = GetPath(hack_op) + "wildcherry.sh";
    LOG(INFO) << "Writing bash out to " << bash_out << " ...";
    ofstream bash_file;
    bash_file.open(bash_out.c_str());
    bash_file << _bash_script;
    bash_file.close();

    LOG(INFO) << "GenerateCode complete (" << bash_out << ")";
    return bash_out;
  }

  JobCode* TranslatorWildCherry::Translate(AggOperator* op) {
    string input_path = op->get_input_paths()[0];
    string output_path = op->get_output_path();
    TemplateDictionary dict("agg");
    dict.SetValue("LOCAL_INPUT_PATH", GenerateTmpPath(input_path));
    dict.SetValue("LOCAL_OUTPUT_PATH", GenerateTmpPath(output_path));

    vector<Column*> columns = op->get_group_bys();
    string get_columns;
    int index = 0;
    for (vector<Column*>::iterator it = columns.begin(); it != columns.end();
         ++it) {
      get_columns += "{ " + boost::lexical_cast<string>(index) + ", " +
        boost::lexical_cast<string>((*it)->get_index()) + ", NULL, NULL}, ";
      index++;
    }
    dict.SetValue("AGG_COLS_IN_COUNT",
                  boost::lexical_cast<string>(columns.size()));
    dict.SetValue("AGG_COLS_IN", get_columns);
    dict.SetValue("IS_COUNT", "0");
    dict.SetValue("AGG_OP", op->get_operator());
    // TODO(matt): Add support for multiple columns agg.
    dict.SetValue("AGG_COL",
                  boost::lexical_cast<string>(op->get_columns()[0]->get_index()) );

    string op_code;
    ExpandTemplate(FLAGS_wildcherry_templates_dir + "wildcherry_agg.c",
                   ctemplate::DO_NOT_STRIP, &dict, &op_code);
    JobCode* job_code = new JobCode(op, op_code);
    return job_code;
  }

  JobCode* TranslatorWildCherry::Translate(CountOperator* op) {
    LOG(INFO) << "Translating Wildcherry Count Operator...";
    string input_path = op->get_input_paths()[0];
    string output_path = op->get_output_path();
    string rel_name = AvoidNameClash(op, 0);
    TemplateDictionary dict("count");

    dict.SetValue("LOCAL_INPUT_PATH", GenerateTmpPath(input_path));
    dict.SetValue("LOCAL_OUTPUT_PATH", GenerateTmpPath(output_path));

    string output_rel = op->get_output_relation()->get_name();
    uint32_t index = 0;
    string get_columns = "";
    get_columns += "{ " + boost::lexical_cast<string>(index) + ", " +
      boost::lexical_cast<string>(op->get_group_bys()[0]->get_index()) + ", NULL, NULL}, ";

    dict.SetValue("AGG_COLS_IN", get_columns);
    dict.SetValue("AGG_COLS_IN_COUNT", "1");
    dict.SetValue("IS_COUNT", "1");
    dict.SetValue("AGG_OP", "+");
    dict.SetValue("AGG_COL", "1");
    string op_code;
    ExpandTemplate(FLAGS_wildcherry_templates_dir + "wildcherry_agg.c",
                   ctemplate::DO_NOT_STRIP, &dict, &op_code);
    JobCode* job_code = new JobCode(op, op_code);
    //std::cout << job_code->get_code() << std::endl;
    return job_code;
  }

  JobCode* TranslatorWildCherry::Translate(CrossJoinOperator* op)  {
    LOG(ERROR) << __FUNCTION__ << ": Not implmented\n";
    // TODO(ionel): Implement.
    return NULL;
  }

  JobCode* TranslatorWildCherry::Translate(DifferenceOperator* op) {
    LOG(ERROR) << __FUNCTION__ << ": Not implmented\n";
    // vector<string> input_paths = op->get_input_paths();
    // TemplateDictionary dict("difference");
    // dict.SetValue("CLASS_NAME", class_name);
    // dict.SetValue("LEFT_PATH", input_paths[0]);
    // dict.SetValue("RIGHT_PATH", input_paths[1]);
    // dict.SetValue("OUTPUT_PATH", op->get_output_path());
    // dict.SetValue("LEFT_REL", AvoidNameClash(op, 0));
    // dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    // dict.SetValue("OUTPUT_REL", op->get_output_relation()->get_name());
    // dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    // string op_code;
    // ExpandTemplate(FLAGS_wildcherry_templates_dir + "wildcherry_difference.c",
    //                ctemplate::DO_NOT_STRIP, &dict, &op_code);
    // JobCode* job_code = new JobCode(op, op_code);
    // return job_code;
    return  NULL;
  }

  JobCode* TranslatorWildCherry::Translate(DistinctOperator* op) {
    LOG(ERROR) << __FUNCTION__ << ": Not implmented\n";
    return NULL;
  }

  JobCode* TranslatorWildCherry::Translate(DivOperator* op) {
    string input_path = GenerateTmpPath(op->get_input_paths()[0]);
    string output_path = GenerateTmpPath(op->get_output_path());

    TemplateDictionary dict("div");
    dict.SetValue("IN_FILE", input_path);
    dict.SetValue("OUT_FILE", output_path);
    dict.SetValue("EVAL_SUM", "1");
    dict.SetValue("OPR", "/");

    //Turn off features for select operator behavior
    dict.SetValue("EVAL_EXPR", "0");
    dict.SetValue("OUT_COL_COUNT", "0");
    dict.SetValue("OUT_COLS", "");
    dict.SetValue("EXPR", "1");

    vector<Value*> values = op->get_values();
    Column* left_column = dynamic_cast<Column*>(values[0]);
    Column* right_column = dynamic_cast<Column*>(values[1]);

    if (right_column != NULL) {
      dict.SetValue("RHS", "COLI(" +
                    boost::lexical_cast<string>(right_column->get_index()) + ")");
      dict.SetValue("ASSIGN_SUM", "COLI(" +
                    boost::lexical_cast<string>(right_column->get_index()) + ")");
      dict.SetValue("UPDATED", "COL(" +
                    boost::lexical_cast<string>(right_column->get_index()) + ")->updated");
    } else {
      dict.SetValue("RHS", values[1]->get_value());
    }

    if (left_column != NULL) {
      dict.SetValue("LHS", "COLI(" +
                    boost::lexical_cast<string>(left_column->get_index()) + ")");
      dict.SetValue("ASSIGN_SUM", "COLI(" +
                    boost::lexical_cast<string>(left_column->get_index()) + ")");
      dict.SetValue("UPDATED", "COL(" +
                    boost::lexical_cast<string>(left_column->get_index()) + ")->updated");
    } else {
      dict.SetValue("LHS", values[0]->get_value());
    }

    string cond = op->get_condition_tree()->toString("wildcherry");
    if (cond != "") {
      dict.SetValue("COND", cond);
    } else {
      dict.SetValue("COND", "1");
    }

    string op_code;
    ExpandTemplate(FLAGS_wildcherry_templates_dir + "wildcherry_sum.c",
                   ctemplate::DO_NOT_STRIP, &dict, &op_code);
    JobCode* job_code = new JobCode(op, op_code);
    return job_code;
  }

  JobCode* TranslatorWildCherry::Translate(IntersectionOperator* op) {
    LOG(ERROR) << __FUNCTION__ << ": Not implmented\n";
    // vector<string> input_paths = op->get_input_paths();
    // TemplateDictionary dict("intersection");
    // dict.SetValue("CLASS_NAME", class_name);
    // dict.SetValue("LEFT_PATH", input_paths[0]);
    // dict.SetValue("RIGHT_PATH", input_paths[1]);
    // dict.SetValue("OUTPUT_PATH", op->get_output_path());
    // dict.SetValue("LEFT_REL", AvoidNameClash(op));
    // dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    // dict.SetValue("OUTPUT_REL", op->get_output_relation()->get_name());
    // dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    // string op_code;
    // ExpandTemplate(FLAGS_wildcherry_templates_dir + "wildcherry_intersection.c",
    //                ctemplate::DO_NOT_STRIP, &dict, &op_code);
    // JobCode* job_code = new HadoopJobCode(op, op_code);
    // return job_code;
    return  NULL;
  }

  JobCode* TranslatorWildCherry::Translate(JoinOperator* op) {
    LOG(INFO) << "Translating Wildcherry Join Operator...";
    string input_path_left = op->get_input_paths()[0];
    string input_path_right = op->get_input_paths()[1];
    Column* column_left = op->get_col_left();
    Column* column_right = op->get_col_right();
    string rel_name = AvoidNameClash(op, 0);
    string output_path = op->get_output_path();

    TemplateDictionary dict("count");

    dict.SetValue("IN_FILE_LEFT", GenerateTmpPath(input_path_left));
    dict.SetValue("IN_FILE_RIGHT", GenerateTmpPath(input_path_right));
    dict.SetValue("OUT_FILE", GenerateTmpPath(output_path));
    dict.SetValue("COL_ID_LEFT",
                  boost::lexical_cast<string>(column_left->get_index()) );
    dict.SetValue("COL_ID_RIGHT",
                  boost::lexical_cast<string>(column_right->get_index()) );
    string op_code;
    ExpandTemplate(FLAGS_wildcherry_templates_dir + "wildcherry_join.c",
                   ctemplate::DO_NOT_STRIP, &dict, &op_code);
    JobCode* job_code = new JobCode(op, op_code);
    //std::cout << job_code->get_code() << std::endl;
    return job_code;
  }

  JobCode* TranslatorWildCherry::Translate(MaxOperator* op)  {
    LOG(ERROR) << __FUNCTION__ << ": Not implmented\n";
    // TODO(ionel): Implement.
    return NULL;
  }

  JobCode* TranslatorWildCherry::Translate(MinOperator* op)  {
    LOG(ERROR) << __FUNCTION__ << ": Not implmented\n";
    // TODO(ionel): Implement.
    return NULL;
  }

  JobCode* TranslatorWildCherry::Translate(MulOperator* op)  {
    string input_path = GenerateTmpPath(op->get_input_paths()[0]);
    string output_path = GenerateTmpPath(op->get_output_path());

    TemplateDictionary dict("mul");
    dict.SetValue("IN_FILE", input_path);
    dict.SetValue("OUT_FILE", output_path);
    dict.SetValue("EVAL_SUM", "1");
    dict.SetValue("OPR", "*");
    //Turn off features for select operator behavior
    dict.SetValue("EVAL_EXPR", "0");
    dict.SetValue("OUT_COL_COUNT", "0");
    dict.SetValue("OUT_COLS", "");
    dict.SetValue("EXPR", "1");

    vector<Value*> values = op->get_values();
    Column* left_column = dynamic_cast<Column*>(values[0]);
    Column* right_column = dynamic_cast<Column*>(values[1]);

    if (right_column != NULL) {
      dict.SetValue("RHS",
                    "COLI(" + boost::lexical_cast<string>(right_column->get_index()) + ")");
      dict.SetValue("ASSIGN_SUM",
                    "COLI(" + boost::lexical_cast<string>(right_column->get_index()) + ")");
      dict.SetValue("UPDATED",
                    "COL(" + boost::lexical_cast<string>(right_column->get_index()) + ")->updated");
    } else {
      dict.SetValue("RHS", values[1]->get_value());
    }

    if (left_column != NULL) {
      dict.SetValue("LHS", "COLI(" + boost::lexical_cast<string>(left_column->get_index()) + ")");
      dict.SetValue("ASSIGN_SUM",
                    "COLI(" + boost::lexical_cast<string>(left_column->get_index()) + ")");
      dict.SetValue("UPDATED",
                    "COL(" + boost::lexical_cast<string>(left_column->get_index()) + ")->updated");
    } else {
      dict.SetValue("LHS", values[0]->get_value());
    }

    string cond = op->get_condition_tree()->toString("wildcherry");
    if (cond != "") {
      dict.SetValue("COND", cond);
    } else {
      dict.SetValue("COND", "1");
    }

    string op_code;
    ExpandTemplate(FLAGS_wildcherry_templates_dir + "wildcherry_sum.c",
                   ctemplate::DO_NOT_STRIP, &dict, &op_code);
    JobCode* job_code = new JobCode(op, op_code);
    return job_code;
    return NULL;
  }

  JobCode* TranslatorWildCherry::Translate(ProjectOperator* op) {
    string input_path = GenerateTmpPath(op->get_input_paths()[0]);
    string output_path = GenerateTmpPath(op->get_output_path());
    string output_rel = op->get_output_relation()->get_name();
    uint32_t index = 0;
    string get_columns = "";

    vector<Column*> columns = op->get_columns();
    for (vector<Column*>::iterator it = columns.begin(); it != columns.end();
         ++it) {
      get_columns += "{ " + boost::lexical_cast<string>(index) + ", " +
        boost::lexical_cast<string>((*it)->get_index()) + ", NULL, NULL}, ";
      index++;
    }

    TemplateDictionary dict("project");
    dict.SetValue("LOCAL_INPUT_PATH", input_path);
    dict.SetValue("LOCAL_OUTPUT_PATH", output_path);
    dict.SetValue("PROJ_COLS", get_columns);
    dict.SetValue("COL_SIZE",
                  boost::lexical_cast<string>(op->get_columns().size()));

    string op_code;
    ExpandTemplate(FLAGS_wildcherry_templates_dir + "wildcherry_project.c",
                   ctemplate::DO_NOT_STRIP, &dict, &op_code);
    JobCode* job_code = new JobCode(op, op_code);
    return job_code;
  }

  JobCode* TranslatorWildCherry::Translate(SelectOperator* op)  {
    string output_path = GenerateTmpPath(op->get_output_path());
    string input_path  = GenerateTmpPath(op->get_input_paths()[0]);
    uint32_t index = 0;
    string get_columns = "";
    vector<Column*> columns = op->get_columns();
    for (vector<Column*>::iterator it = columns.begin(); it != columns.end();
         ++it) {
      get_columns += boost::lexical_cast<string>((*it)->get_index()) + ", ";
      index++;
    }

    TemplateDictionary dict("select");
    dict.SetValue("IN_FILE",  input_path);
    dict.SetValue("OUT_FILE", output_path);
    dict.SetValue("EVAL_SUM", "0");
    dict.SetValue("OPR", "+");

    //Turn off features for select operator behavior
    dict.SetValue("EVAL_EXPR", "1");
    dict.SetValue("OUT_COL_COUNT", boost::lexical_cast<string>(columns.size()));
    dict.SetValue("OUT_COLS", get_columns);
    dict.SetValue("EXPR", op->get_condition_tree()->toString("wildcherry"));

    string op_code;
    ExpandTemplate(FLAGS_wildcherry_templates_dir + "wildcherry_sum.c",
                   ctemplate::DO_NOT_STRIP, &dict, &op_code);
    JobCode* job_code = new JobCode(op, op_code);
    return job_code;
  }

  JobCode* TranslatorWildCherry::Translate(SortOperator* op) {
    LOG(ERROR) << __FUNCTION__ << ": Not implmented\n";
    // TODO(ionel): Implement.
    return NULL;
  }

  JobCode* TranslatorWildCherry::Translate(SubOperator* op) {
    string input_path = GenerateTmpPath(op->get_input_paths()[0]);
    string output_path = GenerateTmpPath(op->get_output_path());

    TemplateDictionary dict("sub");
    dict.SetValue("IN_FILE", input_path);
    dict.SetValue("OUT_FILE", output_path);
    dict.SetValue("EVAL_SUM", "1");
    dict.SetValue("OPR", "-");

    //Turn off features for select operator behavior
    dict.SetValue("EVAL_EXPR", "0");
    dict.SetValue("OUT_COL_COUNT", "0");
    dict.SetValue("OUT_COLS", "");
    dict.SetValue("EXPR", "1");

    vector<Value*> values = op->get_values();
    Column* left_column = dynamic_cast<Column*>(values[0]);
    Column* right_column = dynamic_cast<Column*>(values[1]);

    if (right_column != NULL) {
      dict.SetValue("RHS", "COLI(" + boost::lexical_cast<string>(right_column->get_index()) + ")");
      dict.SetValue("ASSIGN_SUM",
                    "COLI(" + boost::lexical_cast<string>(right_column->get_index()) + ")");
      dict.SetValue("UPDATED",
                    "COL(" + boost::lexical_cast<string>(right_column->get_index()) + ")->updated");
    } else {
      dict.SetValue("RHS", values[1]->get_value());
    }

    if (left_column != NULL) {
      dict.SetValue("LHS", "COLI(" + boost::lexical_cast<string>(left_column->get_index()) + ")");
      dict.SetValue("ASSIGN_SUM",
                    "COLI(" + boost::lexical_cast<string>(left_column->get_index()) + ")");
      dict.SetValue("UPDATED",
                    "COL(" + boost::lexical_cast<string>(left_column->get_index()) + ")->updated");
    } else {
      dict.SetValue("LHS", values[0]->get_value());
    }

    string cond = op->get_condition_tree()->toString("wildcherry");
    if (cond != "") {
      dict.SetValue("COND", cond);
    } else {
      dict.SetValue("COND", "1");
    }

    string op_code;
    ExpandTemplate(FLAGS_wildcherry_templates_dir + "wildcherry_sum.c",
                   ctemplate::DO_NOT_STRIP, &dict, &op_code);
    JobCode* job_code = new JobCode(op, op_code);
    return job_code;
    return NULL;
  }

  JobCode* TranslatorWildCherry::Translate(SumOperator* op) {
    string input_path = GenerateTmpPath(op->get_input_paths()[0]);
    string output_path = GenerateTmpPath(op->get_output_path());
    TemplateDictionary dict("sum");
    dict.SetValue("IN_FILE", input_path);
    dict.SetValue("OUT_FILE", output_path);
    dict.SetValue("EVAL_SUM", "1");
    dict.SetValue("OPR", "+");

    //Turn off features for select operator behavior
    dict.SetValue("EVAL_EXPR", "0");
    dict.SetValue("OUT_COL_COUNT", "0");
    dict.SetValue("OUT_COLS", "");
    dict.SetValue("EXPR", "1");

    vector<Value*> values = op->get_values();
    Column* left_column = dynamic_cast<Column*>(values[0]);
    Column* right_column = dynamic_cast<Column*>(values[1]);

    if (right_column != NULL) {
      dict.SetValue("RHS", "COLI(" + boost::lexical_cast<string>(right_column->get_index()) + ")");
      dict.SetValue("ASSIGN_SUM",
                    "COLI(" + boost::lexical_cast<string>(right_column->get_index()) + ")");
      dict.SetValue("UPDATED",
                    "COL(" + boost::lexical_cast<string>(right_column->get_index()) + ")->updated");
    } else {
      dict.SetValue("RHS", values[1]->get_value());
    }

    if (left_column != NULL) {
      dict.SetValue("LHS", "COLI(" + boost::lexical_cast<string>(left_column->get_index()) + ")");
      dict.SetValue("ASSIGN_SUM",
                    "COLI(" + boost::lexical_cast<string>(left_column->get_index()) + ")");
      dict.SetValue("UPDATED",
                    "COL(" + boost::lexical_cast<string>(left_column->get_index()) + ")->updated");
    } else {
      dict.SetValue("LHS", values[0]->get_value());
    }

    string cond = op->get_condition_tree()->toString("wildcherry");
    if (cond != "") {
      dict.SetValue("COND", cond);
    } else {
      dict.SetValue("COND", "1");
    }

    string op_code;
    ExpandTemplate(FLAGS_wildcherry_templates_dir + "wildcherry_sum.c",
                   ctemplate::DO_NOT_STRIP, &dict, &op_code);
    JobCode* job_code = new JobCode(op, op_code);
    return job_code;
    return NULL;
  }

  JobCode* TranslatorWildCherry::Translate(UdfOperator* op) {
    LOG(ERROR) << __FUNCTION__ << ": Not implmented\n";
    // TODO(ionel): Implement.
    return NULL;
  }

  string TranslatorWildCherry::GenerateTmpPath(const string& path) {
    return path.substr(0, path.size() - 1) + ".in";
  }

  JobCode* TranslatorWildCherry::Translate(UnionOperator* op) {
    LOG(ERROR) << __FUNCTION__ << ": Not implmented\n";
    // vector<string> input_paths = op->get_input_paths();
    // TemplateDictionary dict("union");
    // dict.SetValue("CLASS_NAME", class_name);
    // dict.SetValue("LEFT_PATH", input_paths[0]);
    // dict.SetValue("RIGHT_PATH", input_paths[1]);
    // dict.SetValue("OUTPUT_PATH", op->get_output_path());
    // dict.SetValue("OUTPUT_CODE", "{{OUTPUT_CODE}}");
    // dict.SetValue("OUTPUT_REL", op->get_output_relation()->get_name());
    // dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    // string op_code;
    // ExpandTemplate(FLAGS_wildcherry_templates_dir + "wildcherry_union.c",
    //                ctemplate::DO_NOT_STRIP, &dict, &op_code);
    // JobCode* job_code = new JobCode(op, op_code);
    // return job_code;
    return NULL;
  }

  JobCode* TranslatorWildCherry::Translate(WhileOperator* op)  {
    string input_path = GenerateTmpPath(op->get_input_dir() + "iter/");
    // TODO(ionel): Get the number of iterations the while is supposed to do.
    string op_code = "COUNTER=0 \n while [  \"$COUNTER\" -lt 1 ]";
    //ExpandTemplate(FLAGS_wildcherry_templates_dir + "wildcherry_project.c",
    // ctemplate::DO_NOT_STRIP, &dict, &op_code);
    JobCode* job_code = new JobCode(op, op_code);
    return job_code;
  }

  string TranslatorWildCherry::GenerateGroupByKey(
      const vector<Column*>& group_bys) {
    // TODO(ionel): Implement.
    return "";
  }

  // Create directory
  void TranslatorWildCherry::PrepareCodeDirectory(OperatorInterface* op) {
    string output_path = op->get_output_path();
    string path = op->get_code_dir() + "wildcherry_code/";
    string create_dir = "mkdir -p " + path;
    std::system(create_dir.c_str());

    // Populate compilation directory with utility headers
    string copy_cmd  = "cp -r " + FLAGS_wildcherry_templates_dir +
      "datastructs " + path + "; ";
    copy_cmd += "cp -r " + FLAGS_wildcherry_templates_dir + "fileio " + path +
      "; ";
    copy_cmd += "cp -r " + FLAGS_wildcherry_templates_dir + "libchaste " +
      path + "; ";
    copy_cmd += "cp -r " + FLAGS_wildcherry_templates_dir + "hdfs_copier.cc " +
      path + "; ";
    copy_cmd += "cp -r " + FLAGS_wildcherry_templates_dir +
      "musketeer_build.sh " + path + "; ";
    std::cout << copy_cmd << std::endl;
    std::system(copy_cmd.c_str());
  }

  void TranslatorWildCherry::CompileAll(OperatorInterface* op) {
    LOG(INFO) << "Wildcherry build started for: " << class_name;
    timeval start_time, end_time;
    gettimeofday(&start_time, NULL);

    //Run the build script
    string compile_cmd =  "cd " + GetPath(op) + "; ./musketeer_build.sh " +
      " ~/Musketeer/ext/cake-git/";
    std::cout << compile_cmd << std::endl;
    std::system(compile_cmd.c_str());

    gettimeofday(&end_time, NULL);
    uint64_t compile_time = end_time.tv_sec - start_time.tv_sec;
    cout << "COMPILE TIME: " << compile_time << endl;
    LOG(INFO) << "Wildcherry build ended for: " << class_name;
  }

} // namespace translator
} // namespace musketeer
