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

#include "translation/translator_naiad.h"

#include <boost/lexical_cast.hpp>
#include <ctemplate/template.h>
#include <sys/time.h>

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <utility>

#include "base/common.h"
#include "base/flags.h"

namespace musketeer {
namespace translator {

  using ctemplate::mutable_default_template_cache;

  TranslatorNaiad::TranslatorNaiad(const op_nodes& dag,
                                   const string& class_name):
    TranslatorInterface(dag, class_name) {
    cur_job_code = NULL;
  }

  string TranslatorNaiad::GetBinaryPath(OperatorInterface* op) {
    return FLAGS_generated_code_dir + "Musketeer/bin/Release/Musketeer.exe";
  }

  string TranslatorNaiad::GetSourcePath(OperatorInterface* op) {
    return FLAGS_generated_code_dir + "Musketeer/" + class_name + ".cs";
  }

  string TranslatorNaiad::GetBinaryPath() {
    return FLAGS_generated_code_dir + "Musketeer/bin/Release/Musketeer.exe";
  }

  string TranslatorNaiad::GetSourcePath() {
    return FLAGS_generated_code_dir + "Musketeer/" + class_name + ".cs";
  }

  string TranslatorNaiad::GenerateCode() {
    TemplateDictionary dict("job");
    set<string> inputs = set<string>();
    vector<Relation*> input_relations = *DetermineInputs(dag, &inputs);
    string read_code = GenerateReadCode(input_relations);
    string input_vars_declr;
    string input_vars;
    string on_completed_master;
    string on_completed_worker;
    for (vector<Relation*>::iterator it = input_relations.begin();
         it != input_relations.end(); ++it) {
      string input_rel_name = (*it)->get_name();
      input_vars_declr += "var " + input_rel_name +
        "_input = new BatchedDataSource<string>();\n";
      input_vars += "var " + (*it)->get_name() + " = computation.NewInput(" +
        input_rel_name + "_input).SelectMany(line => read_" +
        input_rel_name + "(line));\n";
      on_completed_master += input_rel_name + "_input.OnCompleted(\"" +
        FLAGS_tmp_data_dir + FLAGS_hdfs_input_dir + input_rel_name + "/" +
        input_rel_name + "\" + computation.Configuration.ProcessID + " +
        "\".in\");\n";
      // on_completed_worker += input_rel_name + "_input.OnNext();\n";
      // on_completed_worker += input_rel_name + "_input.OnCompleted();\n";
      on_completed_worker += input_rel_name + "_input.OnCompleted(\"" +
        FLAGS_tmp_data_dir + FLAGS_hdfs_input_dir + input_rel_name + "/" +
        input_rel_name + "\" + computation.Configuration.ProcessID + " +
        "\".in\");\n";
    }
    string operators_code;
    string fun_code;
    string compactor_code;
    set<string> processed;
    map<string, Relation*> name_rel;
    MarkImmutable(dag, &processed, &name_rel);
    set<shared_ptr<OperatorNode> > leaves = set<shared_ptr<OperatorNode> >();
    TranslateDAG(&operators_code, &fun_code, &compactor_code, dag, &leaves, &inputs);
    pair<string, string> output_code = GenerateOutputCode(leaves);
    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("STRUCTS_CODE", GenerateStructsCode());
    dict.SetValue("INPUT_VARS", input_vars_declr + input_vars);
    dict.SetValue("ON_COMPLETED_MASTER", on_completed_master);
    dict.SetValue("ON_COMPLETED_WORKER", on_completed_worker);
    dict.SetValue("READ_CODE", read_code);
    dict.SetValue("CODE", operators_code);
    dict.SetValue("OUTPUT_CODE", output_code.first);
    dict.SetValue("FUN_CODE", fun_code);
    dict.SetValue("COMPACTOR_CODE", compactor_code);
    dict.SetValue("OUTPUT_CLOSE_CODE", output_code.second);
    dict.SetValue("VARS_DECLARATION",
                  DeclareVariables(input_relations, processed, name_rel));
    string job_code;
    ExpandTemplate(FLAGS_naiad_templates_dir + "JobTemplate.cs", ctemplate::DO_NOT_STRIP,
                   &dict, &job_code);
    return Compile(job_code, input_relations, leaves);
  }

  string TranslatorNaiad::DeclareVariables(vector<Relation*> input_rels,
                                           const set<string>& processed,
                                           const map<string, Relation*>& name_rel) {
    string vars_declaration = "";
    set<string> input_rels_name;
    for (vector<Relation*>::iterator it = input_rels.begin();
         it != input_rels.end(); ++it) {
      input_rels_name.insert((*it)->get_name());
    }
    for (set<string>::const_iterator it = processed.begin(); it != processed.end();
         ++it) {
      if (input_rels_name.find(*it) == input_rels_name.end()) {
        Relation* rel = name_rel.find(*it)->second;
        if (rel->get_columns().size() > 0) {
          vars_declaration += "Stream<" + GenerateType(rel) +
            ", Epoch> " + *it + " = null;\n";
        }
      }
    }
    return vars_declaration;
  }

  string TranslatorNaiad::GenerateStructsCode() {
    const string hash_multipliers[] = {"1234347", "4311", "42", "17", "13", "2", "23", "57", "53",
                                       "113", "3", "7", "5", "1", "11", "19", "29"};
    string structs_code = "";
    for (unordered_map<string, vector<uint16_t>>::iterator it = unique_generated_types_.begin();
         it != unique_generated_types_.end(); ++it) {
      TemplateDictionary dict("Struct");
      dict.SetValue("TYPE", it->first);
      int32_t index = 0;
      string fields = "";
      string constructor = "";
      string hash = "";
      string to_string_args = "";
      for (vector<uint16_t>::iterator t_it = it->second.begin(); t_it != it->second.end();
           ++t_it, ++index) {
        fields += "    public " + Column::stringTypeCSharp(*t_it) + " " +
          Column::indexString(index) + ";\n";
        TemplateDictionary* constr_dict = dict.AddSectionDictionary("CONSTRUCTOR_ARGS");
        string arg_name = Column::indexString(index);
        constr_dict->SetValue("ARG_TYPE", Column::stringTypeCSharp(*t_it));
        constr_dict->SetValue("ARG_NAME", arg_name);
        constructor += "      this." + arg_name + " = " + arg_name + ";\n";
        if (index == 0) {
          hash += arg_name + Column::stringHashCSharp(*t_it);
        } else {
          hash += " + " + hash_multipliers[index - 1] + " * " + arg_name +
            Column::stringHashCSharp(*t_it);
        }
        to_string_args += ", " + arg_name;
        TemplateDictionary* equals_dict = dict.AddSectionDictionary("EQUALS");
        equals_dict->SetValue("EQUAL_TEST", Column::stringEqualsCSharp(*t_it, index));
        TemplateDictionary* compare_dict = dict.AddSectionDictionary("COMPARE_TO");
        compare_dict->SetValue("COMPARE_TEST", Column::stringCompareToCSharp(*t_it, index));
        TemplateDictionary* to_string_dict = dict.AddSectionDictionary("TO_STRING_FORMAT");
        to_string_dict->SetValue("TO_STRING_FORMAT_ENTRY",
                                 "{" + boost::lexical_cast<string>(index) + "}");
      }
      dict.SetValue("FIELDS", fields);
      dict.SetValue("HASH_CODE", hash);
      dict.SetValue("TO_STRING_ARGS", to_string_args);
      dict.SetValue("CONSTRUCTOR", constructor);
      string struct_code = "";
      ExpandTemplate(FLAGS_naiad_templates_dir + "StructTemplate.cs",
                     ctemplate::DO_NOT_STRIP, &dict, &struct_code);
      structs_code += struct_code + "\n";
    }
    return structs_code;
  }

  pair<string, string> TranslatorNaiad::GenerateOutputCode(
      const set<shared_ptr<OperatorNode> >& leaves) {
    string output_code;
    string output_close_code;
    TemplateDictionary dict("Output");
    for (set<shared_ptr<OperatorNode> >::const_iterator it = leaves.begin();
         it != leaves.end(); ++it) {
      OperatorInterface* op = (*it)->get_operator();
      string out_rel_name = op->get_output_relation()->get_name();
      if (op->get_rename()) {
        op->set_rename(false);
        out_rel_name = op->get_output_relation()->get_name();
        op->set_rename(true);
      }
      dict.SetValue("OUTPUT_REL", out_rel_name);
      dict.SetValue("TMP_ROOT", FLAGS_tmp_data_dir);
      string code;
      ExpandTemplate(FLAGS_naiad_templates_dir + "OutputFileTemplate.cs",
                     ctemplate::DO_NOT_STRIP, &dict, &code);
      output_code += code;
      code = "";
      ExpandTemplate(FLAGS_naiad_templates_dir + "OutputFileCloseTemplate.cs",
                     ctemplate::DO_NOT_STRIP, &dict, &code);
      output_close_code += code;
    }
    return make_pair(output_code, output_close_code);
  }

  void TranslatorNaiad::MarkImmutable(const op_nodes& dag, set<string>* processed,
                                      map<string, Relation*>* name_rel) {
    for (op_nodes::const_iterator it = dag.begin(); it != dag.end(); ++it) {
      OperatorInterface* op = (*it)->get_operator();
      string output_rel = op->get_output_relation()->get_name();
      if (processed->find(output_rel) == processed->end()) {
        MarkImmutable(*it, processed, name_rel);
      }
    }
  }

  void TranslatorNaiad::MarkImmutable(shared_ptr<OperatorNode> node,
                                      set<string>* processed,
                                      map<string, Relation*>* name_rel) {
    OperatorInterface* op = node->get_operator();
    string output_rel = op->get_output_relation()->get_name();
    processed->insert(output_rel);
    (*name_rel)[output_rel] = op->get_output_relation();
    if (!node->IsLeaf()) {
      if (op->get_type() == WHILE_OP) {
        MarkImmutableWhile(node, processed, name_rel);
      }
      op_nodes children = node->get_children();
      MarkImmutable(children, processed, name_rel);
    }
  }

  void TranslatorNaiad::MarkImmutableWhile(shared_ptr<OperatorNode> node,
                                           set<string>* processed,
                                           map<string, Relation*>* name_rel) {
    // TODO(ionel): Add support for nested WHILEs
    set<string> input_rel_names;
    set<string> out_rel_names;
    set<shared_ptr<OperatorNode> > visited;
    queue<shared_ptr<OperatorNode> > to_visit;
    op_nodes children = node->get_loop_children();
    for (op_nodes::iterator it = children.begin(); it != children.end(); ++it) {
      to_visit.push(*it);
      visited.insert(*it);
    }
    while (!to_visit.empty()) {
      shared_ptr<OperatorNode> cur_node = to_visit.front();
      OperatorInterface* op = cur_node->get_operator();
      to_visit.pop();
      processed->insert(op->get_output_relation()->get_name());
      (*name_rel)[op->get_output_relation()->get_name()] =
        op->get_output_relation();
      out_rel_names.insert(op->get_output_relation()->get_name());
      vector<Relation*> input_rels = op->get_relations();
      for (vector<Relation*>::iterator it = input_rels.begin();
           it != input_rels.end(); ++it) {
        input_rel_names.insert((*it)->get_name());
        (*name_rel)[(*it)->get_name()] = *it;
      }
      if (!cur_node->IsLeaf()) {
        op_nodes children = cur_node->get_children();
        for (op_nodes::iterator it = children.begin(); it != children.end();
             ++it) {
          if (visited.insert(*it).second) {
            to_visit.push(*it);
          }
        }
      }
    }
    for (set<string>::iterator it = input_rel_names.begin();
         it != input_rel_names.end(); ++it) {
      if (out_rel_names.find(*it) == out_rel_names.end()) {
        // Relation is not written/overwitten in the while loop.
        LOG(INFO) << "Relation " << *it << " is immutable";
        (*name_rel)[*it]->set_immutable(true);
      }
    }
  }

  void TranslatorNaiad::TranslateDAG(string* code, string* fun_code,
                                     string* compactor_code,
                                     const op_nodes& dag,
                                     set<shared_ptr<OperatorNode> >* leaves,
                                     set<string>* processed) {
    for (op_nodes::const_iterator it = dag.begin(); it != dag.end(); ++it) {
      shared_ptr<OperatorNode> node = *it;
      OperatorInterface* op = (*it)->get_operator();
      if (CanSchedule(op, processed)) {
        NaiadJobCode* job_code =
          dynamic_cast<NaiadJobCode*>(TranslateOperator(op));
        *fun_code += job_code->get_agg_fun_code() + "\n";
        *compactor_code += job_code->get_compactor_code() + "\n";
        string out_rel_name = op->get_output_relation()->get_name();
        bool code_generated_cur_op = false;
        if (MustGenerateCode(*it)) {
          if (cur_job_code != NULL) {
            if (op->get_type() == SELECT_OP || op->get_type() == PROJECT_OP ||
                op->get_type() == DIV_OP || op->get_type() == MUL_OP ||
                op->get_type() == SUB_OP || op->get_type() == SUM_OP) {
              // The operator is mergeable into the upstream operator but no downstream operators
              // can be merged into it because it has more than 1 children.
              // Merge the operator into its father.
              code_generated_cur_op = true;
              string cur_out_fun_code = cur_job_code->get_out_fun_code();
              TemplateDictionary out_fun_dict("out_fun");
              StringToTemplateCache("out_fun", cur_out_fun_code, ctemplate::DO_NOT_STRIP);
              out_fun_dict.SetValue("NEXT_OPERATOR", job_code->get_out_fun_code() + "\n return " +
                                    job_code->get_rel_out_name() + "_local;");
              string out_fun_code = "";
              ExpandTemplate("out_fun", ctemplate::DO_NOT_STRIP, &out_fun_dict, &out_fun_code);
              mutable_default_template_cache()->ClearCache();
              // Generate code for the operator
              TemplateDictionary dict("code");
              string tmp_op_code = cur_job_code->get_code();
              StringToTemplateCache("code", tmp_op_code, ctemplate::DO_NOT_STRIP);
              dict.SetValue("OUTPUT_REL", out_rel_name);
              dict.SetValue("NEXT_OPERATOR", out_fun_code);
              string op_code = "";
              ExpandTemplate("code", ctemplate::DO_NOT_STRIP, &dict, &op_code);
              mutable_default_template_cache()->ClearCache();
              *code += op_code;
            } else {
              // Generate code for the current merged operators.
              TemplateDictionary dict_cur("cur_code");
              string tmp_op_code = cur_job_code->get_code();
              StringToTemplateCache("cur_code", tmp_op_code, ctemplate::DO_NOT_STRIP);
              dict_cur.SetValue("NEXT_OPERATOR", cur_job_code->get_out_fun_code());
              dict_cur.SetValue("OUTPUT_REL", cur_job_code->get_rel_out_name());
              string cur_op_code = "";
              ExpandTemplate("cur_code", ctemplate::DO_NOT_STRIP, &dict_cur, &cur_op_code);
              mutable_default_template_cache()->ClearCache();
              *code += cur_op_code;
            }
            cur_job_code = NULL;
          }
          if (!code_generated_cur_op) {
            // Generate code for the current non-mergeable operator.
            code_generated_cur_op = true;
            TemplateDictionary dict("code");
            string tmp_op_code = job_code->get_code();
            StringToTemplateCache("code", tmp_op_code, ctemplate::DO_NOT_STRIP);
            dict.SetValue("OUTPUT_REL", out_rel_name);
            dict.SetValue("NEXT_OPERATOR", job_code->get_out_fun_code() + "\n return " +
                          job_code->get_rel_out_name() + "_local;");
            string op_code = "";
            ExpandTemplate("code", ctemplate::DO_NOT_STRIP, &dict, &op_code);
            mutable_default_template_cache()->ClearCache();
            *code += op_code;
          }
        } else {
          if (cur_job_code == NULL) {
            cur_job_code = job_code;
          }
          // Update the output function by appending the next operator.
          string cur_out_fun_code = cur_job_code->get_out_fun_code();
          TemplateDictionary dict("out_fun");
          StringToTemplateCache("out_fun", cur_out_fun_code, ctemplate::DO_NOT_STRIP);
          dict.SetValue("NEXT_OPERATOR", job_code->get_out_fun_code());
          string next_out_fun_code = "";
          ExpandTemplate("out_fun", ctemplate::DO_NOT_STRIP, &dict, &next_out_fun_code);
          mutable_default_template_cache()->ClearCache();
          cur_job_code->set_out_fun_code(next_out_fun_code + "\n {{NEXT_OPERATOR}}");
          cur_job_code->set_rel_out_name(out_rel_name);
        }
        if (node->IsLeaf()) {
          leaves->insert(node);
          if (!code_generated_cur_op) {
            code_generated_cur_op = true;
            // Generate code for the current merged operators.
            TemplateDictionary dict_cur("cur_code");
            string tmp_op_code = cur_job_code->get_code();
            StringToTemplateCache("cur_code", tmp_op_code, ctemplate::DO_NOT_STRIP);
            dict_cur.SetValue("NEXT_OPERATOR", cur_job_code->get_out_fun_code());
            dict_cur.SetValue("OUTPUT_REL", cur_job_code->get_rel_out_name());
            string cur_op_code = "";
            ExpandTemplate("cur_code", ctemplate::DO_NOT_STRIP, &dict_cur, &cur_op_code);
            mutable_default_template_cache()->ClearCache();
            *code += cur_op_code;
            cur_job_code = NULL;
          }
        }
        if (op->get_rename()) {
          string tmp_rel_name = op->get_output_relation()->get_name();
          op->set_rename(false);
          string rel_name = op->get_output_relation()->get_name();
          op->set_rename(true);
          *code += " " + rel_name + " = " + tmp_rel_name + ";\n";
        }
        processed->insert(op->get_output_relation()->get_name());
        if (!node->IsLeaf()) {
          if (node->get_operator()->get_type() == WHILE_OP) {
            TranslateDAG(code, fun_code, compactor_code,
                         node->get_loop_children(), leaves, processed);
            // TODO(ionel): Rel names should keep track of names already defined.
            // (e.g. when we have a while loop within a while loop.
            *code += "\n}\n";
          }
          TranslateDAG(code, fun_code, compactor_code, node->get_children(),
                       leaves, processed);
        }
      } else {
        LOG(INFO) << "Cannot scheduler operator yet: "
                  << op->get_output_relation()->get_name();
      }
    }
  }

  bool TranslatorNaiad::CanSchedule(OperatorInterface* op,
                                    set<string>* processed) {
    string output = op->get_output_relation()->get_name();
    if (processed->find(output) != processed->end()) {
      LOG(INFO) << "Operator already scheduled";
      return false;
    }
    vector<Relation*> inputs = op->get_relations();
    for (vector<Relation*>::iterator it = inputs.begin(); it != inputs.end();
         ++it) {
      if (processed->find((*it)->get_name()) == processed->end()) {
        LOG(INFO) << "Cannot schedule " << op->get_output_relation()->get_name()
                  << ", input " << (*it)->get_name() << " is missing";
        return false;
      }
    }
    return true;
  }

  string TranslatorNaiad::GenerateReadCode(
      const vector<Relation*>& input_relations) {
    string read_code;
    TemplateDictionary dict("read_code");
    for (vector<Relation*>::const_iterator it = input_relations.begin();
         it != input_relations.end(); ++it) {
      Relation* rel = *it;
      dict.SetValue("FUN_REL_NAME", "read_" + rel->get_name());
      dict.SetValue("TYPE", GenerateType(rel));
      string gen_cols = "";
      vector<Column*> cols = rel->get_columns();
      for (vector<Column*>::size_type index = 0; index < cols.size(); index++) {
        switch (cols[index]->get_type()) {
        case INTEGER_TYPE: {
          gen_cols += "Convert.ToInt32(elements[" +
            boost::lexical_cast<string>(index) + "]),";
          break;
        }
        case STRING_TYPE: {
          gen_cols += "elements[" + boost::lexical_cast<string>(index) + "],";
          break;
        }
        case DOUBLE_TYPE: {
          gen_cols += "Convert.ToDouble(elements[" +
            boost::lexical_cast<string>(index) + "]),";
          break;
        }
        case BOOLEAN_TYPE: {
          gen_cols += "Convert.ToBoolean(elements[" +
            boost::lexical_cast<string>(index) + "]),";
        }
        default: {
          LOG(ERROR) << "Column has unexpected type";
          return NULL;
        }
        }
      }
      if (rel->get_columns().size() == 1) {
        dict.SetValue("GENERATE_COLS", gen_cols.substr(0, gen_cols.size() - 1));
      } else {
        dict.SetValue("GENERATE_COLS", "new " + GenerateType(rel) + "(" +
                      gen_cols.substr(0, gen_cols.size() - 1) + ")");
      }
      string code;
      ExpandTemplate(FLAGS_naiad_templates_dir + "ReadTemplate.cs",
                     ctemplate::DO_NOT_STRIP, &dict, &code);
      read_code += "\n" + code;
    }
    return read_code;
  }

  NaiadJobCode* TranslatorNaiad::Translate(AggOperator* op) {
    TemplateDictionary dict("agg");
    string out_rel_name = op->get_output_relation()->get_name();
    dict.SetValue("INPUT_PATH", op->get_input_paths()[0]);
    dict.SetValue("INPUT_REL", AvoidNameClash(op, 0));
    dict.SetValue("OUTPUT_REL", "{{OUTPUT_REL}}");
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    vector<uint16_t> cols_type;
    vector<Column*> group_bys = op->get_group_bys();
    for (vector<Column*>::size_type index = 0; index < group_bys.size();
         index++) {
      cols_type.push_back(group_bys[index]->get_type());
    }
    vector<Column*> cols = op->get_columns();
    for (vector<Column*>::size_type index = 0; index < cols.size(); ++index) {
      cols_type.push_back(cols[index]->get_type());
    }
    string output_type = GenerateType(cols_type);
    dict.SetValue("OUTPUT_TYPE", output_type);
    dict.SetValue("FUN_NAME", out_rel_name + "Fun");
    dict.SetValue("ROW_TYPE", GenerateType(op->get_relations()[0]));
    // TODO(ionel): Add support for multi column agg.
    dict.SetValue("VAL_TYPE",
                  Column::stringPrimitiveTypeCSharp(op->get_columns()[0]->get_type()));
    if (op->get_operator().compare("+") || op->get_operator().compare("-")) {
      dict.SetValue("INIT_VAL", "0");
    } else {
      dict.SetValue("INIT_VAL", "1");
    }
    dict.SetValue("UPDATE_VAL", "outValue = outValue " +
                  op->get_operator() + " value." +
                  Column::indexString(op->get_columns()[0]->get_index()));
    if (op->hasGroupby()) {
      dict.SetValue("KEY", GenerateSelectCols("row", group_bys));
      dict.SetValue("KEY_TYPE", GenerateType(op->get_group_bys()));
      dict.SetValue("KEY_OUT", GenerateKeyOut(op->get_group_bys().size()));
    } else {
      dict.SetValue("KEY", "\"all\"");
      dict.SetValue("KEY_TYPE", "string ");
      dict.SetValue("KEY_OUT", "key, ");
    }
    string where = op->get_condition_tree()->toString("naiad");
    if (where.compare("true")) {
      dict.SetValue("WHERE", ".Where(row => " + where + ")");
    }
    string fun_code;
    string template_location;
    string flatten = "";
    if (!op->get_operator().compare("-") || !op->get_operator().compare("/")) {
       ExpandTemplate(FLAGS_naiad_templates_dir + "AggregatorTemplate.cs",
                      ctemplate::DO_NOT_STRIP, &dict, &fun_code);
       template_location = FLAGS_naiad_templates_dir + "AggTemplate.cs";
    } else {
      string reducer_name = "Reducer" + out_rel_name;
      flatten = GenerateReducerClass(&dict, reducer_name, op->get_operator(),
                                     op->get_group_bys(), op->get_columns());
      ExpandTemplate(FLAGS_naiad_templates_dir + "ReducerTemplate.cs",
                     ctemplate::DO_NOT_STRIP, &dict, &fun_code);
      template_location = FLAGS_naiad_templates_dir + "GenericAggregatorTemplate.cs";
    }
    string code;
    ExpandTemplate(template_location, ctemplate::DO_NOT_STRIP, &dict, &code);
    NaiadJobCode* job_code = new NaiadJobCode(op, code);
    job_code->set_agg_fun_code(fun_code);
    job_code->set_rel_out_name(out_rel_name);
    job_code->set_out_fun_code(output_type + " " + out_rel_name + "_local = " + flatten + ";");
    return job_code;
  }

  NaiadJobCode* TranslatorNaiad::Translate(CountOperator* op) {
    TemplateDictionary dict("count");
    string out_rel_name = op->get_output_relation()->get_name();
    dict.SetValue("INPUT_PATH", op->get_input_paths()[0]);
    dict.SetValue("INPUT_REL", AvoidNameClash(op, 0));
    dict.SetValue("OUTPUT_REL", "{{OUTPUT_REL}}");
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    vector<uint16_t> cols_type;
    vector<Column*> group_bys = op->get_group_bys();
    for (vector<Column*>::size_type index = 0; index < group_bys.size();
         index++) {
      cols_type.push_back(group_bys[index]->get_type());
    }
    cols_type.push_back(op->get_column()->get_type());
    string output_type = GenerateType(cols_type);
    dict.SetValue("OUTPUT_TYPE", output_type);
    dict.SetValue("FUN_NAME", out_rel_name + "Fun");
    dict.SetValue("ROW_TYPE", GenerateType(op->get_relations()[0]));
    // TODO(ionel): Add support for multi column count.
    dict.SetValue("VAL_TYPE",
                  Column::stringPrimitiveTypeCSharp(op->get_column()->get_type()));
    if (op->hasGroupby()) {
      dict.SetValue("KEY", GenerateSelectCols("row", group_bys));
      dict.SetValue("KEY_TYPE", GenerateType(op->get_group_bys()));
      dict.SetValue("KEY_OUT", GenerateKeyOut(op->get_group_bys().size()));
    } else {
      dict.SetValue("KEY", "\"all\"");
      dict.SetValue("KEY_TYPE", "string ");
      dict.SetValue("KEY_OUT", "key, ");
    }
    string where = op->get_condition_tree()->toString("naiad");
    if (where.compare("true")) {
      dict.SetValue("WHERE", ".Where(row => " + where + ")");
    }
    string fun_code;
    string reducer_name = "Reducer" + out_rel_name;
    vector<Column*> sel_col;
    sel_col.push_back(op->get_column());
    string flatten = GenerateReducerClass(&dict, reducer_name, "count", op->get_group_bys(),
                                          sel_col);
    ExpandTemplate(FLAGS_naiad_templates_dir + "ReducerTemplate.cs",
                   ctemplate::DO_NOT_STRIP, &dict, &fun_code);
    // ExpandTemplate(FLAGS_naiad_templates_dir + "CountAggregatorTemplate.cs",
    //                ctemplate::DO_NOT_STRIP, &dict, &fun_code);
    string code;
    ExpandTemplate(FLAGS_naiad_templates_dir + "GenericAggregatorTemplate.cs",
                   ctemplate::DO_NOT_STRIP, &dict, &code);
    NaiadJobCode* job_code = new NaiadJobCode(op, code);
    job_code->set_agg_fun_code(fun_code);
    job_code->set_rel_out_name(out_rel_name);
    job_code->set_out_fun_code(output_type + " " + out_rel_name + "_local = " + flatten + ";");
    return job_code;
  }

  NaiadJobCode* TranslatorNaiad::Translate(CrossJoinOperator* op) {
    TemplateDictionary dict("crossjoin");
    string out_rel_name = op->get_output_relation()->get_name();
    vector<string> input_paths = op->get_input_paths();
    vector<Relation*> rels = op->get_relations();
    dict.SetValue("LEFT_PATH", input_paths[0]);
    dict.SetValue("RIGHT_PATH", input_paths[1]);
    dict.SetValue("OUTPUT_PATH", op->get_output_path());
    dict.SetValue("LEFT_REL", AvoidNameClash(op, 0));
    dict.SetValue("RIGHT_REL", AvoidNameClash(op, 1));
    dict.SetValue("OUTPUT_REL", "{{OUTPUT_REL}}");
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    dict.SetValue("LEFT_TYPE", GenerateType(rels[0]));
    dict.SetValue("RIGHT_TYPE", GenerateType(rels[1]));
    vector<Column*> cols = rels[0]->get_columns();
    vector<uint16_t> cols_type;
    for (vector<Column*>::size_type index = 0; index < cols.size(); index++) {
      cols_type.push_back(cols[index]->get_type());
    }
    cols = rels[1]->get_columns();
    for (vector<Column*>::size_type index = 0; index < cols.size(); index++) {
      cols_type.push_back(cols[index]->get_type());
    }
    string output_type = GenerateType(cols_type);
    dict.SetValue("OUTPUT_TYPE", output_type);
    vector<int32_t> indexes_selected;
    int32_t rels_size = boost::lexical_cast<int32_t>(rels[0]->get_columns().size());
    for (int32_t index = 0; index < rels_size; ++index) {
      indexes_selected.push_back(index);
    }
    string left_output = GenerateOutput("left", indexes_selected);
    indexes_selected.clear();
    rels_size = boost::lexical_cast<int32_t>(rels[1]->get_columns().size());
    for (int32_t index = 0; index < rels_size; ++index) {
      indexes_selected.push_back(index);
    }
    string right_output = GenerateOutput("right", indexes_selected);
    dict.SetValue("OUTPUT_LEFT", left_output);
    dict.SetValue("OUTPUT_RIGHT", ", " + right_output);
    string code;
    ExpandTemplate(FLAGS_naiad_templates_dir + "CrossJoinTemplate.cs",
                   ctemplate::DO_NOT_STRIP, &dict, &code);
    NaiadJobCode* job_code = new NaiadJobCode(op, code);
    job_code->set_rel_out_name(out_rel_name);
    job_code->set_out_fun_code(output_type + " " + out_rel_name + "_local = new " + output_type +
                               "(" + left_output + ", " + right_output + ");");
    return job_code;
  }

  NaiadJobCode* TranslatorNaiad::Translate(DifferenceOperator* op) {
    // TODO(ionel): Implement!
    TemplateDictionary dict("difference");
    vector<string> input_paths = op->get_input_paths();
    dict.SetValue("LEFT_PATH", input_paths[0]);
    dict.SetValue("RIGHT_PATH", input_paths[1]);
    dict.SetValue("LEFT_REL", AvoidNameClash(op, 0));
    dict.SetValue("RIGHT_REL", AvoidNameClash(op, 1));
    dict.SetValue("OUTPUT_REL", "{{OUTPUT_REL}}");
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string code;
    ExpandTemplate(FLAGS_naiad_templates_dir + "DifferenceTemplate.cs",
                   ctemplate::DO_NOT_STRIP, &dict, &code);
    NaiadJobCode* job_code = new NaiadJobCode(op, code);
    job_code->set_rel_out_name(op->get_output_relation()->get_name());
    return job_code;
  }

  NaiadJobCode* TranslatorNaiad::Translate(DistinctOperator* op) {
    TemplateDictionary dict("distinct");
    dict.SetValue("INPUT_PATH", op->get_input_paths()[0]);
    dict.SetValue("INPUT_REL", AvoidNameClash(op, 0));
    dict.SetValue("OUTPUT_REL", "{{OUTPUT_REL}}");
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string code;
    ExpandTemplate(FLAGS_naiad_templates_dir + "DistinctTemplate.cs",
                   ctemplate::DO_NOT_STRIP, &dict, &code);
    NaiadJobCode* job_code = new NaiadJobCode(op, code);
    job_code->set_rel_out_name(op->get_output_relation()->get_name());
    return job_code;
  }

  NaiadJobCode* TranslatorNaiad::Translate(DivOperator* op) {
    return TranslateMathOp(op, op->get_values(), op->get_condition_tree(), "/");
  }

  NaiadJobCode* TranslatorNaiad::Translate(IntersectionOperator* op) {
    TemplateDictionary dict("intersection");
    vector<string> input_paths = op->get_input_paths();
    dict.SetValue("LEFT_PATH", input_paths[0]);
    dict.SetValue("RIGHT_PATH", input_paths[1]);
    dict.SetValue("LEFT_REL", AvoidNameClash(op, 0));
    dict.SetValue("RIGHT_REL", AvoidNameClash(op, 1));
    dict.SetValue("OUTPUT_REL", "{{OUTPUT_REL}}");
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string code;
    ExpandTemplate(FLAGS_naiad_templates_dir + "IntersectionTemplate.cs",
                   ctemplate::DO_NOT_STRIP, &dict, &code);
    NaiadJobCode* job_code = new NaiadJobCode(op, code);
    job_code->set_rel_out_name(op->get_output_relation()->get_name());
    return job_code;
  }

  NaiadJobCode* TranslatorNaiad::Translate(JoinOperator* op) {
    TemplateDictionary dict("join");
    string out_rel_name = op->get_output_relation()->get_name();
    vector<string> input_paths = op->get_input_paths();
    vector<Relation*> rels = op->get_relations();
    dict.SetValue("LEFT_PATH", input_paths[0]);
    dict.SetValue("RIGHT_PATH", input_paths[1]);
    dict.SetValue("OUTPUT_PATH", op->get_output_path());
    dict.SetValue("LEFT_REL", AvoidNameClash(op, 0));
    dict.SetValue("RIGHT_REL", AvoidNameClash(op, 1));
    dict.SetValue("OUTPUT_REL", "{{OUTPUT_REL}}");
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    dict.SetValue("LEFT_INDEX",
                  Column::indexString(op->get_col_left()->get_index()));
    dict.SetValue("LEFT_TYPE", GenerateType(rels[0]));
    dict.SetValue("RIGHT_INDEX",
                  Column::indexString(op->get_col_right()->get_index()));
    dict.SetValue("RIGHT_TYPE", GenerateType(rels[1]));
    vector<Column*> cols = rels[0]->get_columns();
    vector<uint16_t> cols_type;
    for (vector<Column*>::size_type index = 0; index < cols.size(); index++) {
      cols_type.push_back(cols[index]->get_type());
    }
    cols = rels[1]->get_columns();
    for (vector<Column*>::size_type index = 0; index < cols.size(); index++) {
      if (boost::lexical_cast<int32_t>(index) != op->get_col_right()->get_index()) {
        cols_type.push_back(cols[index]->get_type());
      }
    }
    string output_type = GenerateType(cols_type);
    dict.SetValue("OUTPUT_TYPE", output_type);
    vector<int32_t> indexes_selected;
    int32_t rels_size = boost::lexical_cast<int32_t>(rels[0]->get_columns().size());
    for (int32_t index = 0; index < rels_size; ++index) {
      indexes_selected.push_back(index);
    }
    string left_output = GenerateOutput("left", indexes_selected);
    indexes_selected.clear();
    rels_size = boost::lexical_cast<int32_t>(rels[1]->get_columns().size());
    for (int32_t index = 0; index < rels_size; ++index) {
      if (index != op->get_col_right()->get_index()) {
        indexes_selected.push_back(index);
      }
    }
    string right_output = GenerateOutput("right", indexes_selected);
    dict.SetValue("OUTPUT_LEFT", left_output);
    if (right_output.compare("")) {
      dict.SetValue("OUTPUT_RIGHT", ", " + right_output);
    } else {
      dict.SetValue("OUTPUT_RIGHT", "");
    }
    string code;
    ExpandTemplate(FLAGS_naiad_templates_dir + "JoinTemplate.cs",
                   ctemplate::DO_NOT_STRIP, &dict, &code);
    NaiadJobCode* job_code = new NaiadJobCode(op, code);
    if (rels[0]->isImmutable() || rels[1]->isImmutable()) {
      // Populate fast join templates.
      GenerateFastJoin(&dict, op);
      string compactor;
      ExpandTemplate(FLAGS_naiad_templates_dir + "CompactorTemplate.cs",
                     ctemplate::DO_NOT_STRIP, &dict, &compactor);
      job_code->set_compactor_code(compactor);
    }
    job_code->set_rel_out_name(out_rel_name);
    job_code->set_out_fun_code(output_type + " " + out_rel_name + "_local = new " + output_type +
                               "(" + left_output + ", " + right_output + ");");
    return job_code;
  }

  NaiadJobCode* TranslatorNaiad::Translate(MaxOperator* op) {
    TemplateDictionary dict("max");
    string out_rel_name = op->get_output_relation()->get_name();
    dict.SetValue("INPUT_PATH", op->get_input_paths()[0]);
    dict.SetValue("INPUT_REL", AvoidNameClash(op, 0));
    dict.SetValue("OUTPUT_REL", "{{OUTPUT_REL}}");
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    vector<uint16_t> cols_type;
    vector<Column*> group_bys = op->get_group_bys();
    for (vector<Column*>::size_type index = 0; index < group_bys.size();
         index++) {
      cols_type.push_back(group_bys[index]->get_type());
    }
    cols_type.push_back(op->get_column()->get_type());
    string output_type = GenerateType(cols_type);
    dict.SetValue("OUTPUT_TYPE", output_type);
    dict.SetValue("FUN_NAME", out_rel_name + "Fun");
    dict.SetValue("ROW_TYPE", GenerateType(op->get_relations()[0]));

    dict.SetValue("VAL_TYPE",
                  Column::stringPrimitiveTypeCSharp(op->get_column()->get_type()));
    dict.SetValue("INIT_VAL", "int.MinValue");
    dict.SetValue("UPDATE_VAL", "outValue = Math.Max(outValue, value." +
                  Column::indexString(op->get_column()->get_index()) + ")");
    if (op->hasGroupby()) {
      vector<Column*> group_bys = op->get_group_bys();
      dict.SetValue("KEY", GenerateSelectCols("row", group_bys));
      dict.SetValue("KEY_TYPE", GenerateType(op->get_group_bys()));
      dict.SetValue("KEY_OUT", GenerateKeyOut(op->get_group_bys().size()));
    } else {
      dict.SetValue("KEY", "\"all\"");
      dict.SetValue("KEY_TYPE", "string ");
      dict.SetValue("KEY_OUT", "key, ");
    }
    // TODO(ionel): Add support for selected columns agg.
    string where = op->get_condition_tree()->toString("naiad");
    if (where.compare("true")) {
      dict.SetValue("WHERE", ".Where(row => " + where + ")");
    }
    string fun_code;
    string reducer_name = "Reducer" + out_rel_name;
    vector<Column*> sel_col;
    sel_col.push_back(op->get_column());
    string flatten = GenerateReducerClass(&dict, reducer_name, "max", op->get_group_bys(),
                                          sel_col);
    ExpandTemplate(FLAGS_naiad_templates_dir + "ReducerTemplate.cs",
                   ctemplate::DO_NOT_STRIP, &dict, &fun_code);
    // ExpandTemplate(FLAGS_naiad_templates_dir + "AggregatorTemplate.cs",
    //                ctemplate::DO_NOT_STRIP, &dict, &fun_code);
    string code;
    ExpandTemplate(FLAGS_naiad_templates_dir + "GenericAggregatorTemplate.cs",
                   ctemplate::DO_NOT_STRIP, &dict, &code);
    NaiadJobCode* job_code = new NaiadJobCode(op, code);
    job_code->set_agg_fun_code(fun_code);
    job_code->set_rel_out_name(out_rel_name);
    job_code->set_out_fun_code(output_type + " " + out_rel_name + "_local = " + flatten + ";");
    return job_code;
  }

  NaiadJobCode* TranslatorNaiad::Translate(MinOperator* op) {
    TemplateDictionary dict("min");
    string out_rel_name = op->get_output_relation()->get_name();
    dict.SetValue("INPUT_PATH", op->get_input_paths()[0]);
    dict.SetValue("INPUT_REL", AvoidNameClash(op, 0));
    dict.SetValue("OUTPUT_REL", "{{OUTPUT_REL}}");
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    vector<uint16_t> cols_type;
    vector<Column*> group_bys = op->get_group_bys();
    for (vector<Column*>::size_type index = 0; index < group_bys.size();
         index++) {
      cols_type.push_back(group_bys[index]->get_type());
    }
    cols_type.push_back(op->get_column()->get_type());
    string output_type = GenerateType(cols_type);
    dict.SetValue("OUTPUT_TYPE", output_type);
    dict.SetValue("FUN_NAME", out_rel_name + "Fun");
    dict.SetValue("ROW_TYPE", GenerateType(op->get_relations()[0]));

    dict.SetValue("VAL_TYPE",
                  Column::stringPrimitiveTypeCSharp(op->get_column()->get_type()));
    dict.SetValue("INIT_VAL", "int.MaxValue");
    dict.SetValue("UPDATE_VAL", "outValue = Math.Min(outValue, value." +
                  Column::indexString(op->get_column()->get_index()) + ")");
    if (op->hasGroupby()) {
      vector<Column*> group_bys = op->get_group_bys();
      dict.SetValue("KEY", GenerateSelectCols("row", group_bys));
      dict.SetValue("KEY_TYPE", GenerateType(op->get_group_bys()));
      dict.SetValue("KEY_OUT", GenerateKeyOut(op->get_group_bys().size()));
    } else {
      dict.SetValue("KEY", "\"all\"");
      dict.SetValue("KEY_TYPE", "string ");
      dict.SetValue("KEY_OUT", "key, ");
    }
    // TODO(ionel): Add support for selected columns agg.
    string where = op->get_condition_tree()->toString("naiad");
    if (where.compare("true")) {
      dict.SetValue("WHERE", ".Where(row => " + where + ")");
    }
    string fun_code;
    string reducer_name = "Reducer" + out_rel_name;
    vector<Column*> sel_col;
    sel_col.push_back(op->get_column());
    string flatten = GenerateReducerClass(&dict, reducer_name, "min", op->get_group_bys(),
                                          sel_col);
    ExpandTemplate(FLAGS_naiad_templates_dir + "ReducerTemplate.cs",
                   ctemplate::DO_NOT_STRIP, &dict, &fun_code);
    // ExpandTemplate(FLAGS_naiad_templates_dir + "AggregatorTemplate.cs",
    //                ctemplate::DO_NOT_STRIP, &dict, &fun_code);
    string code;
    ExpandTemplate(FLAGS_naiad_templates_dir + "GenericAggregatorTemplate.cs",
                   ctemplate::DO_NOT_STRIP, &dict, &code);
    NaiadJobCode* job_code = new NaiadJobCode(op, code);
    job_code->set_agg_fun_code(fun_code);
    job_code->set_rel_out_name(out_rel_name);
    job_code->set_out_fun_code(output_type + " " + out_rel_name + "_local = " + flatten + ";");
    return job_code;
  }

  NaiadJobCode* TranslatorNaiad::Translate(MulOperator* op) {
    return TranslateMathOp(op, op->get_values(), op->get_condition_tree(), "*");
  }

  NaiadJobCode* TranslatorNaiad::Translate(ProjectOperator* op) {
    TemplateDictionary dict("project");
    string out_rel_name = op->get_output_relation()->get_name();
    string input_rel_name = AvoidNameClash(op, 0);
    dict.SetValue("INPUT_PATH", op->get_input_paths()[0]);
    dict.SetValue("INPUT_REL", input_rel_name);
    dict.SetValue("OUTPUT_REL", "{{OUTPUT_REL}}");
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    vector<Column*> in_cols = op->get_relations()[0]->get_columns();
    vector<Column*> cols = op->get_columns();
    string where = op->get_condition_tree()->toString("naiad");
    if (where.compare("true")) {
      dict.SetValue("WHERE", ".Where(row => " + where + ")");
    }
    string code;
    string out_fun_code = "";
    if (!SameColumns(in_cols, cols)) {
      out_fun_code = "var " + out_rel_name + "_local = " +
        GenerateSelectCols(input_rel_name + "_local", cols) + ";";
      dict.SetValue("SELECTED_COLS", out_fun_code);
      ExpandTemplate(FLAGS_naiad_templates_dir + "ProjectTemplate.cs",
                     ctemplate::DO_NOT_STRIP, &dict, &code);
    } else {
      ExpandTemplate(FLAGS_naiad_templates_dir + "WhereTemplate.cs",
                     ctemplate::DO_NOT_STRIP, &dict, &code);
    }
    NaiadJobCode* job_code = new NaiadJobCode(op, code);
    job_code->set_rel_out_name(out_rel_name);
    job_code->set_out_fun_code(out_fun_code);
    return job_code;
  }

  NaiadJobCode* TranslatorNaiad::Translate(SelectOperator* op) {
    TemplateDictionary dict("select");
    string out_rel_name = op->get_output_relation()->get_name();
    string input_rel_name = AvoidNameClash(op, 0);
    dict.SetValue("INPUT_PATH", op->get_input_paths()[0]);
    dict.SetValue("INPUT_REL", input_rel_name);
    dict.SetValue("OUTPUT_REL", "{{OUTPUT_REL}}");
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    vector<Column*> in_cols = op->get_relations()[0]->get_columns();
    vector<Column*> cols = op->get_columns();
    string where = op->get_condition_tree()->toString("naiad");
    if (where.compare("true")) {
      dict.SetValue("WHERE", ".Where(row => " + where + ")");
    }
    string code;
    string out_fun_code = "";
    if (!SameColumns(in_cols, cols)) {
      out_fun_code = "var " + out_rel_name + "_local = " +
        GenerateSelectCols(input_rel_name + "_local", cols) + ";";
      dict.SetValue("SELECTED_COLS", out_fun_code);
      ExpandTemplate(FLAGS_naiad_templates_dir + "SelectTemplate.cs",
                     ctemplate::DO_NOT_STRIP, &dict, &code);
    } else {
      ExpandTemplate(FLAGS_naiad_templates_dir + "WhereTemplate.cs",
                     ctemplate::DO_NOT_STRIP, &dict, &code);
    }
    NaiadJobCode* job_code = new NaiadJobCode(op, code);
    job_code->set_rel_out_name(out_rel_name);
    job_code->set_out_fun_code(out_fun_code);
    return job_code;
  }

  NaiadJobCode* TranslatorNaiad::Translate(SortOperator* op) {
    // TODO(ionel): Implement!
    TemplateDictionary dict("sort");
    dict.SetValue("INPUT_PATH", op->get_input_paths()[0]);
    dict.SetValue("INPUT_REL", AvoidNameClash(op, 0));
    dict.SetValue("OUTPUT_REL", "{{OUTPUT_REL}}");
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string code;
    ExpandTemplate(FLAGS_naiad_templates_dir + "SortTemplate.cs", ctemplate::DO_NOT_STRIP,
                   &dict, &code);
    NaiadJobCode* job_code = new NaiadJobCode(op, code);
    job_code->set_rel_out_name(op->get_output_relation()->get_name());
    return job_code;
  }

  NaiadJobCode* TranslatorNaiad::Translate(SubOperator* op) {
    return TranslateMathOp(op, op->get_values(), op->get_condition_tree(), "-");
  }

  NaiadJobCode* TranslatorNaiad::Translate(SumOperator* op) {
    vector<Value*> values = op->get_values();
    return TranslateMathOp(op, values, op->get_condition_tree(), "+");
  }

  NaiadJobCode* TranslatorNaiad::Translate(UdfOperator* op) {
    // TODO(ionel): Implement!
    string code;
    return new NaiadJobCode(op, code);
  }

  NaiadJobCode* TranslatorNaiad::Translate(UnionOperator* op) {
    TemplateDictionary dict("union");
    vector<string> input_paths = op->get_input_paths();
    dict.SetValue("LEFT_PATH", input_paths[0]);
    dict.SetValue("RIGHT_PATH", input_paths[1]);
    dict.SetValue("LEFT_REL", AvoidNameClash(op, 0));
    dict.SetValue("RIGHT_REL", AvoidNameClash(op, 1));
    dict.SetValue("OUTPUT_REL", "{{OUTPUT_REL}}");
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string code;
    ExpandTemplate(FLAGS_naiad_templates_dir + "UnionTemplate.cs", ctemplate::DO_NOT_STRIP,
                   &dict, &code);
    NaiadJobCode* job_code = new NaiadJobCode(op, code);
    job_code->set_rel_out_name(op->get_output_relation()->get_name());
    return job_code;
  }

  NaiadJobCode* TranslatorNaiad::Translate(WhileOperator* op) {
    TemplateDictionary dict("while");
    dict.SetValue("NUM_ITER",
                  op->get_condition_tree()->get_right()->get_value()->get_value());
    string code;
    ExpandTemplate(FLAGS_naiad_templates_dir + "WhileTemplate.cs",
                   ctemplate::DO_NOT_STRIP, &dict, &code);
    NaiadJobCode* job_code = new NaiadJobCode(op, code);
    job_code->set_rel_out_name(op->get_output_relation()->get_name());
    return job_code;
  }

  NaiadJobCode* TranslatorNaiad::TranslateMathOp(OperatorInterface* op,
      vector<Value*> values, ConditionTree* condition_tree, string math_op) {
    TemplateDictionary dict("math_op");
    string out_rel_name = op->get_output_relation()->get_name();
    string input_rel_name = AvoidNameClash(op, 0);
    dict.SetValue("INPUT_PATH", op->get_input_paths()[0]);
    dict.SetValue("INPUT_REL", input_rel_name);
    dict.SetValue("OUTPUT_REL", "{{OUTPUT_REL}}");
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    Column* left_column = dynamic_cast<Column*>(values[0]);
    Column* right_column = dynamic_cast<Column*>(values[1]);
    int32_t left_index = -1;
    int32_t right_index = -1;
    if (left_column != NULL) {
      left_index = left_column->get_index();
    }
    if (right_column != NULL) {
      right_index = right_column->get_index();
    }
    vector<Column*> cols = op->get_relations()[0]->get_columns();
    vector<uint16_t> cols_type;
    pair<uint16_t, uint16_t> selected_col_types = TranslateType(values);
    for (vector<Column*>::size_type index = 0; index < cols.size(); index++) {
      if (left_index == boost::lexical_cast<int32_t>(index)) {
        cols_type.push_back(selected_col_types.first);
      } else if (right_index == boost::lexical_cast<int32_t>(index)) {
        cols_type.push_back(selected_col_types.second);
      } else {
        cols_type.push_back(cols[index]->get_type());
      }
    }
    string selected_cols = "";
    if (cols_type.size() > 1) {
      selected_cols = "new " + GenerateType(cols_type) + "(";
      selected_cols += GenerateOutput(input_rel_name + "_local", op, values, math_op, false) + ")";
    } else {
      selected_cols = GenerateOutput(input_rel_name + "_local", op, values, math_op, true);
    }
    string output_type = GenerateType(cols_type);
    dict.SetValue("OUTPUT_TYPE", output_type);
    dict.SetValue("SELECTED_COLS", selected_cols);
    string where = condition_tree->toString("naiad");
    if (where.compare("true")) {
      dict.SetValue("WHERE", ".Where(row => " + where + ")");
    }
    string code;
    ExpandTemplate(FLAGS_naiad_templates_dir + "MathTemplate.cs", ctemplate::DO_NOT_STRIP,
                   &dict, &code);
    NaiadJobCode* job_code = new NaiadJobCode(op, code);
    job_code->set_rel_out_name(out_rel_name);
    job_code->set_out_fun_code(output_type + " " + out_rel_name +
                               "_local = " + selected_cols + ";");
    return job_code;
  }

  pair<uint16_t, uint16_t> TranslatorNaiad::TranslateType(const vector<Value*>& values) {
    Column* left_column = dynamic_cast<Column*>(values[0]);
    Column* right_column = dynamic_cast<Column*>(values[1]);
    uint16_t left_type = INTEGER_TYPE;
    uint16_t right_type = INTEGER_TYPE;
    if (left_column != NULL) {
      left_type = left_column->get_type();
    }
    if (right_column != NULL) {
      right_type = right_column->get_type();
    }
    if (right_type == DOUBLE_TYPE) {
      left_type = DOUBLE_TYPE;
    }
    return make_pair(left_type, right_type);
  }

  void TranslatorNaiad::GenerateFastJoin(TemplateDictionary* dict,
                                         JoinOperator* op) {
    vector<Relation*> rels = op->get_relations();
    dict->SetValue("LEFT_ROW_TYPE", GenerateType(rels[0]));
    dict->SetValue("RIGHT_ROW_TYPE", GenerateType(rels[1]));
    dict->SetValue("LEFT_KEY_INDEX",
                   Column::indexString(op->get_col_left()->get_index()));
    dict->SetValue("RIGHT_KEY_INDEX",
                   Column::indexString(op->get_col_right()->get_index()));
    // TODO(ionel): Handle the case when the static relation is on the right.
    vector<Column*> cols = rels[0]->get_columns();
    vector<Column*> remainder_cols(cols.begin(), cols.end());
    remainder_cols.erase(remainder_cols.begin() +
                         op->get_col_right()->get_index());
    string build_remainder = "";
    if (remainder_cols.size() == 1) {
      build_remainder = "newRows[i]." +
        Column::indexString(remainder_cols[0]->get_index()) + ";";
    } else {
      build_remainder = "new " + GenerateType(remainder_cols) + "(";
      for (vector<Column*>::iterator it = remainder_cols.begin();
           it != remainder_cols.end(); ++it) {
        build_remainder += "newRows[i]." +
          Column::indexString((*it)->get_index()) + ", ";
      }
      build_remainder.erase(build_remainder.end() - 2, build_remainder.end());
      build_remainder += ");";
    }
    string update_remainder = "(";
    uint32_t left_col_index = op->get_col_left()->get_index();
    cols = rels[0]->get_columns();
    uint32_t index = 0;
    vector<uint16_t> col_types;
    for (vector<Column*>::iterator it = cols.begin(); it != cols.end(); ++it, ++index) {
      if ((*it)->get_index() == boost::lexical_cast<int32_t>(left_col_index)) {
        update_remainder += "record." +
          Column::indexString(op->get_col_right()->get_index()) + ", ";
        col_types.push_back(op->get_col_right()->get_type());
      } else {
        if ((*it)->get_index() < op->get_col_left()->get_index()) {
          update_remainder += "remainderRow." + Column::indexString(index) +
            ", ";
        } else {
          update_remainder += "remainderRow." + Column::indexString(index - 1) +
            ", ";
        }
        col_types.push_back((*it)->get_type());
      }
    }
    uint32_t right_col_index = op->get_col_right()->get_index();
    cols = rels[1]->get_columns();
    index = 0;
    for (vector<Column*>::iterator it = cols.begin(); it != cols.end();
         ++it, ++index) {
      if ((*it)->get_index() != boost::lexical_cast<int32_t>(right_col_index)) {
        update_remainder += "record." +
          Column::indexString((*it)->get_index()) + ", ";
        col_types.push_back((*it)->get_type());
      }
    }
    update_remainder.erase(update_remainder.end() - 2, update_remainder.end());
    update_remainder += ")";
    dict->SetValue("UPDATE_REMAINDER", "new " + GenerateType(col_types) +
                   update_remainder);
    dict->SetValue("REMAINDER_TYPE", GenerateType(remainder_cols));
    dict->SetValue("BUILD_REMAINDER", build_remainder);
  }

  string TranslatorNaiad::GenerateReducerClass(TemplateDictionary* dict, const string& reducer_name,
                                               string op, vector<Column*> group_bys,
                                               vector<Column*> sel_cols) {
    string reducer_vars = "";
    string reducer_args = "";
    string reducer_constructor = "";
    string update_vars = "";
    string val_selector = "new " + reducer_name + "(";
    string flatener = "new ";
    vector<uint16_t> col_types_out;
    for (vector<Column*>::iterator it = group_bys.begin();
         it != group_bys.end(); ++it) {
      col_types_out.push_back((*it)->get_type());
    }
    uint32_t index = 0;
    for (vector<Column*>::iterator it = sel_cols.begin(); it != sel_cols.end();
         ++it, ++index) {
      col_types_out.push_back((*it)->get_type());
      vector<Column*> col;
      col.push_back(*it);
      string var_name = "value" + boost::lexical_cast<string>(index);
      reducer_vars += "public " + GenerateType(col) + " " + var_name + ";\n";
      reducer_args += GenerateType(col) + " " + var_name + ", ";
      reducer_constructor += "this." + var_name + " = " + var_name + ";\n";
      if (index == 0) {
        if (!op.compare("+") || !op.compare("*")) {
          update_vars += var_name + " " + op + "= other." + var_name + ";\n";
        } else if (!op.compare("min")) {
          update_vars += var_name + " = Math.Min(" + var_name + ", other." +
            var_name + ");\n";
        } else if (!op.compare("max")) {
          update_vars += var_name + " = Math.Max(" + var_name + ", other." +
            var_name + ");\n";
        } else if (!op.compare("count")) {
          update_vars += var_name + " += other." + var_name + ";\n";
        } else {
          LOG(ERROR) << "Unexpected operator type: " << op;
        }
      }
      if (index == 0 && !op.compare("count")) {
        val_selector += "1, ";
      } else {
        val_selector += "row." + Column::indexString((*it)->get_index()) + ", ";
      }
    }
    flatener += GenerateType(col_types_out) + "(";
    index = 0;
    if (group_bys.size() > 1) {
      for (vector<Column*>::iterator it = group_bys.begin();
           it != group_bys.end(); ++it, ++index) {
        flatener += "row.First." + Column::indexString(index) + ", ";
      }
    } else {
      flatener += "row.First, ";
    }
    index = 0;
    for (vector<Column*>::iterator it = sel_cols.begin();
         it != sel_cols.end(); ++it, ++index) {
      flatener += "row.Second.value" + boost::lexical_cast<string>(index) + ", ";
    }
    reducer_args.erase(reducer_args.end() - 2, reducer_args.end());
    val_selector.erase(val_selector.end() - 2, val_selector.end());
    val_selector += ")";
    flatener.erase(flatener.end() - 2, flatener.end());
    flatener += ")";
    dict->SetValue("REDUCER_VARS", reducer_vars);
    dict->SetValue("REDUCER_ARGS", reducer_args);
    dict->SetValue("REDUCER_CONSTRUCTOR", reducer_constructor);
    dict->SetValue("UPDATE_VARS", update_vars);
    dict->SetValue("REDUCER_NAME", reducer_name);
    dict->SetValue("VAL_SELECTOR", val_selector);
    dict->SetValue("FLATENER", flatener);
    return flatener;
  }

  string TranslatorNaiad::GenerateKeyOut(uint32_t num) {
    string key_out = "";
    if (num == 1) {
      return "key,";
    }
    for (uint32_t index = 0; index < num; index++) {
      key_out += "key." +  Column::indexString(index) + ",";
    }
    return key_out;
  }

  string TranslatorNaiad::GenerateType(Relation* relation) {
    string type = "";
    vector<Column*> cols = relation->get_columns();
    if (cols.size() == 1) {
      return Column::stringPrimitiveTypeCSharp(cols[0]->get_type());
    }
    vector<uint16_t> types;
    for (vector<Column*>::iterator it = cols.begin(); it != cols.end(); ++it) {
      type += (*it)->translateTypeCSharp();
      types.push_back((*it)->get_type());
    }
    // We only need to add types for which we need to generate structs.
    if (unique_generated_types_.find(type) == unique_generated_types_.end()) {
      unique_generated_types_.insert(pair<string, vector<uint16_t> >(type, types));
    }
    return type;
  }

  string TranslatorNaiad::GenerateType(const vector<uint16_t>& types) {
    string type;
    if (types.size() == 1) {
      return Column::stringPrimitiveTypeCSharp(types[0]);
    }
    for (vector<uint16_t>::const_iterator it = types.begin(); it != types.end(); ++it) {
      type += Column::stringTypeCSharp(*it);
    }
    // We only need to add types for which we need to generate structs.
    if (unique_generated_types_.find(type) == unique_generated_types_.end()) {
      unique_generated_types_.insert(pair<string, vector<uint16_t> >(type, types));
    }
    return type;
  }

  string TranslatorNaiad::GenerateType(const vector<Column*>& cols) {
    vector<uint16_t> cols_type;
    if (cols.size() == 1) {
      return Column::stringPrimitiveTypeCSharp(cols[0]->get_type());
    }
    for (vector<Column*>::size_type index = 0; index < cols.size(); index++) {
      cols_type.push_back(cols[index]->get_type());
    }
    return GenerateType(cols_type);
  }

  // Generate body of function to select the given columns.
  string TranslatorNaiad::GenerateSelectCols(string rel_name, const vector<Column*>& cols) {
    string output = "";
    if (cols.size() > 1) {
      output = "new ";
      string cols_select;
      for (vector<Column*>::const_iterator it = cols.begin();
           it != cols.end(); ++it) {
        output += (*it)->translateTypeCSharp();
        cols_select += rel_name + "." + Column::indexString((*it)->get_index()) + ",";
      }
      output += "(" + cols_select.substr(0, cols_select.size() - 1) + ")";
    } else {
      output = rel_name + "." + Column::indexString(cols[0]->get_index());
    }
    return output;
  }

  string TranslatorNaiad::GenerateOutput(string rel_name,
                                         const vector<int32_t>& indexes_selected) {
    string output = "";
    for (vector<int32_t>::size_type index = 0;
         index < indexes_selected.size() - 1; index++) {
      output += rel_name + "." + Column::indexString(indexes_selected[index]) + ", ";
    }
    if (indexes_selected.size() > 0) {
      output += rel_name + "." +
        Column::indexString(indexes_selected[indexes_selected.size() - 1]);
    }
    return output;
  }

  string TranslatorNaiad::GenerateOutput(string rel_name,
                                         OperatorInterface* op,
                                         const vector<Value*>& values,
                                         string math_op,
                                         bool single_value) {
    string output = "";
    Column* left_column = dynamic_cast<Column*>(values[0]);
    Column* right_column = dynamic_cast<Column*>(values[1]);
    int32_t left_index = -1;
    int32_t right_index = -1;
    if (left_column != NULL) {
      left_index = left_column->get_index();
    }
    if (right_column != NULL) {
      right_index = right_column->get_index();
    }
    vector<Column*> cols = op->get_relations()[0]->get_columns();
    for (vector<Column*>::size_type index = 0; index < cols.size(); index++) {
      if (left_index == boost::lexical_cast<int32_t>(index)) {
        if (single_value) {
          output += rel_name + " " + math_op + " ";
        } else {
          output += rel_name + "." + Column::indexString(index) + " " +
            math_op + " ";
        }
        if (right_index == -1) {
          output += values[1]->get_value() + ",";
        } else {
          if (single_value) {
            output += rel_name + ",";
          } else {
            output += rel_name + "." + Column::indexString(right_index) + ",";
          }
        }
      } else if (right_index == boost::lexical_cast<int32_t>(index)) {
        if (left_index == -1) {
          if (single_value) {
            output += values[0]->get_value() + " " + math_op + " " + rel_name +
              ",";
          } else {
            output += values[0]->get_value() + " " + math_op + " " + rel_name +
              "." + Column::indexString(index) + ",";
          }
        } else {
          if (single_value) {
            output += rel_name + ",";
          } else {
            output += rel_name + "." + Column::indexString(index) + ",";
          }
        }
      } else {
        if (single_value) {
          output += rel_name + ",";
        } else {
          output += rel_name + "." + Column::indexString(index) + ",";
        }
      }
    }
    if (output.size() > 0) {
      return output.substr(0, output.size() - 1);
    }
    return output;
  }

  bool TranslatorNaiad::SameColumns(const vector<Column*>& in_cols,
                                    const vector<Column*>& cols) {
    if (in_cols.size() == cols.size()) {
      for (vector<Column*>::size_type index = 0; index != cols.size(); index++) {
        if (in_cols[index]->get_index() != cols[index]->get_index()) {
          return false;
        }
      }
    } else {
      return false;
    }
    return true;
  }

  string TranslatorNaiad::Compile(const string& code,
                                  const vector<Relation*>& input_rels,
                                  const set<shared_ptr<OperatorNode> >& leaves) {
    // Create the directory where the code will be generated.
    string create_src_dir = "mkdir -p " + FLAGS_generated_code_dir + "Musketeer/";
    std::system(create_src_dir.c_str());

    string source_file = GetSourcePath();
    string binary_file = GetBinaryPath();
    string program_file = FLAGS_generated_code_dir + "Musketeer/Program.cs";
    string project_file = FLAGS_generated_code_dir + "Musketeer/Musketeer.csproj";
    string project_sln_file =  FLAGS_generated_code_dir + "Musketeer/Musketeer.sln";
    string hdfs_put_file = FLAGS_generated_code_dir + "Musketeer/hdfs_put.sh";
    string hdfs_get_file = FLAGS_generated_code_dir + "Musketeer/hdfs_get.sh";
    TemplateDictionary dict("program");
    dict.SetValue("NAIAD_DIR", FLAGS_naiad_dir);
    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("TMP_ROOT", FLAGS_tmp_data_dir);
    dict.SetValue("HDFS_MASTER", FLAGS_hdfs_master);
    dict.SetValue("HDFS_PORT", FLAGS_hdfs_port);
    string input_paths;
    string hdfs_gets;
    for (vector<Relation*>::const_iterator it = input_rels.begin();
         it != input_rels.end(); ++it) {
      string input_rel_name = (*it)->get_name();
      input_paths += "copy_file(distfs, \"" + FLAGS_hdfs_input_dir +
        input_rel_name + "\");\n";
      hdfs_gets += "rm -r " + FLAGS_tmp_data_dir + FLAGS_hdfs_input_dir +
        input_rel_name + "/\n";
      hdfs_gets += "mkdir -p " + FLAGS_tmp_data_dir + FLAGS_hdfs_input_dir +
        input_rel_name + "/\n";
      hdfs_gets += "hadoop fs -get " + FLAGS_hdfs_input_dir + input_rel_name + "/" +
        input_rel_name + "$procid.in " + FLAGS_tmp_data_dir +
        FLAGS_hdfs_input_dir + input_rel_name + "/\n";
    }
    string hdfs_puts;
    for (set<shared_ptr<OperatorNode> >::const_iterator it = leaves.begin();
         it != leaves.end(); ++it) {
      string output_rel_name =
        (*it)->get_operator()->get_output_relation()->get_name();
      hdfs_puts += "hadoop fs -mkdir " + FLAGS_hdfs_input_dir + output_rel_name +
        " ;  hadoop fs -put " + FLAGS_tmp_data_dir + output_rel_name +
        "*.out " + FLAGS_hdfs_input_dir + output_rel_name + "/ ; rm " +
        FLAGS_tmp_data_dir + output_rel_name + "*.out ; ";
    }
    dict.SetValue("INPUT_PATHS", input_paths);
    dict.SetValue("HDFS_GETS", hdfs_gets);
    string program_code;
    ExpandTemplate(FLAGS_naiad_templates_dir + "NaiadMusketeer/Program.cs", ctemplate::DO_NOT_STRIP,
                   &dict, &program_code);
    string project_code;
    ExpandTemplate(FLAGS_naiad_templates_dir + "NaiadMusketeer/Musketeer.csproj",
                   ctemplate::DO_NOT_STRIP, &dict, &project_code);
    string project_sln;
    ExpandTemplate(FLAGS_naiad_templates_dir + "NaiadMusketeer/Musketeer.sln",
                   ctemplate::DO_NOT_STRIP, &dict, &project_sln);
    string hdfs_gets_code;
    ExpandTemplate(FLAGS_naiad_templates_dir + "NaiadMusketeer/hdfs_get_template.sh",
                   ctemplate::DO_NOT_STRIP, &dict, &hdfs_gets_code);
    ofstream program_file_stream;
    program_file_stream.open(program_file.c_str());
    program_file_stream << program_code;
    program_file_stream.close();
    ofstream project_file_stream;
    project_file_stream.open(project_file.c_str());
    project_file_stream << project_code;
    project_file_stream.close();
    ofstream project_sln_stream;
    project_sln_stream.open(project_sln_file.c_str());
    project_sln_stream << project_sln;
    project_sln_stream.close();
    ofstream job_file;
    job_file.open(source_file.c_str());
    job_file << code;
    job_file.close();
    ofstream hdfs_get_file_stream;
    hdfs_get_file_stream.open(hdfs_get_file.c_str());
    hdfs_get_file_stream << hdfs_gets_code;
    hdfs_get_file_stream.close();
    ofstream hdfs_put_file_stream;
    hdfs_put_file_stream.open(hdfs_put_file.c_str());
    hdfs_put_file_stream << hdfs_puts;
    hdfs_put_file_stream.close();
    string copy_get_proc_id = "cp " + FLAGS_naiad_templates_dir +
      "NaiadMusketeer/get_proc_id.sh " + FLAGS_generated_code_dir + "Musketeer/";
    std::system(copy_get_proc_id.c_str());
    string copy_env_fetcher = "cp " + FLAGS_naiad_templates_dir + "env.sh " +
      FLAGS_generated_code_dir + "Musketeer/";
    std::system(copy_env_fetcher.c_str());
    string copy_app_config = "cp " + FLAGS_naiad_templates_dir +
      "NaiadMusketeer/App.config " + FLAGS_generated_code_dir + "Musketeer/";
    std::system(copy_app_config.c_str());
    string copy_app_properties = "cp -r " + FLAGS_naiad_templates_dir +
      "NaiadMusketeer/Properties " + FLAGS_generated_code_dir + "Musketeer/";
    std::system(copy_app_properties.c_str());
    LOG(INFO) << "naiad build started for: " << class_name;
    timeval start_compile;
    gettimeofday(&start_compile, NULL);
    string compile_cmd = "cd " + FLAGS_generated_code_dir + "Musketeer/" +
      "; xbuild /p:Configuration=release";
    std::system(compile_cmd.c_str());
    string rsync_cmd = "parallel-ssh -h " + FLAGS_naiad_hosts_file +
      " -p 50 -i \"rsync -avz --exclude '*.git*' --exclude '*.out' `hostname`:" +
      FLAGS_generated_code_dir + " " + FLAGS_generated_code_dir + "\"";
    std::system(rsync_cmd.c_str());
    LOG(INFO) << "naiad build ended for: " << class_name;
    timeval end_compile;
    gettimeofday(&end_compile, NULL);
    uint32_t compile_time = end_compile.tv_sec - start_compile.tv_sec;
    cout << "COMPILE TIME: " << compile_time << endl;
    return binary_file + " " + class_name;
  }

  bool TranslatorNaiad::MustGenerateCode(shared_ptr<OperatorNode> node) {
    OperatorInterface* op = node->get_operator();
    if (node->get_children().size() != 1 || node->get_loop_children().size() > 0 ||
        op->get_type() == BLACK_BOX_OP || op->get_type() == DIFFERENCE_OP ||
        op->get_type() == DISTINCT_OP || op->get_type() == INTERSECTION_OP ||
        op->get_type() == SORT_OP || op->get_type() == UDF_OP ||
        op->get_type() == UNION_OP || op->get_type() == WHILE_OP) {
      return true;
    }
    if (op->get_type() == SELECT_OP) {
      SelectOperator* select_op = dynamic_cast<SelectOperator*>(op);
      vector<Column*> in_cols = select_op->get_relations()[0]->get_columns();
      vector<Column*> cols = select_op->get_columns();
      if (SameColumns(in_cols, cols)) {
        return true;
      }
    }
    if (op->get_type() == PROJECT_OP) {
      ProjectOperator* project_op = dynamic_cast<ProjectOperator*>(op);
      vector<Column*> in_cols = project_op->get_relations()[0]->get_columns();
      vector<Column*> cols = project_op->get_columns();
      if (SameColumns(in_cols, cols)) {
        return true;
      }
    }
    if (op->get_type() == AGG_OP) {
      AggOperator* agg_op = dynamic_cast<AggOperator*>(op);
      if (!agg_op->get_operator().compare("-") ||
          !agg_op->get_operator().compare("/")) {
        return true;
      }
    }
    OperatorInterface* child_op = node->get_children()[0]->get_operator();
    if (child_op->get_condition_tree() != NULL) {
      return true;
    }
    if (child_op->get_type() == DIV_OP || child_op->get_type() == MUL_OP ||
        child_op->get_type() == SUB_OP || child_op->get_type() == SUM_OP ||
        child_op->get_type() == PROJECT_OP || child_op->get_type() == SELECT_OP) {
      return false;
    }
    return true;
  }

} // namespace translator
} // namespace musketeer
