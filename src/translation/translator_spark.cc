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

#include "translation/translator_spark.h"

#include <boost/lexical_cast.hpp>
#include <ctemplate/template.h>
#include <sys/time.h>

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <string>

namespace musketeer {
namespace translator {

  using ctemplate::mutable_default_template_cache;

  map<string, Relation*> TranslatorSpark::relations;

  TranslatorSpark::TranslatorSpark(const op_nodes& dag,
                                   const string& class_name):
    TranslatorInterface(dag, class_name) {
    cur_job_code = NULL;
  }

  string TranslatorSpark::GetOutputPath(OperatorInterface* op) {
    string relation = op->get_output_relation()->get_name();
    return op->get_input_dir() + relation + "/";
  }

  string TranslatorSpark::GetBinaryPath(OperatorInterface* op) {
    return op->get_code_dir() + class_name + "_code/" + class_name + ".jar";
  }

  string TranslatorSpark::GetSourcePath(OperatorInterface* op) {
    return op->get_code_dir() + class_name + "_code/" + class_name + ".scala";
  }

  string TranslatorSpark::GetOutputPath(const string& class_name,
                                        const string& hdfs_input_dir) {
    return hdfs_input_dir + class_name + "/";
  }

  string TranslatorSpark::GetBinaryPath(const string& class_name,
                                        const string& code_dir) {
    return code_dir + class_name + "_code/" + class_name + ".jar";
  }

  string TranslatorSpark::GetSourcePath(const string& class_name,
                                        const string& code_dir) {
    return code_dir + class_name + "_code/" + class_name + ".scala";
  }

  // TODO(ionel): Port this from the old way that uses case to the new way of
  // implementing operators.
  string TranslatorSpark::GenCrossJoin(Relation* rel1, Relation* rel2) {
    vector<Column*> columns1 = rel1->get_columns();
    vector<Column*> columns2 = rel2->get_columns();
    int nb_cols1 = columns1.size();
    int nb_cols2 = columns2.size();
    string tuple_type1 = "(";
    string tuple_type2 = "(";
    string tuple_type_join1 = "";
    string tuple_type_join2 = "";
    string tuple_type_join1_default = "";
    string tuple_type_join2_default = "";
    string flat_map ="";
    string join = "(";
    string default_join = "(";

    int i = 0;
    for (vector<Column*>::const_iterator it = columns1.begin(); it != columns1.end(); ++it) {
      string var = "x" + boost::lexical_cast<string>(i);
      tuple_type1 += var;
      tuple_type_join1 += var;
      tuple_type_join1_default += OutputDefaultForType((*it)->get_type());
      if (++i < nb_cols1) {
        tuple_type1 += ",";
        tuple_type_join1 += ",";
        tuple_type_join1_default += ",";
      }
    }
    tuple_type1 += ")";
    i = 0;
    for (vector<Column*>::const_iterator it = columns2.begin(); it != columns2.end(); ++it) {
      string var = "y" + boost::lexical_cast<string>(i);
      tuple_type2 += var;
      tuple_type_join2 += var;
      tuple_type_join2_default += OutputDefaultForType((*it)->get_type());
      if (++i < nb_cols2) {
        tuple_type2 += ",";
        tuple_type_join2 += ",";
        tuple_type_join2_default += ",";
      }
    }
    tuple_type2 += ")";
    if (nb_cols1 > 1) {
      join = tuple_type_join1;
    }
    if (nb_cols2 > 1) {
      join += "," + tuple_type_join2;
    }
    join += ")";
    if (nb_cols1 > 1) {
      default_join = tuple_type_join1_default;
    }
    if (nb_cols2 > 1) {
      default_join += "," + tuple_type_join2_default;
    }
    default_join += ")";

    flat_map = "({ case ((" + tuple_type1 + " , " + tuple_type2 +  ")) => ";
    flat_map += "( " + join + " \n ";
    flat_map += "case _  => (" +  default_join + "})";
    return flat_map;
  }

  string TranslatorSpark::GenFlatJoin(const string& input_rel_name, uint16_t col_type,
                                      int32_t col1_index, int32_t col2_index,
                                      Relation* rel1, Relation* rel2) {
    vector<Column*> cols = rel1->get_columns();
    string output = "(";
    bool past_join_col = false;
    for (vector<Column*>::iterator it = cols.begin(); it != cols.end(); ++it) {
      int32_t cur_index = (*it)->get_index();
      if (cur_index == col1_index) {
        past_join_col = true;
        output += input_rel_name + "._1, ";
      } else {
        if (past_join_col) {
          if (cols.size() > 2) {
            output += input_rel_name + "._2._1._" + boost::lexical_cast<string>(cur_index) + ", ";
          } else {
            output += input_rel_name + "._2._1"  + ", ";
            }
          } else {
          if (cols.size() > 2) {
            output += input_rel_name + "._2._1._" + boost::lexical_cast<string>(cur_index + 1) +
              ", ";
          } else {
            output += input_rel_name + "._2._1" + ", ";
          }
        }
      }
    }
    cols = rel2->get_columns();
    // More than just one extra elemenet besides the join key.
    if (cols.size() > 2) {
      for (uint32_t cur_index = 1; cur_index < cols.size(); cur_index++) {
        output += input_rel_name + "._2._2._" + boost::lexical_cast<string>(cur_index) + ", ";
      }
      output.erase(output.end() - 2, output.end());
    } else {
      output += input_rel_name + "._2._2";
    }
    return output + ")";
  }

  string TranslatorSpark::GenOutputCols(const string& name,
                                        const vector<Column*>& cols) {
    string agg_cols = "";
    for (vector<Column*>::const_iterator it = cols.begin(); it != cols.end();
         ++it) {
      agg_cols += name + boost::lexical_cast<string>((*it)->get_index() + 1) + ", ";
    }
    agg_cols.erase(agg_cols.end() - 2, agg_cols.end());
    return agg_cols;
  }

  string TranslatorSpark::GenOutputCols(const string& name,
                                        int32_t agg_col_index,
                                        const vector<Column*>& cols) {
    string sel_cols = name + boost::lexical_cast<string>(agg_col_index);
    for (vector<Column*>::const_iterator it = cols.begin(); it != cols.end();
         ++it) {
      sel_cols += ", " + name + boost::lexical_cast<string>((*it)->get_index() + 1);
    }
    return sel_cols;
  }

  // Pass col_index = -1 if there is no index col.
  string TranslatorSpark::GenerateFlatMapping(const string& input_name, int32_t col_index,
      const vector<Column*>& selected_cols, const vector<Column*>& groupby_cols,
      const vector<Column*>& columns) {
    string flat_mappings = "(";
    if (groupby_cols.size() > 1) {
      uint32_t index = 0;
      for (vector<Column*>::const_iterator it = groupby_cols.begin();
           it != groupby_cols.end(); ++it) {
        flat_mappings += input_name + "._1._" + boost::lexical_cast<string>(++index) + ", ";
      }
      flat_mappings.erase(flat_mappings.end() - 2, flat_mappings.end());
    } else {
      flat_mappings += input_name + "._1";
    }
    if (selected_cols.size() > 0) {
      uint32_t offset = 0;
      if (col_index >= 0) {
        flat_mappings += ", " + input_name + "._2._1";
        offset = 1;
      }
      if (col_index < 0 && selected_cols.size() == 1) {
        flat_mappings += ", " + input_name + "._2";
      } else {
        for (uint32_t index = 0; index < selected_cols.size(); ++index) {
          flat_mappings += ", " + input_name + "._2._" +
            boost::lexical_cast<string>(index + offset + 1);
        }
      }
    } else {
      flat_mappings += ", " + input_name + "._2";
    }
    return flat_mappings + ")";
  }

  string TranslatorSpark::GenFlatMappingConvert(const string& input_name,
                                                const vector<Column*>& columns) {
    string output = "(";
    if (columns.size() > 1) {
      uint32_t index = 0;
      for (vector<Column*>::const_iterator it = columns.begin(); it != columns.end(); ++it) {
        output += input_name + "._1._" + boost::lexical_cast<string>(++index) + ", ";
      }
    } else {
      output = "(" + input_name + "._1, ";
    }
    return output + input_name + "._2.length)";
  }

  string TranslatorSpark::GenerateCode() {
    string code_dir = dag[0]->get_operator()->get_code_dir();
    string output_dir = dag[0]->get_operator()->get_input_dir();
    string bin_name = GetBinaryPath(class_name, code_dir);
    string output_path = GetOutputPath(class_name, output_dir);
    set<string> nodelist = set<string>();
    set<string> inputs = set<string>();
    DetermineInputsSpark(dag, &inputs, &nodelist);
    PrintStringSet("Inputs are ", inputs);
    string header = TranslateHeader(class_name, bin_name, inputs, nodelist, output_dir);
    string ops;
    set<shared_ptr<OperatorNode> > leafs = set<shared_ptr<OperatorNode> >();
    set<string> proc = inputs;
    TranslateDAG(&ops, dag, &leafs, &proc);
    LOG(INFO) << "Size of leaves " << leafs.size();
    string code = header + ops + TranslateTail(leafs, output_path);
    return Compile(code, code_dir);
  }

  void TranslatorSpark::TranslateDAG(string* code, const op_nodes& next_set,
                                     set<shared_ptr<OperatorNode> >* leaves,
                                     set<string>* processed) {
    for (op_nodes::const_iterator it = next_set.begin(); it != next_set.end(); ++it) {
      shared_ptr<OperatorNode> node = *it;
      OperatorInterface* op = (*it)->get_operator();
      string output_rel = op->get_output_relation()->get_name();
      LOG(INFO) << "Translating for " << output_rel;
      // Determine whether should cache or not
      if (node->get_children().size() > 1) {
        to_cache[output_rel] = true;
        // In Spark, we use the following heuristic: cache if
        // more than two operations use the same RDD, so if
        // more than one child
      }
      if (CanSchedule(op, processed)) {
        SparkJobCode* job_code = dynamic_cast<SparkJobCode*>(TranslateOperator(op));
        string out_rel_name = op->get_output_relation()->get_name();
        bool code_generated_cur_op = false;
        if (MustGenerateCode(node)) {
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
              out_fun_dict.SetValue("NEXT_OPERATOR", job_code->get_out_fun_code() +
                                    job_code->get_rel_out_name() + "_local");
              string out_fun_code = "";
              ExpandTemplate("out_fun", ctemplate::DO_NOT_STRIP, &out_fun_dict, &out_fun_code);
              mutable_default_template_cache()->ClearCache();
              // Generate code for the operator
              TemplateDictionary dict("code");
              string tmp_op_code = cur_job_code->get_code();
              StringToTemplateCache("code", tmp_op_code, ctemplate::DO_NOT_STRIP);
              dict.SetValue("OUTPUT", out_rel_name);
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
              dict_cur.SetValue("OUTPUT", cur_job_code->get_rel_out_name());
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
            dict.SetValue("OUTPUT", out_rel_name);
            dict.SetValue("NEXT_OPERATOR", job_code->get_out_fun_code() +
                          job_code->get_rel_out_name() + "_local");
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
          cur_job_code->set_out_fun_code(next_out_fun_code + "{{NEXT_OPERATOR}}");
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
            dict_cur.SetValue("OUTPUT", cur_job_code->get_rel_out_name());
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
          *code += "  " + rel_name + " = " + tmp_rel_name + "\n";
        }
        processed->insert(output_rel);
        if (!node->IsLeaf()) {
          if (node->get_operator()->get_type() == WHILE_OP) {
            TranslateDAG(code, node->get_loop_children(), leaves, processed);
            // TODO(ionel): Rel names should keep track of names already defined.
            // (e.g. when we have a while loop within a while loop.
            set<string> rel_names = set<string>();
            dynamic_cast<WhileOperator*>(
                node->get_operator())->get_condition_tree()->getRelNames(&rel_names);
            string update = GenUpdateConditionInput(rel_names);
            *code += update;
            *code += "\n}\n";
          }
          TranslateDAG(code, node->get_children(), leaves, processed);
        }
      } else {
        LOG(INFO) << "Cannot schedule operator yet: "
                  << op->get_output_relation()->get_name();
      }
    }
  }

  // Check if all the inputs of the operator have been processed.
  bool TranslatorSpark::CanSchedule(OperatorInterface* op,
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
        LOG(INFO) << "Cannot schedule yet: " << (*it)->get_name()
                  << " is missing";
        return false;
      }
    }
    LOG(INFO) << "Can Schedule ";
    return true;
  }

  string TranslatorSpark::GenMinMaxTupleType(Column* col, const vector<Column*>& sel_cols) {
    string res = "(" + col->translateTypeScala() + ", ";
    for (vector<Column*>::const_iterator it = sel_cols.begin(); it != sel_cols.end(); ++it) {
      res += (*it)->translateTypeScala() + ", ";
    }
    res.erase(res.end() - 2, res.end());
    res += ")";
    return res;
  }

  string TranslatorSpark::GenTupleType(const vector<Column*>& columns) {
    string res = "(";
    for (vector<Column*>::const_iterator it = columns.begin(); it != columns.end(); ++it) {
      res += (*it)->translateTypeScala() + ", ";
    }
    res.erase(res.end() - 2, res.end());
    res += ")";
    return res;
  }

  string TranslatorSpark::GenTupleType(Relation* relation) {
    return GenTupleType(relation->get_columns());
  }

  string TranslatorSpark::GenJoinTupleType(const vector<Column*>& columns,
                                           const vector<Column*>& group_bys) {
    string res = "(";
    for (vector<Column*>::const_iterator it = columns.begin(); it != columns.end(); ++it) {
      bool col_in_keys = false;
      int32_t cur_col_index = (*it)->get_index();
      for (vector<Column*>::const_iterator kit = group_bys.begin(); kit != group_bys.end();
           ++kit) {
        if (cur_col_index == (*kit)->get_index()) {
          col_in_keys = true;
          break;
        }
      }
      if (!col_in_keys) {
        res += (*it)->translateTypeScala() + ", ";
      }
    }
    res.erase(res.end() - 2, res.end());
    res += ")";
    return res;
  }

  string TranslatorSpark::GenTupleVar(const string& input_name, Relation* rel,
                                      const vector<Column*>& key_cols) {
    vector<Column*> columns = rel->get_columns();
    string res = "(";
    for (vector<Column*>::const_iterator it = columns.begin(); it != columns.end(); ++it) {
      bool col_in_keys = false;
      int32_t cur_col_index = (*it)->get_index();
      for (vector<Column*>::const_iterator kit = key_cols.begin(); kit != key_cols.end();
           ++kit) {
        if (cur_col_index == (*kit)->get_index()) {
          col_in_keys = true;
          break;
        }
      }
      // Append the column with its current index because we haven't prunned the relation
      // at this point.
      if (!col_in_keys) {
        res += input_name + "._" + boost::lexical_cast<string>(cur_col_index + 1) + ", ";
      }
    }
    res.erase(res.end() - 2, res.end());
    res += ")";
    return res;
  }

  string TranslatorSpark::GenTypedRDD(Relation* relation) {
    TemplateDictionary dict("mappedRDD");
    vector<Column*> columns = relation->get_columns();
    int nb_cols = columns.size();
    dict.SetValue("REL_NAME", relation->get_name());
    string args = "(";
    int i = 0;
    for (vector<Column*>::const_iterator it = columns.begin(); it != columns.end(); ++it) {
      args += "splitted(" + boost::lexical_cast<std::string>(i++) + ").to" +
        (*it)->translateTypeScala();
      if (i < nb_cols) {
        args += ",";
      }
    }
    args += ")";
    return args;
  }

  string TranslatorSpark::TranslateHeader(const string& name,
    const string& bin_name, const set<string>& inputs,
    const set<string>& processed, const string& output_dir) {
    string header;
    TemplateDictionary dict("header");
    dict.SetValue("CLASS_NAME", name);
    dict.SetValue("SPARK_MASTER", FLAGS_spark_master);
    dict.SetValue("SPARK_DIR", FLAGS_spark_dir);
    dict.SetValue("HDFS_MASTER", "hdfs://" + FLAGS_hdfs_master + ":" + FLAGS_hdfs_port);
    dict.SetValue("BIN_NAME", bin_name);
    ExpandTemplate(FLAGS_spark_templates_dir + "HeaderTemplate.scala",
                   ctemplate::DO_NOT_STRIP, &dict, &header);
    string inputs_st = header;
    for (set<string>::const_iterator it = inputs.begin(); it != inputs.end(); ++it) {
      string input = (*it);
      Relation* rel = relations[input];
      dict.SetValue("INPUT_PATH", GetOutputPath(input, output_dir));
      dict.SetValue("REL_NAME", input);
      dict.SetValue("ARGS", GenTypedRDD(rel));
      if (to_cache[input]) {
        //          dict.SetValue("TO_CACHE", ".cache()");
      }
      string save = "";
      ExpandTemplate(FLAGS_spark_templates_dir + "InputTemplate.scala",
                     ctemplate::DO_NOT_STRIP, &dict, &save);
      inputs_st += save + "\n";
    }

    for (set<string>::iterator it = processed.begin(); it != processed.end();
         ++it) {
      string rel_name = *it;
      if (find(inputs.begin(), inputs.end(), rel_name) == inputs.end()) {
        Relation* rel = relations[*it];
        if (rel->get_columns().size() > 0) {
          string type = GenTupleType(rel);
          inputs_st += " var " + rel->get_name() + ":org.apache.spark.rdd.RDD[" + type +
            "] = null.asInstanceOf[org.apache.spark.rdd.RDD[" + type + "]];\n";
        }
      }
    }
    return inputs_st;
  }

  vector<Relation*>* TranslatorSpark::DetermineInputsSpark(const op_nodes& dag,
                                                           set<string>* inputs,
                                                           set<string>* visited) {
    vector<Relation*> *input_rels_out = new vector<Relation*>;
    queue<shared_ptr<OperatorNode> > to_visit;
    for (op_nodes::const_iterator it = dag.begin(); it != dag.end(); ++it) {
      to_visit.push(*it);
      Relation* rel = (*it)->get_operator()->get_output_relation();
      visited->insert(rel->get_name());
      relations[rel->get_name()] = rel;
    }
    while (!to_visit.empty()) {
      shared_ptr<OperatorNode> node = to_visit.front();
      to_visit.pop();
      OperatorInterface* op = node->get_operator();
      LOG(INFO) << "VISITED: " << op->get_output_relation()->get_name();
      vector<Relation*> input_rels = op->get_relations();
      for (vector<Relation*>::iterator it = input_rels.begin();
           it != input_rels.end(); ++it) {
        // Rel is an input if rel not visited or doesn't have parents and
        // not already marked as an input.
        if ((node->get_parents().size() == 0 ||
             !IsGeneratedByOp((*it)->get_name(), node->get_parents())) &&
            visited->find((*it)->get_name()) == visited->end() &&
            inputs->find((*it)->get_name()) == inputs->end()) {
          LOG(INFO) << (*it)->get_name() << " is an input";
          inputs->insert((*it)->get_name());
          input_rels_out->push_back(*it);
          relations[(*it)->get_name()]= *it;
        } else {
          relations[(*it)->get_name()] = *it;
          LOG(INFO) << (*it)->get_name() << " is not an input";
        }
      }
      op_nodes non_loop_children = node->get_children();
      op_nodes children = node->get_loop_children();
      children.insert(children.end(), non_loop_children.begin(),
                      non_loop_children.end());
      for (op_nodes::iterator it = children.begin(); it != children.end();
           ++it) {
        bool can_add = true;
        vector<Relation*> input_rels = (*it)->get_operator()->get_relations();
        for (vector<Relation*>::iterator input_it = input_rels.begin();
             input_it != input_rels.end(); ++input_it) {
          if (visited->find((*input_it)->get_name()) == visited->end() &&
              inputs->find((*input_it)->get_name()) == inputs->end() &&
              IsGeneratedByOp((*input_it)->get_name(), node->get_parents())) {
            can_add = false;
            break;
          }
        }
        if (can_add || op->get_type() == WHILE_OP ||
            (*it)->get_operator()->get_type() == WHILE_OP) {
          Relation* rel = (*it)->get_operator()->get_output_relation();
          relations[rel->get_name()] = rel;
          if (visited->insert(rel->get_name()).second) {
            to_visit.push(*it);
          }
        }
      }
    }
    return input_rels_out;
  }

  string TranslatorSpark::TranslateTail(const set<shared_ptr<OperatorNode> >& leaf_nodes,
                                        const string& output_path) {
    string tail;
    TemplateDictionary dict("tail");
    dict.SetValue("CLASS_NAME", class_name);
    dict.SetValue("OUTPUT_PATH", output_path);
    dict.SetValue("HDFS_MASTER", "hdfs://" + FLAGS_hdfs_master + ":" + FLAGS_hdfs_port);
    for (set<shared_ptr<OperatorNode> >::const_iterator it = leaf_nodes.begin();
         it != leaf_nodes.end(); ++it) {
      string save;
      Relation* output_rel = (*it)->get_operator()->get_output_relation();
      dict.SetValue("REL_NAME",
                    output_rel->get_name());
      dict.SetValue("STRING_MAPPING", OutputMapping(output_rel));
      dict.SetValue("OUTPUT_PATH", GetOutputPath((*it)->get_operator()));
      ExpandTemplate(FLAGS_spark_templates_dir + "SaveTemplate.scala",
                     ctemplate::DO_NOT_STRIP, &dict, &save);
      tail += save + ";\n";
    }
    string end;
    ExpandTemplate(FLAGS_spark_templates_dir + "TailTemplate.scala", ctemplate::DO_NOT_STRIP,
                   &dict, &end);
    tail += end;
    return tail;
  }

  string TranslatorSpark::OutputMapping(Relation* rel) {
    vector<Column*> columns = rel->get_columns();
    uint32_t index = 0;
    string res = "(input:" + GenTupleType(rel) + ") => (";
    if (columns.size() > 1) {
      for (vector<Column*>::const_iterator it = columns.begin(); it != columns.end(); ++it) {
        res += "input._" + boost::lexical_cast<string>((*it)->get_index() + 1) + ".toString()";
        if (++index < columns.size()) {
          res += " + \" \" + ";
        }
      }
    } else {
      res += "input.toString()";
    }
    res += ")";
    return res;
  }

  SparkJobCode* TranslatorSpark::Translate(AggOperator* op) {
    TemplateDictionary dict("agg");
    Relation* input_rel = op->get_relations()[0];
    string input_name_local = input_rel->get_name() + "_local";
    string inputrel_format = GenTupleType(input_rel);
    vector<Column*> agg_cols = op->get_columns();
    string agg = GenerateAggShuffle(op->get_operator(), agg_cols,
                                    input_rel->get_columns().size());

    string output_row = "";
    if (op->hasGroupby()) {
      vector<Column*> group_bys = op->get_group_bys();
      inputrel_format = GenTupleType(agg_cols);
      agg = GenerateAgg(op->get_operator(), agg_cols);
      dict.SetValue("INPUT", "(" + input_name_local + ":(" + GenTupleType(group_bys) + ", " +
                    inputrel_format + "))");
      output_row = GenerateFlatMapping(input_name_local, -1, agg_cols, group_bys,
                                       input_rel->get_columns());
      dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
      dict.SetValue("GROUP_BY", GenTupleType(group_bys));
      dict.SetValue("GROUP_BY_KEY",
                    GenerateAggGroupBy(input_name_local, input_rel, group_bys, agg_cols));
    }

    PopulateCommonValues(op, &dict);
    PopulateCondition(op->get_condition_tree()->toString("spark"), input_rel->get_name(), &dict);
    dict.SetValue("AGG", agg);
    dict.SetValue("INPUTREL_TYPE", inputrel_format);
    string code;
    if (!(op->hasGroupby())) {
      dict.SetValue("OUTPUT_COLS", GenOutputCols("int_" + class_name + "._", agg_cols));
      ExpandTemplate(FLAGS_spark_templates_dir + "AggTemplate.scala",
                     ctemplate::DO_NOT_STRIP, &dict, &code);
    } else {
      ExpandTemplate(FLAGS_spark_templates_dir + "AggTemplateGroupBy.scala",
                     ctemplate::DO_NOT_STRIP, &dict, &code);
    }
    SparkJobCode* job_code = new SparkJobCode(op, code);
    string out_rel_name = op->get_output_relation()->get_name();
    job_code->set_rel_out_name(out_rel_name);
    job_code->set_out_fun_code("val " + out_rel_name + "_local = " + output_row + "; ");
    return job_code;
  }

  SparkJobCode* TranslatorSpark::Translate(CountOperator* op) {
    TemplateDictionary dict("count");
    Relation* input_rel = op->get_relations()[0];
    string input_name_local = input_rel->get_name() + "_local";
    string inputrel_format = GenTupleType(input_rel);

    string output_row = "";
    if (op->hasGroupby()) {
      vector<Column*> group_bys = op->get_group_bys();
      inputrel_format = "(Int)";
      dict.SetValue("INPUT", "(" + input_name_local + ":(" + GenTupleType(group_bys) +
                    ", Seq[" + inputrel_format + "]))");
      output_row = GenFlatMappingConvert(input_name_local, group_bys);
      dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
      dict.SetValue("GROUP_BY", GenTupleType(group_bys));
      dict.SetValue("GROUP_BY_KEY", GenerateCountGroupBy(input_name_local, input_rel, group_bys));
    }

    PopulateCommonValues(op, &dict);
    PopulateCondition(op->get_condition_tree()->toString("spark"), input_rel->get_name(), &dict);
    dict.SetValue("INPUTREL_TYPE", inputrel_format);

    string code;
    if (op->hasGroupby()) {
      ExpandTemplate(FLAGS_spark_templates_dir + "CountTemplateGroupBy.scala",
                     ctemplate::DO_NOT_STRIP, &dict, &code);
    } else {
      Column* col = op->get_column();
      int col_index = col->get_index() + 1;
      dict.SetValue("COL_INDEX", boost::lexical_cast<string>(col_index));
      dict.SetValue("COL_TYPE", col->translateTypeScala());
      ExpandTemplate(FLAGS_spark_templates_dir + "CountTemplate.scala",
                     ctemplate::DO_NOT_STRIP, &dict, &code);
    }
    SparkJobCode* job_code = new SparkJobCode(op, code);
    string out_rel_name = op->get_output_relation()->get_name();
    job_code->set_rel_out_name(out_rel_name);
    job_code->set_out_fun_code("val " + out_rel_name + "_local = " + output_row + "; ");
    return job_code;
  }

  SparkJobCode* TranslatorSpark::Translate(DifferenceOperator* op) {
    TemplateDictionary dict("difference");
    PopulateCommonValues(op, &dict);
    dict.SetValue("REL_NAME", op->get_relations()[0]->get_name());
    dict.SetValue("REL_NAME2", op->get_relations()[1]->get_name());
    string code;
    ExpandTemplate(FLAGS_spark_templates_dir + "DifferenceTemplate.scala",
                   ctemplate::DO_NOT_STRIP, &dict, &code);
    SparkJobCode* job_code = new SparkJobCode(op, code);
    job_code->set_rel_out_name(op->get_output_relation()->get_name());
    return job_code;
  }

  SparkJobCode* TranslatorSpark::Translate(DistinctOperator* op) {
    TemplateDictionary dict("distinct");
    PopulateCommonValues(op, &dict);
    dict.SetValue("REL_NAME", op->get_relations()[0]->get_name());
    string code;
    ExpandTemplate(FLAGS_spark_templates_dir + "DistinctTemplate.scala",
                   ctemplate::DO_NOT_STRIP, &dict, &code);
    SparkJobCode* job_code = new SparkJobCode(op, code);
    job_code->set_rel_out_name(op->get_output_relation()->get_name());
    return job_code;
  }

  SparkJobCode* TranslatorSpark::Translate(DivOperator* op) {
    return TranslateMathOp(op, op->get_values(), op->get_condition_tree(), "/");
  }

  SparkJobCode* TranslatorSpark::Translate(MulOperator* op) {
    return TranslateMathOp(op, op->get_values(), op->get_condition_tree(), "*");
  }

  SparkJobCode* TranslatorSpark::TranslateMathOp(OperatorInterface* op, vector<Value*> values,
                                                 ConditionTree* condition_tree, string math_op) {
    Relation* input_rel = op->get_relations()[0];
    string input_name = input_rel->get_name();
    string input_name_local = input_rel->get_name() + "_local";
    Relation* output_rel = op->get_output_relation();
    string maths = GenerateMaths(input_name_local, math_op, input_rel,
                                 values[0], values[1], output_rel);
    TemplateDictionary dict("math");
    PopulateCommonValues(op, &dict);
    PopulateCondition(condition_tree->toString("spark"), input_name, &dict);
    dict.SetValue("INPUT", "(" + input_name_local + ": " + GenTupleType(input_rel) + ")");
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string code = "";
    ExpandTemplate(FLAGS_spark_templates_dir + "MathTemplate.scala",
                   ctemplate::DO_NOT_STRIP, &dict, &code);
    SparkJobCode* job_code = new SparkJobCode(op, code);
    string out_rel_name = output_rel->get_name();
    job_code->set_rel_out_name(out_rel_name);
    job_code->set_out_fun_code("val " + out_rel_name + "_local = " + maths + "; ");
    return job_code;
  }

  SparkJobCode* TranslatorSpark::Translate(SumOperator* op) {
    return TranslateMathOp(op, op->get_values(), op->get_condition_tree(), "+");
  }

  SparkJobCode* TranslatorSpark::Translate(SubOperator* op) {
    return TranslateMathOp(op, op->get_values(), op->get_condition_tree(), "-");
  }

  SparkJobCode* TranslatorSpark::Translate(IntersectionOperator* op) {
    TemplateDictionary dict("intersection");
    PopulateCommonValues(op, &dict);
    dict.SetValue("REL_NAME", op->get_relations()[0]->get_name());
    dict.SetValue("REL_NAME2", op->get_relations()[1]->get_name());
    string code;
    ExpandTemplate(FLAGS_spark_templates_dir + "IntersectionTemplate.scala",
                   ctemplate::DO_NOT_STRIP, &dict, &code);
    SparkJobCode* job_code = new SparkJobCode(op, code);
    job_code->set_rel_out_name(op->get_output_relation()->get_name());
    return job_code;
  }

  string TranslatorSpark::GenerateAggShuffle(const string& op, const vector<Column*>& sel_cols,
                                             uint32_t num_input_cols) {
    string res = "(";
    if (sel_cols.size() == 1 && num_input_cols == 1) {
      res += "e1 " + op + " e2";
    } else {
      for (vector<Column*>::const_iterator it = sel_cols.begin(); it != sel_cols.end(); ++it) {
        string index = boost::lexical_cast<string>((*it)->get_index() + 1);
        res += "e1._" + index + " " + op + " e2._" + index + ", ";
      }
      res.erase(res.end() - 2, res.end());
    }
    for (int32_t index = 0; index < boost::lexical_cast<int32_t>(num_input_cols); ++index) {
      bool among_selected = false;
      for (vector<Column*>::const_iterator it = sel_cols.begin(); it != sel_cols.end(); ++it) {
        if ((*it)->get_index() == index) {
          among_selected = true;
          break;
        }
      }
      if (!among_selected) {
        res += ", e1._" + boost::lexical_cast<string>(index + 1);
      }
    }
    return res + ")";
  }

  string TranslatorSpark::GenerateAgg(const string& op, const vector<Column*>& sel_cols) {
    string res = "(";
    if (sel_cols.size() == 1) {
      res += "e1 " + op + " e2";
    } else {
      uint32_t cur_index = 0;
      for (vector<Column*>::const_iterator it = sel_cols.begin(); it != sel_cols.end(); ++it) {
        string index = boost::lexical_cast<string>(++cur_index);
        res += "e1._" + index + " " + op + " e2._" + index + ", ";
      }
      res.erase(res.end() - 2, res.end());
    }
    return res + ")";
  }

  string TranslatorSpark::GenerateMaths(const string& input_name, const string& op,
                                        Relation* rel, Value* left_val,
                                        Value* right_val, Relation* output_rel) {
    vector<Column*> columns = rel->get_columns();
    Column* left_column = dynamic_cast<Column*>(left_val);
    Column* right_column = dynamic_cast<Column*>(right_val);
    string left_value = "";
    string right_value = "";
    int32_t col_index_left = -1;
    int32_t col_index_right = -1;
    string maths = "(";

    // Assumption: no two constants
    if (left_column != NULL) {
      col_index_left = left_column->get_index();
    } else {
      left_value = left_val->get_value();
    }
    if (right_column != NULL) {
      col_index_right = right_column->get_index();
    } else {
      right_value = right_val->get_value();
    }
    uint32_t i = 0;
    for (vector<Column*>::const_iterator it = columns.begin(); it != columns.end(); ++it) {
      int32_t col_index = (*it)->get_index();
      if (col_index == col_index_left) {
        if (columns.size() > 1) {
          maths += "(" + input_name + "._" + boost::lexical_cast<string>(col_index + 1) +
            " " + op + " ";
        } else {
          maths += "(" + input_name + " " + op + " ";
        }
        if (col_index_right != -1) {
          if (columns.size() > 1) {
            maths += input_name + "._" + boost::lexical_cast<string>(col_index_right + 1) + ")";
          } else {
            maths += input_name + ")";
          }
        } else {
          maths += right_value + ")";
        }
      } else if (col_index == col_index_right && (col_index_left == -1)) {
        if (columns.size() > 1) {
          maths += "(" + input_name + "._" + boost::lexical_cast<string>(col_index + 1) + " " + op +
            " " + left_value + ")";
        } else {
          maths += "(" + input_name + " " + op + " " + left_value + ")";
        }
      } else {
        if (columns.size() > 1) {
          maths += input_name + "._" + boost::lexical_cast<string>(col_index + 1);
        } else {
          maths += input_name;
        }
      }
      if (++i < columns.size()) {
        maths += ", ";
      }
    }
    maths += ")";
    return maths;
  }

  SparkJobCode* TranslatorSpark::Translate(CrossJoinOperator* op) {
    Relation* input_rel1 = op->get_relations()[0];
    Relation* input_rel2 = op->get_relations()[1];

    TemplateDictionary dict("join");
    PopulateCommonValues(op, &dict);
    dict.SetValue("FLATTEN", GenCrossJoin(input_rel1, input_rel2));
    dict.SetValue("INPUTREL_NAME1", input_rel1->get_name());
    dict.SetValue("INPUTREL_NAME2", input_rel2->get_name());
    string code;
    ExpandTemplate(FLAGS_spark_templates_dir + "CrossJoinTemplate.scala",
                   ctemplate::DO_NOT_STRIP, &dict, &code);
    SparkJobCode* job_code = new SparkJobCode(op, code);
    job_code->set_rel_out_name(op->get_output_relation()->get_name());
    return job_code;
  }


  SparkJobCode* TranslatorSpark::Translate(JoinOperator* op) {
    Relation* input_rel1 = op->get_relations()[0];
    Relation* input_rel2 = op->get_relations()[1];
    Column* col_left = op->get_col_left();
    Column* col_right = op->get_col_right();
    vector<Column*> key_left;
    vector<Column*> key_right;
    key_left.push_back(col_left);
    key_right.push_back(col_right);
    int32_t col_left_index = col_left->get_index();
    int32_t col_right_index = col_right->get_index();
    string join = GenFlatJoin("input", col_left->get_type(), col_left_index, col_right_index,
                              input_rel1, input_rel2);
    string inputrel_type1 = GenJoinTupleType(input_rel1->get_columns(), key_left);
    string inputrel_type2 = GenJoinTupleType(input_rel2->get_columns(), key_right);
    TemplateDictionary dict("join");
    PopulateCommonValues(op, &dict);
    dict.SetValue("INPUT",
                  "(input:(" + input_rel1->get_columns()[col_left_index]->translateTypeScala() +
                  ", (" + inputrel_type1 + ", " + inputrel_type2 + ")))");
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    dict.SetValue("JOIN_COL_TYPE", col_left->translateTypeScala());
    dict.SetValue("KEYRDD1", GenerateKeyRDD("input", input_rel1, key_left, col_left_index));
    dict.SetValue("KEYRDD2", GenerateKeyRDD("input", input_rel2, key_right, col_right_index));
    dict.SetValue("REL_NAME1", input_rel1->get_name());
    dict.SetValue("REL_NAME2", input_rel2->get_name());
    dict.SetValue("INPUTREL_TYPE1", inputrel_type1);
    dict.SetValue("INPUTREL_TYPE2", inputrel_type2);
    string code;
    ExpandTemplate(FLAGS_spark_templates_dir + "JoinTemplate.scala",
                   ctemplate::DO_NOT_STRIP, &dict, &code);
    SparkJobCode* job_code = new SparkJobCode(op, code);
    string out_rel_name = op->get_output_relation()->get_name();
    job_code->set_rel_out_name(out_rel_name);
    job_code->set_out_fun_code("val " + out_rel_name + "_local = " + join + "; ");
    return job_code;
  }

  SparkJobCode* TranslatorSpark::Translate(MinOperator* op) {
    TemplateDictionary dict("min");
    Relation* input_rel = op->get_relations()[0];
    string input_name_local = input_rel->get_name() + "_local";
    string inputrel_format = GenTupleType(input_rel);
    Column* col = op->get_column();
    int col_index = col->get_index();

    string output_row = "";
    if (op->hasGroupby()) {
      vector<Column*> group_bys = op->get_group_bys();
      inputrel_format = GenMinMaxTupleType(col, op->get_selected_columns());
      output_row = GenerateFlatMapping(input_name_local, col_index, op->get_selected_columns(),
                                       group_bys, input_rel->get_columns());
      dict.SetValue("INPUT",
                    "(" + input_name_local + ":(" + GenTupleType(group_bys) +
                    ", " + inputrel_format + "))");
      dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
      dict.SetValue("GROUP_BY", GenTupleType(group_bys));
      dict.SetValue("GROUP_BY_KEY", GenerateMinMaxGroupBy(input_name_local, col_index, input_rel,
                                                          group_bys, op->get_selected_columns()));
    }

    PopulateCommonValues(op, &dict);
    PopulateCondition(op->get_condition_tree()->toString("spark"), input_rel->get_name(), &dict);
    dict.SetValue("INPUTREL_TYPE", inputrel_format);

    string code;
    if (!(op->hasGroupby())) {
      dict.SetValue("COL_INDEX", boost::lexical_cast<string>(col_index + 1));
      dict.SetValue("OUTPUT_COLS", GenOutputCols("int_" + class_name + "._", col_index + 1,
                                                 op->get_selected_columns()));
      ExpandTemplate(FLAGS_spark_templates_dir + "MinTemplate.scala",
                     ctemplate::DO_NOT_STRIP, &dict, &code);
    } else {
      if (op->get_selected_columns().size() > 0) {
        dict.SetValue("COL_INDEX", "._1");
      }
      ExpandTemplate(FLAGS_spark_templates_dir + "MinTemplateGroupBy.scala",
                     ctemplate::DO_NOT_STRIP, &dict, &code);
    }
    SparkJobCode* job_code = new SparkJobCode(op, code);
    string out_rel_name = op->get_output_relation()->get_name();
    job_code->set_rel_out_name(out_rel_name);
    job_code->set_out_fun_code("val " + out_rel_name + "_local = " + output_row + "; ");
    return job_code;
  }

  SparkJobCode* TranslatorSpark::Translate(MaxOperator* op) {
    TemplateDictionary dict("max");
    Relation* input_rel = op->get_relations()[0];
    string input_name_local = input_rel->get_name() + "_local";
    string inputrel_format = GenTupleType(input_rel);
    Column* col = op->get_column();
    int col_index = col->get_index();

    string output_row = "";
    if (op->hasGroupby()) {
      vector<Column*> group_bys = op->get_group_bys();
      inputrel_format = GenMinMaxTupleType(col, op->get_selected_columns());
      output_row = GenerateFlatMapping(input_name_local, col_index, op->get_selected_columns(),
                                       group_bys, input_rel->get_columns());
      dict.SetValue("INPUT",
                    "(" + input_name_local + ":(" + GenTupleType(group_bys) +
                    ", " + inputrel_format + "))");
      dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
      dict.SetValue("GROUP_BY", GenTupleType(group_bys));
      dict.SetValue("GROUP_BY_KEY", GenerateMinMaxGroupBy(input_name_local, col_index, input_rel,
                                                          group_bys, op->get_selected_columns()));
    }

    PopulateCommonValues(op, &dict);
    PopulateCondition(op->get_condition_tree()->toString("spark"), input_rel->get_name(), &dict);
    dict.SetValue("INPUTREL_TYPE", inputrel_format);

    string code;
    if (!(op->hasGroupby())) {
      dict.SetValue("COL_INDEX", boost::lexical_cast<string>(col_index + 1));
      dict.SetValue("OUTPUT_COLS", GenOutputCols("int_" + class_name + "._", col_index + 1,
                                                 op->get_selected_columns()));
      ExpandTemplate(FLAGS_spark_templates_dir + "MaxTemplate.scala", ctemplate::DO_NOT_STRIP,
                     &dict, &code);
    } else {
      if (op->get_selected_columns().size() > 0) {
        dict.SetValue("COL_INDEX", "._1");
      }
      ExpandTemplate(FLAGS_spark_templates_dir + "MaxTemplateGroupBy.scala",
                     ctemplate::DO_NOT_STRIP, &dict, &code);
    }
    SparkJobCode* job_code = new SparkJobCode(op, code);
    string out_rel_name = op->get_output_relation()->get_name();
    job_code->set_rel_out_name(out_rel_name);
    job_code->set_out_fun_code("val " + out_rel_name + "_local = " + output_row + "; ");
    return job_code;
  }

  string TranslatorSpark::GenerateProject(const string& input_name, vector<Column*> proj_cols) {
    string project = "(";
    uint32_t index = 0;
    for (vector<Column*>::const_iterator it = proj_cols.begin(); it != proj_cols.end(); ++it) {
      project += input_name + "._" + boost::lexical_cast<string>((*it)->get_index() + 1);
      if (++index < proj_cols.size()) {
        project += ", ";
      }
    }
    project += ")";
    return project;
  }

  SparkJobCode* TranslatorSpark::Translate(ProjectOperator* op) {
    // TODO(tach): Handle multiple input paths.
    Relation* input_rel = op->get_relations()[0];
    string input_name_local = input_rel->get_name() + "_local";
    TemplateDictionary dict("project");
    PopulateCommonValues(op, &dict);
    PopulateCondition(op->get_condition_tree()->toString("spark"), input_rel->get_name(), &dict);
    dict.SetValue("INPUT", "(" + input_name_local + ": " + GenTupleType(input_rel) + ")");
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string code;
    ExpandTemplate(FLAGS_spark_templates_dir + "ProjectTemplate.scala",
                   ctemplate::DO_NOT_STRIP, &dict, &code);
    SparkJobCode* job_code = new SparkJobCode(op, code);
    string out_rel_name = op->get_output_relation()->get_name();
    job_code->set_rel_out_name(out_rel_name);
    job_code->set_out_fun_code("val " + out_rel_name + "_local = " +
                               GenerateProject(input_name_local, op->get_columns()) + "; ");
    return job_code;
  }

  SparkJobCode* TranslatorSpark::Translate(SelectOperator* op) {
    // TODO(tach): Handle multiple input paths.
    Relation* input_rel = op->get_relations()[0];
    string input_name_local = input_rel->get_name() + "_local";
    TemplateDictionary dict("project");
    PopulateCommonValues(op, &dict);
    PopulateCondition(op->get_condition_tree()->toString("spark"), input_rel->get_name(), &dict);
    dict.SetValue("INPUT", "(" + input_name_local + ": " + GenTupleType(input_rel) + ")");
    dict.SetValue("NEXT_OPERATOR", "{{NEXT_OPERATOR}}");
    string code;
    ExpandTemplate(FLAGS_spark_templates_dir + "SelectTemplate.scala",
                   ctemplate::DO_NOT_STRIP, &dict, &code);
    SparkJobCode* job_code = new SparkJobCode(op, code);
    string out_rel_name = op->get_output_relation()->get_name();
    job_code->set_rel_out_name(out_rel_name);
    job_code->set_out_fun_code("val " + out_rel_name + "_local = " +
                               GenerateProject(input_name_local, op->get_columns()) + "; ");
    return job_code;
  }

  SparkJobCode* TranslatorSpark::Translate(SortOperator* op) {
    Relation* input_rel = op->get_relations()[0];
    Column* sorting_col = op->get_column();
    string rel_name = input_rel->get_name();
    string input_name_local = rel_name + "_local";
    string type = GenTupleType(input_rel);
    string col_type = sorting_col->translateTypeScala();
    string increasing = op->get_increasing() ? "true" : "false";
    vector<Column*> key_col;
    key_col.push_back(sorting_col);
    string keyRDD =
      GenerateKeyRDD(input_name_local, input_rel, key_col, sorting_col->get_index());
    TemplateDictionary dict("sort");
    PopulateCommonValues(op, &dict);
    dict.SetValue("KEYRDD", keyRDD);
    dict.SetValue("COL_TYPE", col_type);
    dict.SetValue("REL_TYPE", type);
    dict.SetValue("REL_NAME", rel_name);
    dict.SetValue("COL",
                  boost::lexical_cast<string>(sorting_col->get_index()));
    dict.SetValue("ORDER", increasing);
    string code;
    ExpandTemplate(FLAGS_spark_templates_dir + "SortTemplate.scala",
                   ctemplate::DO_NOT_STRIP, &dict, &code);
    SparkJobCode* job_code = new SparkJobCode(op, code);
    job_code->set_rel_out_name(op->get_output_relation()->get_name());
    return job_code;
  }

  SparkJobCode* TranslatorSpark::Translate(UdfOperator* op) {
    // TODO(tach): Implement.
    return new SparkJobCode(op, "");
  }

  SparkJobCode* TranslatorSpark::Translate(UnionOperator* op) {
    TemplateDictionary dict("union");
    PopulateCommonValues(op, &dict);
    dict.SetValue("REL_NAME", op->get_relations()[0]->get_name());
    dict.SetValue("REL_NAME2", op->get_relations()[1]->get_name());
    string code;
    ExpandTemplate(FLAGS_spark_templates_dir + "UnionTemplate.scala", ctemplate::DO_NOT_STRIP,
                   &dict, &code);
    SparkJobCode* job_code = new SparkJobCode(op, code);
    job_code->set_rel_out_name(op->get_output_relation()->get_name());
    return job_code;
  }

  string TranslatorSpark::GenConditionInput(const set<string>& cond_names) {
    string conds_vars = "";
    for (set<string>::const_iterator it = cond_names.begin();
         it != cond_names.end(); ++it) {
      conds_vars += "  var " + *it + "_cond = " + *it +
        ".first();\n";
    }
    return conds_vars;
  }

  string TranslatorSpark::GenUpdateConditionInput(const set<string>& cond_names) {
    string conds = "";
    for (set<string>::const_iterator it = cond_names.begin();
         it != cond_names.end(); ++it) {
      conds += "  " + *it + "_cond = " + *it + ".first();\n";
    }
    return conds;
  }

  SparkJobCode* TranslatorSpark::Translate(WhileOperator* op) {
    // TODO(ionel): Extend condition support.
    TemplateDictionary dict("while");
    PopulateCommonValues(op, &dict);
    string condition = op->get_condition_tree()->toString("spark");
    set<string> rel_names = set<string>();
    op->get_condition_tree()->getRelNames(&rel_names);
    string inputs = GenConditionInput(rel_names);
    dict.SetValue("CONDITION_VAR", inputs);
    dict.SetValue("CONDITION", condition);
    dict.SetValue("NUM_ITER",
                  op->get_condition_tree()->get_right()->get_value()->get_value());
    string code;
    ExpandTemplate(FLAGS_spark_templates_dir + "WhileTemplate.scala", ctemplate::DO_NOT_STRIP,
                   &dict, &code);
    SparkJobCode* job_code = new SparkJobCode(op, code);
    job_code->set_rel_out_name(op->get_output_relation()->get_name());
    return job_code;
  }

  string TranslatorSpark::OutputDefaultForType(uint16_t type) {
    switch (type) {
    case INTEGER_TYPE: {
      return "0";
    }
    case STRING_TYPE: {
      return "\"a\"";
    }
    case DOUBLE_TYPE: {
      return "0.0";
    }
    case BOOLEAN_TYPE: {
      return "false";
    }
    default: {
      LOG(ERROR) << "Column has unexpected type";
      return NULL;
    }
    }
  }

  string TranslatorSpark::GenerateAggGroupBy(const string& input_name, Relation* input_rel,
                                             const vector<Column*>& group_bys,
                                             const vector<Column*>& selected_cols) {
    string result = "map((" + input_name + ":" + GenTupleType(input_rel) + ") => (( ";
    uint32_t index = 0;
    // Generate group by part.
    for (vector<Column*>::const_iterator it = group_bys.begin(); it != group_bys.end(); ++it) {
      result += input_name + "._" + boost::lexical_cast<string>((*it)->get_index() + 1);
      if (++index < group_bys.size()) {
        result += ", ";
      }
    }
    result += "), (";
    for (vector<Column*>::const_iterator it = selected_cols.begin(); it != selected_cols.end();
         ++it) {
      result += input_name + "._" + boost::lexical_cast<string>((*it)->get_index() + 1) + ", ";
    }
    result.erase(result.end() - 2, result.end());
    result += ")))";
    return result;
  }

  string TranslatorSpark::GenerateCountGroupBy(const string& input_name, Relation* input_rel,
                                               const vector<Column*>& group_bys) {
    string result = "map((" + input_name + ":" + GenTupleType(input_rel) + ") => (( ";
    uint32_t index = 0;
    // Generate group by part.
    for (vector<Column*>::const_iterator it = group_bys.begin(); it != group_bys.end(); ++it) {
      result += input_name + "._" + boost::lexical_cast<string>((*it)->get_index() + 1);
      if (++index < group_bys.size()) {
        result += ", ";
      }
    }
    result += "), 1))";
    return result;
  }

  string TranslatorSpark::GenerateMinMaxGroupBy(const string& input_name, int32_t col_index,
                                                Relation* input_rel,
                                                const vector<Column*>& group_bys,
                                                const vector<Column*>& selected_cols) {
    string result = "map((" + input_name + ":" + GenTupleType(input_rel) + ") => (( ";
    uint32_t index = 0;
    // Generate group by part.
    for (vector<Column*>::const_iterator it = group_bys.begin(); it != group_bys.end(); ++it) {
      result += input_name + "._" + boost::lexical_cast<string>((*it)->get_index() + 1);
      if (++index < group_bys.size()) {
        result += ", ";
      }
    }
    result += "), ";
    // Add the columns on which we compute max or min
    result += "(" + input_name + "._" + boost::lexical_cast<string>(col_index + 1);
    for (vector<Column*>::const_iterator it = selected_cols.begin(); it != selected_cols.end();
         ++it) {
      result += ", " + input_name + "._" + boost::lexical_cast<string>((*it)->get_index() + 1);
    }
    result += ")))";
    return result;
  }

  string TranslatorSpark::GenerateKeyRDD(const string& input_name,
                                         Relation* input_rel,
                                         const vector<Column*>& key_cols,
                                         int32_t col_index) {
    return "map((" + input_name + ":" + GenTupleType(input_rel->get_columns()) +
      ") => (" + input_name + "._" + boost::lexical_cast<string>(col_index + 1) + "," +
      GenTupleVar(input_name, input_rel, key_cols) + "))";
  }

  void TranslatorSpark::PopulateCondition(string condition,
                                          string input_rel_name,
                                          TemplateDictionary* dict) {
    if (!condition.compare("true")) {
      // The operator doesn't have a where.
      dict->SetValue("CONDITION", input_rel_name);
    } else {
      dict->SetValue("CONDITION", input_rel_name + ".filter(" +
                     input_rel_name + "=>" + condition + ")");
    }
  }

  void TranslatorSpark::PopulateCommonValues(OperatorInterface* op,
                                             TemplateDictionary* dict) {
    string binary_file = GetBinaryPath(op);
    string output_path = op->get_output_path();
    string class_name =  op->get_output_relation()->get_name();
    //      dict->SetValue("OUTPUT", class_name);
    dict->SetValue("OUTPUT", "{{OUTPUT}}");
    dict->SetValue("CLASS_NAME", class_name);
    dict->SetValue("OUTPUT_PATH", output_path);
    dict->SetValue("HDFS_MASTER", "hdfs://" + FLAGS_hdfs_master + ":" + FLAGS_hdfs_port);
    dict->SetValue("SPARK_MASTER", FLAGS_spark_master);
    dict->SetValue("SPARK_DIR", FLAGS_spark_dir);
    dict->SetValue("BIN_NAME", binary_file);
    if (to_cache[op->get_output_relation()->get_name()]) {
      //        dict->SetValue("TO_CACHE", ".cache()");
    }
  }

  string TranslatorSpark::Compile(const string& code, const string& code_dir) {
    ofstream job_file;
    LOG(INFO) << "Class name: " << class_name;
    string source_file = GetSourcePath(class_name, code_dir);
    string binary_file = GetBinaryPath(class_name, code_dir);
    string path = code_dir + class_name + "_code/";
    LOG(INFO) << "Source: " << source_file;
    LOG(INFO) << "Binary: " << binary_file;
    LOG(INFO) << "Path: " << path;
    string create_dir = "mkdir -p " + path;
    std::system(create_dir.c_str());
    job_file.open(source_file.c_str());
    job_file << code;
    job_file.close();
    LOG(INFO) << "spark build started for: " << class_name;
    timeval start_compile;
    gettimeofday(&start_compile, NULL);
    // Start the compilation process.
    string source_folder = path + "src/main/scala/";
    string cmd = "mkdir -p " + source_folder;
    std::system(cmd.c_str());
    // Create the name.sbt file.
    ofstream sbt_file;
    string file_name = path +  class_name + ".sbt";
    sbt_file.open(file_name.c_str());
    sbt_file << "name := \"" + class_name + "\"\n\n";
    sbt_file << "version := \"1.0\"\n\n";
    sbt_file << "scalaVersion := \"" + FLAGS_scala_version + "\"\n\n";
    sbt_file << "libraryDependencies += \"org.apache.spark\" %% \"" <<
      "spark-core\" % \"" + FLAGS_spark_version + "\"\n\n";
    sbt_file << "libraryDependencies += \"org.apache.hadoop\" % " <<
      "\"hadoop-client\" % \"2.0.0-mr1-cdh4.5.0\"\n\n";
    sbt_file << "resolvers ++= Seq(\"Akka Repository\" at " <<
      "\"http://repo.akka.io/releases/\",\"Spray Repository\"" <<
      " at \"http://repo.spray.cc/\", \"Cloudera Repository\"" <<
      " at \"https://repository.cloudera.com/artifactory/cloudera-repos/\")\n\n";
    sbt_file.close();
    cmd = "mv " + source_file + " " + source_folder;
    std::system(cmd.c_str());
    // Run sbt/sbt package in the newly created folder.
    cmd = "cd " + path + ";" + FLAGS_spark_dir + "sbt/sbt package";
    std::system(cmd.c_str());
    //Rename and move compiled jar to main folder
    cmd = "mv " + path + "target/scala-" + FLAGS_scala_major_version +
      "/" + class_name  + "_" + FLAGS_scala_major_version + "-1.0.jar " +
      binary_file;
    std::system(cmd.c_str());
    LOG(INFO) << "spark build ended for: " << class_name;
    timeval end_compile;
    gettimeofday(&end_compile, NULL);
    uint32_t compile_time = end_compile.tv_sec - start_compile.tv_sec;
    cout << "COMPILE TIME: " << compile_time << endl;
    return binary_file;
  }

  bool TranslatorSpark::MustGenerateCode(shared_ptr<OperatorNode> node) {
    OperatorInterface* op = node->get_operator();
    if (node->get_children().size() != 1 || node->get_loop_children().size() > 0 ||
        op->get_type() == BLACK_BOX_OP || op->get_type() == DIFFERENCE_OP ||
        op->get_type() == DISTINCT_OP || op->get_type() == INTERSECTION_OP ||
        op->get_type() == SORT_OP || op->get_type() == UDF_OP ||
        op->get_type() == UNION_OP || op->get_type() == WHILE_OP) {
      return true;
    }
    if ((op->get_type() == AGG_OP || op->get_type() == COUNT_OP || op->get_type() == MIN_OP ||
         op->get_type() == MAX_OP) && !op->hasGroupby()) {
      return true;
    }
    // TODO(ionel): Remove CROSS_JOIN rule once the operator is updated.
    if (op->get_type() == CROSS_JOIN_OP) {
      return true;
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
