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

#include "frontends/beeraph.h"

#include <string>

namespace musketeer {
namespace beeraph {

  using ctemplate::TemplateDictionary;

  BeeraphTranslator::BeeraphTranslator() {
    node_val = "_1";
    edge_val_nocount = "_3";
    edge_val_count = "_4";
    count_val = "_2";
  }

  BeeraphTranslator::~BeeraphTranslator() {
  }

  void BeeraphTranslator::translateToBeer(string file_name) {
    string output_code;
    TemplateDictionary dict("beer");

    string node_name;
    string edges_name;
    string stop_cond_name;

    string gather_op;

    bool uses_outnum;
    string outnum;

    string edges_input;
    string nodes_input;
    string stopping_input;

    string stop_cond;

    string gather_phase;
    string scatter_phase;
    string apply_phase;

    // parsing tools

    string current_line;
    int line_index = 0;

    // load file
    std::ifstream t(file_name.c_str());
    string code((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());

    vector<string> lines = tokenify(code, '\n');

    // determine whether we ever use OUTNUM
    uses_outnum = contains_string("OUTNUM", code);

    // parse line by line
    for (vector<string>::const_iterator it = lines.begin(); it != lines.end(); ++it) {
      string line = (*it);
      if (line_index == 0) {
        vector<string> gat = tokenify(line, ' ');
        edges_input = line;
        edges_name = gat.at(2);
      } else if (line_index == 1) {
        vector<string> gat = tokenify(line, ' ');
        nodes_input = line;
        node_name = gat.at(2);
      } else if (line_index == 2) {
        vector<string> gat = tokenify(line, ' ');
        stopping_input = line;
        stop_cond_name = gat.at(2);
      } else if (contains_string("GATHER", line)) {
        vector<string> gat = tokenify(line, '=');
        gather_op = (gat.at(1));
        boost::algorithm::trim(gather_op);
        generate_gather(gather_op, node_name, &gather_phase);
      } else if (contains_string("APPLY", line)) {
        generate_apply(it, node_name, stop_cond_name, &apply_phase);
      } else if (contains_string("SCATTER", line)) {
        generate_scatter(it, node_name, edges_name, &scatter_phase);
      } else if (contains_string("STOP", line)) {
        vector<string> gat = tokenify(line, '=');
        stop_cond = gat.at(1);
        boost::algorithm::trim(stop_cond);
      }
      line_index++;
    }

    if (uses_outnum) {
      generate_outnum(edges_name, &outnum);
      dict.SetValue("OUT_NAME", "cnt");
    } else {
      dict.SetValue("OUT_NAME", "");
    }

    dict.SetValue("OUTNUM", outnum);
    dict.SetValue("EDGES", edges_input);
    dict.SetValue("NODES", nodes_input);
    dict.SetValue("ITER", stopping_input);
    dict.SetValue("NODE_NAME", node_name);
    dict.SetValue("EDGE_NAME", edges_name);
    dict.SetValue("STOP_COND", stop_cond);
    dict.SetValue("GATHER", gather_phase);
    dict.SetValue("SCATTER", scatter_phase);
    dict.SetValue("APPLY", apply_phase);

    ExpandTemplate("src/beeraph", ctemplate::DO_NOT_STRIP, &dict, &output_code);

    std::ofstream out("beeraph.rap");
    out << output_code;
    out.close();
  }

  bool BeeraphTranslator::contains_string(string token, string st) {
    bool found = false;
    if (st.find(token) != std::string::npos) {
      found = true;
    } else {
      found = false;
    }
    return found;
  }

  vector<string> BeeraphTranslator::tokenify(string st, char delim) {
    vector<string> lines;
    stringstream ss(st);
    string line;
    while (getline(ss, line, delim)) {
      if (!line.empty()) {
        lines.push_back(line);
      }
    }
    return lines;
  }

  void BeeraphTranslator::generate_outnum(string edges, string* res) {
    *res = "COUNT [";
    *res += edges;
    *res += "_1] FROM (";
    *res += edges;
    *res += ") GROUP BY [";
    *res +=  edges;
    *res += "_0] AS node_cnt, \n";
    *res +="(node_cnt) JOIN (";
    *res +=edges;
    *res += ") ON ";
    *res +=edges;
    *res += "_0 AND node_cnt_0 AS ";
    *res +=edges;
    *res += "cnt, \n";

    /** Final format is node node_val outnum dest_node edge_val **/
  }

  void BeeraphTranslator::generate_apply(vector<string>::const_iterator lines,
                                         string nodes, string stop_cond, string* res) {
    string stat;
    int i = 0;

    string nodeval = nodes + node_val;
    while (!contains_string("}", *(++lines))) {
      stat = *lines;
      boost::replace_all(stat, NODEVAL, nodeval);
      if (i == 0) {
        *res += stat + " FROM (gather) GROUP BY [gather_0] AS ";
      } else {
        *res += nodes + boost::lexical_cast<string>(i) + " , \n";
        *res += stat + " FROM (" + nodes + boost::lexical_cast<string>(i) + ") AS ";
      }
      i++;
    }
    *res += nodes + " , \n";
    i = 0;
    ++lines;
    while (!contains_string("}", *(++lines))) {
      stat = *lines;
      if (i++ != 0) {
        *res += "\n";
      }
      *res += stat + " FROM (" + stop_cond + ") AS  " + stop_cond + ",";
    }
  }

  void BeeraphTranslator::generate_scatter(vector<string>::const_iterator lines,
                                           string nodes, string edges, string* res) {
    string stat;
    int i = 0;

    string nodeval = edges + nodes + node_val;
    string edgeval = edges + nodes + node_val;
    string outputval = edges + nodes + count_val;

    while (!contains_string("}", *(++lines))) {
      stat = *lines;
      boost::replace_all(stat, NODEVAL, nodeval);
      boost::replace_all(stat, EDGEVAL, edgeval);
      boost::replace_all(stat, OUTNUM, outputval);
      if (i == 0) {
        *res += stat + " FROM (" + nodes + ") GROUP BY [gather_0] AS ";
      } else {
        *res += nodes + boost::lexical_cast<string>(i) + " , \n";
        *res += stat + " FROM (" + nodes + boost::lexical_cast<string>(i) + ") AS  ";
      }
      i++;
    }
    *res += "scatter";
    *res += " , \n";
  }

  void BeeraphTranslator::generate_gather(string gather, string node_name,
                                          string* gather_phase) {
    vector<string> gather_ops = tokenify(gather, ',');
    int nb_ops = gather_ops.size();
    int index = 0;
    string gather_op;
    // Supported ops: +, -, union, intersection, min, max
    for (vector<string>::const_iterator it = gather_ops.begin();
         it != gather_ops.end(); ++it) {
      gather_op = *it;
      if (gather_op.compare("+") || gather_op.compare("*")) {
        if (index !=nb_ops -1) {
          *gather_phase = "AGG[gather" + boost::lexical_cast<string>(index) + "_1,"
            + gather_op +
            + "] FROM (gather" + boost::lexical_cast<string>(index) +
            + ") GROUP BY [gather" + boost::lexical_cast<string>(index) + "_0] as gather"
            + boost::lexical_cast<string>(++index) + ", \n";
        } else {
          LOG(INFO) << boost::lexical_cast<string>(index);
          *gather_phase = "AGG[gather" + boost::lexical_cast<string>(index) + "_1," + gather_op +
            + "] FROM (gather" + boost::lexical_cast<string>(index) +
            + ") GROUP BY [gather" + boost::lexical_cast<string>(index) + "_0] as gather, \n";
        }
      } else if (gather_op.compare("UNION") || gather_op.compare("INTERSECTION")) {
        if (index == nb_ops - 1) {
          *gather_phase = "(gather";
          *gather_phase += boost::lexical_cast<string>(index);
          *gather_phase +=  ")";
          *gather_phase += gather_op;
          *gather_phase += " ( ";
          *gather_phase += node_name;
          *gather_phase += ") AS gather, \n";
        } else {
          *gather_phase = "(gather" + boost::lexical_cast<string>(index) +  ")" + gather_op +
            " ( " + node_name + ") AS gather" + boost::lexical_cast<string>(++index) + ", \n";
        }
      } else if (gather_op.compare("MINIMUM") || gather_op.compare("MAXIMUM")) {
        if (index == nb_ops - 1) {
          *gather_phase = "MINIMUM [gather " + boost::lexical_cast<string>(index) +
            "_1] from (gather" + boost::lexical_cast<string>(index) + ") AS gather, \n";
        } else {
          *gather_phase = "MINIMUM [gather " + boost::lexical_cast<string>(index) + "_1] " +
            + " from (gather" + boost::lexical_cast<string>(index) + ") AS gather" +
            boost::lexical_cast<string>(++index) +", \n";
        }
      }
    }
  }

} // namespace beeraph
} // namespace musketeer
