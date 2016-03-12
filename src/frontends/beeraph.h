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

#ifndef MUSKETEER_BEERAPH_H
#define MUSKETEER_BEERAPH_H

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>
#include <ctemplate/template.h>
#include <stdio.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

#include "base/common.h"
#include "base/utils.h"

#define NODEVAL "NODEVAL"
#define OUTNUM "OUTNUM"
#define EDGEVAL "EDGEVAL"

namespace musketeer {
namespace beeraph {
    /*
     * Format of a beer style computation is:
     * define Edges
     * define Values
     * define stopping condition
     * if (outnumber is used) {
     * compute outnumber (count edges group by node)
     * create table (edges join outnum)
     * }
     * while(stopping)
     *   scatter
     *   => apply scatter fn on (outnumber x values) or (edges x values)
     *  gather
     *   => apply gather fn on (node x value)
     *  apply
     *   => apply apply fn on (node x value)
     *   => update stopping cond
     *
     */

class BeeraphTranslator {
 public:
  BeeraphTranslator();
  ~BeeraphTranslator();

  void translateToBeer(string code);

 private:
  void generate_gather(string gather, string node_name, string* gather_phase);
  void generate_scatter(vector<string>::const_iterator lines,
                        string nodes, string edges, string* res);
  void generate_apply(vector<string>::const_iterator lines,
                      string input, string stop_cond, string* res);
  void generate_outnum(string edges, string* res);
  bool contains_string(string token, string st);
  vector<string> tokenify(string st, char token);

  string node_val;
  string edge_val_nocount;
  string edge_val_count;
  string count_val;
};

} // namespace beeraph
} // namespace musketeer
#endif
