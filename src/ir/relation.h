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

#ifndef MUSKETEER_RELATION_H
#define MUSKETEER_RELATION_H

#include <algorithm>
#include <string>
#include <vector>

#include "base/common.h"
#include "ir/column.h"

namespace musketeer {

class Relation {
 public:
  Relation(const string& name_, const vector<Column*>& columns_):
    name(name_), columns(columns_), immutable(false) {
  }

  ~Relation() {
    for (vector<Column*>::iterator it = columns.begin();
         it != columns.end(); ++it) {
      //TODO(hack)
      //        delete *it;
    }
    //      columns.clear();
  }

  string get_name();
  void set_name(string name_);
  vector<Column*> get_columns();
  void set_immutable(bool immutable);
  bool isImmutable();
  Relation* copy(string name);

 private:
  string name;
  vector<Column*> columns;
  // Set to true if a relation is not updated in a while loop.
  // False otherwise, including the case when the relation is not part
  // of a while loop.
  bool immutable;
};

} // namespace musketeer
#endif
