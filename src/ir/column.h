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

#ifndef MUSKETEER_COLUMN_H
#define MUSKETEER_COLUMN_H

#include <string>
#include <stdint.h>
#include <boost/lexical_cast.hpp>

#include "base/common.h"
#include "ir/value.h"

#define INTEGER_TYPE 0
#define DOUBLE_TYPE 1
#define STRING_TYPE 2
#define BOOLEAN_TYPE 3

namespace musketeer {

class Column : public Value {
 public:
  Column(const string& relation_, uint32_t index_, uint16_t type):
    Value(relation_ + boost::lexical_cast<string>(index_), type),
      relation(relation_), index(index_) {
  }

  Column* clone();
  string get_relation();
  int32_t get_index();
  string toString(const string& fmw);
  string translateTypeJava();
  string translateTypeC();
  string translateTypeCSharp();
  string translateTypeScala();
  static string stringCompareToCSharp(uint16_t type, int32_t index);
  static string stringEqualsCSharp(uint16_t type, int32_t index);
  static string stringHashCSharp(uint16_t type);
  static string stringPrimitiveTypeCSharp(uint16_t type);
  static string stringTypeJava(uint16_t type);
  static string stringTypeC(uint16_t type);
  static string stringTypeCSharp(uint16_t type);
  static string stringTypeScala(uint16_t type);
  static string indexString(int32_t index);

 private:
  string relation;
  int32_t index;
};

} // namespace musketeer
#endif
