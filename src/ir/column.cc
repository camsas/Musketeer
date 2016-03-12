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

#include "ir/column.h"

#include "ir/relation.h"
#include "translation/translator_spark.h"

namespace musketeer {

  Column* Column::clone() {
    return new Column(relation, index, get_type());
  }

  string Column::get_relation() {
    return relation;
  }

  int32_t Column::get_index() {
    return index;
  }

  string Column::toString(const string& fmw) {
    if (!fmw.compare("hadoop")) {
      return translateTypeJava() + ".valueOf(" + relation + "[" +
        boost::lexical_cast<string>(index) + "])";
    }
    if (!fmw.compare("metis")) {
      return "atoi(row_it->at(" + boost::lexical_cast<string>(index) + "))";
    }
    if (!fmw.compare("naiad")) {
      return "row." + indexString(index);
    }
    if (!fmw.compare("spark")) {
      Relation* rel = translator::TranslatorSpark::relations[relation];
      string res = "";
      if (rel->get_columns().size() > 1) {
        res = relation + "._" + boost::lexical_cast<string>(index + 1);
      } else {
        res = relation;
      }
      return res;
    }
    if (!fmw.compare("wildcherry")) {
      return "COLI(" + boost::lexical_cast<string>(index) + ")";
    }
    LOG(ERROR) << "Unexpected framework: " << fmw;
    return NULL;
  }

  string Column::indexString(int32_t index) {
    string numbers[] = {"first", "second", "third", "fourth", "fifth", "sixth",
                        "seventh", "eigth", "ninth", "tenth", "eleventh", "twelveth",
                        "thirteenth", "fourteenth", "fifteenth", "sixteenth"};
    if (index > 15) {
      LOG(FATAL) << "Index too large";
      return NULL;
    }
    return numbers[index];
  }

  string Column::translateTypeJava() {
    return stringTypeJava(get_type());
  }

  string Column::translateTypeC() {
    return stringTypeC(get_type());
  }

  string Column::translateTypeCSharp() {
    return stringTypeCSharp(get_type());
  }

  string Column::translateTypeScala() {
    return stringTypeScala(get_type());
  }

  string Column::stringTypeJava(uint16_t type) {
    switch (type) {
    case INTEGER_TYPE: {
      return "Integer";
    }
    case STRING_TYPE: {
      return "String";
    }
    case DOUBLE_TYPE: {
      return "Double";
    }
    case BOOLEAN_TYPE: {
      return "Boolean";
    }
    default: {
      LOG(ERROR) << "Column has unexpected type: " << type;
      return "";
    }
    }
  }

  string Column::stringTypeScala(uint16_t type) {
    switch (type) {
    case INTEGER_TYPE:
      return "Int";
    case STRING_TYPE:
      return "String";
    case DOUBLE_TYPE:
      return "Double";
    case BOOLEAN_TYPE:
      return "Boolean";
    default: {
      LOG(ERROR) << "Column has unexpected type: " << type;
      return "";
    }
    }
  }

  string Column::stringTypeC(uint16_t type) {
    switch (type) {
    case INTEGER_TYPE: {
      return "int";
    }
    case STRING_TYPE: {
      return "string";
    }
    case DOUBLE_TYPE: {
      return "double";
    }
    case BOOLEAN_TYPE: {
      return "bool";
    }
    default: {
      LOG(ERROR) << "Column has unexpected type";
      return NULL;
    }
    }
  }

  string Column::stringTypeCSharp(uint16_t type) {
    switch (type) {
    case INTEGER_TYPE: {
      return "Int";
    }
    case STRING_TYPE: {
      return "String";
    }
    case DOUBLE_TYPE: {
      return "Double";
    }
    case BOOLEAN_TYPE: {
      return "Boolean";
    }
    default: {
      LOG(ERROR) << "Column has unexpected type";
      return NULL;
    }
    }
  }

  string Column::stringHashCSharp(uint16_t type) {
    switch (type) {
    case INTEGER_TYPE: {
      return "";
    }
    case STRING_TYPE: {
      return ".GetHashCode()";
    }
    case DOUBLE_TYPE: {
      return ".GetHashCode()";
    }
    case BOOLEAN_TYPE: {
      return "";
    }
    default: {
      LOG(ERROR) << "Column has unexpected type";
      return NULL;
    }
    }
  }

  string Column::stringCompareToCSharp(uint16_t type, int32_t index) {
    string index_string = indexString(index);
    switch (type) {
    case INTEGER_TYPE: {
      return "if (this." + index_string + " != that." + index_string + ")\n        " +
        "return this." + index_string + " - that." + index_string + ";";
    }
    case STRING_TYPE: {
      return "if (!this." + index_string + ".Equals(that." + index_string + "))\n       " +
        "return this." + index_string + ".CompareTo(that." + index_string + ");";
    }
    case DOUBLE_TYPE: {
      return "if (Math.Abs(this." + index_string + " - that." + index_string +
        ") > 0.0000001)\n        return this." + index_string + " - that." + index_string +
        " < 0 ? -1 : 1;";
    }
    case BOOLEAN_TYPE: {
      return "if (this." + index_string + " != that." + index_string + ")\n        " +
        "return this." + index_string + " - that." + index_string + ";";
    }
    default: {
      LOG(ERROR) << "Column has unexpected type";
      return NULL;
    }
    }
  }

  string Column::stringEqualsCSharp(uint16_t type, int32_t index) {
    string index_string = indexString(index);
    switch (type) {
    case INTEGER_TYPE: {
      return "this." + index_string + " == that." + index_string;
    }
    case STRING_TYPE: {
      return "this." + index_string + ".Equals(that." + index_string + ")";
    }
    case DOUBLE_TYPE: {
      return "Math.Abs(this." + index_string + " - that." + index_string + ") < 0.0000001";
    }
    case BOOLEAN_TYPE: {
      return "this." + index_string + " == that." + index_string;
    }
    default: {
      LOG(ERROR) << "Column has unexpected type";
      return NULL;
    }
    }
  }

  string Column::stringPrimitiveTypeCSharp(uint16_t type) {
    switch (type) {
    case INTEGER_TYPE: {
      return "int";
    }
    case STRING_TYPE: {
      return "string";
    }
    case DOUBLE_TYPE: {
      return "double";
    }
    case BOOLEAN_TYPE: {
      return "boolean";
    }
    default: {
      LOG(ERROR) << "Column has unexpected type";
      return NULL;
    }
    }
  }

} // namespace musketeer
