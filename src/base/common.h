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

#ifndef MUSKETEER_COMMON_H
#define MUSKETEER_COMMON_H

using namespace std;  // NOLINT

#include <glog/logging.h>
#include <gflags/gflags.h>

typedef enum {
  AGG_OP,
  BLACK_BOX_OP,
  COUNT_OP,
  CROSS_JOIN_OP,
  DIFFERENCE_OP,
  DISTINCT_OP,
  DIV_OP,
  INPUT_OP,
  INTERSECTION_OP,
  JOIN_OP,
  MAX_OP,
  MIN_OP,
  MUL_OP,
  PROJECT_OP,
  SELECT_OP,
  SORT_OP,
  SUB_OP,
  SUM_OP,
  UDF_OP,
  UNION_OP,
  WHILE_OP
} OperatorType;

#endif  // MUSKETEER_COMMON_H
