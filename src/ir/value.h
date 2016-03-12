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

#ifndef MUSKETEER_VALUE_H
#define MUSKETEER_VALUE_H

#include <string>

namespace musketeer {

class Value {
 public:
  Value(string value_, uint16_t type_): value(value_), type(type_) {
  }

  virtual ~Value() {
  }

  virtual string get_value() {
    return value;
  }

  virtual uint16_t get_type() {
    return type;
  }

  virtual void set_type(uint16_t new_type) {
    type = new_type;
  }

 private:
  string value;
  uint16_t type;
};

} // namespace musketeer
#endif
