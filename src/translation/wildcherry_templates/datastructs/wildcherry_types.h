/*
 * wildcherry_types.h
 *
 *  Created on: Sep 30, 2013
 *      Author: mgrosvenor
 */

#ifndef WILDCHERRY_TYPES_H_
#define WILDCHERRY_TYPES_H_

#include <libchaste/include/data_structs/vector/vector_typed_declare_template.h>

typedef enum { COL_TYPE_STR, COL_TYPE_FLOAT, COL_TYPE_INT } col_type;

typedef struct {
    col_type type;
    union {
        ch_float floating;
        ch_word integer;
    };
    ch_byte* start_mark;
    ch_byte* end_mark;
    ch_bool updated;

} column;

declare_ch_vector(row,column)

ch_word cmp_column(void* lhs, void* rhs);

#endif /* WILDCHERRY_TYPES_H_ */
