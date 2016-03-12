/*
 * wildcherry_types.c
 *
 *  Created on: Sep 30, 2013
 *      Author: mgrosvenor
 */

#include <libchaste/include/chaste.h>
#include "wildcherry_types.h"
#include <libchaste/include/data_structs/vector/vector_typed_define_template.h>

define_ch_vector(row,column)

ch_word cmp_column(void* lhs, void* rhs)
{
    column* lhs_c = lhs;
    column* rhs_c = rhs;

    ch_word lhs_len = lhs_c->end_mark - lhs_c->start_mark;
    ch_word rhs_len = rhs_c->end_mark - rhs_c->start_mark;

    ch_word result = lhs_len == rhs_len ? 0 : lhs_len < rhs_len ? -1 : 1;
    if(!result) return result;

    return strncmp((char*)lhs_c->start_mark, (char*)rhs_c->start_mark, lhs_len);

}
