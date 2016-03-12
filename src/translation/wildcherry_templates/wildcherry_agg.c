
#ifdef IS_TEMPLATE
/*************** TEMPLATE FOO GOES HERE *********************/
/************************************************************/

	#define ALL_KEY "All "

	#define AGG_COLS_IN_COUNT {{AGG_COLS_IN_COUNT}}
    #define AGG_COLS_IN { {{AGG_COLS_IN}} }

    static char* in_file = "{{LOCAL_INPUT_PATH}}";
    static char* out_file = "{{LOCAL_OUTPUT_PATH}}";

    #define IS_COUNT {{IS_COUNT}}
    #define AGG_OP {{AGG_OP}}
    #define AGG_COL ( {{AGG_COL}} )

/************************************************************/
/************************************************************/
#else

    #define ALL_KEY "All "
    #define AGG_COLS_IN_COUNT (2)
    #define AGG_COLS_IN { {0, 0, NULL, NULL } }
	#define AGG_COL (1)
	static char* in_file  = "tests/test_1.in";
    static char* out_file = "test.out";
    #define AGG_OP +
    #define IS_COUNT 1

#endif


#include "libchaste/include/chaste.h"
#include <stdio.h>
#include "fileio/fileio_mmap.h"
#include <stdlib.h>


typedef struct {
    ch_word idx;
    ch_word col_id;
    ch_byte* key_start_mark;
    ch_byte* key_end_mark;
} col_match;


#if AGG_COLS_IN_COUNT > 0
	col_match cols_sorted[AGG_COLS_IN_COUNT] = AGG_COLS_IN;
#else
	col_match cols_sorted[0];
#endif

static file_state_t* in;
static file_state_t* out;

USE_CH_LOGGER_DEFAULT;
USE_CH_OPTIONS;

static struct {
	ch_cstr in;
} options;


static int cmp_by_id(const void* lhs, const void* rhs){
    const col_match* lhs_t = (col_match*)lhs;
    const col_match* rhs_t = (col_match*)rhs;

    return lhs_t->col_id == rhs_t->col_id ? 0 : lhs_t->col_id < rhs_t->col_id ? -1 : 1;
}

static int cmp_by_idx(const void* lhs, const void* rhs){
    const col_match* lhs_t = (col_match*)lhs;
    const col_match* rhs_t = (col_match*)rhs;

    return lhs_t->idx == rhs_t->idx ? 0 : lhs_t->idx < rhs_t->idx ? -1 : 1;

}

static CH_VECTOR(u8)* key_buff = NULL;

static void do_build_key(){

    if(AGG_COLS_IN_COUNT == 0){
        return;
    }

    //Sort by index to write out
    qsort(cols_sorted,AGG_COLS_IN_COUNT,sizeof(col_match),cmp_by_idx);

    for(ch_word i = 0; i < AGG_COLS_IN_COUNT; i++){
        key_buff->push_back_carray(key_buff,  cols_sorted[i].key_start_mark, cols_sorted[i].key_end_mark - cols_sorted[i].key_start_mark);
        key_buff->push_back(key_buff, ' ');
    }

    //Sort back by col_id for the next round of
    qsort(cols_sorted,AGG_COLS_IN_COUNT,sizeof(col_match),cmp_by_id);

}


ch_word agg_fn(ch_word value, void* key, ch_word key_size, void* data, ch_word index)
{

    if(IS_COUNT){
        return index + 1;
    }

    if(index == 0){
        return *(ch_word*)data;
    }

    return value = value AGG_OP *(ch_word*)data;

}



int main(int argc, char** argv)
{

    ch_log_info("Running wildcherry aggregate\n");
    ch_opt_addsi(CH_OPTION_OPTIONAL, 'i', "input", "File to run agg on", &options.in, in_file);
    ch_opt_parse(argc, argv);

    in  = fileio_new("mmap", options.in );
    out = fileio_new("cwrite", out_file);

    ch_word len = 0;
    ch_byte* data = NULL;

    ch_byte* key_start_mark = NULL;
    ch_byte* key_end_mark = NULL;

    ch_byte* data_start_mark = NULL;
    ch_byte* data_end_mark = NULL;
    ch_word data_accum;

    ch_function_hash_map* fn_map = ch_function_hash_map_new(1024 * 1024,agg_fn);

    key_buff = CH_VECTOR_NEW(u8,1024,CH_VECTOR_CMP(u8));
    if(!AGG_COLS_IN_COUNT){ //Only clear out if we are using col keys
        key_buff->push_back_carray(key_buff, (u8*)ALL_KEY , strlen(ALL_KEY) );
    }

    ch_log_info("Starting main loop...\n");
    data = in->read(in, &len);
    if(len <= 0){
        ch_log_fatal("No data suplied! Can't continue\n");
    }

    if(data[len -1] != '\n'){
        ch_log_fatal("No newline at end of file. Cannot continue!\n");
    }

    ch_log_debug1("Got %li bytes\n", len);

    //Sort by ID to do the match
    qsort(cols_sorted,AGG_COLS_IN_COUNT,sizeof(col_match),cmp_by_id);
    ch_word col_id_idx 	= 0;
    ch_word col_id 		= cols_sorted[col_id_idx].col_id;

    //iterate over the data that we have
    ch_word col_idx = 0;
    ch_word row_idx = 0;

    for(ch_word i = 0; i < len; i++){
        if(data[i] == '\n'){
            row_idx++;
            col_idx = 0;
            if(key_start_mark){
                key_end_mark = &data[i];
                cols_sorted[col_id_idx].key_start_mark = key_start_mark;
                cols_sorted[col_id_idx].key_end_mark = key_end_mark;

                key_start_mark = NULL;
                key_end_mark = NULL;
            }

            if(data_start_mark && !data_end_mark){
                data_end_mark = &data[i];
    //            data_accum *= 10;
    //            data_accum += (u8)data[i] - '0';
            }


            //Put together the key in the right order
            do_build_key();
            function_hash_map_push(fn_map,key_buff->first,key_buff->count,&data_accum);

            data_accum      = 0;
            col_id_idx      = 0;
            col_id          = cols_sorted[col_id_idx].col_id;
            data_start_mark = NULL;
            data_end_mark   = NULL;

            if(AGG_COLS_IN_COUNT){ //Only clear out if we are using col keys
                key_buff->clear(key_buff);
            }

            continue;
        }

        if(data[i] == ' '){
            col_idx++;
            continue;
        }


        if(AGG_COLS_IN_COUNT && key_start_mark && !key_end_mark && col_idx == col_id + 1 ){
            key_end_mark = &data[i - 1];
            cols_sorted[col_id_idx].key_start_mark = key_start_mark;
            cols_sorted[col_id_idx].key_end_mark = key_end_mark;

            //Move on to the next col
            col_id_idx++;
            if(col_id_idx < AGG_COLS_IN_COUNT){
            	col_id = cols_sorted[col_id_idx].col_id;
            }

            key_start_mark = NULL;
            key_end_mark = NULL;
            //ch_log_debug1("Key end at %p [idx=%li]\n", key_end_mark, col_idx);
        }

        if(AGG_COLS_IN_COUNT && key_start_mark == NULL && col_id_idx < AGG_COLS_IN_COUNT && col_idx == col_id){
            key_start_mark = &data[i];
            //ch_log_debug1("Key start at %p [idx=%li]\n", key_start_mark, col_idx);
        }


        if(data_start_mark && !data_end_mark && col_idx == AGG_COL + 1 ){
            data_end_mark = &data[i - 1];
            //ch_log_debug1("Key end at %p [idx=%li]\n", data_end_mark, col_idx);
        }

        if(data_start_mark && !data_end_mark){
            data_accum *= 10;
            data_accum += (u8)data[i] - '0';
        }

        if(data_start_mark == NULL && col_idx == AGG_COL){
            data_start_mark = &data[i];
            data_accum = (u8)data[i] - '0';
            //ch_log_debug1("Key start at %p [idx=%li]\n", data_start_mark, col_idx);
        }

    }


    //Dump the output
    char nl = '\n';
    char result[32] = { 0 } ;
    for( ch_function_hash_map_it it = function_hash_map_first(fn_map); it.key != NULL; function_hash_map_next(fn_map,&it) ){
        out->write(out,it.key,it.key_size);
        ch_word result_len = snprintf(result,32,"%li", it.value);
        out->write(out,(ch_byte*)result,result_len);
        out->write(out,(ch_byte*)&nl, 1);
    }


    in->delete(in);
    out->delete(out);

    ch_log_info("Normal exiting main loop...\n");

    return 0;
}
