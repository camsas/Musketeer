
//#include "libchaste/include/log/log_levels.h"
//#define CH_LOG_BUILD_LVL CH_LOG_LVL_DEBUG3

#include "libchaste/include/chaste.h"
#include <stdio.h>
#include "fileio/fileio_mmap.h"
#include <stdlib.h>

//#define IS_TEMPLATE
//#define LJ_JOIN


#ifdef IS_TEMPLATE
/*************** TEMPLATE FOO GOES HERE *********************/
/************************************************************/

    static char* in_file_l  = "{{IN_FILE_LEFT}}";
    static char* in_file_r  = "{{IN_FILE_RIGHT}}";
    static char* out_file   = "{{OUT_FILE}}";
    ch_word col_id_l = {{COL_ID_LEFT}};
    ch_word col_id_r = {{COL_ID_RIGHT}};


/************************************************************/
/************************************************************/
#else
    static char* in_file_l  = "tests/test_1.in";
    static char* in_file_r  = "tests/test_1_r.in";
    static char* out_file   = "test.out";
    ch_word col_id_l = 1;
    ch_word col_id_r = 1;
#endif
/************************************************************/
/************************************************************/

static file_state_t* inl;
static file_state_t* inr;
static file_state_t* out;

//USE_CH_LOGGER(CH_LOG_LVL_DEBUG3,true,CH_LOG_OUT_STDOUT,"");
USE_CH_LOGGER_DEFAULT;
USE_CH_OPTIONS;

static struct {
    ch_cstr left;
    ch_word left_col;
    ch_cstr right;
    ch_word right_col;
    ch_cstr output;
    ch_bool str_key;
} options;


typedef struct{
    u8* start;
    ch_word len;
} row;


//make_perf_module(TSC init_main[1]; TSC lhs[1]; TSC rhs[1];);


int main(int argc, char** argv)
{
    //perf_mod_start(init_main);
    ch_log_info("Running wildcherry project\n");
    ch_opt_addsi(CH_OPTION_OPTIONAL,'l', "left",      "input file for relation", &options.left, in_file_l );
    ch_opt_addii(CH_OPTION_OPTIONAL,'L', "left-col",  "left coloumn number", &options.left_col, col_id_l);
    ch_opt_addsi(CH_OPTION_OPTIONAL,'r', "right",     "input file for relation", &options.right, in_file_r);
    ch_opt_addii(CH_OPTION_OPTIONAL,'R', "right-col", "right coloumn number", &options.right_col, col_id_r);
    ch_opt_addsi(CH_OPTION_OPTIONAL,'o', "output",    "output file for relation", &options.output, out_file );
    ch_opt_addbi(CH_OPTION_FLAG    ,'s', "str-key",   "use string keys, slower but more robust", &options.str_key, false);

    ch_opt_parse(argc, argv);

    inl  = fileio_new("mmap", options.left);
    inr  = fileio_new("mmap", options.right);
    out = fileio_new("cwrite", options.output);
    ch_word lcol_id = options.left_col;
    ch_word rcol_id = options.right_col;

    ch_word lenl = 0;
    ch_byte* datal = NULL;
    ch_word lenr = 0;
    ch_byte* datar = NULL;

    ch_byte* key_start_mark = NULL;
    ch_byte* key_end_mark = NULL;
    ch_byte* row_start_mark = NULL;
    ch_byte* row_end_mark = NULL;

    ch_log_info("Starting main loop...\n");

    datal = inl->read(inl, &lenl);
    if(lenl <= 0){
        ch_log_fatal("No data supplied! Can't continue\n");
    }
    if(datal[lenl -1] != '\n'){
        ch_log_fatal("No newline at end of file. Cannot continue!\n");
    }

    ch_log_debug1("Got %li bytes from left %p\n", lenl, (void*)datal);


    datar = inr->read(inr, &lenr);
    if(lenr <= 0){
        ch_log_fatal("No data supplied! Can't continue\n");
    }
    if(datar[lenr -1] != '\n'){
        ch_log_fatal("No newline at end of file. Cannot continue!\n");
    }


    ch_log_debug1("Got %li bytes from right at %p\n", lenr, (void*)datar);


    ch_hash_map* map = ch_hash_map_new(1024 * 1024,sizeof(row),NULL);

    //perf_mod_end(init_main,0);
    //iterate over the data that we have in the left file, push it into the hash map
    ch_word col_idx = 0;
    //ch_word row_idx = 0;
    ch_log_info("Loading left data...\n");
    row_start_mark = &datal[0];
    u64 left_key_uint = 0;
    for(ch_word i = 0; i < lenl; i++){
        if(datal[i] == '\n'){


            if(!key_end_mark){
                key_end_mark = &datal[i-1];
            }

            row_end_mark = &datal[i];

            row lrow = { .start = row_start_mark, .len = (row_end_mark - row_start_mark )  };

            ch_word key_len = key_end_mark- key_start_mark;
            ch_log_debug1("Key len=%li, row len=%li, key=%.*s\n", key_len, lrow.len, key_len, key_start_mark);

            if(options.str_key){
            	hash_map_push_unsafe_ptr(map,key_start_mark,key_len,&lrow);
            }
            else{
                ch_log_debug1("Key=%lu\n", left_key_uint);
                hash_map_push(map,&left_key_uint,sizeof(left_key_uint),&lrow);
            }

            //row_idx++;
            col_idx = 0;
            key_start_mark = NULL;
            key_end_mark = NULL;
            row_start_mark = &datal[i+1];
            left_key_uint = 0;
            //ch_log_debug3("Now on row = %li\n", row_idx);

            continue;
        }

        if(datal[i] == ' '){
            col_idx++;
            continue;
        }

        if(key_start_mark && !key_end_mark && col_idx == lcol_id + 1 ){
            key_end_mark = &datal[i - 1];
            //ch_log_debug1("Key end at %p [idx=%li]\n", key_end_mark, col_idx);
            continue;
        }

        if(key_start_mark && !key_end_mark){
            left_key_uint *= 10;
            left_key_uint += (u8)datal[i] - '0';
            continue;
        }

        if(key_start_mark == NULL && col_idx == lcol_id){
            key_start_mark = &datal[i];
            left_key_uint = (u8)datal[i] - '0';
            //ch_log_debug1("Key start at %p [idx=%li]\n", key_start_mark, col_idx);
            continue;
        }

    }

    ch_log_info("Loading right data...\n");
    //Iterate over the data in the right file, compare it to the left map
    col_idx = 0;
    //row_idx = 0;

    u64 right_key_uint = 0;
    row_start_mark = &datar[0];
    for(ch_word i = 0; i < lenr; i++){
        if(datar[i] == '\n'){

            ch_log_debug1("Key_uint=%lu\n", right_key_uint);

            if(!key_end_mark){
                key_end_mark = &datar[i-1];
            }

            row_end_mark = &datar[i];
            const ch_word key_len = key_end_mark - key_start_mark;
            //ch_word row_len = row_end_mark - row_start_mark;

            ch_hash_map_it it = { 0 };
            if(options.str_key){
            	//ch_log_debug1("[R:] %.*s -> %.*s\n", key_len, key_start_mark, row_len, row_start_mark);
            	it = hash_map_get_first(map,key_start_mark,key_len);
            }
            else{
            	//ch_log_debug1("[R:] %lu -> %.*s\n", right_key_uint, row_len, row_start_mark);
            	it = hash_map_get_first(map,&right_key_uint,sizeof(right_key_uint));
            }

            ch_word collisions = 0;
            while(it.value){

                row rrow1 = { .start = row_start_mark, .len = (key_start_mark - row_start_mark )  };
                row rrow2 = { .start = key_start_mark + key_len + 1, .len = (row_end_mark - (key_start_mark + key_len + 1) )  };
                //ch_log_debug1("RRow1 len=%li, RRow2 len=%li\n", rrow1.len, rrow2.len);

                row* lrow = (row*)it.value;
                //ch_log_debug1("lKey len=%li, lrow len=%li, lkey=%.*s\n", it.key_size, lrow->len, it.key_size, it.key);
                //ch_log_debug1("{L:} %.*s -> %.*s\n", it.key_size, it.key, lrow->len, lrow->start);
                ch_log_debug1("{L:} %lu -> %.*s\n", *(u64*)it.key, lrow->len, lrow->start);
                it = hash_map_get_next(it);
                out->write(out,lrow->start, lrow->len);

                char* sp = " ";
                out->write(out, (u8*)sp, 1);
                out->write(out,rrow1.start, rrow1.len);

                //out->write(out, (u8*)sp, 1);
                out->write(out,rrow2.start, rrow2.len);

                char* nl = "\n";
                out->write(out, (u8*)nl, 1);
                collisions++;
            }

            //ch_log_info("Collisions =%lu\n", collisions);


            //row_idx++;
            col_idx = 0;
            key_start_mark = NULL;
            key_end_mark = NULL;
            row_start_mark = &datar[i+1];
            right_key_uint = 0;
            //ch_log_debug3("Now on row = %li\n", row_idx);

            continue;
        }

        if(datar[i] == ' '){
            col_idx++;
            continue;
        }


        if(key_start_mark && !key_end_mark && col_idx == rcol_id + 1 ){
            key_end_mark = &datar[i - 1];
            ch_log_debug1("Key end at %p [idx=%li]\n", key_end_mark, col_idx);
            continue;
        }

        if(key_start_mark && !key_end_mark){
            right_key_uint *= 10;
            right_key_uint += (u8)datar[i] - '0';
            continue;
        }

        if(key_start_mark == NULL && col_idx == rcol_id){
            key_start_mark = &datar[i];
            right_key_uint = (u8)datar[i] - '0';
            ch_log_debug1("Key start at %p [idx=%li]\n", key_start_mark, col_idx);
            continue;
        }

    }


    inl->delete(inl);
    inr->delete(inr);
    out->delete(out);

    ch_log_info("Normal exiting main loop...\n");
   // print_perf_stats(get_perf_mod_ref);

    return 0;
}
