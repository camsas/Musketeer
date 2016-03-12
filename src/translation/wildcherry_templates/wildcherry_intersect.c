#define _IS_TEMPLATE

#include "libchaste/include/chaste.h"
#include <libchaste/include/parsing/numeric_parser.h>
#include "fileio/fileio_mmap.h"
#include "datastructs/wildcherry_types.h"

#include <stdio.h>
#include <stdlib.h>

#define COL(i) ( row->off(row, i) )
#define COLI(i) ( row->off(row, i)->integer )
#define COLF(i) ( row->off(row, i)->floating )

static file_state_t* in_l;
static file_state_t* in_r;
static file_state_t* out;

static struct {
    ch_cstr in_l;
    ch_cstr in_r;
    ch_bool is_diff;
    ch_word log_level;
} options;


#if IS_TEMPLATE
/*************** TEMPLATE FOO GOES HERE *********************/
/************************************************************/

<PUT A COPY OF THE STUFF BELOW IN HERE AND ADD TEMPLATE MAGIC>

/************************************************************/
/************************************************************/
#else
    static char* in_lhs_file  = "tests/test_1.in";
    static char* in_rhs_file  = "tests/test_1_r.in";
    static char* out_file = "test.out";
    ch_word is_diff = 0;
#endif


static inline void write_out(column* col){
    out->write(out,col->start_mark, (col->end_mark - col->start_mark));
}


USE_CH_LOGGER(CH_LOG_LVL_INFO,1,CH_LOG_OUT_STDERR,"");
USE_CH_OPTIONS;
int main(int argc, char** argv)
{
    ch_log_info("Running wildcherry sum\n");
    ch_opt_addsi(CH_OPTION_OPTIONAL, 'l', "input-left", "RHS file to run agg on", &options.in_l, in_lhs_file);
    ch_opt_addsi(CH_OPTION_OPTIONAL, 'r', "input-right", "LHS file to run agg on", &options.in_r, in_rhs_file);
    ch_opt_addbi(CH_OPTION_FLAG, 'd', "diff", "Produce the difference instead of intersect", &options.is_diff, is_diff);
    ch_opt_addii(CH_OPTION_OPTIONAL, 'v', "log-level", "Log level verbosity", &options.log_level, CH_LOG_LVL_INFO);
    ch_opt_parse(argc, argv);

    ch_log_settings.log_level = options.log_level;
    ch_log_settings.subsec_digits = 6;

    in_l  = fileio_new("mmap", options.in_l );
    in_r  = fileio_new("mmap", options.in_r );
    out = fileio_new("cwrite", out_file);

    ch_word len      = 0;
    ch_byte* data    = NULL;

    column col_value = { 0 } ;
    ch_word value    = 0;
    ch_hash_map* map = ch_hash_map_new(1024 * 1024, sizeof(value), NULL);

    ch_log_info("Starting main loop...\n");
    ch_log_info("Reading in left file...\n");
    data = in_l->read(in_l, &len);
    if(len <= 0){
        ch_log_fatal("No data supplied! Can't continue\n");
    }

    if(data[len -1] != '\n'){
        ch_log_fatal("No newline at end of file. Cannot continue!\n");
    }


    ch_log_info("Got %li bytes\n", len);
    ch_log_info("Analyzing in left file...\n");
    for(ch_word i = 0; i < len; i++){
        col_value.type       = COL_TYPE_STR;
        col_value.start_mark = &data[i];
        col_value.updated    = 0;

        for( ; data[i] != '\n'; i++){}
        col_value.end_mark = &data[i + 1];

        ch_hash_map_it it = hash_map_get_first(map,col_value.start_mark,(col_value.end_mark - col_value.start_mark) );
        if(!it.value){
            hash_map_push(map,col_value.start_mark,(col_value.end_mark - col_value.start_mark), &value );
        }

    }

    ch_log_info("Reading in right file...\n");
    ch_log_info("Got %li bytes\n", len);
    data = in_r->read(in_r, &len);

    if(data[len -1] != '\n'){
        ch_log_fatal("No newline at end of file. Cannot continue!\n");
    }


    if(len <= 0){
        ch_log_fatal("No data supplied! Can't continue\n");
    }

    ch_log_info("Analyzing in right file...\n");
    for(ch_word i = 0; i < len; i++){
        col_value.type       = COL_TYPE_STR;
        col_value.start_mark = &data[i];
        col_value.updated    = 0;

        for( ; data[i] != '\n'; i++){}
        col_value.end_mark = &data[i + 1];

        ch_hash_map_it it = hash_map_get_first(map,col_value.start_mark,(col_value.end_mark - col_value.start_mark)  );
        if(options.is_diff){
            if( it.value == NULL){
                write_out(&col_value);
            }
            else{
                *(ch_word*)it.value = 1; //Mark this as "seen"
            }
        }
        else
        {
            if( it.value != NULL){
                write_out(&col_value);
            }
        }
    }

    if(options.is_diff){
        ch_log_info("Outputting in left file...\n");
        for( ch_hash_map_it it = hash_map_first(map); it.value; hash_map_next(map, &it)){
            if(*(ch_word*)it.value == 0){
                out->write(out,it.key, it.key_size);
            }
        }
    }


    in_l->delete(in_l);
    in_r->delete(in_r);
    out->delete(out);

    ch_log_info("Normal exiting main loop...\n");

    return 0;
}
