#include "libchaste/include/chaste.h"
#include <stdio.h>
#include "fileio/fileio_mmap.h"
#include <stdlib.h>


typedef struct {
    ch_word idx;
    ch_word col_id;
    ch_byte* start_mark;
    ch_byte* end_mark;
} col_match;


#ifdef IS_TEMPLATE
/*************** TEMPLATE FOO GOES HERE *********************/
/************************************************************/

    #define PROJ_COLS_IN_COUNT {{COL_SIZE}}
    #define PROJ_COLS_IN { {{PROJ_COLS}} }

    static char* in_file = "{{LOCAL_INPUT_PATH}}";
    static char* out_file = "{{LOCAL_OUTPUT_PATH}}";

/************************************************************/
/************************************************************/
#else

    #define PROJ_COLS_IN_COUNT 1
    #define PROJ_COLS_IN { {0, 0, NULL, NULL } }

    static char* in_file = "test.out";
    static char* out_file = "tests/test_1.in";

#endif

col_match cols_sorted[PROJ_COLS_IN_COUNT] = PROJ_COLS_IN;

static file_state_t* in;
static file_state_t* out;

USE_CH_LOGGER_DEFAULT;
USE_CH_OPTIONS;

static struct {
    ch_cstr input;
    ch_cstr output;
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

static void do_write_out(){

    //Sort by index to write out
    qsort(cols_sorted,PROJ_COLS_IN_COUNT,sizeof(col_match),cmp_by_idx);

    const char* sp = " ";
    for(ch_word i = 0; i < PROJ_COLS_IN_COUNT; i++){
        out->write(out, cols_sorted[i].start_mark, cols_sorted[i].end_mark - cols_sorted[i].start_mark);
        if(i < PROJ_COLS_IN_COUNT - 1){
            out->write(out,(ch_byte*)sp,1);
        }
    }

    const char* nl = "\n";
    out->write(out,(ch_byte*)nl,1);

    qsort(cols_sorted,PROJ_COLS_IN_COUNT,sizeof(col_match),cmp_by_id);

}

int main(int argc, char** argv)
{

    ch_log_info("Running wildcherry project\n");
    ch_opt_addsi(CH_OPTION_OPTIONAL,'i', "input", "input file for relation", &options.input, in_file);
    ch_opt_addsi(CH_OPTION_OPTIONAL,'o', "output", "output file for relation", &options.output, out_file);
    ch_opt_parse(argc, argv);

    in  = fileio_new("mmap", options.input);
    out = fileio_new("cwrite", options.output);

    ch_word len = 0;
    ch_byte* data = NULL;

    ch_byte* start_mark = NULL;
    ch_byte* end_mark = NULL;

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
    qsort(cols_sorted,PROJ_COLS_IN_COUNT,sizeof(col_match),cmp_by_id);
    ch_word col_id_idx = 0;
    ch_word col_id = cols_sorted[col_id_idx].col_id;

    //iterate over the data that we have
    ch_word col_idx = 0;
    ch_word row_idx = 0;

    for(ch_word i = 0; i < len; i++){
        if(data[i] == '\n'){
            row_idx++;
            col_idx = 0;
            if(start_mark){
                end_mark = &data[i];
                cols_sorted[col_id_idx].start_mark = start_mark;
                cols_sorted[col_id_idx].end_mark = end_mark;

                start_mark = NULL;
                end_mark = NULL;
            }

            //Write out the wole row now that it's built up
            do_write_out();

            col_id_idx = 0;
            col_id = cols_sorted[col_id_idx].col_id;


            continue;
        }

        if(data[i] == ' '){
            col_idx++;
            continue;
        }

        if(start_mark && col_idx == col_id + 1 ){
            end_mark = &data[i - 1];

            cols_sorted[col_id_idx].start_mark = start_mark;
            cols_sorted[col_id_idx].end_mark = end_mark;

            start_mark = NULL;
            end_mark = NULL;
            col_id_idx++;
            col_id = cols_sorted[col_id_idx].col_id;

            if(start_mark == NULL && col_idx == col_id && col_id_idx < PROJ_COLS_IN_COUNT){
                start_mark = &data[i];
            }
        }

        if(start_mark == NULL && col_idx == col_id && col_id_idx < PROJ_COLS_IN_COUNT){
            start_mark = &data[i];
            continue;
        }

    }


    in->delete(in);
    out->delete(out);

    ch_log_info("Normal exiting main loop...\n");

    return 0;
}
