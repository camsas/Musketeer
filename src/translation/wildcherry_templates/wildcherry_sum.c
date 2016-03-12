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

static file_state_t* in;
static file_state_t* out;

static struct {
    ch_cstr in;
} options;


#ifdef IS_TEMPLATE
/*************** TEMPLATE FOO GOES HERE *********************/
/************************************************************/

    static char* in_file  = "{{IN_FILE}}";
    static char* out_file = "{{OUT_FILE}}";

    #define EVAL_SUM {{EVAL_SUM}}  //Set this to 1 if you want to evaluate a summation expression of the form LHS = LHS (OPR) RHS
    #if EVAL_SUM
        #define SUM_EXPR  {{ASSIGN_SUM}} = {{LHS}}  {{OPR}}  {{RHS}};  {{UPDATED}} = 1;
        #define COND {{COND}}
    #endif

    #define EVAL_EXPR {{EVAL_EXPR}} //Set this to 1 if you wish to evaluate a selection expression
    #if EVAL_EXPR
        #define OUT_COL_COUNT {{OUT_COL_COUNT}} //Number of columns to output
        #define OUT_COLS { {{OUT_COLS}} }  //List of column indexes to output
        #define EXPR  ( {{EXPR}} ) //Selection expression to evaluate
    #endif


/************************************************************/
/************************************************************/
#else
    static char* in_file  = "tests/test_1.in";
    static char* out_file = "test.out";

    #define EVAL_SUM 1 //Set this to 1 if you want to evaluate a summation expression of the form LHS = LHS (OPR) RHS
    #define LHS 1      //Column number for the left hand side
    #define RHS 1      //Column number for the right hand side
    #define OPR +      //Operator to use
    #define SUM_EXPR  COLI(LHS) = COLI(LHS)  OPR  COLI(RHS); COL(LHS)->updated = 1;
    #define COND (1)

    #define EVAL_EXPR 1                                 //Set this to 1 if you wish to evaluate a selection expression
    #define OUT_COL_COUNT 3                            //Number of columns to output
    #define OUT_COLS { 2,1,0 }                           //Columns to output
    #define EXPR (  COLI(1) > COLI(2) && COLI(2) < 5 ) //Selection expression to evaluate

#endif


static inline void col_fn(CH_VECTOR(row)* row, CH_VECTOR(row)* row_out)
{
    #if EVAL_SUM
    {
        SUM_EXPR;

        if(COND){
            row_out->push_back_carray(row_out, row->first,row->count);
        }
        return;
    }
    #endif

    #if EVAL_EXPR
    {
        ch_word out_cols[OUT_COL_COUNT] = OUT_COLS;
        if(EXPR){
            for(ch_word i = 0; i < OUT_COL_COUNT; i++ ){
                row_out->push_back(row_out, *row->off(row,out_cols[i]));
            }
        }
        return;
    }
    #endif


}


static u64 row_n = 0;
static inline void write_out(CH_VECTOR(row)* row){
    if(!row->count){
        return;
    }

    ch_cstr sp = " ";
    ch_cstr nl = "\n";

    row_n++;

    for(ch_word i = 0; i < row->count; i++){
        column* col = row->off(row,i);
        char num2str[64] = { 0 };
        ch_word num2str_len = 0;

        if(col->updated){
            switch(col->type){
                case COL_TYPE_INT: {
                    num2str_len =  snprintf(num2str, 64, "%li", col->integer);
                    out->write(out,(u8*)num2str,num2str_len);
                    break;
                }
                case COL_TYPE_FLOAT:{
                    num2str_len =  snprintf(num2str, 64, "%lf", col->floating);
                    out->write(out,(u8*)num2str,num2str_len);
                    break;
                }
                default:
                    ch_log_fatal("Error, string column updated on row %lu!\n", row_n);
            }
        }
        else{
            out->write(out,col->start_mark, col->end_mark - col->start_mark);
        }

        if(i < row->count - 1){
            out->write(out, (u8*)sp, 1);
        }
    }

    out->write(out, (u8*)nl, 1);
}


USE_CH_LOGGER_DEFAULT;
USE_CH_OPTIONS;
int main(int argc, char** argv)
{

    ch_log_info("Running wildcherry sum\n");
    ch_opt_addsi(CH_OPTION_OPTIONAL, 'i', "input", "File to run agg on", &options.in, in_file);
    ch_opt_parse(argc, argv);

    in  = fileio_new("mmap", options.in );
    out = fileio_new("cwrite", out_file);

    ch_word len = 0;
    ch_byte* data = NULL;

    num_result_t num = {0};
    column col_value = { 0 } ;

    CH_VECTOR(row)* row = CH_VECTOR_NEW(row,10,NULL);
    CH_VECTOR(row)* row_out = CH_VECTOR_NEW(row,10,NULL);


    ch_log_info("Starting main loop...\n");
    data = in->read(in, &len);
    if(len <= 0){
        ch_log_fatal("No data supplied! Can't continue\n");
    }
    if(data[len -1] != '\n'){
        ch_log_fatal("No newline at end of file. Cannot continue!\n");
    }


    ch_log_debug1("Got %li bytes\n", len);

    for(ch_word i = 0; i < len; i++){
        col_value.start_mark = &data[i];

        num = parse_number( (char*)data, i);

        switch(num.type){
            case CH_UINT64: //Fall through intentional
            case CH_INT64 :{
                //ch_log_info("Found (u)int\n");
                col_value.type     = COL_TYPE_INT;
                col_value.integer  = num.val_int;
                col_value.end_mark = &data[num.index];
                col_value.updated  = 0;
                i                  = num.index;
                row->push_back(row,col_value);
                break;
            }
            case CH_DOUBLE:{
                //ch_log_info("Found double\n");
                col_value.type     = COL_TYPE_FLOAT;
                col_value.floating = num.val_dble;
                col_value.end_mark = &data[num.index];
                col_value.updated  = 0;
                i                  = num.index;
                row->push_back(row,col_value);
                break;
            }
            default:{
                col_value.type       = COL_TYPE_STR;
                col_value.start_mark = &data[i];
                col_value.updated    = 0;

                for( ; !( data[i] == ' ' || data[i] == '\n'); i++){}
                col_value.end_mark = &data[i];
                if(col_value.start_mark != col_value.end_mark){
                    row->push_back(row,col_value);
                }
            }
        }

        //Do per row processing
        if(data[i] == '\n'){
            col_fn(row, row_out);
            write_out(row_out);
            row->clear(row);
            row_out->clear(row_out);
        }
    }

    in->delete(in);
    out->delete(out);

    ch_log_info("Normal exiting main loop...\n");

    return 0;
}
