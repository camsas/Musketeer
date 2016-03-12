ROOT_DIR=../..
SUFFIX=ir

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

OBJS = agg_operator.o black_box_operator.o count_operator.o \
	cross_join_operator.o difference_operator.o div_operator.o \
	intersection_operator.o join_operator.o max_operator.o min_operator.o \
	project_operator.o select_operator.o sort_operator.o sum_operator.o \
	union_operator.o mul_operator.o project_operator.o select_operator.o \
	sort_operator.o sub_operator.o sum_operator.o udf_operator.o \
	union_operator.o while_operator.o condition_tree.o input_operator.o \
	distinct_operator.o column.o relation.o

all: $(addprefix $(OBJ_DIR)/, $(OBJS)) .setup
