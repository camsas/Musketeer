ROOT_DIR=../..
SUFFIX=frontends

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

OBJS = operator_node.o relations_type.o tree_traversal.o beeraph.o mindi.o

PBS =

all: protobufs $(addprefix $(OBJ_DIR)/, $(OBJS)) .setup

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))
