ROOT_DIR=../..
SUFFIX=scheduling

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

OBJS = operator_scheduler.o scheduler_dynamic.o scheduler_interface.o scheduler_simulator.o

PBS =

all: protobufs $(addprefix $(OBJ_DIR)/, $(OBJS)) .setup

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))
