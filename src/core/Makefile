ROOT_DIR=../..
SUFFIX=core

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

OBJS = daemon.o daemon_connection.o history_storage.o job_run.o

PBS =

all: protobufs $(addprefix $(OBJ_DIR)/, $(OBJS)) .setup

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))
