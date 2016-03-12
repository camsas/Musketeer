ROOT_DIR=../..
SUFFIX=base

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

OBJS = utils.o hdfs_utils.o

PBS = job_run.pb.o job.pb.o

all: protobufs $(addprefix $(OBJ_DIR)/, $(OBJS)) .setup

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))
