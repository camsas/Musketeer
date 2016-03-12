ROOT_DIR=../..
SUFFIX=monitoring

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

OBJS = spark_monitor.o hadoop_monitor.o

all: .setup $(addprefix $(OBJ_DIR)/, $(OBJS))
