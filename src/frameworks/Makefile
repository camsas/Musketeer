ROOT_DIR=../..
SUFFIX=frameworks

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

OBJS = graphchi_framework.o hadoop_framework.o metis_framework.o \
       naiad_framework.o powergraph_framework.o powerlyra_framework.o spark_framework.o \
       wildcherry_framework.o graphchi_dispatcher.o hadoop_dispatcher.o \
       spark_dispatcher.o metis_dispatcher.o naiad_dispatcher.o \
       powergraph_dispatcher.o powerlyra_dispatcher.o wildcherry_dispatcher.o

all: .setup $(addprefix $(OBJ_DIR)/, $(OBJS))
