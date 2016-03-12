ROOT_DIR=../..
SUFFIX=translation

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

OBJS = graphchi_job_code.o hadoop_job_code.o mapreduce_job_code.o \
       metis_job_code.o naiad_job_code.o powergraph_job_code.o \
       translator_graphchi.o translator_hadoop.o translator_metis.o \
       translator_naiad.o translator_powergraph.o translator_spark.o \
       translator_wildcherry.o spark_job_code.o

all: $(addprefix $(OBJ_DIR)/, $(OBJS)) .setup
