ROOT_DIR=..
SUFFIX=

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

BINS = musketeer submit_job
LIBS += -lantlr3c -lctemplate -pthread -ljansson -lcurl -ltbb
OBJS = RLPlusLexer.o RLPlusParser.o
ANTLR_OUT = RLPlusLexer.h RLPlusLexer.c RLPlusParser.h RLPlusParser.c
all: .setup $(addprefix $(SRC_ROOT_DIR)/, $(ANTLR_OUT)) $(BINS)

$(addprefix $(SRC_ROOT_DIR)/, $(ANTLR_OUT)): RLPlus.g
	antlr3 RLPlus.g

antlr: $(addprefix $(SRC_ROOT_DIR)/, $(ANTLR_OUT))

submit_job: $(addprefix $(OBJ_DIR)/, $(OBJS)) $(addprefix $(SRC_ROOT_DIR)/, $(ANTLR_OUT))
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/base all
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/frontends all
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/translation all
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/frameworks all
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/monitoring all
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/ir all
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/scheduling all
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/core all
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/tests/mindi all
	$(call quiet-command, \
		$(CXX) $(CPPFLAGS) \
		$(SRC_ROOT_DIR)/submit_job.cc \
		$(OPTFLAGS) \
		$(BUILD_DIR)/base/job.pb.o \
		$(BUILD_DIR)/base/job_run.pb.o \
		$(LIBS) \
		-o $(BUILD_DIR)/submit_job, \
		" DYNLNK $(BUILD_DIR)/submit_job")

musketeer: $(addprefix $(OBJ_DIR)/, $(OBJS)) $(addprefix $(SRC_ROOT_DIR)/, $(ANTLR_OUT))
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/base all
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/frontends all
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/translation all
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/frameworks all
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/monitoring all
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/ir all
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/scheduling all
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/core all
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/tests/mindi all
	$(call quiet-command, \
		$(CXX) $(CPPFLAGS) \
		$(SRC_ROOT_DIR)/musketeer.cc \
		$(OPTFLAGS) \
		$(BUILD_DIR)/RLPlusLexer.o \
		$(BUILD_DIR)/RLPlusParser.o \
		$(BUILD_DIR)/base/hdfs_utils.o \
		$(BUILD_DIR)/base/job.pb.o \
		$(BUILD_DIR)/base/utils.o \
		$(BUILD_DIR)/core/daemon.o \
		$(BUILD_DIR)/core/daemon_connection.o \
		$(BUILD_DIR)/core/history_storage.o \
		$(BUILD_DIR)/core/job_run.o \
		$(BUILD_DIR)/frameworks/graphchi_dispatcher.o \
		$(BUILD_DIR)/frameworks/graphchi_framework.o \
		$(BUILD_DIR)/frameworks/hadoop_dispatcher.o \
		$(BUILD_DIR)/frameworks/hadoop_framework.o \
		$(BUILD_DIR)/frameworks/metis_dispatcher.o \
		$(BUILD_DIR)/frameworks/metis_framework.o \
		$(BUILD_DIR)/frameworks/naiad_dispatcher.o \
		$(BUILD_DIR)/frameworks/naiad_framework.o \
		$(BUILD_DIR)/frameworks/powergraph_dispatcher.o \
		$(BUILD_DIR)/frameworks/powergraph_framework.o \
		$(BUILD_DIR)/frameworks/powerlyra_dispatcher.o \
		$(BUILD_DIR)/frameworks/powerlyra_framework.o \
		$(BUILD_DIR)/frameworks/spark_dispatcher.o \
		$(BUILD_DIR)/frameworks/spark_framework.o \
		$(BUILD_DIR)/frameworks/wildcherry_dispatcher.o \
		$(BUILD_DIR)/frameworks/wildcherry_framework.o \
		$(BUILD_DIR)/frontends/beeraph.o \
		$(BUILD_DIR)/frontends/mindi.o \
		$(BUILD_DIR)/frontends/operator_node.o \
		$(BUILD_DIR)/frontends/relations_type.o \
		$(BUILD_DIR)/frontends/tree_traversal.o \
		$(BUILD_DIR)/ir/agg_operator.o \
		$(BUILD_DIR)/ir/black_box_operator.o \
		$(BUILD_DIR)/ir/column.o \
		$(BUILD_DIR)/ir/condition_tree.o \
		$(BUILD_DIR)/ir/count_operator.o \
		$(BUILD_DIR)/ir/cross_join_operator.o \
		$(BUILD_DIR)/ir/difference_operator.o \
		$(BUILD_DIR)/ir/distinct_operator.o \
		$(BUILD_DIR)/ir/div_operator.o \
		$(BUILD_DIR)/ir/input_operator.o \
		$(BUILD_DIR)/ir/intersection_operator.o \
		$(BUILD_DIR)/ir/join_operator.o \
		$(BUILD_DIR)/ir/max_operator.o \
		$(BUILD_DIR)/ir/min_operator.o \
		$(BUILD_DIR)/ir/mul_operator.o \
		$(BUILD_DIR)/ir/project_operator.o \
		$(BUILD_DIR)/ir/relation.o \
		$(BUILD_DIR)/ir/select_operator.o \
		$(BUILD_DIR)/ir/sub_operator.o \
		$(BUILD_DIR)/ir/sum_operator.o \
		$(BUILD_DIR)/ir/sort_operator.o \
		$(BUILD_DIR)/ir/udf_operator.o \
		$(BUILD_DIR)/ir/union_operator.o \
		$(BUILD_DIR)/ir/while_operator.o \
		$(BUILD_DIR)/monitoring/hadoop_monitor.o \
		$(BUILD_DIR)/monitoring/spark_monitor.o \
		$(BUILD_DIR)/translation/hadoop_job_code.o \
		$(BUILD_DIR)/translation/mapreduce_job_code.o \
		$(BUILD_DIR)/translation/metis_job_code.o \
		$(BUILD_DIR)/translation/naiad_job_code.o \
		$(BUILD_DIR)/translation/spark_job_code.o \
		$(BUILD_DIR)/translation/translator_graphchi.o \
		$(BUILD_DIR)/translation/translator_hadoop.o \
		$(BUILD_DIR)/translation/translator_metis.o \
		$(BUILD_DIR)/translation/translator_naiad.o \
		$(BUILD_DIR)/translation/translator_powergraph.o \
		$(BUILD_DIR)/translation/translator_spark.o \
		$(BUILD_DIR)/translation/translator_wildcherry.o \
		$(BUILD_DIR)/scheduling/scheduler_interface.o \
		$(BUILD_DIR)/scheduling/operator_scheduler.o \
		$(BUILD_DIR)/scheduling/scheduler_dynamic.o \
		$(BUILD_DIR)/scheduling/scheduler_simulator.o \
		$(BUILD_DIR)/tests/mindi/netflix.o \
		$(LIBS) \
		-o $(BUILD_DIR)/musketeer, \
		" DYNLNK $(BUILD_DIR)/musketeer")

# $(SRC_ROOT_DIR)/RLPlus%.c $(SRC_ROOT_DIR)/RLPlus%.h: RLPlus.g
# 	antlr3 RLPlus.g
