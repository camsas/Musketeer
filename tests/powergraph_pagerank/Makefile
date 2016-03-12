CXX = clang++
JAVA_HOME = /usr/lib/jvm/java-7-openjdk-amd64/
OBJ_DIR = .

LIBS = -pthread -lz -ltcmalloc -levent -levent_pthread -ljson \
       -lboost_filesystem -lboost_program_options -lboost_system \
       -lboost_iostreams -lboost_date_time -lhdfs -lgraphlab

#LIBS = -ldl -lnuma -lc -lm -lpthread -lhdfs \
#       -L$(JAVA_HOME)/jre/lib/amd64/server/ -ljvm -lz -lgomp
BINS = pagerank_bin
OBJS = pagerank.o
OBJ_BIN = $(addprefix $(OBJ_DIR)/, $(BINS))

CPPFLAGS = -I/usr/local/include -I/home/icg27/graphlab/src \
           -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux/ \
           -fopenmp -Wall -Wno-strict-aliasing -D_GNU_SOURCE -Wall -g -O0 \
           -fno-omit-frame-pointer

all: $(OBJ_BIN)

# Make object file (generic).
$(OBJ_DIR)/%_bin: $(OBJ_DIR)/%.cc
	@echo "MAKE     $@"
	$(CXX) $(CFLAGS) $(CPPFLAGS) -o $@ $< $(LIBS)
