CXX = g++
JAVA_HOME = /usr/lib/jvm/java-7-openjdk-amd64/
OBJ_DIR = .

LIBS = -pthread -lz -ltcmalloc -levent -levent_pthreads -ljson \
       -lboost_filesystem -lboost_program_options -lboost_system \
       -lboost_iostreams -lboost_date_time -lhdfs -lgraphlab \
       -L$(JAVA_HOME)/jre/lib/amd64/server/
BINS = {{CLASS_NAME}}_bin
OBJS = {{CLASS_NAME}}.o
OBJ_BIN = $(addprefix $(OBJ_DIR)/, $(BINS))

CPPFLAGS = -I/usr/local/include -I$(JAVA_HOME)/include \
           -I$(JAVA_HOME)/include/linux/ -Wall -Wno-strict-aliasing -g -O0

all: $(OBJ_BIN)

# Make object file (generic).
$(OBJ_DIR)/%_bin: $(OBJ_DIR)/%.cc
	@echo "MAKE     $@"
	$(CXX) $(CFLAGS) $(CPPFLAGS) -o $@ $< $(LIBS)
