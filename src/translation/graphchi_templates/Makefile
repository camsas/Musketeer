CXX = g++
JAVA_HOME = /usr/lib/jvm/java-7-openjdk-amd64/
OBJ_DIR = .

LIBS = -ldl -lnuma -lc -lm -lpthread -lhdfs \
       -L$(JAVA_HOME)/jre/lib/amd64/server/ -ljvm -lz -lgomp
BINS = {{CLASS_NAME}}_bin DataTransformer_bin
OBJS = {{CLASS_NAME}}.o DataTransformer.o
OBJ_BIN = $(addprefix $(OBJ_DIR)/, $(BINS))

CPPFLAGS = -I/usr/local/include -I{{GRAPHCHI_DIR}}src \
           -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux/ -pthread \
           -fopenmp -Wall -Wno-strict-aliasing -D_GNU_SOURCE -Wall -g -O0 \
           -fno-omit-frame-pointer

all: $(OBJ_BIN)

# Make object file (generic).
$(OBJ_DIR)/%_bin: $(OBJ_DIR)/%.cc
	@echo "MAKE     $@"
	$(CXX) $(CFLAGS) $(CPPFLAGS) -o $@ $< $(LIBS)
