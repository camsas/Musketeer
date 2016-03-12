.PHONY: clean test dependencies help info
.DEFAULT: all

# Get common build settings
include include/Makefile.config
include include/Makefile.common

all: tests-clean dependencies musketeer

help: info

info:
	@echo "C++ compiler: $(CXX)"
	@echo "CPPFLAGS: $(CPPFLAGS)"
	@echo "CXXFLAGS: $(CXXFLAGS)"
	@echo "Platform: $(PLATFORM)"
	@echo "Build output in $(BUILD_DIR)"
	@echo
	@echo "Targets:"
	@echo "  - all: build all code"
	@echo "  - clean: remove all temporary build infrastructure"
	@echo "  - examples: build example jobs"
	@echo "  - dependencies: set up external dependencies (should only run once "
	@echo "                  and automatically, but can be re-run manually)"
	@echo "  - test: run unit tests (must be preceded by clean & make all)"
	@echo

dependencies: ext/.ext-ok

ext/.ext-ok:
	$(SCRIPTS_DIR)/setup.sh

musketeer: dependencies base
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR) all

base:
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR) antlr
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/base all

test: dependencies
	$(MAKE) $(MAKEFLAGS) -C tests run

tests-clean:
	rm -f build/tests/all_tests.txt
	mkdir -p build/tests
	touch build/tests/all_tests.txt

clean:
	rm -rf build
	rm -rf src/generated-cxx/*
	rm -rf src/generated-c/*
	rm -f ext/.ext-ok
	find src/ -depth -name .setup -type f -delete

lint:
	python tests/all_lint.py src/ False

lint-verb:
	python tests/all_lint.py src/ True
