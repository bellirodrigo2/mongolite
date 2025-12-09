# Single Makefile for entire project

CC = gcc
CFLAGS = -Wall -Wextra -std=c11 -O2 -fPIC -Isrc
DEBUG_FLAGS = -g -O0
LDFLAGS = -lpthread
AR = ar

# Directories
SRC_DIR = src
TEST_DIR = tests
BUILD_DIR = build
BIN_DIR = bin

# Source files
GERROR_SRC = $(SRC_DIR)/gerror.c
WTREE_SRC = $(SRC_DIR)/wtree.c
LMDB_SRC = $(SRC_DIR)/mdb.c $(SRC_DIR)/midl.c

# All library sources
LIB_SOURCES = $(GERROR_SRC) $(WTREE_SRC) $(LMDB_SRC)
LIB_OBJECTS = $(patsubst $(SRC_DIR)/%.c,$(BUILD_DIR)/%.o,$(LIB_SOURCES))

# Test sources
TEST_SOURCES = $(wildcard $(TEST_DIR)/test_*.c)
TEST_BINARIES = $(patsubst $(TEST_DIR)/%.c,$(BIN_DIR)/%,$(TEST_SOURCES))

# Library targets
STATIC_LIB = $(BUILD_DIR)/libwtree.a
SHARED_LIB = $(BUILD_DIR)/libwtree.so

# Default target
all: lib

# Create directories
$(BUILD_DIR) $(BIN_DIR):
	@mkdir -p $@

# Compile library objects
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c | $(BUILD_DIR)
	@echo "  CC  $<"
	@$(CC) $(CFLAGS) -c $< -o $@

# Build static library
lib: $(STATIC_LIB)

$(STATIC_LIB): $(LIB_OBJECTS)
	@echo "  AR  $@"
	@$(AR) rcs $@ $^
	@echo "Library built: $@"

# Build shared library
shared: $(LIB_OBJECTS) | $(BUILD_DIR)
	@echo "  LD  $(SHARED_LIB)"
	@$(CC) -shared -o $(SHARED_LIB) $^ $(LDFLAGS)
	@echo "Shared library built: $(SHARED_LIB)"

# Build test executables
$(BIN_DIR)/test_gerror: $(TEST_DIR)/test_gerror.c $(GERROR_SRC) | $(BIN_DIR)
	@echo "  CC  $@"
	@$(CC) $(CFLAGS) $(DEBUG_FLAGS) -o $@ $^ $(LDFLAGS)

$(BIN_DIR)/test_wtree: $(TEST_DIR)/test_wtree.c $(LIB_SOURCES) | $(BIN_DIR)
	@echo "  CC  $@"
	@$(CC) $(CFLAGS) $(DEBUG_FLAGS) -o $@ $^ $(LDFLAGS)

# Build all tests
build-tests: $(TEST_BINARIES)

# Run all tests
test: build-tests
	@echo "\n=== Running Tests ===\n"
	@failed=0; \
	for test in $(TEST_BINARIES); do \
		echo "Running $$(basename $$test):"; \
		$$test || failed=1; \
	done; \
	if [ $$failed -eq 0 ]; then \
		echo "\n✓ All tests passed!\n"; \
	else \
		echo "\n✗ Some tests failed!\n"; \
		exit 1; \
	fi

# Run specific test
test-gerror: $(BIN_DIR)/test_gerror
	@echo "\n=== Running gerror tests ===\n"
	@$(BIN_DIR)/test_gerror

test-wtree: $(BIN_DIR)/test_wtree
	@echo "\n=== Running wtree tests ===\n"
	@$(BIN_DIR)/test_wtree

# Memory check with valgrind
memcheck: build-tests
	@echo "\n=== Memory Check ===\n"
	@for test in $(TEST_BINARIES); do \
		echo "\nChecking $$(basename $$test):"; \
		valgrind --leak-check=full --quiet --error-exitcode=1 $$test > /dev/null 2>&1; \
		if [ $$? -eq 0 ]; then \
			echo "  ✓ No memory leaks"; \
		else \
			echo "  ✗ Memory issues detected (run with valgrind for details)"; \
		fi; \
	done

memcheck-verbose: build-tests
	@for test in $(TEST_BINARIES); do \
		echo "\n=== Checking $$(basename $$test) ==="; \
		valgrind --leak-check=full --show-leak-kinds=all $$test; \
	done

# Build examples
example: $(SRC_DIR)/example_modular.c $(STATIC_LIB)
	@echo "  CC  example"
	@$(CC) $(CFLAGS) -o $(BIN_DIR)/example $< $(STATIC_LIB) $(LDFLAGS)
	@echo "Example built: $(BIN_DIR)/example"

# Install (optional)
PREFIX ?= /usr/local
INSTALL_LIB = $(PREFIX)/lib
INSTALL_INC = $(PREFIX)/include

install: $(STATIC_LIB)
	@echo "Installing to $(PREFIX)..."
	@mkdir -p $(INSTALL_LIB) $(INSTALL_INC)
	@cp $(STATIC_LIB) $(INSTALL_LIB)/
	@cp $(SRC_DIR)/wtree.h $(SRC_DIR)/gerror.h $(INSTALL_INC)/
	@echo "Installation complete!"

uninstall:
	@echo "Removing from $(PREFIX)..."
	@rm -f $(INSTALL_LIB)/libwtree.a
	@rm -f $(INSTALL_INC)/wtree.h $(INSTALL_INC)/gerror.h
	@echo "Uninstallation complete!"

# Clean
clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR) $(BIN_DIR)
	@rm -rf test_wtree_db
	@rm -f core *.core vgcore.*
	@echo "Clean complete!"

# Development helpers
debug: CFLAGS += $(DEBUG_FLAGS)
debug: clean lib

# Format code (requires clang-format)
format:
	@echo "Formatting code..."
	@find $(SRC_DIR) $(TEST_DIR) -name "*.c" -o -name "*.h" | xargs clang-format -i
	@echo "Format complete!"

# Show help
help:
	@echo "wtree build system"
	@echo ""
	@echo "Targets:"
	@echo "  all             - Build static library (default)"
	@echo "  lib             - Build static library"
	@echo "  shared          - Build shared library"
	@echo "  test            - Run all tests"
	@echo "  test-gerror     - Run gerror tests only"
	@echo "  test-wtree      - Run wtree tests only"
	@echo "  memcheck        - Check for memory leaks (brief)"
	@echo "  memcheck-verbose- Check for memory leaks (detailed)"
	@echo "  example         - Build example program"
	@echo "  install         - Install library and headers"
	@echo "  uninstall       - Remove installed files"
	@echo "  clean           - Remove all build artifacts"
	@echo "  debug           - Build with debug symbols"
	@echo "  format          - Format code with clang-format"
	@echo "  help            - Show this help"
	@echo ""
	@echo "Variables:"
	@echo "  PREFIX=/path    - Installation prefix (default: /usr/local)"
	@echo "  CC=compiler     - C compiler (default: gcc)"
	@echo ""
	@echo "Examples:"
	@echo "  make            - Build library"
	@echo "  make test       - Build and run tests"
	@echo "  make install PREFIX=/opt/local"

# Info about project
info:
	@echo "Project structure:"
	@echo "  Sources:  $(words $(LIB_SOURCES)) files"
	@echo "  Tests:    $(words $(TEST_SOURCES)) files"
	@echo "  Library:  $(STATIC_LIB)"
	@ls -lh $(STATIC_LIB) 2>/dev/null || echo "  (not built yet)"

.PHONY: all lib shared build-tests test test-gerror test-wtree \
        memcheck memcheck-verbose example install uninstall \
        clean debug format help info