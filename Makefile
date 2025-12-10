# Single Makefile for entire project

# =========================
# Compiler
# =========================

CC = gcc
CFLAGS = -Wall -Wextra -std=c11 -O2 -fPIC -Isrc
# CC = tcc
# CFLAGS = -Wall -Wextra -O2 -Isrc
CFLAGS += -Iexternal/lmdb/libraries/liblmdb

DEBUG_FLAGS = -g -O0
LDFLAGS =
AR = ar

# =========================
# Directories
# =========================
SRC_DIR = src
TEST_DIR = tests
BUILD_DIR = build
BIN_DIR = bin

# =========================
# LMDB (external build)
# =========================
LMDB_DIR = external/lmdb/libraries/liblmdb
LMDB_BUILD_LIB = $(LMDB_DIR)/liblmdb.a
LOCAL_LMDB_LIB = $(BUILD_DIR)/liblmdb.a
LMDB_LIB = $(LOCAL_LMDB_LIB)

$(LMDB_BUILD_LIB):
	@echo "=== Building ONLY liblmdb.a ==="
	@$(MAKE) -C $(LMDB_DIR) liblmdb.a

$(LOCAL_LMDB_LIB): $(LMDB_BUILD_LIB) | $(BUILD_DIR)
	@echo "=== Copying LMDB to build/ ==="
	@cp $(LMDB_BUILD_LIB) $(LOCAL_LMDB_LIB)


# =========================
# Source files
# =========================
GERROR_SRC = $(SRC_DIR)/gerror.c
WTREE_SRC = $(SRC_DIR)/wtree.c

# Only YOUR sources go here
LIB_SOURCES = $(GERROR_SRC) $(WTREE_SRC)
LIB_OBJECTS = $(patsubst $(SRC_DIR)/%.c,$(BUILD_DIR)/%.o,$(LIB_SOURCES))

# =========================
# Test sources
# =========================
TEST_SOURCES = $(wildcard $(TEST_DIR)/test_*.c)
TEST_BINARIES = $(patsubst $(TEST_DIR)/%.c,$(BIN_DIR)/%,$(TEST_SOURCES))

# =========================
# Library targets
# =========================
STATIC_LIB = $(BUILD_DIR)/libwtree.a

# =========================
# Default target
# =========================
all: lib

# =========================
# Create directories
# =========================
$(BUILD_DIR) $(BIN_DIR):
	@mkdir -p $@

# =========================
# Compile YOUR objects
# =========================
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c | $(BUILD_DIR)
	@echo "  CC  $<"
	@$(CC) $(CFLAGS) -c $< -o $@

# =========================
# Build LMDB automatically
# =========================
$(LMDB_BUILD_LIB):
	@echo "=== Building LMDB ==="
	@$(MAKE) -C $(LMDB_DIR)

$(LOCAL_LMDB_LIB): $(LMDB_BUILD_LIB) | $(BUILD_DIR)
	@echo "=== Copying LMDB to build/ ==="
	@cp $(LMDB_BUILD_LIB) $(LOCAL_LMDB_LIB)

# =========================
# Build static library
# =========================
lib: $(LOCAL_LMDB_LIB) $(STATIC_LIB)

$(STATIC_LIB): $(LIB_OBJECTS)
	@echo "  AR  $@"
	@$(AR) rcs $@ $^
	@echo "Library built: $@"

# =========================
# Build test executables
# =========================
$(BIN_DIR)/test_gerror: $(TEST_DIR)/test_gerror.c $(GERROR_SRC) $(LMDB_LIB) | $(BIN_DIR)
	@echo "  CC  $@"
	@$(CC) $(CFLAGS) $(DEBUG_FLAGS) -o $@ $< $(GERROR_SRC) $(LMDB_LIB) $(LDFLAGS)

$(BIN_DIR)/test_wtree: $(TEST_DIR)/test_wtree.c $(LIB_SOURCES) $(LMDB_LIB) | $(BIN_DIR)
	@echo "  CC  $@"
	@$(CC) $(CFLAGS) $(DEBUG_FLAGS) -o $@ $< $(LIB_SOURCES) $(LMDB_LIB) $(LDFLAGS)

# =========================
# Build all tests
# =========================
build-tests: $(TEST_BINARIES)

# =========================
# Run all tests
# =========================
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

# =========================
# Run specific tests
# =========================
test-gerror: $(BIN_DIR)/test_gerror
	@$(BIN_DIR)/test_gerror

test-wtree: $(BIN_DIR)/test_wtree
	@$(BIN_DIR)/test_wtree

# =========================
# Build example
# =========================
example: $(SRC_DIR)/example_modular.c $(STATIC_LIB) $(LMDB_LIB) | $(BIN_DIR)
	@echo "  CC  example"
	@$(CC) $(CFLAGS) -o $(BIN_DIR)/example $< $(STATIC_LIB) $(LMDB_LIB) $(LDFLAGS)
	@echo "Example built: $(BIN_DIR)/example"

# =========================
# Clean
# =========================
clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR) $(BIN_DIR)
	@rm -rf test_wtree_db
	@echo "Clean complete!"

# =========================
# Debug
# =========================
debug: CFLAGS += $(DEBUG_FLAGS)
debug: clean lib

.PHONY: all lib build-tests test test-gerror test-wtree example clean debug
