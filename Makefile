# Mongolite Makefile (portable, CMake-first)

BUILD_DIR := build
BUILD_TYPE := Release

.PHONY: all build test benchmark \
        bench-insert bench-find bench-update bench-delete \
        clean rebuild

all: build

# Configure and build (CMake creates the build dir)
build:
	cmake -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=$(BUILD_TYPE)
	cmake --build $(BUILD_DIR)

# Run tests
test:
	ctest --test-dir $(BUILD_DIR) --output-on-failure

# Run all benchmarks
benchmark:
	$(BUILD_DIR)/bin/bench_insert
	$(BUILD_DIR)/bin/bench_find
	$(BUILD_DIR)/bin/bench_update
	$(BUILD_DIR)/bin/bench_delete

# Run individual benchmarks
bench-insert:
	$(BUILD_DIR)/bin/bench_insert

bench-find:
	$(BUILD_DIR)/bin/bench_find

bench-update:
	$(BUILD_DIR)/bin/bench_update

bench-delete:
	$(BUILD_DIR)/bin/bench_delete

# Clean (portable)
clean:
	cmake --build $(BUILD_DIR) --target clean || true

# Full rebuild
rebuild: clean build
