# ============================================================
# Mongolite Makefile - Convenience wrapper for CMake
# ============================================================
#
# Build Types:
#   make              - Release build (default)
#   make debug        - Debug build
#   make release      - Release build
#   make valgrind     - Debug build with Valgrind-friendly flags
#   make sanitize     - Debug build with ASan/UBSan
#
# Run:
#   make test         - Run all tests
#   make test-valgrind        - Run tests with Valgrind (summary)
#   make test-valgrind-verbose - Run tests with full Valgrind output
#   make benchmark    - Run all benchmarks
#   make bench-insert - Run insert benchmark only
#   make bench-find   - Run find benchmark only
#   make bench-update - Run update benchmark only
#   make bench-delete - Run delete benchmark only
#   make bench-bson   - Run bson_update benchmark only
#
# Other:
#   make clean        - Clean build artifacts
#   make rebuild      - Clean and rebuild
#   make help         - Show this help
# ============================================================

BUILD_DIR := build
BUILD_TYPE := Release

# Allow custom generator (e.g., Ninja)
CMAKE_GENERATOR ?=
CMAKE_GEN_FLAG :=
ifneq ($(CMAKE_GENERATOR),)
    CMAKE_GEN_FLAG := -G "$(CMAKE_GENERATOR)"
endif

# CMake options (can be overridden)
ENABLE_SANITIZERS ?= OFF
ENABLE_VALGRIND ?= OFF
ENABLE_LTO ?= OFF

.PHONY: all build debug release valgrind sanitize test benchmark clean rebuild help
.PHONY: bench-insert bench-find bench-update bench-delete bench-bson
.PHONY: test-valgrind test-valgrind-verbose

# ============================================================
# Default target
# ============================================================

all: build

# ============================================================
# Build targets
# ============================================================

build:
	@cmake -S . -B $(BUILD_DIR) \
		$(CMAKE_GEN_FLAG) \
		-DCMAKE_BUILD_TYPE=$(BUILD_TYPE) \
		-DENABLE_SANITIZERS=$(ENABLE_SANITIZERS) \
		-DENABLE_VALGRIND=$(ENABLE_VALGRIND) \
		-DENABLE_LTO=$(ENABLE_LTO)
	@cmake --build $(BUILD_DIR) -j$$(nproc)

debug:
	@$(MAKE) BUILD_TYPE=Debug build

release:
	@$(MAKE) BUILD_TYPE=Release build

valgrind:
	@$(MAKE) BUILD_TYPE=Debug ENABLE_VALGRIND=ON build

sanitize:
	@$(MAKE) BUILD_TYPE=Debug ENABLE_SANITIZERS=ON build

lto:
	@$(MAKE) BUILD_TYPE=Release ENABLE_LTO=ON build

# ============================================================
# Test targets
# ============================================================

test:
	@ctest --test-dir $(BUILD_DIR) --output-on-failure

# Run tests under Valgrind with summarized output
test-valgrind:
	@echo "Running tests with Valgrind (summary mode)..."
	@echo "=============================================="
	@failed=0; \
	for test in $(BUILD_DIR)/bin/test_*; do \
		name=$$(basename $$test); \
		if [ "$$name" = "test_debug_update" ]; then continue; fi; \
		result=$$(valgrind --leak-check=full --error-exitcode=1 \
			--quiet --log-fd=1 $$test 2>&1); \
		exit_code=$$?; \
		if [ $$exit_code -eq 0 ]; then \
			echo "✓ $$name"; \
		else \
			echo "✗ $$name"; \
			echo "$$result" | grep -E "(definitely|indirectly|possibly) lost:" || true; \
			echo "$$result" | grep -E "ERROR SUMMARY:" || true; \
			failed=$$((failed + 1)); \
		fi; \
	done; \
	echo "=============================================="; \
	if [ $$failed -eq 0 ]; then \
		echo "All tests passed Valgrind checks!"; \
	else \
		echo "$$failed test(s) failed Valgrind checks"; \
		exit 1; \
	fi

# Run tests with verbose Valgrind output (for debugging)
test-valgrind-verbose:
	@for test in $(BUILD_DIR)/bin/test_*; do \
		name=$$(basename $$test); \
		if [ "$$name" = "test_debug_update" ]; then continue; fi; \
		echo "=== $$name ==="; \
		valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes $$test; \
		echo ""; \
	done

# ============================================================
# Benchmark targets
# ============================================================

benchmark: bench-insert bench-find bench-update bench-delete

bench-insert:
	@$(BUILD_DIR)/bin/bench_insert

bench-find:
	@$(BUILD_DIR)/bin/bench_find

bench-update:
	@$(BUILD_DIR)/bin/bench_update

bench-delete:
	@$(BUILD_DIR)/bin/bench_delete

bench-bson:
	@$(BUILD_DIR)/bin/bench_bson_update

# ============================================================
# Utility targets
# ============================================================

clean:
	@cmake --build $(BUILD_DIR) --target clean 2>/dev/null || true

rebuild: clean build

# ============================================================
# Help
# ============================================================

help:
	@echo ""
	@echo "Mongolite Build System"
	@echo "======================"
	@echo ""
	@echo "Build Commands:"
	@echo "  make              Build with Release config (default)"
	@echo "  make debug        Build with Debug config (-O0 -g)"
	@echo "  make release      Build with Release config (-O3 -march=native)"
	@echo "  make valgrind     Build Debug with Valgrind-friendly flags (-O1 -g3)"
	@echo "  make sanitize     Build Debug with ASan/UBSan enabled"
	@echo "  make lto          Build Release with Link-Time Optimization"
	@echo ""
	@echo "Test Commands:"
	@echo "  make test                 Run all unit tests"
	@echo "  make test-valgrind        Run tests with Valgrind (summary)"
	@echo "  make test-valgrind-verbose Run tests with full Valgrind output"
	@echo ""
	@echo "Benchmark Commands:"
	@echo "  make benchmark    Run all benchmarks"
	@echo "  make bench-insert Run insert benchmark only"
	@echo "  make bench-find   Run find benchmark only"
	@echo "  make bench-update Run update benchmark only"
	@echo "  make bench-delete Run delete benchmark only"
	@echo "  make bench-bson   Run bson_update benchmark only"
	@echo ""
	@echo "Utility Commands:"
	@echo "  make clean        Clean build artifacts"
	@echo "  make rebuild      Clean and rebuild"
	@echo "  make help         Show this help"
	@echo ""
	@echo "Advanced Options:"
	@echo "  CMAKE_GENERATOR=Ninja make   Use Ninja instead of Make"
	@echo "  ENABLE_LTO=ON make release   Enable LTO for release build"
	@echo ""
