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
#   make coverage     - Debug build with coverage instrumentation
#
# Run:
#   make test         - Run unit tests (excluding stress tests)
#   make test-stress  - Run stress tests only
#   make test-all     - Run all tests (including stress)
#   make test-valgrind        - Run tests with Valgrind (summary)
#   make test-valgrind-verbose - Run tests with full Valgrind output
#   make test-sanitize - Run tests with ASan/UBSan checks
#   make benchmark    - Run all benchmarks
#   make bench-insert - Run insert benchmark only
#   make bench-find   - Run find benchmark only
#   make bench-update - Run update benchmark only
#   make bench-delete - Run delete benchmark only
#   make bench-bson   - Run bson_update benchmark only
#
# Coverage:
#   make coverage     - Build with coverage instrumentation
#   make coverage-run - Run tests and generate coverage report
#   make coverage-html - Generate HTML coverage report (lcov)
#   make coverage-clean - Clean coverage data files
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
ENABLE_COVERAGE ?= OFF

# Coverage output directories
COVERAGE_DIR := $(BUILD_DIR)/coverage
COVERAGE_INFO := $(COVERAGE_DIR)/coverage.info

.PHONY: all build debug release valgrind sanitize test test-stress test-all benchmark clean rebuild help
.PHONY: bench-insert bench-find bench-update bench-delete bench-bson
.PHONY: test-valgrind test-valgrind-verbose test-sanitize
.PHONY: coverage coverage-run coverage-html coverage-clean coverage-report

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
		-DENABLE_COVERAGE=$(ENABLE_COVERAGE) \
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
	@ctest --test-dir $(BUILD_DIR) --output-on-failure -LE stress

test-stress:
	@ctest --test-dir $(BUILD_DIR) --output-on-failure -L stress

test-all:
	@ctest --test-dir $(BUILD_DIR) --output-on-failure

test-sanitize:
	@echo "Running tests with ASan + UBSan checks..."
	export ASAN_OPTIONS="detect_leaks=1:abort_on_error=1:print_stacktrace=1" && \
	export UBSAN_OPTIONS="print_stacktrace=1:abort_on_error=1" && \
	cd build && ctest --output-on-failure

# Run tests under Valgrind with summarized output
test-valgrind:
	@echo "Running tests with Valgrind (summary mode)..."
	@echo "=============================================="
	@failed=0; \
	for test in $(BUILD_DIR)/bin/test_* $(BUILD_DIR)/tests/wtree/wtree_*; do \
		[ -x "$$test" ] || continue; \
		name=$$(basename $$test); \
		if [ "$$name" = "test_debug_update" ]; then continue; fi; \
		if echo "$$name" | grep -q "stress"; then continue; fi; \
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
	@for test in $(BUILD_DIR)/bin/test_* $(BUILD_DIR)/tests/wtree/wtree_*; do \
		[ -x "$$test" ] || continue; \
		name=$$(basename $$test); \
		if [ "$$name" = "test_debug_update" ]; then continue; fi; \
		if echo "$$name" | grep -q "stress"; then continue; fi; \
		echo "=== $$name ==="; \
		valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes $$test; \
		echo ""; \
	done

# ============================================================
# Coverage targets
# ============================================================

# Build with coverage instrumentation
coverage:
	@$(MAKE) BUILD_TYPE=Debug ENABLE_COVERAGE=ON build

# Clean coverage data (before running tests)
coverage-clean:
	@echo "Cleaning coverage data..."
	@find $(BUILD_DIR) -name "*.gcda" -delete 2>/dev/null || true
	@rm -rf $(COVERAGE_DIR)

# Run tests and collect coverage
coverage-run: coverage-clean
	@echo "Running tests with coverage instrumentation..."
	@ctest --test-dir $(BUILD_DIR) --output-on-failure
	@mkdir -p $(COVERAGE_DIR)

# Generate HTML report with lcov
coverage-html: coverage-run
	@echo "Generating coverage report..."
	@lcov --capture \
		--directory $(BUILD_DIR)/src \
		--directory $(BUILD_DIR)/tests \
		--output-file $(COVERAGE_INFO) \
		--ignore-errors mismatch \
		--rc lcov_branch_coverage=1
	@lcov --remove $(COVERAGE_INFO) \
		'/usr/*' \
		'*/external/*' \
		'*/_deps/*' \
		'*/tests/*' \
		--output-file $(COVERAGE_INFO) \
		--ignore-errors unused \
		--rc lcov_branch_coverage=1
	@genhtml $(COVERAGE_INFO) \
		--output-directory $(COVERAGE_DIR)/html \
		--title "Mongolite Coverage Report" \
		--legend \
		--branch-coverage \
		--function-coverage
	@echo ""
	@echo "=============================================="
	@echo "Coverage report generated: $(COVERAGE_DIR)/html/index.html"
	@echo "=============================================="

# Quick coverage summary (text-based, no HTML)
coverage-report: coverage-run
	@echo ""
	@echo "=============================================="
	@echo "Coverage Summary"
	@echo "=============================================="
	@lcov --capture \
		--directory $(BUILD_DIR)/src \
		--output-file $(COVERAGE_INFO) \
		--ignore-errors mismatch \
		--rc lcov_branch_coverage=1 \
		--quiet
	@lcov --remove $(COVERAGE_INFO) \
		'/usr/*' \
		'*/external/*' \
		'*/_deps/*' \
		'*/tests/*' \
		--output-file $(COVERAGE_INFO) \
		--ignore-errors unused \
		--rc lcov_branch_coverage=1 \
		--quiet
	@lcov --summary $(COVERAGE_INFO) --rc lcov_branch_coverage=1
	@echo ""

# Alternative: gcovr-based report (simpler, works if lcov not available)
coverage-gcovr: coverage-run
	@echo "Generating coverage report with gcovr..."
	@mkdir -p $(COVERAGE_DIR)
	@gcovr --root . \
		--filter 'src/' \
		--exclude 'external/' \
		--exclude 'tests/' \
		--exclude 'build/_deps/' \
		--html-details $(COVERAGE_DIR)/gcovr.html \
		--print-summary
	@echo ""
	@echo "Coverage report: $(COVERAGE_DIR)/gcovr.html"

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
	@echo "  make coverage     Build Debug with coverage instrumentation"
	@echo "  make lto          Build Release with Link-Time Optimization"
	@echo ""
	@echo "Test Commands:"
	@echo "  make test                 Run unit tests (excluding stress tests)"
	@echo "  make test-stress          Run stress tests only"
	@echo "  make test-all             Run all tests (including stress)"
	@echo "  make test-valgrind        Run tests with Valgrind (summary)"
	@echo "  make test-valgrind-verbose Run tests with full Valgrind output"
	@echo "  make test-sanitize        Run tests with ASan/UBSan checks"
	@echo ""
	@echo "Coverage Commands (requires: apt install lcov gcovr):"
	@echo "  make coverage       Build with coverage instrumentation"
	@echo "  make coverage-html  Run tests and generate HTML report (lcov)"
	@echo "  make coverage-report Run tests and show summary (text)"
	@echo "  make coverage-gcovr  Run tests and generate gcovr report"
	@echo "  make coverage-clean  Clean coverage data files"
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
	@echo "  CMAKE_GENERATOR="MinGW Makefiles" Use MinGW Makefiles on Windows"
	@echo "  ENABLE_LTO=ON make release   Enable LTO for release build"
	@echo ""
