# Mongolite Makefile (Windows/MinGW wrapper for CMake)

BUILD_DIR = build
BUILD_TYPE = Release
GENERATOR = "MinGW Makefiles"

.PHONY: all build test benchmark bench-insert bench-find bench-update bench-delete clean rebuild

all: build

# Configure and build
build:
	@if not exist $(BUILD_DIR) mkdir $(BUILD_DIR)
	cd $(BUILD_DIR) && cmake .. -G $(GENERATOR) -DCMAKE_BUILD_TYPE=$(BUILD_TYPE)
	cd $(BUILD_DIR) && cmake --build .

# Run tests
test:
	cd $(BUILD_DIR) && ctest --output-on-failure

# Run all benchmarks
benchmark:
	$(BUILD_DIR)\bin\bench_insert.exe
	$(BUILD_DIR)\bin\bench_find.exe
	$(BUILD_DIR)\bin\bench_update.exe
	$(BUILD_DIR)\bin\bench_delete.exe

# Run individual benchmarks
bench-insert:
	$(BUILD_DIR)\bin\bench_insert.exe

bench-find:
	$(BUILD_DIR)\bin\bench_find.exe

bench-update:
	$(BUILD_DIR)\bin\bench_update.exe

bench-delete:
	$(BUILD_DIR)\bin\bench_delete.exe

# Clean build directory
clean:
	@if exist $(BUILD_DIR) rmdir /s /q $(BUILD_DIR)

# Full rebuild
rebuild: clean build
