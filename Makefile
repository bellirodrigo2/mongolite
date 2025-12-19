BUILD_DIR := build
BUILD_TYPE := Release

CMAKE_GENERATOR ?=

CMAKE_GEN_FLAG :=
ifneq ($(CMAKE_GENERATOR),)
    CMAKE_GEN_FLAG := -G "$(CMAKE_GENERATOR)"
endif

.PHONY: all build test benchmark clean rebuild

all: build

build:
	cmake -S . -B $(BUILD_DIR) \
		$(CMAKE_GEN_FLAG) \
		-DCMAKE_BUILD_TYPE=$(BUILD_TYPE)
	cmake --build $(BUILD_DIR)

test:
	ctest --test-dir $(BUILD_DIR) --output-on-failure

benchmark:
	$(BUILD_DIR)/bin/bench_insert
	$(BUILD_DIR)/bin/bench_find
	$(BUILD_DIR)/bin/bench_update
	$(BUILD_DIR)/bin/bench_delete

clean:
	cmake --build $(BUILD_DIR) --target clean || true

rebuild: clean build
