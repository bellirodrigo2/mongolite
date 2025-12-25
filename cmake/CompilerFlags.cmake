# ============================================================
# CompilerFlags.cmake - Shared compiler flag definitions
# ============================================================
#
# Options:
#   ENABLE_SANITIZERS - Enable ASan/UBSan for Debug builds
#   ENABLE_VALGRIND   - Valgrind-friendly build (disables sanitizers)
#   ENABLE_LTO        - Enable Link-Time Optimization for Release
#   ENABLE_COVERAGE   - Enable code coverage instrumentation
#
# Build Types:
#   Debug         - Full debug symbols, no optimization
#   Release       - Full optimization, no debug
#   RelWithDebInfo - Optimization with debug symbols
# ============================================================

# ============================================================
# Build Options
# ============================================================

option(ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer (Debug only)" OFF)
option(ENABLE_VALGRIND "Valgrind-friendly build (disables sanitizers, uses -O1)" OFF)
option(ENABLE_LTO "Enable Link-Time Optimization (Release only)" OFF)
option(ENABLE_COVERAGE "Enable code coverage instrumentation (gcov/lcov)" OFF)

# Valgrind and sanitizers are mutually exclusive
if(ENABLE_VALGRIND AND ENABLE_SANITIZERS)
    message(WARNING "ENABLE_VALGRIND and ENABLE_SANITIZERS are mutually exclusive. Disabling sanitizers.")
    set(ENABLE_SANITIZERS OFF CACHE BOOL "" FORCE)
endif()

# Coverage and sanitizers are mutually exclusive (ASan interferes with gcov)
if(ENABLE_COVERAGE AND ENABLE_SANITIZERS)
    message(WARNING "ENABLE_COVERAGE and ENABLE_SANITIZERS are mutually exclusive. Disabling sanitizers.")
    set(ENABLE_SANITIZERS OFF CACHE BOOL "" FORCE)
endif()

# Coverage requires Debug or RelWithDebInfo build
if(ENABLE_COVERAGE AND CMAKE_BUILD_TYPE STREQUAL "Release")
    message(WARNING "Coverage requires Debug build. Switching to Debug.")
    set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "" FORCE)
endif()

# ============================================================
# Compiler Detection
# ============================================================

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    set(MONGOLITE_GCC_LIKE TRUE)
else()
    set(MONGOLITE_GCC_LIKE FALSE)
endif()

# ============================================================
# Base Flags (all build types)
# ============================================================

if(MSVC)
    add_compile_options(/W4)
else()
    add_compile_options(-Wall -Wextra -fPIC)
endif()

# ============================================================
# Debug Build Flags
# ============================================================

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    if(MSVC)
        add_compile_options(/Zi /Od)
    else()
        if(ENABLE_COVERAGE)
            # Coverage instrumentation: -O0 for accurate line coverage
            add_compile_options(-g -O0 --coverage -fprofile-arcs -ftest-coverage)
            add_link_options(--coverage)
            message(STATUS "Coverage enabled: gcov instrumentation active")
        elseif(ENABLE_VALGRIND)
            # Valgrind works better with -O1 (some optimizations help readability)
            add_compile_options(-g3 -O1 -fno-omit-frame-pointer)
            message(STATUS "Valgrind mode: using -O1 -g3 -fno-omit-frame-pointer")
        elseif(ENABLE_SANITIZERS)
            # Sanitizers need -O1 minimum for good stack traces
            add_compile_options(-g -O1 -fno-omit-frame-pointer)
            add_compile_options(-fsanitize=address,undefined)
            add_link_options(-fsanitize=address,undefined)
            message(STATUS "Sanitizers enabled: ASan + UBSan")
        else()
            # Pure debug: no optimization
            add_compile_options(-g -O0)
        endif()
    endif()
endif()

# ============================================================
# Release Build Flags
# ============================================================

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    # Define NDEBUG for release (required for Google Benchmark)
    add_compile_definitions(NDEBUG)

    if(MSVC)
        add_compile_options(/O2)
        if(ENABLE_LTO)
            add_compile_options(/GL)
            add_link_options(/LTCG)
        endif()
    else()
        add_compile_options(-O3 -march=native)
        if(ENABLE_LTO)
            add_compile_options(-flto)
            add_link_options(-flto)
            message(STATUS "LTO enabled for Release build")
        endif()
    endif()
endif()

# ============================================================
# RelWithDebInfo Build Flags
# ============================================================

if(CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    add_compile_definitions(NDEBUG)

    if(MSVC)
        add_compile_options(/O2 /Zi)
    else()
        add_compile_options(-O2 -g -march=native)
        # LTO optional for RelWithDebInfo
        if(ENABLE_LTO)
            add_compile_options(-flto)
            add_link_options(-flto)
        endif()
    endif()
endif()

# ============================================================
# Platform-specific
# ============================================================

if(WIN32)
    set(SYSTEM_NETWORK_LIBS ws2_32)
else()
    set(SYSTEM_NETWORK_LIBS)
endif()

if(MINGW)
    add_compile_definitions(__USE_MINGW_ANSI_STDIO=1)
endif()

# ============================================================
# Print configuration summary
# ============================================================

message(STATUS "")
message(STATUS "=== Mongolite Build Configuration ===")
message(STATUS "Build type:       ${CMAKE_BUILD_TYPE}")
message(STATUS "Sanitizers:       ${ENABLE_SANITIZERS}")
message(STATUS "Valgrind mode:    ${ENABLE_VALGRIND}")
message(STATUS "Coverage:         ${ENABLE_COVERAGE}")
message(STATUS "LTO:              ${ENABLE_LTO}")
message(STATUS "C Compiler:       ${CMAKE_C_COMPILER_ID} ${CMAKE_C_COMPILER_VERSION}")
message(STATUS "=====================================")
message(STATUS "")
