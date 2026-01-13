# Download and build cmocka using FetchContent
include(FetchContent)

FetchContent_Declare(
    cmocka
    GIT_REPOSITORY https://git.cryptomilk.org/projects/cmocka.git
    GIT_TAG        cmocka-1.1.7
)

# Configure cmocka options
set(WITH_STATIC_LIB ON CACHE BOOL "Build static library" FORCE)
set(WITH_EXAMPLES OFF CACHE BOOL "Build examples" FORCE)
set(UNIT_TESTING OFF CACHE BOOL "Build cmocka unit tests" FORCE)

FetchContent_MakeAvailable(cmocka)

# Determine the correct cmocka library target
if(TARGET cmocka-static)
    set(CMOCKA_LIBRARY cmocka-static)
elseif(TARGET cmocka::cmocka)
    set(CMOCKA_LIBRARY cmocka::cmocka)
else()
    # On Windows with MSVC, the library might just be named 'cmocka'
    set(CMOCKA_LIBRARY cmocka)
endif()

# ============================================================
# Macro to add wtree test (with wtree_ prefix to avoid conflicts)
# ============================================================

macro(add_wtree_test NAME)
    set(TARGET_NAME wtree_${NAME})
    add_executable(${TARGET_NAME} ${NAME}.c)
    target_include_directories(${TARGET_NAME} PRIVATE ${cmocka_SOURCE_DIR}/include)
    target_link_libraries(${TARGET_NAME} PRIVATE wtree3 ${CMOCKA_LIBRARY})
    add_test(NAME ${TARGET_NAME} COMMAND ${TARGET_NAME})
endmacro()

# Test for gerror module
add_wtree_test(test_gerror)

# Test for wtree3 module
add_wtree_test(test_wtree3)

# Test for wtree3 advanced edge cases
add_wtree_test(test_wtree3_advanced)

# Test for wtree3 upsert functionality
add_wtree_test(test_wtree3_upsert)

# Test for LMDB error translation
add_wtree_test(test_error_translation)

# Stress test for wtree3
add_wtree_test(test_wtree3_stress)

# Test for wtree3 Tier 1 operations
add_wtree_test(test_wtree3_tier1)

# Test for wtree3 Tier 2 operations
add_wtree_test(test_wtree3_tier2)

# Test for wtree3 coverage improvement (NULL checks and edge cases)
add_wtree_test(test_wtree3_coverage)

# Test for wtree3 critical bugs (Phase 1 bug fixes)
add_wtree_test(test_wtree3_bugs)

# Test for wvector module (generic dynamic array)
add_wtree_test(test_wvector)

# Test for wtree3 persistence (new simplified API)
add_wtree_test(test_wtree3_persistence_new)

# NOTE: test_wtree3_persistence.c is excluded - uses outdated API (wtree3_user_data_persistence_t, key_fn)

# Test for wtree3 custom dupsort comparison on non-unique indexes
add_wtree_test(test_wtree3_dupsort)

# Mark stress tests with "stress" label for separate execution
set_tests_properties(wtree_test_wtree3_stress PROPERTIES LABELS "stress")

# List of all wtree test targets for platform-specific settings
set(WTREE_TEST_TARGETS
    wtree_test_gerror
    wtree_test_wtree3
    wtree_test_wtree3_advanced
    wtree_test_wtree3_upsert
    wtree_test_error_translation
    wtree_test_wtree3_stress
    wtree_test_wtree3_tier1
    wtree_test_wtree3_tier2
    wtree_test_wtree3_coverage
    wtree_test_wtree3_bugs
    wtree_test_wvector
    wtree_test_wtree3_persistence_new
    wtree_test_wtree3_dupsort
)

# Platform-specific settings
if(WIN32)
    foreach(TARGET ${WTREE_TEST_TARGETS})
        target_compile_definitions(${TARGET} PRIVATE _CRT_SECURE_NO_WARNINGS)

        # Copy cmocka DLL to test directory on Windows
        add_custom_command(TARGET ${TARGET} POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy_if_different
                $<TARGET_FILE:cmocka>
                $<TARGET_FILE_DIR:${TARGET}>
            COMMENT "Copying cmocka DLL to test directory"
        )
    endforeach()

    # For MinGW builds, copy required runtime DLLs
    if(MINGW)
        # Find MinGW bin directory
        get_filename_component(MINGW_BIN_DIR ${CMAKE_C_COMPILER} DIRECTORY)

        # Copy required MinGW DLLs
        set(MINGW_DLLS
            "${MINGW_BIN_DIR}/libwinpthread-1.dll"
            "${MINGW_BIN_DIR}/libgcc_s_seh-1.dll"
        )

        foreach(DLL ${MINGW_DLLS})
            if(EXISTS ${DLL})
                foreach(TARGET ${WTREE_TEST_TARGETS})
                    add_custom_command(TARGET ${TARGET} POST_BUILD
                        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                            ${DLL}
                            $<TARGET_FILE_DIR:${TARGET}>
                        COMMENT "Copying ${DLL}"
                    )
                endforeach()
            endif()
        endforeach()
    endif()
endif()
