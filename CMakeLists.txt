cmake_minimum_required(VERSION 3.16)
project(pi_market_analytics LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
add_compile_options(-Wall -Wextra -Wpedantic)

option(PMA_NATIVE "Tune for the build machine's CPU (-mcpu=native / -march=native)" ON)
if(PMA_NATIVE)
  include(CheckCXXCompilerFlag)
  check_cxx_compiler_flag(-mcpu=native HAS_MCPU_NATIVE)    # ARM (the Pi)
  check_cxx_compiler_flag(-march=native HAS_MARCH_NATIVE)  # x86 (your laptop)
  if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm" AND HAS_MCPU_NATIVE)
    add_compile_options(-mcpu=native)
  elseif(HAS_MARCH_NATIVE)
    add_compile_options(-march=native)
  endif()
endif()

find_package(Threads REQUIRED)

# Header-only core
add_library(pma_core INTERFACE)
target_include_directories(pma_core INTERFACE cpp/include)

# Tests (no external framework yet; swap in GoogleTest later if you like)
enable_testing()
add_executable(pma_tests cpp/tests/test_core.cpp)
target_link_libraries(pma_tests PRIVATE pma_core Threads::Threads)
add_test(NAME pma_tests COMMAND pma_tests)

# Latency / throughput benchmark
add_executable(pma_bench cpp/bench/bench.cpp)
target_link_libraries(pma_bench PRIVATE pma_core Threads::Threads)

# Python module (skipped if pybind11 is not installed)
find_package(pybind11 CONFIG QUIET)
if(pybind11_FOUND)
  pybind11_add_module(pma_core_py cpp/src/bindings.cpp)
  target_link_libraries(pma_core_py PRIVATE pma_core)
  set_target_properties(pma_core_py PROPERTIES
    OUTPUT_NAME pma_core
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/python)
else()
  message(WARNING "pybind11 not found: Python module will not be built. "
                  "Install with `pip install pybind11` and pass "
                  "-Dpybind11_DIR=$(python3 -m pybind11 --cmakedir)")
endif()
