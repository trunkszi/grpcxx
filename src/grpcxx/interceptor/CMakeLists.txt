cmake_minimum_required(VERSION 3.28)

get_filename_component(GRPCXX_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../.." ABSOLUTE)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  project(grpcxx_interceptor LANGUAGES CXX)
endif()

if(NOT TARGET grpcxx::core)
  set(GRPCXX_BUILD_TESTING OFF CACHE BOOL "" FORCE)
  set(GRPCXX_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
  add_subdirectory("${GRPCXX_ROOT}" "${CMAKE_CURRENT_BINARY_DIR}/grpcxx_root")
endif()

find_package(fmt CONFIG REQUIRED)

add_library(grpcxx_interceptor INTERFACE)
add_library(grpcxx::interceptor ALIAS grpcxx_interceptor)

target_include_directories(grpcxx_interceptor INTERFACE "${GRPCXX_ROOT}/src")
target_compile_options(grpcxx_interceptor INTERFACE -std=c++26 -fno-exceptions -Wall -Wextra -Wswitch)
target_link_libraries(grpcxx_interceptor INTERFACE grpcxx::core fmt::fmt)
