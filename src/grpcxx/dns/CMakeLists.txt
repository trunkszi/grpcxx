cmake_minimum_required(VERSION 3.28)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  project(grpcxx_dns LANGUAGES CXX)
endif()

get_filename_component(GRPCXX_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../.." ABSOLUTE)

if(NOT TARGET grpcxx::core)
  set(GRPCXX_BUILD_TESTING OFF CACHE BOOL "" FORCE)
  set(GRPCXX_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
  add_subdirectory("${GRPCXX_ROOT}" "${CMAKE_CURRENT_BINARY_DIR}/grpcxx_root")
endif()

find_package(fmt CONFIG REQUIRED)
find_path(GRPCXX_CARES_INCLUDE_DIR NAMES ares.h)
find_library(GRPCXX_CARES_LIBRARY NAMES cares)
if(NOT GRPCXX_CARES_INCLUDE_DIR OR NOT GRPCXX_CARES_LIBRARY)
  message(FATAL_ERROR "c-ares headers/libraries not found; install c-ares or set GRPCXX_CARES_INCLUDE_DIR/GRPCXX_CARES_LIBRARY")
endif()

add_library(grpcxx_dns STATIC ares_channel.cc dns_service.cc)
add_library(grpcxx::dns ALIAS grpcxx_dns)

set_target_properties(grpcxx_dns PROPERTIES CXX_EXTENSIONS OFF)
target_compile_options(grpcxx_dns PRIVATE -std=c++26 -fno-exceptions -Wall -Wextra -Wswitch)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_compile_options(grpcxx_dns PRIVATE -freflection)
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  target_compile_options(grpcxx_dns PRIVATE -freflection-latest)
endif()

target_include_directories(grpcxx_dns
  PUBLIC "${GRPCXX_ROOT}/src"
  PRIVATE "${GRPCXX_CARES_INCLUDE_DIR}"
)
target_link_libraries(grpcxx_dns
  PUBLIC grpcxx::core fmt::fmt
  PRIVATE "${GRPCXX_CARES_LIBRARY}"
)
