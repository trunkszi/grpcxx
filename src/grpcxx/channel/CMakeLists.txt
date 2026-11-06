cmake_minimum_required(VERSION 3.28)

get_filename_component(GRPCXX_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../.." ABSOLUTE)

if(NOT TARGET grpcxx::core)
  set(GRPCXX_BUILD_TESTING OFF CACHE BOOL "" FORCE)
  set(GRPCXX_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
  add_subdirectory("${GRPCXX_ROOT}" "${CMAKE_CURRENT_BINARY_DIR}/grpcxx_root")
endif()

find_package(fmt CONFIG REQUIRED)

add_library(grpcxx_channel STATIC
  channel_connectivity_fsm.cc
  load_balancer.cc
  channel_coordinator_service.cc
  call_deadline.cc)
add_library(grpcxx::channel ALIAS grpcxx_channel)

target_include_directories(grpcxx_channel PUBLIC "${GRPCXX_ROOT}/src")
target_compile_options(grpcxx_channel PUBLIC -std=c++26 -fno-exceptions -Wall -Wextra -Wswitch)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_compile_options(grpcxx_channel PUBLIC -freflection)
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  target_compile_options(grpcxx_channel PUBLIC -freflection-latest)
endif()
target_link_libraries(grpcxx_channel PUBLIC grpcxx::core fmt::fmt)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  enable_testing()
  add_subdirectory("${GRPCXX_ROOT}/tests/units/channel"
                   "${CMAKE_CURRENT_BINARY_DIR}/channel_tests")
endif()
