cmake_minimum_required(VERSION 4.0)
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  project(grpcxx_h2_tests LANGUAGES CXX)
  enable_testing()
  set(CMAKE_CXX_STANDARD 26)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  get_filename_component(GRPCXX_SOURCE_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../.." ABSOLUTE)
  add_subdirectory("${GRPCXX_SOURCE_ROOT}/src/grpcxx/h2" "${CMAKE_CURRENT_BINARY_DIR}/h2")
endif()
find_package(GTest CONFIG REQUIRED)
add_executable(test_h2_in_memory_unary
  test_h2_in_memory_unary.cc
  test_h2_boundaries.cc
  test_h2_protocol_security.cc
  test_h2_flow_and_status.cc)
target_link_libraries(test_h2_in_memory_unary PRIVATE grpcxx::h2 GTest::gtest_main)
target_compile_options(test_h2_in_memory_unary PRIVATE -fno-exceptions -Wall -Wextra -Wswitch)
add_test(NAME grpcxx.h2.in_memory_unary COMMAND test_h2_in_memory_unary)
set_tests_properties(grpcxx.h2.in_memory_unary PROPERTIES LABELS "unit;library;h2;session")
target_include_directories(test_h2_in_memory_unary PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../../../src")
if(NOT TARGET grpcxx::core AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/../../../src/grpcxx/core/status.cc")
  target_sources(test_h2_in_memory_unary PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../src/grpcxx/core/status.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../src/grpcxx/core/timeout.cc")
endif()

if(TARGET grpcxx::client AND TARGET grpcxx::server AND TARGET grpcxx::channel AND TARGET grpcxx::streaming)
  add_library(grpcxx-transport-codegen OBJECT EXCLUDE_FROM_ALL
    ../../../src/grpcxx/h2/h2_client_session.cc
    ../../../src/grpcxx/h2/h2_server_session.cc
    ../../../src/grpcxx/client/client_channel_service.cc
    ../../../src/grpcxx/client/client_channel_io.cc
    ../../../src/grpcxx/server/grpc_server_service.cc
    ../../../src/grpcxx/server/grpc_server_io.cc
    ../../../src/grpcxx/channel/channel_coordinator_service.cc
    ../../../src/grpcxx/streaming/stream_multiplexer.cc
    ../../../src/grpcxx/streaming/stream_multiplexer_io.cc)
  target_link_libraries(grpcxx-transport-codegen PRIVATE
    grpcxx::core
    grpcxx::h2
    grpcxx::client
    grpcxx::server
    grpcxx::channel
    grpcxx::streaming)
  target_compile_options(grpcxx-transport-codegen PRIVATE -O3 -fno-lto -fno-rtti)
endif()
