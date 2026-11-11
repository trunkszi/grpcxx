cmake_minimum_required(VERSION 4.0)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  project(grpcxx_streaming LANGUAGES CXX)
  set(CMAKE_CXX26_STANDARD_COMPILE_OPTION "-std=c++26")
  set(CMAKE_CXX26_EXTENSION_COMPILE_OPTION "-std=gnu++26")
  set(CMAKE_CXX_STANDARD 26)
  set(CMAKE_CXX_EXTENSIONS OFF)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  enable_testing()
endif()

get_filename_component(GRPCXX_STREAMING_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../.." ABSOLUTE)

add_library(grpcxx_streaming
  stream_multiplexer.cc
  stream_multiplexer_io.cc
  server_stream_responder.cc
  client_stream_writer.cc)
add_library(grpcxx::streaming ALIAS grpcxx_streaming)
target_include_directories(grpcxx_streaming PUBLIC "${GRPCXX_STREAMING_ROOT}/src")
target_compile_options(grpcxx_streaming PUBLIC -std=c++26 -fno-exceptions -Wall -Wextra -Wswitch)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_compile_options(grpcxx_streaming PUBLIC -freflection)
elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  target_compile_options(grpcxx_streaming PUBLIC -freflection-latest)
endif()

if(TARGET grpcxx::core)
  target_link_libraries(grpcxx_streaming PUBLIC grpcxx::core grpcxx::framing)
elseif(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  find_package(uarch CONFIG REQUIRED)
  set(GRPCXX_DEPENDENCY_INCLUDE_DIR "${GRPCXX_STREAMING_ROOT}/src"
      CACHE PATH "Canonical framework headers for standalone verification")
  target_include_directories(grpcxx_streaming PUBLIC "${GRPCXX_DEPENDENCY_INCLUDE_DIR}")
  target_link_libraries(grpcxx_streaming PUBLIC uarch::uarch)
  target_sources(grpcxx_streaming PRIVATE
    "${GRPCXX_DEPENDENCY_INCLUDE_DIR}/grpcxx/core/status.cc"
    "${GRPCXX_DEPENDENCY_INCLUDE_DIR}/grpcxx/core/metadata.cc")
else()
  message(FATAL_ERROR "grpcxx::streaming requires grpcxx::core")
endif()

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR OR GRPCXX_BUILD_TESTING)
  find_package(GTest CONFIG REQUIRED)
  find_package(fmt CONFIG REQUIRED)

  add_executable(streaming_fsm
    "${GRPCXX_STREAMING_ROOT}/tests/units/streaming/test_stream_fsm.cc")
  target_link_libraries(streaming_fsm PRIVATE grpcxx::streaming GTest::gtest_main)
  add_test(NAME streaming_fsm COMMAND streaming_fsm)
  set_tests_properties(streaming_fsm PROPERTIES LABELS "unit;library;streaming;fsm")

  add_executable(streaming_backpressure
    "${GRPCXX_STREAMING_ROOT}/tests/units/streaming/test_stream_backpressure.cc")
  target_link_libraries(streaming_backpressure PRIVATE grpcxx::streaming GTest::gtest_main)
  add_test(NAME streaming_backpressure COMMAND streaming_backpressure)
  set_tests_properties(streaming_backpressure PROPERTIES LABELS "unit;library;streaming;backpressure")

  add_executable(streaming_four_shapes
    "${GRPCXX_STREAMING_ROOT}/tests/e2e/streaming/test_four_streaming_shapes_e2e.cc")
  target_link_libraries(streaming_four_shapes PRIVATE grpcxx::streaming fmt::fmt)
  target_compile_definitions(streaming_four_shapes PRIVATE FMT_EXCEPTIONS=0)
  add_test(NAME streaming_four_shapes COMMAND streaming_four_shapes)
  set_tests_properties(streaming_four_shapes PROPERTIES LABELS "e2e;library;streaming;four_shapes")
endif()

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR OR GRPCXX_BUILD_EXAMPLES)
  find_package(fmt CONFIG REQUIRED)
  add_executable(streaming_showcase
    "${GRPCXX_STREAMING_ROOT}/examples/streaming_showcase/main.cc")
  target_link_libraries(streaming_showcase PRIVATE grpcxx::streaming fmt::fmt)
  target_compile_definitions(streaming_showcase PRIVATE FMT_EXCEPTIONS=0)
endif()
