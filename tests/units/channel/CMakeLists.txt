cmake_minimum_required(VERSION 3.28)

get_filename_component(GRPCXX_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../.." ABSOLUTE)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  project(grpcxx_channel_tests LANGUAGES CXX)
  enable_testing()
endif()

if(NOT TARGET grpcxx::channel)
  add_subdirectory("${GRPCXX_ROOT}/src/grpcxx/channel"
                   "${CMAKE_CURRENT_BINARY_DIR}/grpcxx_channel")
endif()

find_package(GTest CONFIG REQUIRED)

add_executable(test_channel_fsm_and_lb test_channel_fsm_and_lb.cc)
target_link_libraries(test_channel_fsm_and_lb PRIVATE grpcxx::channel GTest::gtest_main)
target_compile_options(test_channel_fsm_and_lb PRIVATE -std=c++26 -fno-exceptions -Wall -Wextra -Wswitch)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_compile_options(test_channel_fsm_and_lb PRIVATE -freflection)
endif()

add_executable(test_channel_coordinator_service test_channel_coordinator_service.cc)
target_link_libraries(test_channel_coordinator_service PRIVATE grpcxx::channel GTest::gtest_main)
target_compile_options(test_channel_coordinator_service PRIVATE -std=c++26 -fno-exceptions -Wall -Wextra -Wswitch)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_compile_options(test_channel_coordinator_service PRIVATE -freflection)
endif()

include(CTest)
add_test(NAME grpcxx.channel.fsm_and_lb COMMAND test_channel_fsm_and_lb)
set_tests_properties(grpcxx.channel.fsm_and_lb PROPERTIES LABELS "unit;library;channel;fsm")
add_test(NAME grpcxx.channel.coordinator COMMAND test_channel_coordinator_service)
set_tests_properties(grpcxx.channel.coordinator PROPERTIES LABELS "unit;library;channel;coordinator")
