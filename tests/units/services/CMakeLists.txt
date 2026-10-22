cmake_minimum_required(VERSION 3.28)

get_filename_component(GRPCXX_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../.." ABSOLUTE)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  project(grpcxx_services_tests LANGUAGES CXX)
  enable_testing()
endif()

if(NOT TARGET grpcxx::services)
  add_subdirectory("${GRPCXX_ROOT}/src/grpcxx/services"
                   "${CMAKE_CURRENT_BINARY_DIR}/grpcxx_services")
endif()

find_package(GTest CONFIG REQUIRED)

add_executable(test_health_and_reflection test_health_and_reflection.cc)
target_link_libraries(test_health_and_reflection PRIVATE grpcxx::services GTest::gtest_main)
target_compile_options(test_health_and_reflection PRIVATE -std=c++26 -fno-exceptions -Wall -Wextra -Wswitch)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_compile_options(test_health_and_reflection PRIVATE -freflection)
endif()

include(CTest)
add_test(NAME test_health_and_reflection COMMAND test_health_and_reflection)
set_tests_properties(test_health_and_reflection PROPERTIES LABELS "unit;library;services;builtin")
