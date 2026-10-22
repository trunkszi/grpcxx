cmake_minimum_required(VERSION 3.28)

get_filename_component(GRPCXX_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../.." ABSOLUTE)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  project(grpcxx_dns_tests LANGUAGES CXX)
  enable_testing()
  set(CMAKE_CXX_EXTENSIONS OFF)
endif()

if(NOT TARGET grpcxx::dns)
  add_subdirectory("${GRPCXX_ROOT}/src/grpcxx/dns"
                   "${CMAKE_CURRENT_BINARY_DIR}/grpcxx_dns")
endif()

find_package(GTest CONFIG REQUIRED)

add_executable(test_dns_service test_dns_service.cc)
target_compile_options(test_dns_service PRIVATE -std=c++26 -fno-exceptions -Wall -Wextra -Wswitch)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_compile_options(test_dns_service PRIVATE -freflection)
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  target_compile_options(test_dns_service PRIVATE -freflection-latest)
endif()
target_link_libraries(test_dns_service PRIVATE grpcxx::dns GTest::gtest_main)

include(CTest)
add_test(NAME grpcxx.dns.service COMMAND test_dns_service)
set_tests_properties(grpcxx.dns.service PROPERTIES LABELS "unit;library;dns;service")
