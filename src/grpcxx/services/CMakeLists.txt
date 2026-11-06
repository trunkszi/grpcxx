cmake_minimum_required(VERSION 3.28)

get_filename_component(GRPCXX_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../.." ABSOLUTE)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  project(grpcxx_services LANGUAGES CXX)
endif()

if(NOT TARGET grpcxx::core)
  set(GRPCXX_BUILD_TESTING OFF CACHE BOOL "" FORCE)
  set(GRPCXX_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
  add_subdirectory("${GRPCXX_ROOT}" "${CMAKE_CURRENT_BINARY_DIR}/grpcxx_root")
endif()

find_package(fmt CONFIG REQUIRED)

add_library(grpcxx_services STATIC
  health_service.cc
  reflection_service.cc
)
add_library(grpcxx::services ALIAS grpcxx_services)

target_include_directories(grpcxx_services PUBLIC "${GRPCXX_ROOT}/src")
target_compile_options(grpcxx_services PUBLIC -std=c++26 -fno-exceptions -Wall -Wextra -Wswitch)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_compile_options(grpcxx_services PUBLIC -freflection)
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  target_compile_options(grpcxx_services PUBLIC -freflection-latest)
endif()

if(NOT TARGET grpcxx::interceptor)
  add_subdirectory("${GRPCXX_ROOT}/src/grpcxx/interceptor"
                   "${CMAKE_CURRENT_BINARY_DIR}/grpcxx_interceptor")
endif()
target_link_libraries(grpcxx_services PUBLIC grpcxx::core grpcxx::interceptor fmt::fmt)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  enable_testing()
  add_subdirectory("${GRPCXX_ROOT}/tests/units/services"
                   "${CMAKE_CURRENT_BINARY_DIR}/services_tests")
endif()
