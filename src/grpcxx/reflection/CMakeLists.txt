cmake_minimum_required(VERSION 4.0)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  project(grpcxx_reflection LANGUAGES CXX)
endif()
if(NOT TARGET fmt::fmt)
  find_package(fmt CONFIG REQUIRED)
endif()

if(NOT TARGET grpcxx_reflection)
  add_library(grpcxx_reflection INTERFACE)
  add_library(grpcxx::reflection ALIAS grpcxx_reflection)
  target_include_directories(grpcxx_reflection INTERFACE "${CMAKE_CURRENT_LIST_DIR}/../..")
  target_compile_options(grpcxx_reflection INTERFACE -std=c++26)
  target_link_libraries(grpcxx_reflection INTERFACE fmt::fmt)
  if(TARGET grpcxx::core)
    target_link_libraries(grpcxx_reflection INTERFACE grpcxx::core)
  endif()
  if(TARGET grpcxx::framing)
    target_link_libraries(grpcxx_reflection INTERFACE grpcxx::framing)
  endif()
  if(TARGET grpcxx::h2)
    target_link_libraries(grpcxx_reflection INTERFACE grpcxx::h2)
  endif()
endif()

if(TARGET grpcxx::framing)
  add_subdirectory("${CMAKE_CURRENT_LIST_DIR}/../framing"
                   "${CMAKE_CURRENT_BINARY_DIR}/framing_probes")
endif()

if(TARGET grpcxx::core AND NOT TARGET grpcxx_reflection_probes)
  string(REGEX REPLACE "(^| )-O[0-9sg]( |$)" " -O3 " CMAKE_CXX_FLAGS_DEBUG
                       "${CMAKE_CXX_FLAGS_DEBUG}")
  add_library(grpcxx_reflection_probes OBJECT reflection_probes.cc)
  set_target_properties(grpcxx_reflection_probes PROPERTIES POSITION_INDEPENDENT_CODE ON)
  target_link_libraries(grpcxx_reflection_probes PRIVATE grpcxx::core grpcxx::reflection)
  target_compile_options(grpcxx_reflection_probes PRIVATE -O3 -fno-lto -fno-rtti
                                                            -fomit-frame-pointer
                                                            -fno-asynchronous-unwind-tables
                                                            -fno-unwind-tables)
endif()

if(TARGET grpcxx_framing_probes AND TARGET grpcxx_reflection_probes AND NOT TARGET grpcxx_probes)
  add_library(grpcxx_probes STATIC
              $<TARGET_OBJECTS:grpcxx_framing_probes>
              $<TARGET_OBJECTS:grpcxx_reflection_probes>)
endif()
