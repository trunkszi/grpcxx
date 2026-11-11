cmake_minimum_required(VERSION 4.0)

if(NOT TARGET grpcxx::framing)
  return()
endif()

string(REGEX REPLACE "(^| )-O[0-9sg]( |$)" " -O3 " CMAKE_CXX_FLAGS_DEBUG
                     "${CMAKE_CXX_FLAGS_DEBUG}")

if(NOT TARGET grpcxx_framing_probes)
  add_library(grpcxx_framing_probes OBJECT framing_probes.cc)
  set_target_properties(grpcxx_framing_probes PROPERTIES POSITION_INDEPENDENT_CODE ON)
  target_link_libraries(grpcxx_framing_probes PRIVATE grpcxx::framing)
  target_compile_options(grpcxx_framing_probes PRIVATE -O3 -fno-lto -fno-rtti -fomit-frame-pointer
                                                       -fno-asynchronous-unwind-tables
                                                       -fno-unwind-tables)
endif()
