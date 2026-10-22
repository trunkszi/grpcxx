cmake_minimum_required(VERSION 3.27)
get_filename_component(GRPCXX_SOURCE_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../.." ABSOLUTE)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  project(grpcxx_codec_verification LANGUAGES CXX)
  include(CTest)
  enable_testing()
  set(GRPCXX_CORE_SOURCE_DIR "${GRPCXX_SOURCE_ROOT}/src" CACHE PATH "Core source include directory")
  set(GRPCXX_REFLECTION_SOURCE_DIR "${GRPCXX_SOURCE_ROOT}/src" CACHE PATH "Reflection source include directory")
  set(FORY_SOURCE_DIR "" CACHE PATH "Optional Apache Fory C++ checkout")
  set(GRPCXX_DEPENDENCY_INCLUDE_DIR "${GRPCXX_SOURCE_ROOT}/src" CACHE PATH "Canonical framework source headers")
  set(CMAKE_CXX_STANDARD 23)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  option(GRPCXX_ENABLE_PROTOBUF "Enable Protobuf codec" ON)
  option(GRPCXX_ENABLE_FORY "Enable Fory codec" ON)
  option(GRPCXX_ENABLE_CAPNP "Enable Capnp codec" ON)
  find_package(fmt CONFIG REQUIRED)
  if(GRPCXX_ENABLE_FORY AND FORY_SOURCE_DIR)
    set(FORY_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    include_directories(BEFORE SYSTEM "${FORY_SOURCE_DIR}")
    add_subdirectory("${FORY_SOURCE_DIR}" "${CMAKE_CURRENT_BINARY_DIR}/fory" SYSTEM)
  endif()
  add_compile_options(-std=c++26 -fno-exceptions -Wall -Wextra -Wswitch)
  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    add_compile_options(-freflection)
  elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    add_compile_options(-freflection-latest)
  endif()
  if(NOT TARGET grpcxx::core)
    add_library(grpcxx_codec_core STATIC
      "${GRPCXX_CORE_SOURCE_DIR}/grpcxx/framing/aligned_message_buf.cc"
      "${GRPCXX_CORE_SOURCE_DIR}/grpcxx/core/metadata.cc"
      "${GRPCXX_CORE_SOURCE_DIR}/grpcxx/core/status.cc")
    target_include_directories(grpcxx_codec_core PUBLIC
      "${GRPCXX_DEPENDENCY_INCLUDE_DIR}"
      "${GRPCXX_CORE_SOURCE_DIR}" "${GRPCXX_REFLECTION_SOURCE_DIR}")
    target_link_libraries(grpcxx_codec_core PUBLIC fmt::fmt)
    add_library(grpcxx::core ALIAS grpcxx_codec_core)
    add_library(grpcxx::framing ALIAS grpcxx_codec_core)
  endif()
  add_subdirectory("${GRPCXX_SOURCE_ROOT}/src/grpcxx/codec" "${CMAKE_CURRENT_BINARY_DIR}/codec")
  add_subdirectory("${GRPCXX_SOURCE_ROOT}/src/grpcxx/tools" "${CMAKE_CURRENT_BINARY_DIR}/tools")
  add_subdirectory("${GRPCXX_SOURCE_ROOT}/src/grpcxx/h2" "${CMAKE_CURRENT_BINARY_DIR}/h2")
  add_subdirectory("${GRPCXX_SOURCE_ROOT}/src/grpcxx/reflection" "${CMAKE_CURRENT_BINARY_DIR}/reflection")
  add_subdirectory("${GRPCXX_SOURCE_ROOT}/tests/units/reflection" "${CMAKE_CURRENT_BINARY_DIR}/reflection_tests")
  add_subdirectory("${GRPCXX_SOURCE_ROOT}/tests/units/h2" "${CMAKE_CURRENT_BINARY_DIR}/h2_tests")
endif()

if(GRPCXX_BUILD_TESTING OR CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  find_package(GTest CONFIG REQUIRED)
endif()
if(GRPCXX_BUILD_BENCHMARKS OR CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  find_package(benchmark CONFIG REQUIRED)
endif()
set(generated_dir "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${generated_dir}")
add_library(grpcxx_codec_fixtures INTERFACE)
target_include_directories(grpcxx_codec_fixtures INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}" "${generated_dir}")
target_link_libraries(grpcxx_codec_fixtures INTERFACE grpcxx::codec fmt::fmt)

if(GRPCXX_ENABLE_PROTOBUF)
  find_package(Protobuf REQUIRED)
  add_custom_command(OUTPUT "${generated_dir}/showcase.pb.cc" "${generated_dir}/showcase.pb.h"
    COMMAND protobuf::protoc --cpp_out=${generated_dir} -I "${CMAKE_CURRENT_SOURCE_DIR}/fixtures"
      "${CMAKE_CURRENT_SOURCE_DIR}/fixtures/showcase.proto"
    DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/fixtures/showcase.proto" VERBATIM)
  add_custom_command(OUTPUT "${generated_dir}/required.pb.cc" "${generated_dir}/required.pb.h"
    COMMAND protobuf::protoc --cpp_out=${generated_dir} -I "${CMAKE_CURRENT_SOURCE_DIR}/fixtures"
      "${CMAKE_CURRENT_SOURCE_DIR}/fixtures/required.proto"
    DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/fixtures/required.proto" VERBATIM)
  add_library(grpcxx_proto_fixture STATIC "${generated_dir}/showcase.pb.cc"
    "${generated_dir}/required.pb.cc")
  target_include_directories(grpcxx_proto_fixture PUBLIC "${generated_dir}")
  target_link_libraries(grpcxx_proto_fixture PUBLIC protobuf::libprotobuf)
  target_link_libraries(grpcxx_codec_fixtures INTERFACE grpcxx::codec_protobuf grpcxx_proto_fixture)
endif()
if(GRPCXX_ENABLE_FORY)
  target_link_libraries(grpcxx_codec_fixtures INTERFACE grpcxx::codec_fory)
endif()
if(GRPCXX_ENABLE_CAPNP)
  find_package(CapnProto CONFIG REQUIRED)
  find_program(GRPCXX_CAPNP_COMPILER capnp REQUIRED)
  get_target_property(capnp_includes CapnProto::capnp INTERFACE_INCLUDE_DIRECTORIES)
  list(GET capnp_includes 0 capnp_include)
  add_custom_command(OUTPUT "${generated_dir}/showcase.capnp.c++" "${generated_dir}/showcase.capnp.h"
    COMMAND "${CMAKE_COMMAND}" -E env --unset=PWD "${GRPCXX_CAPNP_COMPILER}" compile -oc++:${generated_dir}
      -I "${capnp_include}"
      --src-prefix=${CMAKE_CURRENT_SOURCE_DIR}/fixtures
      "${CMAKE_CURRENT_SOURCE_DIR}/fixtures/showcase.capnp"
    DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/fixtures/showcase.capnp" VERBATIM)
  add_library(grpcxx_capnp_fixture STATIC "${generated_dir}/showcase.capnp.c++")
  target_include_directories(grpcxx_capnp_fixture PUBLIC "${generated_dir}")
  target_link_libraries(grpcxx_capnp_fixture PUBLIC CapnProto::capnp-rpc)
  target_link_libraries(grpcxx_codec_fixtures INTERFACE grpcxx::codec_capnp grpcxx_capnp_fixture)
endif()

if(GRPCXX_BUILD_TESTING OR CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  add_executable(test_multi_codec_and_codegen test_multi_codec_and_codegen.cc)
  target_link_libraries(test_multi_codec_and_codegen PRIVATE grpcxx_codec_fixtures GTest::gtest_main)
  add_test(NAME unit.library.codec.multi_codec COMMAND test_multi_codec_and_codegen)
  set_tests_properties(unit.library.codec.multi_codec PROPERTIES LABELS "unit;library;codec;multi_codec")

  find_package(Python3 REQUIRED COMPONENTS Interpreter)
  add_test(NAME unit.library.codec.codegen_tool_resolution
    COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/test_codegen_tool_resolution.py"
      $<TARGET_FILE:grpcxx-codegen>)
  set_tests_properties(unit.library.codec.codegen_tool_resolution PROPERTIES
    LABELS "unit;library;codec;tools")
  add_test(NAME unit.library.codec.codegen_no_input COMMAND grpcxx-codegen)
  add_test(NAME unit.library.codec.codegen_missing_output COMMAND grpcxx-codegen --out)
  add_test(NAME unit.library.codec.codegen_missing_import COMMAND grpcxx-codegen -I)
  add_test(NAME unit.library.codec.codegen_missing_compiler COMMAND grpcxx-codegen --protoc)
  add_test(NAME unit.library.codec.codegen_unknown_option COMMAND grpcxx-codegen --unknown)
  add_test(NAME unit.library.codec.codegen_unsupported_extension
    COMMAND grpcxx-codegen --out "${CMAKE_CURRENT_BINARY_DIR}/rejected" unsupported.txt)
  set(codegen_protoc "${Protobuf_PROTOC_EXECUTABLE}")
  if(TARGET protobuf::protoc)
    set(codegen_protoc "$<TARGET_FILE:protobuf::protoc>")

endif()
add_test(NAME unit.library.codec.codegen_output_collision
  COMMAND grpcxx-codegen --out "${CMAKE_CURRENT_BINARY_DIR}/collision"
    --protoc "${codegen_protoc}"
    "${CMAKE_CURRENT_SOURCE_DIR}/fixtures/showcase.proto"
    "${CMAKE_CURRENT_SOURCE_DIR}/fixtures/showcase.proto")
set_tests_properties(
  unit.library.codec.codegen_no_input
  unit.library.codec.codegen_missing_output
  unit.library.codec.codegen_missing_import
  unit.library.codec.codegen_missing_compiler
  unit.library.codec.codegen_unknown_option
  unit.library.codec.codegen_unsupported_extension
  unit.library.codec.codegen_output_collision
  PROPERTIES WILL_FAIL TRUE LABELS "unit;library;codec;multi_codec")

endif()

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  add_subdirectory("${GRPCXX_SOURCE_ROOT}/benches/codec" "${CMAKE_CURRENT_BINARY_DIR}/benches")
  add_subdirectory("${GRPCXX_SOURCE_ROOT}/examples/multi_codec_showcase" "${CMAKE_CURRENT_BINARY_DIR}/showcase")
endif()

set(contract_sources)
set(contract_definitions)
if(GRPCXX_ENABLE_PROTOBUF)
  list(APPEND contract_sources proto)
  list(APPEND contract_definitions GRPCXX_TEST_PROTO_CONTRACT)
endif()
if(GRPCXX_ENABLE_FORY)
  list(APPEND contract_sources fdl)
  list(APPEND contract_definitions GRPCXX_TEST_FORY_CONTRACT)
endif()
if(GRPCXX_ENABLE_CAPNP)
  list(APPEND contract_sources capnp)
  list(APPEND contract_definitions GRPCXX_TEST_CAPNP_CONTRACT)
endif()
set(contract_outputs)
set(contract_imports)
if(GRPCXX_ENABLE_CAPNP)
  list(APPEND contract_imports -I "${capnp_include}")
endif()
foreach(extension IN LISTS contract_sources)
  set(contract_protoc)
  if(extension STREQUAL "proto")
    list(APPEND contract_protoc --protoc "$<TARGET_FILE:protobuf::protoc>")
  endif()
  file(MAKE_DIRECTORY "${generated_dir}/${extension}")
  add_custom_command(OUTPUT "${generated_dir}/${extension}/showcase.grpcxx.h"
    COMMAND grpcxx-codegen --out "${generated_dir}/${extension}"
      ${contract_protoc}
      -I "${CMAKE_CURRENT_SOURCE_DIR}/fixtures" ${contract_imports}
      "${CMAKE_CURRENT_SOURCE_DIR}/fixtures/showcase.${extension}"
    DEPENDS grpcxx-codegen protoc-gen-grpcxx
      "${CMAKE_CURRENT_SOURCE_DIR}/fixtures/showcase.${extension}"
    VERBATIM)
  list(APPEND contract_outputs "${generated_dir}/${extension}/showcase.grpcxx.h")
endforeach()
if(contract_sources AND (GRPCXX_BUILD_TESTING OR CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR))
  add_executable(test_codegen_reflection test_codegen_reflection.cc ${contract_outputs})
  target_include_directories(test_codegen_reflection PRIVATE "${GRPCXX_SOURCE_ROOT}/src")
  target_link_libraries(test_codegen_reflection PRIVATE grpcxx_codec_fixtures grpcxx::h2 GTest::gtest_main)
  target_compile_definitions(test_codegen_reflection PRIVATE ${contract_definitions})
  add_test(NAME unit.library.codec.multi_codec_codegen COMMAND test_codegen_reflection)
  set_tests_properties(unit.library.codec.multi_codec_codegen PROPERTIES LABELS "unit;library;codec;multi_codec")
endif()
