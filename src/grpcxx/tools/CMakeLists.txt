cmake_minimum_required(VERSION 3.27)
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  project(grpcxx_codegen_tools LANGUAGES CXX)
endif()

find_package(Protobuf REQUIRED)
find_package(CapnProto CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_path(GRPCXX_PROTOC_INCLUDE_DIR google/protobuf/compiler/code_generator.h REQUIRED)

add_executable(protoc-gen-grpcxx
  protoc_gen_grpcxx/main.cc
  protoc_gen_grpcxx/contract_generator.cc)
target_link_libraries(protoc-gen-grpcxx PRIVATE protobuf::libprotoc protobuf::libprotobuf fmt::fmt)
target_include_directories(protoc-gen-grpcxx PRIVATE ${GRPCXX_PROTOC_INCLUDE_DIR})
target_compile_options(protoc-gen-grpcxx PRIVATE -std=c++26 -fno-exceptions -Wall -Wextra -Wswitch)

add_executable(grpcxx-codegen
  grpcxx_codegen/main.cc
  grpcxx_codegen/idl_compiler.cc)
target_link_libraries(grpcxx-codegen PRIVATE CapnProto::capnp CapnProto::kj fmt::fmt)
target_compile_options(grpcxx-codegen PRIVATE -std=c++26 -fno-exceptions -Wall -Wextra -Wswitch)
target_compile_definitions(grpcxx-codegen PRIVATE
  GRPCXX_FDL_ADAPTER="${CMAKE_CURRENT_SOURCE_DIR}/grpcxx_codegen/fdl_contract.py")

function(grpcxx_generate_contracts target)
  cmake_parse_arguments(GEN "" "OUTPUT_DIRECTORY;PROTOC" "FILES;IMPORT_DIRECTORIES" ${ARGN})
  if(NOT GEN_FILES OR NOT GEN_OUTPUT_DIRECTORY)
    message(FATAL_ERROR "grpcxx_generate_contracts requires FILES and OUTPUT_DIRECTORY")
  endif()
  get_filename_component(GEN_OUTPUT_DIRECTORY "${GEN_OUTPUT_DIRECTORY}" ABSOLUTE
    BASE_DIR "${CMAKE_CURRENT_BINARY_DIR}")
  set(arguments --out "${GEN_OUTPUT_DIRECTORY}")
  foreach(directory IN LISTS GEN_IMPORT_DIRECTORIES)
    list(APPEND arguments -I "${directory}")
  endforeach()
  if(GEN_PROTOC)
    list(APPEND arguments --protoc "${GEN_PROTOC}")
  endif()
  set(headers)
  foreach(schema IN LISTS GEN_FILES)
    get_filename_component(stem "${schema}" NAME_WLE)
    if("${GEN_OUTPUT_DIRECTORY}/${stem}.grpcxx.h" IN_LIST headers)
      message(FATAL_ERROR "Contract output name collision for ${schema}")
    endif()
    list(APPEND headers "${GEN_OUTPUT_DIRECTORY}/${stem}.grpcxx.h")
  endforeach()
  add_custom_command(OUTPUT ${headers}
    COMMAND $<TARGET_FILE:grpcxx-codegen> ${arguments} ${GEN_FILES}
    DEPENDS grpcxx-codegen protoc-gen-grpcxx ${GEN_FILES}
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    VERBATIM COMMAND_EXPAND_LISTS)
  add_custom_target(${target} DEPENDS ${headers})
endfunction()
