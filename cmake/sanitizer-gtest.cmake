# vcpkg gtest is built with -fno-sanitize-recover, and ~TestInfo trips UBSan
# vptr after the suite finishes. Rebuild the harness from the installed
# amalgamation. Clang libc++ annotates std::string only in TUs built with
# -fsanitize=address, so those objects keep address sanitizer and pass
# -fno-sanitize=undefined. GCC objects pass -fno-sanitize=all.
# Test code stays instrumented.
if(GRPCXX_BUILD_TESTING AND "${CMAKE_CXX_FLAGS}" MATCHES "-fsanitize=")
  get_filename_component(_grpcxx_gtest_prefix "${GTest_DIR}/../.." ABSOLUTE)
  set(_grpcxx_gtest_all "${_grpcxx_gtest_prefix}/src/gtest-all.cc")
  set(_grpcxx_gtest_main_src "${_grpcxx_gtest_prefix}/src/gtest_main.cc")
  if(NOT EXISTS "${_grpcxx_gtest_all}" OR NOT EXISTS "${_grpcxx_gtest_main_src}")
    message(FATAL_ERROR "gtest amalgamation missing under ${_grpcxx_gtest_prefix}")
  endif()

  # libc++ poisons std::string gaps only in TUs built with -fsanitize=address.
  # gtest must use that mode or vector<string> moves report container-overflow.
  # UBSan vptr stays off: TestInfo teardown trips it under -fno-sanitize-recover.
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set(_grpcxx_gtest_sanitize -fno-sanitize=undefined)
  else()
    set(_grpcxx_gtest_sanitize -fno-sanitize=all)
  endif()

  add_library(grpcxx_unsanitized_gtest STATIC "${_grpcxx_gtest_all}")
  set_target_properties(grpcxx_unsanitized_gtest PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
  target_include_directories(grpcxx_unsanitized_gtest
    PUBLIC "${_grpcxx_gtest_prefix}/include"
    PRIVATE "${_grpcxx_gtest_prefix}")
  target_compile_options(grpcxx_unsanitized_gtest PRIVATE ${_grpcxx_gtest_sanitize})
  target_link_libraries(grpcxx_unsanitized_gtest PUBLIC Threads::Threads)

  add_library(grpcxx_unsanitized_gtest_main STATIC "${_grpcxx_gtest_main_src}")
  set_target_properties(grpcxx_unsanitized_gtest_main PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
  target_include_directories(grpcxx_unsanitized_gtest_main PRIVATE "${_grpcxx_gtest_prefix}/include")
  target_compile_options(grpcxx_unsanitized_gtest_main PRIVATE ${_grpcxx_gtest_sanitize})
  target_link_libraries(grpcxx_unsanitized_gtest_main PUBLIC grpcxx_unsanitized_gtest)

  set(_grpcxx_gtest_lib "${CMAKE_BINARY_DIR}/libgrpcxx_unsanitized_gtest.a")
  set(_grpcxx_gtest_main_lib "${CMAKE_BINARY_DIR}/libgrpcxx_unsanitized_gtest_main.a")
  set_target_properties(GTest::gtest PROPERTIES
    IMPORTED_LOCATION "${_grpcxx_gtest_lib}"
    IMPORTED_LOCATION_RELEASE "${_grpcxx_gtest_lib}"
    IMPORTED_LOCATION_DEBUG "${_grpcxx_gtest_lib}")
  set_target_properties(GTest::gtest_main PROPERTIES
    IMPORTED_LOCATION "${_grpcxx_gtest_main_lib}"
    IMPORTED_LOCATION_RELEASE "${_grpcxx_gtest_main_lib}"
    IMPORTED_LOCATION_DEBUG "${_grpcxx_gtest_main_lib}")
endif()

function(grpcxx_unsanitized_gtest_dependencies directory)
  if(NOT TARGET grpcxx_unsanitized_gtest)
    return()
  endif()
  get_property(targets DIRECTORY "${directory}" PROPERTY BUILDSYSTEM_TARGETS)
  foreach(target IN LISTS targets)
    get_target_property(libs "${target}" LINK_LIBRARIES)
    if(libs AND libs MATCHES "GTest::")
      add_dependencies("${target}" grpcxx_unsanitized_gtest grpcxx_unsanitized_gtest_main)
    endif()
  endforeach()
  get_property(children DIRECTORY "${directory}" PROPERTY SUBDIRECTORIES)
  foreach(child IN LISTS children)
    grpcxx_unsanitized_gtest_dependencies("${child}")
  endforeach()
endfunction()
