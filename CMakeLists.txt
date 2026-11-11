cmake_minimum_required(VERSION 4.0)

project(grpcxx VERSION 0.8.0 LANGUAGES C CXX)

option(GRPCXX_BUILD_TESTING "Build grpcxx unit tests" ON)
option(GRPCXX_BUILD_BENCHMARKS "Build grpcxx microbenchmarks" ON)
option(GRPCXX_BUILD_EXAMPLES "Build grpcxx examples" ON)
option(GRPCXX_ENABLE_PROTOBUF "Enable the Protobuf codec" ON)
option(GRPCXX_ENABLE_FORY "Enable the Apache Fory codec" ON)
option(GRPCXX_ENABLE_CAPNP "Enable the CapnProto codec" ON)
set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
# Debug and diagnostic paths must survive moving an installed prefix away from its builder.
add_compile_options("-ffile-prefix-map=${PROJECT_SOURCE_DIR}=grpcxx"
                    "-ffile-prefix-map=${PROJECT_BINARY_DIR}=grpcxx-build")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  execute_process(COMMAND "${CMAKE_CXX_COMPILER}" -print-file-name=libstdc++.so
    OUTPUT_VARIABLE grpcxx_cxx_runtime OUTPUT_STRIP_TRAILING_WHITESPACE COMMAND_ERROR_IS_FATAL ANY)
  get_filename_component(GRPCXX_CXX_RUNTIME_DIR "${grpcxx_cxx_runtime}" DIRECTORY)
  list(PREPEND CMAKE_BUILD_RPATH "${GRPCXX_CXX_RUNTIME_DIR}")
endif()
execute_process(COMMAND "${CMAKE_CXX_COMPILER}" -dumpmachine
  OUTPUT_VARIABLE grpcxx_target_triple OUTPUT_STRIP_TRAILING_WHITESPACE RESULT_VARIABLE grpcxx_triple_status)
foreach(runtime_directory IN LISTS CMAKE_CXX_IMPLICIT_LINK_DIRECTORIES)
  if((grpcxx_triple_status EQUAL 0 AND grpcxx_target_triple MATCHES "musl")
      OR NOT runtime_directory MATCHES "/sysroot(/|$)")
    list(APPEND CMAKE_BUILD_RPATH "${runtime_directory}")
  endif()
endforeach()

# Apply the complete label set after GoogleTest exposes discovered test names.
function(grpcxx_label_discovered_tests target labels)
	file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/${target}_labels.cmake"
		"if(DEFINED ${target}_TESTS)\n"
		"  set_tests_properties(\${${target}_TESTS} PROPERTIES LABELS \"${labels}\")\n"
		"endif()\n")
	set_property(DIRECTORY APPEND PROPERTY TEST_INCLUDE_FILES
		"${CMAKE_CURRENT_BINARY_DIR}/${target}_labels.cmake")
endfunction()

find_package(uarch CONFIG REQUIRED)
find_package(uring CONFIG REQUIRED)
find_package(cares CONFIG REQUIRED)
find_package(nexora CONFIG REQUIRED)
include(cmake/GrpcxxCompressionDependencies.cmake)
if(GRPCXX_ENABLE_FORY)
  find_package(fory CONFIG QUIET)
  if(NOT TARGET fory::serialization)
    find_package(Fory CONFIG REQUIRED)
  endif()
  # Fory's public templates require RTTI, including in generated codec consumers.
  set_property(TARGET fory::serialization APPEND PROPERTY INTERFACE_COMPILE_OPTIONS -frtti)
endif()
if(GRPCXX_ENABLE_PROTOBUF)
  set(protobuf_MODULE_COMPATIBLE ON)
  find_package(Protobuf CONFIG QUIET)
  if(NOT Protobuf_FOUND)
    find_package(Protobuf REQUIRED)
  endif()
endif()
find_package(fmt CONFIG REQUIRED)

if (GRPCXX_BUILD_TESTING)
	find_package(GTest CONFIG REQUIRED)
	include(CTest)
	enable_testing()
	include(GoogleTest)
	include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/sanitizer-gtest.cmake")
endif ()

if (GRPCXX_BUILD_BENCHMARKS)
	find_package(benchmark CONFIG REQUIRED)
endif ()

function(grpcxx_capnp_sanitizer_stack)
  # Uninstrumented KJ requires callbacks on the physical stack, outside ASan's fake stack.
  set(directories "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/tests/units/codec")
  if(GRPCXX_BUILD_EXAMPLES)
    list(APPEND directories "${CMAKE_CURRENT_SOURCE_DIR}/examples/multi_codec_showcase")
  endif()
  foreach(directory IN LISTS directories)
    get_property(tests DIRECTORY "${directory}" PROPERTY TESTS)
    foreach(test IN LISTS tests)
      if(test MATCHES "^(multi_codec_showcase_smoke|unit.library.codec.multi_codec(_codegen)?|integration.example.codec.multi_codec_showcase)$")
        set_tests_properties("${test}" DIRECTORY "${directory}" PROPERTIES
          ENVIRONMENT_MODIFICATION "ASAN_OPTIONS=string_append::detect_stack_use_after_return=0")
      endif()
    endforeach()
  endforeach()
endfunction()

# Public targets carry the same language and runtime constraints as their consumers.
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
	set(GRPCXX_REFLECTION_FLAG -freflection)
elseif (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
	set(GRPCXX_REFLECTION_FLAG -freflection-latest)
else ()
	message(FATAL_ERROR
		"grpcxx requires GCC or Clang with P2996 reflection support")
endif ()

add_library(grpcxx_compile_options INTERFACE)
target_compile_features(grpcxx_compile_options INTERFACE cxx_std_26)
target_compile_options(grpcxx_compile_options
	INTERFACE
		${GRPCXX_REFLECTION_FLAG}
		-fno-exceptions
		-Wall
		-Wextra
		-Wswitch
)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  # Host sysroot flags must not select a different standard library than the C++26 compiler.
  target_link_options(grpcxx_compile_options INTERFACE "$<BUILD_INTERFACE:-L${GRPCXX_CXX_RUNTIME_DIR}>")
endif()
if(grpcxx_triple_status EQUAL 0 AND grpcxx_target_triple MATCHES "musl")
  # musl defaults secondary pthreads to 128 KiB unless PT_GNU_STACK p_memsz is specified.
  target_link_options(grpcxx_compile_options INTERFACE -Wl,-z,stack-size=8388608)
endif()
get_target_property(grpcxx_common_options grpcxx_compile_options INTERFACE_COMPILE_OPTIONS)
add_compile_options(${grpcxx_common_options})

add_library(grpcxx_core STATIC
	src/grpcxx/core/status.cc
	src/grpcxx/core/metadata.cc
	src/grpcxx/core/timeout.cc
)
add_library(grpcxx::core ALIAS grpcxx_core)

target_include_directories(grpcxx_core
	PUBLIC
		${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_link_libraries(grpcxx_core
	PUBLIC
		grpcxx_compile_options
		uarch::uarch
	PRIVATE
		fmt::fmt
)

add_library(grpcxx_framing STATIC
	src/grpcxx/framing/aligned_message_buf.cc
	src/grpcxx/framing/length_prefixed_frame.cc
)
add_library(grpcxx::framing ALIAS grpcxx_framing)

target_link_libraries(grpcxx_framing
	PUBLIC
		grpcxx::core
)

add_subdirectory(src/grpcxx/h2)
add_subdirectory(src/grpcxx/reflection)

add_subdirectory(src/grpcxx/client)
add_subdirectory(src/grpcxx/server)
add_subdirectory(src/grpcxx/channel)

add_subdirectory(src/grpcxx/interceptor)
add_subdirectory(src/grpcxx/services)
add_subdirectory(src/grpcxx/streaming)
add_subdirectory(src/grpcxx/codec)
add_subdirectory(src/grpcxx/tools)
add_subdirectory(src/grpcxx/compression)
add_subdirectory(src/grpcxx/sync)

if (GRPCXX_BUILD_TESTING)
	add_subdirectory(tests/units/reflection)
	add_subdirectory(tests/units/h2)

	add_subdirectory(tests/units/channel)

	add_subdirectory(tests/units/services)
	add_subdirectory(tests/e2e/unary)
	add_subdirectory(tests/units/compression)
	add_subdirectory(tests/units/sync)
endif ()

if (GRPCXX_BUILD_TESTING OR GRPCXX_BUILD_BENCHMARKS OR GRPCXX_BUILD_EXAMPLES)
	add_subdirectory(tests/units/codec)
endif ()

if (GRPCXX_BUILD_EXAMPLES)
	add_subdirectory(examples/cpp26_reflection_unary)
	if(GRPCXX_ENABLE_CAPNP)
		add_subdirectory(examples/multi_codec_showcase)
	endif()
	add_subdirectory(examples/tls_and_compression)
	add_subdirectory(examples/task_service)
endif ()

if (GRPCXX_BUILD_TESTING)
	add_subdirectory(tests/e2e/task_service)

	add_executable(test_core_primitives
		tests/units/core/test_core_primitives.cc
	)
	target_link_libraries(test_core_primitives
		PRIVATE
			grpcxx::core
			GTest::gtest_main
	)
	gtest_discover_tests(test_core_primitives
		PROPERTIES LABELS "unit;library;core;primitives"
	)
	grpcxx_label_discovered_tests(test_core_primitives
		"unit;library;core;primitives")

	add_executable(test_core_types
		tests/units/core/test_core_types.cc
	)
	target_link_libraries(test_core_types
		PRIVATE
			grpcxx::core
			GTest::gtest_main
	)
	gtest_discover_tests(test_core_types
		PROPERTIES LABELS "unit;library;core;types"
	)
	grpcxx_label_discovered_tests(test_core_types
		"unit;library;core;types")

	add_executable(test_aligned_framing
		tests/units/framing/test_aligned_framing.cc
	)
	target_link_libraries(test_aligned_framing
		PRIVATE
			grpcxx::framing
			GTest::gtest_main
	)
	gtest_discover_tests(test_aligned_framing
		PROPERTIES LABELS "unit;library;framing;aligned_frame"
	)
	grpcxx_label_discovered_tests(test_aligned_framing
		"unit;library;framing;aligned_frame")

	add_executable(test_header_mirror
		tests/units/framing/test_header_mirror.cc
	)
	target_link_libraries(test_header_mirror
		PRIVATE
			grpcxx::framing
			GTest::gtest_main
	)
	gtest_discover_tests(test_header_mirror
		PROPERTIES LABELS "unit;library;framing;header_mirror"
	)
	grpcxx_label_discovered_tests(test_header_mirror
		"unit;library;framing;header_mirror")

	add_executable(test_shutdown_deadline
		tests/units/client/test_shutdown_deadline.cc
	)
	target_link_libraries(test_shutdown_deadline
		PRIVATE
			grpcxx::client
			GTest::gtest_main
	)
	gtest_discover_tests(test_shutdown_deadline
		PROPERTIES LABELS "unit;library;client;shutdown"
	)
	grpcxx_label_discovered_tests(test_shutdown_deadline
		"unit;library;client;shutdown")
endif ()

if (GRPCXX_BUILD_BENCHMARKS)
	add_subdirectory(benches/reflection)
	add_subdirectory(benches/codec)
	add_subdirectory(benches/compression)
	add_subdirectory(benches/e2e)
	add_executable(bench_aligned_framing
		benches/framing/bench_aligned_framing.cc
	)
	target_link_libraries(bench_aligned_framing
		PRIVATE
			benchmark::benchmark
			grpcxx::framing
	)
	# build.py invokes the benchmark from build/<preset>/bin.
	set_target_properties(bench_aligned_framing
		PROPERTIES
			RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
	)
endif ()

if(GRPCXX_BUILD_TESTING AND GRPCXX_ENABLE_CAPNP AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU"
    AND CMAKE_CXX_FLAGS MATCHES "-fsanitize=address")
  cmake_language(DEFER CALL grpcxx_capnp_sanitizer_stack)
endif()

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
add_library(grpcxx INTERFACE)
add_library(grpcxx::grpcxx ALIAS grpcxx)
set(grpcxx_export_targets grpcxx grpcxx_compile_options grpcxx_core grpcxx_framing
  grpcxx_h2 grpcxx_reflection grpcxx_client grpcxx_server grpcxx_channel
  grpcxx_interceptor grpcxx_services grpcxx_streaming grpcxx_codec
  grpcxx_compression grpcxx_sync)
foreach(codec IN ITEMS protobuf fory capnp)
  if(TARGET grpcxx_codec_${codec})
    list(APPEND grpcxx_export_targets grpcxx_codec_${codec})
    target_link_libraries(grpcxx INTERFACE grpcxx_codec_${codec})
  endif()
endforeach()
target_link_libraries(grpcxx INTERFACE
  grpcxx_client grpcxx_server grpcxx_interceptor grpcxx_reflection grpcxx_h2
  grpcxx_services grpcxx_streaming grpcxx_compression grpcxx_channel grpcxx_sync grpcxx_codec)
foreach(target IN LISTS grpcxx_export_targets)
  if(NOT target STREQUAL "grpcxx" AND NOT target STREQUAL "grpcxx_compile_options")
    string(REGEX REPLACE "^grpcxx_" "" export_name "${target}")
    set_target_properties(${target} PROPERTIES EXPORT_NAME "${export_name}"
      INTERFACE_INCLUDE_DIRECTORIES "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>;$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>")
  endif()
endforeach()
install(TARGETS ${grpcxx_export_targets} EXPORT grpcxx-targets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(TARGETS protoc-gen-grpcxx EXPORT grpcxx-targets RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
if(TARGET grpcxx-codegen)
  install(TARGETS grpcxx-codegen EXPORT grpcxx-targets RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
  install(PROGRAMS src/grpcxx/tools/grpcxx_codegen/fdl_contract.py DESTINATION tools/grpcxx)
endif()
install(DIRECTORY src/grpcxx/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/grpcxx
  FILES_MATCHING PATTERN "*.h" PATTERN "*.hpp" PATTERN "*.inc" PATTERN "*.tpp"
  PATTERN "*_internal.h" EXCLUDE
  PATTERN "*_detail.h" EXCLUDE
  PATTERN "probes" EXCLUDE)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/GrpcxxCodegen.cmake"
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/grpcxx/tools)
configure_package_config_file(cmake/grpcxxConfig.cmake.in
  "${CMAKE_CURRENT_BINARY_DIR}/grpcxxConfig.cmake"
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/grpcxx)
write_basic_package_version_file("${CMAKE_CURRENT_BINARY_DIR}/grpcxxConfigVersion.cmake"
  VERSION ${PROJECT_VERSION} COMPATIBILITY SameMajorVersion)
install(FILES "${PROJECT_SOURCE_DIR}/cmake/GrpcxxCompressionDependencies.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/grpcxxConfig.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/grpcxxConfigVersion.cmake"
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/grpcxx)
install(EXPORT grpcxx-targets NAMESPACE grpcxx:: DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/grpcxx)
if(GRPCXX_BUILD_TESTING)
  grpcxx_unsanitized_gtest_dependencies("${CMAKE_CURRENT_SOURCE_DIR}")
endif()
