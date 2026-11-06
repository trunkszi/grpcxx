cmake_minimum_required(VERSION 3.30)

project(grpcxx VERSION 0.8.0 LANGUAGES C CXX)

option(GRPCXX_BUILD_TESTING "Build grpcxx unit tests" ON)
option(GRPCXX_BUILD_BENCHMARKS "Build grpcxx microbenchmarks" ON)
option(GRPCXX_BUILD_EXAMPLES "Build grpcxx examples" ON)
option(GRPCXX_ENABLE_PROTOBUF "Enable the Protobuf codec" ON)
option(GRPCXX_ENABLE_FORY "Enable the Apache Fory codec" ON)
option(GRPCXX_ENABLE_CAPNP "Enable the CapnProto codec" ON)
if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.30)
	set(CMAKE_CXX_STANDARD 26)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)
foreach(runtime_directory IN LISTS CMAKE_CXX_IMPLICIT_LINK_DIRECTORIES)
  if(NOT runtime_directory MATCHES "/sysroot(/|$)")
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
find_package(liburing CONFIG REQUIRED)
if(GRPCXX_ENABLE_FORY)
  find_package(fory CONFIG QUIET)
  if(NOT TARGET fory::serialization)
    find_package(Fory CONFIG REQUIRED)
  endif()
  # Fory's public templates require RTTI, including in generated codec consumers.
  set_property(TARGET fory::serialization APPEND PROPERTY INTERFACE_COMPILE_OPTIONS -frtti)
endif()
find_package(fmt CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)

if (GRPCXX_BUILD_TESTING)
	find_package(GTest CONFIG REQUIRED)
	include(CTest)
	enable_testing()
	include(GoogleTest)
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
if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.30)
	target_compile_features(grpcxx_compile_options INTERFACE cxx_std_26)
else()
	target_compile_options(grpcxx_compile_options INTERFACE -std=c++26)
endif()
target_compile_options(grpcxx_compile_options
	INTERFACE
		${GRPCXX_REFLECTION_FLAG}
		-fno-exceptions
		-Wall
		-Wextra
		-Wswitch
)
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
add_subdirectory(src/grpcxx/uring)
add_subdirectory(src/grpcxx/client)
add_subdirectory(src/grpcxx/server)
add_subdirectory(src/grpcxx/channel)
add_subdirectory(src/grpcxx/dns)
add_subdirectory(src/grpcxx/interceptor)
add_subdirectory(src/grpcxx/services)
add_subdirectory(src/grpcxx/streaming)
add_subdirectory(src/grpcxx/codec)
add_subdirectory(src/grpcxx/tools)
add_subdirectory(src/grpcxx/compression)
add_subdirectory(src/grpcxx/tls)

if (GRPCXX_BUILD_TESTING)
	add_subdirectory(tests/units/reflection)
	add_subdirectory(tests/units/h2)
	add_subdirectory(tests/units/uring)
	add_subdirectory(tests/units/channel)
	add_subdirectory(tests/units/dns)
	add_subdirectory(tests/units/services)
	add_subdirectory(tests/e2e/unary)
	add_subdirectory(tests/units/compression)
	add_subdirectory(tests/units/tls)
endif ()

if (GRPCXX_BUILD_TESTING OR GRPCXX_BUILD_BENCHMARKS OR GRPCXX_BUILD_EXAMPLES)
	add_subdirectory(tests/units/codec)
endif ()

if (GRPCXX_BUILD_EXAMPLES)
	add_subdirectory(examples/cpp26_reflection_unary)
	add_subdirectory(examples/multi_codec_showcase)
	add_subdirectory(examples/tls_and_compression)
	add_subdirectory(examples/order_service)
endif ()

if (GRPCXX_BUILD_TESTING)
	add_subdirectory(tests/e2e/order_gateway)

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
