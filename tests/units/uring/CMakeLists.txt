add_executable(grpcxx-proactor-service-tests test_proactor_service.cc)
target_link_libraries(grpcxx-proactor-service-tests PRIVATE grpcxx::uring GTest::gtest_main)
gtest_discover_tests(grpcxx-proactor-service-tests TEST_LIST grpcxx_uring_tests)
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/labels.cmake"
    "set_tests_properties(\${grpcxx_uring_tests} PROPERTIES LABELS \"unit;library;uring;proactor\")\n")
set_property(DIRECTORY APPEND PROPERTY TEST_INCLUDE_FILES "${CMAKE_CURRENT_BINARY_DIR}/labels.cmake")

add_library(grpcxx-proactor-codegen OBJECT EXCLUDE_FROM_ALL
    ../../../src/grpcxx/uring/proactor_service.cc
    ../../../src/grpcxx/uring/uring_ring.cc
    ../../../src/grpcxx/framing/length_prefixed_frame.cc
    ../../../src/grpcxx/framing/aligned_message_buf.cc
    ../../../src/grpcxx/h2/nghttp2_server_session.cc
    ../../../src/grpcxx/h2/nghttp2_client_session.cc
    ../../../src/grpcxx/channel/load_balancer.cc
    ../../../src/grpcxx/server/grpc_server_service.cc
    ../../../src/grpcxx/client/client_channel_service.cc
    ../../../src/grpcxx/services/health_service.cc
    ../../../src/grpcxx/services/reflection_service.cc
    ../../../src/grpcxx/streaming/stream_multiplexer.cc)
target_link_libraries(grpcxx-proactor-codegen PRIVATE
    grpcxx::core
    grpcxx::uring
    grpcxx::framing
    grpcxx::h2
    grpcxx::channel
    grpcxx::server
    grpcxx::client
    grpcxx::services
    grpcxx::streaming)
target_compile_options(grpcxx-proactor-codegen PRIVATE -O3 -fno-lto)
