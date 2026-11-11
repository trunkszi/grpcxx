cmake_minimum_required(VERSION 4.0)
add_executable(grpcxx-unary-loopback-tests
    test_unary_loopback_e2e.cc
    test_unary_ownership.cc
    test_proactor_streaming.cc
    test_proactor_resilience.cc
    test_proactor_drain.cc
    test_phase2_nexora_integration.cc
    test_endpoint_refresh.cc
)
target_link_libraries(grpcxx-unary-loopback-tests PRIVATE grpcxx::client grpcxx::server grpcxx::sync grpcxx::reflection GTest::gtest_main)
gtest_discover_tests(grpcxx-unary-loopback-tests TEST_LIST grpcxx_unary_tests)
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/labels.cmake"
    "set_tests_properties(\${grpcxx_unary_tests} PROPERTIES LABELS \"e2e;library;unary;loopback\")\n")
set_property(DIRECTORY APPEND PROPERTY TEST_INCLUDE_FILES "${CMAKE_CURRENT_BINARY_DIR}/labels.cmake")
