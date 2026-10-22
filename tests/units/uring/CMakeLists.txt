add_executable(grpcxx-proactor-service-tests test_proactor_service.cc)
target_link_libraries(grpcxx-proactor-service-tests PRIVATE grpcxx::uring GTest::gtest_main)
gtest_discover_tests(grpcxx-proactor-service-tests TEST_LIST grpcxx_uring_tests)
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/labels.cmake"
    "set_tests_properties(\${grpcxx_uring_tests} PROPERTIES LABELS \"unit;library;uring;proactor\")\n")
set_property(DIRECTORY APPEND PROPERTY TEST_INCLUDE_FILES "${CMAKE_CURRENT_BINARY_DIR}/labels.cmake")
