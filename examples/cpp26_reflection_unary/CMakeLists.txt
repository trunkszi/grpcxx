add_executable(grpcxx-cpp26-reflection-unary main.cc)
target_link_libraries(grpcxx-cpp26-reflection-unary PRIVATE grpcxx::client grpcxx::server grpcxx::reflection fmt::fmt)
add_test(NAME cpp26-reflection-unary COMMAND grpcxx-cpp26-reflection-unary)
set_tests_properties(cpp26-reflection-unary PROPERTIES LABELS "e2e;example;reflection;unary")
