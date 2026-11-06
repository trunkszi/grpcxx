add_library(grpcxx_server STATIC
  grpc_server_service.cc
)
add_library(grpcxx::server ALIAS grpcxx_server)
target_include_directories(grpcxx_server PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../..")
target_link_libraries(grpcxx_server PUBLIC grpcxx::core grpcxx::uring grpcxx::h2)
