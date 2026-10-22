add_library(grpcxx_client STATIC
  client_channel_service.cc
)
add_library(grpcxx::client ALIAS grpcxx_client)
target_include_directories(grpcxx_client PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../..")
target_link_libraries(grpcxx_client PUBLIC grpcxx::core grpcxx::uring grpcxx::h2)
