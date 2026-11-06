add_library(grpcxx_uring STATIC
  uring_ring.cc
  uring_socket.cc
  uring_acceptor.cc
  proactor_service.cc
)
add_library(grpcxx::uring ALIAS grpcxx_uring)
target_include_directories(grpcxx_uring PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../..")
target_link_libraries(grpcxx_uring PUBLIC grpcxx::core)
target_link_libraries(grpcxx_uring PRIVATE liburing::liburing uarch::uarch)
