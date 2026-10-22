add_library(grpcxx_uring STATIC
  uring_ring.cc
  uring_socket.cc
  uring_acceptor.cc
  proactor_service.cc
)
add_library(grpcxx::uring ALIAS grpcxx_uring)
target_include_directories(grpcxx_uring PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../..")
target_link_libraries(grpcxx_uring PUBLIC grpcxx::core)
find_path(GRPCXX_URING_INCLUDE_DIR liburing.h REQUIRED)
find_library(GRPCXX_URING_LIBRARY uring REQUIRED)
target_include_directories(grpcxx_uring SYSTEM PRIVATE "${GRPCXX_URING_INCLUDE_DIR}")
target_link_libraries(grpcxx_uring PRIVATE "${GRPCXX_URING_LIBRARY}")
