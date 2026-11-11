cmake_minimum_required(VERSION 4.0)
add_library(grpcxx_sync STATIC
  call_options.cc
  sync_wire_transport.cc
  rpc_stream.cc
  synchronous_channel.cc
  synchronous_channel_h2.cc
  synchronous_server.cc
  synchronous_server_h2.cc
)
add_library(grpcxx::sync ALIAS grpcxx_sync)

target_include_directories(grpcxx_sync
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(grpcxx_sync
  PUBLIC
    grpcxx_compile_options
    grpcxx::core
    grpcxx::framing
    grpcxx::compression
    grpcxx::reflection
    uarch::uarch
    nexora::nexora
  PRIVATE
    uring::uring
    cares::cares
)
