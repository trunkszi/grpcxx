cmake_minimum_required(VERSION 4.0)
find_package(ZLIB REQUIRED)
if(NOT TARGET fmt::fmt)
  find_package(fmt CONFIG REQUIRED)
endif()

add_library(grpcxx_compression STATIC
  compression_registry.cc
  identity_compressor.cc
  zstd_compressor.cc
  lz4_compressor.cc
  gzip_compressor.cc
  deflate_compressor.cc
  snappy_compressor.cc
)
add_library(grpcxx::compression ALIAS grpcxx_compression)
target_include_directories(grpcxx_compression
  PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../.."
)
target_link_libraries(grpcxx_compression
  PUBLIC grpcxx::core grpcxx::framing
  PRIVATE fmt::fmt ZLIB::ZLIB grpcxx_dep::zstd grpcxx_dep::lz4 grpcxx_dep::snappy
)
