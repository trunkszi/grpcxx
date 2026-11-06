find_package(ZLIB REQUIRED)
if(NOT TARGET fmt::fmt)
  find_package(fmt CONFIG REQUIRED)
endif()
find_path(GRPCXX_ZSTD_INCLUDE_DIR NAMES zstd.h REQUIRED)
find_path(GRPCXX_LZ4_INCLUDE_DIR NAMES lz4frame.h REQUIRED)
find_path(GRPCXX_SNAPPY_INCLUDE_DIR NAMES snappy-c.h REQUIRED)
find_library(GRPCXX_ZSTD_LIBRARY NAMES zstd REQUIRED)
find_library(GRPCXX_LZ4_LIBRARY NAMES lz4 REQUIRED)
find_library(GRPCXX_SNAPPY_LIBRARY NAMES snappy REQUIRED)

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
  PRIVATE "${GRPCXX_ZSTD_INCLUDE_DIR}" "${GRPCXX_LZ4_INCLUDE_DIR}" "${GRPCXX_SNAPPY_INCLUDE_DIR}"
)
target_link_libraries(grpcxx_compression
  PUBLIC grpcxx::core grpcxx::framing
  PRIVATE fmt::fmt ZLIB::ZLIB "${GRPCXX_ZSTD_LIBRARY}" "${GRPCXX_LZ4_LIBRARY}" "${GRPCXX_SNAPPY_LIBRARY}"
)
