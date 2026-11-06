find_package(OpenSSL REQUIRED)
if(NOT TARGET fmt::fmt)
  find_package(fmt CONFIG REQUIRED)
endif()

add_library(grpcxx_tls tls_context.cc tls_session.cc)
add_library(grpcxx::tls ALIAS grpcxx_tls)
target_include_directories(grpcxx_tls PUBLIC ${CMAKE_CURRENT_LIST_DIR}/../..)
target_link_libraries(grpcxx_tls PUBLIC grpcxx::core PRIVATE OpenSSL::SSL fmt::fmt)
