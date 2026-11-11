cmake_minimum_required(VERSION 4.0)
add_executable(bench_compression bench_compression.cc)
target_link_libraries(bench_compression PRIVATE grpcxx::compression benchmark::benchmark_main)
