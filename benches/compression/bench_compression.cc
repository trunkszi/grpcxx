#include <benchmark/benchmark.h>

#include <cstddef>
#include <span>
#include <string>

#include "grpcxx/compression/deflate_compressor.h"
#include "grpcxx/compression/gzip_compressor.h"
#include "grpcxx/compression/lz4_compressor.h"
#include "grpcxx/compression/snappy_compressor.h"
#include "grpcxx/compression/zstd_compressor.h"

namespace grpcxx {
namespace {

template <typename Compressor>
void Compress(benchmark::State& state) {
  Compressor compressor;
  const std::string input(static_cast<std::size_t>(state.range(0)), 'x');
  const auto payload = std::as_bytes(std::span(input));
  for (auto ignored : state) {
    static_cast<void>(ignored);
    auto result = compressor.Compress(payload);
    if (!result) {
      state.SkipWithError("compression failed");
      break;
    }
    benchmark::DoNotOptimize(result->Payload().data());
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(state.iterations() * state.range(0));
}

template <typename Compressor>
void Decompress(benchmark::State& state) {
  Compressor compressor;
  const std::string input(static_cast<std::size_t>(state.range(0)), 'x');
  auto encoded = compressor.Compress(std::as_bytes(std::span(input)));
  if (!encoded) {
    state.SkipWithError("compression setup failed");
    return;
  }
  for (auto ignored : state) {
    static_cast<void>(ignored);
    auto result = compressor.Decompress(encoded->Payload(), input.size());
    if (!result) {
      state.SkipWithError("decompression failed");
      break;
    }
    benchmark::DoNotOptimize(result->Payload().data());
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(state.iterations() * state.range(0));
}

BENCHMARK_TEMPLATE(Compress, ZstdCompressor)->Arg(256)->Arg(4096)->Arg(65536);
BENCHMARK_TEMPLATE(Decompress, ZstdCompressor)->Arg(256)->Arg(4096)->Arg(65536);
BENCHMARK_TEMPLATE(Compress, Lz4Compressor)->Arg(256)->Arg(4096)->Arg(65536);
BENCHMARK_TEMPLATE(Decompress, Lz4Compressor)->Arg(256)->Arg(4096)->Arg(65536);
BENCHMARK_TEMPLATE(Compress, GzipCompressor)->Arg(256)->Arg(4096)->Arg(65536);
BENCHMARK_TEMPLATE(Decompress, GzipCompressor)->Arg(256)->Arg(4096)->Arg(65536);
BENCHMARK_TEMPLATE(Compress, DeflateCompressor)->Arg(256)->Arg(4096)->Arg(65536);
BENCHMARK_TEMPLATE(Decompress, DeflateCompressor)->Arg(256)->Arg(4096)->Arg(65536);
BENCHMARK_TEMPLATE(Compress, SnappyCompressor)->Arg(256)->Arg(4096)->Arg(65536);
BENCHMARK_TEMPLATE(Decompress, SnappyCompressor)->Arg(256)->Arg(4096)->Arg(65536);

}  // namespace
}  // namespace grpcxx
