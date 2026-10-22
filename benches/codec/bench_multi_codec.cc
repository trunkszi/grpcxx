/** Round-trip timings include message creation, encode allocation, and decode. */
#include <benchmark/benchmark.h>

#include "codec_workload.h"

namespace {
/** Reuse a caller-owned buffer handle; exclude codec context registration. */
template <typename Engine, typename Workload>
auto Measure(benchmark::State& state, Engine& codec, Workload workload) -> void {
  grpcxx::AlignedMessageBuf buffer;
  std::int64_t bytes = 0;
  for (auto _ : state) {
    static_cast<void>(_);
    auto result = workload(codec, buffer);
    if (!result) {
      state.SkipWithError("codec round trip failed");
      return;
    }
    bytes += static_cast<std::int64_t>(*result);
    benchmark::DoNotOptimize(buffer.Payload().data());
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(bytes);
}

#if defined(GRPCXX_ENABLE_PROTOBUF)
auto Protobuf(benchmark::State& state) -> void {
  grpcxx::ProtobufCodec codec;
  Measure(state, codec, grpcxx::codec_fixture_support::ProtobufRoundTrip);
}
BENCHMARK(Protobuf);
#endif
#if defined(GRPCXX_ENABLE_FORY)
auto Fory(benchmark::State& state) -> void {
  grpcxx::ForyCodec codec;
  codec.Engine().register_struct<grpcxx::codec_fixture_support::Trade>(1);
  Measure(state, codec, grpcxx::codec_fixture_support::ForyRoundTrip);
}
BENCHMARK(Fory);
#endif
#if defined(GRPCXX_ENABLE_CAPNP)
auto CapnpRoundTripWithBorrowedRead(benchmark::State& state) -> void {
  grpcxx::CapnpCodec codec;
  Measure(state, codec, grpcxx::codec_fixture_support::CapnpRoundTrip);
}
BENCHMARK(CapnpRoundTripWithBorrowedRead);
#endif
}  // namespace

BENCHMARK_MAIN();
