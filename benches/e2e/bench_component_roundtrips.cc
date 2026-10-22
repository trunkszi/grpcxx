#include "grpcxx/codec/capnp_codec.h"
#include "grpcxx/codec/fory_codec.h"
#include "grpcxx/codec/protobuf_codec.h"
#include "grpcxx/compression/compression_registry.h"
#include "latency_samples.h"
#if defined(GRPCXX_ENABLE_PROTOBUF)
#include <google/protobuf/wrappers.pb.h>
#endif
#if defined(GRPCXX_ENABLE_CAPNP)
#include <capnp/schema.capnp.h>
#endif
#include <string>

namespace {
using e2e_bench::Samples;
void Compression(benchmark::State& state) {
  Samples samples{state};
  grpcxx::CompressionRegistry registry;
  const std::string input(1024, 'x');
  const auto bytes = std::as_bytes(std::span{input});
  const auto algorithm = static_cast<grpcxx::CompressionAlgorithm>(state.range(0));
  for ([[maybe_unused]] auto iteration : state) {
    samples.Begin();
    auto encoded = registry.Compress(algorithm, bytes);
    if (!encoded) {
      state.SkipWithError("registry compression failed");
      break;
    }
    auto decoded = registry.Decompress(
        encoded->Compressed() ? algorithm : grpcxx::CompressionAlgorithm::kIdentity,
        encoded->Payload(), input.size());
    if (!decoded || !std::ranges::equal(decoded->Payload(), bytes)) {
      state.SkipWithError("registry decompression mismatch");
      break;
    }
    benchmark::DoNotOptimize(decoded->Payload().data());
    samples.End();
  }
  samples.Report(state, input.size() * 2);
}
BENCHMARK(Compression)
    ->Name("compression_registry")
    ->DenseRange(0, 5)
    ->Unit(benchmark::kNanosecond);

#if defined(GRPCXX_ENABLE_PROTOBUF)
void Protobuf(benchmark::State& state) {
  Samples samples{state};
  google::protobuf::StringValue input;
  input.set_value("EURUSD");
  grpcxx::AlignedMessageBuf buffer;
  for ([[maybe_unused]] auto iteration : state) {
    samples.Begin();
    auto encoded = grpcxx::ProtobufCodec::Encode(input, buffer);
    google::protobuf::StringValue output;
    if (!encoded || !grpcxx::ProtobufCodec::Decode(buffer.Payload(), output) ||
        output.value() != input.value()) {
      state.SkipWithError("Protobuf roundtrip failed");
      break;
    }
    benchmark::DoNotOptimize(output);
    samples.End();
  }
  samples.Report(state, input.value().size() * 2);
}
BENCHMARK(Protobuf)->Name("protobuf_roundtrip")->Unit(benchmark::kNanosecond);
#endif
#if defined(GRPCXX_ENABLE_FORY)
void Fory(benchmark::State& state) {
  Samples samples{state};
  grpcxx::ForyCodec codec;
  const std::string input{"EURUSD"};
  grpcxx::AlignedMessageBuf buffer;
  for ([[maybe_unused]] auto iteration : state) {
    samples.Begin();
    auto encoded = codec.Encode(input, buffer);
    std::string output;
    if (!encoded || !codec.Decode(buffer.Payload(), output) || output != input) {
      state.SkipWithError("Fory roundtrip failed");
      break;
    }
    benchmark::DoNotOptimize(output);
    samples.End();
  }
  samples.Report(state, input.size() * 2);
}
BENCHMARK(Fory)->Name("fory_roundtrip")->Unit(benchmark::kNanosecond);
#endif
#if defined(GRPCXX_ENABLE_CAPNP)
void Capnp(benchmark::State& state) {
  Samples samples{state};
  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<capnp::schema::Node>();
  root.setId(73);
  root.setDisplayName("EURUSD");
  grpcxx::AlignedMessageBuf buffer;
  for ([[maybe_unused]] auto iteration : state) {
    samples.Begin();
    auto encoded = grpcxx::CapnpCodec::Encode(root.asReader(), buffer);
    if (!encoded) {
      state.SkipWithError("Capnp encode failed");
      break;
    }
    auto decoded = grpcxx::CapnpCodec::ReadAligned(buffer.Payload());
    if (!decoded || (*decoded)->getRoot<capnp::schema::Node>().getId() != 73) {
      state.SkipWithError("Capnp borrowed read failed");
      break;
    }
    benchmark::DoNotOptimize(decoded->get());
    samples.End();
  }
  samples.Report(state, 12);
}
BENCHMARK(Capnp)->Name("capnp_roundtrip")->Unit(benchmark::kNanosecond);
#endif
}  // namespace
