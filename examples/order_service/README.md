# Caller-driven order gateway loopback

`TradingLoop` and `order_gateway::LoopbackTransport` satisfy `grpcxx::ServiceConcept`. One caller
advances a monotonic nanosecond timestamp and polls bounded work. The example does
not create threads or require DNS, sockets, or io_uring permissions.

The annotated member-function contract is consumed by the canonical `ServiceRouter`
and `ClientStub`. Real `Nghttp2ClientSession` and `Nghttp2ServerSession` exchange HTTP/2
bytes for unary calls. The router-issued `UnaryResponder` remains in its pending
order slot until a risk tick completes, its deadline expires, or cancellation wins.
Cancellation completes that same capability with `kCancelled`; it never emits an
execution report. Expiration includes `now == deadline`.

The local codec copies trivially-copyable example records and is intended only for
same-process loopback. It is not a portable serialization format. `ClientContext`
contains a formatted `grpc-timeout`; the loopback adapter validates/parses and
bridges it to `ServerContext` because the current HTTP/2 `Submit` API accepts only
path and payload. It preserves the original absolute deadline when timeout unit
rounding would extend it. This does not demonstrate arbitrary metadata over a socket.

Server streaming and bidirectional streaming use paired canonical `StreamMultiplexer`
instances and typed server/client adapters. The local transport forwards validated
stream events and refunds payload-window credits on subsequent polls. Queue pressure
retains accepted messages, rejects additional messages with `kResourceExhausted`, and
triggers the canonical write-ready callback after draining. Half-close and cancellation
persist in the multiplexer. Fixed capacities bound pending calls and observations;
start validates the receive limit before allocating stream storage.

`PushRawFrame` uses `LengthPrefixedFrame`, submits every successfully parsed order,
and rejects oversized lengths at the header. Parsing may allocate canonical frame
storage. Shutdown rejects new work, drains accepted requests and both halves of open
streams, then releases the transport. A subsequent `Start` constructs fresh sessions
and clears observations. `ShutdownDrainedResponder` records successful risk completion
during draining, not expiration.

## Build

Use the root uv-managed CLI and its configured C++26 reflection preset. The
`gcc-glibc` preset uses GCC 16.2 and local dependency prefixes. See the
[CLI guide](../../scripts/cli/README.md) for setup and other presets.

```sh
./build.py --help
./build.py build -t order_service
./build.py test --filter OrderGateway
./build/gcc-glibc/bin/order_service
./build.py bench -t bench_order_pipeline -- --benchmark_min_time=0.01s
```

The example, test, and benchmark directories also support root `add_subdirectory`.
Existing canonical targets are reused. The example registers its own CTest smoke and
adds smoke tests for the four other example targets when those targets exist at the
end of root configuration. Their source directories remain the integrating root's
responsibility. No real uring/DNS service classes are shadowed by the loopback adapter.

## Measurements

The benchmark executable covers actual HTTP/2 deferred roundtrips, aligned frame
allocation/parsing, canonical reflected routing, delegate invocation, all six registry
encodings (including identity), and enabled Protobuf/Fory/CapnProto roundtrips.
`ORDER_PIPELINE_BUILD_COMPONENT_BENCHMARKS=OFF` limits it to gateway/framing/routing/delegate cases;
individual codec options respect the root's `GRPCXX_ENABLE_*` settings.

Every completed iteration records a steady-clock nanosecond sample in a vector sized
before timing starts. `p50_ns`, `p99_ns`, and `p999_ns` are nearest-rank percentiles of
those per-message samples, including timer instrumentation. They are not percentiles
of repetition means. `items_per_second` and `bytes_per_second` come from Google
Benchmark's processed-item/byte counters. `cycles_per_msg` averages serialized RDTSC
deltas. These are local TSC ticks, not calibrated hardware core cycles. Scheduler noise
and CPU migration can affect results; no production latency guarantee is inferred.
