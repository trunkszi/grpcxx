# TLS 1.3 and message compression

This example drives a client and server through memory BIOs on one thread. It verifies the server certificate and ALPN `h2`, then sends Zstd, LZ4, Gzip, Deflate, and Snappy compressed messages inside encrypted TLS records and validates the recovered payloads. No sockets or background threads are created.

The PEM files under `tests/units/tls/` are public test credentials for localhost, never deployment credentials. Replace them with your own certificate, private key, and trust configuration in an application. Context construction can load files; connection progress uses only in-memory bytes.

## Build and run

Use the root uv-managed CLI with a configured reflection-capable C++26
preset and the compression/OpenSSL dependencies:

```sh
./build.py --help
./build.py build -t tls_and_compression
./build.py test --label tls
./build.py test --label compression
./build.py bench -t bench_compression
./build/gcc-glibc/bin/tls_and_compression
```

The example consumes the canonical core and framing sources under `src/`.
See the [CLI guide](../../scripts/cli/README.md) for tool setup and preset
selection.

## Compression and TLS boundaries

`CompressionRegistry::ParseEncoding` validates an incoming `grpc-encoding` token. `AcceptEncoding` advertises supported codecs; `Negotiate` selects the first supported token in the peer's supplied preference order. Identity remains available when no common compressor is advertised.

Messages smaller than the configurable 256-byte threshold, and messages that would grow after compression, retain their original payload and a clear compressed flag. An incoming clear flag must be decoded as identity even when the RPC negotiated another algorithm. All compressed and decoded message payloads use `AlignedMessageBuf` and remain eight-byte aligned.

LZ4 uses the standard checksummed LZ4 frame format, rather than an unspecified raw block format. Gzip and Deflate use the Gzip and Zlib wrappers respectively. Decoders accept one complete message stream, reject trailing bytes, and enforce the receive limit before growing output beyond it. Snappy and sized Zstd/LZ4 frames preflight their advertised output lengths.

`TlsSession::PollOnce` performs one handshake or shutdown step. Feed received ciphertext, drain generated ciphertext, and retry operations that return zero progress on a later caller tick. After a pending plaintext write, retry the same plaintext until accepted; record writes may accept a prefix. Transport EOF must be reported through `MarkTransportEof`, allowing a missing TLS close-notify to be distinguished from graceful shutdown.
