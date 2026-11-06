@0xddb66f293a151dc1;
using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("capnp_fixture::nested");
annotation grpcxxClientStreaming(method) :Bool;
annotation grpcxxServerStreaming(method) :Bool;
struct Request { id @0 :Int64; symbol @1 :Text; }
struct Response { id @0 :Int64; }
interface Showcase {
  unary @0 (request :Request) -> (response :Response);
  serverStream @1 (request :Request) -> (response :Response)
      $grpcxxServerStreaming(true);
  clientStream @2 (request :Request) -> (response :Response)
      $grpcxxClientStreaming(true);
  bidi @3 (request :Request) -> (response :Response)
      $grpcxxClientStreaming(true) $grpcxxServerStreaming(true);
}

interface Renamed $Cxx.name("RenamedInterface") {
  original @0 (request :Request) -> (response :Response) $Cxx.name("renamedCall");
}
