@0xe59cbe736248ad15;
using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("examples::record_batch::capnp");

struct RecordRequest {
  recordId @0 :Int64;
  sequence @1 :Int64;
  count @2 :Int64;
  tag @3 :Text;
}
struct RecordReply {
  recordId @0 :Int64;
  value @1 :Int64;
  count @2 :Int64;
  server @3 :Int64;
}

interface RecordStore {
  get @0 (request :RecordRequest) -> (response :RecordReply);
  put @1 (request :RecordRequest) -> (response :RecordReply);
}
