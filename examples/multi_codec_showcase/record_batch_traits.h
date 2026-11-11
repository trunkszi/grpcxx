#pragma once

#include <grpcxx/codec/capnp_codec.h>
#include <grpcxx/codec/fory_codec.h>
#include <grpcxx/codec/protobuf_codec.h>

#include <cstdint>
#include <string_view>

#if defined(GRPCXX_ENABLE_PROTOBUF)
#include "proto/record_batch.grpcxx.h"
#endif
#if defined(GRPCXX_ENABLE_FORY)
#include "fory/record_batch.grpcxx.h"
#endif
#if defined(GRPCXX_ENABLE_CAPNP)
#include "capnp/record_batch.grpcxx.h"
#endif

namespace examples::record_batch {

inline constexpr std::string_view kTag = "record";
inline constexpr std::int64_t kValue = 10000;

/** Adapt generated Protobuf and Fory value messages to the same record operations. */
template <typename Service, typename Engine, typename Request, typename Reply>
struct ValueTraits {
  using Contract = Service;
  using Codec = Engine;
  using GetRequest = Request;
  using PutRequest = Request;
  using GetReply = Reply;
  using PutReply = Reply;

  static void Configure(Codec& codec) noexcept {
    if constexpr (requires { codec.Engine(); }) {
      codec.Engine().template register_struct<Request>(1);
      codec.Engine().template register_struct<Reply>(2);
    }
  }
  static auto MakeGet(std::int64_t id) noexcept -> Request {
    Request request;
    request.set_record_id(id);
    request.set_sequence(id);
    request.set_tag(kTag.data());
    return request;
  }
  static auto MakePut(std::int64_t id, std::int64_t count) noexcept -> Request {
    Request request;
    request.set_record_id(id);
    request.set_sequence(id);
    request.set_count(count);
    request.set_tag(kTag.data());
    return request;
  }
  static auto RecordId(const Request& request) noexcept -> std::int64_t {
    return request.record_id();
  }
  static auto Count(const Request& request) noexcept -> std::int64_t { return request.count(); }
  static bool ValidGet(const Request& request) noexcept {
    return request.record_id() > 0 && request.tag() == kTag;
  }
  static bool ValidPut(const Request& request) noexcept {
    return request.record_id() > 0 && request.tag() == kTag && request.count() > 0;
  }
  static auto GetReplyFor(std::int64_t id, std::int64_t server) noexcept -> Reply {
    Reply response;
    response.set_record_id(id);
    response.set_value(kValue + server);
    response.set_count(server);
    response.set_server(server);
    return response;
  }
  static auto PutReplyFor(std::int64_t id, std::int64_t count,
                          std::int64_t server) noexcept -> Reply {
    Reply response;
    response.set_record_id(id);
    response.set_value(count);
    response.set_count(count);
    response.set_server(server);
    return response;
  }
  static bool MatchesGet(const Reply& response, std::int64_t id, std::int64_t server) noexcept {
    return response.record_id() == id && response.value() == kValue + server &&
           response.server() == server;
  }
  static bool MatchesPut(const Reply& response, std::int64_t id, std::int64_t count,
                         std::int64_t server) noexcept {
    return response.record_id() == id && response.count() == count && response.server() == server;
  }
};

#if defined(GRPCXX_ENABLE_PROTOBUF)
using ProtobufTraits = ValueTraits<proto::RecordStoreContract, grpcxx::ProtobufCodec,
                                   proto::RecordRequest, proto::RecordReply>;
#endif
#if defined(GRPCXX_ENABLE_FORY)
using ForyTraits = ValueTraits<fory::RecordStoreContract, grpcxx::ForyCodec, fory::RecordRequest,
                               fory::RecordReply>;
#endif

#if defined(GRPCXX_ENABLE_CAPNP)
/** Cap'n Proto RPC parameters and results own their arenas across deferred completion. */
struct CapnpTraits {
  using Contract = capnp::RecordStoreContract;
  using Codec = grpcxx::CapnpCodec;
  using GetSchema = capnp::RecordStore::GetParams;
  using GetRequest = grpcxx::CapnpMessage<GetSchema>;
  using GetReply = grpcxx::CapnpMessage<capnp::RecordStore::GetResults>;
  using PutRequest = grpcxx::CapnpMessage<capnp::RecordStore::PutParams>;
  using PutReply = grpcxx::CapnpMessage<capnp::RecordStore::PutResults>;
  static void Configure(Codec&) noexcept {}
  static auto MakeGet(std::int64_t id) noexcept -> GetRequest {
    GetRequest request;
    auto root = request.GetBuilder().initRequest();
    root.setRecordId(id);
    root.setSequence(id);
    root.setTag(kTag.data());
    return request;
  }
  static auto MakePut(std::int64_t id, std::int64_t count) noexcept -> PutRequest {
    PutRequest request;
    auto root = request.GetBuilder().initRequest();
    root.setRecordId(id);
    root.setSequence(id);
    root.setCount(count);
    root.setTag(kTag.data());
    return request;
  }
  static auto RecordId(const GetRequest& request) noexcept -> std::int64_t {
    return request.GetReader().getRequest().getRecordId();
  }
  static auto RecordId(const PutRequest& request) noexcept -> std::int64_t {
    return request.GetReader().getRequest().getRecordId();
  }
  static auto Count(const PutRequest& request) noexcept -> std::int64_t {
    return request.GetReader().getRequest().getCount();
  }
  static bool ValidGet(const GetRequest& request) noexcept {
    const auto root = request.GetReader().getRequest();
    return root.getRecordId() > 0 && root.getTag() == kTag.data();
  }
  static bool ValidPut(const PutRequest& request) noexcept {
    const auto root = request.GetReader().getRequest();
    return root.getRecordId() > 0 && root.getTag() == kTag.data() && root.getCount() > 0;
  }
  static auto GetReplyFor(std::int64_t id, std::int64_t server) noexcept -> GetReply {
    GetReply response;
    auto root = response.GetBuilder().initResponse();
    root.setRecordId(id);
    root.setValue(kValue + server);
    root.setCount(server);
    root.setServer(server);
    return response;
  }
  static auto PutReplyFor(std::int64_t id, std::int64_t count,
                          std::int64_t server) noexcept -> PutReply {
    PutReply response;
    auto root = response.GetBuilder().initResponse();
    root.setRecordId(id);
    root.setValue(count);
    root.setCount(count);
    root.setServer(server);
    return response;
  }
  static bool MatchesGet(const GetReply& response, std::int64_t id, std::int64_t server) noexcept {
    const auto root = response.GetReader().getResponse();
    return root.getRecordId() == id && root.getValue() == kValue + server &&
           root.getServer() == server;
  }
  static bool MatchesPut(const PutReply& response, std::int64_t id, std::int64_t count,
                         std::int64_t server) noexcept {
    const auto root = response.GetReader().getResponse();
    return root.getRecordId() == id && root.getCount() == count && root.getServer() == server;
  }
};
#endif

}  // namespace examples::record_batch
