#pragma once
#include <grpcxx/core/status.h>
#include <grpcxx/core/timeout.h>
#include <grpcxx/h2/content_type.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <bit>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <meta>
#include <nexora/http2/connection.hpp>
#include <span>
#include <string_view>
#include <uarch/uarch.hpp>

namespace grpcxx::h2_detail {
struct RuntimeFlags {
  using Word = std::uint64_t;
  bool driving[[= uarch::bits::Flag{0}]];
  bool failed[[= uarch::bits::Flag{1}]];
  bool draining[[= uarch::bits::Flag{2}]];
  bool type_ok[[= uarch::bits::Flag{3}]];
  bool status_seen[[= uarch::bits::Flag{4}]];
  bool invalid[[= uarch::bits::Flag{5}]];
  bool streaming[[= uarch::bits::Flag{6}]];
  bool end_requested[[= uarch::bits::Flag{7}]];
  bool ended[[= uarch::bits::Flag{8}]];
  bool dispatched[[= uarch::bits::Flag{9}]];
  bool sending[[= uarch::bits::Flag{10}]];
  bool headers_sent[[= uarch::bits::Flag{11}]];
  bool finish_requested[[= uarch::bits::Flag{12}]];
  bool rx_ready[[= uarch::bits::Flag{13}]];
  bool tx_ready[[= uarch::bits::Flag{14}]];
  bool connect_ready[[= uarch::bits::Flag{15}]];
  bool registered[[= uarch::bits::Flag{16}]];
  bool fatal[[= uarch::bits::Flag{17}]];
  bool established[[= uarch::bits::Flag{18}]];
  bool closing[[= uarch::bits::Flag{19}]];
  bool ready[[= uarch::bits::Flag{20}]];
  bool rejected[[= uarch::bits::Flag{21}]];
  bool trailers_only[[= uarch::bits::Flag{22}]];
  bool rst_after_reply[[= uarch::bits::Flag{23}]];
  bool peer_eof[[= uarch::bits::Flag{24}]];
  bool handshake_done[[= uarch::bits::Flag{25}]];
};

class FlagState {
  uarch::BitWord<RuntimeFlags> word_{};

 public:
  constexpr void Reset() noexcept { word_ = {}; }
  template <std::meta::info Field>
    requires(std::meta::is_nonstatic_data_member(Field))
  [[nodiscard]] constexpr bool Test() const noexcept {
    return word_.template test<Field>();
  }
  template <std::meta::info Field>
    requires(std::meta::is_nonstatic_data_member(Field))
  constexpr bool Set(bool value) noexcept {
    word_ = word_.template with<Field>(value);
    return value;
  }
};
static_assert(sizeof(FlagState) == sizeof(std::uint64_t));

inline constexpr std::size_t kMaxConcurrentStreams = 16;
inline constexpr std::size_t kMaxPathBytes = 512;
inline constexpr std::size_t kMessageCapacity = 8uz * 1024uz * 1024uz;
inline constexpr std::size_t kIoBufferBytes = 262144;
inline constexpr std::size_t kDataHeadroomBytes = 16384;
inline constexpr std::size_t kHeaderHeadroomBytes = 4096;
inline constexpr std::size_t kCreditHeadroomBytes = 1024;
inline constexpr std::size_t kFeedSliceBytes = 256;
inline constexpr std::uint32_t kInitialWindowBytes = 16U * 1024U * 1024U;

[[nodiscard]] inline std::uint64_t CachedThreadId() noexcept {
  thread_local std::uint64_t tid{};
  if (tid == 0) [[unlikely]] {
    tid = static_cast<std::uint64_t>(::gettid());
  }
  return tid;
}

[[nodiscard]] inline std::uint64_t SteadyNowNs() noexcept {
  timespec ts{};
  ::clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<std::uint64_t>(ts.tv_sec) * 1'000'000'000ULL +
         static_cast<std::uint64_t>(ts.tv_nsec);
}

[[nodiscard]] inline std::uint64_t ClockNow(std::uint64_t provided) noexcept {
  return provided != 0 ? provided : SteadyNowNs();
}

[[nodiscard]] inline Status ProtocolError(nexora::Error error) noexcept {
  return Status{error == nexora::Error::BufferOverflow() ? StatusCode::kResourceExhausted
                                                         : StatusCode::kInternal};
}

[[nodiscard]] inline std::span<const std::uint8_t> Wire(std::span<const std::byte> bytes) noexcept {
  return {std::bit_cast<const std::uint8_t*>(bytes.data()), bytes.size()};
}

[[nodiscard]] inline std::span<const std::byte> Bytes(
    std::span<const std::uint8_t> bytes) noexcept {
  return std::as_bytes(bytes);
}

[[nodiscard]] constexpr std::string_view FormatGrpcStatus(int status,
                                                          std::array<char, 4>& digits) noexcept {
  const auto clamped = (status >= 0 && status <= 16) ? status : 2;
  if (clamped >= 10) {
    digits[0] = '1';
    digits[1] = static_cast<char>('0' + (clamped - 10));
    return {digits.data(), 2};
  }
  digits[0] = static_cast<char>('0' + clamped);
  return {digits.data(), 1};
}

[[nodiscard]] constexpr std::string_view FormatPortU16(std::uint16_t port,
                                                       std::span<char, 8> out) noexcept {
  if (port == 0) {
    out[0] = '0';
    return {out.data(), 1};
  }
  constexpr auto kTen = uarch::arithmetic::divisor_u32<10U>();
  std::size_t pos = out.size();
  std::uint32_t val = port;
  while (val != 0) {
    const auto [quot, rem] = kTen.div_rem(val);
    out[--pos] = static_cast<char>('0' + rem);
    val = quot;
  }
  return {out.data() + pos, out.size() - pos};
}

[[nodiscard]] constexpr bool IsUnreserved(unsigned char ch) noexcept {
  return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') ||
         ch == '-' || ch == '.' || ch == '_' || ch == '~';
}

[[nodiscard]] constexpr int HexNibble(unsigned char ch) noexcept {
  if (ch >= '0' && ch <= '9') return ch - '0';
  if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
  if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
  return -1;
}

[[nodiscard]] constexpr std::string_view PercentEncodeInto(std::string_view input,
                                                           std::span<char> out) noexcept {
  constexpr std::string_view kHex = "0123456789ABCDEF";
  std::size_t pos = 0;
  for (const char raw : input) {
    const auto ch = static_cast<unsigned char>(raw);
    if (IsUnreserved(ch)) {
      if (pos + 1 > out.size()) break;
      out[pos++] = raw;
    } else {
      if (pos + 3 > out.size()) break;
      out[pos++] = '%';
      out[pos++] = kHex[(ch >> 4U) & 0x0fU];
      out[pos++] = kHex[ch & 0x0fU];
    }
  }
  return {out.data(), pos};
}

[[nodiscard]] constexpr std::string_view PercentDecodeInto(std::string_view input,
                                                           std::span<char> out) noexcept {
  std::size_t pos = 0;
  for (std::size_t idx = 0; idx < input.size() && pos < out.size(); ++idx) {
    if (input[idx] == '%' && idx + 2 < input.size()) {
      const int hi = HexNibble(static_cast<unsigned char>(input[idx + 1]));
      const int lo = HexNibble(static_cast<unsigned char>(input[idx + 2]));
      if (hi >= 0 && lo >= 0) {
        out[pos++] = static_cast<char>((hi << 4) | lo);
        idx += 2;
        continue;
      }
    }
    out[pos++] = input[idx];
  }
  return {out.data(), pos};
}

struct MessageBuffer {
  static constexpr std::size_t kMaxWireBytes = kMessageCapacity + 5;
  static constexpr std::size_t kPrefixOffset = 3;
  std::unique_ptr<std::byte[]> storage{};
  std::size_t capacity{};
  std::size_t size{};

  [[nodiscard]] std::byte* Data() const noexcept {
    return storage ? storage.get() + kPrefixOffset : nullptr;
  }
  [[nodiscard]] std::span<const std::byte> WireBytes() const noexcept {
    return size ? std::span<const std::byte>(Data(), size) : std::span<const std::byte>{};
  }
  void Reset() noexcept { size = 0; }
  [[nodiscard]] bool Reserve(std::size_t needed) noexcept {
    if (needed > kMaxWireBytes) return false;
    if (needed <= capacity) return true;
    const auto next = std::min(kMaxWireBytes, std::max(4096uz, std::bit_ceil(needed)));
    auto fresh = std::make_unique_for_overwrite<std::byte[]>(next + 8);
    if (size != 0) std::memcpy(fresh.get() + kPrefixOffset, Data(), size);
    storage = std::move(fresh);
    capacity = next;
    return true;
  }
  [[nodiscard]] bool StoreRaw(std::span<const std::byte> raw) noexcept {
    if (raw.size() > kMessageCapacity) return false;
    if (raw.empty()) {
      size = 0;
      return true;
    }
    if (!Reserve(raw.size() + 5)) return false;
    std::memcpy(Data() + 5, raw.data(), raw.size());
    size = raw.size();
    return true;
  }
  [[nodiscard]] std::span<const std::byte> RawBytes() const noexcept {
    return size ? std::span<const std::byte>(Data() + 5, size) : std::span<const std::byte>{};
  }
  [[nodiscard]] bool Append(std::span<const std::uint8_t> data) noexcept {
    if (data.size() > kMaxWireBytes - size) return false;
    if (data.empty()) return true;
    if (!Reserve(size + data.size())) return false;
    std::memcpy(Data() + size, data.data(), data.size());
    size += data.size();
    return true;
  }
  [[nodiscard]] bool Assign(std::span<const std::byte> payload) noexcept {
    if (payload.size() > kMessageCapacity || !Reserve(payload.size() + 5)) return false;
    Data()[0] = std::byte{};
    auto length = static_cast<std::uint32_t>(payload.size());
    if constexpr (std::endian::native == std::endian::little) length = std::byteswap(length);
    std::memcpy(Data() + 1, &length, 4);
    if (!payload.empty()) std::memcpy(Data() + 5, payload.data(), payload.size());
    size = payload.size() + 5;
    return true;
  }
  template <typename Visitor, typename AlivePred>
  [[nodiscard]] bool Decode(std::span<const std::uint8_t> incoming, Visitor&& visitor,
                            AlivePred&& alive) noexcept {
    while (!incoming.empty()) {
      const auto count = std::min(incoming.size(), kMaxWireBytes - size);
      if (count == 0 || !Append(incoming.first(count))) return false;
      incoming = incoming.subspan(count);
      while (size >= 5) {
        if (Data()[0] != std::byte{}) return false;
        std::uint32_t length{};
        std::memcpy(&length, Data() + 1, 4);
        if constexpr (std::endian::native == std::endian::little) length = std::byteswap(length);
        if (length > kMessageCapacity) return false;
        const auto frame_size = std::size_t{length} + 5;
        if (size < frame_size) break;
        visitor(std::span<const std::byte>(Data(), size).subspan(5, length));
        if (!alive()) return true;
        size -= frame_size;
        if (size != 0) std::memmove(Data(), Data() + frame_size, size);
      }
    }
    return true;
  }
  [[nodiscard]] std::expected<std::span<const std::byte>, Status> Payload() const noexcept {
    if (size < 5 || Data()[0] != std::byte{}) return std::unexpected(Status{StatusCode::kInternal});
    std::uint32_t length{};
    std::memcpy(&length, Data() + 1, 4);
    if constexpr (std::endian::native == std::endian::little) length = std::byteswap(length);
    if (length > kMessageCapacity || length != size - 5)
      return std::unexpected(Status{StatusCode::kInternal});
    return std::span<const std::byte>(Data(), size).subspan(5, length);
  }
};

[[nodiscard]] inline nexora::Http2Limits Limits() noexcept {
  return {.io_buffer_bytes = kIoBufferBytes,
          .header_block_bytes = 65536,
          .max_coalesced_body_bytes = 0,
          .coalesce_message_body = false};
}

[[nodiscard]] inline nexora::Http2Settings Settings() noexcept {
  nexora::Http2Settings settings;
  settings.max_concurrent_streams = static_cast<std::uint32_t>(kMaxConcurrentStreams);
  settings.initial_window_size = kInitialWindowBytes;
  return settings;
}

struct SessionCore {
  struct PendingRst {
    std::uint32_t stream{};
    nexora::Http2ErrorCode code{nexora::Http2ErrorCode::kNoError};
  };

  nexora::Http2Connection h2;
  std::array<std::byte, kIoBufferBytes> output{};
  std::array<std::uint32_t, 64> stream_credit{}, credit_ids{};
  std::array<PendingRst, 32> pending_rst{};
  std::size_t pending_rst_count{};
  FlagState flags;
  std::uint64_t rejected{};

  explicit SessionCore(nexora::Http2Role role) noexcept : h2(role, Settings(), Limits()) {
    flags.Set<^^RuntimeFlags::failed>(!h2.InitiateHandshake());
    if (!flags.Test<^^RuntimeFlags::failed>())
      flags.Set<^^RuntimeFlags::failed>(!h2.SubmitWindowUpdate(0, kInitialWindowBytes - 65535U));
  }

  [[nodiscard]] bool HeaderReady() const noexcept {
    return h2.PendingOutput().size() + kHeaderHeadroomBytes <= kIoBufferBytes;
  }

  [[nodiscard]] std::size_t DataBudget() const noexcept {
    const auto pending = h2.PendingOutput().size();
    if (pending + kDataHeadroomBytes + 9 >= kIoBufferBytes) return 0;
    return kIoBufferBytes - kDataHeadroomBytes - 9 - pending;
  }

  [[nodiscard]] bool HasPendingControl() const noexcept {
    return pending_rst_count != 0 ||
           h2.ConnectionRecvWindow() < static_cast<std::int64_t>(kInitialWindowBytes);
  }

  [[nodiscard]] bool QueueOrSubmitRst(std::uint32_t stream, nexora::Http2ErrorCode code) noexcept {
    auto r = h2.SubmitRstStream(stream, code);
    if (r || r.error() == nexora::Error::FromProto(nexora::ProtoErrorCode::kHttp2StreamClosed))
      return true;
    if (r.error() != nexora::Error::BufferOverflow() || pending_rst_count >= pending_rst.size())
      return false;
    pending_rst[pending_rst_count++] = {stream, code};
    return true;
  }

  template <typename DrainFn>
  [[nodiscard]] bool Feed(std::span<const std::byte> bytes, DrainFn&& drain) noexcept {
    const auto wire = Wire(bytes);
    std::size_t offset = 0;
    while (offset < wire.size() || h2.IsIngressPaused()) {
      const auto slice = wire.subspan(offset, std::min(kFeedSliceBytes, wire.size() - offset));
      const bool had_events_before = h2.PeekEventView().has_value();
      const auto fed = h2.FeedBytes(slice);
      if (!fed.Ok()) return false;
      const auto consumed = fed.Consumed();
      offset += consumed;
      const bool has_events = h2.PeekEventView().has_value();
      if (!drain()) return false;
      if (consumed == 0 && !had_events_before && !has_events && h2.IsIngressPaused()) {
        return offset == wire.size();
      }
    }
    return true;
  }

  [[nodiscard]] std::expected<std::span<const std::byte>, Status> Send() noexcept {
    if (!FlushCredits()) return std::unexpected(Status{StatusCode::kInternal});
    const auto bytes = h2.PendingOutput();
    if (bytes.size() > output.size())
      return std::unexpected(Status{StatusCode::kResourceExhausted});
    if (!bytes.empty()) std::memcpy(output.data(), bytes.data(), bytes.size());
    h2.ConsumeOutput(bytes.size());
    return std::span<const std::byte>(output).first(bytes.size());
  }

  [[nodiscard]] bool FlushCredits() noexcept {
    std::size_t rst_idx = 0;
    while (rst_idx < pending_rst_count && h2.PendingOutput().size() + 13 <= kIoBufferBytes) {
      auto r = h2.SubmitRstStream(pending_rst[rst_idx].stream, pending_rst[rst_idx].code);
      if (!r && r.error() != nexora::Error::FromProto(nexora::ProtoErrorCode::kHttp2StreamClosed)) {
        if (r.error() == nexora::Error::BufferOverflow()) break;
        return false;
      }
      ++rst_idx;
    }
    if (rst_idx != 0) {
      pending_rst_count -= rst_idx;
      if (pending_rst_count != 0)
        std::memmove(pending_rst.data(), pending_rst.data() + rst_idx,
                     pending_rst_count * sizeof(PendingRst));
    }
    const auto deficit = static_cast<std::int64_t>(kInitialWindowBytes) - h2.ConnectionRecvWindow();
    if (deficit > 0 && h2.PendingOutput().size() + kCreditHeadroomBytes <= kIoBufferBytes) {
      const auto inc = static_cast<std::uint32_t>(std::min<std::int64_t>(deficit, 0x7fffffffLL));
      if (!h2.SubmitWindowUpdate(0, inc)) return false;
    }
    for (std::size_t slot = 0; slot < stream_credit.size(); ++slot) {
      if (!stream_credit[slot] || h2.PendingOutput().size() + kCreditHeadroomBytes > kIoBufferBytes)
        continue;
      auto update = h2.SubmitWindowUpdate(credit_ids[slot], stream_credit[slot]);
      if (!update &&
          update.error() != nexora::Error::FromProto(nexora::ProtoErrorCode::kHttp2StreamClosed))
        return false;
      stream_credit[slot] = 0;
    }
    return true;
  }

  [[nodiscard]] bool Credit(std::uint32_t stream, std::size_t bytes, bool end) noexcept {
    if (bytes == 0) return FlushCredits();
    if (!end && stream != 0) {
      std::size_t target = stream_credit.size();
      for (std::size_t slot = 0; slot < stream_credit.size(); ++slot) {
        if (stream_credit[slot] != 0 && credit_ids[slot] == stream) {
          target = slot;
          break;
        }
        if (stream_credit[slot] == 0 && target == stream_credit.size()) target = slot;
      }
      if (target != stream_credit.size()) {
        credit_ids[target] = stream;
        stream_credit[target] += static_cast<std::uint32_t>(bytes);
      }
    }
    return FlushCredits();
  }
};

struct DriveScope {
  bool* direct{};
  FlagState* flags{};
  explicit DriveScope(bool& value) noexcept : direct(&value) { value = true; }
  explicit DriveScope(FlagState& value) noexcept : flags(&value) {
    flags->Set<^^RuntimeFlags::driving>(true);
  }
  ~DriveScope() {
    if (direct)
      *direct = false;
    else
      flags->Set<^^RuntimeFlags::driving>(false);
  }
};
}  // namespace grpcxx::h2_detail
