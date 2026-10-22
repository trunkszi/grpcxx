#pragma once

#include <grpcxx/core/status.h>
#include <grpcxx/framing/aligned_message_buf.h>

#include <concepts>
#include <cstddef>
#include <expected>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>

namespace grpcxx {

/** Payload serialization seam; framing and compression belong to the caller. */
template <typename Engine, typename Message>
concept Codec = requires(Engine& engine, const Message& input, Message& output,
                         AlignedMessageBuf& buffer, std::span<const std::byte> payload) {
  requires std::same_as<std::remove_cv_t<decltype(Engine::kContentType)>, std::string_view>;
  { engine.Encode(input, buffer) } noexcept -> std::same_as<std::expected<void, Status>>;
  { engine.Decode(payload, output) } noexcept -> std::same_as<std::expected<void, Status>>;
};

}  // namespace grpcxx
