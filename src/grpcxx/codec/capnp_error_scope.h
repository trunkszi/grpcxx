#pragma once

#if defined(GRPCXX_ENABLE_CAPNP)

#include <kj/exception.h>

#include <utility>

namespace grpcxx::codec_detail {

/** Record recoverable KJ validation failures without C++ exception unwinding. */
class CapnpErrorScope final : public kj::ExceptionCallback {
 public:
  ~CapnpErrorScope() noexcept override = default;

  void onRecoverableException(kj::Exception&&) override { failed_ = true; }

  [[nodiscard]] auto Failed() const noexcept -> bool { return failed_; }

 private:
  bool failed_ = false;
};

/** KJ's callback seam works with both exception-enabled and disabled libraries. */
template <typename Function>
[[nodiscard]] auto RunCapnpOperation(Function&& operation) noexcept -> bool {
  CapnpErrorScope scope;
  std::forward<Function>(operation)();
  return !scope.Failed();
}

}  // namespace grpcxx::codec_detail

#endif
