#pragma once

#include <filesystem>
#include <span>
#include <string>
#include <vector>

namespace grpcxx::tools {
/** Standalone compiler driver; child tools receive argv without a shell. */
class IdlCompiler {
 public:
  int Run(std::span<const std::string> arguments) const;
  int CompileCapnpRequest(const std::filesystem::path& output) const;

 private:
  int Compile(const std::filesystem::path& input, const std::filesystem::path& output,
              std::span<const std::string> imports, const std::string& protoc) const;
};
}  // namespace grpcxx::tools
