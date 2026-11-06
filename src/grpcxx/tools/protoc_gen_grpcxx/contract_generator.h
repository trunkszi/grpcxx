#pragma once

#include <google/protobuf/compiler/code_generator.h>

#include <cstdint>

namespace grpcxx::tools {
/** Emits annotated contracts from compiler-validated Protobuf descriptors. */
class ContractGenerator final : public google::protobuf::compiler::CodeGenerator {
 public:
  bool Generate(const google::protobuf::FileDescriptor* file, const std::string& parameter,
                google::protobuf::compiler::GeneratorContext* context,
                std::string* error) const override;
  std::uint64_t GetSupportedFeatures() const override { return FEATURE_PROTO3_OPTIONAL; }
};
}  // namespace grpcxx::tools
