#include "contract_generator.h"

#include <fmt/format.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>

#include <algorithm>
#include <array>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>

namespace grpcxx::tools {
namespace {
std::string Identifier(std::string name) {
  constexpr std::array keywords{"alignas",
                                "alignof",
                                "and",
                                "and_eq",
                                "asm",
                                "atomic_cancel",
                                "atomic_commit",
                                "atomic_noexcept",
                                "auto",
                                "bitand",
                                "bitor",
                                "bool",
                                "break",
                                "case",
                                "catch",
                                "char",
                                "char8_t",
                                "char16_t",
                                "char32_t",
                                "class",
                                "compl",
                                "concept",
                                "const",
                                "consteval",
                                "constexpr",
                                "constinit",
                                "const_cast",
                                "continue",
                                "co_await",
                                "co_return",
                                "co_yield",
                                "decltype",
                                "default",
                                "delete",
                                "do",
                                "double",
                                "dynamic_cast",
                                "else",
                                "enum",
                                "explicit",
                                "export",
                                "extern",
                                "false",
                                "float",
                                "for",
                                "friend",
                                "goto",
                                "if",
                                "inline",
                                "int",
                                "long",
                                "mutable",
                                "namespace",
                                "new",
                                "noexcept",
                                "not",
                                "not_eq",
                                "nullptr",
                                "operator",
                                "or",
                                "or_eq",
                                "private",
                                "protected",
                                "public",
                                "reflexpr",
                                "register",
                                "reinterpret_cast",
                                "requires",
                                "return",
                                "short",
                                "signed",
                                "sizeof",
                                "static",
                                "static_assert",
                                "static_cast",
                                "struct",
                                "switch",
                                "synchronized",
                                "template",
                                "this",
                                "thread_local",
                                "throw",
                                "true",
                                "try",
                                "typedef",
                                "typeid",
                                "typename",
                                "union",
                                "unsigned",
                                "using",
                                "virtual",
                                "void",
                                "volatile",
                                "wchar_t",
                                "while",
                                "xor",
                                "xor_eq"};
  if (std::ranges::any_of(keywords, [&](std::string_view keyword) { return keyword == name; }))
    name += '_';
  return name;
}
std::string CppNamespace(std::string_view package) {
  std::string result;
  for (auto segment : package | std::views::split('.')) {
    if (!result.empty()) result += "::";
    result += Identifier(std::string(segment.data(), segment.size()));
  }
  return result;
}
std::string CppType(const google::protobuf::Descriptor& descriptor) {
  if (const auto* parent = descriptor.containing_type(); parent != nullptr) {
    return CppType(*parent) + "_" + Identifier(descriptor.name());
  }
  auto result = CppNamespace(descriptor.file()->package());
  if (!result.empty()) result += "::";
  return "::" + result + Identifier(descriptor.name());
}
}  // namespace
bool ContractGenerator::Generate(const google::protobuf::FileDescriptor* file,
                                 const std::string& parameter,
                                 google::protobuf::compiler::GeneratorContext* context,
                                 std::string* error) const {
  if (!parameter.empty()) {
    *error = "protoc-gen-grpcxx accepts no generator parameters";
    return false;
  }
  const auto stem = file->name().substr(0, file->name().rfind('.'));
  auto output = fmt::format(
      "/** Generated ServiceContract declarations. */\n#pragma once\n"
      "#include <grpcxx/tools/service_annotations.h>\n#include \"{}.pb.h\"\n",
      stem);
  const auto cpp_namespace = CppNamespace(file->package());
  if (!cpp_namespace.empty()) {
    output += fmt::format("namespace {} {{\n", cpp_namespace);
  }
  for (const auto service_index : std::views::iota(0, file->service_count())) {
    const auto& service = *file->service(service_index);
    output += fmt::format("struct [[= grpcxx::service{{.name = \"{}\"}}]] {}Contract {{\n",
                          service.full_name(), service.name());
    for (const auto method_index : std::views::iota(0, service.method_count())) {
      const auto& method = *service.method(method_index);
      const auto* kind = method.client_streaming()
                             ? (method.server_streaming() ? "kBidiStreaming" : "kClientStreaming")
                             : (method.server_streaming() ? "kServerStreaming" : "kUnary");
      auto cpp_method_name = Identifier(method.name());
      if (cpp_method_name == service.name() + "Contract") cpp_method_name += '_';
      output += fmt::format(
          "  [[= grpcxx::rpc{{.name = \"{}\", .kind = grpcxx::RpcKind::{}}}]]\n"
          "  {} {}(const {}&) noexcept;\n",
          method.name(), kind, CppType(*method.output_type()), cpp_method_name,
          CppType(*method.input_type()));
    }
    output += "};\n";
  }
  if (!cpp_namespace.empty()) output += "}\n";
  std::unique_ptr<google::protobuf::io::ZeroCopyOutputStream> stream(
      context->Open(stem + ".grpcxx.h"));
  google::protobuf::io::CodedOutputStream coded(stream.get());
  coded.WriteString(output);
  if (coded.HadError()) {
    *error = "failed writing generated contract";
    return false;
  }
  return true;
}
}  // namespace grpcxx::tools
