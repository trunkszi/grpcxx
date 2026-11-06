#include "idl_compiler.h"

#include <capnp/schema.capnp.h>
#include <capnp/serialize.h>
#include <fmt/format.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cctype>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <ranges>
#include <set>
#include <string_view>
#include <unordered_map>
#include <utility>

#ifndef GRPCXX_FDL_ADAPTER
#define GRPCXX_FDL_ADAPTER "fdl_contract.py"
#endif

namespace grpcxx::tools {
namespace {
std::string FindTool(const char* variable, std::string_view name) {
  if (variable != nullptr) {
    if (const auto* configured = std::getenv(variable);
        configured != nullptr && *configured != '\0') {
      return configured;
    }
  }
  if (const auto* home = std::getenv("HOME"); home != nullptr && *home != '\0') {
    const auto local = std::filesystem::path(home) / ".local/bin" / name;
    std::error_code error;
    if (std::filesystem::is_regular_file(local, error) && !error) {
      /** Preserve the venv entry path instead of invoking Python through an outside symlink. */
      const auto target = std::filesystem::read_symlink(local, error);
      if (!error) return (target.is_absolute() ? target : local.parent_path() / target).string();
      return local.string();
    }
  }
  return std::string(name);
}

int Execute(std::vector<std::string> arguments, bool capnp_plugin = false) {
  std::vector<char*> argv;
  for (auto& argument : arguments) argv.push_back(argument.data());
  argv.push_back(nullptr);
  const auto child = fork();
  if (child == -1) {
    fmt::print(stderr, "grpcxx-codegen: fork failed: {}\n", errno);
    return 1;
  }
  if (child == 0) {
    if (unsetenv("PWD") != 0) _exit(127);
    if (capnp_plugin && setenv("GRPCXX_CAPNP_PLUGIN", "1", 1) != 0) _exit(127);
    execvp(argv.front(), argv.data());
    _exit(127);
  }
  int status{};
  while (waitpid(child, &status, 0) == -1) {
    if (errno == EINTR) continue;
    fmt::print(stderr, "grpcxx-codegen: waitpid failed: {}\n", errno);
    return 1;
  }
  return WIFEXITED(status) ? WEXITSTATUS(status) : 1;
}

std::filesystem::path Executable() {
  std::array<char, 4096> buffer{};
  const auto size = readlink("/proc/self/exe", buffer.data(), buffer.size());
  if (size < 0 || static_cast<std::size_t>(size) == buffer.size()) return {};
  return std::string(buffer.data(), static_cast<std::size_t>(size));
}

std::string Text(capnp::Text::Reader value) { return std::string(value.cStr(), value.size()); }

std::string Pascal(std::string value) {
  if (!value.empty())
    value.front() = static_cast<char>(std::toupper(static_cast<unsigned char>(value.front())));
  return value;
}

std::string Namespace(capnp::schema::Node::Reader node,
                      const std::unordered_map<std::uint64_t, capnp::schema::Node::Reader>& nodes) {
  for (const auto annotation : node.getAnnotations()) {
    if (auto found = nodes.find(annotation.getId()); found != nodes.end()) {
      if (Text(found->second.getDisplayName()).ends_with("c++.capnp:namespace")) {
        return Text(annotation.getValue().getText());
      }
    }
  }
  return {};
}

std::string CppName(capnp::List<capnp::schema::Annotation>::Reader annotations,
                    std::string fallback,
                    const std::unordered_map<std::uint64_t, capnp::schema::Node::Reader>& nodes) {
  for (const auto annotation : annotations) {
    if (const auto found = nodes.find(annotation.getId()); found != nodes.end()) {
      if (Text(found->second.getDisplayName()).ends_with("c++.capnp:name")) {
        return Text(annotation.getValue().getText());
      }
    }
  }
  return fallback;
}
}  // namespace

int IdlCompiler::Run(std::span<const std::string> arguments) const {
  std::filesystem::path output = ".";
  std::string protoc = FindTool("PROTOC", "protoc");
  std::vector<std::string> imports;
  std::vector<std::string> inputs;
  for (std::size_t index = 0; index < arguments.size(); ++index) {
    const auto& argument = arguments[index];
    if (argument == "--help") {
      fmt::print(
          "Usage: grpcxx-codegen --out DIR [-I DIR] [--protoc PATH] FILE.proto|FILE.fdl|FILE.capnp "
          "...\n");
      return 0;
    }
    if (argument == "--capnp-plugin") return CompileCapnpRequest(".");
    if (argument == "--out" || argument == "-I" || argument == "--protoc") {
      if (++index == arguments.size()) {
        fmt::print(stderr, "grpcxx-codegen: missing argument after {}\n", argument);
        return 1;
      }
      if (argument == "--out")
        output = arguments[index];
      else if (argument == "--protoc")
        protoc = arguments[index];
      else
        imports.push_back(arguments[index]);
    } else if (argument.starts_with("-I") && argument.size() > 2) {
      imports.push_back(argument.substr(2));
    } else if (argument.starts_with('-')) {
      fmt::print(stderr, "grpcxx-codegen: unknown option {}\n", argument);
      return 1;
    } else
      inputs.push_back(argument);
  }
  if (inputs.empty()) {
    /** capnp invokes compiler plugins without arguments. */
    if (const auto* mode = std::getenv("GRPCXX_CAPNP_PLUGIN");
        arguments.empty() && mode != nullptr && std::string_view(mode) == "1") {
      return CompileCapnpRequest(".");
    }
    fmt::print(stderr, "grpcxx-codegen: no input files\n");
    return 1;
  }
  std::set<std::string> output_names;
  for (const auto& input : inputs) {
    const auto stem = std::filesystem::path(input).stem().string();
    if (!output_names.insert(stem).second) {
      fmt::print(stderr, "grpcxx-codegen: contract output name collision for {}\n", input);
      return 1;
    }
  }
  std::error_code error;
  std::filesystem::create_directories(output, error);
  if (error) {
    fmt::print(stderr, "grpcxx-codegen: {}\n", error.message());
    return 1;
  }
  for (const auto& input : inputs) {
    if (const auto result = Compile(input, output, imports, protoc); result != 0) return result;
  }
  return 0;
}

int IdlCompiler::Compile(const std::filesystem::path& input, const std::filesystem::path& output,
                         std::span<const std::string> imports, const std::string& protoc) const {
  const auto executable = Executable();
  if (executable.empty()) {
    fmt::print(stderr, "grpcxx-codegen: cannot locate compiler executable\n");
    return 1;
  }
  const auto parent = input.has_parent_path() ? input.parent_path() : std::filesystem::path(".");
  const auto extension = input.extension();
  std::vector<std::string> command;
  if (extension == ".proto") {
    command = {
        protoc, "--cpp_out=" + output.string(), "--grpcxx_out=" + output.string(),
        "--plugin=protoc-gen-grpcxx=" + (executable.parent_path() / "protoc-gen-grpcxx").string(),
        "-I" + parent.string()};
  } else if (extension == ".fdl") {
    command = {FindTool("FORYC", "foryc"),
               "--lang",
               "cpp",
               "--output",
               output.string(),
               "-I",
               parent.string()};
  } else if (extension == ".capnp") {
    command = {FindTool(nullptr, "capnp"),
               "compile",
               "--src-prefix=" + parent.string(),
               "-oc++:" + output.string(),
               "-o" + executable.string() + ":" + output.string(),
               "-I" + parent.string()};
  } else {
    fmt::print(stderr, "grpcxx-codegen: unsupported input extension {}\n", extension.string());
    return 1;
  }
  for (const auto& path : imports) command.push_back("-I" + path);
  command.push_back(input.string());
  if (const auto result = Execute(std::move(command), extension == ".capnp"); result != 0)
    return result;
  if (extension == ".fdl") {
    std::vector<std::string> adapter{FindTool("FORY_PYTHON", "python3"), GRPCXX_FDL_ADAPTER,
                                     input.string(), output.string()};
    for (const auto& path : imports) adapter.push_back(path);
    return Execute(std::move(adapter));
  }
  return 0;
}

int IdlCompiler::CompileCapnpRequest(const std::filesystem::path& output) const {
  capnp::ReaderOptions options;
  options.traversalLimitInWords = 64 * 1024 * 1024;
  capnp::StreamFdMessageReader message(STDIN_FILENO, options);
  const auto request = message.getRoot<capnp::schema::CodeGeneratorRequest>();
  std::unordered_map<std::uint64_t, capnp::schema::Node::Reader> nodes;
  for (const auto node : request.getNodes()) nodes.emplace(node.getId(), node);
  for (const auto file : request.getRequestedFiles()) {
    const auto file_node = nodes.find(file.getId());
    if (file_node == nodes.end()) return 1;
    for (const auto& [id, node] : nodes) {
      if (!node.isInterface() || node.getScopeId() == file.getId()) continue;
      auto scope = node.getScopeId();
      for (std::size_t depth = 0; scope != 0 && depth < nodes.size(); ++depth) {
        if (scope == file.getId()) {
          fmt::print(stderr, "grpcxx-codegen: nested Capnp interfaces are unsupported: {}\n",
                     Text(node.getDisplayName()));
          return 1;
        }
        const auto parent = nodes.find(scope);
        if (parent == nodes.end()) break;
        scope = parent->second.getScopeId();
      }
    }
    const auto cpp_namespace = Namespace(file_node->second, nodes);
    std::string wire_namespace;
    for (const auto part : cpp_namespace | std::views::split(std::string_view("::"))) {
      if (!wire_namespace.empty()) wire_namespace += '.';
      wire_namespace.append(part.data(), part.size());
    }
    const auto source = std::filesystem::path(Text(file.getFilename()));
    auto generated = fmt::format(
        "/** Generated ServiceContract declarations. */\n#pragma once\n"
        "#include <grpcxx/tools/service_annotations.h>\n"
        "#include <grpcxx/codec/capnp_codec.h>\n#include \"{}.h\"\n",
        source.string());
    if (!cpp_namespace.empty()) generated += fmt::format("namespace {} {{\n", cpp_namespace);
    for (const auto nested : file_node->second.getNestedNodes()) {
      const auto found = nodes.find(nested.getId());
      if (found == nodes.end() || !found->second.isInterface()) continue;
      if (found->second.getInterface().getSuperclasses().size() != 0 ||
          found->second.getParameters().size() != 0) {
        fmt::print(stderr,
                   "grpcxx-codegen: generic and inherited Capnp interfaces are unsupported\n");
        return 1;
      }
      const auto service_name = Text(nested.getName());
      const auto cpp_service_name = CppName(found->second.getAnnotations(), service_name, nodes);
      const auto cpp_service_type =
          "::" + cpp_namespace + (cpp_namespace.empty() ? "" : "::") + cpp_service_name;
      generated += fmt::format("struct [[= grpcxx::service{{.name = \"{}{}{}\"}}]] {}Contract {{\n",
                               wire_namespace, wire_namespace.empty() ? "" : ".", service_name,
                               cpp_service_name);
      for (const auto method : found->second.getInterface().getMethods()) {
        bool client_streaming = false;
        bool server_streaming = false;
        for (const auto annotation : method.getAnnotations()) {
          const auto definition = nodes.find(annotation.getId());
          if (definition == nodes.end()) continue;
          const auto name = Text(definition->second.getDisplayName());
          if ((name.ends_with(":grpcxxClientStreaming") ||
               name.ends_with(":grpcxxServerStreaming")) &&
              !annotation.getValue().isBool()) {
            fmt::print(stderr, "grpcxx-codegen: streaming annotations must have Bool type\n");
            return 1;
          }
          if (name.ends_with(":grpcxxClientStreaming"))
            client_streaming = annotation.getValue().getBool();
          if (name.ends_with(":grpcxxServerStreaming"))
            server_streaming = annotation.getValue().getBool();
        }
        const auto* kind = client_streaming
                               ? (server_streaming ? "kBidiStreaming" : "kClientStreaming")
                               : (server_streaming ? "kServerStreaming" : "kUnary");
        const auto method_name = Text(method.getName());
        const auto cpp_payload_name = Pascal(CppName(method.getAnnotations(), method_name, nodes));
        auto cpp_method_name = cpp_payload_name;
        if (cpp_method_name == cpp_service_name + "Contract") cpp_method_name += '_';
        generated += fmt::format(
            "  [[= grpcxx::rpc{{.name = \"{}\", .kind = grpcxx::RpcKind::{}}}]]\n"
            "  grpcxx::CapnpMessage<{}::{}Results> {}(const grpcxx::CapnpMessage<{}::{}Params>&) "
            "noexcept;\n",
            method_name, kind, cpp_service_type, cpp_payload_name, cpp_method_name,
            cpp_service_type, cpp_payload_name);
      }
      generated += "};\n";
    }
    if (!cpp_namespace.empty()) generated += "}\n";
    auto destination = output / source;
    destination.replace_extension(".grpcxx.h");
    std::error_code error;
    std::filesystem::create_directories(destination.parent_path(), error);
    if (error) return 1;
    std::ofstream stream(destination, std::ios::binary);
    stream << generated;
    if (!stream) return 1;
  }
  return 0;
}
}  // namespace grpcxx::tools
