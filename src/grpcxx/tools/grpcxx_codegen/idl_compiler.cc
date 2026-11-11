#include "idl_compiler.h"

#include <capnp/schema.capnp.h>
#include <capnp/serialize.h>
#include <fmt/format.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <optional>
#include <ranges>
#include <string_view>
#include <utility>

namespace grpcxx::tools {
namespace {
std::filesystem::path Executable() {
  std::error_code error;
  const auto executable = std::filesystem::read_symlink("/proc/self/exe", error);
  return error ? std::filesystem::path{} : executable;
}

std::string FdlAdapter() {
  if (const auto* configured = std::getenv("GRPCXX_FDL_ADAPTER");
      configured != nullptr && *configured != '\0')
    return configured;
  const auto directory = Executable().parent_path();
  for (const auto relative :
       {"../tools/grpcxx/fdl_contract.py", "../share/grpcxx/fdl_contract.py"}) {
    const auto candidate = directory / relative;
    std::error_code error;
    if (std::filesystem::is_regular_file(candidate, error) && !error) return candidate.string();
  }
  return {};
}

std::string FindTool(const char* variable, std::string_view name) {
  if (variable != nullptr) {
    if (const auto* configured = std::getenv(variable);
        configured != nullptr && *configured != '\0') {
      return configured;
    }
  }
  const auto sibling = Executable().parent_path() / name;
  std::error_code sibling_error;
  if (std::filesystem::is_regular_file(sibling, sibling_error) && !sibling_error)
    return sibling.string();
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

std::string FdlPython(const std::string& foryc) {
  if (const auto* configured = std::getenv("FORY_PYTHON");
      configured != nullptr && *configured != '\0') {
    return configured;
  }
  /** Resolving this symlink leaves the venv prefix and drops fory_compiler. */
  const auto sibling = std::filesystem::path(foryc).parent_path() / "python3";
  std::error_code error;
  if (std::filesystem::is_regular_file(sibling, error) && !error) return sibling.string();
  if (const auto* home = std::getenv("HOME"); home != nullptr && *home != '\0') {
    const auto venv = std::filesystem::path(home) / ".local/share/fory-venv/bin/python3";
    if (std::filesystem::is_regular_file(venv, error) && !error) return venv.string();
  }
  return FindTool("FORY_PYTHON", "python3");
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

std::string Text(capnp::Text::Reader value) { return std::string(value.cStr(), value.size()); }

std::string Pascal(std::string value) {
  if (!value.empty())
    value.front() = static_cast<char>(std::toupper(static_cast<unsigned char>(value.front())));
  return value;
}

struct NodeEntry {
  std::uint64_t id{};
  capnp::schema::Node::Reader node{};
};

[[nodiscard]] auto FindNode(std::span<const NodeEntry> nodes, std::uint64_t id) noexcept
    -> std::optional<capnp::schema::Node::Reader> {
  const auto it = std::ranges::lower_bound(nodes, id, {}, &NodeEntry::id);
  if (it != nodes.end() && it->id == id) {
    return it->node;
  }
  return std::nullopt;
}

std::string Namespace(capnp::schema::Node::Reader node, std::span<const NodeEntry> nodes) {
  for (const auto annotation : node.getAnnotations()) {
    if (const auto found = FindNode(nodes, annotation.getId()); found.has_value()) {
      if (Text(found->getDisplayName()).ends_with("c++.capnp:namespace")) {
        return Text(annotation.getValue().getText());
      }
    }
  }
  return {};
}

std::string CppName(capnp::List<capnp::schema::Annotation>::Reader annotations,
                    std::string fallback, std::span<const NodeEntry> nodes) {
  for (const auto annotation : annotations) {
    if (const auto found = FindNode(nodes, annotation.getId()); found.has_value()) {
      if (Text(found->getDisplayName()).ends_with("c++.capnp:name")) {
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
  std::vector<std::string> output_names;
  output_names.reserve(inputs.size());
  for (const auto& input : inputs) {
    const auto stem = std::filesystem::path(input).stem().string();
    const auto pos = std::ranges::lower_bound(output_names, stem);
    if (pos != output_names.end() && *pos == stem) {
      fmt::print(stderr, "grpcxx-codegen: contract output name collision for {}\n", input);
      return 1;
    }
    output_names.insert(pos, stem);
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
  std::string foryc;
  if (extension == ".proto") {
    command = {
        protoc, "--cpp_out=" + output.string(), "--grpcxx_out=" + output.string(),
        "--plugin=protoc-gen-grpcxx=" + (executable.parent_path() / "protoc-gen-grpcxx").string(),
        "-I" + parent.string()};
  } else if (extension == ".fdl") {
    foryc = FindTool("FORYC", "foryc");
    command = {foryc, "--lang", "cpp", "--output", output.string(), "-I", parent.string()};
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
    const auto adapter_path = FdlAdapter();
    if (adapter_path.empty()) {
      fmt::print(stderr, "grpcxx-codegen: cannot locate FDL adapter\n");
      return 1;
    }
    std::vector<std::string> adapter{FdlPython(foryc), adapter_path, input.string(),
                                     output.string()};
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
  std::vector<NodeEntry> nodes;
  const auto raw_nodes = request.getNodes();
  nodes.reserve(raw_nodes.size());
  for (const auto node : raw_nodes) {
    nodes.push_back(NodeEntry{.id = node.getId(), .node = node});
  }
  std::ranges::sort(nodes, {}, &NodeEntry::id);
  const auto unique_tail = std::ranges::unique(nodes, {}, &NodeEntry::id);
  nodes.erase(unique_tail.begin(), unique_tail.end());
  for (const auto file : request.getRequestedFiles()) {
    const auto file_node = FindNode(nodes, file.getId());
    if (!file_node.has_value()) return 1;
    for (const auto& [id, node] : nodes) {
      static_cast<void>(id);
      if (!node.isInterface() || node.getScopeId() == file.getId()) continue;
      auto scope = node.getScopeId();
      for (std::size_t depth = 0; scope != 0 && depth < nodes.size(); ++depth) {
        if (scope == file.getId()) {
          fmt::print(stderr, "grpcxx-codegen: nested Capnp interfaces are unsupported: {}\n",
                     Text(node.getDisplayName()));
          return 1;
        }
        const auto parent = FindNode(nodes, scope);
        if (!parent.has_value()) break;
        scope = parent->getScopeId();
      }
    }
    const auto cpp_namespace = Namespace(*file_node, nodes);
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
    for (const auto nested : file_node->getNestedNodes()) {
      const auto found = FindNode(nodes, nested.getId());
      if (!found.has_value() || !found->isInterface()) continue;
      if (found->getInterface().getSuperclasses().size() != 0 ||
          found->getParameters().size() != 0) {
        fmt::print(stderr,
                   "grpcxx-codegen: generic and inherited Capnp interfaces are unsupported\n");
        return 1;
      }
      const auto service_name = Text(nested.getName());
      const auto cpp_service_name = CppName(found->getAnnotations(), service_name, nodes);
      const auto cpp_service_type =
          "::" + cpp_namespace + (cpp_namespace.empty() ? "" : "::") + cpp_service_name;
      generated += fmt::format(
          "struct [[= grpcxx::service{{.name = \"{}{}{}\"}}]] {}Contract;\nstruct {}Contract {{\n",
          wire_namespace, wire_namespace.empty() ? "" : ".", service_name, cpp_service_name,
          cpp_service_name);
      for (const auto method : found->getInterface().getMethods()) {
        bool client_streaming = false;
        bool server_streaming = false;
        for (const auto annotation : method.getAnnotations()) {
          const auto definition = FindNode(nodes, annotation.getId());
          if (!definition.has_value()) continue;
          const auto name = Text(definition->getDisplayName());
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
