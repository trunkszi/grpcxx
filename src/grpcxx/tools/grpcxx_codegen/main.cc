#include <ranges>
#include <string>
#include <vector>

#include "idl_compiler.h"

int main(int argc, char** argv) {
  std::vector<std::string> arguments;
  for (const auto index : std::views::iota(1, argc)) arguments.emplace_back(argv[index]);
  return grpcxx::tools::IdlCompiler{}.Run(arguments);
}
