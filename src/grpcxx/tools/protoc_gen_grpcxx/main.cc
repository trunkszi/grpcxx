#include <google/protobuf/compiler/plugin.h>

#include "contract_generator.h"

int main(int argc, char** argv) {
  const grpcxx::tools::ContractGenerator generator;
  return google::protobuf::compiler::PluginMain(argc, argv, &generator);
}
