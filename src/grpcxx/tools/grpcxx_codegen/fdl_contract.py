"""Emit contracts using Apache Fory's validated AST and C++ name resolver."""

import pathlib
import sys

from fory_compiler.cli import resolve_imports
from fory_compiler.generators.base import GeneratorOptions
from fory_compiler.generators.cpp import CppGenerator
from fory_compiler.ir.validator import SchemaValidator


def main():
    source, destination = map(pathlib.Path, sys.argv[1:3])
    schema = resolve_imports(source, list(map(pathlib.Path, sys.argv[3:])))
    validator = SchemaValidator(schema)
    validator.validate()
    if validator.errors:
        raise ValueError(str(validator.errors))
    generator = CppGenerator(schema, GeneratorOptions(output_dir=destination))
    generated = generator.generate()
    message_header = next(item.path for item in generated if item.path.endswith(".h"))
    lines = ["/** Generated ServiceContract declarations. */", "#pragma once",
             "#include <grpcxx/tools/service_annotations.h>",
             f'#include "cpp/{message_header}"']
    namespace = generator.get_namespace()
    if namespace:
        lines.append(f"namespace {namespace} {{")
    for service in schema.services:
        if generator.is_imported_type(service):
            continue
        name = generator.get_grpc_service_name(service)
        lines.append(f'struct [[= grpcxx::service{{.name = "{name}"}}]] {service.name}Contract {{')
        for method in service.methods:
            kind = {(False, False): "kUnary", (False, True): "kServerStreaming",
                    (True, False): "kClientStreaming", (True, True): "kBidiStreaming"}[
                        method.client_streaming, method.server_streaming]
            request = generator.service_type_path(method.request_type)
            response = generator.service_type_path(method.response_type)
            method_identifier = generator.sanitize_cpp_identifier(method.name)
            if method_identifier == service.name + "Contract":
                method_identifier += "_"
            lines.extend([f'  [[= grpcxx::rpc{{.name = "{method.name}", .kind = grpcxx::RpcKind::{kind}}}]]',
                          f"  {response} {method_identifier}(const {request}&) noexcept;"])
        lines.append("};")
    if namespace:
        lines.append("}")
    (destination / (source.stem + ".grpcxx.h")).write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, StopIteration) as error:
        print(f"grpcxx-codegen: {error}", file=sys.stderr)
        sys.exit(1)
