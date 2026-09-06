"""Generate the E2E client from the checked-in OpenRPC artifact."""

from __future__ import annotations

import json
import re
from pathlib import Path

from openrpc import OpenRPC
from openrpcclientgenerator import Language, generate

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "generated" / "nano-node-v28.2.openrpc.json"
OUTPUT = Path(__file__).resolve().parent / "generated" / "nano_gateway_client"


def main() -> None:
    document = json.loads(SOURCE.read_text())
    source_digest = document["x-nano-artifact-sha256"]

    # openrpcclientgenerator 0.51.7 models method errors as inline objects and
    # cannot parse valid OpenRPC $ref error descriptors. Errors do not affect
    # client method generation, so remove only that generator-incompatible
    # metadata while retaining every method, parameter, and result schema.
    for method in document["methods"]:
        method.pop("errors", None)

    openrpc = OpenRPC.model_validate(document)
    generate(openrpc, Language.PYTHON, "http://127.0.0.1:8090/rpc", OUTPUT)
    client_path = next(OUTPUT.glob("python/*/*/client.py"))
    client = client_path.read_text()
    model_path = client_path.with_name("models.py")
    models = model_path.read_text()
    aliases: list[str] = []
    for name, schema in document.get("components", {}).get("schemas", {}).items():
        if schema.get("type") != "array":
            continue
        item = schema.get("items", {})
        item_type = item.get("$ref", "").rsplit("/", 1)[-1] or "Any"
        aliases.append(f"{name} = list[{item_type}]\n")
    if aliases:
        model_path.write_text(models.rstrip() + "\n\n" + "\n".join(aliases))
        models = model_path.read_text()
    model_names = set(re.findall(r"^class (\w+)\(", models, re.MULTILINE))
    model_names.update(re.findall(r"^(\w+) = ", models, re.MULTILINE))

    # The generator imports every schema, but only emits model classes for
    # schemas with properties. Keep only imports backed by generated classes;
    # the generated method signatures still retain every schema-derived type.
    def keep_generated_models(match: re.Match[str]) -> str:
        names = re.findall(r"    (\w+),", match.group(1))
        import_end = match.end()
        client_body = client[: match.start()] + client[import_end:]
        kept = "\n".join(
            f"    {name},"
            for name in names
            if name in model_names and re.search(rf"\b{re.escape(name)}\b", client_body)
        )
        return f"from nano_gateway_http_client.models import (\n{kept}\n)"

    client_path.write_text(
        re.sub(
            r"from nano_gateway_http_client\.models import \((.*?)\n\)",
            keep_generated_models,
            client,
            flags=re.DOTALL,
        )
    )
    (OUTPUT / "source-digest.txt").write_text(f"{source_digest}\n")


if __name__ == "__main__":
    main()
