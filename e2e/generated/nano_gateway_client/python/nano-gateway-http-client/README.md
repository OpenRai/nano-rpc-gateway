# Nano Gateway HTTP Client

Nano Gateway Python HTTP client.

## Example Usage

```python
import asyncio

from nano_gateway_client.client import NanoGatewayClient


async def main() -> None:
    # Get an instance of the client.
    client = NanoGatewayClient(headers={})
    # Use client for method calls...


if __name__ == "__main__":
    asyncio.run(main())
```

## Build Installable Tarball Locally

```shell
python3 setupy.py sdist --formats=gztar
```
