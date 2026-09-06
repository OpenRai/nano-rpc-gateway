from setuptools import setup

setup(
    name="nano-gateway-client",
    version="0.1.0",
    description="Nano Gateway Python HTTP client.",
    packages=["nano_gateway_http_client"],
    install_requires=[
        "jsonrpc2-pyclient>=5.2.0",
        "py-undefined>=0.1.5",
        "pydantic>=2.5.3"
    ],
    include_package_data=True,
)
