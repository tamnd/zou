"""What the extension exports, said in a way a type checker can read.

The package ships a py.typed, and a py.typed next to a compiled module
nothing can introspect is a promise nobody can keep, so this is here.
"""

from typing import Sequence

class ZouError(RuntimeError):
    code: str
    message: str

class Response:
    status: int
    headers: list[tuple[str, str]]
    body: bytes

class Zou:
    anon_key: str
    service_role_key: str
    dsn: str
    target: str
    tenant: str

    def request(
        self,
        method: str,
        path: str,
        headers: Sequence[tuple[str, str]] | None = ...,
        body: bytes | None = ...,
    ) -> Response: ...
    def listen(self, port: int = ...) -> int: ...
    def branch(self, name: str) -> Zou: ...
    def branchable(self) -> bool: ...
    def checkpoint(self) -> None: ...
    def close(self) -> None: ...

def open(
    target: str = ...,
    tenant: str | None = ...,
    pg_bin: str | None = ...,
    runtime: str | None = ...,
    jwt_secret: str | None = ...,
    schemas: list[str] | None = ...,
    shared_buffers: str | None = ...,
    fixture: bool = ...,
) -> Zou: ...
