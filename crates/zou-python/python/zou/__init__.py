"""A whole Supabase compatible project inside a python process.

The extension moves bytes: a method, a path, headers, a body, and what
came back. This turns that into an httpx transport, because a transport
is what supabase-py takes, and a supabase client built on it is talking
to a database in this process without a socket under any of it.

    from zou import create_fixture

    zou = create_fixture()
    supabase = zou.client()
    supabase.table("todos").select("*").eq("done", False).execute()
    zou.close()
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from . import _zou
from ._zou import Response, ZouError

# supabase-py builds urls out of a base, so there has to be one even when
# nothing is listening. Nothing resolves this name and nothing needs to:
# the origin is stripped off again on the way in.
ORIGIN = "http://zou.embedded"

__all__ = ["create_zou", "create_fixture", "Zou", "Response", "ZouError", "ORIGIN"]


def create_zou(
    dir: str | os.PathLike[str] | None = None,
    *,
    url: str | None = None,
    tenant: str | None = None,
    pg_bin: str | os.PathLike[str] | None = None,
    runtime: str | os.PathLike[str] | None = None,
    jwt_secret: str | None = None,
    schemas: Sequence[str] | None = None,
    shared_buffers: str | None = None,
    fixture: bool = False,
) -> "Zou":
    """Open a project.

    ``dir`` is a directory of objects, ``url`` a bucket or a sqlite file
    or a .zou file, and neither is ephemeral: a store of this handle's
    own that goes away when it closes.

    ``fixture=True`` is the fast one, see :func:`create_fixture`.
    """
    if dir is not None and url is not None:
        raise ValueError("dir and url are two ways of saying the same thing, pick one")
    target = url if url is not None else ("" if dir is None else os.fspath(dir))
    handle = _zou.open(
        target=target,
        tenant=tenant,
        pg_bin=os.fspath(pg_bin) if pg_bin is not None else os.environ.get("ZOU_PG_BIN"),
        runtime=os.fspath(runtime) if runtime is not None else None,
        jwt_secret=jwt_secret,
        schemas=list(schemas) if schemas is not None else None,
        shared_buffers=shared_buffers,
        fixture=fixture,
    )
    return Zou(handle)


def create_fixture(**options: Any) -> "Zou":
    """A database per test, in the time a test can afford.

    An ephemeral project runs initdb, which is seconds. This branches a
    template the machine already built instead, so what is left is the
    postmaster start and it is tens of milliseconds. The first call on a
    cold machine builds the template and pays the initdb once, and
    ``ZOU_TEMPLATE_CACHE`` says where it goes if a CI job wants to cache
    the directory.

    Fixtures see nothing of each other and closing one takes it off the
    store, so a setUp and a tearDown is the whole story.
    """
    options["fixture"] = True
    return create_zou(**options)


class Zou:
    """One open project."""

    def __init__(self, handle: Any) -> None:
        self._handle = handle
        self._port: int | None = None

    @property
    def anon_key(self) -> str:
        """The key a browser gets."""
        return self._handle.anon_key

    @property
    def service_role_key(self) -> str:
        """The key that skips row level security. Keep it on the server."""
        return self._handle.service_role_key

    @property
    def dsn(self) -> str:
        """For psql, or for psycopg, on the database directly."""
        return self._handle.dsn

    @property
    def target(self) -> str:
        """The store everything durable is on."""
        return self._handle.target

    @property
    def tenant(self) -> str:
        """The database inside it."""
        return self._handle.tenant

    @property
    def url(self) -> str:
        """Where this project is, for something that wants a url.

        It is the port once :meth:`listen` has been called and a name
        that resolves nowhere before that, since before that there is
        nothing to resolve.
        """
        return ORIGIN if self._port is None else f"http://127.0.0.1:{self._port}"

    def request(
        self,
        method: str,
        path: str,
        headers: Mapping[str, str] | Iterable[tuple[str, str]] | None = None,
        body: bytes | None = None,
    ) -> Response:
        """Answer one request, in this process. No socket, no port.

        Headers are pairs rather than a dict when a header may be there
        more than once, since a dict quietly keeps the last one.
        """
        if isinstance(headers, Mapping):
            pairs = list(headers.items())
        else:
            pairs = list(headers or [])
        return self._handle.request(method, path, pairs, body)

    def client(self, key: str | None = None, options: Any = None) -> Any:
        """A supabase-py client, wired to answer in this process.

        The key is the anon one unless you ask for the other, so a test
        that means to skip row level security has to say so in the same
        place it would against a hosted project.
        """
        try:
            from supabase import create_client
            from supabase.lib.client_options import SyncClientOptions
        except ImportError as e:  # pragma: no cover, it is the message that matters
            raise ImportError(
                "client() needs supabase, install it alongside zou, or use request() or listen() instead"
            ) from e

        options = options or SyncClientOptions()
        options.httpx_client = self.httpx_client()
        options.auto_refresh_token = False
        options.persist_session = False
        return create_client(self.url, key or self.anon_key, options)

    def httpx_client(self, **kwargs: Any) -> Any:
        """An ``httpx.Client`` whose requests are answered in process.

        This is the layer under :meth:`client`, for anything that takes
        an httpx client of its own.
        """
        import httpx

        return httpx.Client(transport=self.transport(), base_url=self.url, **kwargs)

    def transport(self) -> Any:
        """The httpx transport itself, for a client somebody else built."""
        import httpx

        handle = self

        class ZouTransport(httpx.BaseTransport):
            def handle_request(self, request: Any) -> Any:
                answer = handle.request(
                    request.method,
                    request.url.raw_path.decode("ascii"),
                    list(request.headers.multi_items()),
                    request.read() or None,
                )
                # A 204 or a 304 with a body in it is an error in every
                # http client rather than an empty answer, which is a
                # thing to know before it happens.
                empty = answer.status in (204, 205, 304)
                return httpx.Response(
                    answer.status,
                    headers=answer.headers,
                    content=b"" if empty else answer.body,
                    request=request,
                )

        return ZouTransport()

    def listen(self, port: int = 0) -> int:
        """Put the same front door on a port as well, and say which one.

        No port, or 0, asks the kernel for one.
        """
        self._port = self._handle.listen(port)
        return self._port

    def branch(self, name: str) -> "Zou":
        """A copy on write branch, open and ready, as a second project to
        close like the first."""
        return Zou(self._handle.branch(name))

    def branchable(self) -> bool:
        """Whether a branch of this database would serve yet."""
        return self._handle.branchable()

    def checkpoint(self) -> None:
        """Push everything committed so far into the store."""
        self._handle.checkpoint()

    def close(self) -> None:
        """Stop postgres and remove the running copy.

        Calling it twice is fine, which is what makes it safe in a
        tearDown that may or may not be the one that got there first.
        """
        self._handle.close()

    def __enter__(self) -> "Zou":
        return self

    def __exit__(self, *exception: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"<zou.Zou {self.tenant} on {self.target}>"
