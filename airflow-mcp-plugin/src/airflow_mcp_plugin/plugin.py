from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp
import anyio
import yaml
from airflow.plugins_manager import AirflowPlugin  # type: ignore[import-not-found]
from mcp import types
from mcp.server.lowlevel import Server
from mcp.server.streamable_http import StreamableHTTPServerTransport
from starlette.requests import Request
from starlette.responses import JSONResponse

from airflow_mcp_plugin.toolset import AirflowOpenAPIToolset

logger = logging.getLogger(__name__)


def _get_airflow_major_version() -> int:
    """Return Airflow major version (2 or 3)."""
    try:
        import airflow  # type: ignore[import-not-found]

        version = getattr(airflow, "__version__", "3.0.0")
        return int(version.split(".")[0])
    except Exception:
        return 3


def _compute_airflow_prefix(request: Request) -> str:
    """Detect deployment path prefix (e.g., Astronomer's '/<deployment>') and drop '/mcp'.

    Prefers 'X-Forwarded-Prefix' header when present, otherwise uses ASGI root_path.
    Ensures the returned prefix does not include the plugin mount ('/mcp').
    """
    forwarded_prefix = request.headers.get("x-forwarded-prefix") or ""
    root_path = request.scope.get("root_path") or ""
    prefix = forwarded_prefix or root_path or ""
    if prefix.endswith("/"):
        prefix = prefix[:-1]
    if prefix.endswith("/mcp"):
        prefix = prefix[: -len("/mcp")] or ""
    return prefix


def _compute_airflow_prefix_from_wsgi(path: str, script_root: str, host_url: str) -> str:
    """Compute Airflow base URL prefix for Flask/WSGI (Airflow 2). Drops /mcp from path."""
    full_path = (script_root or "") + (path or "")
    prefix = full_path.rstrip("/")
    if prefix.endswith("/mcp"):
        prefix = prefix[: -len("/mcp")] or ""
    return prefix


class StatelessMCPMount:
    """ASGI app that serves MCP over Streamable HTTP, backed by Airflow OpenAPI (A2 or A3)."""

    def __init__(self, airflow_version: int = 3) -> None:
        self._airflow_version = airflow_version
        self._openapi_spec: dict[str, Any] | None = None
        self._spec_lock = asyncio.Lock()
        self._toolsets: dict[bool, AirflowOpenAPIToolset] = {}

    def _spec_url(self, base_url: str) -> str:
        if self._airflow_version == 2:
            return f"{base_url.rstrip('/')}/api/v1/openapi.json"
        return f"{base_url.rstrip('/')}/openapi.json"

    def _api_base_url(self, base_url: str) -> str:
        if self._airflow_version == 2:
            return f"{base_url.rstrip('/')}/api/v1"
        return base_url.rstrip("/")

    async def _ensure_openapi_spec(self, base_url: str, token: str) -> dict[str, Any] | None:
        if self._openapi_spec is not None:
            return self._openapi_spec

        async with self._spec_lock:
            if self._openapi_spec is not None:
                return self._openapi_spec

            timeout = aiohttp.ClientTimeout(total=30)
            headers = {"Authorization": f"Bearer {token}"}
            spec_url = self._spec_url(base_url)
            try:
                async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
                    async with session.get(spec_url) as response:
                        response.raise_for_status()
                        content_type = response.headers.get("content-type", "").lower()
                        if "application/json" in content_type:
                            self._openapi_spec = await response.json()
                        else:
                            text = await response.text()
                            self._openapi_spec = yaml.safe_load(text) or {}
                        return self._openapi_spec
            except Exception as exc:
                if self._airflow_version == 2 and "openapi.json" in spec_url:
                    try:
                        yaml_url = spec_url.replace("openapi.json", "openapi.yaml")
                        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
                            async with session.get(yaml_url) as resp:
                                resp.raise_for_status()
                                text = await resp.text()
                                self._openapi_spec = yaml.safe_load(text) or {}
                                return self._openapi_spec
                    except Exception as exc2:
                        logger.error("Failed to fetch OpenAPI spec (yaml fallback): %s", exc2)
                else:
                    logger.error("Failed to fetch OpenAPI spec: %s", exc)
                return None

    def _get_toolset(self, spec: dict[str, Any], allow_mutations: bool) -> AirflowOpenAPIToolset:
        key = allow_mutations
        if key not in self._toolsets:
            self._toolsets[key] = AirflowOpenAPIToolset(spec, allow_mutations)
        return self._toolsets[key]

    def _build_server(
        self, toolset: AirflowOpenAPIToolset, api_base_url: str, token: str
    ) -> Server:
        server = Server(name="Airflow MCP Plugin", version="0.2.0")

        @server.list_tools()
        async def _list_tools(_: types.ListToolsRequest | None = None):
            return toolset.list_tools()

        @server.call_tool()
        async def _call_tool(tool_name: str, arguments: dict[str, Any]):
            return await toolset.call_tool(tool_name, arguments or {}, api_base_url, token)

        return server

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("path") in {None, ""}:
            scope = dict(scope)
            scope["path"] = "/"

        request = Request(scope, receive=receive)

        auth_header = request.headers.get("authorization") or request.headers.get("Authorization")
        if not auth_header or not auth_header.lower().startswith("bearer "):
            response = JSONResponse({"error": "Authorization Bearer token required"}, status_code=401)
            await response(scope, receive, send)
            return

        token = auth_header.split(" ", 1)[1].strip()
        mode_param = (request.query_params.get("mode") or "safe").lower()
        allow_mutations = mode_param == "unsafe"

        url = request.url
        airflow_prefix = _compute_airflow_prefix(request)
        base_url = f"{url.scheme}://{url.netloc}{airflow_prefix}"
        api_base_url = self._api_base_url(base_url)

        spec = await self._ensure_openapi_spec(base_url, token)
        if spec is None:
            response = JSONResponse({"error": "Failed to fetch Airflow OpenAPI spec"}, status_code=502)
            await response(scope, receive, send)
            return

        toolset = self._get_toolset(spec, allow_mutations)
        server = self._build_server(toolset, api_base_url, token)
        transport = StreamableHTTPServerTransport(mcp_session_id=None)
        initialization = server.create_initialization_options()

        async with transport.connect() as (read_stream, write_stream):
            async with anyio.create_task_group() as task_group:
                task_group.start_soon(
                    server.run,
                    read_stream,
                    write_stream,
                    initialization,
                    False,
                    True,
                )
                try:
                    await transport.handle_request(scope, receive, send)
                finally:
                    task_group.cancel_scope.cancel()


def _create_flask_blueprint_for_mcp() -> "Any":
    """Create a Flask Blueprint that bridges WSGI to the ASGI StatelessMCPMount (Airflow 2)."""
    try:
        from flask import Blueprint, request as flask_request, Response  # type: ignore[import-not-found]
    except ImportError:
        raise RuntimeError("Flask is required for Airflow 2; install apache-airflow 2.x") from None

    mount = StatelessMCPMount(airflow_version=2)
    blueprint = Blueprint("airflow_mcp", __name__, url_prefix="/mcp")

    @blueprint.route("/", methods=["GET", "POST", "OPTIONS"])
    def mcp_handler() -> "Any":
        body = flask_request.get_data()
        path = flask_request.path or "/"
        script_root = getattr(flask_request, "script_root", "") or ""
        host_url = (flask_request.host_url or "").rstrip("/")
        prefix = _compute_airflow_prefix_from_wsgi(path, script_root, host_url)
        base_url = host_url + prefix

        scope = {
            "type": "http",
            "method": flask_request.method,
            "path": path,
            "query_string": flask_request.query_string or b"",
            "headers": [
                (k.lower().encode("latin1"), v.encode("latin1"))
                for k, v in flask_request.headers
                if k.lower() != "transfer-encoding"
            ],
            "scheme": flask_request.scheme or "http",
            "server": (flask_request.host or "localhost", flask_request.environ.get("SERVER_PORT", 80)),
            "client": ("127.0.0.1", 0),
            "root_path": script_root,
        }
        response_holder: list[dict[str, Any]] = []
        request_event = {"type": "http.request", "body": body, "more_body": False}

        async def receive() -> dict[str, Any]:
            return request_event

        def send(message: dict[str, Any]) -> None:
            response_holder.append(message)

        async def run_app() -> None:
            await mount(scope, receive, send)

        asyncio.run(run_app())

        if not response_holder:
            return Response("Internal Server Error", status=500)
        start = next((m for m in response_holder if m.get("type") == "http.response.start"), None)
        body_msg = next((m for m in response_holder if m.get("type") == "http.response.body"), None)
        if not start:
            return Response("Internal Server Error", status=500)
        status = start.get("status", 500)
        headers = start.get("headers", [])
        body_bytes = b""
        if body_msg:
            body_bytes = body_msg.get("body", b"")
        resp = Response(body_bytes, status=status)
        for name, value in headers:
            if name.lower() == b"content-type":
                resp.content_type = value.decode("latin1")
            else:
                resp.headers[name.decode("latin1")] = value.decode("latin1")
        return resp

    return blueprint


class AirflowMCPPlugin(AirflowPlugin):
    name = "airflow_mcp_plugin"

    fastapi_apps: list[dict[str, Any]] = []
    flask_blueprints: list[Any] = []


# Set version-specific plugin hooks at import time (Airflow reads class attributes)
_plugin_version = _get_airflow_major_version()
if _plugin_version >= 3:
    AirflowMCPPlugin.fastapi_apps = [
        {"app": StatelessMCPMount(airflow_version=3), "url_prefix": "/mcp", "name": "Airflow MCP"}
    ]
else:
    AirflowMCPPlugin.flask_blueprints = [_create_flask_blueprint_for_mcp()]
