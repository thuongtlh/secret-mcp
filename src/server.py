#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import quote, unquote

import anyio
import mcp.types as types
from anyio import get_cancelled_exc_class
from anyio import run_process
from contextlib import AsyncExitStack
from mcp.server import NotificationOptions, Server
from mcp.server.lowlevel.helper_types import ReadResourceContents
from mcp.server.session import ServerSession
from mcp.server.stdio import stdio_server

VERSION = "1.0.0"
LOGGER_NAME = "git-modified-files"
RESOURCE_SCHEME = "git-modified"
RESOURCE_TEMPLATE = "git-modified://file/{variant}/{encodedPath}"
DEFAULT_POLL_INTERVAL = 2.0


@dataclass(slots=True)
class GitFileRecord:
    path: str
    original_path: Optional[str]
    index_status: str
    worktree_status: str
    status: str
    is_untracked: bool
    has_staged_changes: bool
    has_workspace_changes: bool
    summary: str


class GitModifiedServer:
    def __init__(self, poll_interval: float = DEFAULT_POLL_INTERVAL) -> None:
        self.poll_interval = poll_interval
        self.repo_root = self._detect_repo_root()
        self.repo_available = self.repo_root is not None
        self.working_directory = self.repo_root or Path.cwd()

        self.server = Server(
            name="git-modified-files",
            version=VERSION,
        )

        self.server.list_resources()(self._list_resources)
        self.server.list_resource_templates()(self._list_resource_templates)
        self.server.read_resource()(self._read_resource)
        self.server.list_tools()(self._list_tools)
        self.server.call_tool()(self._call_tool)

        self._log_queue: list[tuple[str, Any, Optional[str]]] = []
        self._session: ServerSession | None = None
        self._cached_files: list[GitFileRecord] = []
        self._last_signature: Optional[str] = None
        self._last_git_error: Optional[str] = None
        self._cache_lock = anyio.Lock()

        if self.repo_available:
            self.queue_log(
                "info",
                {"message": "Monitoring git repository", "root": str(self.working_directory)},
            )
        else:
            self.queue_log(
                "warning",
                {
                    "message": "No git repository detected. Resources and tools will remain empty until git is available.",
                    "cwd": str(Path.cwd()),
                },
            )

    def queue_log(self, level: str, data: Any, logger: Optional[str] = LOGGER_NAME) -> None:
        self._log_queue.append((level, data, logger))

    async def _send_log(self, level: str, data: Any, logger: Optional[str] = LOGGER_NAME) -> None:
        session = self._session
        if session is None:
            self.queue_log(level, data, logger)
            return
        try:
            await session.send_log_message(level, data, logger)
        except Exception:
            self.queue_log(level, data, logger)

    async def flush_logs(self) -> None:
        if not self._log_queue:
            return
        pending = list(self._log_queue)
        self._log_queue.clear()
        for level, data, logger in pending:
            await self._send_log(level, data, logger)

    async def serve(self) -> None:
        init_options = self.server.create_initialization_options(
            NotificationOptions(resources_changed=True)
        )

        await self._update_cache(notify=False, session=None)

        async with stdio_server() as (read_stream, write_stream):
            async with AsyncExitStack() as stack:
                lifespan_context = await stack.enter_async_context(self.server.lifespan(self.server))
                session = await stack.enter_async_context(
                    ServerSession(read_stream, write_stream, init_options)
                )
                self._session = session

                await self.flush_logs()
                await self._update_cache(notify=False, session=session)

                async with anyio.create_task_group() as task_group:
                    if self.repo_available and self.poll_interval > 0:
                        task_group.start_soon(self._poll_git_status, session)

                    async for message in session.incoming_messages:
                        task_group.start_soon(
                            self.server._handle_message,
                            message,
                            session,
                            lifespan_context,
                            False,
                        )
        self._session = None

    async def _poll_git_status(self, session: ServerSession) -> None:
        try:
            while True:
                await anyio.sleep(self.poll_interval)
                await self._update_cache(notify=True, session=session)
        except get_cancelled_exc_class():
            return
        except Exception as error:
            await self._send_log(
                "error",
                {
                    "message": "Polling git status failed",
                    "details": self._to_loggable_error(error),
                },
            )

    async def _update_cache(self, *, notify: bool, session: ServerSession | None) -> list[GitFileRecord]:
        async with self._cache_lock:
            if not self.repo_available:
                self._cached_files = []
                self._last_signature = "NO_REPO"
                return self._cached_files

            try:
                stdout = await self._run_git_status()
            except Exception as error:
                await self._handle_git_error(error)
                return self._cached_files

            files = self._parse_porcelain(stdout)
            signature = self._compute_signature(files)
            changed = signature != self._last_signature
            self._cached_files = files
            self._last_signature = signature
            self._last_git_error = None

            if notify and changed and session is not None:
                await session.send_resource_list_changed()

            return self._cached_files

    async def _handle_git_error(self, error: Exception) -> None:
        details = self._to_loggable_error(error)
        message = details.get("message")
        if message and message == self._last_git_error:
            return
        self._last_git_error = message
        await self._send_log(
            "error",
            {"message": "Failed to read git status", "details": details},
        )

    async def _run_git_status(self) -> str:
        result = await run_process(
            ["git", "status", "--porcelain=v1", "-z", "--untracked-files=all"],
            cwd=str(self.working_directory),
            check=False,
        )
        if result.returncode != 0:
            stderr = (result.stderr or b"").decode("utf-8", errors="replace")
            raise RuntimeError(f"git status failed: {stderr.strip() or result.returncode}")
        stdout = (result.stdout or b"").decode("utf-8", errors="replace")
        return stdout

    def _parse_porcelain(self, raw: str) -> list[GitFileRecord]:
        entries = raw.split("\0")
        files: list[GitFileRecord] = []
        i = 0
        while i < len(entries):
            entry = entries[i]
            i += 1
            if not entry:
                continue

            status = entry[:2]
            if status == "!!":
                continue

            index_status = status[0]
            worktree_status = status[1]
            raw_path = entry[3:]

            original_path: Optional[str] = None
            if index_status in {"R", "C"} and i < len(entries):
                original_path = entries[i] or None
                i += 1

            is_untracked = status == "??"
            has_staged_changes = not is_untracked and index_status not in {" ", "?"}
            has_workspace_changes = is_untracked or worktree_status not in {" ", "?"}

            record = GitFileRecord(
                path=raw_path,
                original_path=original_path,
                index_status=index_status,
                worktree_status=worktree_status,
                status=status,
                is_untracked=is_untracked,
                has_staged_changes=has_staged_changes,
                has_workspace_changes=has_workspace_changes,
                summary="",
            )
            record.summary = self._build_summary(record)
            files.append(record)
        return files

    @staticmethod
    def _compute_signature(files: Iterable[GitFileRecord]) -> str:
        payload = [
            {
                "path": record.path,
                "index": record.index_status,
                "worktree": record.worktree_status,
                "original": record.original_path,
            }
            for record in files
        ]
        return json.dumps(payload, sort_keys=True)

    def _build_summary(self, record: GitFileRecord) -> str:
        if record.is_untracked:
            return "Untracked file"

        parts: list[str] = []
        if record.has_staged_changes:
            detail = self._describe_status_char(record.index_status)
            if record.index_status == "R" and record.original_path:
                detail += f" from {record.original_path}"
            parts.append(f"Staged {detail}")
        if record.has_workspace_changes:
            detail = self._describe_status_char(record.worktree_status)
            parts.append(f"Workspace {detail}")
        if not parts:
            parts.append("No pending changes")
        return "; ".join(parts)

    @staticmethod
    def _describe_status_char(char: str) -> str:
        meanings = {
            "M": "modified",
            "A": "added",
            "D": "deleted",
            "R": "renamed",
            "C": "copied",
            "U": "unmerged",
            "?": "untracked",
            "!": "ignored",
            "T": "type changed",
            "B": "broken",
            " ": "clean",
        }
        return meanings.get(char, "changed")

    def _build_variant_description(self, record: GitFileRecord, variant: str) -> str:
        if variant == "workspace":
            if record.is_untracked:
                return "Untracked workspace file"
            if record.worktree_status == "D":
                return "Workspace deletion"
            if record.worktree_status == " " and record.has_staged_changes:
                return "Workspace matches HEAD"
            return f"Workspace {self._describe_status_char(record.worktree_status)}"
        if not record.has_staged_changes:
            return "No staged changes"
        if record.index_status == "D":
            return "Staged deletion"
        return f"Staged {self._describe_status_char(record.index_status)}"

    async def _list_resources(self) -> list[types.Resource]:
        await self._update_cache(notify=False, session=self.server.request_context.session)
        resources: list[types.Resource] = []
        for record in sorted(self._cached_files, key=lambda item: item.path):
            resources.extend(self._resource_entries_for_record(record))
        return resources

    async def _list_resource_templates(self) -> list[types.ResourceTemplate]:
        return [
            types.ResourceTemplate(
                name="git-modified-file",
                uriTemplate=RESOURCE_TEMPLATE,
                description="Workspace or staged snapshots for files reported by git status.",
                mimeType="text/plain",
            )
        ]

    def _resource_entries_for_record(self, record: GitFileRecord) -> list[types.Resource]:
        entries: list[types.Resource] = []
        if record.has_workspace_changes or record.is_untracked:
            entries.append(self._create_resource(record, "workspace"))
        if record.has_staged_changes:
            entries.append(self._create_resource(record, "staged"))
        return entries

    def _create_resource(self, record: GitFileRecord, variant: str) -> types.Resource:
        uri = self._build_resource_uri(variant, record.path)
        title = f"{variant.title()} • {record.path}"
        description = self._build_variant_description(record, variant)
        return types.Resource(
            name=f"{variant}:{record.path}",
            uri=uri,
            title=title,
            description=description,
            mimeType="text/plain",
            _meta={
                "variant": variant,
                "path": record.path,
                "originalPath": record.original_path,
                "status": {
                    "index": record.index_status,
                    "worktree": record.worktree_status,
                    "summary": record.summary,
                },
            },
        )

    async def _read_resource(self, uri: str) -> Iterable[ReadResourceContents]:
        parsed = self._parse_resource_uri(uri)
        if not parsed:
            return [ReadResourceContents(content=f"Unsupported resource URI: {uri}")]
        variant, path = parsed
        record = next((item for item in self._cached_files if item.path == path), None)
        if record is None:
            await self._update_cache(notify=False, session=self.server.request_context.session)
            record = next((item for item in self._cached_files if item.path == path), None)
        if record is None:
            return [ReadResourceContents(content=f"{path} is not currently reported as modified.")]

        result = await self._load_file_variant(record, variant)
        if not result[0]:
            return [ReadResourceContents(content=result[1])]

        return [ReadResourceContents(content=result[1], mime_type="text/plain")]

    def _parse_resource_uri(self, uri: str) -> Optional[tuple[str, str]]:
        if not uri.startswith(f"{RESOURCE_SCHEME}://"):
            return None
        try:
            _, rest = uri.split("://", 1)
            parts = rest.split("/", 2)
            if len(parts) != 3:
                return None
            if parts[0] != "file":
                return None
            variant = parts[1]
            path = unquote(parts[2])
            if variant not in {"workspace", "staged"}:
                return None
            return variant, path
        except ValueError:
            return None

    async def _list_tools(self) -> list[types.Tool]:
        return [
            types.Tool(
                name="list_git_modified_files",
                title="List git-modified files",
                description="Summarize staged, unstaged, and untracked files reported by git status.",
                inputSchema={"type": "object", "properties": {}, "additionalProperties": False},
            ),
            types.Tool(
                name="read_git_modified_file",
                title="Read a git-modified file",
                description="Return the contents of a modified file from the workspace or staged index.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "variant": {
                            "type": "string",
                            "enum": ["workspace", "staged"],
                            "default": "workspace",
                        },
                    },
                    "required": ["path"],
                    "additionalProperties": False,
                },
            ),
        ]

    async def _call_tool(self, name: str, arguments: dict[str, Any]) -> Iterable[types.TextContent]:
        if name == "list_git_modified_files":
            return await self._tool_list_modified()
        if name == "read_git_modified_file":
            return await self._tool_read_file(arguments)
        raise ValueError(f"Unknown tool: {name}")

    async def _tool_list_modified(self) -> Iterable[types.TextContent]:
        if not self.repo_available:
            return [types.TextContent(type="text", text="No git repository detected.")]

        await self._update_cache(notify=False, session=self.server.request_context.session)
        if not self._cached_files:
            return [types.TextContent(type="text", text="Working tree is clean.")]

        lines = [self._format_file_summary(record) for record in sorted(self._cached_files, key=lambda item: item.path)]
        return [types.TextContent(type="text", text="\n\n".join(lines))]

    async def _tool_read_file(self, arguments: dict[str, Any]) -> Iterable[types.TextContent]:
        if not self.repo_available:
            return [types.TextContent(type="text", text="No git repository detected.")]

        path = arguments.get("path")
        if not isinstance(path, str) or not path:
            return [types.TextContent(type="text", text="Parameter 'path' must be a non-empty string.")]
        variant = arguments.get("variant", "workspace")
        if variant not in {"workspace", "staged"}:
            return [types.TextContent(type="text", text="Variant must be 'workspace' or 'staged'.")]

        await self._update_cache(notify=False, session=self.server.request_context.session)
        record = next((item for item in self._cached_files if item.path == path), None)
        if record is None:
            return [types.TextContent(type="text", text=f"{path} is not currently reported as modified.")]

        ok, message = await self._load_file_variant(record, variant)
        header = f"{variant} • {path}"
        if not ok:
            return [types.TextContent(type="text", text=f"{header}\n{message}")]
        return [types.TextContent(type="text", text=f"{header}\n\n{message}")]

    async def _load_file_variant(self, record: GitFileRecord, variant: str) -> tuple[bool, str]:
        if variant == "workspace":
            if not record.has_workspace_changes and not record.is_untracked:
                return False, f"No workspace changes detected for {record.path}."
            if record.worktree_status == "D":
                return False, f"{record.path} has been deleted in the workspace."
            try:
                text = await self._read_workspace_file(record.path)
                return True, text
            except Exception as error:
                details = self._to_loggable_error(error)
                return False, f"Unable to read {record.path} from the workspace: {details.get('message', 'Unknown error')}"
        if variant == "staged":
            if not record.has_staged_changes:
                return False, f"No staged changes detected for {record.path}."
            if record.index_status == "D":
                return False, f"{record.path} is staged for deletion."
            try:
                text = await self._read_staged_file(record.path)
                return True, text
            except Exception as error:
                details = self._to_loggable_error(error)
                return False, f"Unable to read {record.path} from the index: {details.get('message', 'Unknown error')}"
        return False, f"Unsupported variant '{variant}'."

    async def _read_workspace_file(self, file_path: str) -> str:
        absolute = self._resolve_path_within_repo(file_path)
        async with await anyio.open_file(absolute, "r", encoding="utf-8") as handle:
            return await handle.read()

    async def _read_staged_file(self, file_path: str) -> str:
        self._resolve_path_within_repo(file_path)
        result = await run_process(
            ["git", "show", f":{file_path}"],
            cwd=str(self.working_directory),
            check=False,
        )
        if result.returncode != 0:
            stderr = (result.stderr or b"").decode("utf-8", errors="replace")
            raise RuntimeError(stderr.strip() or f"git show exited with {result.returncode}")
        return (result.stdout or b"").decode("utf-8", errors="replace")

    def _resolve_path_within_repo(self, file_path: str) -> Path:
        absolute = (self.working_directory / file_path).resolve()
        try:
            absolute.relative_to(self.working_directory)
        except ValueError as error:
            raise ValueError(f"Resolved path escapes the repository: {file_path}") from error
        return absolute

    def _format_file_summary(self, record: GitFileRecord) -> str:
        segments = [f"{record.status} {record.path}"]
        if record.original_path:
            segments.append(f"(from {record.original_path})")
        segments.append(f"→ {record.summary}")

        resource_pointers = []
        if record.has_workspace_changes or record.is_untracked:
            resource_pointers.append(f"workspace: {self._build_resource_uri('workspace', record.path)}")
        if record.has_staged_changes:
            resource_pointers.append(f"staged: {self._build_resource_uri('staged', record.path)}")
        if resource_pointers:
            segments.append(" | ".join(resource_pointers))
        return "\n  ".join(segments)

    @staticmethod
    def _build_resource_uri(variant: str, path: str) -> str:
        return RESOURCE_TEMPLATE.format(variant=variant, encodedPath=quote(path))

    @staticmethod
    def _to_loggable_error(error: Exception) -> dict[str, Any]:
        if isinstance(error, Exception):
            message = str(error)
            data: dict[str, Any] = {"message": message}
            if hasattr(error, "stderr"):
                data["stderr"] = getattr(error, "stderr")
            if hasattr(error, "stdout"):
                data["stdout"] = getattr(error, "stdout")
            return data
        return {"message": repr(error)}

    @staticmethod
    def _detect_repo_root() -> Optional[Path]:
        try:
            completed = subprocess.run(
                ["git", "rev-parse", "--show-toplevel"],
                check=True,
                capture_output=True,
                text=True,
            )
        except Exception:
            return None
        return Path(completed.stdout.strip()) if completed.stdout.strip() else None


def main() -> None:
    server = GitModifiedServer()
    try:
        anyio.run(server.serve)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
