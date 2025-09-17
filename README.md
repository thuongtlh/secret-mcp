# Git Modified Files MCP Server (Python)

This project implements a Model Context Protocol (MCP) server in Python that
surfaces git-modified files to clients such as GitHub Copilot. The server makes
both workspace and staged snapshots of each modified file available so a client
can fetch exactly the content it needs while you iterate on code.

## Requirements

- Python 3.10 or newer
- Git available on the `PATH`

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

You can use any virtual environment manager you prefer; the commands above show
one option using the Python standard library.

## Running the server

The server uses the MCP stdio transport. Launch it inside the git repository you
want to expose:

```bash
python src/server.py
```

The process watches `git status` every two seconds and automatically notifies
connected clients when the set of modified files changes.

## Connecting from GitHub Copilot

Create (or edit) an MCP agent definition under
`~/.config/github-copilot/agents/git-modified-files.json` with contents similar
to the following, then restart Copilot or reload your editor:

```json
{
  "name": "git-modified-files",
  "command": "python",
  "args": ["src/server.py"],
  "cwd": "/absolute/path/to/your/repository"
}
```

Adjust `cwd` to point at the repository you want Copilot to inspect. Copilot
will launch the command above and communicate with the server over stdio using
the MCP protocol.

## MCP surface area

### Resources

- `git-modified://file/{variant}/{encodedPath}` – exposes the workspace
  (`variant` of `workspace`) or staged (`variant` of `staged`) snapshot for a
  git-modified file. The `encodedPath` component is URL-encoded. Listing this
  template yields one resource per available snapshot, and each resource carries
  metadata describing the underlying git status.

### Tools

- `list_git_modified_files` – returns a human-readable summary of every file
  reported by `git status`, including the MCP resource URIs for each snapshot.
- `read_git_modified_file` – accepts a `path` and optional `variant`
  (`workspace` or `staged`, defaulting to `workspace`) and returns the file
  contents through a tool call.

### Logging

The server emits MCP logging notifications that describe startup, shutdown, git
status polling errors, and other runtime events. Clients that surface these
messages can provide immediate feedback when, for example, the process is not
running inside a git repository.

## Development notes

- The polling interval defaults to two seconds; adjust `DEFAULT_POLL_INTERVAL`
  in `src/server.py` if you need a different cadence.
- File reads are validated to ensure paths stay within the active repository
  before accessing the workspace or staged index.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) if the
file is present in your clone for additional details.
