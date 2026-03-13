"""CLI entry point for the mismo MCP server.

Usage
-----
.. code-block:: bash

    # Load linkage from parquet files and serve via stdio (for MCP clients)
    python -m mismo.mcp --dir /path/to/parquets

    # Use a custom server name
    python -m mismo.mcp --dir /path/to/parquets --name "my-linkage"

    # Use a specific ibis backend (default: duckdb)
    python -m mismo.mcp --dir /path/to/parquets --backend duckdb

    # Serve over HTTP for testing (not stdio)
    python -m mismo.mcp --dir /path/to/parquets --transport streamable-http --port 8000

Configure in Claude Desktop's claude_desktop_config.json:

.. code-block:: json

    {
        "mcpServers": {
            "linkage": {
                "command": "python",
                "args": ["-m", "mismo.mcp", "--dir", "/path/to/parquets"]
            }
        }
    }

Configure in Claude Code (~/.claude.json or project .claude.json):

.. code-block:: json

    {
        "mcpServers": {
            "linkage": {
                "command": "python",
                "args": ["-m", "mismo.mcp", "--dir", "/path/to/parquets"]
            }
        }
    }
"""

from __future__ import annotations

import argparse
import sys


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m mismo.mcp",
        description="Start an MCP server for analyzing a mismo Linkage.",
    )
    parser.add_argument(
        "--dir",
        required=True,
        metavar="PATH",
        help=(
            "Directory containing left.parquet, right.parquet, links.parquet "
            "(written by Linkage.to_parquets())"
        ),
    )
    parser.add_argument(
        "--name",
        default="mismo-linkage",
        help="Display name of the MCP server (default: mismo-linkage)",
    )
    parser.add_argument(
        "--backend",
        default="duckdb",
        choices=["duckdb", "sqlite", "pandas"],
        help="Ibis backend to use for reading parquets (default: duckdb)",
    )
    parser.add_argument(
        "--transport",
        default="stdio",
        choices=["stdio", "streamable-http"],
        help="MCP transport to use (default: stdio)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port for HTTP transport (default: 8000)",
    )

    args = parser.parse_args()

    # Load the linkage
    try:
        import ibis

        if args.backend == "duckdb":
            backend = ibis.duckdb.connect()
        elif args.backend == "sqlite":
            backend = ibis.sqlite.connect()
        else:
            backend = ibis.pandas.connect({})

        from mismo.linkage._linkage import Linkage

        linkage = Linkage.from_parquets(args.dir, backend=backend)
    except Exception as e:
        print(f"Error loading linkage from {args.dir!r}: {e}", file=sys.stderr)
        sys.exit(1)

    # Create and run the server
    from mismo.mcp._server import create_server

    server = create_server(linkage, name=args.name)

    if args.transport == "stdio":
        server.run(transport="stdio")
    else:
        server.run(transport="streamable-http", port=args.port)


if __name__ == "__main__":
    main()
