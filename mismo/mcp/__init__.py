"""MCP (Model Context Protocol) server for linkage analysis.

Exposes the ``mismo.analysis`` API as MCP tools so AI agents can
interactively explore and analyze a linkage.

Usage
-----
**Start the server from the command line:**

.. code-block:: bash

    # Load from parquet files
    python -m mismo.mcp --dir /path/to/parquets

**Or start programmatically and connect to it:**

.. code-block:: python

    import mismo
    from mismo.mcp import create_server

    linkage = mismo.Linkage(left=..., right=..., links=...)
    server = create_server(linkage, name="my-linkage")
    server.run()  # stdio transport by default

**Configure in Claude Desktop / Claude Code:**

Add to your MCP config:

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

from mismo.mcp._server import create_server as create_server
