"""MCP server that exposes mismo.analysis as tools for AI agents.

Design principles
-----------------
- Every tool returns JSON-serializable Python objects (dicts/lists)
- Tables are sampled/limited so responses stay small
- Chart specs are returned as JSON strings (full Vega-Lite spec)
- Errors surface as clear messages, not tracebacks
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from mcp.server.fastmcp import FastMCP

from mismo.linkage._linkage import Linkage


def create_server(linkage: Linkage, *, name: str = "mismo-linkage") -> FastMCP:
    """Create a FastMCP server that wraps a Linkage for AI agent interaction.

    Parameters
    ----------
    linkage
        The linkage to analyze. Loaded once at server creation time.
    name
        Display name of the MCP server.

    Returns
    -------
    A configured FastMCP instance ready to run.

    Examples
    --------
    >>> import mismo
    >>> from mismo.mcp import create_server
    >>> linkage = mismo.Linkage(left=left, right=right, links=links)
    >>> server = create_server(linkage, name="my-project-linkage")
    >>> server.run()  # serves via stdio by default
    """
    mcp = FastMCP(name)

    # ------------------------------------------------------------------
    # Helper: serialize ibis table rows
    # ------------------------------------------------------------------

    def _rows(table: Any, limit: int = 100) -> list[dict]:
        """Execute an ibis table and return as a list of plain dicts."""
        import pandas as pd

        df = table.limit(limit).execute()
        # Convert numpy/pandas scalars to plain Python types
        result = []
        for _, row in df.iterrows():
            d = {}
            for k, v in row.items():
                if hasattr(v, "item"):
                    v = v.item()
                elif isinstance(v, float) and v != v:  # NaN
                    v = None
                d[k] = v
            result.append(d)
        return result

    # ------------------------------------------------------------------
    # Tools: overview
    # ------------------------------------------------------------------

    @mcp.tool()
    def get_summary() -> dict[str, Any]:
        """Get scalar summary statistics about the linkage.

        Returns counts and rates for left table, right table, and links:
        - n_left / n_right: total records on each side
        - n_links: total link pairs
        - left_coverage / right_coverage: fraction of records that are linked
        - left_unlinked / right_unlinked: count of records with 0 links
        - left_multiply_linked / right_multiply_linked: count with ≥2 links
        - left_avg_links / right_avg_links: mean links per record
        - left_max_links / right_max_links: max links on any single record
        """
        from mismo import analysis

        return analysis.summary(linkage)

    @mcp.tool()
    def list_columns() -> dict[str, list[str]]:
        """List columns available on left table, right table, and links table.

        Use this to know what column names you can pass to other tools.
        """
        return {
            "left": list(linkage.left.columns),
            "right": list(linkage.right.columns),
            "links": list(linkage.links.columns),
        }

    # ------------------------------------------------------------------
    # Tools: record-level analysis
    # ------------------------------------------------------------------

    @mcp.tool()
    def get_link_distribution(
        side: Annotated[str, "Which table to analyze: 'left' or 'right'"] = "left",
    ) -> list[dict[str, Any]]:
        """Get the distribution of link counts across records.

        Returns a list of {n_links, n_records, pct} showing how many
        records have 0 links, 1 link, 2 links, etc.

        This is the data behind the link_count bar chart.
        """
        table = linkage.left if side == "left" else linkage.right
        counts = table.link_counts().order_by("n_links").execute()
        total = int(counts["n_records"].sum())
        result = []
        for _, row in counts.iterrows():
            n = int(row["n_records"])
            result.append(
                {
                    "n_links": int(row["n_links"]),
                    "n_records": n,
                    "pct": round(n / total * 100, 1) if total > 0 else 0,
                }
            )
        return result

    @mcp.tool()
    def sample_unlinked_records(
        side: Annotated[str, "Which table: 'left' or 'right'"] = "left",
        n: Annotated[int, "Max number of records to return (default 20)"] = 20,
    ) -> list[dict[str, Any]]:
        """Sample records that have zero links — the 'misses'.

        These records were not matched to anything on the other side.
        Inspecting them can reveal why the linker failed (e.g. missing
        data, typos, unusual formats).
        """
        from mismo import analysis

        return _rows(analysis.unlinked(linkage, side), limit=n)

    @mcp.tool()
    def sample_multiply_linked_records(
        side: Annotated[str, "Which table: 'left' or 'right'"] = "left",
        n: Annotated[int, "Max number of records to return (default 20)"] = 20,
        min_links: Annotated[int, "Minimum link count to qualify (default 2)"] = 2,
    ) -> list[dict[str, Any]]:
        """Sample records that matched multiple counterparts (ambiguous).

        These records may indicate:
        - Duplicates in the other table
        - Blocking/matching criteria that are too broad
        - Genuine one-to-many relationships
        """
        from mismo import analysis

        return _rows(analysis.multiply_linked(linkage, side, min_links=min_links), limit=n)

    @mcp.tool()
    def sample_linked_pairs(
        n: Annotated[int, "Max number of pairs to return (default 20)"] = 20,
        category: Annotated[
            str,
            "Filter: 'all' (default), 'singly_linked', or 'multiply_linked'",
        ] = "all",
    ) -> list[dict[str, Any]]:
        """Sample linked record pairs showing fields from both sides.

        Columns are suffixed _l (left) and _r (right). Inspect these to
        see what kinds of records are being matched together, and whether
        the matches look correct.
        """
        from mismo import analysis

        return _rows(
            analysis.sample_pairs(linkage, n=n, category=category),  # type: ignore[arg-type]
            limit=n,
        )

    @mcp.tool()
    def get_link_attribute_counts(
        column: Annotated[str, "Column name on the links table to group by"],
    ) -> list[dict[str, Any]]:
        """Count links grouped by a categorical column on the links table.

        Useful when links have labels like match_level, linker_name,
        or blocking_key. Returns [{column_value, count, fraction}].
        """
        from mismo import analysis

        return analysis.link_attribute_counts(linkage, column)

    @mcp.tool()
    def get_column_stats(
        column: Annotated[str, "Column name to analyze"],
        side: Annotated[str, "Which table: 'left' or 'right'"] = "left",
    ) -> dict[str, Any]:
        """Get basic statistics for a specific column.

        Returns count, null_count, n_distinct, and for numeric/date
        columns also min, max, and mean.
        """
        from mismo import analysis

        return analysis.column_stats(linkage, column, side)

    # ------------------------------------------------------------------
    # Tools: chart specs
    # ------------------------------------------------------------------

    @mcp.tool()
    def list_available_charts() -> list[dict[str, Any]]:
        """List all available chart types with their descriptions and arguments.

        Use this to discover what charts you can generate, then call
        get_chart_spec with the chart name and arguments.
        """
        from mismo.analysis import charts

        return charts.available_charts()

    @mcp.tool()
    def get_chart_spec(
        chart_name: Annotated[
            str,
            "Name of the chart (use list_available_charts to see options)",
        ],
        kwargs_json: Annotated[
            str,
            "JSON object of keyword arguments for the chart function (default '{}')",
        ] = "{}",
    ) -> str:
        """Generate a Vega-Lite chart specification as a JSON string.

        The returned JSON can be:
        - Rendered in Python: alt.Chart.from_dict(json.loads(spec))
        - Embedded in HTML: vegaEmbed('#view', JSON.parse(spec))

        Use list_available_charts() first to see what charts exist and
        what arguments they accept.

        Example calls:
        - get_chart_spec("link_count", '{"side": "right"}')
        - get_chart_spec("coverage_donut", '{}')
        - get_chart_spec("score_histogram", '{"column": "score"}')
        """
        from mismo.analysis import charts

        chart_fn = getattr(charts, chart_name, None)
        if chart_fn is None:
            available = [c["name"] for c in charts.available_charts()]
            raise ValueError(
                f"Unknown chart {chart_name!r}. Available: {available}"
            )
        try:
            kwargs = json.loads(kwargs_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"kwargs_json is not valid JSON: {e}") from e

        spec = chart_fn(linkage, **kwargs)
        return json.dumps(spec, indent=2)

    # ------------------------------------------------------------------
    # Tools: search / filter
    # ------------------------------------------------------------------

    @mcp.tool()
    def search_records(
        column: Annotated[str, "Column to search in"],
        value: Annotated[str, "Value to search for (substring match for strings)"],
        side: Annotated[str, "Which table: 'left' or 'right'"] = "left",
        n: Annotated[int, "Max results (default 20)"] = 20,
    ) -> list[dict[str, Any]]:
        """Search for records matching a value in a specific column.

        For string columns, does a case-insensitive substring match.
        For other types, does an exact match (value is cast from string).

        Returns matching records with their link count appended.
        """
        import ibis
        from ibis import _

        table = linkage.left if side == "left" else linkage.right
        if column not in table.columns:
            raise ValueError(
                f"Column {column!r} not found. Available: {table.columns}"
            )

        dtype = table.schema()[column]
        with_n = table.with_n_links()

        if dtype.is_string():
            filtered = with_n.filter(
                with_n[column].lower().contains(value.lower())
            )
        else:
            try:
                cast_val = ibis.literal(value).cast(dtype)
                filtered = with_n.filter(with_n[column] == cast_val)
            except Exception:
                raise ValueError(
                    f"Cannot filter {dtype} column {column!r} "
                    f"with string value {value!r}"
                )

        return _rows(filtered, limit=n)

    @mcp.tool()
    def get_links_for_record(
        record_id: Annotated[int | str, "The record_id value to look up"],
        side: Annotated[
            str, "Which side this record_id is on: 'left' or 'right'"
        ] = "left",
    ) -> dict[str, Any]:
        """Get all links for a specific record, with data from both sides.

        Returns the record itself plus a list of all matched records
        on the other side (with all their fields).
        """
        import ibis

        table = linkage.left if side == "left" else linkage.right

        # Find the record
        try:
            dtype = table.schema()["record_id"]
            cast_id = ibis.literal(record_id).cast(dtype)
        except Exception:
            cast_id = ibis.literal(record_id)

        records = table.filter(table.record_id == cast_id).execute()
        if records.empty:
            return {"error": f"No record found with record_id={record_id!r} on {side} side"}

        record = records.iloc[0].to_dict()
        record = {k: (v.item() if hasattr(v, "item") else v) for k, v in record.items()}

        # Find linked records on the other side
        if side == "left":
            my_links = linkage.links.filter(
                linkage.links.record_id_l == cast_id
            )
            other_ids = my_links.record_id_r
            other_table = linkage.right
        else:
            my_links = linkage.links.filter(
                linkage.links.record_id_r == cast_id
            )
            other_ids = my_links.record_id_l
            other_table = linkage.left

        other_records = _rows(
            other_table.filter(other_table.record_id.isin(other_ids))
        )
        links_data = _rows(my_links)

        return {
            "record": record,
            "n_links": len(other_records),
            "linked_records": other_records,
            "link_rows": links_data,
        }

    return mcp
