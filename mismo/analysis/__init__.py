"""Analysis tools for reviewing and understanding a Linkage.

This module provides a clean programmatic API for analyzing linkage
performance and quality. Functions here return plain Python objects
(dicts, ibis Tables, DataFrames) that can be:

- Used directly in Python for data exploration
- Wrapped by an MCP server for AI agent interaction
- Exposed via an HTTP API or web frontend

Quick start
-----------
>>> import mismo
>>> from mismo import analysis
>>> # Create or load a linkage
>>> linkage = mismo.Linkage(...)
>>> # Get a summary dict
>>> analysis.summary(linkage)
{'n_left': 1000, 'n_right': 800, 'n_links': 950, ...}
>>> # Explore unlinked records
>>> analysis.unlinked(linkage, "left").execute()
>>> # Get a Vega-Lite chart spec (works in Altair and JS Vega-Embed)
>>> spec = analysis.charts.link_count(linkage)
>>> alt.Chart.from_dict(spec)  # Python
"""

from __future__ import annotations

from mismo.analysis import charts as charts
from mismo.analysis._records import (
    multiply_linked as multiply_linked,
    sample_pairs as sample_pairs,
    singly_linked as singly_linked,
    unlinked as unlinked,
    with_link_category as with_link_category,
)
from mismo.analysis._summary import (
    column_stats as column_stats,
    link_attribute_counts as link_attribute_counts,
    summary as summary,
)
