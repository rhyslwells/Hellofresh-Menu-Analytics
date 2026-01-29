#!/usr/bin/env python3
"""
Generate a Plotly figure and embed it as HTML div inside a Markdown file.

Usage:
    python scripts/embed_plotly_to_md.py --out hfresh/output/plotly_test/report.md

This writes a Markdown file that contains the Plotly HTML div (uses CDN for plotly.js).
"""
from __future__ import annotations
import argparse
import os
import plotly.graph_objs as go
from plotly.offline import plot


def make_fig() -> go.Figure:
    x = [1, 2, 3, 4, 5]
    y = [10, 15, 13, 17, 22]
    fig = go.Figure(go.Scatter(x=x, y=y, mode="lines+markers", name="Sample"))
    fig.update_layout(title="Sample Plotly Chart", template="simple_white")
    return fig


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Plotly HTML and embed into Markdown.")
    parser.add_argument("--out", "-o", default="hfresh/output/plotly_test/report.md", help="Output markdown file path")
    parser.add_argument("--html-only", action="store_true", help="Write only the HTML file (skip markdown)")
    args = parser.parse_args()

    out_path = args.out
    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    fig = make_fig()
    # produce an HTML <div> with a script that loads plotly from CDN
    div = plot(fig, include_plotlyjs="cdn", output_type="div")

    if not args.html_only:
        md = f"""# Plotly Embed Test

This Markdown file includes an embedded Plotly chart (HTML).

{div}
"""

        with open(out_path, "w", encoding="utf-8") as f:
            f.write(md)

        print(f"Wrote {out_path}")

    # Also write a standalone HTML file to test GitHub rendering of raw HTML
    html_path = os.path.splitext(out_path)[0] + ".html"
    html_doc = f"""<!doctype html>
<html lang=\"en\">\n<head>\n  <meta charset=\"utf-8\">\n  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n  <title>Plotly Embed Test</title>\n</head>\n<body>\n  <h1>Plotly Embed Test</h1>\n  {div}\n</body>\n</html>"""

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_doc)

    print(f"Wrote {html_path}")


if __name__ == "__main__":
    main()
