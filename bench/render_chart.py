#!/usr/bin/env python3
"""Render docs/performance.png from go bench output.

Usage: python3 bench/render_chart.py broker.bench replication.bench [-o docs/performance.png]

Requires matplotlib (python3 -m pip install matplotlib). Records/sec is
computed as 1e9 / ns_per_op x records_per_op.
"""
import argparse
import json
import platform
import re
import sys

ROWS = [
    ("One durable publish", "PublishDurable", 1),
    ("Durable publishes sharing a sync", "PublishParallelDurable", 1),
    ("Durable batch of 32", "PublishBatchDurable", 32),
    ("One publish waited on the follower", "SynchronousPublish", 1),
    ("Replicated publishes sharing a sync", "SynchronousPublishParallel", 1),
    ("Replicated batch of 32", "SynchronousPublishBatch32", 32),
    ("In-memory publish, no disk", "PublishInMemory", 1),
    ("Fetch of 32 cached records", "FetchBatch32", 32),
]

UNIT = {"ns": 1.0, "µs": 1e3, "us": 1e3, "ms": 1e6, "s": 1e9}
BENCH = re.compile(
    r"^Benchmark(?P<name>[A-Za-z0-9_]+)(?:-\d+)?\s+\d+\s+(?P<val>[\d.]+)\s*(?P<unit>ns|µs|us|ms|s)/op(?:\s|$)"
)


def parse(path):
    rows = {}
    with open(path, encoding="utf-8", errors="replace") as f:
        for raw in f:
            m = BENCH.match(raw.strip())
            if not m:
                continue
            rows.setdefault(m.group("name"), float(m.group("val")) * UNIT[m.group("unit")])
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("bench", nargs="+")
    parser.add_argument("-o", default="docs/performance.png")
    args = parser.parse_args()

    results = {}
    for raw in args.bench:
        results.update(parse(raw))

    labels, values, colors = [], [], []
    for label, name, records in ROWS:
        ns = results.get(name)
        if ns is None:
            continue
        labels.append(label)
        values.append(1e9 / ns * records)
        colors.append("#33727e" if "Synchronous" not in name else "#b66a38")

    try:
        import matplotlib.pyplot as plt
        import matplotlib.ticker as ticker
    except ImportError:
        sys.exit("matplotlib is required: python3 -m pip install matplotlib")

    fig, ax = plt.subplots(figsize=(10.8, 6.0), dpi=110)
    fig.patch.set_facecolor("#f7f6f3")
    ax.set_facecolor("#f7f6f3")

    ypos = list(range(len(labels)))[::-1]
    bars = ax.barh(ypos, values, color=colors, height=0.62)
    ax.set_yticks(ypos, labels)
    ax.set_xscale("log")
    ax.set_xlim(50, values[-1] * 20 if values else 1e8)
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.grid(axis="x", which="major", color="#d8d5cf", linewidth=0.8)
    ax.set_axisbelow(True)

    title_machine = platform.machine() or "arm64"
    ax.set_title("records acknowledged per second - 64 byte values - 500 ms samples",
                 fontsize=9.5, color="#202020", loc="left", pad=12)
    fig.suptitle(f"mq durable message queue - {title_machine}", fontsize=13,
                 color="#202020", fontweight="bold", x=0.025, ha="left", y=0.97)

    for bar, value in zip(bars, values):
        ax.text(value * 1.15, bar.get_y() + bar.get_height() / 2,
                f"{value:,.0f} rec/s" if value >= 100 else f"{value:.0f} rec/s",
                va="center", fontsize=9.5, color="#202020")

    for spine in ("top", "right", "left", "bottom"):
        ax.spines[spine].set_visible(False)
    fig.tight_layout(rect=(0, 0, 1, 0.9))
    fig.savefig(args.o, bbox_inches="tight")
    print(f"wrote {args.o}")


if __name__ == "__main__":
    main()
