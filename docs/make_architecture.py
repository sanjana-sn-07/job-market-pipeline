"""Regenerates docs/architecture.png.

Kept in the repo so the diagram can be updated when the DAG changes, rather than
being a binary nobody can edit. Run: python docs/make_architecture.py
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

# draw.io default palettes, so the output looks like the original diagram
GREEN  = ("#d5e8d4", "#82b366")
BLUE   = ("#dae8fc", "#6c8ebf")
ORANGE = ("#ffe6cc", "#d79b00")
PURPLE = ("#e1d5e7", "#9673a6")
GREY   = ("#f5f5f5", "#999999")
NAVY   = ("#1f5fbf", "#12407f")

W, H = 25.0, 6.4          # default box size
fig, ax = plt.subplots(figsize=(10.5, 13.5))
ax.set_xlim(0, 100); ax.set_ylim(0, 100); ax.axis("off")

boxes = {}


def box(key, x, y, label, sub=None, palette=BLUE, w=W, h=H, bold=False):
    face, edge = palette
    ax.add_patch(FancyBboxPatch(
        (x - w / 2, y - h / 2), w, h,
        boxstyle="round,pad=0,rounding_size=1.1",
        facecolor=face, edgecolor=edge,
        linewidth=1.9 if bold else 1.2))
    color = "white" if palette is NAVY else "#1a1a1a"
    if sub:
        ax.text(x, y + 1.05, label, ha="center", va="center",
                fontsize=8.6, fontweight="bold", color=color)
        ax.text(x, y - 1.45, sub, ha="center", va="center",
                fontsize=7.0, color=color, alpha=0.85)
    else:
        ax.text(x, y, label, ha="center", va="center",
                fontsize=8.6, fontweight="bold", color=color)
    boxes[key] = (x, y, w, h)


def arrow(a, b, dashed=False, both=False):
    (ax1, ay1, _, ah1), (ax2, ay2, _, ah2) = boxes[a], boxes[b]
    if abs(ay1 - ay2) > 1e-6:                       # vertical
        start, end = (ax1, ay1 - ah1 / 2), (ax2, ay2 + ah2 / 2)
    else:                                            # horizontal
        w1, w2 = boxes[a][2], boxes[b][2]
        if ax1 < ax2: start, end = (ax1 + w1 / 2, ay1), (ax2 - w2 / 2, ay2)
        else:         start, end = (ax1 - w1 / 2, ay1), (ax2 + w2 / 2, ay2)
    ax.add_patch(FancyArrowPatch(
        start, end,
        arrowstyle="<|-|>" if both else "-|>",
        mutation_scale=11, linewidth=1.15, color="#444444",
        linestyle="--" if dashed else "-",
        shrinkA=0, shrinkB=0))


def elbow(a, b):
    """Right-angle connector from a task down-and-across into a merge point."""
    (ax1, ay1, _, ah1), (ax2, ay2, _, ah2) = boxes[a], boxes[b]
    ymid = (ay1 - ah1 / 2 + ay2 + ah2 / 2) / 2
    ax.plot([ax1, ax1], [ay1 - ah1 / 2, ymid], color="#444444", linewidth=1.15)
    ax.plot([ax1, ax2], [ymid, ymid], color="#444444", linewidth=1.15)
    ax.add_patch(FancyArrowPatch((ax2, ymid), (ax2, ay2 + ah2 / 2),
                                 arrowstyle="-|>", mutation_scale=11,
                                 linewidth=1.15, color="#444444",
                                 shrinkA=0, shrinkB=0))


ax.text(50, 97.5, "JOB MARKET ANALYTICS PIPELINE",
        ha="center", va="center", fontsize=15, fontweight="bold")
ax.text(50, 94.3, "8-task Airflow DAG  ·  daily  ·  CeleryExecutor on Docker",
        ha="center", va="center", fontsize=8.6, color="#555555")

# sources
box("usajobs", 24, 87, "USAJOBS API", palette=GREEN)
box("adzuna",  76, 87, "Adzuna API",  palette=GREEN)

# the eight tasks
box("t1", 24, 77, "1  ingest_usajobs", "raw_jobs · ON CONFLICT DO NOTHING")
box("t2", 76, 77, "2  ingest_adzuna",  "raw_jobs · ON CONFLICT DO NOTHING")
box("t3", 50, 66, "3  clean_jobs",     "processed_jobs · seniority, work type")
box("t4", 50, 56, "4  extract_skills", "keyword regex · 48 curated skills")
box("t5", 50, 46, "5  extract_skills_llm", "GPT-4o-mini · __processed__ sentinel")
box("t6", 50, 36, "6  upload_to_s3",   "Hive-partitioned JSON backup")
box("t7", 50, 26, "7  run_dbt",        "dbt build · 1 seed · 5 models · 23 tests",
    bold=True)
box("t8", 50, 16, "8  run_forecast",   "Prophet · 26-week horizon")

# external services
box("secrets", 13.5, 46, "AWS Secrets Manager", palette=ORANGE, w=23, h=5.4)
box("openai",  86.5, 46, "OpenAI API",          palette=GREEN,  w=23, h=5.4)
box("s3",      86.5, 36, "AWS S3",              palette=ORANGE, w=23, h=5.4)
# CI sits apart from the flow -- it gates the repo, not any single task
box("ci",      13.5, 16, "GitHub Actions CI",   "25 pytest on every push",
    palette=GREY, w=23, h=6.0)

# storage and serving
box("db", 50, 6.5, "PostgreSQL  /  AWS RDS", palette=ORANGE, w=34, h=6.4)

arrow("usajobs", "t1"); arrow("adzuna", "t2")
elbow("t1", "t3");      elbow("t2", "t3")
for a, b in [("t3","t4"), ("t4","t5"), ("t5","t6"), ("t6","t7"), ("t7","t8"), ("t8","db")]:
    arrow(a, b)
arrow("secrets", "t5", dashed=True)
arrow("openai",  "t5", dashed=True)
arrow("t6", "s3", dashed=True)

# dbt reads and writes the warehouse it sits in front of
ax.plot([62.5, 73, 73], [26, 26, 6.5], color="#6c8ebf", linewidth=1.15, linestyle="--")
ax.add_patch(FancyArrowPatch((73, 6.5), (67, 6.5), arrowstyle="<|-|>",
                             mutation_scale=11, linewidth=1.15,
                             color="#6c8ebf", linestyle="--",
                             shrinkA=0, shrinkB=0))
ax.text(74.5, 16.5, "reads &\nwrites", fontsize=6.8, color="#6c8ebf",
        ha="left", va="center")

ax.text(50, 1.4,
        "Serving layer:  Streamlit dashboard (6 charts)   ·   Tableau Public",
        ha="center", va="center", fontsize=8.4, color="#6a4c93",
        fontweight="bold")

plt.tight_layout()
out = __file__.rsplit("/", 1)[0] + "/architecture.png"
plt.savefig(out, dpi=170, bbox_inches="tight", facecolor="white")
print("wrote", out)
