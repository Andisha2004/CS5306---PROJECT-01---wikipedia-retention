# src/analysis/summarize_and_plot.py
from __future__ import annotations
import csv
from pathlib import Path
import matplotlib.pyplot as plt

ROOT  = Path(__file__).resolve().parents[2]
CLEAN = ROOT / "data" / "clean"
CLEAN.mkdir(parents=True, exist_ok=True)

# 1) Read per-month metrics and write a fresh summary CSV
rows = []
for ym in ("2024-08", "2024-09"):
    p = CLEAN / f"retention_{ym}.csv"
    if not p.exists():
        print(f"[warn] missing {p.name} — run analyze_retention.py first")
        continue
    with p.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            if row["cohort_month"] != ym:
                continue
            users = int(row["users"])
            edited_again = int(row["edited_again"])
            same_month = int(row["same_month"])
            within_7d = int(row["within_7d"])
            next_month = int(row["next_month"])

            # percentages
            den = max(users, 1)
            pct_same   = 100.0 * same_month / den
            pct_w7     = 100.0 * within_7d / den
            pct_next   = 100.0 * next_month / den

            rows.append(dict(
                month=ym, n_cohort=users,
                pct_edited_again_same_month=round(pct_same, 1),
                pct_within_7_days=round(pct_w7, 1),
                pct_edited_next_month=round(pct_next, 1),
            ))

# overwrite the summary CSV
summary_csv = CLEAN / "retention_summary.csv"
with summary_csv.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(
        f,
        fieldnames=["month","n_cohort","pct_edited_again_same_month","pct_within_7_days","pct_edited_next_month"]
    )
    w.writeheader()
    w.writerows(rows)

print(f"[save] wrote {len(rows)} rows → {summary_csv}")

# # 2) Plot "Edited Again (Same Month) % by Cohort Month"
# months = [r["month"] for r in rows]
# vals   = [r["pct_edited_again_same_month"] for r in rows]

# plt.figure(figsize=(6,4))
# plt.title("Edited Again (Same Month) % by Cohort Month")
# plt.bar(months, vals)  # default color; one bar per month
# plt.ylabel("% of Cohort")
# plt.xlabel("Cohort month")
# plt.ylim(0, max(vals + [10]))  # give it some headroom
# out_png = CLEAN / "retention_summary.png"
# plt.tight_layout()
# plt.savefig(out_png, dpi=180)
# print(f"[save] plot → {out_png}")
# 2) Plot "Edited Again (Same Month) % by Cohort Month"
months = [r["month"] for r in rows]
vals   = [r["pct_edited_again_same_month"] for r in rows]

plt.figure(figsize=(6,4))
plt.title("Edited Again (Same Month) % by Cohort Month")
bars = plt.bar(months, vals)

# add value labels
for b, v in zip(bars, vals):
    plt.text(b.get_x() + b.get_width()/2, b.get_height() + 1,
             f"{v:.1f}%", ha="center", va="bottom", fontsize=9)

plt.ylabel("% of Cohort")
plt.xlabel("Cohort month")
plt.ylim(0, max(vals + [10]) + 5)
plt.tight_layout()

out_png = CLEAN / "retention_summary.png"
out_svg = CLEAN / "retention_summary.svg"
plt.savefig(out_png, dpi=180)
plt.savefig(out_svg)  # crisp for slides
print(f"[save] plot → {out_png}")
print(f"[save] plot → {out_svg}")
