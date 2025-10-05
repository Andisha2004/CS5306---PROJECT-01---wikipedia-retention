# # src/collect/analyze_retention.py
# import csv
# from pathlib import Path
# from collections import defaultdict
# import matplotlib.pyplot as plt

# ROOT = Path(__file__).resolve().parents[2]
# CLEAN = ROOT / "data" / "clean"
# CLEAN.mkdir(parents=True, exist_ok=True)

# def month_from_filename(p: Path) -> str:
#     # retention_2024-08.csv -> 2024-08
#     return p.stem.replace("retention_", "")

# def load_metrics(path: Path):
#     n = 0
#     edited_again = 0
#     within7 = 0
#     edited_next = 0
#     with open(path, newline="", encoding="utf-8") as f:
#         r = csv.DictReader(f)
#         for row in r:
#             n += 1
#             edited_again += int(row.get("edited_again_same_month") or 0)
#             within7 += int(row["within_7_days"] or 0)
#             edited_next += int(row["edited_next_month"] or 0)
#     pct = lambda k: (k / n * 100.0) if n else 0.0
#     return {
#         "n_cohort": n,
#         "pct_edited_again_same_month": pct(edited_again),
#         "pct_within_7_days": pct(within7),
#         "pct_edited_next_month": pct(edited_next),
#     }

# def main():
#     rows = []
#     for p in sorted(CLEAN.glob("retention_*.csv")):
#         m = month_from_filename(p)
#         met = load_metrics(p)
#         rows.append({"month": m, **met})

#     # Write summary CSV
#     out_csv = CLEAN / "retention_summary.csv"
#     with open(out_csv, "w", newline="", encoding="utf-8") as f:
#         w = csv.DictWriter(
#             f,
#             fieldnames=["month","n_cohort","pct_edited_again_same_month","pct_within_7_days","pct_edited_next_month"]
#         )
#         w.writeheader()
#         for r in rows:
#             w.writerow(r)
#     print(f"[write] {out_csv}")

#     # Simple bar chart (Edited again same month)
#     months = [r["month"] for r in rows]
#     vals   = [r["pct_edited_again_same_month"] for r in rows]
#     plt.figure()
#     plt.bar(months, vals)
#     plt.title("Edited Again (Same Month) % by Cohort Month")
#     plt.ylabel("% of cohort")
#     plt.xlabel("Cohort month")
#     plt.tight_layout()
#     out_png = CLEAN / "retention_summary.png"
#     plt.savefig(out_png, dpi=150)
#     print(f"[write] {out_png}")

# if __name__ == "__main__":
#     main()

from __future__ import annotations
import csv
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta  # pip install python-dateutil

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw"
CLEAN = ROOT / "data" / "clean"
CLEAN.mkdir(parents=True, exist_ok=True)

def parse_ts(s: str | None):
    if not s:
        return None
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    if " " in s and "T" not in s:
        s = s.replace(" ", "T", 1)
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

def month_start(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

def same_month(a: datetime, b: datetime) -> bool:
    return a.year == b.year and a.month == b.month

def next_month(a: datetime, b: datetime) -> bool:
    nm = month_start(a) + relativedelta(months=1)
    return b.year == nm.year and b.month == nm.month

def read_map(path: Path) -> dict[int, datetime]:
    m: dict[int, datetime] = {}
    with path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            uid = row.get("userid")
            t = parse_ts(row.get("t"))
            if uid and t:
                try:
                    m[int(uid)] = t
                except ValueError:
                    pass
    return m

def analyze_month(ym: str):
    first_p = RAW / f"rc_first_edits_{ym}.csv"
    second_p = RAW / f"rc_second_edits_{ym}.csv"

    if not first_p.exists() or not second_p.exists():
        print(f"[warn] missing data for {ym}")
        return

    first = read_map(first_p)
    second = read_map(second_p)

    users = len(first)
    edited = same_m = next_m = within7 = within30 = 0

    for uid, t0 in first.items():
        t1 = second.get(uid)
        if not t1 or t1 <= t0:
            continue
        edited += 1
        if same_month(t0, t1):  same_m += 1
        elif next_month(t0, t1): next_m += 1
        days = (t1 - t0).total_seconds() / 86400.0
        if days <= 7:  within7 += 1
        if days <= 30: within30 += 1

    out_csv = CLEAN / f"retention_{ym}.csv"
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["cohort_month","users","edited_again","same_month","next_month","within_7d","within_30d"])
        w.writerow([ym, users, edited, same_m, next_m, within7, within30])

    u = max(users, 1)
    print(f"\n[metrics] {ym}")
    print(f"  users: {users}")
    print(f"  edited again: {edited} ({edited/u:.1%})")
    print(f"    ├─ same month: {same_m} ({same_m/u:.1%})")
    print(f"    ├─ next month: {next_m} ({next_m/u:.1%})")
    print(f"    ├─ within 7d:  {within7} ({within7/u:.1%})")
    print(f"    └─ within 30d: {within30} ({within30/u:.1%})")

def main():
    for ym in ("2024-08", "2024-09"):
        analyze_month(ym)

if __name__ == "__main__":
    main()
