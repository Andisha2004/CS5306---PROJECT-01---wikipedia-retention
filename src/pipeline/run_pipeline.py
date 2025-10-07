import os
import pandas as pd

from src.collect.first_edits import collect_first_edits
from src.features.feedback_72h import add_feedback_flags
from src.features.retention import add_retention_flags

def run_month(start_iso: str, end_iso: str, out_dir: str = "data"):
    # dirs
    raw_dir = os.path.join(out_dir, "raw")
    der_dir = os.path.join(out_dir, "derived")
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(der_dir, exist_ok=True)

    # file paths (YYYY-MM taken from start)
    ym = start_iso[:7]
    first_path   = os.path.join(raw_dir, f"first_edits_{ym}.csv")
    withfb_path  = os.path.join(der_dir, f"first_edits_with_feedback_{ym}.csv")
    final_path   = os.path.join(der_dir, f"retention_with_feedback_{ym}.csv")
    metrics_path = os.path.join(out_dir, f"metrics_by_cohort_{ym}.csv")

    # A) FIRST EDITS — generate CSV if missing (new collector writes directly)
    if not os.path.exists(first_path):
        max_users = int(os.environ.get("MAX_USERS", "200000"))
        print(f"[first-edits] collecting new accounts (MAX_USERS={max_users}) …")
        collect_first_edits(start_iso, end_iso, out_csv=first_path, max_users=max_users)

    # load
    df_first = pd.read_csv(first_path)
    # self-heal schema if someone left an old file around
    required = {"user", "userid", "first_ts", "first_title"}
    if not required.issubset(df_first.columns):
        print(f"[warn] {first_path} has wrong schema; regenerating with new collector…")
        max_users = int(os.environ.get("MAX_USERS", "200000"))
        collect_first_edits(start_iso, end_iso, out_csv=first_path, max_users=max_users)
        df_first = pd.read_csv(first_path)

    # Optional: limit rows for fast debug runs
    sample_n = int(os.environ.get("SAMPLE_N", "0"))
    if sample_n and len(df_first) > sample_n:
        df_first = df_first.head(sample_n).copy()
        print(f"[debug] Using SAMPLE_N={sample_n}: limiting to {len(df_first)} users")

    # B) FEEDBACK ≤72h
    print("[feedback] tagging early feedback (talk/revert ≤72h)…")
    df_fb = add_feedback_flags(df_first, window_hours=72)
    df_fb.to_csv(withfb_path, index=False)

    # C) RETENTION (7d / 30d)
    print("[retention] computing 7d / 30d retention…")
    df_final = add_retention_flags(df_fb)
    df_final.to_csv(final_path, index=False)

    # D) METRICS BY COHORT
    def pct_bool(s: pd.Series) -> float:
        return round(100.0 * (s.astype(bool).sum() / len(s)) if len(s) else 0.0, 1)

    grp = (
        df_final
        .groupby("feedback_72h", dropna=False)
        .agg(
            n=("user", "count"),
            retained_7d=("retained_7d", pct_bool),
            retained_30d=("retained_30d", pct_bool),
            talk_any=("talk_count_72h", lambda s: round(100.0 * (s > 0).mean() if len(s) else 0.0, 1)),
            revert_any=("revert_count_72h", lambda s: round(100.0 * (s > 0).mean() if len(s) else 0.0, 1)),
        )
        .reset_index()
    )
    grp["cohort"] = grp["feedback_72h"].map({True: "With feedback ≤72h", False: "No feedback ≤72h"})
    grp = grp[["cohort", "n", "retained_7d", "retained_30d", "talk_any", "revert_any"]]

    grp.to_csv(metrics_path, index=False)
    print("\n[metrics]")
    print(grp.to_string(index=False))
    print(f"\n[save] {metrics_path}")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="e.g., 2024-09-01T00:00:00Z")
    ap.add_argument("--end", required=True, help="e.g., 2024-10-01T00:00:00Z")
    ap.add_argument("--out", default="data")
    args = ap.parse_args()
    run_month(args.start, args.end, args.out)
