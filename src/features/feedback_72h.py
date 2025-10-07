import pandas as pd
from datetime import datetime, timedelta, timezone
from dateutil import parser as dparser
from typing import Dict, Tuple
from src.common.mw import MWClient

REVERT_TAGS = {"mw-rollback", "mw-undo", "mw-manual-revert"}

def _utc(ts: str) -> datetime:
    return dparser.parse(ts).astimezone(timezone.utc)

def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _talk_title(user: str) -> str:
    # MediaWiki canonicalizes spaces/underscores internally; "User talk:Name"
    return f"User talk:{user}"

def talk_messages_within(mw: MWClient, username: str, t0: datetime, t1: datetime) -> int:
    """Count revisions to User_talk:username by others in [t0,t1)."""
    title = _talk_title(username)
    params = {
        "action": "query",
        "prop": "revisions",
        "titles": title,
        "rvprop": "timestamp|user",
        "rvdir": "newer",
        "rvstart": _iso(t0),
        "rvend": _iso(t1),
        "rvlimit": "500",
    }
    count = 0
    for page in mw.query_all(params):
        pages = page.get("query", {}).get("pages", [])
        if not pages:
            continue
        revs = pages[0].get("revisions", [])
        for rev in revs:
            if rev.get("user") and rev["user"] != username:
                count += 1
    return count

def reverts_on_page_within(mw: MWClient, title: str, actor: str, t0: datetime, t1: datetime) -> int:
    """Count recentchanges on `title` with revert tags by others in [t0,t1)."""
    params = {
        "action": "query",
        "list": "recentchanges",
        "rctitle": title,
        "rcstart": _iso(t1),  # scan older toward t0
        "rcend": _iso(t0),
        "rcdir": "older",
        "rcprop": "user|timestamp|tags",
        "rclimit": "500",
    }
    count = 0
    for page in mw.query_all(params):
        for rc in page.get("query", {}).get("recentchanges", []):
            if rc.get("user") == actor:
                continue
            tags = set(rc.get("tags", []))
            if tags & REVERT_TAGS:
                # within window already guaranteed by rcstart/rcend
                count += 1
    return count

def add_feedback_flags(first_df: pd.DataFrame, window_hours: int = 72) -> pd.DataFrame:
    """Return df with columns: talk_count_72h, revert_count_72h, feedback_72h."""
    mw = MWClient()
    out = first_df.copy()
    out["talk_count_72h"] = 0
    out["revert_count_72h"] = 0

    for i, row in out.iterrows():
        t0 = _utc(row["first_ts"])
        t1 = t0 + timedelta(hours=window_hours)

        talk_n = talk_messages_within(mw, row["user"], t0, t1)
        rev_n  = reverts_on_page_within(mw, row["first_title"], row["user"], t0, t1)

        out.at[i, "talk_count_72h"] = talk_n
        out.at[i, "revert_count_72h"] = rev_n

    out["feedback_72h"] = (out["talk_count_72h"] > 0) | (out["revert_count_72h"] > 0)
    return out

if __name__ == "__main__":
    import argparse, os
    ap = argparse.ArgumentParser()
    ap.add_argument("--first-edits", required=True)
    ap.add_argument("--out", default="data/derived/first_edits_with_feedback.csv")
    ap.add_argument("--hours", type=int, default=72)
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df = pd.read_csv(args.first_edits)
    enriched = add_feedback_flags(df, args.hours)
    enriched.to_csv(args.out, index=False)
    print(f"[save] {len(enriched)} rows → {args.out}")
