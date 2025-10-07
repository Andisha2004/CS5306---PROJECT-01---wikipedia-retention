# import pandas as pd
# from datetime import datetime, timezone
# from dateutil import parser as dparser
# from typing import Dict, List
# from tqdm import tqdm
# from src.common.mw import MWClient

# def iso(ts: str) -> str:
#     # Normalize to strict UTC ISO8601 (MediaWiki format)
#     return dparser.parse(ts).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# def collect_first_edits(start_iso: str, end_iso: str, max_scan: int = 200_000) -> pd.DataFrame:
#     """
#     Find users whose first-ever edit occurred within [start,end).
#     Returns columns:
#       user, userid, first_pageid, first_title, first_revid, first_ts
#     """
#     mw = MWClient()
#     start_iso, end_iso = iso(start_iso), iso(end_iso)

#     # 1) Scan recentchanges for edits in window (namespace all, skip bots)
#     params = {
#         "action": "query",
#         "list": "recentchanges",
#         "rcstart": end_iso,         # API reads backwards in time by default
#         "rcend": start_iso,
#         "rcdir": "older",
#         "rcshow": "!bot",
#         "rctype": "edit|new",
#         "rcprop": "user|userid|title|ids|timestamp|tags|flags",
#         "rclimit": "500",
#     }

#     earliest_seen: Dict[str, Dict] = {}
#     scanned = 0

#     for page in mw.query_all(params):
#         for rc in page.get("query", {}).get("recentchanges", []):
#             scanned += 1
#             if scanned > max_scan:
#                 break
#             if rc.get("anon"):
#                 continue  # only registered newcomers
#             user = rc.get("user")
#             if user not in earliest_seen:
#                 earliest_seen[user] = rc
#         if scanned > max_scan:
#             break

#     if not earliest_seen:
#         return pd.DataFrame(columns=["user","userid","first_pageid","first_title","first_revid","first_ts"])

#     # 2) Verify each candidate is truly the first-ever edit via usercontribs (oldest=first)
#     rows: List[Dict] = []
#     for user, rc in tqdm(earliest_seen.items(), desc="Verifying first-ever edits"):
#         uc = {
#             "action": "query",
#             "list": "usercontribs",
#             "ucuser": user,
#             "ucdir": "older",
#             "uclimit": "1",
#             "ucprop": "ids|title|timestamp",
#         }
#         data = mw.query(uc)
#         contribs = data.get("query", {}).get("usercontribs", [])
#         if not contribs:
#             continue
#         oldest = contribs[0]
#         # accept only if oldest timestamp lies within our window
#         ts = oldest["timestamp"]
#         if start_iso <= ts < end_iso:
#             rows.append({
#                 "user": user,
#                 "userid": rc.get("userid"),
#                 "first_pageid": oldest.get("pageid"),
#                 "first_title": oldest.get("title"),
#                 "first_revid": oldest.get("revid"),
#                 "first_ts": ts,
#             })

#     df = pd.DataFrame(rows).drop_duplicates(subset=["user"]).reset_index(drop=True)
#     return df

# if __name__ == "__main__":
#     import argparse, os
#     ap = argparse.ArgumentParser()
#     ap.add_argument("--start", required=True)
#     ap.add_argument("--end", required=True)
#     ap.add_argument("--out", default="data/raw/first_edits.csv")
#     ap.add_argument("--max-scan", type=int, default=200000)
#     args = ap.parse_args()

#     os.makedirs(os.path.dirname(args.out), exist_ok=True)
#     df = collect_first_edits(args.start, args.end, args.max_scan)
#     df.to_csv(args.out, index=False)
#     print(f"[save] {len(df)} rows → {args.out}")

import os
import sys
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime, timezone
from dateutil import parser as dparser
from tqdm import tqdm

from src.common.mw import MWClient

def iso(ts: str) -> str:
    return dparser.parse(ts).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _utc(ts: str) -> datetime:
    return dparser.parse(ts).astimezone(timezone.utc)

def fetch_new_users(start_iso: str, end_iso: str, max_users: int = 200_000) -> List[Dict]:
    """
    Get account creations in [start,end) via logevents (letype=newusers).
    Returns list of dicts with 'user', 'userid', 'created'.
    """
    mw = MWClient()
    start_iso, end_iso = iso(start_iso), iso(end_iso)

    params = {
        "action": "query",
        "list": "logevents",
        "letype": "newusers",
        "lestart": end_iso,   # read older toward start
        "leend": start_iso,
        "ledir": "older",
        "leprop": "timestamp|userid|user|details",
        "lelimit": "500",
    }

    out: List[Dict] = []
    seen = set()

    print(f"[newusers] window {start_iso} .. {end_iso}")
    for page in mw.query_all(params):
        batch = page.get("query", {}).get("logevents", [])
        if not batch:
            continue
        for ev in batch:
            user = ev.get("user")
            if not user or user in seen:
                continue
            seen.add(user)
            out.append({
                "user": user,
                "userid": ev.get("userid"),
                "created": ev.get("timestamp"),
            })
            if len(out) >= max_users:
                break
        print(f"[newusers] batch: {len(batch)} (total unique users: {len(out)})")
        if len(out) >= max_users:
            break

    print(f"[newusers] total new accounts: {len(out)}")
    return out

def fetch_first_edit(mw: MWClient, user: str) -> Optional[Dict]:
    """
    Oldest contribution for user (their first-ever edit), or None if no edits.
    """
    params = {
        "action": "query",
        "list": "usercontribs",
        "ucuser": user,
        "ucdir": "newer",
        "uclimit": "1",
        "ucprop": "ids|title|timestamp|comment|sizediff",
    }
    data = mw.query(params)
    rows = data.get("query", {}).get("usercontribs", [])
    if not rows:
        return None
    r = rows[0]
    return {
        "first_pageid": r.get("pageid"),
        "first_title": r.get("title"),
        "first_revid": r.get("revid"),
        "first_ts": r.get("timestamp"),
        "first_comment": r.get("comment", ""),
        "first_sizediff": r.get("sizediff"),
    }

def collect_first_edits(start_iso: str, end_iso: str, out_csv: str, max_users: int = 200_000):
    mw = MWClient()
    start_iso, end_iso = iso(start_iso), iso(end_iso)

    # 1) New accounts created in window
    new_users = fetch_new_users(start_iso, end_iso, max_users=max_users)
    if not new_users:
        pd.DataFrame(columns=[
            "user","userid","created","first_pageid","first_title","first_revid",
            "first_ts","first_comment","first_sizediff"
        ]).to_csv(out_csv, index=False)
        print(f"[save] 0 rows → {out_csv}")
        return

    # 2) First edit for each new account
    rows: List[Dict] = []
    for nu in tqdm(new_users, desc="Fetching first edits"):
        fe = fetch_first_edit(mw, nu["user"])
        if fe is None:
            # Some new accounts never make an edit
            continue
        # Keep only those whose first-ever edit happened inside the window
        if start_iso <= fe["first_ts"] < end_iso:
            rows.append({**nu, **fe})

    df = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(f"[save] {len(df)} rows → {out_csv}")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--out", default="data/raw/first_edits.csv")
    ap.add_argument("--max-users", type=int, default=200000)
    args = ap.parse_args()

    collect_first_edits(args.start, args.end, args.out, args.max_users)
