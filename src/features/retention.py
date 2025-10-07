# import pandas as pd
# from datetime import timedelta, timezone
# from dateutil import parser as dparser
# from src.common.mw import MWClient

# def _utc(ts: str):
#     return dparser.parse(ts).astimezone(timezone.utc)

# def next_edit_after(mw: MWClient, user: str, after_ts: str):
#     """Return timestamp of user's next edit strictly after `after_ts`, else None."""
#     params = {
#         "action": "query",
#         "list": "usercontribs",
#         "ucuser": user,
#         "ucstart": after_ts,     # start at t0 but we'll skip equal
#         "ucdir": "newer",
#         "uclimit": "2",
#         "ucprop": "timestamp",
#     }
#     data = mw.query(params)
#     rows = data.get("query", {}).get("usercontribs", [])
#     # The first row may be the t0 edit; find the first strictly later one
#     later = [r for r in rows if r["timestamp"] > after_ts]
#     return later[0]["timestamp"] if later else None

# def add_retention_flags(df_with_firsts_and_feedback: pd.DataFrame) -> pd.DataFrame:
#     mw = MWClient()
#     out = df_with_firsts_and_feedback.copy()

#     out["next_edit_ts"] = None
#     out["retained_7d"] = False
#     out["retained_30d"] = False

#     for i, row in out.iterrows():
#         t0_iso = row["first_ts"]
#         t0 = _utc(t0_iso)
#         ts_next = next_edit_after(mw, row["user"], t0_iso)
#         out.at[i, "next_edit_ts"] = ts_next
#         if ts_next:
#             tnext = _utc(ts_next)
#             out.at[i, "retained_7d"]  = (tnext <= t0 + timedelta(days=7))
#             out.at[i, "retained_30d"] = (tnext <= t0 + timedelta(days=30))

#     return out

# if __name__ == "__main__":
#     import argparse, os
#     ap = argparse.ArgumentParser()
#     ap.add_argument("--infile", required=True)
#     ap.add_argument("--out", default="data/derived/retention_with_feedback.csv")
#     args = ap.parse_args()

#     os.makedirs(os.path.dirname(args.out), exist_ok=True)
#     df = pd.read_csv(args.infile)
#     enriched = add_retention_flags(df)
#     enriched.to_csv(args.out, index=False)
#     print(f"[save] {len(enriched)} rows → {args.out}")
import pandas as pd
from datetime import timedelta, timezone
from dateutil import parser as dparser
from src.common.mw import MWClient

def _utc(ts: str):
    return dparser.parse(ts).astimezone(timezone.utc)

def _iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def next_edit_after(mw: MWClient, userid: int, first_ts_iso: str):
    """
    Return timestamp of the user's first contribution STRICTLY after first_ts.
    Use user ID to avoid rename quirks; start at first_ts + 1s to exclude the first edit.
    """
    t0_plus = _utc(first_ts_iso) + timedelta(seconds=1)
    params = {
        "action": "query",
        "list": "usercontribs",
        "ucuserids": str(userid),        # <-- use numeric ID
        "ucstart": _iso(t0_plus),        # <-- start just after first edit
        "ucdir": "newer",
        "uclimit": "1",
        "ucprop": "timestamp",
    }
    data = mw.query(params)
    rows = data.get("query", {}).get("usercontribs", [])
    return rows[0]["timestamp"] if rows else None

def add_retention_flags(df_with_firsts_and_feedback: pd.DataFrame) -> pd.DataFrame:
    mw = MWClient()
    out = df_with_firsts_and_feedback.copy()

    out["next_edit_ts"] = None
    out["days_to_next_edit"] = pd.NA
    out["retained_7d"] = False
    out["retained_30d"] = False

    for i, row in out.iterrows():
        ts_next = None
        try:
            ts_next = next_edit_after(mw, int(row["userid"]), str(row["first_ts"]))
        except Exception:
            ts_next = None

        out.at[i, "next_edit_ts"] = ts_next
        if ts_next:
            t0 = _utc(str(row["first_ts"]))
            tnext = _utc(ts_next)
            delta_days = (tnext - t0).total_seconds() / 86400.0
            out.at[i, "days_to_next_edit"] = round(delta_days, 3)
            out.at[i, "retained_7d"]  = (tnext <= t0 + timedelta(days=7))
            out.at[i, "retained_30d"] = (tnext <= t0 + timedelta(days=30))

        if i % 100 == 0:
            print(f"[retention] {i}/{len(out)} users processed...")

    return out

if __name__ == "__main__":
    import argparse, os
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", default="data/derived/retention_with_feedback.csv")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df = pd.read_csv(args.infile)
    enriched = add_retention_flags(df)
    enriched.to_csv(args.out, index=False)
    print(f"[save] {len(enriched)} rows → {args.out}")
