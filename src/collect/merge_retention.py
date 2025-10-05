# import csv
# from dataclasses import dataclass
# from datetime import datetime, timedelta
# from pathlib import Path
# from typing import Dict, Optional, Tuple

# ROOT = Path(__file__).resolve().parents[2]
# RAW = ROOT / "data" / "raw"
# OUT = ROOT / "data" / "clean"
# OUT.mkdir(parents=True, exist_ok=True)

# def log(m): print(m, flush=True)

# def parse_iso_z(s: str) -> datetime:
#     # "2024-08-15T12:34:56Z" -> aware UTC datetime
#     return datetime.fromisoformat(s.replace("Z", "+00:00"))

# @dataclass
# class FirstEdit:
#     userid: Optional[int]
#     name: str
#     t0: datetime
#     revid0: int
#     pageid0: int
#     title0: str

# @dataclass
# class RcEdit:
#     userid: Optional[int]
#     name: str
#     t: datetime
#     revid: int
#     pageid: int
#     title: str

# def load_first_edits(path: Path) -> Dict[str, FirstEdit]:
#     out: Dict[str, FirstEdit] = {}
#     with open(path, newline="", encoding="utf-8") as f:
#         r = csv.DictReader(f)
#         for row in r:
#             name = (row.get("name") or "").strip()
#             if not name:
#                 continue
#             try:
#                 userid = int(row["userid"]) if row.get("userid") else None
#             except Exception:
#                 userid = None
#             t0 = parse_iso_z(row["t0"])
#             out[name] = FirstEdit(
#                 userid=userid,
#                 name=name,
#                 t0=t0,
#                 revid0=int(row["revid"]),
#                 pageid0=int(row["pageid"]),
#                 title0=row.get("title", ""),
#             )
#     log(f"[load_first_edits] {path.name}: {len(out)} rows")
#     return out

# def load_rc(path: Path) -> Dict[str, RcEdit]:
#     out: Dict[str, RcEdit] = {}
#     if not path.exists():
#         log(f"[load_rc] {path.name} not found → treating as empty")
#         return out
#     with open(path, newline="", encoding="utf-8") as f:
#         r = csv.DictReader(f)
#         for row in r:
#             name = (row.get("name") or "").strip()
#             if not name:
#                 continue
#             try:
#                 userid = int(row["userid"]) if row.get("userid") else None
#             except Exception:
#                 userid = None
#             try:
#                 revid = int(row["revid"]) if row.get("revid") else None
#                 pageid = int(row["pageid"]) if row.get("pageid") else None
#             except Exception:
#                 revid, pageid = None, None
#             if not row.get("t0") and row.get("t"):
#                 # rc_first_edits_*.csv may use column "t" or "t0"; normalize here
#                 t = parse_iso_z(row["t"])
#             else:
#                 t = parse_iso_z(row.get("t0") or row["t"])
#             out[name] = RcEdit(
#                 userid=userid,
#                 name=name,
#                 t=t,
#                 revid=revid or -1,
#                 pageid=pageid or -1,
#                 title=row.get("title", ""),
#             )
#     log(f"[load_rc] {path.name}: {len(out)} rows")
#     return out

# def retention_for_month(
#     cohort_csv: Path,
#     rc_same_month_csv: Path,
#     rc_next_month_csv: Optional[Path],
#     horizon_days: int = 7,
# ) -> Tuple[Path, Dict[str, float]]:
#     """Join cohort with RC edits to compute retention metrics and write a per-user CSV."""
#     fe = load_first_edits(cohort_csv)
#     rc_same = load_rc(rc_same_month_csv)
#     rc_next = load_rc(rc_next_month_csv) if rc_next_month_csv else {}

#     out_path = OUT / f"retention_{cohort_csv.stem.replace('first_edits_', '')}.csv"
#     with open(out_path, "w", newline="", encoding="utf-8") as f:
#         w = csv.DictWriter(
#             f,
#             fieldnames=[
#                 "userid",
#                 "name",
#                 "t0",
#                 "revid0",
#                 "pageid0",
#                 "title0",
#                 "rc_same_t",
#                 "rc_same_revid",
#                 "rc_same_pageid",
#                 "rc_same_title",
#                 "edited_again_same_month",
#                 "within_7_days",
#                 "rc_next_t",
#                 "edited_next_month",
#             ],
#         )
#         w.writeheader()
#         edited_again = 0
#         within_h = 0
#         edited_next = 0

#         for name, row in fe.items():
#             rc_s = rc_same.get(name)
#             rc_n = rc_next.get(name)

#             # must be a later edit (strictly after first edit) AND different revid
#             edited_again_same = False
#             within_h_days = False
#             rc_s_t_str = ""
#             rc_s_revid = ""
#             rc_s_pageid = ""
#             rc_s_title = ""

#             if rc_s:
#                 if rc_s.t > row.t0 and rc_s.revid != row.revid0:
#                     edited_again_same = True
#                     if rc_s.t <= row.t0 + timedelta(days=horizon_days):
#                         within_h_days = True
#                 rc_s_t_str = rc_s.t.isoformat()
#                 rc_s_revid = rc_s.revid
#                 rc_s_pageid = rc_s.pageid
#                 rc_s_title = rc_s.title

#             edited_next_flag = False
#             rc_n_t_str = ""
#             if rc_n:
#                 # Any mainspace edit in the next month counts as “edited next month”
#                 edited_next_flag = True
#                 rc_n_t_str = rc_n.t.isoformat()

#             if edited_again_same:
#                 edited_again += 1
#             if within_h_days:
#                 within_h += 1
#             if edited_next_flag:
#                 edited_next += 1

#             w.writerow({
#                 "userid": row.userid or "",
#                 "name": name,
#                 "t0": row.t0.isoformat(),
#                 "revid0": row.revid0,
#                 "pageid0": row.pageid0,
#                 "title0": row.title0,
#                 "rc_same_t": rc_s_t_str,
#                 "rc_same_revid": rc_s_revid,
#                 "rc_same_pageid": rc_s_pageid,
#                 "rc_same_title": rc_s_title,
#                 "edited_again_same_month": int(edited_again_same),
#                 "within_7_days": int(within_h_days),
#                 "rc_next_t": rc_n_t_str,
#                 "edited_next_month": int(edited_next_flag),
#             })

#     n = len(fe)
#     metrics = {
#         "n_cohort": float(n),
#         "pct_edited_again_same_month": (edited_again / n * 100.0) if n else 0.0,
#         "pct_within_7_days": (within_h / n * 100.0) if n else 0.0,
#         "pct_edited_next_month": (edited_next / n * 100.0) if n else 0.0,
#     }
#     log(f"[metrics] cohort={n} "
#         f"edited_again_same_month={edited_again} ({metrics['pct_edited_again_same_month']:.1f}%) "
#         f"within_{horizon_days}d={within_h} ({metrics['pct_within_7_days']:.1f}%) "
#         f"edited_next_month={edited_next} ({metrics['pct_edited_next_month']:.1f}%)")
#     log(f"[write] per-user output → {out_path}")
#     return out_path, metrics

# if __name__ == "__main__":
#     log("[main] root  = " + str(ROOT))
#     log("[main] raw   = " + str(RAW))
#     log("[main] out   = " + str(OUT))

#     # Files produced by your earlier scripts
#     AUG_FE  = RAW / "first_edits_2024-08.csv"
#     SEP_FE  = RAW / "first_edits_2024-09.csv"

#     AUG_RC  = RAW / "rc_first_edits_2024-08.csv"
#     SEP_RC  = RAW / "rc_first_edits_2024-09.csv"

#     # Run Aug cohort:
#     #  - same-month RC: AUG_RC
#     #  - next-month RC: SEP_RC
#     retention_for_month(AUG_FE, AUG_RC, SEP_RC, horizon_days=7)

#     # Run Sep cohort (example): same-month RC only
#     retention_for_month(SEP_FE, SEP_RC, None, horizon_days=7)

#     log("[main] Done.")

# src/collect/rc_first_edits.py
import requests, time, csv
from pathlib import Path
from datetime import datetime

API  = "https://en.wikipedia.org/w/api.php"
HEAD = {"User-Agent": "Cornell-INFO-Crowdsourcing-Project/1.0 (as3254@cornell.edu)"}

ROOT = Path(__file__).resolve().parents[2]
RAW  = ROOT / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)

def log(m): print(m, flush=True)

def q(params, retries=5):
    """Polite MediaWiki request with maxlag/retries."""
    params = {**params, "format": "json", "maxlag": 5}
    for i in range(retries):
        r = requests.get(API, params=params, headers=HEAD, timeout=30)
        if r.status_code == 503 and "maxlag" in r.text.lower():
            sl = 2 * (i + 1)
            log(f"[maxlag http] sleeping {sl}s"); time.sleep(sl); continue
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            if data["error"].get("code") == "maxlag":
                sl = 2 * (i + 1)
                log(f"[maxlag json] sleeping {sl}s"); time.sleep(sl); continue
            raise RuntimeError(f"API error: {data['error']}")
        return data
    raise RuntimeError("Exceeded retries to MediaWiki API")

def batch(iterable, n):
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) == n:
            yield buf; buf = []
    if buf: yield buf

def load_cohort_names(first_edits_csv: Path):
    """Read cohort usernames from first_edits_YYYY-MM.csv."""
    names = []
    with open(first_edits_csv, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            name = (row.get("name") or "").strip()
            if name:
                names.append(name)
    log(f"[cohort] loaded {len(names)} names from {first_edits_csv.name}")
    return sorted(set(names))

def fetch_earliest_edits_usercontribs(usernames, start_iso, end_iso):
    """
    For the given usernames, find their earliest mainspace edit within [start_iso, end_iso).
    Uses list=usercontribs, so it works for historical months (no 30d RC limit).
    Returns dict: name -> edit dict
    """
    earliest = {}
    total = len(usernames)
    seen = 0

    for group in batch(sorted(usernames), 50):
        params = {
            "action": "query",
            "list": "usercontribs",
            "ucuser": "|".join(group),
            "ucnamespace": 0,               # mainspace only
            "ucstart": start_iso,           # older bound
            "ucend": end_iso,               # newer bound (exclusive)
            "ucdir": "newer",               # chronological: earliest first
            "uclimit": 500,
            "ucprop": "ids|title|timestamp|comment|sizediff|userid|user|tags",
        }
        data = q(params)
        ucs = data.get("query", {}).get("usercontribs", [])
        for uc in ucs:
            name = uc.get("user")
            if not name or name in earliest:
                continue
            earliest[name] = {
                "userid": uc.get("userid") or "",
                "name": name,
                "t": uc["timestamp"],
                "revid": uc.get("revid"),
                "pageid": uc.get("pageid"),
                "title": uc.get("title"),
                "sizediff": uc.get("sizediff"),
                "tags": "|".join(uc.get("tags", [])),
            }
        seen += len(group)
        if seen % 250 == 0 or seen == total:
            log(f"[usercontribs] processed {seen}/{total} users; found {len(earliest)} earliest edits")
        time.sleep(0.03)  # be polite

    log(f"[usercontribs] earliest edits found: {len(earliest)} / {total}")
    return earliest

def save_rc_csv(user_first_map, out_csv_path: Path):
    """Write rc_first_edits_YYYY-MM.csv with the same schema merge_retention expects."""
    out_csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["userid","name","t","revid","pageid","title","sizediff","tags"]
        )
        w.writeheader()
        for name in sorted(user_first_map.keys()):
            e = user_first_map[name]
            w.writerow({
                "userid": e.get("userid",""),
                "name": name,
                "t": e.get("t",""),
                "revid": e.get("revid",""),
                "pageid": e.get("pageid",""),
                "title": e.get("title",""),
                "sizediff": e.get("sizediff",""),
                "tags": e.get("tags",""),
            })
    log(f"[save] wrote {len(user_first_map)} rows → {out_csv_path}")

def collect_month_for_cohort(month_start_iso, next_month_start_iso,
                             cohort_csv_path: Path, out_csv_path: Path):
    """
    Load cohort usernames from first_edits_YYYY-MM.csv and, for each user,
    fetch their earliest mainspace edit within the same window using usercontribs.
    """
    log(f"[collect] window {month_start_iso} .. {next_month_start_iso}")
    cohort_names = load_cohort_names(cohort_csv_path)
    user_first_map = fetch_earliest_edits_usercontribs(
        cohort_names, month_start_iso, next_month_start_iso
    )
    save_rc_csv(user_first_map, out_csv_path)

if __name__ == "__main__":
    # Windows you care about
    AUG_START = "2024-08-01T00:00:00Z"
    SEP_START = "2024-09-01T00:00:00Z"
    OCT_START = "2024-10-01T00:00:00Z"

    # Cohort (from first_edits.py) and outputs
    AUG_COHORT = RAW / "first_edits_2024-08.csv"
    SEP_COHORT = RAW / "first_edits_2024-09.csv"

    AUG_OUT = RAW / "rc_first_edits_2024-08.csv"
    SEP_OUT = RAW / "rc_first_edits_2024-09.csv"

    log("[main] root  = " + str(ROOT))
    log("[main] raw   = " + str(RAW))

    # Build RC files for historical months using usercontribs:
    collect_month_for_cohort(AUG_START, SEP_START, AUG_COHORT, AUG_OUT)
    collect_month_for_cohort(SEP_START, OCT_START, SEP_COHORT, SEP_OUT)

    log("[main] Done.")
