# import requests, time, math, sys, csv
# from pathlib import Path
# from datetime import datetime

# API = "https://en.wikipedia.org/w/api.php"
# HEAD = {"User-Agent": "Cornell-INFO-Crowdsourcing-Project/1.0 (as3254@cornell.edu)"}

# ROOT = Path(__file__).resolve().parents[2]
# RAW = ROOT / "data" / "raw"
# RAW.mkdir(parents=True, exist_ok=True)

# AUG_START = "2024-08-01T00:00:00Z"
# SEP_START = "2024-09-01T00:00:00Z"
# OCT_START = "2024-10-01T00:00:00Z"

# def log(m): print(m, flush=True)

# def q(params, retries=5):
#     params = {**params, "format":"json", "maxlag":5}
#     for i in range(retries):
#         r = requests.get(API, params=params, headers=HEAD, timeout=30)
#         if r.status_code == 503 and "maxlag" in r.text.lower():
#             sl = 2*(i+1); log(f"[maxlag http] {sl}s"); time.sleep(sl); continue
#         r.raise_for_status()
#         data = r.json()
#         if "error" in data:
#             if data["error"].get("code") == "maxlag":
#                 sl = 2*(i+1); log(f"[maxlag json] {sl}s"); time.sleep(sl); continue
#             raise RuntimeError(f"API error: {data['error']}")
#         return data
#     raise RuntimeError("Exceeded retries")

# def parse_newuser_entry(e):
#     params = e.get("params", {}) or {}
#     userid = e.get("userid") or params.get("newuserid")
#     name = params.get("newuser") or e.get("user") or ""
#     title = e.get("title") or ""
#     if (not name) and title.startswith("User:"):
#         name = title.split("User:", 1)[1]
#     try:
#         userid = int(userid) if userid not in (None, "",) else None
#     except Exception:
#         userid = None
#     return {"userid": userid, "name": name.strip()}

# def new_accounts(lestart_newer_iso, leend_older_iso, max_pages=30):
#     users, cont, page = [], None, 0
#     while True:
#         page += 1
#         params = {
#             "action":"query","list":"logevents","letype":"newusers",
#             "leprop":"title|user|userid|comment|details|timestamp",
#             "ledir":"older","lestart": lestart_newer_iso,"leend": leend_older_iso,"lelimit": 500
#         }
#         if cont:
#             if "lecontinue" in cont: params["lecontinue"] = cont["lecontinue"]
#             if "continue"   in cont: params["continue"]   = cont["continue"]
#         data = q(params)
#         events = data.get("query", {}).get("logevents", [])
#         log(f"[new_accounts] page {page}: events={len(events)}; total={len(users)}")
#         if not events: break
#         for e in events:
#             u = parse_newuser_entry(e)
#             if u["name"]:
#                 users.append(u)
#         cont = data.get("continue")
#         if not cont: log("[new_accounts] done."); break
#         if page >= max_pages: log(f"[new_accounts] page cap {max_pages}; stop."); break
#         time.sleep(0.03)

#     # De-dup by username without pandas to keep integer IDs intact
#     seen, out = set(), []
#     for u in users:
#         if u["name"] in seen: continue
#         seen.add(u["name"])
#         out.append(u)
#     log(f"[new_accounts] unique usernames: {len(out)}")
#     return out

# def batch(iterable, n):
#     b = []
#     for x in iterable:
#         b.append(x); 
#         if len(b) == n:
#             yield b; b = []
#     if b: yield b

# def fetch_first_edits_batched(user_objs):
#     """
#     Returns dict keyed by username with their first mainspace edit (if any).
#     Tries userids in batches of 50; falls back to usernames in batches of 50.
#     """
#     by_name = {u["name"]: {"userid": u["userid"], "name": u["name"]} for u in user_objs}
#     results = {}

#     # 1) Try user IDs in batches
#     id_users = [u for u in user_objs if u["userid"]]
#     for group in batch(id_users, 50):
#         ids = "|".join(str(u["userid"]) for u in group)
#         data = q({
#             "action":"query","list":"usercontribs",
#             "ucuserids": ids, "ucdir":"newer","uclimit":1,"ucnamespace":0,
#             "ucprop":"ids|title|timestamp|comment|tags|sizediff|ids|userid|user"
#         })
#         for uc in data.get("query", {}).get("usercontribs", []):
#             name = uc.get("user")
#             if not name: continue
#             results[name] = {
#                 "t0": uc["timestamp"], "revid": uc["revid"], "pageid": uc["pageid"],
#                 "title": uc["title"], "sizediff": uc.get("sizediff"), "tags": "|".join(uc.get("tags",[]))
#             }
#         time.sleep(0.03)

#     # 2) For those still missing, try by usernames in batches
#     missing_names = [n for n in by_name.keys() if n not in results]
#     for group in batch(missing_names, 50):
#         names = "|".join(group)
#         data = q({
#             "action":"query","list":"usercontribs",
#             "ucuser": names, "ucdir":"newer","uclimit":1,"ucnamespace":0,
#             "ucprop":"ids|title|timestamp|comment|tags|sizediff|ids|userid|user"
#         })
#         for uc in data.get("query", {}).get("usercontribs", []):
#             name = uc.get("user")
#             if not name: continue
#             results[name] = {
#                 "t0": uc["timestamp"], "revid": uc["revid"], "pageid": uc["pageid"],
#                 "title": uc["title"], "sizediff": uc.get("sizediff"), "tags": "|".join(uc.get("tags",[]))
#             }
#         time.sleep(0.03)

#     return results

# def collect_month(month_start_iso, next_month_start_iso, out_csv_path, user_page_cap=25, process_cap=4000, write_chunk=300):
#     """
#     Pull up to `user_page_cap` log pages (~user_page_cap*500 accounts), 
#     then process at most `process_cap` users, writing incrementally every `write_chunk`.
#     """
#     log(f"\n[collect_month] Cohort window: {month_start_iso} .. {next_month_start_iso}")
#     # Pull fewer pages to keep things quick
#     users = new_accounts(lestart_newer_iso=next_month_start_iso, leend_older_iso=month_start_iso, max_pages=user_page_cap)
#     if process_cap and len(users) > process_cap:
#         users = users[:process_cap]
#         log(f"[collect_month] trimming to {process_cap} users")

#     # Fetch first edits in batches
#     name_to_first = fetch_first_edits_batched(users)

#     lower = datetime.fromisoformat(month_start_iso.replace("Z","+00:00"))
#     upper = datetime.fromisoformat(next_month_start_iso.replace("Z","+00:00"))

#     # Prepare writer (incremental)
#     out_csv_path.parent.mkdir(parents=True, exist_ok=True)
#     wrote_header = False
#     kept = 0
#     with open(out_csv_path, "w", newline="", encoding="utf-8") as f:
#         w = csv.DictWriter(f, fieldnames=["userid","name","t0","revid","pageid","title","sizediff","tags"])
#         w.writeheader(); wrote_header = True

#         for idx, u in enumerate(users, 1):
#             name = u["name"]
#             fe = name_to_first.get(name)
#             if not fe: 
#                 if idx % 500 == 0: log(f"[collect_month] processed {idx}, kept={kept} (no first-edit for many accounts)")
#                 continue
#             # Only keep if first edit within this month
#             t0 = datetime.fromisoformat(fe["t0"].replace("Z","+00:00"))
#             if not (lower <= t0 < upper): 
#                 if idx % 500 == 0: log(f"[collect_month] processed {idx}, kept={kept} (out-of-window skips)")
#                 continue

#             row = {
#                 "userid": u["userid"] if u["userid"] is not None else "",
#                 "name": name, "t0": fe["t0"], "revid": fe["revid"], "pageid": fe["pageid"],
#                 "title": fe["title"], "sizediff": fe.get("sizediff"), "tags": fe.get("tags","")
#             }
#             w.writerow(row); kept += 1

#             if kept % write_chunk == 0:
#                 log(f"[collect_month] wrote {kept} rows so far → {out_csv_path}")
#                 f.flush()

#     log(f"[collect_month] Saved {kept} newcomers → {out_csv_path}")

# if __name__ == "__main__":
#     log("[main] root  = " + str(ROOT))
#     log("[main] raw   = " + str(RAW))

#     # August 2024 (small, fast pull)
#     collect_month(AUG_START, SEP_START, RAW / "first_edits_2024-08.csv",
#                   user_page_cap=12,   # ~6,000 accounts scanned
#                   process_cap=4000,   # only process first 4k accounts
#                   write_chunk=200)

#     # September 2024
#     collect_month(SEP_START, OCT_START, RAW / "first_edits_2024-09.csv",
#                   user_page_cap=12,
#                   process_cap=4000,
#                   write_chunk=200)

#     # # August 2024
#     # collect_month(
#     #   AUG_START, SEP_START, RAW / "first_edits_2024-08.csv",
#     #   user_page_cap=200,    # scan ~100k account creations
#     #   process_cap=100000,   # process up to 100k users
#     #   write_chunk=200       # save every 200 kept rows
#     # )

#     # # September 2024
#     # collect_month(
#     #   SEP_START, OCT_START, RAW / "first_edits_2024-09.csv",
#     #   user_page_cap=200, process_cap=100000, write_chunk=200
#     # )

#     log("[main] Done.")

# src/collect/rc_first_edits.py
import requests, time, csv
from pathlib import Path
from datetime import datetime, timezone

API  = "https://en.wikipedia.org/w/api.php"
HEAD = {"User-Agent": "Cornell-INFO-Crowdsourcing-Project/1.0 (as3254@cornell.edu)"}

ROOT = Path(__file__).resolve().parents[2]
RAW  = ROOT / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)

def log(m): print(m, flush=True)

def q(params, retries=5):
    params = {**params, "format":"json", "maxlag":5}
    for i in range(retries):
        r = requests.get(API, params=params, headers=HEAD, timeout=30)
        if r.status_code == 503 and "maxlag" in r.text.lower():
            sl = 2*(i+1); log(f"[maxlag http] {sl}s"); time.sleep(sl); continue
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            if data["error"].get("code") == "maxlag":
                sl = 2*(i+1); log(f"[maxlag json] {sl}s"); time.sleep(sl); continue
            raise RuntimeError(f"API error: {data['error']}")
        return data
    raise RuntimeError("Exceeded retries")

def parse_iso_z(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)

def load_cohort(path: Path):
    """Return list of dicts with name + t0 + userid from first_edits_YYYY-MM.csv."""
    out = []
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            name = (row.get("name") or "").strip()
            if not name: continue
            t0 = parse_iso_z(row["t0"])
            try:
                userid = int(row["userid"]) if row.get("userid") else None
            except Exception:
                userid = None
            out.append({"name": name, "t0": t0, "userid": userid})
    log(f"[cohort] {path.name}: {len(out)} users")
    return out

def fetch_next_edit_after_t0(user, month_start_iso: str, next_month_start_iso: str):
    """
    For a single user, return their first mainspace edit STRICTLY AFTER t0
    and before next_month_start_iso. Otherwise return None.
    """
    name = user["name"]
    t0   = user["t0"]

    # Query starting at t0, chronological order; ask for up to 2 edits
    data = q({
        "action": "query",
        "list": "usercontribs",
        "ucuser": name,
        "ucnamespace": 0,
        "ucdir": "newer",
        "ucstart": t0.isoformat().replace("+00:00", "Z"),   # inclusive boundary
        "ucend": next_month_start_iso,                      # exclusive upper bound
        "uclimit": 2,
        "ucprop": "ids|title|timestamp|comment|sizediff|userid|user|tags",
    })

    ucs = data.get("query", {}).get("usercontribs", [])
    # Find the first contrib STRICTLY after t0 (ignore the edit at exactly t0)
    for uc in ucs:
        t = parse_iso_z(uc["timestamp"])
        if t > t0:
            return {
                "userid": uc.get("userid") or "",
                "name": name,
                "t": uc["timestamp"],
                "revid": uc.get("revid"),
                "pageid": uc.get("pageid"),
                "title": uc.get("title"),
                "sizediff": uc.get("sizediff"),
                "tags": "|".join(uc.get("tags", [])),
            }
    return None

def collect_next_edits_for_cohort(month_start_iso, next_month_start_iso,
                                  cohort_csv_path: Path, out_csv_path: Path,
                                  sleep_s: float = 0.02):
    cohort = load_cohort(cohort_csv_path)
    found = {}
    for i, user in enumerate(cohort, 1):
        rec = fetch_next_edit_after_t0(user, month_start_iso, next_month_start_iso)
        if rec: found[user["name"]] = rec
        if i % 50 == 0: log(f"[progress] {i}/{len(cohort)} processed; found {len(found)}")
        time.sleep(sleep_s)  # be polite

    out_csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["userid","name","t","revid","pageid","title","sizediff","tags"])
        w.writeheader()
        for name in sorted(found.keys()):
            w.writerow(found[name])
    log(f"[save] wrote {len(found)} rows → {out_csv_path}")

if __name__ == "__main__":
    AUG_START = "2024-08-01T00:00:00Z"
    SEP_START = "2024-09-01T00:00:00Z"
    OCT_START = "2024-10-01T00:00:00Z"

    ROOT = Path(__file__).resolve().parents[2]
    RAW  = ROOT / "data" / "raw"

    AUG_COHORT = RAW / "first_edits_2024-08.csv"
    SEP_COHORT = RAW / "first_edits_2024-09.csv"

    AUG_OUT = RAW / "rc_second_edits_2024-08.csv"
    SEP_OUT = RAW / "rc_second_edits_2024-09.csv"

    log("[main] fetching true second edits (strictly after t0)…")
    collect_next_edits_for_cohort(AUG_START, SEP_START, AUG_COHORT, AUG_OUT)
    collect_next_edits_for_cohort(SEP_START, OCT_START, SEP_COHORT, SEP_OUT)
    log("[main] Done.")
