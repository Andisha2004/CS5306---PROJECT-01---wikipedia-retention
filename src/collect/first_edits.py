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

#     # # August 2024 (small, fast pull)
#     # collect_month(AUG_START, SEP_START, RAW / "first_edits_2024-08.csv",
#     #               user_page_cap=12,   # ~6,000 accounts scanned
#     #               process_cap=4000,   # only process first 4k accounts
#     #               write_chunk=200)

#     # # September 2024
#     # collect_month(SEP_START, OCT_START, RAW / "first_edits_2024-09.csv",
#     #               user_page_cap=12,
#     #               process_cap=4000,
#     #               write_chunk=200)

#     # August 2024
#     collect_month(
#       AUG_START, SEP_START, RAW / "first_edits_2024-08.csv",
#       user_page_cap=200,    # scan ~100k account creations
#       process_cap=100000,   # process up to 100k users
#       write_chunk=200       # save every 200 kept rows
#     )

#     # September 2024
#     collect_month(
#       SEP_START, OCT_START, RAW / "first_edits_2024-09.csv",
#       user_page_cap=200, process_cap=100000, write_chunk=200
#     )

#     log("[main] Done.")

# src/collect/first_edits.py
import requests, time, csv
from pathlib import Path
from datetime import datetime

API  = "https://en.wikipedia.org/w/api.php"
HEAD = {"User-Agent": "Cornell-INFO-Crowdsourcing-Project/1.0 (as3254@cornell.edu)"}

# Resolve repo root and data dirs
ROOT = Path(__file__).resolve().parents[2]
RAW  = ROOT / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)

# Cohort windows (UTC)
AUG_START = "2024-08-01T00:00:00Z"
SEP_START = "2024-09-01T00:00:00Z"
OCT_START = "2024-10-01T00:00:00Z"

# Tunables
SLEEP_SECS = 0.0  # set to 0.02 later to be extra polite once stable

def log(m): print(m, flush=True)

def q(params, retries=5):
    """MediaWiki API helper with simple maxlag backoff."""
    params = {**params, "format":"json", "maxlag":5}
    for i in range(retries):
        r = requests.get(API, params=params, headers=HEAD, timeout=30)
        # HTTP-level maxlag
        if r.status_code == 503 and "maxlag" in r.text.lower():
            sl = 2*(i+1); log(f"[maxlag http] backing off {sl}s"); time.sleep(sl); continue
        r.raise_for_status()
        data = r.json()
        # JSON-level error
        if "error" in data:
            if data["error"].get("code") == "maxlag":
                sl = 2*(i+1); log(f"[maxlag json] backing off {sl}s"); time.sleep(sl); continue
            raise RuntimeError(f"API error: {data['error']}")
        return data
    raise RuntimeError("Exceeded retries to MediaWiki")

def parse_newuser_entry(e):
    """Extract (userid, username) from a logevents row."""
    params = e.get("params", {}) or {}
    userid = e.get("userid") or params.get("newuserid")
    name   = params.get("newuser") or e.get("user") or ""
    title  = e.get("title") or ""
    if (not name) and title.startswith("User:"):
        name = title.split("User:", 1)[1]
    try:
        userid = int(userid) if userid not in (None, "") else None
    except Exception:
        userid = None
    return {"userid": userid, "name": (name or "").strip()}

def new_accounts(lestart_newer_iso, leend_older_iso, max_pages=60):
    """
    Pull newly CREATED accounts (not autocreate) between two bounds.
    We walk from newer->older with ledir=older.
    """
    users, cont, page = [], None, 0
    while True:
        page += 1
        params = {
            "action":"query",
            "list":"logevents",
            "letype":"newusers",
            # Filter OUT autocreate; keep only manual creations
            "leaction":"newusers/create|newusers/create2",
            "leprop":"title|user|userid|comment|details|timestamp",
            "ledir":"older",
            "lestart": lestart_newer_iso,   # newer bound
            "leend":   leend_older_iso,     # older bound
            "lelimit": 500
        }
        if cont:
            if "lecontinue" in cont: params["lecontinue"] = cont["lecontinue"]
            if "continue"   in cont: params["continue"]   = cont["continue"]

        data = q(params)
        events = data.get("query", {}).get("logevents", [])
        log(f"[new_accounts] page {page}: events={len(events)}; total={len(users)}")
        if not events: break

        for e in events:
            u = parse_newuser_entry(e)
            if u["name"]:
                users.append(u)

        cont = data.get("continue")
        if not cont:
            log("[new_accounts] done."); break
        if page >= max_pages:
            log(f"[new_accounts] hit page cap {max_pages}; stop."); break
        time.sleep(SLEEP_SECS)

    # De-dup by username without pandas (keeps int userids intact)
    seen, out = set(), []
    for u in users:
        if u["name"] in seen: continue
        seen.add(u["name"])
        out.append(u)
    log(f"[new_accounts] unique usernames: {len(out)}")
    return out

def batch(iterable, n):
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) == n:
            yield buf; buf = []
    if buf: yield buf

def fetch_first_edits_batched(user_objs):
    """
    Earliest (dir=newer, limit=1) mainspace edit per user.
    Try userids first in batches (fast), then usernames for those missing.
    Returns: dict name -> {t0, revid, pageid, title, sizediff, tags}
    """
    results = {}
    # 1) Try by userids
    with_ids = [u for u in user_objs if u["userid"]]
    for grp in batch(with_ids, 50):
        ids = "|".join(str(u["userid"]) for u in grp)
        data = q({
            "action":"query","list":"usercontribs",
            "ucuserids": ids,
            "ucdir":"newer","uclimit":1,"ucnamespace":0,
            "ucprop":"ids|title|timestamp|comment|tags|sizediff|ids|userid|user"
        })
        for uc in data.get("query", {}).get("usercontribs", []):
            name = uc.get("user")
            if not name: continue
            results[name] = {
                "t0": uc["timestamp"], "revid": uc.get("revid"),
                "pageid": uc.get("pageid"), "title": uc.get("title"),
                "sizediff": uc.get("sizediff"), "tags": "|".join(uc.get("tags", []))
            }
        time.sleep(SLEEP_SECS)

    # 2) Fallback by usernames
    missing = [u["name"] for u in user_objs if u["name"] not in results]
    for grp in batch(missing, 50):
        names = "|".join(grp)
        data = q({
            "action":"query","list":"usercontribs",
            "ucuser": names,
            "ucdir":"newer","uclimit":1,"ucnamespace":0,
            "ucprop":"ids|title|timestamp|comment|tags|sizediff|ids|userid|user"
        })
        for uc in data.get("query", {}).get("usercontribs", []):
            name = uc.get("user")
            if not name: continue
            results[name] = {
                "t0": uc["timestamp"], "revid": uc.get("revid"),
                "pageid": uc.get("pageid"), "title": uc.get("title"),
                "sizediff": uc.get("sizediff"), "tags": "|".join(uc.get("tags", []))
            }
        time.sleep(SLEEP_SECS)

    return results

def collect_month(month_start_iso, next_month_start_iso, out_csv_path,
                  user_page_cap=60, process_cap=30000, write_chunk=200,
                  target_kept=3000):
    """
    Pull new accounts (manual creations only), find each user's *first-ever* mainspace edit,
    keep those whose first-ever edit falls inside the month, and write incrementally.
    Early-stop once we hit target_kept.
    """
    log(f"\n[collect_month] Cohort window: {month_start_iso} .. {next_month_start_iso}")

    # 1) Pull a finite number of new accounts (fast)
    users = new_accounts(lestart_newer_iso=next_month_start_iso,
                         leend_older_iso=month_start_iso,
                         max_pages=user_page_cap)
    if process_cap and len(users) > process_cap:
        users = users[:process_cap]
        log(f"[collect_month] trimming to {process_cap} users before lookups")

    # 2) Fetch earliest edits in big batches
    name_to_first = fetch_first_edits_batched(users)

    lower = datetime.fromisoformat(month_start_iso.replace("Z","+00:00"))
    upper = datetime.fromisoformat(next_month_start_iso.replace("Z","+00:00"))

    # 3) Write incrementally
    out_csv_path.parent.mkdir(parents=True, exist_ok=True)
    kept = 0
    with open(out_csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["userid","name","t0","revid","pageid","title","sizediff","tags"]
        )
        w.writeheader()

        for idx, u in enumerate(users, 1):
            name = u["name"]
            fe = name_to_first.get(name)
            if not fe:
                if idx % 500 == 0: log(f"[collect_month] processed {idx}, kept={kept} (many no-mainspace-ever)")
                continue
            t0 = datetime.fromisoformat(fe["t0"].replace("Z","+00:00"))
            if not (lower <= t0 < upper):
                # their first-ever edit is outside this month → not a newcomer in this month
                if idx % 500 == 0: log(f"[collect_month] processed {idx}, kept={kept} (out-of-window)")
                continue

            row = {
                "userid": u["userid"] if u["userid"] is not None else "",
                "name": name, "t0": fe["t0"], "revid": fe["revid"], "pageid": fe["pageid"],
                "title": fe["title"], "sizediff": fe.get("sizediff"), "tags": fe.get("tags","")
            }
            w.writerow(row); kept += 1
            if kept % write_chunk == 0:
                log(f"[collect_month] wrote {kept} rows → {out_csv_path}")
                f.flush()

            # EARLY STOP when you have enough for your cohort
            if target_kept and kept >= target_kept:
                log(f"[collect_month] hit target_kept={target_kept}; stopping early.")
                return

    log(f"[collect_month] Saved {kept} newcomers → {out_csv_path}")

if __name__ == "__main__":
    log("[main] root  = " + str(ROOT))
    log("[main] raw   = " + str(RAW))

    # August 2024 — aim for ~3k newcomers
    collect_month(
        AUG_START, SEP_START, RAW / "first_edits_2024-08.csv",
        user_page_cap=60,      # ~30k manual creations scanned
        process_cap=30000,     # cap processed users
        write_chunk=200,       # incremental flush
        target_kept=3000       # stop once you have enough
    )

    # September 2024 — same settings
    collect_month(
        SEP_START, OCT_START, RAW / "first_edits_2024-09.csv",
        user_page_cap=60, process_cap=30000, write_chunk=200, target_kept=3000
    )

    log("[main] Done.")
