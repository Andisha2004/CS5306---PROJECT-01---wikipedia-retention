import requests, time, csv
from pathlib import Path
from datetime import datetime, timedelta

API = "https://en.wikipedia.org/w/api.php"
HEAD = {"User-Agent": "Cornell-INFO-Crowdsourcing-Project/1.0 (as3254@cornell.edu)"}

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw"
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

def month_rc(first_iso, next_iso, max_pages=200):
    """Scan recentchanges in mainspace (ns=0) for the month; return earliest edit per user."""
    log(f"[recentchanges] window {first_iso} .. {next_iso}")
    users_first = {}  # name -> first edit dict
    cont = None
    pages = 0
    while True:
        pages += 1
        params = {
            "action":"query", "list":"recentchanges",
            "rcstart": next_iso,    # newer
            "rcend": first_iso,     # older
            "rcdir":"older",        # walk backward in time
            "rcnamespace": 0,       # mainspace only
            "rctype":"edit|new",    # edits and new pages
            "rcprop":"user|userid|title|ids|timestamp|comment|sizes|flags|tags",
            "rclimit": 500
        }
        if cont:
            params.update(cont)
        data = q(params)
        rcs = data.get("query", {}).get("recentchanges", [])
        log(f"[recentchanges] page {pages}: {len(rcs)} rows (unique users so far: {len(users_first)})")
        if not rcs: break

        # record earliest-in-month (since we're going older, first time we see user is their earliest)
        for rc in rcs:
            name = rc.get("user")
            if not name or name in users_first:
                continue
            users_first[name] = {
                "userid": rc.get("userid") or "",
                "name": name,
                "t0": rc["timestamp"],
                "revid": rc.get("revid"),
                "pageid": rc.get("pageid"),
                "title": rc.get("title"),
                "sizediff": (rc.get("newlen") or 0) - (rc.get("oldlen") or 0),
                "tags": "|".join(rc.get("tags", []))
            }

        cont = data.get("continue")
        if not cont:
            log("[recentchanges] done."); break
        if pages >= max_pages:
            log(f"[recentchanges] hit cap {max_pages}; stop."); break
        time.sleep(0.03)

    return list(users_first.values())

def save_csv(rows, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["userid","name","t0","revid","pageid","title","sizediff","tags"])
        w.writeheader()
        for r in rows: w.writerow(r)
    log(f"[save] {len(rows)} rows → {path}")

def collect_month(month_start_iso, next_month_start_iso, out_csv_path, page_cap=200, sample_cap=None):
    rows = month_rc(month_start_iso, next_month_start_iso, max_pages=page_cap)
    # (optional) downsample for speed while prototyping
    if sample_cap and len(rows) > sample_cap:
        rows = rows[:sample_cap]
        log(f"[collect_month] downsampled to {sample_cap}")
    save_csv(rows, out_csv_path)

if __name__ == "__main__":
    # August 2024
    collect_month("2024-08-01T00:00:00Z", "2024-09-01T00:00:00Z",
                  RAW / "rc_first_edits_2024-08.csv",
                  page_cap=200, sample_cap=None)
    # September 2024
    collect_month("2024-09-01T00:00:00Z", "2024-10-01T00:00:00Z",
                  RAW / "rc_first_edits_2024-09.csv",
                  page_cap=200, sample_cap=None)
