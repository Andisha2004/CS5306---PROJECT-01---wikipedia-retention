import time
import requests
from typing import Dict, Iterator
from requests.adapters import HTTPAdapter, Retry

API_URL = "https://en.wikipedia.org/w/api.php"
UA = "WikipediaRetentionStudy/1.0 (research; contact: example@example.com)"

class MWClient:
    def __init__(self, url: str = API_URL, sleep: float = 0.1):
        self.url = url
        self.sleep = sleep
        self.session = requests.Session()
        retries = Retry(
            total=5, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504)
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.headers.update({"User-Agent": UA})

    def query(self, params: Dict) -> Dict:
        """One-shot query (no continuation)."""
        params = {"format": "json", "formatversion": "2", **params}
        r = self.session.get(self.url, params=params, timeout=30)
        r.raise_for_status()
        time.sleep(self.sleep)
        return r.json()

    def query_all(self, params: Dict, cont_key: str = "continue") -> Iterator[Dict]:
        """Generator over all pages of a query (handles 'continue')."""
        base = {"format": "json", "formatversion": "2", **params}
        cont = {}
        while True:
            merged = {**base, **cont}
            r = self.session.get(self.url, params=merged, timeout=30)
            r.raise_for_status()
            data = r.json()
            yield data
            time.sleep(self.sleep)
            if cont_key in data:
                cont = data[cont_key]
            else:
                break
