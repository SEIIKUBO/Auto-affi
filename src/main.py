import os, sys, json, time, base64, logging, math, re
import requests, yaml
from slugify import slugify

# ------------ logging (file + console) ------------
LOG = logging.getLogger("runner")
LOG.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
fh = logging.FileHandler("run.log", encoding="utf-8")
fh.setFormatter(fmt); LOG.addHandler(fh)
ch = logging.StreamHandler()
ch.setFormatter(fmt); LOG.addHandler(ch)

# ------------ env ------------
WP_URL = (os.getenv("WP_SITE_URL") or "").rstrip("/")
WP_USER = os.getenv("WP_USERNAME")
WP_APP_PW = os.getenv("WP_APP_PASSWORD")
ALERT = os.getenv("ALERT_WEBHOOK_URL")

# ------------ helpers ------------
def b64cred(user, pwd):
    return base64.b64encode(f"{user}:{pwd}".encode()).decode()

def http_json(method, url, **kw):
    r = requests.request(method, url, timeout=30, **kw)
    if not r.ok:
        LOG.error(f"http_error: {method} {url} -> {r.status_code} {r.text[:400]}")
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
    return r.json()

def sanitize_keyword(kw: str) -> str:
    if not kw:
        return ""
    kw = kw.replace("\u3000", " ")                      # 全角スペース→半角
    kw = kw.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    kw = re.sub(r"\s+", " ", kw)
    return kw.strip()

def ensure_categories(names):
    """作成はしない。見つからなければ '未分類' にフォールバック。"""
    ids = []
    for name in names:
        try:
            q = http_json("GET", f"{WP_URL}/wp-json/wp/v2/categories",
                          params={"search": name, "per_page": 100})
            cid = next((c["id"] for c in q if c["name"].lower() == name.lower()), None)
            if cid:
                ids.append(cid); continue
            default = http_json("GET", f"{WP_URL}/wp-json/wp/v2/categories",
                                params={"slug": "uncategorized"})
            ids.append(default[0]["id"] if default else 1)
            LOG.warning(f"category '{name}' not found; fallback to Uncategorized")
        except Exception as e:
            LOG.error(f"category ensure failed: {e}; fallback to id=1")
            ids.append(1)
    return ids

def wp_post_exists(slug):
    q = http_json("GET", f"{WP_URL}/wp-json/wp/v2/posts", params={"slug": slug})
    return len(q) > 0

def rakuten_items(app_id, kw, endpoint, max_per_seed, genreId=None):
    app_id = (app_id or "").strip()
    kw = sanitize_keyword(kw)
    params = {"applicationId": app_id, "keyword": kw,
              "hits": int(max_per_seed), "format": "json"}
    if genreId: params["genreId"] = genreId
    try:
        r = requests.get(endpoint, params=params, timeout=30)
        if not r.ok:
            LOG.error(f"rakuten_api_error kw='{kw}': HTTP {r.status_code} - {r.text[:300]}")
            if r.status_code == 400:   # この語だけスキップ
                return []
            r.raise_for_status()
        j = r.json()
        return j.get("Items", [])
    except Exception as e:
        LOG.error(f"rakuten_request_exception kw='{kw}': {e}")
        return []

def enrich(items):
    out = []
    for it in items:
        i = it["Item"]
        price = i.get("itemPrice") or 0
        rev = float(i.get("reviewAverage") or 0.0)
        rct = int(i.get("reviewCount") or 0)
        density = rev * math.log1p(max(rct, 1))
        out.append({
            "name
