import os, sys, json, time, base64, logging, math, re
import requests, yaml
from slugify import slugify

# ---------- logging ----------
LOG = logging.getLogger("runner")
logging.basicConfig(filename="run.log", level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

# ---------- env ----------
WP_URL = (os.getenv("WP_SITE_URL") or "").rstrip("/")
WP_USER = os.getenv("WP_USERNAME")
WP_APP_PW = os.getenv("WP_APP_PASSWORD")
ALERT = os.getenv("ALERT_WEBHOOK_URL")

# ---------- helpers ----------
def b64cred(user, pwd):
    return base64.b64encode(f"{user}:{pwd}".encode()).decode()

def http_json(method, url, **kw):
    r = requests.request(method, url, timeout=30, **kw)
    if not r.ok:
        # 生メッセージを短く残す（デバッグ用）
        LOG.error(f"http_error: {method} {url} -> {r.status_code} {r.text[:400]}")
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
    return r.json()

def sanitize_keyword(kw: str) -> str:
    """改行/タブ/全角スペースを除去し、連続空白を1つに"""
    if not kw:
        return ""
    kw = kw.replace("\u3000", " ")                      # 全角スペース→半角
    kw = kw.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    kw = re.sub(r"\s+", " ", kw)                        # 連続空白圧縮
    return kw.strip()

def ensure_categories(names):
    """
    APIで新規作成はしない（権限最小のため）。
    見つからなければ「未分類」にフォールバック。
    """
    ids = []
    for name in names:
        try:
            q = http_json("GET", f"{WP_URL}/wp-json/wp/v2/categories",
                          params={"search": name, "per_page": 100})
            cid = next((c["id"] for c in q if c["name"].lower() == name.lower()), None)
            if cid:
                ids.append(cid)
                continue
            # fallback: Uncategorized
            default = http_json("GET", f"{WP_URL}/wp-json/wp/v2/categories",
                                params={"slug": "uncategorized"})
            if default:
                ids.append(default[0]["id"])
                LOG.warning(f"category '{name}' not found; fallback to Uncategorized")
            else:
                ids.append(1)  # 最終手段
        except Exception as e:
            LOG.error(f"category ensure failed: {e}; fallback to id=1")
            ids.append(1)
    return ids

def wp_post_exists(slug):
    q = http_json("GET", f"{WP_URL}/wp-json/wp/v2/posts", params={"slug": slug})
    return len(q) > 0

def rakuten_items(app_id, kw, endpoint, max_per_seed, genreId=None):
    app_id = (app_id or "").strip()                     # 改行や空白を除去
    kw = sanitize_keyword(kw)
    params = {"applicationId": app_id, "keyword": kw,
              "hits": int(max_per_seed), "format": "json"}
    if genreId:
        params["genreId"] = genreId
    r = requests.get(endpoint, params=params, timeout=30)
    if not r.ok:
        LOG.error(f"rakuten_api_error: HTTP {r.status_code} - {r.text[:400]}")
        r.raise_for_status()
    return r.json().get("Items", [])

def enrich(items):
    out = []
    for it in items:
        i = it["Item"]
        price = i.get("itemPrice") or 0
        rev = float(i.get("reviewAverage") or 0.0)
        rct = int(i.get("reviewCount") or 0)
        density = rev * math.log1p(max(rct, 1))
        out.append({
            "name": i.get("itemName"),
            "url": i.get("itemUrl"),
            "image": (i.get("mediumImageUrls") or [{"imageUrl": ""}])[0]["imageUrl"],
            "price": price, "review_avg": rev, "review_count": rct,
            "score": round(density, 2)
        })
    out.sort(key=lambda x: (-x["score"], x["price"]))
    return out

def render_html(kw, items, conf, cats):
    n = min(10, len(items))
    title_patterns = conf.get("ab_tests", {}).get("title_patterns")
    if conf["content"].get("ab_test") and title_patterns:
        title = title_patterns[hash(kw) % len(title_patterns)]
    else:
        title = f"{kw}の比較"
    title = title.format(kw=kw, n=n)

    rows = []
    for i, x in enumerate(items[:n], start=1):
        rows.append(
            f"<tr><td>{i}</td><td><a href='{x['url']}' rel='sponsored noopener'>{x['name']}</a></td>"
            f"<td>¥{x['price']}</td><td>{x['review_avg']} ({x['review_count']})_]()_]()
