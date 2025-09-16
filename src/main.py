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
            f"<td>¥{x['price']}</td><td>{x['review_avg']} ({x['review_count']})</td><td>{x['score']}</td></tr>"
        )
    table = ("<table><thead><tr><th>#</th><th>商品</th><th>価格</th><th>評価</th><th>指標</th></tr></thead>"
             "<tbody>" + "".join(rows) + "</tbody></table>")

    disclosure = conf["site"]["affiliate_disclosure"]
    body = (
        f"<p>{disclosure}</p><h2>{kw}の要点</h2><ul>"
        f"<li>レビュー密度（⭐×件数の対数）で上位{n}件を抽出</li>"
        f"<li>在庫や価格は変動します。購入前にリンク先で最新情報を確認してください。</li></ul>{table}"
        f"<p>※本ページはAPIデータをもとに自動生成し、転記ではなく数値集計で付加価値を加えています。</p>"
    )
    return title, body

def notify(msg):
    if not ALERT:
        return
    try:
        requests.post(ALERT, json={"content": msg}, timeout=10)
    except Exception as e:
        LOG.error(f"alert failed: {e}")

# ---------- main ----------
def main():
    # 設定ファイルの取得
    try:
        cfg_path = sys.argv[sys.argv.index("--config") + 1]
    except ValueError:
        cfg_path = "config/app.yaml"
    with open(cfg_path, "r", encoding="utf-8") as f:
        conf = yaml.safe_load(f)

    # ABテスト/ルール読込
    conf["ab_tests"] = yaml.safe_load(open("src/ab_tests.yaml", "r", encoding="utf-8")) \
        if conf.get("content", {}).get("ab_test") else {}
    rules = yaml.safe_load(open(conf["review"]["rules_file"], "r", encoding="utf-8"))

    # カテゴリ確定（作成はしない）
    cats = ensure_categories(conf["site"]["category_names"])

    posted = 0
    for raw_kw in conf["keywords"]["seeds"]:
        kw = sanitize_keyword(raw_kw)
        if not kw:
            continue
        if posted >= int(conf["site"]["posts_per_run"]):
            break

        slug = slugify(kw)
        if wp_post_exists(slug):
            LOG.info(f"skip exists: {kw}")
            continue

        LOG.info(f"query kw='{kw}'")
        arr = rakuten_items(
            (os.getenv("RAKUTEN_APP_ID") or "").strip(),
            kw,
            conf["data_sources"]["rakuten"]["endpoint"],
            conf["data_sources"]["rakuten"]["max_per_seed"],
            conf["data_sources"]["rakuten"].get("genreId")
        )
        items = [
            it for it in enrich(arr)
            if it["price"] >= conf["content"]["price_floor"]
            and it["review_avg"] >= conf["content"]["review_floor"]
        ]
        if len(items) < 3:
            LOG.info(f"thin content for {kw}, skipping")
            continue

        title, html = render_html(kw, items, conf, cats)

        # ルールチェック
        blocked = False
        for bad in rules["prohibited_phrases"]:
            if bad in title or bad in html:
                LOG.info(f"blocked phrase: {bad} in {kw}")
                blocked = True
                break
        if blocked:
            continue

        # 投稿
        payload = {
            "title": title,
            "slug": slug,
            "status": "publish",
            "content": html,
            "categories": cats
        }
        headers = {
            "Authorization": f"Basic {b64cred(WP_USER, WP_APP_PW)}",
            "Content-Type": "application/json"
        }
        try:
            http_json("POST", f"{WP_URL}/wp-json/wp/v2/posts",
                      data=json.dumps(payload), headers=headers)
            posted += 1
            LOG.info(f"posted: {kw}")
        except Exception as e:
            LOG.error(f"post failed: {e}")
            notify(f"[AUTO-REV] post failed for {kw}: {e}")
            time.sleep(2)

    LOG.info(f"done, posted={posted}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        notify(f"[AUTO-REV] job failed: {e}")
        raise
