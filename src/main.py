import os, sys, json, time, base64, logging, math, re
import requests, yaml
from slugify import slugify

# ------------ logging (file + console) ------------
LOG = logging.getLogger("runner")
LOG.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
fh = logging.FileHandler("run.log", encoding="utf-8"); fh.setFormatter(fmt); LOG.addHandler(fh)
ch = logging.StreamHandler(); ch.setFormatter(fmt); LOG.addHandler(ch)

# ------------ env ------------
WP_URL = (os.getenv("WP_SITE_URL") or "").rstrip("/")
WP_USER = os.getenv("WP_USERNAME")
WP_APP_PW = os.getenv("WP_APP_PASSWORD")
ALERT = os.getenv("ALERT_WEBHOOK_URL")

def b64cred(u, p): return base64.b64encode(f"{u}:{p}".encode()).decode()

def http_json(method, url, **kw):
    r = requests.request(method, url, timeout=30, **kw)
    if not r.ok:
        LOG.error(f"http_error: {method} {url} -> {r.status_code} {r.text[:400]}")
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
    return r.json()

# ------------ helpers ------------
def sanitize_keyword(kw:str)->str:
    if not kw: return ""
    kw = kw.replace("\u3000"," ").replace("\r"," ").replace("\n"," ").replace("\t"," ")
    kw = re.sub(r"\s+"," ",kw)
    return kw.strip()

def stars(avg: float) -> str:
    # 表示用（例：★★★★☆ 4.2）
    full = int(round(avg))
    full = max(0, min(full, 5))
    return "★"*full + "☆"*(5-full) + f" {avg:.1f}"

def ensure_categories(names):
    ids=[]
    for name in names:
        try:
            q=http_json("GET", f"{WP_URL}/wp-json/wp/v2/categories", params={"search":name,"per_page":100})
            cid=next((c["id"] for c in q if c["name"].lower()==name.lower()), None)
            if cid: ids.append(cid); continue
            default=http_json("GET", f"{WP_URL}/wp-json/wp/v2/categories", params={"slug":"uncategorized"})
            ids.append(default[0]["id"] if default else 1)
            LOG.warning(f"category '{name}' not found; fallback to Uncategorized")
        except Exception as e:
            LOG.error(f"category ensure failed: {e}; fallback to id=1"); ids.append(1)
    return ids

def wp_post_exists(slug):
    q=http_json("GET", f"{WP_URL}/wp-json/wp/v2/posts", params={"slug":slug})
    return len(q)>0

def rakuten_items(app_id, kw, endpoint, hits, genreId=None):
    app_id=(app_id or "").strip(); kw=sanitize_keyword(kw)
    params={"applicationId":app_id,"keyword":kw,"hits":int(hits),"format":"json"}
    if genreId: params["genreId"]=genreId
    try:
        r=requests.get(endpoint, params=params, timeout=30)
        if not r.ok:
            LOG.error(f"rakuten_api_error kw='{kw}': HTTP {r.status_code} - {r.text[:300]}")
            if r.status_code==400: return []
            r.raise_for_status()
        return (r.json() or {}).get("Items",[])
    except Exception as e:
        LOG.error(f"rakuten_request_exception kw='{kw}': {e}")
        return []

def enrich(items):
    out=[]
    for it in items:
        i=it["Item"]; price=i.get("itemPrice") or 0
        rev=float(i.get("reviewAverage") or 0.0); rct=int(i.get("reviewCount") or 0)
        score=rev*math.log1p(max(rct,1))
        out.append({
            "name":i.get("itemName"),
            "url":i.get("itemUrl"),
            "image":(i.get("mediumImageUrls") or [{"imageUrl":""}])[0]["imageUrl"],
            "price":price,"review_avg":rev,"review_count":rct,"score":round(score,2)
        })
    out.sort(key=lambda x:(-x["score"], x["price"]))
    return out

# ------------ content render (Gutenberg blocks) ------------
def render_blocks(kw, items, conf):
    disclosure = conf["site"]["affiliate_disclosure"]
    n = min(10, len(items))
    top = items[:3]               # カードで魅せる
    rest = items[:n]              # 表で一覧

    # タイトル（ABテスト対応）
    title_patterns = conf.get("ab_tests", {}).get("title_patterns")
    title = (title_patterns[hash(kw)%len(title_patterns)] if conf["content"].get("ab_test") and title_patterns
             else f"{kw}の比較")
    title = title.format(kw=kw, n=n)

    blocks = []

    # 開示文
    blocks.append(f"<!-- wp:paragraph --><p>{disclosure}</p><!-- /wp:paragraph -->")

    # リード
    lead = f"{kw}をレビュー密度（⭐×件数の対数）と価格で自動集計。『転記』ではなく数値で比較し、買いどきを示します。"
    blocks.append(f"<!-- wp:paragraph --><p>{lead}</p><!-- /wp:paragraph -->")

    # 目次
    toc = "<!-- wp:list --><ul>" \
          "<li><a href=\"#top-picks\">上位3つ</a></li>" \
          "<li><a href=\"#compare\">比較表</a></li>" \
          "<li><a href=\"#howto\">失敗しない選び方</a></li>" \
          "<li><a href=\"#faq\">FAQ</a></li></ul><!-- /wp:list -->"
    blocks.append(toc)

    # 上位3つ（カラム + 画像 + ボタン）
    blocks.append(f"<!-- wp:heading --><h2 id=\"top-picks\">上位3つのおすすめ</h2><!-- /wp:heading -->")
    for x in top:
        blocks.append("<!-- wp:columns -->"
                      "<div class=\"wp-block-columns\">"
                        "<div class=\"wp-block-column\" style=\"flex-basis:25%\">"
                          f"<!-- wp:image {{\"sizeSlug\":\"medium\"}} --><figure class=\"wp-block-image size-medium\">"
                          f"<img src=\"{x['image']}\" alt=\"{x['name']}\"/></figure><!-- /wp:image -->"
                        "</div>"
                        "<div class=\"wp-block-column\" style=\"flex-basis:75%\">"
                          f"<!-- wp:heading {{\"level\":3}} --><h3>{x['name']}</h3><!-- /wp:heading -->"
                          f"<!-- wp:paragraph --><p>価格目安：¥{x['price']}／評価：{stars(x['review_avg'])}（{x['review_count']}件）</p><!-- /wp:paragraph -->"
                          f"<!-- wp:buttons --><div class=\"wp-block-buttons\"><div class=\"wp-block-button\"><a class=\"wp-block-button__link\" href=\"{x['url']}\" rel=\"sponsored noopener\">最安値を確認</a></div></div><!-- /wp:buttons -->"
                        "</div>"
                      "</div><!-- /wp:columns -->")

    # 比較表
    blocks.append(f"<!-- wp:heading --><h2 id=\"compare\">比較表</h2><!-- /wp:heading -->")
    table_rows = []
    table_rows.append("<tr><th>#</th><th>商品</th><th>価格</th><th>評価</th><th>レビュー件数</th></tr>")
    for i, x in enumerate(rest, start=1):
        table_rows.append(
            f"<tr><td>{i}</td>"
            f"<td><a href=\"{x['url']}\" rel=\"sponsored noopener\">{x['name']}</a></td>"
            f"<td>¥{x['price']}</td>"
            f"<td>{x['review_avg']:.1f}</td>"
            f"<td>{x['review_count']}</td></tr>"
        )
    table_html = "<table>" + "".join(table_rows) + "</table>"
    blocks.append(f"<!-- wp:table --><figure class=\"wp-block-table\">{table_html}</figure><!-- /wp:table -->")

    # 選び方
    blocks.append(f"<!-- wp:heading --><h2 id=\"howto\">失敗しない選び方</h2><!-- /wp:heading -->")
    tips = [
        "レビュー平均だけでなく、<strong>レビュー件数</strong>も見る（サンプルが多いほどブレにくい）",
        "<strong>価格×用途</strong>のバランス（過剰性能に注意）",
        "在庫は変動するため、<strong>リンク先で最新価格・納期</strong>を確認",
    ]
    blocks.append("<!-- wp:list --><ul>" + "".join([f"<li>{t}</li>" for t in tips]) + "</ul><!-- /wp:list -->")

    # FAQ
    blocks.append(f"<!-- wp:heading --><h2 id=\"faq\">FAQ</h2><!-- /wp:heading -->")
    blocks.append("<!-- wp:paragraph --><p><strong>Q. 最安値は常に正確？</strong><br/>A. 価格は変動します。購入前に必ずリンク先の最新情報をご確認ください。</p><!-- /wp:paragraph -->")
    blocks.append("<!-- wp:paragraph --><p><strong>Q. レビューの信頼性は？</strong><br/>A. 平均値と件数を併記し、極端な偏りを避けるためにレビュー密度で並べています。</p><!-- /wp:paragraph -->")

    # 免責
    blocks.append("<!-- wp:paragraph --><p>※本ページはAPIデータをもとに自動生成し、引用の範囲でリンクのみ掲載しています。</p><!-- /wp:paragraph -->")

    content = "\n".join(blocks)
    excerpt = f"{kw}の上位{min(3, n)}モデルをカードで紹介。レビュー密度と価格で機械集計した比較表つき。"
    return title, content, excerpt

# ------------ notify ------------
def notify(msg):
    if not ALERT: return
    try: requests.post(ALERT, json={"content":msg}, timeout=10)
    except Exception as e: LOG.error(f"alert failed: {e}")

# ------------ main ------------
def main():
    try:
        cfg=sys.argv[sys.argv.index("--config")+1]
    except ValueError:
        cfg="config/app.yaml"
    with open(cfg,"r",encoding="utf-8") as f:
        conf=yaml.safe_load(f)

    conf["ab_tests"]=yaml.safe_load(open("src/ab_tests.yaml","r",encoding="utf-8")) \
        if conf.get("content",{}).get("ab_test") else {}
    rules=yaml.safe_load(open(conf["review"]["rules_file"],"r",encoding="utf-8"))
    cats=ensure_categories(conf["site"]["category_names"])
    min_items=int(conf.get("content",{}).get("min_items",3))

    posted=0
    for raw_kw in conf["keywords"]["seeds"]:
        kw=sanitize_keyword(raw_kw)
        if not kw: continue
        if posted>=int(conf["site"]["posts_per_run"]): break

        slug=slugify(kw)
        if wp_post_exists(slug):
            LOG.info(f"skip exists (slug duplicate): {kw}"); continue

        LOG.info(f"query kw='{kw}'")
        arr=raken = rakuten_items((os.getenv("RAKUTEN_APP_ID") or "").strip(),
                                  kw, conf["data_sources"]["rakuten"]["endpoint"],
                                  conf["data_sources"]["rakuten"]["max_per_seed"],
                                  conf["data_sources"]["rakuten"].get("genreId"))
        enriched=enrich(arr)
        # フィルタ
        after_price=[it for it in enriched if it["price"]>=conf["content"]["price_floor"]]
        after_review=[it for it in after_price if it["review_avg"]>=conf["content"]["review_floor"]]
        LOG.info(f"stats kw='{kw}': total={len(arr)}, enriched={len(enriched)}, "
                 f"after_price={len(after_price)}, after_review={len(after_review)}")
        if len(after_review)<min_items:
            LOG.info(f"skip thin (<{min_items} items) for '{kw}'"); continue

        # コンテンツ生成（Gutenberg）
        title, content, excerpt = render_blocks(kw, after_review, conf)

        # 禁止表現チェック
        if any(bad in title or bad in content for bad in rules["prohibited_phrases"]):
            LOG.info(f"blocked by rule for '{kw}'"); continue

        payload={"title":title,"slug":slug,"status":"publish",
                 "content":content,"categories":cats,"excerpt":excerpt}
        headers={"Authorization": f"Basic {b64cred(WP_USER, WP_APP_PW)}","Content-Type":"application/json"}
        try:
            http_json("POST", f"{WP_URL}/wp-json/wp/v2/posts", data=json.dumps(payload), headers=headers)
            posted+=1; LOG.info(f"posted: {kw}")
        except RuntimeError as e:
            # 権限不足ならドラフトにフォールバック
            if "HTTP 401" in str(e) or "HTTP 403" in str(e):
                LOG.warning("permission denied for publish; fallback to draft")
                payload["status"]="draft"
                http_json("POST", f"{WP_URL}/wp-json/wp/v2/posts", data=json.dumps(payload), headers=headers)
                posted+=1; LOG.info(f"posted as draft: {kw}")
            else:
                LOG.error(f"post failed: {e}"); notify(f"[AUTO-REV] post failed for {kw}: {e}")
                time.sleep(2)

    LOG.info(f"done, posted={posted}")

if __name__=="__main__":
    try: main()
    except Exception as e:
        notify(f"[AUTO-REV] job failed: {e}"); raise
