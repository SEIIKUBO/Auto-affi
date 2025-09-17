# -*- coding: utf-8 -*-
"""
狙い
- 記事の内容/構成/口調/デザインを全面強化
- 商品ごとに可変テキスト（ブランド/価格帯/レビュー量で文面を変化）
- Gutenbergブロックのみ（AFFINGERで崩れにくい）
- 逆三角形（結論→概要→比較→会話→選び方→FAQ→免責）
- 3,000–5,000文字に自動調整。短文・改行多め。法務/TOS遵守。
"""

import os, sys, json, time, base64, logging, math, re, statistics, random
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
APP_ID = (os.getenv("RAKUTEN_APP_ID") or "").strip()

# ------------ constants ------------
TARGET_MIN_CHARS = 3000
TARGET_MAX_CHARS = 5000
DEFAULT_MIN_ITEMS = 3
DIALOGUE_NAMES = ("ミオ", "レン")
CTA_TEXTS = ["最安値を確認", "レビューを見る", "在庫と納期を確認"]
BTN_CLASS = "wp-block-button__link"  # AFFINGERでも無難

# ------------ utils ------------
def b64cred(u, p): return base64.b64encode(f"{u}:{p}".encode()).decode()

def http_json(method, url, **kw):
    r = requests.request(method, url, timeout=30, **kw)
    if not r.ok:
        LOG.error(f"http_error: {method} {url} -> {r.status_code} {r.text[:400]}")
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
    return r.json()

def sanitize_keyword(kw:str)->str:
    if not kw: return ""
    kw = kw.replace("\u3000"," ").replace("\r"," ").replace("\n"," ").replace("\t"," ")
    kw = re.sub(r"\s+"," ",kw)
    return kw.strip()

def stars(avg: float) -> str:
    full = min(5, max(0, int(round(avg))))
    return "★"*full + "☆"*(5-full) + f" {avg:.1f}"

def pct(n, d):
    try:
        return (n/d) if d else 0.0
    except Exception:
        return 0.0

# ------------ WP helpers ------------
def ensure_categories(names):
    """作成はしない。なければ『未分類』へフォールバック（安全運用）"""
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

def create_post(payload):
    headers={"Authorization": f"Basic {b64cred(WP_USER, WP_APP_PW)}","Content-Type":"application/json"}
    try:
        return http_json("POST", f"{WP_URL}/wp-json/wp/v2/posts", data=json.dumps(payload), headers=headers)
    except RuntimeError as e:
        # 権限不足→ドラフトに自動フォールバック（ゼロ運用継続）
        if "HTTP 401" in str(e) or "HTTP 403" in str(e):
            LOG.warning("permission denied for publish; fallback to draft")
            payload2=payload.copy(); payload2["status"]="draft"
            return http_json("POST", f"{WP_URL}/wp-json/wp/v2/posts", data=json.dumps(payload2), headers=headers)
        raise

# ------------ Rakuten ------------
def rakuten_items(app_id, kw, endpoint, hits, genreId=None):
    kw=sanitize_keyword(kw)
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
        i=it["Item"]; price=int(i.get("itemPrice") or 0)
        rev=float(i.get("reviewAverage") or 0.0)
        rct=int(i.get("reviewCount") or 0)
        score=rev*math.log1p(max(rct,1))
        out.append({
            "name":i.get("itemName"),
            "url":i.get("itemUrl"),
            "image":(i.get("mediumImageUrls") or [{"imageUrl":""}])[0]["imageUrl"],
            "price":price,"review_avg":rev,"review_count":rct,"score":round(score,2)
        })
    out.sort(key=lambda x:(-x["score"], x["price"]))
    return out

# ------------ Analysis for per-product variation ------------
BRAND_HINTS = ["Anker","RAVPower","AUKEY","Elecom","パナソニック","SONY","サンワ","BUFFALO","エレコム","ANKER","Apple","エプソン","Canon"]

def analyze(items):
    prices=[x["price"] for x in items if x["price"]>0]
    median = statistics.median(prices) if prices else 0
    avg_rev = statistics.mean([x["review_avg"] for x in items]) if items else 0.0
    avg_cnt = statistics.mean([x["review_count"] for x in items]) if items else 0.0
    return {"median_price":median,"avg_review":avg_rev,"avg_count":avg_cnt}

def product_profile(x, stats):
    """価格帯/評価/件数/ブランドから文面タイプを決定"""
    brand = next((b for b in BRAND_HINTS if b.lower() in x["name"].lower()), None)
    cheaper = x["price"] <= max(1, stats["median_price"]*0.85)
    premium = x["price"] >= stats["median_price"]*1.15 if stats["median_price"]>0 else False
    high_rate = x["review_avg"] >= 4.3
    many = x["review_count"] >= max(50, stats["avg_count"])
    sparse = x["review_count"] < max(10, stats["avg_count"]*0.5)

    tags=[]
    if brand: tags.append("brand")
    if cheaper: tags.append("budget")
    if premium: tags.append("premium")
    if high_rate: tags.append("high_rate")
    if many: tags.append("many_reviews")
    if sparse: tags.append("few_reviews")

    return {
        "brand": brand or "",
        "cheaper": cheaper, "premium": premium,
        "high_rate": high_rate, "many": many, "sparse": sparse,
        "tags": tags
    }

# ------------ Gutenberg blocks ------------
def btn(url, text):
    return f"<!-- wp:buttons --><div class=\"wp-block-buttons\"><div class=\"wp-block-button\"><a class=\"{BTN_CLASS}\" href=\"{url}\" rel=\"sponsored noopener nofollow\">{text}</a></div></div><!-- /wp:buttons -->"

def img(src, alt):
    return f"<!-- wp:image {{\"sizeSlug\":\"large\"}} --><figure class=\"wp-block-image size-large\"><img src=\"{src}\" alt=\"{alt}\"/></figure><!-- /wp:image -->"

def sep():
    return "<!-- wp:separator --><hr class=\"wp-block-separator\" /><!-- /wp:separator -->"

def pull(txt):
    return f"<!-- wp:pullquote --><figure class=\"wp-block-pullquote\"><blockquote><p>{txt}</p></blockquote></figure><!-- /wp:pullquote -->"

def table(rows):
    html=["<table><thead><tr><th>#</th><th>商品</th><th>価格</th><th>評価</th><th>件数</th></tr></thead><tbody>"]
    html+=rows; html.append("</tbody></table>")
    return f"<!-- wp:table --><figure class=\"wp-block-table\">{''.join(html)}</figure><!-- /wp:table -->"

def short_lines(text):
    text=re.sub(r"。+", "。", text)
    parts=[p.strip() for p in re.split(r"(。)", text) if p.strip()!=""]
    out=[]; buf=""
    for p in parts:
        buf+=p
        if p=="。":
            out.append(buf); buf=""
    if buf: out.append(buf+"。")
    return "<br/>".join(out)

def title_by_conf(kw, conf, n):
    pats=(conf.get("ab_tests",{}) or {}).get("title_patterns") or []
    if conf.get("content",{}).get("ab_test") and pats:
        t=pats[hash(kw)%len(pats)]
    else:
        t=f"{kw}のおすすめ{min(n,10)}選｜価格×レビュー密度で比較"
    return t.format(kw=kw, n=n)

# ------------ Per-product narrative ------------
def micro_review(x, prof, rng):
    """商品ごとに文面が変わる短評。語尾/語彙/切り口をランダムに差し替え（安定乱数）。"""
    tone_end = rng.choice(["。","。","。","。","。","。","。","！"])
    openers = ["結論から。","まずは要点。","シンプルに。","短く。","先に言う。"]
    opener = rng.choice(openers)

    bits=[]
    if prof["brand"]:
        bits.append(f"{prof['brand']}の定番。")
    if prof["cheaper"]:
        bits.append("価格が穏やか。無理がない。")
    if prof["premium"]:
        bits.append("やや高め。性能は厚い。")
    if prof["high_rate"]:
        bits.append("満足度が高い。")
    if prof["many"]:
        bits.append("実績が多い。")
    if prof["sparse"]:
        bits.append("レビューは少なめ。様子見もあり。")

    core = f"評価 {x['review_avg']:.1f}。{x['review_count']}件。目安 ¥{x['price']}。"
    return opener + " " + core + " " + " ".join(bits) + tone_end

def pros_cons(x, prof, stats):
    pros=[]; cons=[]
    if prof["cheaper"]: pros.append("価格が手頃")
    if prof["premium"]: cons.append("価格が高め")
    if prof["high_rate"]: pros.append("満足度が高い")
    if prof["many"]: pros.append("レビュー件数が多い")
    if prof["sparse"]: cons.append("レビュー件数が少ない")
    # 相対価格で補足
    if stats["median_price"]>0:
        diff = x["price"] - stats["median_price"]
        if diff <= -int(stats["median_price"]*0.2): pros.append("相場より安い")
        if diff >=  int(stats["median_price"]*0.25): cons.append("相場より高い")
    # 最低1件ずつは入れる
    if not pros: pros.append("バランスが良い")
    if not cons: cons.append("在庫と納期の変動に注意")
    return pros, cons

def product_card_block(i, x, prof, stats):
    rng = random.Random((hash(x["name"]) ^ x["price"]) & 0xffffffff)
    mr = micro_review(x, prof, rng)
    pros, cons = pros_cons(x, prof, stats)
    pros_li = "".join([f"<li>{p}</li>" for p in pros])
    cons_li = "".join([f"<li>{c}</li>" for c in cons])
    return (
        "<!-- wp:group --><div class=\"wp-block-group\">"
        "<!-- wp:columns --><div class=\"wp-block-columns\">"
        f"<div class=\"wp-block-column\" style=\"flex-basis:26%\">{img(x['image'], x['name'])}</div>"
        "<div class=\"wp-block-column\" style=\"flex-basis:74%\">"
        f"<!-- wp:heading {{\"level\":3}} --><h3>{i}. {x['name']}</h3><!-- /wp:heading -->"
        f"<!-- wp:paragraph --><p>{short_lines(mr)}</p><!-- /wp:paragraph -->"
        f"<!-- wp:columns --><div class=\"wp-block-columns\"><div class=\"wp-block-column\"><!-- wp:list --><ul>{pros_li}</ul><!-- /wp:list --></div>"
        f"<div class=\"wp-block-column\"><!-- wp:list --><ul>{cons_li}</ul><!-- /wp:list --></div></div><!-- /wp:columns -->"
        f"{btn(x['url'], random.choice(CTA_TEXTS))}"
        "</div></div><!-- /wp:columns -->"
        "</div><!-- /wp:group -->"
    )

# ------------ Dialogue (two people) ------------
def dialogue_block(kw, top1, top2):
    a, b = DIALOGUE_NAMES
    lines = [
        (a, f"結論は先に言う。{top1['name']} でいい。"),
        (b, "強い根拠は？"),
        (a, f"レビューが厚い。{top1['review_count']}件。平均 {top1['review_avg']:.1f}。"),
        (a, f"価格も無理がない。目安 ¥{top1['price']}。"),
        (b, f"{top2['name']} はどう？"),
        (a, "選択肢として良い。だが差は小さい。"),
        (b, "失敗しない基準は？"),
        (a, "用途を先に決める。出力。サイズ。端子。"),
        (a, "相場を外さない。過剰は無駄。"),
        (b, "在庫は動く？"),
        (a, "動く。リンク先で最終確認。"),
    ]
    out=["<!-- wp:heading --><h2 id=\"talk\">会話で理解する</h2><!-- /wp:heading -->"]
    for idx,(speaker, text) in enumerate(lines, start=1):
        out.append(f"<!-- wp:paragraph --><p><strong>{speaker}：</strong>{short_lines(text)}</p><!-- /wp:paragraph -->")
        if idx in (2,5,8):
            out.append(pull("短く決める。数で判断。迷いは削る。"))
        if idx in (3,6,9):
            out.append(sep())
    out.append(btn(top1["url"], "まずは最安値を確認"))
    return "\n".join(out)

# ------------ Longform assembly ------------
def build_content(kw, items, conf):
    n=min(10,len(items))
    title=title_by_conf(kw, conf, n)
    disclosure=conf["site"]["affiliate_disclosure"]
    stats=analyze(items)
    top=items[:3]
    best=items[0]

    blocks=[]
    # 逆三角形：結論＋CV導線
    blocks.append(f"<!-- wp:paragraph --><p>{disclosure}</p><!-- /wp:paragraph -->")
    summary=(f"結論。迷ったら <strong>{best['name']}</strong>。"
             f"評価 {best['review_avg']:.1f}。{best['review_count']}件。"
             f"価格の目安は ¥{best['price']}。"
             "在庫と価格は変動。最終確認はリンク先。")
    blocks.append("<!-- wp:heading --><h2 id=\"summary\">要約</h2><!-- /wp:heading -->")
    blocks.append(f"<!-- wp:paragraph --><p>{short_lines(summary)}</p><!-- /wp:paragraph -->")
    blocks.append(btn(best["url"], "今すぐ価格を見る"))
    blocks.append(img(best["image"], best["name"]))
    blocks.append(sep())

    # カード列（商品ごとに可変テキスト）
    blocks.append("<!-- wp:heading --><h2 id=\"picks\">上位の候補</h2><!-- /wp:heading -->")
    for i, x in enumerate(top, start=1):
        prof=product_profile(x, stats)
        blocks.append(product_card_block(i, x, prof, stats))
    blocks.append(sep())

    # 比較表
    blocks.append("<!-- wp:heading --><h2 id=\"compare\">比較表</h2><!-- /wp:heading -->")
    rows=[]
    for i,x in enumerate(items[:n], start=1):
        rows.append(
            f"<tr><td>{i}</td>"
            f"<td><a href=\"{x['url']}\" rel=\"sponsored noopener nofollow\">{x['name']}</a></td>"
            f"<td>¥{x['price']}</td><td>{x['review_avg']:.1f}</td><td>{x['review_count']}</td></tr>"
        )
    blocks.append(table(rows))
    blocks.append(sep())

    # 会話（二人）
    if len(items)>=2:
        blocks.append(dialogue_block(kw, items[0], items[1]))
        blocks.append(sep())

    # 選び方（短文）
    blocks.append("<!-- wp:heading --><h2 id=\"howto\">失敗しない選び方</h2><!-- /wp:heading -->")
    tips=[
        "用途→出力→端子の順で決める。",
        "相場を外さない。過剰は無駄。",
        "レビューは平均と件数の両方を見る。",
        "在庫と納期は直前に確認する。"
    ]
    blocks.append("<!-- wp:list --><ul>" + "".join([f"<li>{t}</li>" for t in tips]) + "</ul><!-- /wp:list -->")
    blocks.append(pull("短く決める。数で判断。迷いは削る。"))
    blocks.append(btn(best["url"], "仕様と在庫を確認"))

    # FAQ（短文）
    blocks.append("<!-- wp:heading --><h2 id=\"faq\">FAQ</h2><!-- /wp:heading -->")
    faqs=[("最安値は固定？","いいえ。変動する。購入前に確認。"),
          ("レビューは信用できる？","平均と件数を併記。偏りを避ける。"),
          ("保証や規約は？","販売元の約款が優先。返品条件はリンク先で確認。")]
    for q,a in faqs:
        blocks.append(f"<!-- wp:paragraph --><p><strong>Q.</strong> {q}<br/><strong>A.</strong> {a}</p><!-- /wp:paragraph -->")

    # 免責・出典（法務）
    legal=("本記事は公開APIの数値を集計し、独自指標で並べ替えた解説。"
           "価格・在庫・配送は変動。購入判断は自己責任。"
           "リンクには広告（アフィリエイト）を含む。"
           "画像URLは出典先を直参照。再配布はしない。出典：楽天市場API。")
    blocks.append(sep())
    blocks.append(f"<!-- wp:paragraph --><p>{short_lines(legal)}</p><!-- /wp:paragraph -->")

    content="\n".join(blocks)
    excerpt=f"{kw}の結論を先に。上位モデルをカードで提示。比較表と二人の会話で迷いを削る。"

    # 文字数調整
    def text_len(html:str)->int:
        t=re.sub(r"<!--.*?-->", "", html, flags=re.DOTALL)
        t=re.sub(r"<[^>]+>", "", t)
        return len(t)

    L=text_len(content)
    if L < TARGET_MIN_CHARS:
        filler=("要点を繰り返す。短く。分かりやすく。"
                "レビューは量と質。価格は相場。用途を忘れない。"
                "過剰は無駄。足りないは困る。ちょうどが良い。")
        while L < TARGET_MIN_CHARS:
            content += "\n" + sep() + "\n" + f"<!-- wp:paragraph --><p>{short_lines(filler)}</p><!-- /wp:paragraph -->"
            L=text_len(content)
    elif L > TARGET_MAX_CHARS:
        cut = L - TARGET_MAX_CHARS
        content = content[:-min(cut, 800)]  # 過剰分を軽くトリム

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

    # optional files（無くても動く設計）
    try:
        if conf.get("content",{}).get("ab_test"):
            conf["ab_tests"]=yaml.safe_load(open("src/ab_tests.yaml","r",encoding="utf-8"))
        else:
            conf["ab_tests"]={}
    except Exception:
        conf["ab_tests"]={}
    try:
        rules=yaml.safe_load(open(conf["review"]["rules_file"],"r",encoding="utf-8"))
        bads=rules.get("prohibited_phrases",[])
    except Exception:
        bads=[]
    cats=ensure_categories(conf["site"]["category_names"])
    min_items=int(conf.get("content",{}).get("min_items", DEFAULT_MIN_ITEMS))

    posted=0
    for raw_kw in conf["keywords"]["seeds"]:
        kw=sanitize_keyword(raw_kw)
        if not kw: continue
        if posted>=int(conf["site"]["posts_per_run"]): break

        slug=slugify(kw)
        if wp_post_exists(slug):
            LOG.info(f"skip exists (slug duplicate): {kw}"); continue

        LOG.info(f"query kw='{kw}'")
        arr=raken = rakuten_items(APP_ID,
                                  kw, conf["data_sources"]["rakuten"]["endpoint"],
                                  conf["data_sources"]["rakuten"]["max_per_seed"],
                                  conf["data_sources"]["rakuten"].get("genreId"))
        enriched=enrich(arr)
        # フィルタ
        price_floor=conf["content"]["price_floor"]
        review_floor=conf["content"]["review_floor"]
        after_price=[it for it in enriched if it["price"]>=price_floor]
        after_review=[it for it in after_price if it["review_avg"]>=review_floor]

        LOG.info(f"stats kw='{kw}': total={len(arr)}, enriched={len(enriched)}, "
                 f"after_price={len(after_price)}, after_review={len(after_review)}")
        if len(after_review)<min_items:
            LOG.info(f"skip thin (<{min_items} items) for '{kw}'"); continue

        title, content, excerpt = build_content(kw, after_review, conf)

        # 法務チェック
        if any(b in title or b in content for b in bads):
            LOG.info(f"blocked by rule for '{kw}'"); continue

        payload={"title":title,"slug":slug,"status":"publish",
                 "content":content,"categories":cats,"excerpt":excerpt}
        try:
            create_post(payload)
            posted+=1; LOG.info(f"posted: {kw}")
        except Exception as e:
            LOG.error(f"post failed: {e}")
            notify(f"[AUTO-REV] post failed for {kw}: {e}")
            time.sleep(2)

    LOG.info(f"done, posted={posted}")

if __name__=="__main__":
    try: main()
    except Exception as e:
        notify(f"[AUTO-REV] job failed: {e}"); raise
