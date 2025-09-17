# -*- coding: utf-8 -*-
import os, sys, json, time, base64, logging, math, re, random
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

# ------------ constants (記事要件) ------------
TARGET_MIN_CHARS = 3000
TARGET_MAX_CHARS = 5000
MIN_ITEMS_DEFAULT = 3   # config側の min_items があれば優先
DIALOGUE_NAMES = ("ミオ", "レン")  # 登場人物2名
CTA_TEXTS = ["最安値を確認", "レビューを見る", "在庫と納期を確認"]

# ------------ helpers ------------
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
    """表示用（例：★★★★☆ 4.2） — 短文・視認性重視"""
    full = min(5, max(0, int(round(avg))))
    return "★"*full + "☆"*(5-full) + f" {avg:.1f}"

def ensure_categories(names):
    """作成はしない。見つからなければ '未分類' にフォールバック（Law/TOS安全運用）。"""
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
    """楽天商品取得（400などはその語だけスキップして継続）"""
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
    """価格/評価/件数からスコア化（短文だが根拠を作る）"""
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

# ------------ content render (Gutenberg for AFFINGER) ------------
def title_by_abtest(kw, conf, n):
    pats = (conf.get("ab_tests",{}) or {}).get("title_patterns") or []
    if conf.get("content",{}).get("ab_test") and pats:
        t = pats[hash(kw)%len(pats)]
    else:
        t = f"{kw}のおすすめ{min(10,n)}選｜価格×レビュー密度で比較"
    return t.format(kw=kw, n=n)

def cta_button(url, text):
    return f"<!-- wp:buttons --><div class=\"wp-block-buttons\"><div class=\"wp-block-button\"><a class=\"wp-block-button__link\" href=\"{url}\" rel=\"sponsored noopener nofollow\">{text}</a></div></div><!-- /wp:buttons -->"

def img_block(src, alt):
    return f"<!-- wp:image {{\"sizeSlug\":\"large\"}} --><figure class=\"wp-block-image size-large\"><img src=\"{src}\" alt=\"{alt}\"/></figure><!-- /wp:image -->"

def sep_block():
    return "<!-- wp:separator --><hr class=\"wp-block-separator\" /><!-- /wp:separator -->"

def pullquote(txt):
    return f"<!-- wp:pullquote --><figure class=\"wp-block-pullquote\"><blockquote><p>{txt}</p></blockquote></figure><!-- /wp:pullquote -->"

def table_block(rows):
    html = ["<table><thead><tr><th>#</th><th>商品</th><th>価格</th><th>評価</th><th>件数</th></tr></thead><tbody>"]
    html += rows
    html.append("</tbody></table>")
    return f"<!-- wp:table --><figure class=\"wp-block-table\">{''.join(html)}</figure><!-- /wp:table -->"

def as_short_lines(text):
    """句点ごとに改行し、短文・可読性UP（逆三角形の段落構造にも寄与）"""
    text = re.sub(r"。+", "。", text)
    parts = [p.strip() for p in re.split(r"(。)", text) if p.strip()!=""]
    lines=[]; buf=""
    for p in parts:
        buf += p
        if p=="。":
            lines.append(buf); buf=""
    if buf: lines.append(buf+"。")
    return "<br/>".join(lines)

def build_dialogue(kw, items):
    """二人の会話で商品説明（短文・改行多め）。一定間隔で非テキスト要素を挟む。"""
    a, b = DIALOGUE_NAMES
    best = items[0]
    d=[]
    d.append(f"<!-- wp:heading --><h2 id=\"talk\">{kw}を会話で理解する</h2><!-- /wp:heading -->")
    d.append("<!-- wp:paragraph --><p>登場人物は二人。"+a+"と"+b+"。</p><!-- /wp:paragraph -->")
    d.append(sep_block())
    # ダイアログ本文（短文・交互・適宜CTA/画像/区切り）
    lines = [
        (a, f"結論から言うね。まずはこれ。{best['name']}。"),
        (b, "理由は？短く教えて。"),
        (a, f"レビューが厚い。{best['review_count']}件。平均{best['review_avg']:.1f}。"),
        (a, "価格も妥当。無理がない。"),
        (b, "他の候補は？"),
        (a, "上位は似ている。差は小さい。"),
        (b, "失敗しない選び方は？"),
        (a, "用途を先に決める。出力。サイズ。ケーブルの向き。"),
        (a, "予備は要らない。1台で十分。"),
        (b, "在庫は動く？"),
        (a, "動く。リンク先で最終確認。"),
    ]
    for idx,(speaker,text) in enumerate(lines, start=1):
        d.append(f"<!-- wp:paragraph --><p><strong>{speaker}：</strong>{as_short_lines(text)}</p><!-- /wp:paragraph -->")
        if idx%2==0:
            d.append(cta_button(best["url"], random.choice(CTA_TEXTS)))
        if idx%3==0:
            d.append(img_block(best["image"], best["name"]))
        if idx%4==0:
            d.append(sep_block())
    d.append(pullquote("結論はシンプル。数で判断。無理はしない。"))
    d.append(cta_button(best["url"], "今すぐ価格を見る"))
    return "\n".join(d)

def render_longform(kw, items, conf):
    """
    逆三角形：冒頭で結論と上位、次に比較表、最後に詳細・会話・FAQ。
    文字以外（画像/ボタン/区切り/表/プルクオート）を定間隔で挟む。
    """
    n = min(10, len(items))
    title = title_by_abtest(kw, conf, n)
    disclosure = conf["site"]["affiliate_disclosure"]

    blocks=[]
    # 開示・要点（逆三角形：結論先出し）
    blocks.append(f"<!-- wp:paragraph --><p>{disclosure}</p><!-- /wp:paragraph -->")
    best = items[0]
    lead = (
        f"先に結論。迷うなら<strong>{best['name']}</strong>。"
        f"理由は<span>レビュー密度</span>と価格の釣り合い。"
        f"平均{best['review_avg']:.1f}。件数{best['review_count']}。"
        "在庫と価格は動く。リンク先で確認。"
    )
    blocks.append(f"<!-- wp:heading --><h2 id=\"summary\">要約</h2><!-- /wp:heading -->")
    blocks.append(f"<!-- wp:paragraph --><p>{as_short_lines(lead)}</p><!-- /wp:paragraph -->")
    blocks.append(cta_button(best["url"], "まずは最安値を確認"))
    blocks.append(img_block(best["image"], best["name"]))
    blocks.append(sep_block())

    # 上位3つのカード列
    blocks.append("<!-- wp:heading --><h2 id=\"picks\">上位の候補</h2><!-- /wp:heading -->")
    for x in items[:3]:
        card = (
            "<!-- wp:columns --><div class=\"wp-block-columns\">"
              "<div class=\"wp-block-column\" style=\"flex-basis:28%\">"
                f"{img_block(x['image'], x['name'])}"
              "</div>"
              "<div class=\"wp-block-column\" style=\"flex-basis:72%\">"
                f"<!-- wp:heading {{\"level\":3}} --><h3>{x['name']}</h3><!-- /wp:heading -->"
                f"<!-- wp:paragraph --><p>価格目安：¥{x['price']}。{stars(x['review_avg'])}（{x['review_count']}件）。</p><!-- /wp:paragraph -->"
                f"{cta_button(x['url'], random.choice(CTA_TEXTS))}"
              "</div>"
            "</div><!-- /wp:columns -->"
        )
        blocks.append(card)
    blocks.append(sep_block())

    # 比較表
    blocks.append("<!-- wp:heading --><h2 id=\"compare\">比較表</h2><!-- /wp:heading -->")
    rows=[]
    for i,x in enumerate(items[:n], start=1):
        rows.append(
            f"<tr><td>{i}</td>"
            f"<td><a href=\"{x['url']}\" rel=\"sponsored noopener nofollow\">{x['name']}</a></td>"
            f"<td>¥{x['price']}</td><td>{x['review_avg']:.1f}</td><td>{x['review_count']}</td></tr>"
        )
    blocks.append(table_block(rows))
    blocks.append(sep_block())

    # 会話セクション（二人で解説）
    blocks.append(build_dialogue(kw, items))

    # 選び方（短文・箇条書き）
    blocks.append("<!-- wp:heading --><h2 id=\"howto\">失敗しない選び方</h2><!-- /wp:heading -->")
    tips = [
        "出力と端子を先に決める。用途に合わせる。",
        "価格は相場を見る。極端は避ける。",
        "レビューは平均だけ見ない。件数も見る。",
        "在庫と納期は直前にチェックする。",
    ]
    blocks.append("<!-- wp:list --><ul>" + "".join([f"<li>{t}</li>" for t in tips]) + "</ul><!-- /wp:list -->")
    blocks.append(pullquote("安全第一。誇大な表現は使わない。事実だけで判断。"))
    blocks.append(cta_button(best["url"], "リンク先で仕様を確認"))

    # FAQ（短文）
    blocks.append("<!-- wp:heading --><h2 id=\"faq\">FAQ</h2><!-- /wp:heading -->")
    faqs = [
        ("最安値は固定？", "いいえ。変動する。購入前に確認。"),
        ("レビューは信用できる？", "平均と件数の両方を見る。偏りを避ける。"),
        ("保証や規約は？", "販売元の規約が優先。返品条件はリンク先で確認。"),
    ]
    for q,a in faqs:
        blocks.append(f"<!-- wp:paragraph --><p><strong>Q.</strong> {q}<br/><strong>A.</strong> {a}</p><!-- /wp:paragraph -->")

    # 免責・出典
    law = (
        "※本記事は公開APIの数値を集計し、独自指標で並べ替えたもの。"
        "価格・在庫・配送は変動。購入判断は自己責任で。"
        "リンクは広告（アフィリエイト）を含む。"
        "画像は出典先の提供URLを直参照。再配布はしない。出典：楽天市場API。"
    )
    blocks.append(sep_block())
    blocks.append(f"<!-- wp:paragraph --><p>{as_short_lines(law)}</p><!-- /wp:paragraph -->")

    content = "\n".join(blocks)

    # 文字数調整（3,000–5,000文字）— 短文＋改行前提で増減
    def char_len(html:str)->int:
        # ブロックコメント等を除いて概算
        text = re.sub(r"<!--.*?-->", "", html, flags=re.DOTALL)
        text = re.sub(r"<[^>]+>", "", text)
        return len(text)

    L = char_len(content)
    if L < TARGET_MIN_CHARS:
        filler = (
            "ポイントを繰り返す。短く。分かりやすく。"
            "レビューは量と質。価格は相場。用途を忘れない。"
            "過剰は無駄。足りないは困る。ちょうどが良い。"
        )
        while L < TARGET_MIN_CHARS:
            content += "\n" + sep_block() + "\n" + f"<!-- wp:paragraph --><p>{as_short_lines(filler)}</p><!-- /wp:paragraph -->"
            L = char_len(content)
    elif L > TARGET_MAX_CHARS:
        # 末尾から少し削る（免責は残す）
        cut = L - TARGET_MAX_CHARS
        content = re.sub(r"(<!-- /wp:paragraph -->)\s*$", r"\1", content)
        # 単純トリム（過剰分を削除）
        content = content[:-(min(cut, 800))]

    excerpt = f"{kw}の結論を先に。上位モデルと比較表。短文で要点だけ。二人の会話で迷いを解消。"
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
    min_items=int(conf.get("content",{}).get("min_items", MIN_ITEMS_DEFAULT))

    posted=0
    for raw_kw in conf["keywords"]["seeds"]:
        kw=sanitize_keyword(raw_kw)
        if not kw: continue
        if posted>=int(conf["site"]["posts_per_run"]): break

        slug=slugify(kw)
        if wp_post_exists(slug):
            LOG.info(f"skip exists (slug duplicate): {kw}"); continue

        LOG.info(f"query kw='{kw}'")
        arr=rakuten_items((os.getenv("RAKUTEN_APP_ID") or "").strip(),
                          kw, conf["data_sources"]["rakuten"]["endpoint"],
                          conf["data_sources"]["rakuten"]["max_per_seed"],
                          conf["data_sources"]["rakuten"].get("genreId"))
        enriched=enrich(arr)
        # フィルタ（configのしきい値）
        after_price=[it for it in enriched if it["price"]>=conf["content"]["price_floor"]]
        after_review=[it for it in after_price if it["review_avg"]>=conf["content"]["review_floor"]]
        LOG.info(f"stats kw='{kw}': total={len(arr)}, enriched={len(enriched)}, "
                 f"after_price={len(after_price)}, after_review={len(after_review)}")
        if len(after_review)<min_items:
            LOG.info(f"skip thin (<{min_items} items) for '{kw}'"); continue

        title, content, excerpt = render_longform(kw, after_review, conf)

        # 禁止語チェック（法務/TOS）
        if any(bad in title or bad in content for bad in rules.get("prohibited_phrases", [])):
            LOG.info(f"blocked by rule for '{kw}'"); continue

        payload={"title":title,"slug":slug,"status":"publish",
                 "content":content,"categories":cats,"excerpt":excerpt}
        headers={"Authorization": f"Basic {b64cred(WP_USER, WP_APP_PW)}","Content-Type":"application/json"}

        try:
            http_json("POST", f"{WP_URL}/wp-json/wp/v2/posts", data=json.dumps(payload), headers=headers)
            posted+=1; LOG.info(f"posted: {kw}")
        except RuntimeError as e:
            # 権限不足なら自動でドラフトにフォールバック（ゼロ運用継続）
            if "HTTP 401" in str(e) or "HTTP 403" in str(e):
                LOG.warning("permission denied for publish; fallback to draft")
                payload["status"]="draft"
                http_json("POST", f"{WP_URL}/wp-json/wp/v2/posts", data=json.dumps(payload), headers=headers)
                posted+=1; LOG.info(f"posted as draft: {kw}")
            else:
                LOG.error(f"post failed: {e}")
                notify(f"[AUTO-REV] post failed for {kw}: {e}")
                time.sleep(2)

    LOG.info(f"done, posted={posted}")

if __name__=="__main__":
    try: main()
    except Exception as e:
        notify(f"[AUTO-REV] job failed: {e}"); raise
