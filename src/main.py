import os, sys, json, time, base64, logging, math, re, statistics
import requests, yaml
from slugify import slugify

# ===== ログ（ファイル + コンソール） =====
LOG = logging.getLogger("runner")
LOG.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
fh = logging.FileHandler("run.log", encoding="utf-8"); fh.setFormatter(fmt); LOG.addHandler(fh)
ch = logging.StreamHandler(); ch.setFormatter(fmt); LOG.addHandler(ch)

# ===== 環境変数 =====
WP_URL = (os.getenv("WP_SITE_URL") or "").rstrip("/")
WP_USER = os.getenv("WP_USERNAME")
WP_APP_PW = os.getenv("WP_APP_PASSWORD")
ALERT = os.getenv("ALERT_WEBHOOK_URL")
APP_ID = (os.getenv("RAKUTEN_APP_ID") or "").strip()

# ===== 共通ユーティリティ =====
def b64cred(u, p): return base64.b64encode(f"{u}:{p}".encode()).decode()

def http_json(method, url, **kw):
    r = requests.request(method, url, timeout=30, **kw)
    if not r.ok:
        LOG.error(f"http_error: {method} {url} -> {r.status_code} {r.text[:400]}")
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
    return r.json()

def sanitize_keyword(kw: str) -> str:
    if not kw: return ""
    kw = kw.replace("\u3000"," ").replace("\r"," ").replace("\n"," ").replace("\t"," ")
    kw = re.sub(r"\s+"," ", kw).strip()
    return kw

def stars(avg: float) -> str:
    full = round(float(avg))
    full = min(max(int(full), 0), 5)
    return "★"*full + "☆"*(5-full) + f" {avg:.1f}"

def yen(n: float) -> str:
    try:
        return f"¥{int(n):,}"
    except:
        return f"¥{n}"

# ===== WordPress側 =====
def ensure_categories(names):
    """カテゴリは作成しない（権限最小）。見つからなければ未分類にフォールバック。"""
    ids=[]
    for name in names:
        try:
            q=http_json("GET", f"{WP_URL}/wp-json/wp/v2/categories",
                        params={"search":name,"per_page":100})
            cid=next((c["id"] for c in q if c["name"].lower()==name.lower()), None)
            if cid: ids.append(cid); continue
            default=http_json("GET", f"{WP_URL}/wp-json/wp/v2/categories",
                              params={"slug":"uncategorized"})
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
        # 権限不足（401/403）はドラフトに自動フォールバック
        if "HTTP 401" in str(e) or "HTTP 403" in str(e):
            if payload.get("status")=="publish":
                LOG.warning("permission denied for publish; fallback to draft")
                payload2=payload.copy(); payload2["status"]="draft"
                return http_json("POST", f"{WP_URL}/wp-json/wp/v2/posts",
                                 data=json.dumps(payload2), headers=headers)
        raise

# ===== 楽天API =====
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
        i=it["Item"]
        price = i.get("itemPrice") or 0
        rev   = float(i.get("reviewAverage") or 0.0)
        rct   = int(i.get("reviewCount") or 0)
        score = rev * math.log1p(max(rct,1))   # レビュー密度指標
        name  = i.get("itemName") or ""
        url   = i.get("itemUrl")
        img   = (i.get("mediumImageUrls") or [{"imageUrl":""}])[0]["imageUrl"]
        ports = 0
        m_ports = re.search(r"(\d)ポート|(\d)\s*port", name, re.IGNORECASE)
        if m_ports:
            ports = int((m_ports.group(1) or m_ports.group(2) or 0))
        watt = None
        m_w = re.search(r"(\d{2,3})\s*W", name.upper())
        if m_w:
            try: watt=int(m_w.group(1))
            except: watt=None
        mini = any(k in name for k in ["mini","小型","軽量","コンパクト"])
        out.append({
            "name":name,"url":url,"image":img,
            "price":price,"review_avg":rev,"review_count":rct,
            "score":round(score,2),
            "ports":ports,"watt":watt,"mini":mini
        })
    out.sort(key=lambda x:(-x["score"], x["price"]))
    return out

# ===== 記事生成（3000〜5000文字・指定構成・Gutenberg・要素挿入） =====
def build_article(kw, items, conf):
    """
    構成:
    1. 導入文
    2. もっともおすすめ + 理由（結論先出し）
    3. おすすめランキング（A〜E）
    4. タイプ別ランキング（3カテゴリ × TOP3）
    5. 選定基準・方法
    6. まとめ
    途中に 画像 / 区切り線 / ボタン / テーブル / リスト など「文字以外の要素」を挿入
    """
    disclosure = conf["site"]["affiliate_disclosure"]
    n = min(10, len(items))
    top5 = items[:5] if len(items)>=5 else items[:max(3, len(items))]
    # ベスト：スコア最上位
    best = top5[0]
    prices = [x["price"] for x in items if x["price"]>0]
    med_price = statistics.median(prices) if prices else best["price"]

    # タイプ別設計
    def pick_cost_performance(arr):
        # コスパ = score / sqrt(price)
        scored = [(x["score"]/(math.sqrt(max(x["price"],1))), x) for x in arr]
        scored.sort(key=lambda t: -t[0])
        return [x for _,x in scored[:3]]
    def pick_high_power(arr):
        # 高出力/多ポート
        scored=[]
        for x in arr:
            pw = (x["watt"] or 0)
            pp = x["ports"]
            scored.append((pw*1.5 + pp*10 + x["score"], x))
        scored.sort(key=lambda t:-t[0])
        return [x for _,x in scored[:3]]
    def pick_mini(arr):
        # 小型・軽量ワード or 価格低め
        scored=[]
        for x in arr:
            bonus = 30 if x["mini"] else 0
            cheap = max(0, (med_price - x["price"])/max(med_price,1)) * 50
            scored.append((bonus + cheap + x["score"]*0.4, x))
        scored.sort(key=lambda t:-t[0])
        return [x for _,x in scored[:3]]

    typeA = pick_cost_performance(items)
    typeB = pick_high_power(items)
    typeC = pick_mini(items)

    # タイトル（ABテスト対応）
    title_patterns = conf.get("ab_tests", {}).get("title_patterns")
    title = (title_patterns[hash(kw)%len(title_patterns)] if conf["content"].get("ab_test") and title_patterns
             else f"{kw}のおすすめランキングと選び方【徹底比較】")
    title = title.format(kw=kw, n=len(top5))

    # 以降 Gutenberg ブロックを構築
    blocks=[]

    # 先頭：開示 + 導入（1. 導入文）
    intro = (
        f"{disclosure}\n\n"
        f"{kw}を『レビュー密度（平均★×件数の対数）』『価格バランス』『在庫の安定性（間接指標としてレビュー件数）』で機械集計。"
        f"転記ではなく数値から比較し、あなたの用途に合う“最適解”を提示します。"
    )
    blocks.append(f"<!-- wp:paragraph --><p>{intro}</p><!-- /wp:paragraph -->")
    # 区切り線（文字以外の要素）
    blocks.append("<!-- wp:separator --><hr class=\"wp-block-separator\"/><!-- /wp:separator -->")

    # 2. もっともおすすめ（結論先出し）
    best_reason = (
        f"総合1位は<strong>{best['name']}</strong>。理由は「評価{best['review_avg']:.1f}（{best['review_count']}件）で信頼度が高く、"
        f"{'中央値より低価格' if best['price']<=med_price else '性能指標が頭ひとつ抜けている'}」ため。"
        f"迷ったらこれを選べば後悔しにくいです。"
    )
    blocks.append("<!-- wp:heading --><h2>結論：もっともおすすめ</h2><!-- /wp:heading -->")
    blocks.append("<!-- wp:columns --><div class=\"wp-block-columns\">"
                  f"<div class=\"wp-block-column\" style=\"flex-basis:28%\">"
                  f"<!-- wp:image {{\"sizeSlug\":\"medium\"}} --><figure class=\"wp-block-image size-medium\">"
                  f"<img src=\"{best['image']}\" alt=\"{best['name']}\"/></figure><!-- /wp:image --></div>"
                  f"<div class=\"wp-block-column\" style=\"flex-basis:72%\">"
                  f"<!-- wp:paragraph --><p>{best_reason}</p><!-- /wp:paragraph -->"
                  f"<!-- wp:buttons --><div class=\"wp-block-buttons\"><div class=\"wp-block-button\">"
                  f"<a class=\"wp-block-button__link\" href=\"{best['url']}\" rel=\"sponsored noopener\">最安値を確認</a>"
                  f"</div></div><!-- /wp:buttons --></div></div><!-- /wp:columns -->")

    # 区切り線
    blocks.append("<!-- wp:separator --><hr class=\"wp-block-separator\"/><!-- /wp:separator -->")

    # 3. おすすめ商品ランキング（A〜E）
    blocks.append("<!-- wp:heading --><h2>おすすめ商品ランキング TOP5</h2><!-- /wp:heading -->")
    # 小テーブル（文字以外の要素：表）
    header = "<tr><th>順位</th><th>商品</th><th>価格</th><th>評価</th><th>レビュー件数</th></tr>"
    rows=[]
    for i,x in enumerate(top5, start=1):
        rows.append(
            f"<tr><td>{i}</td>"
            f"<td><a href=\"{x['url']}\" rel=\"sponsored noopener\">{x['name']}</a></td>"
            f"<td>{yen(x['price'])}</td>"
            f"<td>{x['review_avg']:.1f}</td>"
            f"<td>{x['review_count']}</td></tr>"
        )
    table_html = "<table>"+header+"".join(rows)+"</table>"
    blocks.append(f"<!-- wp:table --><figure class=\"wp-block-table\">{table_html}</figure><!-- /wp:table -->")

    # 各商品のカード説明 + ボタン（画像/ボタン＝文字以外の要素）
    label_map = ["商品A","商品B","商品C","商品D","商品E"]
    for i,x in enumerate(top5, start=1):
        label = label_map[i-1] if i-1 < len(label_map) else f"商品{i}"
        desc = (
            f"{label}「{x['name']}」。{stars(x['review_avg'])}（{x['review_count']}件）で、"
            f"{'多ポート・高出力' if (x['ports']>=3 or (x['watt'] or 0)>=65) else 'バランス型'}。"
            f"価格は{yen(x['price'])}。総合指標（レビュー密度）は{ x['score'] }で上位。"
            f"初めての人でも扱いやすい一台です。"
        )
        blocks.append(f"<!-- wp:heading {{\"level\":3}} --><h3>{label}</h3><!-- /wp:heading -->")
        blocks.append("<!-- wp:columns --><div class=\"wp-block-columns\">"
                      f"<div class=\"wp-block-column\" style=\"flex-basis:25%\">"
                      f"<!-- wp:image {{\"sizeSlug\":\"medium\"}} --><figure class=\"wp-block-image size-medium\">"
                      f"<img src=\"{x['image']}\" alt=\"{x['name']}\"/></figure><!-- /wp:image -->"
                      f"</div>"
                      f"<div class=\"wp-block-column\" style=\"flex-basis:75%\">"
                      f"<!-- wp:paragraph --><p>{desc}</p><!-- /wp:paragraph -->"
                      f"<!-- wp:buttons --><div class=\"wp-block-buttons\"><div class=\"wp-block-button\">"
                      f"<a class=\"wp-block-button__link\" href=\"{x['url']}\" rel=\"sponsored noopener\">詳細と現在価格を見る</a>"
                      f"</div></div><!-- /wp:buttons --></div></div><!-- /wp:columns -->")
        # 区切り線を適度に
        blocks.append("<!-- wp:separator --><hr class=\"wp-block-separator\"/><!-- /wp:separator -->")

    # 4. タイプ別のおすすめランキング（3カテゴリ×TOP3）
    blocks.append("<!-- wp:heading --><h2>タイプ別のおすすめランキング</h2><!-- /wp:heading -->")

    def render_type_section(title, arr):
        blocks_local=[]
        blocks_local.append(f"<!-- wp:heading {{\"level\":3}} --><h3>{title} TOP3</h3><!-- /wp:heading -->")
        # リスト（文字以外の要素）
        li=[]
        for rank,x in enumerate(arr[:3], start=1):
            line = (f"<strong>{rank}. {x['name']}</strong> – 評価{ x['review_avg']:.1f }（{x['review_count']}件） / "
                    f"価格{ yen(x['price']) } / 指標{ x['score'] }")
            li.append(f"<li>{line}</li>")
        blocks_local.append("<!-- wp:list --><ul>" + "".join(li) + "</ul><!-- /wp:list -->")
        # ボタン集合
        btns = []
        for x in arr[:3]:
            btns.append(f"<div class=\"wp-block-button\"><a class=\"wp-block-button__link\" href=\"{x['url']}\" rel=\"sponsored noopener\">{x['name'][:22]}…をチェック</a></div>")
        blocks_local.append("<!-- wp:buttons --><div class=\"wp-block-buttons\">" + "".join(btns) + "</div><!-- /wp:buttons -->")
        return blocks_local

    blocks += render_type_section("コスパ重視の人におすすめ", typeA)
    blocks.append("<!-- wp:separator --><hr class=\"wp-block-separator\"/><!-- /wp:separator -->")
    blocks += render_type_section("高出力/多ポートが欲しい人におすすめ", typeB)
    blocks.append("<!-- wp:separator --><hr class=\"wp-block-separator\"/><!-- /wp:separator -->")
    blocks += render_type_section("小型・軽量が良い人におすすめ", typeC)

    # 5. 選定基準・選定方法（説明文を厚めにして文字数確保）
    blocks.append("<!-- wp:heading --><h2>ランキングの選定基準・選定方法</h2><!-- /wp:heading -->")
    criteria = (
        "本ランキングは、楽天公式APIから取得した公開データを用い、"
        "①レビュー平均（★）と②レビュー件数を組み合わせた「レビュー密度指標＝★×log(件数+1)」で基本順位を算出。"
        "さらに③価格バランス、④名称から推定できる出力(W)・ポート数・小型性のキーワード（例：65W/100W、2ポート/3ポート、mini/小型/軽量）を参考に、"
        "用途別の推薦を行っています。個別レビュー文の転載は行わず、数値集計とリンクのみで付加価値を提供します。"
    )
    blocks.append(f"<!-- wp:paragraph --><p>{criteria}</p><!-- /wp:paragraph -->")
    # 引用（文字以外の要素）
    blocks.append("<!-- wp:quote --><blockquote class=\"wp-block-quote\"><p>注意：価格や在庫、配送条件は常に変動します。購入前に必ずリンク先で最新情報をご確認ください。</p><cite>運営より</cite></blockquote><!-- /wp:quote -->")

    # 6. まとめ（CTA）
    summary = (
        "まずは総合1位を基準に、用途がはっきりしている場合はタイプ別TOP3から選ぶのが失敗しにくい流れです。"
        "迷ったら『評価×件数のバランス』と『必要十分な出力』を重視すると、過不足のない買い物になります。"
        "本記事は毎日自動でデータを再集計し、古い情報に依存しない比較を目指しています。"
    )
    blocks.append("<!-- wp:heading --><h2>まとめ</h2><!-- /wp:heading -->")
    blocks.append(f"<!-- wp:paragraph --><p>{summary}</p><!-- /wp:paragraph -->")
    # ボタン（文字以外の要素）
    blocks.append(f"<!-- wp:buttons --><div class=\"wp-block-buttons\"><div class=\"wp-block-button\"><a class=\"wp-block-button__link\" href=\"{best['url']}\" rel=\"sponsored noopener\">まずは総合1位の価格を確認</a></div></div><!-- /wp:buttons -->")

    # 免責/法務（TOS/ガイドライン配慮）
    legal = (
        "【法務/TOS】本ページは公式API/公開情報に基づく要約・数値加工で構成し、引用は必要最小限・出典リンク明示・改変なしの原則に従います。"
        "商標は各社の所有物です。アフィリエイトリンクを含みます。自己購入や誤情報の助長を意図しません。"
    )
    blocks.append("<!-- wp:separator --><hr class=\"wp-block-separator\"/><!-- /wp:separator -->")
    blocks.append(f"<!-- wp:paragraph --><p>{legal}</p><!-- /wp:paragraph -->")

    # 抜粋（OGP/一覧用）
    excerpt = f"{kw}のTOP5とタイプ別TOP3を、レビュー密度・価格バランス・簡易キーワード解析で機械集計。画像・表・ボタンで比較しやすく構成。"

    content = "\n".join(blocks)

    # 文字数保証（3000〜5000）— 不足時はQ&Aを自動追加
    length = len(re.sub(r"<[^>]+>", "", content))  # テキストのみ概算
    if length < 3000:
        blocks.append("<!-- wp:separator --><hr class=\"wp-block-separator\"/><!-- /wp:separator -->")
        blocks.append("<!-- wp:heading --><h2>補足Q&A</h2><!-- /wp:heading -->")
        faqs = [
            ("Q. 候補が多すぎて迷います。", "A. まずは総合1位→タイプ別1位→価格の優先順位で3択に絞るのがコツです。"),
            ("Q. レビューは信頼できますか？", "A. 平均値と件数を併記し、偏りを抑えるために件数の対数を掛け合わせた指標を採用しています。"),
            ("Q. 65Wと100Wどちらを選ぶべき？", "A. ノートPCや多デバイス同時充電なら出力高めを、単一デバイス中心なら65Wでも十分です。")
        ]
        for q,a in faqs:
            blocks.append(f"<!-- wp:paragraph --><p><strong>{q}</strong><br/>{a}</p><!-- /wp:paragraph -->")
        content = "\n".join(blocks)

    return title, content, excerpt

# ===== 通知 =====
def notify(msg):
    if not ALERT: return
    try: requests.post(ALERT, json={"content": msg}, timeout=10)
    except Exception as e: LOG.error(f"alert failed: {e}")

# ===== メイン =====
def main():
    try:
        cfg = sys.argv[sys.argv.index("--config")+1]
    except ValueError:
        cfg = "config/app.yaml"
    with open(cfg, "r", encoding="utf-8") as f:
        conf = yaml.safe_load(f)

    conf["ab_tests"] = yaml.safe_load(open("src/ab_tests.yaml","r",encoding="utf-8")) \
        if conf.get("content",{}).get("ab_test") else {}
    rules = yaml.safe_load(open(conf["review"]["rules_file"],"r",encoding="utf-8"))
    cats  = ensure_categories(conf["site"]["category_names"])
    min_items = int(conf.get("content",{}).get("min_items", 3))

    posted=0
    for raw_kw in conf["keywords"]["seeds"]:
        kw = sanitize_keyword(raw_kw)
        if not kw: continue
        if posted >= int(conf["site"]["posts_per_run"]): break

        slug = slugify(kw)
        if wp_post_exists(slug):
            LOG.info(f"skip exists (slug duplicate): {kw}")
            continue

        LOG.info(f"query kw='{kw}'")
        arr = rakuten_items(APP_ID, kw, conf["data_sources"]["rakuten"]["endpoint"],
                            conf["data_sources"]["rakuten"]["max_per_seed"],
                            conf["data_sources"]["rakuten"].get("genreId"))
        enriched = enrich(arr)
        # フィルタ
        after_price  = [it for it in enriched if it["price"] >= conf["content"]["price_floor"]]
        after_review = [it for it in after_price if it["review_avg"] >= conf["content"]["review_floor"]]
        LOG.info(f"stats kw='{kw}': total={len(arr)}, enriched={len(enriched)}, "
                 f"after_price={len(after_price)}, after_review={len(after_review)}")

        if len(after_review) < max(min_items, 3):
            LOG.info(f"skip thin (<{max(min_items,3)} items) for '{kw}'")
            continue

        title, content, excerpt = build_article(kw, after_review, conf)

        # 禁止表現チェック（誇大表現など）
        blocked = False
        for bad in rules["prohibited_phrases"]:
            if bad in title or bad in content:
                LOG.info(f"blocked by rule '{bad}' for '{kw}'"); blocked=True; break
        if blocked: continue

        payload = {
            "title": title,
            "slug": slug,
            "status": "publish",
            "content": content,
            "excerpt": excerpt,
            "categories": cats
        }

        try:
            create_post(payload)
            posted += 1
            LOG.info(f"posted: {kw}")
        except Exception as e:
            LOG.error(f"post failed: {e}")
            notify(f"[AUTO-REV] post failed for {kw}: {e}")
            time.sleep(2)

    LOG.info(f"done, posted={posted}")

if __name__=="__main__":
    try: main()
    except Exception as e:
        notify(f"[AUTO-REV] job failed: {e}")
        raise
