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
    # 表示用（例：★★★★☆ 4.2）
    full = max(0, min(5, int(round(avg))))
    return "★"*full + "☆"*(5-full) + f" {avg:.1f}"

def ensure_categories(names):
    """作成はしない（最小権限）。見つからなければ '未分類' にフォールバック。"""
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
            if r.status_code==400: return []  # その語だけスキップして続行
            r.raise_for_status()
        j=r.json() or {}
        return j.get("Items",[])
    except Exception as e:
        LOG.error(f"rakuten_request_exception kw='{kw}': {e}")
        return []

def enrich(items):
    out=[]
    for it in items:
        i=it["Item"]
        price=i.get("itemPrice") or 0
        rev=float(i.get("reviewAverage") or 0.0)
        rct=int(i.get("reviewCount") or 0)
        score=rev*math.log1p(max(rct,1))  # レビュー密度
        out.append({
            "name": i.get("itemName"),
            "url": i.get("itemUrl"),
            "image": (i.get("mediumImageUrls") or [{"imageUrl":""}])[0]["imageUrl"],
            "price": price,
            "review_avg": rev,
            "review_count": rct,
            "score": round(score,2)
        })
    out.sort(key=lambda x:(-x["score"], x["price"]))
    return out

# ------------ longform content (human-like) ------------
def human_intro(kw, n, top_price_min, top_price_max):
    # 200–300字程度
    return (
        f"{kw}を買うなら、正直“勘”では外します。ここではレビュー密度（⭐×件数の対数）と価格を機械的に集計し、"
        f"上位{n}件から実用本位で絞り込みました。だいたい¥{top_price_min:,}〜¥{top_price_max:,}のレンジが買いどころ。"
        f"数値を土台に、人の目線で使い勝手まで踏み込みます。"
    )

def human_takeaways(items):
    # 上位3件の所感（各120–180字×3＝360–540字）
    lines=[]
    for x in items[:3]:
        note=[]
        if x["review_avg"]>=4.3: note.append("満足度が安定")
        elif x["review_avg"]>=3.8: note.append("評価は堅実")
        if x["price"]<=3000: note.append("価格優位")
        elif x["price"]>=8000: note.append("高価だが納得感")
        skew = "レビュー件数が多く信頼しやすい" if x["review_count"]>=500 else "レビュー件数は控えめ"
        note.append(skew)
        lines.append(f"・{x['name']}：{stars(x['review_avg'])}／¥{x['price']:,}。{ '・'.join(note) }。リンク先で在庫と実売を要確認。")
    return "\n".join(lines)

def human_buying_guide(kw):
    # 600–900字
    pts=[
        "目的の明確化：通勤・出張・自宅据え置きで求める要件は変わります。迷ったら“毎日持ち歩ける重さか”を最優先に。",
        "性能の見極め：カタログ値より“継続出力/実測レビュー”を重視。数分だけ高出力でも、日常の満足度は上がりません。",
        "将来の拡張：端子の規格や互換性は寿命に直結。今だけでなく、半年先の使い道を想像して選びます。",
        "サポートと保証：初期不良時の対応は地味に重要。レビュー欄の“交換の早さ/丁寧さ”は指標になります。",
        "価格の節度：最安狙いは故障リスクと表裏一体。“安い・軽い・速い”は同時に取りづらい前提で、バランスを取る。"
    ]
    return f"{kw}で失敗しないコツはシンプルです。\n" + "\n".join([f"{i+1}. {t}" for i,t in enumerate(pts)])

def human_use_cases(items):
    # 用途別おすすめ（800–1000字相当）
    pick = items[:5]
    blocks=[]
    labels=["軽さ優先","コスパ重視","とにかく安心","初めての1台","サブ用/職場置き"]
    for label, x in zip(labels, pick):
        line = f"{x['name']}… 評価{stars(x['review_avg'])} / 目安¥{x['price']:,}。レビュー件数{ x['review_count'] }件級。"
        why=[]
        if x["price"]<3000: why.append("価格が軽く手を出しやすい")
        if x["review_count"]>800: why.append("母数が多くハズレ率が低い")
        if x["review_avg"]>=4.5: why.append("満足度が突出")
        if not why: why.append("仕様のバランスが良い")
        line += " " + "・".join(why) + "。"
        blocks.append(line)
    return "\n".join(blocks)

def human_comparison(items):
    # 比較の着眼点（600–800字）
    best = items[0]
    cheap = min(items, key=lambda x:x["price"])
    many = max(items, key=lambda x:x["review_count"])
    return (
        f"比較すると、総合1位は「{best['name']}」。レビュー密度の高さが決め手です。"
        f"最安は「{cheap['name']}」で¥{cheap['price']:,}、予算を抑えたい人向き。"
        f"母数が最も多いのは「{many['name']}」（{many['review_count']}件）で、トレンドの“安心感”を取りにいくならこれ。"
        " 迷ったら“レビュー件数→平均評価→価格”の順で優先度をつけると決まりやすい。"
    )

def human_fails():
    # 失敗あるある（300–500字）
    lst=[
        "出力だけ見て重さを見落とす → 毎日カバンが重くなり不使用に",
        "最安だけで選び、ノイズや発熱で後悔 → レビューの“音/温度”ワードを確認",
        "ケーブル相性を考慮せず速度が出ない → 手持ちケーブルの規格を再確認"
    ]
    return "買ってから後悔しがちなパターンは次の通り。\n- " + "\n- ".join(lst)

def human_faq():
    # FAQ（400–600字）
    qa=[
        ("最安は常に正確ですか？","価格は変動します。本文はAPIの取得時点の目安です。購入前にリンク先の最新情報をご確認ください。"),
        ("口コミは信用できますか？","平均と件数を併記し、極端な偏りを避けるためレビュー密度で並べています。個別の使用環境差は必ずあります。"),
        ("広告ですか？","一部リンクはアフィリエイトです。収益は運営費に充てますが、並び順は数値指標で自動決定しています。")
    ]
    return "\n".join([f"Q. {q}\nA. {a}" for q,a in qa])

def human_summary(kw):
    # 200–300字
    return f"{kw}は“数値で候補を絞り、人の目で最終判断”が近道。今日の最適解は人によって違います。迷ったら上位3つから、用途に合うものを選べばまず失敗しません。"

def build_blocks_longform(kw, items, conf):
    """Gutenbergブロックで3,000–5,000字に収まる本文を生成"""
    n=min(10,len(items))
    top=items[:3]
    prices=[x["price"] for x in items[:n] if isinstance(x["price"], (int,float))]
    pmin=min(prices) if prices else 0
    pmax=max(prices) if prices else 0

    # タイトル（ABテスト対応）
    title_patterns = conf.get("ab_tests", {}).get("title_patterns")
    title = (title_patterns[hash(kw)%len(title_patterns)] if conf["content"].get("ab_test") and title_patterns
             else f"{kw}のおすすめ{min(n,10)}選｜失敗しない選び方と比較")
    title = title.format(kw=kw, n=n)

    disclosure = conf["site"]["affiliate_disclosure"]

    blocks=[]
    # 開示
    blocks.append(f"<!-- wp:paragraph --><p>{disclosure}</p><!-- /wp:paragraph -->")
    # リード（人間味）
    blocks.append(f"<!-- wp:paragraph --><p>{human_intro(kw,n,pmin,pmax)}</p><!-- /wp:paragraph -->")

    # 目次
    blocks.append("<!-- wp:list --><ul>"
                  "<li><a href=\"#top-picks\">まずは上位3つ</a></li>"
                  "<li><a href=\"#compare\">比較表</a></li>"
                  "<li><a href=\"#takeaways\">要点メモ</a></li>"
                  "<li><a href=\"#usecases\">用途別おすすめ</a></li>"
                  "<li><a href=\"#howto\">失敗しない選び方</a></li>"
                  "<li><a href=\"#compare-deep\">比較の着眼点</a></li>"
                  "<li><a href=\"#faq\">FAQ</a></li>"
                  "</ul><!-- /wp:list -->")

    # 上位3つ（カード）
    blocks.append(f"<!-- wp:heading --><h2 id=\"top-picks\">まずは上位3つ</h2><!-- /wp:heading -->")
    for x in top:
        blocks.append(
            "<!-- wp:columns --><div class=\"wp-block-columns\">"
              "<div class=\"wp-block-column\" style=\"flex-basis:25%\">"
                f"<!-- wp:image {{\"sizeSlug\":\"medium\"}} --><figure class=\"wp-block-image size-medium\"><img src=\"{x['image']}\" alt=\"{x['name']}\"/></figure><!-- /wp:image -->"
              "</div>"
              "<div class=\"wp-block-column\" style=\"flex-basis:75%\">"
                f"<!-- wp:heading {{\"level\":3}} --><h3>{x['name']}</h3><!-- /wp:heading -->"
                f"<!-- wp:paragraph --><p>価格目安：¥{x['price']:,}／評価：{stars(x['review_avg'])}（{x['review_count']}件）。レビュー密度スコア：{x['score']}。</p><!-- /wp:paragraph -->"
                f"<!-- wp:buttons --><div class=\"wp-block-buttons\"><div class=\"wp-block-button\"><a class=\"wp-block-button__link\" href=\"{x['url']}\" rel=\"sponsored noopener\">最安値を確認</a></div></div><!-- /wp:buttons -->"
              "</div>"
            "</div><!-- /wp:columns -->"
        )

    # 比較表
    rows=["<tr><th>#</th><th>商品</th><th>価格</th><th>評価</th><th>レビュー件数</th></tr>"]
    for i,x in enumerate(items[:n], start=1):
        rows.append(
            f"<tr><td>{i}</td><td><a href=\"{x['url']}\" rel=\"sponsored noopener\">{x['name']}</a></td>"
            f"<td>¥{x['price']:,}</td><td>{x['review_avg']:.1f}</td><td>{x['review_count']}</td></tr>"
        )
    table_html = "<table>" + "".join(rows) + "</table>"
    blocks.append(f"<!-- wp:heading --><h2 id=\"compare\">比較表</h2><!-- /wp:heading -->")
    blocks.append(f"<!-- wp:table --><figure class=\"wp-block-table\">{table_html}</figure><!-- /wp:table -->")

    # 要点メモ（人間味の短評）
    blocks.append(f"<!-- wp:heading --><h2 id=\"takeaways\">要点メモ</h2><!-- /wp:heading -->")
    blocks.append(f"<!-- wp:paragraph --><p>{human_takeaways(items)}</p><!-- /wp:paragraph -->")

    # 用途別おすすめ
    blocks.append(f"<!-- wp:heading --><h2 id=\"usecases\">用途別おすすめ</h2><!-- /wp:heading -->")
    blocks.append(f"<!-- wp:paragraph --><p>{human_use_cases(items)}</p><!-- /wp:paragraph -->")

    # 失敗しない選び方
    blocks.append(f"<!-- wp:heading --><h2 id=\"howto\">失敗しない選び方</h2><!-- /wp:heading -->")
    blocks.append(f"<!-- wp:paragraph --><p>{human_buying_guide(kw)}</p><!-- /wp:paragraph -->")

    # 比較の着眼点
    blocks.append(f"<!-- wp:heading --><h2 id=\"compare-deep\">比較の着眼点</h2><!-- /wp:heading -->")
    blocks.append(f"<!-- wp:paragraph --><p>{human_comparison(items)}</p><!-- /wp:paragraph -->")

    # FAQ
    blocks.append(f"<!-- wp:heading --><h2 id=\"faq\">FAQ</h2><!-- /wp:heading -->")
    blocks.append(f"<!-- wp:paragraph --><p>{human_faq()}</p><!-- /wp:paragraph -->")

    # 免責
    blocks.append("<!-- wp:paragraph --><p>※価格や在庫は取得時点の目安です。購入前に必ずリンク先の最新情報をご確認ください。本ページはAPIデータをもとに数値集計で編集しており、テキスト引用や再配布は行っていません。</p><!-- /wp:paragraph -->")

    content = "\n".join(blocks)
    excerpt = f"{kw}の上位{min(3,n)}モデルをカードで紹介。レビュー密度と価格を元に人力観点で補足した比較表つき。"

    # 文字数調整（3,000–5,000）
    def visible_chars(s:str)->int:
        # 簡易にタグを除いた文字数を見積もり
        return len(re.sub(r"<!--.*?-->|<[^>]+>", "", s))
    base_len = visible_chars(content)
    target_min, target_max = 3000, 5000

    if base_len < target_min:
        # 追記パラグラフを用意
        extras = [
            "編集部メモ：数値は嘘をつきませんが、使い心地は手のサイズや環境に左右されます。最後は“自分の生活に馴染むか”を基準にすると後悔しません。",
            "小ワザ：迷ったら“返品が容易なショップ”を選ぶと心理的安全性が上がります。レビューは鮮度（直近の投稿）にも注目。",
            "メンテ観点：保証やサポートの評判は長期満足度に直結。価格差が小さい場合はサポート重視で。"
        ]
        idx=0
        while visible_chars(content) < target_min and idx < len(extras):
            blocks.append(f"<!-- wp:paragraph --><p>{extras[idx]}</p><!-- /wp:paragraph -->")
            content = "\n".join(blocks); idx+=1

    # 上振れは放置（5,000を大きく超えない設計）
    return title, content, excerpt

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
        arr=rakuten_items((os.getenv("RAKUTEN_APP_ID") or "").strip(),
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

        # コンテンツ生成（Gutenberg・長文）
        title, content, excerpt = build_blocks_longform(kw, after_review, conf)

        # 禁止表現チェック（誇大/絶対表現など）
        blocked=False
        for bad in rules.get("prohibited_phrases",[]):
            if bad in title or bad in content:
                LOG.info(f"blocked by rule '{bad}' for '{kw}'"); blocked=True; break
        if blocked: continue

        payload={"title":title,"slug":slug,"status":"publish",
                 "content":content,"categories":cats,"excerpt":excerpt}
        headers={"Authorization": f"Basic {b64cred(WP_USER, WP_APP_PW)}","Content-Type":"application/json"}

        try:
            http_json("POST", f"{WP_URL}/wp-json/wp/v2/posts", data=json.dumps(payload), headers=headers)
            posted+=1; LOG.info(f"posted: {kw}")
        except RuntimeError as e:
            # 権限不足→下書きにフォールバック
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
