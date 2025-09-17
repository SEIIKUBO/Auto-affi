# -*- coding: utf-8 -*-
"""
Hybrid pipeline:
- Facts: Rakuten API (price/review/url/image)
- Copy/Design: ChatGPT API -> Gutenberg blocks (AFFINGER-friendly)
- Hard guards: length, required blocks, law/TOS wording
- Fail-safe: falls back to deterministic template if LLM fails
"""

import os, sys, json, time, base64, logging, math, re, statistics, random
import requests, yaml

# ---------- logging (file + console) ----------
LOG = logging.getLogger("runner")
LOG.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
fh = logging.FileHandler("run.log", encoding="utf-8"); fh.setFormatter(_fmt); LOG.addHandler(fh)
ch = logging.StreamHandler(); ch.setFormatter(_fmt); LOG.addHandler(ch)

# ---------- env ----------
WP_URL = (os.getenv("WP_SITE_URL") or "").rstrip("/")
WP_USER = os.getenv("WP_USERNAME")
WP_APP_PW = os.getenv("WP_APP_PASSWORD")
ALERT = os.getenv("ALERT_WEBHOOK_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
RAKUTEN_APP_ID = (os.getenv("RAKUTEN_APP_ID") or "").strip()

# ---------- defaults / knobs ----------
TARGET_MIN_CHARS = 3000
TARGET_MAX_CHARS = 5000
DEFAULT_MIN_ITEMS = 3
LLM_MODEL = "gpt-4o-mini"     # 低コスト高品質。必要なら config.llm.model で上書き
LLM_TEMPERATURE = 0.4
LLM_MAXTOKENS = 3500          # 十分長文を出す余裕
DIALOGUE_NAMES = ("ミオ", "レン")
BTN_CLASS = "wp-block-button__link"

# ---------- utils ----------
def b64cred(u, p): return base64.b64encode(f"{u}:{p}".encode()).decode()

def http_json(method, url, **kw):
    r = requests.request(method, url, timeout=30, **kw)
    if not r.ok:
        LOG.error(f"http_error: {method} {url} -> {r.status_code} {r.text[:400]}")
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
    return r.json()

def sanitize_keyword(kw: str) -> str:
    if not kw: return ""
    kw = kw.replace("\u3000", " ").replace("\r", " ").replace("\n", " ").replace("\t", " ")
    kw = re.sub(r"\s+", " ", kw)
    return kw.strip()

def ensure_categories(names):
    """カテゴリ作成はしない。なければ '未分類' にフォールバック（安全運用）"""
    ids = []
    for name in names:
        try:
            q = http_json("GET", f"{WP_URL}/wp-json/wp/v2/categories",
                          params={"search": name, "per_page": 100})
            cid = next((c["id"] for c in q if c["name"].lower() == name.lower()), None)
            if cid:
                ids.append(cid); continue
            default = http_json("GET", f"{WP_URL}/wp-json/wp/v2/categories", params={"slug": "uncategorized"})
            ids.append(default[0]["id"] if default else 1)
            LOG.warning(f"category '{name}' not found; fallback to Uncategorized")
        except Exception as e:
            LOG.error(f"category ensure failed: {e}; fallback to id=1"); ids.append(1)
    return ids

def wp_post_exists(slug):
    q = http_json("GET", f"{WP_URL}/wp-json/wp/v2/posts", params={"slug": slug})
    return len(q) > 0

# ---------- Rakuten ----------
def rakuten_items(app_id, kw, endpoint, hits, genreId=None):
    kw = sanitize_keyword(kw)
    params = {"applicationId": app_id, "keyword": kw, "hits": int(hits), "format": "json"}
    if genreId: params["genreId"] = genreId
    try:
        r = requests.get(endpoint, params=params, timeout=30)
        if not r.ok:
            LOG.error(f"rakuten_api_error kw='{kw}': HTTP {r.status_code} - {r.text[:300]}")
            if r.status_code == 400: return []
            r.raise_for_status()
        return (r.json() or {}).get("Items", [])
    except Exception as e:
        LOG.error(f"rakuten_request_exception kw='{kw}': {e}")
        return []

def enrich(items):
    out = []
    for it in items:
        i = it["Item"]
        price = int(i.get("itemPrice") or 0)
        rev = float(i.get("reviewAverage") or 0.0)
        rct = int(i.get("reviewCount") or 0)
        score = rev * math.log1p(max(rct, 1))
        out.append({
            "name": i.get("itemName"),
            "url": i.get("itemUrl"),
            "image": (i.get("mediumImageUrls") or [{"imageUrl": ""}])[0]["imageUrl"],
            "price": price, "review_avg": rev, "review_count": rct, "score": round(score, 2)
        })
    out.sort(key=lambda x: (-x["score"], x["price"]))
    return out

# ---------- OpenAI (Chat Completions) ----------
def openai_client():
    # 実行環境に openai パッケージがある前提（publish.ymlでインストール）
    from openai import OpenAI
    return OpenAI(api_key=OPENAI_API_KEY)

def build_facts_json(kw, items):
    """LLMへ渡す“事実パック”。数値とURLのみ。"""
    payload = {
        "keyword": kw,
        "items": [
            {
                "rank": idx + 1,
                "name": x["name"],
                "price": x["price"],
                "review_avg": x["review_avg"],
                "review_count": x["review_count"],
                "url": x["url"],
                "image": x["image"]
            } for idx, x in enumerate(items[:10])
        ],
        "policy": {
            "affiliate_disclosure": "当サイトのリンクには広告（アフィリエイトリンク）が含まれます。",
            "law_notes": [
                "価格・在庫・配送は変動。購入前にリンク先で確認。",
                "誇大な断定表現を避け、事実と主観を分離。",
                "画像は出典URLを直接参照。再配布しない。",
                "リンクには rel='sponsored noopener nofollow' を付与。"
            ]
        }
    }
    return payload

def sys_prompt():
    return (
        "あなたは日本語のシニア編集者です。WordPress(AFFINGER)に最適化した"
        "Gutenbergブロックのみで本文を生成します。Markdownは使いません。\n"
        "必須要件：\n"
        "1) 逆三角形型（結論→上位→比較→会話→選び方→FAQ→免責）。\n"
        "2) 二人の会話（登場人物名は固定：ミオ／レン）。短文。改行多め。\n"
        "3) 3,000〜5,000文字。句点ごとに改行し可読性を上げる。\n"
        "4) 文字以外の要素を定間隔で挿入（wp:image / wp:buttons / wp:table / wp:pullquote / wp:separator）。\n"
        "5) 数値・URLは提供事実のみ。推測禁止。価格目安は“〜円”ではなく提供値だけ。\n"
        "6) すべての外部リンクには rel='sponsored noopener nofollow' を付ける。\n"
        "7) 誇大・断定表現を避け、広告開示文を先頭付近に入れる。\n"
        "8) 出力は本文ブロックのみ。タイトルやメタは含めない。\n"
    )

def user_prompt(facts_json, min_chars, max_chars):
    return (
        "【事実データ(JSON)】\n"
        + json.dumps(facts_json, ensure_ascii=False)
        + "\n\n【指示】\n"
        f"- 文字数は {min_chars}〜{max_chars} 文字。\n"
        "- Gutenbergブロック（<!-- wp:* -->）のみで構成する。\n"
        "- 目次は不要。見出しIDは summary / picks / compare / talk / howto / faq を使う。\n"
        "- 上位3商品はカード風（画像・短評・長所短所・ボタン）。\n"
        "- 比較表は提供データの上位N件で作成。\n"
        "- 会話セクションはミオとレンが交互に短文で説明。\n"
        "- 購入判断は自己責任で、在庫と価格はリンク先で最終確認と明記。\n"
        "- AFFINGERで崩れにくい標準ブロックのみ使う。\n"
        "- 出力は本文のみ。コードブロックや余計な注釈は禁止。\n"
    )

def validate_content(html: str, min_chars: int, max_chars: int) -> (bool, list):
    """必須ブロックや長さを検証。NGなら理由を返す。"""
    errs = []
    text = re.sub(r"<!--.*?-->", "", html, flags=re.DOTALL)
    text = re.sub(r"<[^>]+>", "", text)
    L = len(text)

    if L < min_chars: errs.append(f"too_short:{L}")
    if L > max_chars: errs.append(f"too_long:{L}")

    required_ids = ["summary", "picks", "compare", "talk", "howto", "faq"]
    for rid in required_ids:
        if f'id="{rid}"' not in html:
            errs.append(f"missing_section:{rid}")

    # 必須ブロック（最低回数）
    if html.count("<!-- wp:image") < 2: errs.append("few_images")
    if html.count("<!-- wp:buttons") < 2: errs.append("few_buttons")
    if html.count("<!-- wp:table") < 1: errs.append("missing_table")
    if html.count("<!-- wp:pullquote") < 1: errs.append("missing_pullquote")
    if html.count("<!-- wp:separator") < 2: errs.append("few_separators")

    # rel属性の確認（最低1つ）
    if "rel=\"sponsored noopener nofollow\"" not in html:
        errs.append("missing_rel_sponsored")

    return (len(errs) == 0), errs

def save_artifacts(prompt_obj, content):
    try:
        with open("llm_prompt.json", "w", encoding="utf-8") as f:
            json.dump(prompt_obj, f, ensure_ascii=False, indent=2)
        with open("llm_output.txt", "w", encoding="utf-8") as f:
            f.write(content)
    except Exception as e:
        LOG.error(f"artifact save failed: {e}")

def llm_generate_gutenberg(kw, items, conf) -> str:
    """LLMで本文を生成。2回までリトライ。失敗時は空文字を返す。"""
    if not OPENAI_API_KEY:
        LOG.warning("OPENAI_API_KEY not set; skip LLM")
        return ""
    try:
        from openai import OpenAI
    except Exception:
        LOG.error("openai package not found")
        return ""

    # config上書き
    llm_cfg = conf.get("llm", {}) if isinstance(conf.get("llm"), dict) else {}
    model = llm_cfg.get("model", LLM_MODEL)
    temp = float(llm_cfg.get("temperature", LLM_TEMPERATURE))
    min_chars = int(llm_cfg.get("min_chars", TARGET_MIN_CHARS))
    max_chars = int(llm_cfg.get("max_chars", TARGET_MAX_CHARS))

    facts = build_facts_json(kw, items)
    prompt_obj = {
        "system": sys_prompt(),
        "user": user_prompt(facts, min_chars, max_chars)
    }

    client = openai_client()
    last_html = ""
    for attempt in range(1, 3):  # 最大2回
        try:
            resp = client.chat.completions.create(
                model=model,
                temperature=temp,
                max_tokens=LLM_MAXTOKENS,
                messages=[
                    {"role": "system", "content": prompt_obj["system"]},
                    {"role": "user", "content": prompt_obj["user"]}
                ]
            )
            html = (resp.choices[0].message.content or "").strip()
            ok, errs = validate_content(html, min_chars, max_chars)
            if ok:
                save_artifacts(prompt_obj, html)
                return html
            else:
                LOG.warning(f"llm validation failed (attempt {attempt}): {errs}")
                # 追撃プロンプトで修正依頼
                fix_inst = (
                    "修正してください。以下の不備をすべて解消し、本文のみ再出力："
                    + ", ".join(errs)
                )
                resp2 = client.chat.completions.create(
                    model=model,
                    temperature=temp,
                    max_tokens=LLM_MAXTOKENS,
                    messages=[
                        {"role": "system", "content": prompt_obj["system"]},
                        {"role": "user", "content": prompt_obj["user"]},
                        {"role": "assistant", "content": html},
                        {"role": "user", "content": fix_inst}
                    ]
                )
                html2 = (resp2.choices[0].message.content or "").strip()
                ok2, errs2 = validate_content(html2, min_chars, max_chars)
                save_artifacts(prompt_obj, html2)
                if ok2:
                    return html2
                else:
                    LOG.error(f"llm second validation failed: {errs2}")
                    last_html = html2
        except Exception as e:
            LOG.error(f"llm call failed (attempt {attempt}): {e}")
            time.sleep(2)

    save_artifacts(prompt_obj, last_html or "")
    return ""  # 失敗時はテンプレにフォールバック

# ---------- Fallback template (deterministic, Gutenberg) ----------
def fallback_template(kw, items, conf) -> str:
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
        html += rows; html.append("</tbody></table>")
        return f"<!-- wp:table --><figure class=\"wp-block-table\">{''.join(html)}</figure><!-- /wp:table -->"

    n = min(10, len(items))
    best = items[0]
    blocks = []
    blocks.append(f"<!-- wp:paragraph --><p>{conf['site']['affiliate_disclosure']}</p><!-- /wp:paragraph -->")
    blocks.append("<!-- wp:heading --><h2 id=\"summary\">要約</h2><!-- /wp:heading -->")
    blocks.append(f"<!-- wp:paragraph --><p>結論。{best['name']}。評価 {best['review_avg']:.1f}（{best['review_count']}件）。価格 ¥{best['price']}。在庫と価格は変動。リンク先で確認。</p><!-- /wp:paragraph -->")
    blocks.append(btn(best["url"], "最安値を確認"))
    blocks.append(img(best["image"], best["name"]))
    blocks.append(sep())

    blocks.append("<!-- wp:heading --><h2 id=\"picks\">上位の候補</h2><!-- /wp:heading -->")
    for i, x in enumerate(items[:3], start=1):
        blocks.append(
            "<!-- wp:columns --><div class=\"wp-block-columns\">"
            f"<div class=\"wp-block-column\" style=\"flex-basis:28%\">{img(x['image'], x['name'])}</div>"
            "<div class=\"wp-block-column\" style=\"flex-basis:72%\">"
            f"<!-- wp:heading {{\"level\":3}} --><h3>{i}. {x['name']}</h3><!-- /wp:heading -->"
            f"<!-- wp:paragraph --><p>価格 ¥{x['price']}。評価 {x['review_avg']:.1f}（{x['review_count']}件）。</p><!-- /wp:paragraph -->"
            f"{btn(x['url'], '在庫を見る')}"
            "</div></div><!-- /wp:columns -->"
        )
    blocks.append(sep())

    rows=[]
    for i, x in enumerate(items[:n], start=1):
        rows.append(
            f"<tr><td>{i}</td><td><a href=\"{x['url']}\" rel=\"sponsored noopener nofollow\">{x['name']}</a></td><td>¥{x['price']}</td><td>{x['review_avg']:.1f}</td><td>{x['review_count']}</td></tr>"
        )
    blocks.append("<!-- wp:heading --><h2 id=\"compare\">比較表</h2><!-- /wp:heading -->")
    blocks.append(table(rows))
    blocks.append(sep())

    # 最低限の会話/選び方/FAQ/免責
    blocks.append("<!-- wp:heading --><h2 id=\"talk\">会話</h2><!-- /wp:heading -->")
    blocks.append("<!-- wp:paragraph --><p><strong>ミオ：</strong>結論は先に。迷うなら上位から選ぶ。</p><!-- /wp:paragraph -->")
    blocks.append("<!-- wp:paragraph --><p><strong>レン：</strong>理由はシンプル。レビュー密度と価格の釣り合い。</p><!-- /wp:paragraph -->")
    blocks.append(pull("短く決める。数で判断。迷いは削る。"))
    blocks.append(sep())
    blocks.append("<!-- wp:heading --><h2 id=\"howto\">選び方</h2><!-- /wp:heading -->")
    blocks.append("<!-- wp:list --><ul><li>用途→出力→端子の順で決める。</li><li>相場を外さない。</li><li>在庫と納期を直前に確認。</li></ul><!-- /wp:list -->")
    blocks.append(sep())
    blocks.append("<!-- wp:heading --><h2 id=\"faq\">FAQ</h2><!-- /wp:heading -->")
    blocks.append("<!-- wp:paragraph --><p><strong>Q.</strong> 最安値は固定？<br/><strong>A.</strong> いいえ。変動する。購入前に確認。</p><!-- /wp:paragraph -->")
    blocks.append("<!-- wp:paragraph --><p><strong>Q.</strong> レビューは信用できる？<br/><strong>A.</strong> 平均と件数の両方を見る。</p><!-- /wp:paragraph -->")
    blocks.append(sep())
    blocks.append("<!-- wp:paragraph --><p>本記事は公開APIの数値を集計した解説。価格・在庫・配送は変動。購入判断は自己責任。リンクには広告（アフィリエイト）を含む。画像は出典URLの直参照で再配布しない。出典：楽天市場API。</p><!-- /wp:paragraph -->")

    return "\n".join(blocks)

# ---------- Title/Excerpt ----------
def build_title_excerpt(kw, n, best, conf):
    pats = (conf.get("ab_tests") or {}).get("title_patterns") or []
    if conf.get("content", {}).get("ab_test") and pats:
        title = pats[hash(kw) % len(pats)].format(kw=kw, n=n)
    else:
        title = f"{kw}のおすすめ{min(n,10)}選｜価格×レビュー密度で比較"
    excerpt = f"{kw}の結論を先に。上位モデルをカードで提示。比較表と会話で迷いを削る。"
    return title, excerpt

# ---------- WP post ----------
def create_post(payload):
    headers={"Authorization": f"Basic {b64cred(WP_USER, WP_APP_PW)}","Content-Type":"application/json"}
    try:
        return http_json("POST", f"{WP_URL}/wp-json/wp/v2/posts", data=json.dumps(payload), headers=headers)
    except RuntimeError as e:
        # 権限不足→ドラフトに自動フォールバック
        if "HTTP 401" in str(e) or "HTTP 403" in str(e):
            LOG.warning("permission denied for publish; fallback to draft")
            payload2 = payload.copy(); payload2["status"] = "draft"
            return http_json("POST", f"{WP_URL}/wp-json/wp/v2/posts", data=json.dumps(payload2), headers=headers)
        raise

# ---------- notify ----------
def notify(msg):
    if not ALERT: return
    try: requests.post(ALERT, json={"content": msg}, timeout=10)
    except Exception as e: LOG.error(f"alert failed: {e}")

# ---------- main ----------
def main():
    try:
        cfg = sys.argv[sys.argv.index("--config")+1]
    except ValueError:
        cfg = "config/app.yaml"
    with open(cfg, "r", encoding="utf-8") as f:
        conf = yaml.safe_load(f)

    # optional files
    try:
        if conf.get("content", {}).get("ab_test"):
            conf["ab_tests"] = yaml.safe_load(open("src/ab_tests.yaml", "r", encoding="utf-8"))
        else:
            conf["ab_tests"] = {}
    except Exception:
        conf["ab_tests"] = {}

    try:
        rules = yaml.safe_load(open(conf["review"]["rules_file"], "r", encoding="utf-8"))
        bads = rules.get("prohibited_phrases", [])
    except Exception:
        bads = []

    cats = ensure_categories(conf["site"]["category_names"])
    min_items = int(conf.get("content", {}).get("min_items", DEFAULT_MIN_ITEMS))

    posted = 0
    for raw_kw in conf["keywords"]["seeds"]:
        kw = sanitize_keyword(raw_kw)
        if not kw: continue
        if posted >= int(conf["site"]["posts_per_run"]): break

        # slug重複回避
        from slugify import slugify
        slug = slugify(kw)
        if wp_post_exists(slug):
            LOG.info(f"skip exists (slug duplicate): {kw}")
            continue

        LOG.info(f"query kw='{kw}'")
        arr = rakuten_items(RAKUTEN_APP_ID,
                            kw, conf["data_sources"]["rakuten"]["endpoint"],
                            conf["data_sources"]["rakuten"]["max_per_seed"],
                            conf["data_sources"]["rakuten"].get("genreId"))
        enriched = enrich(arr)
        # フィルタ
        after_price  = [it for it in enriched if it["price"] >= conf["content"]["price_floor"]]
        after_review = [it for it in after_price if it["review_avg"] >= conf["content"]["review_floor"]]
        LOG.info(f"stats kw='{kw}': total={len(arr)}, enriched={len(enriched)}, after_price={len(after_price)}, after_review={len(after_review)}")
        if len(after_review) < min_items:
            LOG.info(f"skip thin (<{min_items} items) for '{kw}'")
            continue

        n = min(10, len(after_review))
        best = after_review[0]
        title, excerpt = build_title_excerpt(kw, n, best, conf)

        # LLM生成（失敗したらテンプレ）
        body_html = llm_generate_gutenberg(kw, after_review, conf)
        if not body_html:
            LOG.warning("LLM failed or disabled; falling back to template")
            body_html = fallback_template(kw, after_review, conf)

        # 法務チェック
        if any(bad in body_html for bad in bads):
            LOG.info(f"blocked by rule for '{kw}'")
            continue

        payload = {
            "title": title,
            "slug": slug,
            "status": "publish",
            "content": body_html,
            "categories": cats,
            "excerpt": excerpt
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

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        notify(f"[AUTO-REV] job failed: {e}")
        raise
