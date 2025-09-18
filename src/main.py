# -*- coding: utf-8 -*-
"""
Hybrid v2 (strict LLM required, structured alerts, robust config)
- Facts: Rakuten API
- Body: ChatGPT API（必須）
- No template fallback: 失敗/未設定時は投稿せずDiscordへ通知
- Alerts: 人間可読 + JSON（コピペでAIが解析しやすい）
- Config robustness: 重要キーが欠落しても安全なデフォルトで補正し、警告通知
- Guards: 文字数/必須ブロック/rel属性/法務NG→投稿しない＋通知
"""

import os, sys, json, time, base64, logging, math, re, statistics
import requests, yaml
from slugify import slugify
from datetime import datetime, timezone, timedelta

# ---------- logging ----------
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

# ---------- knobs ----------
TARGET_MIN_CHARS = 3000
TARGET_MAX_CHARS = 5000
DEFAULT_MIN_ITEMS = 3
LLM_MODEL = "gpt-4o-mini"
LLM_TEMPERATURE = 0.4
LLM_MAXTOKENS = 5500
BTN_CLASS = "wp-block-button__link"

# ---------- utils ----------
def jst_now_iso():
    jst = timezone(timedelta(hours=9))
    return datetime.now(jst).isoformat(timespec="seconds")

def runtime_context():
    repo = os.getenv("GITHUB_REPOSITORY") or ""
    run_id = os.getenv("GITHUB_RUN_ID") or ""
    attempt = os.getenv("GITHUB_RUN_ATTEMPT") or ""
    ref_name = os.getenv("GITHUB_REF_NAME") or ""
    sha = os.getenv("GITHUB_SHA") or ""
    workflow = os.getenv("GITHUB_WORKFLOW") or "publish"
    server = os.getenv("GITHUB_SERVER_URL") or "https://github.com"
    run_url = f"{server}/{repo}/actions/runs/{run_id}" if repo and run_id else ""
    return {"repo": repo, "workflow": workflow, "run_id": run_id, "run_attempt": attempt,
            "branch": ref_name, "sha": sha, "run_url": run_url}

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
    return re.sub(r"\s+"," ",kw).strip()

def notify_event(event:str, severity:str, kw:str="", stage:str="", reason:str="", **extra):
    """
    人間可読 + JSON をまとめて送信。
    先頭行: [AUTO-REV][severity] EVENT=... KW="..."
    続けて JSON を ```json ... ``` で同梱（AIがそのまま解析可能）。
    """
    payload = {"event": event, "severity": severity, "kw": kw, "stage": stage,
               "reason": reason, "ts_jst": jst_now_iso(), "ctx": runtime_context()}
    payload.update(extra or {})
    head = f"[AUTO-REV][{severity.upper()}] EVENT={event} KW=\"{kw}\""
    content = head + "\n```json\n" + json.dumps(payload, ensure_ascii=False, indent=2) + "\n```"
    if not ALERT:
        LOG.warning(f"[alert skipped] {head} | reason={reason}")
        return
    try:
        requests.post(ALERT, json={"content": content}, timeout=10)
    except Exception as e:
        LOG.error(f"alert failed: {e}")

# ---------- WP ----------
def ensure_categories(names):
    ids=[]
    for name in names:
        try:
            q=http_json("GET", f"{WP_URL}/wp-json/wp/v2/categories", params={"search":name,"per_page":100})
            cid=next((c["id"] for c in q if c["name"].lower()==name.lower()), None)
            if cid: ids.append(cid); continue
            default=http_json("GET", f"{WP_URL}/wp-json/wp/v2/categories", params={"slug":"uncategorized"})
            ids.append(default[0]["id"] if default else 1)
        except Exception as e:
            LOG.error(f"category ensure failed: {e}"); ids.append(1)
    return ids

def wp_post_exists(slug):
    q=http_json("GET", f"{WP_URL}/wp-json/wp/v2/posts", params={"slug":slug})
    return len(q)>0

# ---------- Rakuten ----------
def rakuten_items(app_id, kw, endpoint, hits, genreId=None):
    kw=sanitize_keyword(kw)
    params={"applicationId":app_id,"keyword":kw,"hits":int(hits),"format":"json"}
    if genreId: params["genreId"]=genreId
    try:
        r=requests.get(endpoint, params=params, timeout=30)
        if not r.ok:
            LOG.error(f"rakuten_api_error kw='{kw}': HTTP {r.status_code} - {r.text[:300]}")
            if r.status_code==400: return []
        return (r.json() or {}).get("Items",[])
    except Exception as e:
        LOG.error(f"rakuten_request_exception kw='{kw}': {e}")
        return []

def enrich(items):
    out=[]
    for it in items:
        i=it["Item"]
        price=int(i.get("itemPrice") or 0)
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

# ---------- OpenAI ----------
def openai_client():
    from openai import OpenAI
    return OpenAI(api_key=OPENAI_API_KEY)

# ---------- Prompts（仕様書反映・厳密JSON返却） ----------
def sys_prompt():
    return (
        "あなたは「一次情報最優先・法令順守のアフィリエイト記事ライター兼編集者」です。"
        "日本語で、H2中心の構成・正確性重視・不必要に煽らないトーンで執筆します。"
        "楽天APIで提供された事実のみを使用し、価格・在庫は変動前提。未確定の価格や推測は禁止。"
        "すべての外部リンクには rel='sponsored noopener nofollow' を付与。"
        "WordPress(AFFINGER)に貼れるよう本文はGutenbergブロックで生成。"
        "出力は厳密なJSONのみ。余計な文字や説明は禁止。"
        "JSON schema: "
        "{"
        "\"titles\": [string x8],"
        "\"meta\": {\"slug\": string, \"description\": string, \"intent_map\": [{\"heading\": string, \"query_examples\": [string]}]},"
        "\"outline\": [{\"h2\": string, \"h3\": [string]}],"
        "\"body_gutenberg\": string,"
        "\"table_gutenberg\": string,"
        "\"ctas\": {\"hesitant\": string, \"decider\": string, \"comparer\": string},"
        "\"footnotes\": [string],"
        "\"jsonld\": string,"
        "\"ogp_prompts\": [string]"
        "}"
        "文字数は本文合計でおよそ3000〜5000字。段落は短く。"
    )

def build_facts_json(kw, items, inputs, policy):
    return {
        "keyword": kw,
        "items": [{
            "rank": idx+1,
            "name": x["name"], "price": x["price"],
            "review_avg": x["review_avg"], "review_count": x["review_count"],
            "url": x["url"], "image": x["image"]
        } for idx,x in enumerate(items[:10])],
        "inputs": inputs,
        "policy": policy
    }

def user_prompt(facts_json):
    spec = """
【目的】
検索流入と指名流入の両方で読者の意思決定を助け、適切なCTAで離脱せずに比較→選択→購入へ導く記事を作る。

【読者像】
- 読者タイプ: {初心者}
- 主要ニーズ: {コスパが良く失敗しにくいものだけ知りたい}
- シチュエーション: {一人暮らし/家族}

【入力（可能な範囲で埋める）】
- 記事タイプ: {比較まとめ/ランキング/用途別おすすめ}
- 注意点/除外: {医療・効果の断定表現を禁止、未確定の価格は書かない}
- 競合比較の軸（3〜5）（順不同）: {耐久性/保証/カラーバリエーション/追加オプション/流通度合い/話題性}
- 内部リンク予定: {URL}

【厳守事項（重要）】
1) 一次情報（公式サイト/メーカー/正規販売ページ/公式X・プレス）を優先し、事実は必ず根拠リンクを脚注で示す。
2) 価格・在庫・キャンペーンは変動前提。「執筆時点」表現と注意書きを必ず入れる。最安や断定表現は禁止。
3) 比較は公正・具体。欠点も必ず記載。誇大・医療/効果効能の断定・体験の一般化をしない。
4) Amazonアソシエイト等の開示文を本文冒頭か直後に掲載。
5) 読みやすさ優先：H2中心、段落短め、箇条書きを多用。結論→理由→具体例の順で。

【出力仕様（JSONに格納）】
1. titles: タイトル案 ×8（32〜48字 / クリックベイト不可）
2. meta: slug／meta description（全角80〜120字）／主要見出しの想定検索意図マッピング
3. outline: 記事アウトライン（H2/H3）
4. body_gutenberg: 本文（Gutenbergブロックで出力）
5. table_gutenberg: 比較表（Gutenbergのtableブロック）
6. ctas: CTAブロック（3パターン / 迷っている人・即決したい人・さらに比較したい人）
7. footnotes: 脚注（参照URL列挙。提供データのURLのみ使用）
8. jsonld: 構造化データ（FAQPage + ItemList。価格は不明なら記載しない）
9. ogp_prompts: OGP/アイキャッチ画像プロンプト案 ×3
"""
    return ("【事実データ(JSON)】\n" + json.dumps(facts_json, ensure_ascii=False)
            + "\n\n【仕様書】\n" + spec
            + "\n【注意】\n"
            "- 事実は提供JSONのみ。未記載の数値や主張は書かない。\n"
            "- すべてのリンクに rel='sponsored noopener nofollow'。\n"
            "- 出力は厳密JSONのみ。")

# ---------- LLM ----------
def save_artifacts(prompt_obj, raw, plan):
    try:
        with open("llm_prompt.json","w",encoding="utf-8") as f: json.dump(prompt_obj,f,ensure_ascii=False,indent=2)
        with open("llm_output.txt","w",encoding="utf-8") as f: f.write(raw or "")
        if plan is not None:
            with open("llm_plan.json","w",encoding="utf-8") as f: json.dump(plan,f,ensure_ascii=False,indent=2)
    except Exception as e:
        LOG.error(f"artifact save failed: {e}")

def llm_plan_json(kw, items, conf, inputs, policy):
    if not OPENAI_API_KEY:
        notify_event(event="LLM_DISABLED", severity="error", kw=kw, stage="setup", reason="OPENAI_API_KEY missing")
        LOG.error("OPENAI_API_KEY missing")
        return None
    from openai import OpenAI
    client = openai_client()
    llm_cfg = conf.get("llm", {}) if isinstance(conf.get("llm"), dict) else {}
    model = llm_cfg.get("model", LLM_MODEL)
    temp  = float(llm_cfg.get("temperature", LLM_TEMPERATURE))
    facts = build_facts_json(kw, items, inputs, policy)
    prompt_obj = {"system": sys_prompt(), "user": user_prompt(facts)}
    raw = ""
    for attempt in range(1,3):
        try:
            resp = client.chat.completions.create(
                model=model, temperature=temp, max_tokens=LLM_MAXTOKENS,
                messages=[{"role":"system","content":prompt_obj["system"]},
                          {"role":"user","content":prompt_obj["user"]}]
            )
            raw = (resp.choices[0].message.content or "").strip()
            plan = None
            try:
                plan = json.loads(raw)
            except Exception:
                m=re.search(r"\{.*\}", raw, flags=re.DOTALL)
                if m:
                    try: plan = json.loads(m.group(0))
                    except Exception: plan = None
            save_artifacts(prompt_obj, raw, plan)
            if plan: return plan
            notify_event(event="LLM_PARSE_ERROR", severity="warning", kw=kw, stage="llm_call",
                         reason="LLM応答をJSONとして解釈できない", attempt=attempt, sample=raw[:500])
        except Exception as e:
            notify_event(event="LLM_CALL_FAILED", severity="error", kw=kw, stage="llm_call",
                         reason="OpenAI API呼び出しに失敗", attempt=attempt, exception=str(e))
            time.sleep(2)
    notify_event(event="LLM_FAILED_FINAL", severity="error", kw=kw, stage="llm_call", reason="再試行の結果も失敗")
    return None

# ---------- validate ----------
def validate_gutenberg(html: str, min_chars:int, max_chars:int):
    errs=[]
    text=re.sub(r"<!--.*?-->", "", html, flags=re.DOTALL)
    text=re.sub(r"<[^>]+>", "", text)
    L=len(text)
    if L<min_chars: errs.append(f"too_short:{L}")
    if L>max_chars: errs.append(f"too_long:{L}")
    if "rel=\"sponsored noopener nofollow\"" not in html: errs.append("missing_rel_sponsored")
    if html.count("<!-- wp:table")<1: errs.append("missing_table")
    if html.count("<!-- wp:buttons")<1: errs.append("few_buttons")
    return (len(errs)==0), errs

def create_post(payload):
    headers={"Authorization": f"Basic {b64cred(WP_USER, WP_APP_PW)}","Content-Type":"application/json"}
    try:
        return http_json("POST", f"{WP_URL}/wp-json/wp/v2/posts", data=json.dumps(payload), headers=headers)
    except RuntimeError as e:
        if "HTTP 401" in str(e) or "HTTP 403" in str(e):
            payload2=payload.copy(); payload2["status"]="draft"
            return http_json("POST", f"{WP_URL}/wp-json/wp/v2/posts", data=json.dumps(payload2), headers=headers)
        raise

# ---------- main ----------
def main():
    try:
        cfg=sys.argv[sys.argv.index("--config")+1]
    except ValueError:
        cfg="config/app.yaml"
    with open(cfg,"r",encoding="utf-8") as f:
        conf=yaml.safe_load(f)

    # optional
    try:
        conf["ab_tests"]=yaml.safe_load(open("src/ab_tests.yaml","r",encoding="utf-8")) if conf.get("content",{}).get("ab_test") else {}
    except Exception:
        conf["ab_tests"]={}
    try:
        rules=yaml.safe_load(open(conf["review"]["rules_file"],"r",encoding="utf-8"))
        bads=rules.get("prohibited_phrases",[])
    except Exception:
        bads=[]

    # ----- Robust config defaults & warnings -----
    site = conf.get("site") or {}
    category_names = site.get("category_names") or ["レビュー"]
    posts_per_run = int(site.get("posts_per_run", 1))
    internal_link = site.get("internal_link", "")

    disclosure = site.get("affiliate_disclosure")
    if not disclosure:
        disclosure = "当サイトはアフィリエイト広告（Amazonアソシエイト含む）を利用しています。"
        notify_event(event="CONFIG_DEFAULTED", severity="warning", stage="setup",
                     reason="site.affiliate_disclosure が未設定のためデフォルトを適用",
                     defaults={"affiliate_disclosure": disclosure})

    cats=ensure_categories(category_names)
    min_items=int(conf.get("content",{}).get("min_items", DEFAULT_MIN_ITEMS))

    inputs={
        "article_type": "比較まとめ/ランキング/用途別おすすめ",
        "notes": "医療・効果の断定表現を禁止、未確定の価格は書かない",
        "compare_axes": ["耐久性","保証","カラーバリエーション","追加オプション","流通度合い","話題性"],
        "internal_link": internal_link
    }
    policy={
        "disclosure": disclosure,
        "cta_note": "購入判断は自己責任。価格・在庫は変動。リンク先で最終確認。"
    }

    # 事前チェック：APIキー
    if not OPENAI_API_KEY:
        notify_event(event="LLM_DISABLED", severity="error", kw="", stage="setup", reason="OPENAI_API_KEY missing（Run中断）")
        LOG.error("OPENAI_API_KEY missing — abort run")
        return

    posted=0
    for raw_kw in conf["keywords"]["seeds"]:
        if posted>=posts_per_run: break
        kw=sanitize_keyword(raw_kw)
        if not kw: continue
        slug=slugify(kw)
        if wp_post_exists(slug):
            LOG.info(f"skip exists (slug duplicate): {kw}")
            continue

        LOG.info(f"query kw='{kw}'")
        arr = rakuten_items(RAKUTEN_APP_ID,
                            kw, conf["data_sources"]["rakuten"]["endpoint"],
                            conf["data_sources"]["rakuten"]["max_per_seed"],
                            conf["data_sources"]["rakuten"].get("genreId"))
        enriched=enrich(arr)
        after_price=[it for it in enriched if it["price"]>=conf["content"]["price_floor"]]
        after_review=[it for it in after_price if it["review_avg"]>=conf["content"]["review_floor"]]
        LOG.info(f"stats kw='{kw}': total={len(arr)}, enriched={len(enriched)}, after_price={len(after_price)}, after_review={len(after_review)}")
        if len(after_review)<min_items:
            LOG.info(f"skip thin (<{min_items}) for '{kw}'")
            continue

        plan = llm_plan_json(kw, after_review, conf, inputs, policy)
        if not plan or not isinstance(plan, dict) or not plan.get("body_gutenberg"):
            # 失敗通知は llm_plan_json 内で実施済み
            continue

        body = plan["body_gutenberg"]
        if plan.get("table_gutenberg"): body += "\n" + plan["table_gutenberg"]
        if plan.get("ctas"):
            for k in ("hesitant","decider","comparer"):
                v = plan["ctas"].get(k)
                if v: body += "\n" + v
        if plan.get("footnotes"):
            body += "\n<!-- wp:heading --><h2>参考・出典</h2><!-- /wp:heading -->"
            lis = "".join([f"<li><a href=\"{u}\" rel=\"sponsored noopener nofollow\">{u}</a></li>" for u in plan["footnotes"]])
            body += f"<!-- wp:list --><ul>{lis}</ul><!-- /wp:list -->"
        if plan.get("jsonld"):
            body += f"\n<!-- wp:html --><script type=\"application/ld+json\">{plan['jsonld']}</script><!-- /wp:html -->"

        titles = plan.get("titles") or []
        meta = plan.get("meta") or {}
        title = titles[0] if titles else f"{kw}のおすすめ{min(10,len(after_review))}選"
        excerpt = meta.get("description") or "価格は変動。詳細はリンク先で確認。"

        ok, errs = validate_gutenberg(body, TARGET_MIN_CHARS, TARGET_MAX_CHARS)
        if not ok:
            notify_event(event="VALIDATION_FAILED", severity="warning", kw=kw, stage="validation",
                         reason="本文検証NG", errors=errs)
            LOG.error(f"validation failed: {errs}")
            continue

        if any(b in body for b in bads):
            notify_event(event="BLOCKED_BY_RULE", severity="warning", kw=kw, stage="rule",
                         reason="法務/NG語に該当", ng_hits=[b for b in bads if b in body])
            LOG.info(f"blocked by rule for '{kw}'")
            continue

        payload={"title":title,"slug":slug,"status":"publish",
                 "content":body,"categories":cats,"excerpt":excerpt}
        try:
            create_post(payload)
            posted+=1; LOG.info(f"posted: {kw}")
        except Exception as e:
            notify_event(event="POST_FAILED", severity="error", kw=kw, stage="post",
                         reason="WordPress投稿に失敗", exception=str(e))
            LOG.error(f"post failed: {e}")
            time.sleep(2)

    LOG.info(f"done, posted={posted}")

if __name__=="__main__":
    try: main()
    except Exception as e:
        notify_event(event="RUN_FAILED", severity="error", kw="", stage="run", reason="未捕捉の例外", exception=str(e))
        raise
