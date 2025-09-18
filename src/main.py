# -*- coding: utf-8 -*-
"""
Hybrid v3.1 (LLM required, model fallback, structured alerts, robust config, keyword expansion)
- Facts: Rakuten API（30件/ページを自動ページング）
- Body: ChatGPT API（必須。テンプレ無し）
- Alerts: 人間可読 + JSON（Discord）
- Config: 欠落は安全デフォルト＋警告通知
- Keywords: mode=static | pools | expand_llm
- Slug: 重複時は -YYYYMMDD（既定ON）
- Guards: 文字数/必須ブロック/rel属性/法務NG→投稿しない＋通知
- NEW: LLMモデルは conf.llm.model が未提供/未権限なら conf.llm.fallback_models の順で自動フォールバック
"""

import os, sys, json, time, base64, logging, math, re, random
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
DEFAULT_MIN_ITEMS = 2
LLM_DEFAULT_MODEL = "gpt-4o-mini"
LLM_TEMPERATURE = 0.4
LLM_MAXTOKENS = 5500

# ---------- time utils ----------
def jst_tz(): return timezone(timedelta(hours=9))
def jst_now_iso(): return datetime.now(jst_tz()).isoformat(timespec="seconds")
def jst_date_compact(): return datetime.now(jst_tz()).strftime("%Y%m%d")

# ---------- runtime ctx ----------
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

# ---------- small utils ----------
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

# ---------- Alerts (Discord) ----------
def notify_event(event:str, severity:str, kw:str="", stage:str="", reason:str="", **extra):
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

def unique_slug(base_slug:str, enable_date_suffix:bool)->str:
    if not wp_post_exists(base_slug): return base_slug
    if not enable_date_suffix: return base_slug
    date = jst_date_compact()
    slug = f"{base_slug}-{date}"
    if not wp_post_exists(slug):
        notify_event("DUPLICATE_SLUG_SUFFIX","info", base_slug,"prepost","slug重複→日付付与", new_slug=slug)
        return slug
    n=2
    while wp_post_exists(f"{slug}-{n}") and n<=5: n+=1
    new_slug=f"{slug}-{n}"
    notify_event("DUPLICATE_SLUG_SUFFIX","info", base_slug,"prepost","slug重複→日付+連番", new_slug=new_slug)
    return new_slug

# ---------- Rakuten ----------
def rakuten_items(app_id, kw, endpoint, want_max, genreId=None):
    kw = sanitize_keyword(kw)
    per_page = 30
    remaining = max(1, int(want_max))
    page = 1
    max_pages = min(5, (remaining + per_page - 1)//per_page)
    out = []
    while remaining > 0 and page <= max_pages:
        hits = min(per_page, remaining)
        params = {"applicationId": app_id, "keyword": kw, "hits": hits, "page": page, "format":"json"}
        if genreId: params["genreId"] = genreId
        try:
            r = requests.get(endpoint, params=params, timeout=30)
            if not r.ok:
                LOG.error(f"rakuten_api_error kw='{kw}': HTTP {r.status_code} - {r.text[:300]}")
                break
            items = (r.json() or {}).get("Items", []) or []
            out.extend(items)
            if len(items) < hits: break
            remaining -= hits; page += 1
            time.sleep(0.25)
        except Exception as e:
            LOG.error(f"rakuten_request_exception kw='{kw}': {e}")
            break
    return out

def enrich(items):
    out=[]
    for it in items:
        i=it.get("Item", {})
        price=int(i.get("itemPrice") or 0)
        rev=float(i.get("reviewAverage") or 0.0)
        rct=int(i.get("reviewCount") or 0)
        score=rev*math.log1p(max(rct,1))
        out.append({
            "name":i.get("itemName",""), "url":i.get("itemUrl",""),
            "image":(i.get("mediumImageUrls") or [{"imageUrl":""}])[0].get("imageUrl",""),
            "price":price,"review_avg":rev,"review_count":rct,"score":round(score,2)
        })
    out=[x for x in out if x["name"] and x["url"]]
    out.sort(key=lambda x:(-x["score"], x["price"]))
    return out

# ---------- OpenAI ----------
def openai_client():
    from openai import OpenAI
    return OpenAI(api_key=OPENAI_API_KEY)

# ---------- Prompts ----------
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
        "inputs": inputs, "policy": policy
    }

def user_prompt(facts_json):
    spec = """
【目的】
検索流入と指名流入の両方で読者の意思決定を助け、適切なCTAで離脱せずに比較→選択→購入へ導く記事を作る。
【厳守事項】一次情報URLの脚注、価格は変動、誇大・断定禁止、開示文を本文冒頭、H2中心で短段落。
【出力】titles/meta/outline/body_gutenberg/table_gutenberg/ctas/footnotes/jsonld/ogp_prompts（厳密JSON）。
"""
    return ("【事実データ(JSON)】\n" + json.dumps(facts_json, ensure_ascii=False)
            + "\n\n【仕様書】\n" + spec
            + "\n【注意】\n- 事実は提供JSONのみ。未記載の数値や主張は書かない。\n- すべてのリンクに rel='sponsored noopener nofollow'。\n- 出力は厳密JSONのみ。")

# ---------- LLM (with fallback) ----------
def save_artifacts(prompt_obj, raw, plan):
    try:
        with open("llm_prompt.json","w",encoding="utf-8") as f: json.dump(prompt_obj,f,ensure_ascii=False,indent=2)
        with open("llm_output.txt","w",encoding="utf-8") as f: f.write(raw or "")
        if plan is not None:
            with open("llm_plan.json","w",encoding="utf-8") as f: json.dump(plan,f,ensure_ascii=False,indent=2)
    except Exception as e:
        LOG.error(f"artifact save failed: {e}")

def is_model_not_found(exc_text:str)->bool:
    if not exc_text: return False
    t = exc_text.lower()
    return ("model_not_found" in t) or ("does not exist" in t and "model" in t) or ("error code: 404" in t)

def llm_plan_json(kw, items, conf, inputs, policy):
    if not OPENAI_API_KEY:
        notify_event("LLM_DISABLED","error", kw,"setup","OPENAI_API_KEY missing")
        LOG.error("OPENAI_API_KEY missing"); return None

    client = openai_client()
    llm_cfg = conf.get("llm", {}) if isinstance(conf.get("llm"), dict) else {}
    primary = llm_cfg.get("model", LLM_DEFAULT_MODEL)
    fallbacks = llm_cfg.get("fallback_models", ["gpt-4o-mini", "gpt-4o", "o4-mini"])
    models_to_try = [m for m in [primary] + fallbacks if m]

    facts = build_facts_json(kw, items, inputs, policy)
    prompt_obj = {"system": sys_prompt(), "user": user_prompt(facts)}
    raw = ""

    for model in models_to_try:
        for attempt in range(1,3):
            try:
                resp = client.chat.completions.create(
                    model=model, temperature=float(llm_cfg.get("temperature", LLM_TEMPERATURE)),
                    max_tokens=LLM_MAXTOKENS,
                    messages=[
                        {"role":"system","content":prompt_obj["system"]},
                        {"role":"user","content":prompt_obj["user"]}
                    ]
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
                save_artifacts({**prompt_obj, "model_used": model}, raw, plan)
                if plan:
                    if model != primary:
                        notify_event("LLM_MODEL_FALLBACK","warning", kw,"llm_call",
                                     f"指定モデルからフォールバックして成功", from_model=primary, to_model=model)
                    return plan
                notify_event("LLM_PARSE_ERROR","warning", kw,"llm_call",
                             "LLM応答をJSONとして解釈できない", attempt=attempt, model=model, sample=raw[:500])
            except Exception as e:
                et = str(e)
                notify_event("LLM_CALL_FAILED","error", kw,"llm_call","OpenAI API呼び出しに失敗",
                             attempt=attempt, model=model, exception=et)
                # モデル未提供/未権限なら次のモデルへ
                if is_model_not_found(et):
                    break
                time.sleep(2)
        # 次モデルへ（primaryで404等→fallback通知は成功時に送る）
    notify_event("LLM_FAILED_FINAL","error", kw,"llm_call","全モデル試行が失敗")
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

# ---------- Keywords ----------
def kw_from_pools(conf, need:int):
    pools = (conf.get("keywords") or {}).get("pools") or {}
    nouns = pools.get("nouns") or []
    modifiers = pools.get("modifiers") or []
    specs = pools.get("specs") or []
    cands = set()
    for n in nouns:
        cands.add(n)
        for m in modifiers:
            cands.add(f"{n} {m}")
        for s in specs:
            cands.add(f"{n} {s}")
    cands = list(cands)
    random.shuffle(cands)
    return cands[:max(need, 0)]

def kw_expand_llm(conf, need:int):
    if not OPENAI_API_KEY:
        notify_event("KW_EXPAND_SKIP","warning","","kw","OPENAI_API_KEY missing")
        return []
    client = openai_client()
    themes = (conf.get("keywords") or {}).get("themes") or ["家電","キッチン","日用品","ガジェット","カー用品","子育て","アウトドア","掃除","収納","照明","文房具","防犯"]
    prompt = (
        "日本語で、購買意図が強いロングテール商品キーワードを生成してください。"
        "形式は JSON 配列（文字列のみ）。各キーワードは 12〜30 文字で、具体的な製品名＋用途やスペックを含め、誇大表現は不可。"
        f"ジャンルの例: {', '.join(themes)}。例: 'コードレス 掃除機 軽量', 'ヘアドライヤー 速乾 静音' など。"
    )
    try:
        resp = client.chat.completions.create(
            model=(conf.get("llm",{}) or {}).get("model", LLM_DEFAULT_MODEL),
            temperature=0.5, max_tokens=800,
            messages=[{"role":"system","content":"短く正確に。出力はJSON配列のみ。"},
                      {"role":"user","content":prompt}]
        )
        raw = (resp.choices[0].message.content or "").strip()
        arr = json.loads(re.search(r"\[.*\]", raw, flags=re.DOTALL).group(0)) if "[" in raw else json.loads(raw)
        arr = [sanitize_keyword(x) for x in arr if isinstance(x,str)]
        random.shuffle(arr)
        return arr[:need]
    except Exception as e:
        notify_event("KW_EXPAND_FAILED","warning","","kw","LLMでのキーワード拡張に失敗", exception=str(e))
        return []

def get_seed_list(conf, posts_per_run:int):
    kwcfg = conf.get("keywords") or {}
    mode = kwcfg.get("mode","static")
    base = kwcfg.get("seeds") or []
    need = int(kwcfg.get("max_candidates", posts_per_run*4))
    if mode == "static":
        return base
    elif mode == "pools":
        extra = kw_from_pools(conf, need=need)
        return (base or []) + extra
    elif mode == "expand_llm":
        extra = kw_expand_llm(conf, need=need)
        return (base or []) + extra
    else:
        return base

# ---------- WP create ----------
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

    # ab/rules（任意）
    try:
        conf["ab_tests"]=yaml.safe_load(open("src/ab_tests.yaml","r",encoding="utf-8")) if conf.get("content",{}).get("ab_test") else {}
    except Exception: conf["ab_tests"]={}
    try:
        rules=yaml.safe_load(open(conf["review"]["rules_file"],"r",encoding="utf-8"))
        bads=rules.get("prohibited_phrases",[])
    except Exception: bads=[]

    # config defaults
    site = conf.get("site") or {}
    category_names = site.get("category_names") or ["レビュー"]
    posts_per_run = int(site.get("posts_per_run", 1))
    internal_link = site.get("internal_link","")
    unique_slug_date = bool(site.get("unique_slug_date", True))
    disclosure = site.get("affiliate_disclosure") or "当サイトはアフィリエイト広告（Amazonアソシエイト含む）を利用しています。"
    if not site.get("affiliate_disclosure"):
        notify_event("CONFIG_DEFAULTED","warning","","setup","site.affiliate_disclosure 未設定→デフォルトを適用",
                     defaults={"affiliate_disclosure": disclosure})

    cats=ensure_categories(category_names)
    min_items=int(conf.get("content",{}).get("min_items", DEFAULT_MIN_ITEMS))

    inputs={"article_type":"比較まとめ/ランキング/用途別おすすめ",
            "notes":"医療・効果の断定表現を禁止、未確定の価格は書かない",
            "compare_axes":["耐久性","保証","カラーバリエーション","追加オプション","流通度合い","話題性"],
            "internal_link": internal_link}
    policy={"disclosure":disclosure,"cta_note":"購入判断は自己責任。価格・在庫は変動。リンク先で最終確認。"}

    if not OPENAI_API_KEY:
        notify_event("LLM_DISABLED","error","","setup","OPENAI_API_KEY missing（Run中断）")
        LOG.error("OPENAI_API_KEY missing — abort"); return

    seeds = get_seed_list(conf, posts_per_run)
    if not seeds:
        notify_event("NO_SEEDS","warning","","kw","候補キーワードが空のため、投稿なし")
        return

    posted=0
    for raw_kw in seeds:
        if posted>=posts_per_run: break
        kw=sanitize_keyword(raw_kw)
        if not kw: continue

        base_slug=slugify(kw)
        slug = unique_slug(base_slug, unique_slug_date)
        if not unique_slug_date and wp_post_exists(slug):
            LOG.info(f"skip exists (slug duplicate): {kw}")
            continue

        LOG.info(f"query kw='{kw}'")
        arr = rakuten_items(RAKUTEN_APP_ID, kw,
                            conf["data_sources"]["rakuten"]["endpoint"],
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
            continue

        body = plan["body_gutenberg"]
        if plan.get("table_gutenberg"): body += "\n" + plan["table_gutenberg"]
        if plan.get("ctas"):
            for k in ("hesitant","decider","comparer"):
                v = plan["ctas"].get(k); 
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
            notify_event("VALIDATION_FAILED","warning", kw,"validation","本文検証NG", errors=errs)
            LOG.error(f"validation failed: {errs}")
            continue

        if any(b in body for b in (bads or [])):
            notify_event("BLOCKED_BY_RULE","warning", kw,"rule","法務/NG語に該当")
            LOG.info(f"blocked by rule for '{kw}'")
            continue

        payload={"title":title,"slug":slug,"status":"publish",
                 "content":body,"categories":cats,"excerpt":excerpt}
        try:
            create_post(payload)
            posted+=1; LOG.info(f"posted: {kw}")
        except Exception as e:
            notify_event("POST_FAILED","error", kw,"post","WordPress投稿に失敗", exception=str(e))
            LOG.error(f"post failed: {e}")
            time.sleep(2)

    LOG.info(f"done, posted={posted}")

if __name__=="__main__":
    try: main()
    except Exception as e:
        notify_event("RUN_FAILED","error","","run","未捕捉の例外", exception=str(e))
        raise
