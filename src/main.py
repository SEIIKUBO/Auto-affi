# -*- coding: utf-8 -*-
import os, sys, json, time, logging, base64, textwrap, traceback
from datetime import datetime, timezone, timedelta
import requests, yaml
from slugify import slugify

"""
gpt-5 最適化（Responses API）
- response_format は使わない（400回避）
- text.format も送らない（型不一致400回避）。Markdownはプロンプトで強制。
- gpt-5 → gpt-4o → gpt-4o-mini に自動フォールバック
- 失敗時は投稿せず Discord に整形JSONで通知
- Rakuten hits<=30 厳守、400はDiscord通知
- AFFINGERボタン・表・最小文字数のバリデーション
- プロンプト/出力をデバッグ保存（llm_prompt.json / llm_output.txt）
"""

# ---------------- 基本設定 ----------------
logging.basicConfig(filename='run.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
JST = timezone(timedelta(hours=9))

RAKUTEN_APP_ID  = os.getenv("RAKUTEN_APP_ID", "").strip()
WP_SITE_URL     = os.getenv("WP_SITE_URL", "").rstrip("/")
WP_USERNAME     = os.getenv("WP_USERNAME", "")
WP_APP_PASSWORD = os.getenv("WP_APP_PASSWORD", "")
OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY", "")
ALERT_WEBHOOK   = os.getenv("ALERT_WEBHOOK_URL", "")

def jst_now_iso(): return datetime.now(JST).isoformat()

def alert(event, severity, payload):
    if not ALERT_WEBHOOK: return
    base = {
        "event": event, "severity": severity, "ts_jst": jst_now_iso(),
        "ctx": {
            "repo": os.getenv("GITHUB_REPOSITORY",""),
            "workflow": os.getenv("GITHUB_WORKFLOW",""),
            "run_id": os.getenv("GITHUB_RUN_ID",""),
            "run_attempt": os.getenv("GITHUB_RUN_ATTEMPT",""),
            "branch": os.getenv("GITHUB_REF_NAME",""),
            "sha": os.getenv("GITHUB_SHA",""),
            "run_url": f"https://github.com/{os.getenv('GITHUB_REPOSITORY','')}/actions/runs/{os.getenv('GITHUB_RUN_ID','')}"
        }
    }
    base.update(payload or {})
    try:
        title = f"[AUTO-REV][{severity.upper()}] EVENT={event}"
        content = title + "\n" + "```json\n" + json.dumps(base, ensure_ascii=False, indent=2) + "\n```"
        requests.post(ALERT_WEBHOOK, json={"content": content}, timeout=15)
    except Exception:
        logging.exception("alert_failed")

def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not cfg.get("site", {}).get("affiliate_disclosure"):
        alert("CONFIG_DEFAULTED", "warning", {
            "kw":"", "stage":"setup",
            "reason":"site.affiliate_disclosure が未設定のためデフォルトを適用",
            "defaults":{"affiliate_disclosure":"当サイトはアフィリエイト広告（Amazonアソシエイト含む）を利用しています。"}
        })
        cfg.setdefault("site", {})["affiliate_disclosure"] = "当サイトはアフィリエイト広告（Amazonアソシエイト含む）を利用しています。"
    return cfg

# ---------------- WordPress ----------------
def wp_auth_header():
    token = base64.b64encode(f"{WP_USERNAME}:{WP_APP_PASSWORD}".encode()).decode()
    return {"Authorization": f"Basic {token}"}

def wp_check_auth():
    try:
        r = requests.get(f"{WP_SITE_URL}/wp-json/wp/v2/users/me", headers=wp_auth_header(), timeout=20)
        if r.status_code == 200:
            logging.info(f"wp_auth_ok: user={r.json().get('name','?')}")
            return True
        alert("WP_AUTH_FAILED","error",{"stage":"wp_auth","reason":f"HTTP {r.status_code}","resp":r.text[:400]})
    except Exception as e:
        alert("WP_AUTH_FAILED","error",{"stage":"wp_auth","exception":repr(e)})
    return False

def wp_slug_exists(slug):
    try:
        r = requests.get(f"{WP_SITE_URL}/wp-json/wp/v2/posts?slug={slug}", headers=wp_auth_header(), timeout=25)
        return r.status_code==200 and len(r.json())>0
    except Exception:
        return False

def wp_publish(title, content_md, slug, status="publish", categories=None, tags=None):
    data = {"title": title, "content": content_md, "slug": slug, "status": status}
    if categories: data["categories"] = categories
    if tags: data["tags"] = tags
    try:
        r = requests.post(f"{WP_SITE_URL}/wp-json/wp/v2/posts",
                          headers={**wp_auth_header(),"Content-Type":"application/json"},
                          data=json.dumps(data), timeout=40)
        if r.status_code in (200,201): return True, r.json()
        logging.error(f"http_error: POST /posts -> {r.status_code} {r.text[:400]}")
        alert("WP_POST_FAILED","error",{"stage":"publish","reason":f"HTTP {r.status_code}","resp":r.text[:400]})
        return False, r.text
    except Exception as e:
        alert("WP_POST_FAILED","error",{"stage":"publish","exception":repr(e)})
        return False, repr(e)

# ---------------- Rakuten ----------------
RAKUTEN_ENDPOINT = "https://app.rakuten.co.jp/services/api/IchibaItem/Search/20220601"

def rakuten_items(keyword, hits=30):
    params = {
        "applicationId": RAKUTEN_APP_ID, "keyword": keyword,
        "hits": min(int(hits), 30), "page": 1, "format": "json",
        "availability": 1, "sort": "-reviewCount"
    }
    try:
        r = requests.get(RAKUTEN_ENDPOINT, params=params, timeout=30)
        if r.status_code != 200:
            alert("RAKUTEN_API_ERROR","error",{
                "kw":keyword,"stage":"fetch","reason":f"HTTP {r.status_code}","resp":r.text[:400]
            })
            return []
        j = r.json()
        out=[]
        for it in j.get("Items", []):
            i = it.get("Item", {})
            out.append({
                "name": i.get("itemName",""),
                "url": i.get("itemUrl",""),
                "price": i.get("itemPrice",0),
                "shop": i.get("shopName",""),
                "review_count": i.get("reviewCount",0),
                "review_avg": i.get("reviewAverage",0),
                "caption": i.get("itemCaption","")[:200]
            })
        return out
    except Exception as e:
        alert("RAKUTEN_API_ERROR","error",{"kw":keyword,"stage":"fetch","exception":repr(e)})
        return []

def filter_items(items, cfg):
    min_rev = cfg["rakuten"].get("min_review_count", 10)
    min_price = cfg["rakuten"].get("min_price", 0)
    max_price = cfg["rakuten"].get("max_price", 10**9)
    return [x for x in items if x["review_count"]>=min_rev and min_price<=x["price"]<=max_price]

# ---------------- OpenAI（Responses API） ----------------
class LLMClient:
    def __init__(self, cfg):
        self.model_primary = cfg["llm"].get("model","gpt-4o")
        self.fallback_models = cfg["llm"].get("fallback_models", ["gpt-4o","gpt-4o-mini"])
        self.temperature = float(cfg["llm"].get("temperature", 0.35))
        self.max_output_tokens = int(cfg["llm"].get("max_output_tokens", 6000))
        self.timeout = int(cfg["llm"].get("timeout_sec", 120))
        if not OPENAI_API_KEY:
            alert("LLM_NOT_CONFIGURED","error",{"stage":"setup","reason":"OPENAI_API_KEY 未設定"})
            raise RuntimeError("OPENAI_API_KEY missing")
        self.base_url = "https://api.openai.com/v1"

    def _responses_call(self, prompt, model):
        url = f"{self.base_url}/responses"
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
        # NOTE: text.format や response_format は送らない（400回避）
        body = {
            "model": model,
            "input": prompt,
            "temperature": self.temperature,
            "max_output_tokens": self.max_output_tokens
        }
        r = requests.post(url, headers=headers, data=json.dumps(body), timeout=self.timeout)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code} - {r.text}")
        data = r.json()
        text = data.get("output_text")
        if not text:
            try:
                chunks=[]
                for out in data.get("output", []):
                    for c in out.get("content", []):
                        if c.get("type") in ("output_text","text"):
                            chunks.append(c.get("text",""))
                text = "\n".join(chunks).strip()
            except Exception:
                text = ""
        if not text.strip():
            raise ValueError("empty_completion")
        return text

    def complete(self, prompt):
        tried=[]
        for model in [self.model_primary] + self.fallback_models:
            try:
                txt = self._responses_call(prompt, model)
                if model != self.model_primary:
                    alert("LLM_MODEL_FALLBACK","warning",{"from_model":self.model_primary,"to_model":model})
                return txt, model
            except Exception as e:
                tried.append({"model":model,"exception":repr(e)})
                alert("LLM_CALL_FAILED","error",{"stage":"llm_call","model":model,"exception":repr(e)})
                time.sleep(1.0)
        raise RuntimeError(f"LLM failed: {tried}")

# ---------------- 生成プロンプト ----------------
PROMPT_SPEC = """あなたは「一次情報最優先・法令順守のアフィリエイト記事ライター兼編集者」です。
日本語で、H2中心・短文・逆三角形・**Markdown**で執筆。

【目的】検索/指名流入の意思決定を助け、比較→選択→購入に導く。
【守る】一次情報リンク/価格変動注意/誇大NG/開示文必須/H2中心/表とCTA必須/AFFINGERボタン使用。
【CTA例】[st-mybutton url="{URL}" title="楽天で価格を見る" rel="nofollow" target="_blank" class="stmybtn st-color"]楽天で価格を見る[/st-mybutton]

【対象キーワード】{KW}

【比較候補（一次情報URL=楽天/公式）】
{ITEMS}

【サイト設定】開示文: {DISCLOSURE}

出力要件：
- 1500〜3000字。結論→理由→具体。箇条書き多め。H2/H3適切。
- Markdownの比較表（| を使う）を必ず1つ以上。
- AFFINGERボタンを最低3つ以上（各候補の下に）。
- 本文末に脚注として引用URLを列挙。
- 余計な前置きは不要、本文のみ。
"""

def build_items_block(items):
    lines=[]
    for i, it in enumerate(items, 1):
        lines.append(f"- {i}. {it['name']} | 参考価格: {it['price']}円（変動あり） | レビュー: {it['review_avg']} / {it['review_count']}件 | 販売: {it['shop']}\n  URL: {it['url']}")
    return "\n".join(lines)

def validate_markdown(md, cfg):
    min_chars = int(cfg["content"].get("min_chars", 2000))
    need_tables = int(cfg["content"].get("require_tables", 1))
    need_buttons = int(cfg["content"].get("require_buttons", 3))
    t = md.strip()
    if len(t) < min_chars: return False, f"too_short:{len(t)}"
    if "## " not in t: return False, "missing_h2"
    if t.count("|") < (need_tables * 8): return False, "missing_table"
    if t.count("[st-mybutton") < need_buttons: return False, "few_buttons"
    return True, "ok"

def make_slug(title):
    s = slugify(title, lowercase=True, allow_unicode=False)
    return s[:80] if len(s)>80 else s

def extract_title(md):
    for line in md.splitlines():
        t = line.strip("# ").strip()
        if t: return t[:80]
    return "auto-generated-article"

def save_debug_prompt(kw, prompt):
    rec = {"kw": kw, "prompt": prompt}
    with open("llm_prompt.json","a",encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def save_debug_output(kw, model, text):
    with open("llm_output.txt","a",encoding="utf-8") as f:
        f.write(f"\n===== {kw} | model={model} =====\n")
        f.write(text+"\n")

def kw_expand(llm: LLMClient, seeds, how_many=8):
    prompt = f"""以下の日本語シードから、家電/デジタル/日用品/生活/育児など**異なるジャンル**に広げて検索意図が明確なキーワードを {how_many} 件、JSON配列だけで出力して。
例: ["65W USB充電器 比較","電動歯ブラシ コスパ 用途別","ロボット掃除機 静音 小型"]
シード: {json.dumps(seeds, ensure_ascii=False)}"""
    try:
        txt, used_model = llm.complete(prompt)
        arr=[]
        try:
            arr = json.loads(txt)
            if not isinstance(arr, list): arr=[]
        except Exception:
            arr = [x.strip("・- ") for x in txt.splitlines() if x.strip()]
        out = [x for x in arr if isinstance(x,str) and 3<=len(x)<=30]
        if not out: raise ValueError("kw_empty")
        return out[:how_many]
    except Exception:
        alert("KW_EXPAND_FAILED","warning",{"kw":"","stage":"kw","reason":"LLMでのキーワード拡張に失敗"})
        return seeds[:how_many]

def generate_article(llm: LLMClient, kw, items, cfg):
    prompt = PROMPT_SPEC.format(KW=kw, ITEMS=build_items_block(items), DISCLOSURE=cfg["site"]["affiliate_disclosure"])
    save_debug_prompt(kw, prompt)
    md, used_model = llm.complete(prompt)
    save_debug_output(kw, used_model, md)
    return md, used_model

# ---------------- メイン ----------------
def main():
    if not (RAKUTEN_APP_ID and WP_SITE_URL and WP_USERNAME and WP_APP_PASSWORD and OPENAI_API_KEY):
        alert("ENV_MISSING","error",{"stage":"setup","reason":"必要な環境変数不足（RAKUTEN/WORDPRESS/OPENAI/ALERT）"})
        raise SystemExit(2)

    cfg_path = sys.argv[sys.argv.index("--config")+1] if "--config" in sys.argv else "config/app.yaml"
    cfg = load_config(cfg_path)

    if not wp_check_auth(): raise SystemExit(3)

    llm = LLMClient(cfg)

    seeds   = cfg["keywords"].get("seeds", [])
    per_run = int(cfg["keywords"].get("per_run", 6))
    expanded = kw_expand(llm, seeds, how_many=per_run)

    posted = 0
    for kw in expanded:
        logging.info(f"query kw='{kw}'")
        items = rakuten_items(kw, hits=cfg["rakuten"].get("hits", 30))
        items_f = filter_items(items, cfg)
        logging.info(f"stats kw='{kw}': total={len(items)}, after_filters={len(items_f)}")
        if len(items_f) < int(cfg["rakuten"].get("min_items_after_filters", 3)):
            logging.info(f"skip thin (<{cfg['rakuten'].get('min_items_after_filters',3)}) for '{kw}'")
            continue

        try:
            md, used_model = generate_article(llm, kw, items_f[:6], cfg)
        except Exception:
            alert("LLM_FAILED_FINAL","error",{"kw":kw,"stage":"llm_call","reason":"再試行の結果も失敗"})
            continue

        ok, why = validate_markdown(md, cfg)
        if not ok:
            alert("VALIDATION_FAILED","warning",{"kw":kw,"stage":"validation","reason":"本文検証NG","errors":[why]})
            continue

        title = extract_title(md)
        slug  = make_slug(title)
        if wp_slug_exists(slug):
            logging.info(f"skip exists (slug duplicate): {title}")
            continue

        status = "publish" if not cfg.get("debug",{}).get("draft_mode", False) else "draft"
        okp, resp = wp_publish(title, md, slug, status=status,
                               categories=cfg.get("wp",{}).get("category_ids"),
                               tags=cfg.get("wp",{}).get("tag_ids"))
        if okp: posted += 1

    logging.info(f"done, posted={posted}")

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        logging.exception("fatal")
        alert("RUN_FAILED","error",{"kw":"","stage":"run","reason":"未捕捉の例外","exception":repr(e)})
        raise
