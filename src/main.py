# -*- coding: utf-8 -*-
import os, sys, json, time, math, base64, logging, traceback, textwrap
from datetime import datetime, timezone, timedelta
import requests, yaml
from slugify import slugify
from urllib.parse import urlencode

"""
gpt-5 最適化版（Responses API）
- すべてのLLM呼び出しを Responses API に寄せる
- gpt-5 で empty_completion / パラメタ不一致が出たら自動で gpt-4o → gpt-4o-mini にフォールバック
- テンプレ出力は廃止（AI失敗時は投稿しない＋Discord通知のみ）
- AFFINGER向けCTAボタン必須、Markdown表必須、最小文字数チェック
- Rakuten API: hits<=30 厳守、400時は理由をDiscordへ
- ログ: run.log、Discordは“コピペで判断できるJSON本文”を送信
"""

# --------------- ログ設定 ---------------
logging.basicConfig(
    filename='run.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
JST = timezone(timedelta(hours=9))

# --------------- 環境変数 ---------------
RAKUTEN_APP_ID   = os.getenv("RAKUTEN_APP_ID", "").strip()
WP_SITE_URL      = os.getenv("WP_SITE_URL", "").rstrip("/")
WP_USERNAME      = os.getenv("WP_USERNAME", "")
WP_APP_PASSWORD  = os.getenv("WP_APP_PASSWORD", "")
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY", "")
ALERT_WEBHOOK    = os.getenv("ALERT_WEBHOOK_URL", "")

# --------------- ヘルパ ---------------
def jst_now_iso():
    return datetime.now(JST).isoformat()

def alert(event, severity, payload):
    """DiscordにJSONを投げる。本文は見れば状況が一発でわかる形にする。"""
    if not ALERT_WEBHOOK:
        return
    base = {
        "event": event,
        "severity": severity,
        "ts_jst": jst_now_iso(),
        "ctx": {
            "repo": os.getenv("GITHUB_REPOSITORY", ""),
            "workflow": os.getenv("GITHUB_WORKFLOW", ""),
            "run_id": os.getenv("GITHUB_RUN_ID", ""),
            "run_attempt": os.getenv("GITHUB_RUN_ATTEMPT", ""),
            "branch": os.getenv("GITHUB_REF_NAME", ""),
            "sha": os.getenv("GITHUB_SHA", ""),
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
    # 必須の開示文が未設定なら警告し、デフォルト適用（投稿は継続）
    if not cfg.get("site", {}).get("affiliate_disclosure"):
        alert("CONFIG_DEFAULTED", "warning", {
            "kw": "",
            "stage": "setup",
            "reason": "site.affiliate_disclosure が未設定のためデフォルトを適用",
            "defaults": {"affiliate_disclosure": "当サイトはアフィリエイト広告（Amazonアソシエイト含む）を利用しています。"}
        })
        cfg.setdefault("site", {})["affiliate_disclosure"] = "当サイトはアフィリエイト広告（Amazonアソシエイト含む）を利用しています。"
    return cfg

# -------- WordPress --------
def wp_auth_header():
    token = base64.b64encode(f"{WP_USERNAME}:{WP_APP_PASSWORD}".encode()).decode()
    return {"Authorization": f"Basic {token}"}

def wp_check_auth():
    try:
        r = requests.get(f"{WP_SITE_URL}/wp-json/wp/v2/users/me", headers=wp_auth_header(), timeout=20)
        if r.status_code == 200:
            user = r.json().get("name","?")
            logging.info(f"wp_auth_ok: user={user}")
            return True
        else:
            alert("WP_AUTH_FAILED", "error", {
                "stage": "wp_auth",
                "reason": f"HTTP {r.status_code}",
                "resp": r.text[:400]
            })
            return False
    except Exception as e:
        alert("WP_AUTH_FAILED", "error", {
            "stage": "wp_auth",
            "exception": repr(e)
        })
        return False

def wp_slug_exists(slug):
    try:
        r = requests.get(f"{WP_SITE_URL}/wp-json/wp/v2/posts?slug={slug}", headers=wp_auth_header(), timeout=25)
        if r.status_code == 200:
            arr = r.json()
            return len(arr) > 0
        return False
    except Exception:
        return False

def wp_publish(title, content_md, slug, status="publish", categories=None, tags=None):
    data = {
        "title": title,
        "content": content_md,
        "slug": slug,
        "status": status
    }
    if categories: data["categories"] = categories
    if tags: data["tags"] = tags
    try:
        r = requests.post(f"{WP_SITE_URL}/wp-json/wp/v2/posts", headers={
            **wp_auth_header(),
            "Content-Type": "application/json"
        }, data=json.dumps(data), timeout=40)
        if r.status_code in (200,201):
            return True, r.json()
        else:
            logging.error(f"http_error: POST .../posts -> {r.status_code} {r.text[:400]}")
            alert("WP_POST_FAILED", "error", {
                "stage": "publish",
                "reason": f"HTTP {r.status_code}",
                "resp": r.text[:400]
            })
            return False, r.text
    except Exception as e:
        alert("WP_POST_FAILED", "error", {
            "stage": "publish",
            "exception": repr(e)
        })
        return False, repr(e)

# -------- Rakuten API --------
RAKUTEN_ENDPOINT = "https://app.rakuten.co.jp/services/api/IchibaItem/Search/20220601"

def rakuten_items(keyword, hits=30):
    params = {
        "applicationId": RAKUTEN_APP_ID,
        "keyword": keyword,
        "hits": min(int(hits), 30),   # 30超は400
        "page": 1,
        "format": "json",
        "availability": 1,
        "sort": "-reviewCount"
    }
    try:
        r = requests.get(RAKUTEN_ENDPOINT, params=params, timeout=30)
        if r.status_code != 200:
            alert("RAKUTEN_API_ERROR", "error", {
                "kw": keyword,
                "stage": "fetch",
                "reason": f"HTTP {r.status_code}",
                "resp": r.text[:400]
            })
            return []
        j = r.json()
        items = j.get("Items", [])
        res = []
        for it in items:
            i = it.get("Item", {})
            res.append({
                "name": i.get("itemName",""),
                "url": i.get("itemUrl",""),
                "price": i.get("itemPrice", 0),
                "shop": i.get("shopName",""),
                "review_count": i.get("reviewCount",0),
                "review_avg": i.get("reviewAverage",0),
                "caption": i.get("itemCaption","")[:200]
            })
        return res
    except Exception as e:
        alert("RAKUTEN_API_ERROR", "error", {
            "kw": keyword,
            "stage": "fetch",
            "exception": repr(e)
        })
        return []

def filter_items(items, cfg):
    min_rev = cfg["rakuten"].get("min_review_count", 10)
    min_price = cfg["rakuten"].get("min_price", 0)
    max_price = cfg["rakuten"].get("max_price", 10**9)
    out = [x for x in items if x["review_count"] >= min_rev and min_price <= x["price"] <= max_price]
    return out

# -------- OpenAI（Responses API + フォールバック） --------
class LLMClient:
    def __init__(self, cfg):
        self.model_primary = cfg["llm"].get("model","gpt-4o")
        self.fallback_models = cfg["llm"].get("fallback_models", ["gpt-4o", "gpt-4o-mini"])
        self.temperature = float(cfg["llm"].get("temperature", 0.4))
        self.max_output_tokens = int(cfg["llm"].get("max_output_tokens", 6000))
        self.timeout = int(cfg["llm"].get("timeout_sec", 120))
        if not OPENAI_API_KEY:
            alert("LLM_NOT_CONFIGURED", "error", {
                "stage": "setup",
                "reason": "OPENAI_API_KEY 未設定"
            })
            raise RuntimeError("OPENAI_API_KEY missing")
        self.base_url = "https://api.openai.com/v1"

    def _responses_call(self, prompt, model):
        url = f"{self.base_url}/responses"
        headers = {
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json"
        }
        body = {
            "model": model,
            "input": prompt,
            "temperature": self.temperature,
            "max_output_tokens": self.max_output_tokens,
            "response_format": {"type": "text"}
        }
        r = requests.post(url, headers=headers, data=json.dumps(body), timeout=self.timeout)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code} - {r.text[:400]}")
        data = r.json()
        # 安全にテキスト抽出（output_text優先、なければ掘る）
        text = data.get("output_text")
        if not text:
            # fallback extraction
            try:
                outs = data.get("output", [])
                chunks = []
                for out in outs:
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
        # primary → fallbacks
        tried = []
        for model in [self.model_primary] + self.fallback_models:
            try:
                txt = self._responses_call(prompt, model)
                if model != self.model_primary:
                    alert("LLM_MODEL_FALLBACK", "warning", {
                        "from_model": self.model_primary,
                        "to_model": model
                    })
                return txt, model
            except Exception as e:
                tried.append({"model": model, "exception": repr(e)})
                alert("LLM_CALL_FAILED", "error", {
                    "stage": "llm_call",
                    "model": model,
                    "exception": repr(e)
                })
                time.sleep(1.2)
        # 全滅
        raise RuntimeError(f"LLM failed: {tried}")

# -------- コンテンツ生成 --------
PROMPT_SPEC = """あなたは「一次情報最優先・法令順守のアフィリエイト記事ライター兼編集者」です。
日本語で、H2中心の構成・正確性重視・不必要に煽らないトーンで執筆してください。

【目的】
検索流入と指名流入の両方で読者の意思決定を助け、適切なCTAで離脱せずに比較→選択→購入へ導く記事を作る。

【読者像】
- 読者タイプ: 初心者
- 主要ニーズ: コスパが良く失敗しにくいものだけ知りたい
- シチュエーション: 一人暮らし/家族

【入力（供給データ）】
- 記事タイプ: 比較まとめ/ランキング/用途別おすすめ
- 注意点/除外: 医療・効果の断定表現を禁止、未確定の価格は書かない
- 競合比較の軸（参考）: 耐久性/保証/カラバリ/追加オプション/流通度合い/話題性
- 内部リンク予定: 省略可

【厳守事項】
1) 事実は一次情報（正規販売ページ=楽天の該当商品ページ、メーカー公式）に依拠。本文末に脚注でURL列挙。捏造しない。
2) 価格・在庫・キャンペーンは変動前提。「執筆時点」表現と注意書きを必ず入れる。最安や断定表現は禁止。
3) 比較は公正・具体。欠点も必ず記載。誇大・医療/効果効能の断定・体験の一般化をしない。
4) 冒頭直後に開示文：『当サイトはアフィリエイト広告（Amazonアソシエイト含む）を利用しています。』
5) H2中心、段落短め、箇条書き多め。逆三角形（結論→理由→具体）。1文は短く。Markdownで。
6) 必須: Markdownの比較表（|を使う）。CTAブロック3種（迷っている/即決/さらに比較）。AFFINGERボタン短コードを使う。
   例: [st-mybutton url="{URL}" title="楽天で価格を見る" rel="nofollow" target="_blank" class="stmybtn st-color"]楽天で価格を見る[/st-mybutton]

【出力仕様】
- 1500〜3000字。H2/H3を適切に。
- 「文字以外の要素」を一定間隔で（表・箇条書き・引用など）。
- 本文末に「脚注:」として参照URLを列挙。

【対象カテゴリ/キーワード】
{KW}

【比較候補（楽天公式/正規販売ページが優先）】
以下の候補を事実の根拠として使い、名称・参考価格（変動注意）・レビュー傾向を要約。URLは脚注にも列挙。
{ITEMS}

【サイト設定】
開示文: {DISCLOSURE}

以上を満たす記事本文（Markdownのみ出力。余計な前置きや説明は書かない）を生成してください。
"""

def build_items_block(items):
    lines = []
    for i, it in enumerate(items, 1):
        lines.append(f"- {i}. {it['name']} | 参考価格: {it['price']}円（変動あり） | レビュー: {it['review_avg']} / {it['review_count']}件 | 販売: {it['shop']}\n  URL: {it['url']}")
    return "\n".join(lines)

def validate_markdown(md, cfg):
    min_chars = int(cfg["content"].get("min_chars", 2000))
    need_tables = int(cfg["content"].get("require_tables", 1))
    need_buttons = int(cfg["content"].get("require_buttons", 3))

    text = md.strip()
    if len(text) < min_chars:
        return False, f"too_short:{len(text)}"
    # 見出し
    if "## " not in text:
        return False, "missing_h2"
    # 表（|記号をざっくり検査）
    if text.count("|") < (need_tables * 8):  # 1表=最低8個ぐらいの'|'を想定
        return False, "missing_table"
    # AFFINGERボタン
    if text.count("[st-mybutton") < need_buttons:
        return False, "few_buttons"
    return True, "ok"

def make_slug(title):
    # 日本語タイトル想定→ローマ字風slug化。重複回避は呼び出し側で。
    s = slugify(title, lowercase=True, allow_unicode=False)
    return s[:80] if len(s)>80 else s

def extract_title(md):
    # 最初の行 or 最初の見出しをタイトルに採用
    for line in md.splitlines():
        t = line.strip("# ").strip()
        if t:
            return t[:80]
    return "auto-generated-article"

def kw_expand(llm: LLMClient, seeds, how_many=8):
    prompt = f"""以下の日本語シードから、家電・デジタル・日用品・生活・育児など**異なるジャンル**に広げて検索意図が明確なキーワードを {how_many} 件、JSON配列で出力して。名詞＋意図（例: 「65W USB充電器 比較」「電動歯ブラシ コスパ 用途別」）。重複や近すぎる語は避ける。
シード: {json.dumps(seeds, ensure_ascii=False)}"""
    try:
        txt, used_model = llm.complete(prompt)
        # JSONの可能性/プレーンの可能性どちらもケア
        arr = []
        try:
            arr = json.loads(txt)
            if not isinstance(arr, list):
                arr = []
        except Exception:
            # 改行区切りfallback
            arr = [x.strip("・- ") for x in txt.splitlines() if x.strip()]
        # 正規化
        out = []
        for x in arr:
            if isinstance(x, str) and 3 <= len(x) <= 30:
                out.append(x)
        if not out:
            raise ValueError("kw_empty")
        return out[:how_many]
    except Exception as e:
        alert("KW_EXPAND_FAILED", "warning", {
            "kw": "",
            "stage": "kw",
            "reason": "LLMでのキーワード拡張に失敗",
        })
        # 失敗時はシードをそのまま使う（数は制限）
        return seeds[:how_many]

def generate_article(llm: LLMClient, kw, items, cfg):
    prompt = PROMPT_SPEC.format(
        KW=kw,
        ITEMS=build_items_block(items),
        DISCLOSURE=cfg["site"]["affiliate_disclosure"]
    )
    md, used_model = llm.complete(prompt)
    return md, used_model

# --------------- メイン ---------------
def main():
    if not (RAKUTEN_APP_ID and WP_SITE_URL and WP_USERNAME and WP_APP_PASSWORD and OPENAI_API_KEY):
        alert("ENV_MISSING", "error", {
            "stage": "setup",
            "reason": "必要な環境変数不足（RAKUTEN/WORDPRESS/OPENAI/ALERT）"
        })
        raise SystemExit(2)

    cfg_path = sys.argv[sys.argv.index("--config")+1] if "--config" in sys.argv else "config/app.yaml"
    cfg = load_config(cfg_path)

    if not wp_check_auth():
        raise SystemExit(3)

    llm = LLMClient(cfg)

    # キーワード決定
    seeds = cfg["keywords"].get("seeds", [])
    per_run = int(cfg["keywords"].get("per_run", 6))
    expanded = kw_expand(llm, seeds, how_many=per_run)

    posted_count = 0
    for kw in expanded:
        logging.info(f"query kw='{kw}'")
        items = rakuten_items(kw, hits=cfg["rakuten"].get("hits", 30))
        items_f = filter_items(items, cfg)
        logging.info(f"stats kw='{kw}': total={len(items)}, after_filters={len(items_f)}")
        if len(items_f) < int(cfg["rakuten"].get("min_items_after_filters", 3)):
            logging.info(f"skip thin (<{cfg['rakuten'].get('min_items_after_filters',3)}) for '{kw}'")
            continue

        # 生成
        try:
            md, used_model = generate_article(llm, kw, items_f[:6], cfg)
        except Exception as e:
            alert("LLM_FAILED_FINAL", "error", {"kw": kw, "stage": "llm_call", "reason": "再試行の結果も失敗"})
            continue

        ok, why = validate_markdown(md, cfg)
        if not ok:
            alert("VALIDATION_FAILED", "warning", {"kw": kw, "stage": "validation", "reason": "本文検証NG", "errors": [why]})
            continue

        # タイトル・スラッグ
        title = extract_title(md)
        slug = make_slug(title)
        if wp_slug_exists(slug):
            logging.info(f"skip exists (slug duplicate): {title}")
            continue

        # 投稿
        status = "publish" if not cfg.get("debug", {}).get("draft_mode", False) else "draft"
        okp, resp = wp_publish(title, md, slug, status=status,
                               categories=cfg.get("wp", {}).get("category_ids"),
                               tags=cfg.get("wp", {}).get("tag_ids"))
        if okp:
            posted_count += 1
        else:
            # 失敗は alert済み
            pass

    logging.info(f"done, posted={posted_count}")

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        logging.exception("fatal")
        alert("RUN_FAILED", "error", {
            "kw": "",
            "stage": "run",
            "reason": "未捕捉の例外",
            "exception": repr(e)
        })
        raise
