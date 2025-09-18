#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, json, time, re, logging, math
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

import requests
import yaml
from slugify import slugify

# -----------------------------
# ロギング
# -----------------------------
LOG_FILE = "run.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

def jst_now_iso():
    return datetime.now(timezone(timedelta(hours=9))).isoformat()

# -----------------------------
# 通知（Discord）
# -----------------------------
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "")

def alert(event: str, severity: str, payload: Dict[str, Any]):
    """DiscordへJSONで通知。人間が読める1行＋JSON詳細を送る。"""
    if not ALERT_WEBHOOK_URL:
        return
    safe = {
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
            "run_url": f"https://github.com/{os.getenv('GITHUB_REPOSITORY','')}/actions/runs/{os.getenv('GITHUB_RUN_ID','')}",
        },
    }
    safe.update(payload or {})
    head = f"[AUTO-REV][{severity.upper()}] EVENT={event} KW=\"{safe.get('kw','')}\""
    try:
        requests.post(ALERT_WEBHOOK_URL, json={"content": head + "\n```json\n" + json.dumps(safe, ensure_ascii=False, indent=2) + "\n```"}, timeout=15)
    except Exception as e:
        logging.error(f"alert_failed: {e}")

# -----------------------------
# 設定ロード
# -----------------------------
def load_config(path="config/app.yaml")->Dict[str,Any]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # 足りないキーのデフォルト
    cfg.setdefault("site", {}).setdefault("posts_per_run", 1)
    cfg["site"].setdefault("affiliate_disclosure", "当サイトはアフィリエイト広告（Amazonアソシエイト含む）を利用しています。")
    cfg.setdefault("llm", {}).setdefault("model", "gpt-4o-mini")
    cfg["llm"].setdefault("fallback_models", ["gpt-4o", "gpt-4o-mini"])
    cfg["llm"].setdefault("temperature", 0.4)
    cfg.setdefault("content", {}).setdefault("min_items", 3)
    cfg["content"].setdefault("price_floor", 2000)
    cfg["content"].setdefault("review_floor", 3.8)
    cfg["content"].setdefault("min_chars", 1800)  # 検証下限（文字）
    cfg.setdefault("keywords", {}).setdefault("mode", "expand_llm")
    cfg["keywords"].setdefault("max_candidates", 30)
    cfg.setdefault("data_sources", {}).setdefault("rakuten", {"endpoint": "https://app.rakuten.co.jp/services/api/IchibaItem/Search/20220601","max_per_seed":60})
    return cfg

CONFIG = load_config()

# -----------------------------
# OpenAI 呼び出し（Chat Completions）
# -----------------------------
from openai import OpenAI
_openai = OpenAI()

def _is_reasoning_like(model:str)->bool:
    m = model.lower()
    return m.startswith("gpt-5") or m.startswith("o")  # gpt-5 / o* 系は max_completion_tokens を要求しやすい

def llm_chat(model:str, messages:List[Dict[str,str]], purpose:str, kw:str, max_tokens:int=2200, temperature:float=0.4)->Optional[str]:
    """
    gpt-5系では max_tokens 非対応 → max_completion_tokens を使う。
    さらに 'temperature' 非対応ケースでは自動で外して再試行。
    """
    def _try_call(mdl:str, allow_temp:bool)->str:
        kwargs: Dict[str,Any] = dict(model=mdl, messages=messages)
        if _is_reasoning_like(mdl):
            kwargs["max_completion_tokens"] = max_tokens
            if allow_temp and not mdl.lower().startswith("gpt-5"):  # gpt-5 は temperature 非対応の報告があるためデフォ外す
                kwargs["temperature"] = temperature
        else:
            kwargs["max_tokens"] = max_tokens
            kwargs["temperature"] = temperature

        return _openai.chat.completions.create(**kwargs).choices[0].message.content

    try:
        try:
            return _try_call(model, allow_temp=True)
        except Exception as e1:
            em = str(e1)
            # max_tokens -> max_completion_tokens 指摘 or temperature 非対応をハンドリング
            if "max_tokens" in em and "max_completion_tokens" in em:
                try:
                    return _try_call(model, allow_temp=True)  # 上で既に切替済み。再掲してもOK
                except Exception as e2:
                    raise e2
            if "Unsupported parameter" in em and "temperature" in em:
                try:
                    return _try_call(model, allow_temp=False)
                except Exception as e3:
                    raise e3
            raise e1
    except Exception as e:
        alert("LLM_CALL_FAILED", "error", {
            "kw": kw, "stage": purpose, "model": model, "exception": f"{type(e).__name__}: {e}"
        })
        # フォールバック
        for fb in CONFIG["llm"].get("fallback_models", []):
            try:
                alert("LLM_MODEL_FALLBACK", "warning", {"kw": kw, "from_model": model, "to_model": fb})
                return llm_chat(fb, messages, purpose, kw, max_tokens=max_tokens, temperature=temperature)
            except Exception:
                continue
        alert("LLM_FAILED_FINAL", "error", {"kw": kw, "stage": purpose})
        return None

# -----------------------------
# 楽天API
# -----------------------------
RAKUTEN_APP_ID = os.getenv("RAKUTEN_APP_ID", "")

def rakuten_items(keyword:str, max_total:int=60)->List[Dict[str,Any]]:
    """hits<=30 の制限を守りながらページング"""
    url = CONFIG["data_sources"]["rakuten"]["endpoint"]
    collected = []
    page = 1
    while len(collected) < max_total and page <= 3:
        hits = min(30, max_total - len(collected))
        params = {
            "applicationId": RAKUTEN_APP_ID,
            "keyword": keyword,
            "hits": hits,
            "page": page,
            "formatVersion": 2,
            "sort": "-reviewCount",
        }
        r = requests.get(url, params=params, timeout=25)
        if r.status_code == 400:
            alert("RAKUTEN_API_ERROR", "error", {"kw": keyword, "stage":"fetch", "reason": r.text})
            break
        r.raise_for_status()
        data = r.json()
        items = data.get("Items", [])
        for it in items:
            collected.append(it)
        if len(items) < hits:
            break
        page += 1
    return collected

def filter_items(items:List[Dict[str,Any]])->List[Dict[str,Any]]:
    pf = CONFIG["content"]["price_floor"]
    rf = CONFIG["content"]["review_floor"]
    ok = []
    for it in items:
        try:
            price = int(it.get("itemPrice", 0))
            ra = float(it.get("reviewAverage", 0) or 0)
            if price >= pf and ra >= rf:
                ok.append(it)
        except Exception:
            continue
    return ok

# -----------------------------
# 検証
# -----------------------------
def has_table(md:str)->bool:
    # Markdown表ヘッダ（製品名…）があるか
    return bool(re.search(r"^\|?\s*製品名\s*\|", md, flags=re.MULTILINE))

def count_cta(md:str)->int:
    # 「購入はこちら」「公式サイト」「比較表を見る」などのリンクをCTAとしてカウント
    return len(re.findall(r"\[(?:購入はこちら|公式サイト|比較表を見る|最安をチェック|楽天で見る)\]\([^)]+\)", md))

def validate(md:str)->List[str]:
    errs = []
    if len(md) < CONFIG["content"]["min_chars"]:
        errs.append(f"too_short:{len(md)}")
    if not has_table(md):
        errs.append("missing_table")
    if count_cta(md) < 3:
        errs.append("few_buttons")
    return errs

# -----------------------------
# キーワード拡張
# -----------------------------
def expand_keywords()->List[str]:
    mode = CONFIG.get("keywords", {}).get("mode","seeds")
    seeds: List[str] = CONFIG.get("keywords", {}).get("seeds", []) or []
    if mode != "expand_llm" or not seeds:
        return seeds

    pools = CONFIG["keywords"].get("pools", {})
    nouns = pools.get("nouns", [])
    modifiers = pools.get("modifiers", [])
    specs = pools.get("specs", [])
    max_cand = CONFIG["keywords"].get("max_candidates", 30)

    system = {
        "role":"system",
        "content":"あなたは日本語で検索意図に沿ったロングテール商品キーワードを作るアナリストです。重複・不自然な組合せを避け、購買に近い語を優先します。"
    }
    user = {
        "role":"user",
        "content":(
            "以下の要素から多ジャンルのキーワード候補を日本語で生成してください。\n"
            f"- シード: {', '.join(seeds)}\n"
            f"- 名詞群: {', '.join(nouns)}\n"
            f"- 修飾語: {', '.join(modifiers)}\n"
            f"- 仕様語: {', '.join(specs)}\n"
            f"- 形式: 箇条書きで{max_cand}個まで。1行1キーワード。一般名詞+具体語（例:『USB充電器 65W 急速』）\n"
            "- NG: 医療効能/誇大/不明確なメーカー名"
        )
    }
    model = CONFIG["llm"]["model"]
    text = llm_chat(model, [system, user], "kw", kw="", max_tokens=600, temperature=0.2)
    if not text:
        alert("KW_EXPAND_FAILED", "warning", {"kw":"", "stage":"kw", "reason":"LLMでのキーワード拡張に失敗"})
        return seeds
    cands = []
    for line in text.splitlines():
        s = line.strip("-•* \t")
        if not s: continue
        if len(s) > 40: continue
        cands.append(s)
    # 先頭にseed優先
    uniq = []
    for x in seeds + cands:
        if x not in uniq:
            uniq.append(x)
    return uniq[:max_cand]

# -----------------------------
# 本文プロンプト
# -----------------------------
BASE_SPEC = """
あなたは「一次情報最優先・法令順守のアフィリエイト記事ライター兼編集者」です。
日本語で、H2中心・正確性重視・不必要に煽らないトーンで執筆してください。

【目的】
検索流入と指名流入の両方で読者の意思決定を助け、適切なCTAで比較→選択→購入へ導く。

【厳守】
1) 事実は一次情報（公式/正規販売ページ）に脚注でリンク。
2) 価格・在庫・キャンペーンは変動前提。「執筆時点」の注意書きを必ず入れる。断定表現禁止。
3) 比較は公正。長所/短所の両方を記載。医療/効果効能の断定NG。
4) 冒頭または直後に開示文：『当サイトはアフィリエイト広告（Amazonアソシエイト含む）を利用しています。』
5) H2中心・短段落・箇条書き多用。結論→理由→具体例。

【出力仕様】
- 文字数：2000〜3500字（短すぎ禁止）
- 必須の比較表（Markdown）：列=『製品名 | ここが強い | 注意点 | 重さ/サイズ | 主な指標 | 公式参考リンク』
- CTAブロックは3つ以上（例：『[購入はこちら](URL)』『[公式サイト](URL)』『[比較表を見る](#)』）
- 末尾に脚注セクション（楽天など正規販売ページを含める）
"""

def build_user_prompt(kw:str, items:List[Dict[str,Any]], affiliate_disclosure:str)->str:
    lines = []
    lines.append(f"# テーマ: {kw}")
    lines.append("")
    lines.append("【比較対象（一次情報の参考）】")
    for it in items[:6]:
        name = it.get("itemName","")[:80]
        url = it.get("itemUrl","")
        price = it.get("itemPrice","")
        ra = it.get("reviewAverage","")
        lines.append(f"- {name} | 価格目安: {price}円 | 参考: {url} | レビュー平均: {ra}")
    lines.append("")
    lines.append("【記事タイプ】比較まとめ/ランキング/用途別おすすめ（適切に構成）")
    lines.append("【注意点】医療・効果の断定表現禁止。未確定の価格は『執筆時点の参考価格』とする。")
    lines.append("【開示文】" + affiliate_disclosure)
    return "\n".join(lines)

# -----------------------------
# WordPress 投稿
# -----------------------------
WP_SITE_URL = os.getenv("WP_SITE_URL", "").rstrip("/")
WP_USERNAME = os.getenv("WP_USERNAME", "")
WP_APP_PASSWORD = os.getenv("WP_APP_PASSWORD", "")

def wp_post(title:str, content_md:str, slug_hint:str, categories:List[str])->bool:
    if not WP_SITE_URL or not WP_USERNAME or not WP_APP_PASSWORD:
        alert("WP_CONFIG_MISSING","error",{"kw":title,"stage":"publish","reason":"WP環境変数が不足"})
        return False
    # slug 重複回避
    base = slugify(slug_hint or title)[:60]
    slug = base + "-" + datetime.now().strftime("%Y%m%d")
    # カテゴリID解決（なければ自動作成はせず未分類）
    term_ids = []
    try:
        r = requests.get(f"{WP_SITE_URL}/wp-json/wp/v2/categories", params={"per_page":100}, timeout=20,
                         auth=(WP_USERNAME, WP_APP_PASSWORD))
        if r.ok:
            cats = {c["name"]:c["id"] for c in r.json()}
            for cn in categories:
                if cn in cats:
                    term_ids.append(cats[cn])
    except Exception:
        pass

    payload = {
        "title": title,
        "content": content_md,
        "status": "publish",
        "slug": slug,
    }
    if term_ids:
        payload["categories"] = term_ids

    r = requests.post(f"{WP_SITE_URL}/wp-json/wp/v2/posts",
                      json=payload, timeout=30, auth=(WP_USERNAME, WP_APP_PASSWORD))
    if not r.ok:
        alert("WP_POST_FAILED","error",{"kw":title,"stage":"publish","http":r.status_code,"resp":r.text[:500]})
        logging.error(f"http_error: POST {WP_SITE_URL}/wp-json/wp/v2/posts -> {r.status_code} {r.text}")
        return False
    return True

# -----------------------------
# 本文生成 → 検証 → 必要なら追記リトライ
# -----------------------------
def generate_article(kw:str, items:List[Dict[str,Any]])->Optional[str]:
    model = CONFIG["llm"]["model"]
    affiliate_disclosure = CONFIG["site"]["affiliate_disclosure"]
    system = {"role":"system", "content": BASE_SPEC}
    user = {"role":"user", "content": build_user_prompt(kw, items, affiliate_disclosure)}

    md = llm_chat(model, [system, user], "llm_call", kw, max_tokens=2300, temperature=CONFIG["llm"]["temperature"])
    if not md:
        return None

    errs = validate(md)
    if not errs:
        return md

    # 追記指示で一度だけ修正依頼
    alert("VALIDATION_FAILED","warning",{"kw":kw,"stage":"validation","reason":"本文検証NG","errors":errs})
    fix = {
        "role":"user",
        "content":(
            "次の不足をすべて解消して追記・修正してください。\n"
            f"- 不足: {', '.join(errs)}\n"
            "- 必須: 比較表（指定カラム）、CTAリンク3つ以上、『執筆時点』の注意書き、脚注のURL列挙。\n"
            "- 文字数: 2200字以上を確実に満たす。\n"
            "- 既存本文は活かしつつ不足分を補完し、最終版として出力。"
        )
    }
    md2 = llm_chat(model, [system, {"role":"assistant","content":md}, fix], "llm_repair", kw, max_tokens=1200, temperature=CONFIG["llm"]["temperature"])
    if not md2:
        return md
    if not validate(md2):
        return md2
    return md  # まだNGなら最初版を返す（投稿はスキップされ得る）

# -----------------------------
# Main
# -----------------------------
def main():
    try:
        seeds = expand_keywords()
        if not seeds:
            alert("KW_EMPTY","error",{"kw":"","stage":"kw","reason":"キーワードが空"})
            return

        posted = 0
        max_posts = CONFIG["site"]["posts_per_run"]
        cats = CONFIG["site"].get("category_names", ["レビュー"])

        for kw in seeds:
            if posted >= max_posts:
                break

            logging.info(f"query kw='{kw}'")
            items = rakuten_items(kw, max_total=CONFIG["data_sources"]["rakuten"]["max_per_seed"])
            good = filter_items(items)
            logging.info(f"stats kw='{kw}': total={len(items)}, after_filters={len(good)}")

            if len(good) < CONFIG["content"]["min_items"]:
                logging.info(f"skip thin (<{CONFIG['content']['min_items']}) for '{kw}'")
                continue

            md = generate_article(kw, good)
            if not md:
                continue

            title = f"{kw}のおすすめ比較【失敗しにくい選び方と注意点】"
            if wp_post(title, md, slug_hint=kw, categories=cats):
                posted += 1
                logging.info(f"posted '{kw}'")

        logging.info(f"done, posted={posted}")
    except Exception as e:
        logging.exception("run_failed")
        alert("RUN_FAILED", "error", {"kw":"", "stage":"run", "reason":"未捕捉の例外", "exception": str(e)})
        raise

if __name__ == "__main__":
    main()
