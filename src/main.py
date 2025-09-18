#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, json, re, logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

import requests
import yaml
from slugify import slugify
from openai import OpenAI

# ============ Logging ============
LOG_FILE = "run.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

def jst_now_iso():
    return datetime.now(timezone(timedelta(hours=9))).isoformat()

# ============ Discord Alert ============
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "")

def alert(event: str, severity: str, payload: Dict[str, Any]):
    if not ALERT_WEBHOOK_URL:
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
            "run_url": f"https://github.com/{os.getenv('GITHUB_REPOSITORY','')}/actions/runs/{os.getenv('GITHUB_RUN_ID','')}",
        },
    }
    base.update(payload or {})
    head = f"[AUTO-REV][{severity.upper()}] EVENT={event} KW=\"{base.get('kw','')}\""
    try:
        requests.post(ALERT_WEBHOOK_URL, json={
            "content": head + "\n```json\n" + json.dumps(base, ensure_ascii=False, indent=2) + "\n```"
        }, timeout=15)
    except Exception as e:
        logging.error(f"alert_failed: {e}")

# ============ Config ============
def load_config(path="config/app.yaml")->Dict[str,Any]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg.setdefault("site", {}).setdefault("posts_per_run", 1)
    cfg["site"].setdefault("affiliate_disclosure", "当サイトはアフィリエイト広告（Amazonアソシエイト含む）を利用しています。")
    cfg.setdefault("llm", {}).setdefault("model", "gpt-4o-mini")
    cfg["llm"].setdefault("fallback_models", ["gpt-4o","gpt-4o-mini"])
    cfg["llm"].setdefault("temperature", 0.4)
    cfg.setdefault("content", {}).setdefault("min_items", 3)
    cfg["content"].setdefault("price_floor", 2000)
    cfg["content"].setdefault("review_floor", 3.8)
    cfg["content"].setdefault("min_chars", 1800)
    cfg.setdefault("keywords", {}).setdefault("mode", "expand_llm")
    cfg["keywords"].setdefault("max_candidates", 30)
    cfg.setdefault("data_sources", {}).setdefault("rakuten", {
        "endpoint":"https://app.rakuten.co.jp/services/api/IchibaItem/Search/20220601",
        "max_per_seed":60
    })
    cfg.setdefault("site", {}).setdefault("category_names", ["レビュー"])
    return cfg

CONFIG = load_config()

# ============ OpenAI wrapper ============
_openai = OpenAI()

def _is_reasoning_like(model:str)->bool:
    m = model.lower()
    return m.startswith("gpt-5") or m.startswith("o")

def _extract_text(resp) -> str:
    """
    Chat Completions の choice から安全にテキストを引く。
    content が None/空なら空文字を返す（呼び出し側で判定）。
    """
    try:
        choice = resp.choices[0]
        msg = choice.message
        text = getattr(msg, "content", None)
        if text is None:
            # 一部モデルは refusal や tool_calls を返す場合がある
            # ここでは空文字扱いにして上位でリトライ/フォールバックする
            return ""
        return str(text)
    except Exception:
        return ""

def llm_chat(model:str, messages:List[Dict[str,str]], purpose:str, kw:str,
             max_tokens:int=2200, temperature:float=0.4) -> Optional[str]:
    """
    - reasoning系 (gpt-5など): max_completion_tokens を使用
    - それ以外: max_tokens を使用
    - content が空のときは例外を投げてフォールバックさせる
    """
    def _try(mdl:str, allow_temp:bool)->str:
        kwargs: Dict[str,Any] = dict(model=mdl, messages=messages)
        if _is_reasoning_like(mdl):
            kwargs["max_completion_tokens"] = max_tokens
            if allow_temp and not mdl.lower().startswith("gpt-5"):
                kwargs["temperature"] = temperature
        else:
            kwargs["max_tokens"] = max_tokens
            kwargs["temperature"] = temperature
        resp = _openai.chat.completions.create(**kwargs)
        text = _extract_text(resp)
        if not text or not text.strip():
            raise ValueError("empty_completion")
        return text

    try:
        try:
            return _try(model, allow_temp=True)
        except Exception as e1:
            em = str(e1)
            # パラメータ系の自動切替（max_tokens→max_completion_tokens / temperature除去）
            if "Unsupported parameter" in em and "temperature" in em:
                return _try(model, allow_temp=False)
            if "max_tokens" in em and "max_completion_tokens" in em:
                return _try(model, allow_temp=True)
            raise e1
    except Exception as e:
        alert("LLM_CALL_FAILED", "error", {
            "kw": kw, "stage": purpose, "model": model, "exception": f"{type(e).__name__}: {e}"
        })
        for fb in CONFIG["llm"].get("fallback_models", []):
            try:
                alert("LLM_MODEL_FALLBACK", "warning", {"kw": kw, "from_model": model, "to_model": fb})
                return _try(fb, allow_temp=True)
            except Exception as e2:
                alert("LLM_FALLBACK_ATTEMPT", "warning", {"kw": kw, "from": model, "to": fb, "exception": f"{type(e2).__name__}: {e2}"})
                continue
        alert("LLM_FAILED_FINAL", "error", {"kw": kw, "stage": purpose})
        return None

# ============ Rakuten ============
RAKUTEN_APP_ID = os.getenv("RAKUTEN_APP_ID","")

def rakuten_items(keyword:str, max_total:int=60)->List[Dict[str,Any]]:
    url = CONFIG["data_sources"]["rakuten"]["endpoint"]
    out: List[Dict[str,Any]] = []
    page = 1
    while len(out) < max_total and page <= 3:
        hits = min(30, max_total - len(out))
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
            alert("RAKUTEN_API_ERROR","error",{"kw":keyword,"stage":"fetch","reason":r.text})
            break
        r.raise_for_status()
        data = r.json()
        items = data.get("Items", [])
        out.extend(items)
        if len(items) < hits: break
        page += 1
    return out

def filter_items(items:List[Dict[str,Any]])->List[Dict[str,Any]]:
    pf = CONFIG["content"]["price_floor"]
    rf = CONFIG["content"]["review_floor"]
    ok = []
    for it in items:
        try:
            if int(it.get("itemPrice",0)) >= pf and float(it.get("reviewAverage",0) or 0) >= rf:
                ok.append(it)
        except Exception:
            continue
    return ok

# ============ Validators ============
def has_table(md:str)->bool:
    return bool(re.search(r"^\|?\s*製品名\s*\|", md, flags=re.MULTILINE))

def count_cta(md:str)->int:
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

# ============ Keyword expand ============
def expand_keywords()->List[str]:
    mode = CONFIG.get("keywords",{}).get("mode","seeds")
    seeds = CONFIG.get("keywords",{}).get("seeds",[]) or []
    if mode != "expand_llm" or not seeds:
        return seeds

    pools = CONFIG["keywords"].get("pools",{})
    nouns = pools.get("nouns",[])
    modifiers = pools.get("modifiers",[])
    specs = pools.get("specs",[])
    max_cand = CONFIG["keywords"].get("max_candidates",30)

    system = {"role":"system","content":"日本語のロングテール商品キーワードを作るアナリスト。購買に近い語を優先。"}
    user = {"role":"user","content":(
        "以下の要素から、自然な日本語キーワードを箇条書きで最大{n}件。1行1キーワードのみ、前後に余計な文や記号は書かない。\n"
        "- シード: {seeds}\n- 名詞群: {nouns}\n- 修飾語: {mods}\n- 仕様語: {specs}\n"
        "NG: 医療効能/誇大/曖昧メーカー名"
    ).format(n=max_cand, seeds=", ".join(seeds), nouns=", ".join(nouns), mods=", ".join(modifiers), specs=", ".join(specs))}

    model = CONFIG["llm"]["model"]
    text = llm_chat(model, [system, user], "kw", kw="", max_tokens=600, temperature=0.2)
    if not text:
        alert("KW_EXPAND_FAILED","warning",{"kw":"","stage":"kw","reason":"LLMでのキーワード拡張に失敗"})
        return seeds

    out = []
    for line in text.splitlines():
        s = line.strip()
        s = s.lstrip("-•* \t").strip()
        if not s: continue
        if len(s) > 40: continue
        if s not in out: out.append(s)
    if not out:
        alert("KW_EXPAND_PARSE_EMPTY","warning",{"kw":"","stage":"kw","reason":"拡張結果が空（解析後）"})
        return seeds

    uniq = []
    for x in seeds + out:
        if x not in uniq:
            uniq.append(x)
    return uniq[:max_cand]

# ============ Article prompts ============
BASE_SPEC = """
あなたは「一次情報最優先・法令順守のアフィリエイト記事ライター兼編集者」です。
H2中心・短文多め・結論→理由→具体例。誇大/断定NG。価格/在庫は変動前提で「執筆時点」注意書き。

必須:
- 2000〜3500字（不足なら追記）
- 比較表（列=製品名|ここが強い|注意点|重さ/サイズ|主な指標|公式参考リンク）
- CTAリンク3つ以上（例: [購入はこちら](URL), [公式サイト](URL), [比較表を見る](#)）
- 末尾に脚注（一次情報URL）
- 冒頭または直後に開示文を掲載
出力はMarkdown本文のみ。前置きやコードブロックは不要。
"""

USER_SPEC = """
【記事タイプ】比較まとめ/用途別おすすめ
【注意】医療・効果断定禁止。価格は『執筆時点の参考価格』と明記。
【開示文】{disclosure}
"""

def build_user_prompt(kw:str, items:List[Dict[str,Any]], disclosure:str)->str:
    lines = [f"# テーマ: {kw}", "", "【比較対象（一次情報の参考）】"]
    for it in items[:6]:
        lines.append(f"- {it.get('itemName','')[:80]} | 参考: {it.get('itemUrl','')} | 価格目安:{it.get('itemPrice','')}円 | レビュー:{it.get('reviewAverage','')}")
    lines.append(USER_SPEC.format(disclosure=disclosure))
    return "\n".join(lines)

def build_force_prompt(errs:List[str])->str:
    return (
        "上記の原稿は要件不足です。以下を必ず満たす完全版Markdown本文のみを出力してください。\n"
        f"- 不足: {', '.join(errs)}\n"
        "- 必須: 指定の比較表, CTA3つ以上, 『執筆時点』注意, 脚注URL, 2000字以上\n"
        "- 余分な前置き/謝罪/コードブロック禁止"
    )

# ============ WordPress ============
WP_SITE_URL = os.getenv("WP_SITE_URL","").rstrip("/")
WP_USERNAME = os.getenv("WP_USERNAME","")
WP_APP_PASSWORD = os.getenv("WP_APP_PASSWORD","")

def check_wp_auth()->bool:
    if not (WP_SITE_URL and WP_USERNAME and WP_APP_PASSWORD):
        alert("WP_CONFIG_MISSING","error",{"kw":"","stage":"precheck","reason":"WP環境変数が未設定"})
        return False
    try:
        r = requests.get(f"{WP_SITE_URL}/wp-json/wp/v2/users/me", timeout=20, auth=(WP_USERNAME, WP_APP_PASSWORD))
        if not r.ok:
            alert("WP_AUTH_FAILED","error",{"kw":"","stage":"precheck","http":r.status_code,"resp":r.text[:400]})
            return False
        data = r.json()
        logging.info(f"wp_auth_ok: user={data.get('name')}")
        return True
    except Exception as e:
        alert("WP_AUTH_ERROR","error",{"kw":"","stage":"precheck","exception":str(e)})
        return False

def wp_post(title:str, content_md:str, slug_hint:str, categories:List[str])->bool:
    base = slugify(slug_hint or title)[:60]
    slug = base + "-" + datetime.now().strftime("%Y%m%d")
    term_ids = []
    try:
        rc = requests.get(f"{WP_SITE_URL}/wp-json/wp/v2/categories",
