#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, json, time, re, logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

import requests
import yaml
from slugify import slugify
from openai import OpenAI

# --------------------------------
# ログ
# --------------------------------
LOG_FILE = "run.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

def jst_now_iso():
    return datetime.now(timezone(timedelta(hours=9))).isoformat()

# --------------------------------
# Discord 通知
# --------------------------------
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

# --------------------------------
# 設定
# --------------------------------
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
    return cfg

CONFIG = load_config()

# --------------------------------
# OpenAI ラッパ
# --------------------------------
_openai = OpenAI()

def _is_reasoning_like(model:str)->bool:
    m = model.lower()
    return m.startswith("gpt-5") or m.startswith("o")

def llm_chat(model:str, messages:List[Dict[str,str]], purpose:str, kw:str,
             max_tokens:int=2200, temperature:float=0.4)->Optional[str]:
    def _try(mdl:str, allow_temp:bool)->str:
        kwargs: Dict[str,Any] = dict(model=mdl, messages=messages)
        if _is_reasoning_like(mdl):
            kwargs["max_completion_tokens"] = max_tokens
            if allow_temp and not mdl.lower().startswith("gpt-5"):
                kwargs["temperature"] = temperature
        else:
            kwargs["max_tokens"] = max_tokens
            kwargs["temperature"] = temperature
        return _openai.chat.completions.create(**kwargs).choices[0].message.content

    try:
        try:
            return _try(model, allow_temp=True)
        except Exception as e1:
            em = str(e1)
            if "max_tokens" in em and "max_completion_tokens" in em:
                # パラメータ自動切替は上で実施済み。念のため再試行。
                return _try(model, allow_temp=True)
            if "Unsupported parameter" in em and "temperature" in em:
                return _try(model, allow_temp=False)
            raise e1
    except Exception as e:
        alert("LLM_CALL_FAILED", "error", {
            "kw": kw, "stage": purpose, "model": model, "exception": f"{type(e).__name__}: {e}"
        })
        for fb in CONFIG["llm"].get("fallback_models", []):
            try:
                alert("LLM_MODEL_FALLBACK", "warning", {"kw": kw, "from_model": model, "to_model": fb})
                return llm_chat(fb, messages, purpose, kw, max_tokens=max_tokens, temperature=temperature)
            except Exception:
                continue
        alert("LLM_FAILED_FINAL", "error", {"kw": kw, "stage": purpose})
        return None

# --------------------------------
# 楽天API
# --------------------------------
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

# --------------------------------
# 検証
# --------------------------------
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

# --------------------------------
# キーワード拡張
# --------------------------------
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
        "以下の要素から、自然な日本語キーワードを箇条書きで最大{n}件。1行1キーワード。\n"
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
        s = line.strip("-•* \t")
        if not s: continue
        if len(s) > 40: continue
        if s not in out:
            out.append(s)
    # 先頭にseed優先
    uniq = []
    for x in seeds + out:
        if x not in uniq:
            uniq.append(x)
    return uniq[:max_cand]

# --------------------------------
# 記事プロンプト
# --------------------------------
BASE_SPEC = """
あなたは「一次情報最優先・法令順守のアフィリエイト記事ライター兼編集者」です。
H2中心・短文多め・結論→理由→具体例。誇大/断定NG。価格/在庫は変動前提で「執筆時点」注意書き。

必須:
- 2000〜3500字
- 比較表（列=製品名|ここが強い|注意点|重さ/サイズ|主な指標|公式参考リンク）
- CTAリンク3つ以上（例: [購入はこちら](URL), [公式サイト](URL), [比較表を見る](#)）
- 末尾に脚注（一次情報URL）
- 冒頭または直後に開示文を掲載
"""

def build_user_prompt(kw:str, items:List[Dict[str,Any]], disclosure:str)->str:
    lines = [f"# テーマ: {kw}", "", "【比較対象（一次情報の参考）】"]
    for it in items[:6]:
        lines.append(f"- {it.get('itemName','')[:80]} | 参考: {it.get('itemUrl','')} | 価格目安:{it.get('itemPrice','')}円 | レビュー:{it.get('reviewAverage','')}")
    lines += [
        "", "【記事タイプ】比較まとめ/用途別おすすめ",
        "【注意】医療・効果断定禁止。価格は『執筆時点の参考価格』と明記。",
        "【開示文】" + disclosure
    ]
    return "\n".join(lines)

# --------------------------------
# WordPress
# --------------------------------
WP_SITE_URL = os.getenv("WP_SITE_URL","").rstrip("/")
WP_USERNAME = os.getenv("WP_USERNAME","")
WP_APP_PASSWORD = os.getenv("WP_APP_PASSWORD","")

def check_wp_auth()->bool:
    """事前に /users/me で権限確認し、問題があれば即通知して打ち切る"""
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
                          params={"per_page":100}, timeout=20, auth=(WP_USERNAME, WP_APP_PASSWORD))
        if rc.ok:
            cats = {c["name"]:c["id"] for c in rc.json()}
            for cn in categories:
                if cn in cats: term_ids.append(cats[cn])
    except Exception as e:
        logging.warning(f"wp_cat_fetch_warn: {e}")

    payload = {"title": title, "content": content_md, "status": "publish", "slug": slug}
    if term_ids: payload["categories"] = term_ids

    logging.info(f"wp_post_try: title='{title[:30]}' slug='{slug}'")
    r = requests.post(f"{WP_SITE_URL}/wp-json/wp/v2/posts",
                      json=payload, timeout=40, auth=(WP_USERNAME, WP_APP_PASSWORD))
    if not r.ok:
        logging.error(f"wp_post_fail: http={r.status_code} resp={r.text[:400]}")
        alert("WP_POST_FAILED","error",{"kw":title,"stage":"publish","http":r.status_code,"resp":r.text[:400]})
        return False
    logging.info(f"wp_post_ok: id={r.json().get('id')} slug={slug}")
    return True

# --------------------------------
# 生成 → 検証 → 追記
# --------------------------------
def generate_article(kw:str, items:List[Dict[str,Any]])->Optional[str]:
    system = {"role":"system","content":BASE_SPEC}
    user = {"role":"user","content":build_user_prompt(kw, items, CONFIG["site"]["affiliate_disclosure"])}
    model = CONFIG["llm"]["model"]

    md = llm_chat(model, [system, user], "llm_call", kw, max_tokens=2300, temperature=CONFIG["llm"]["temperature"])
    if not md:
        return None

    errs = validate(md)
    if not errs:
        return md

    alert("VALIDATION_FAILED","warning",{"kw":kw,"stage":"validation","errors":errs})
    fix = {"role":"user","content":(
        "不足をすべて解消して最終稿を出力:\n"
        f"- 不足: {', '.join(errs)}\n"
        "- 必須: 指定の比較表, CTA3つ以上, 『執筆時点』注意, 脚注URL, 2000字以上\n"
        "- 既存本文は保持しつつ追記/修正のみ"
    )}
    md2 = llm_chat(model, [system, {"role":"assistant","content":md}, fix],
                   "llm_repair", kw, max_tokens=1200, temperature=CONFIG["llm"]["temperature"])
    return md2 or md

# --------------------------------
# Main
# --------------------------------
def main():
    try:
        # 1) WP 認証事前チェック（失敗なら即終了）
        if not check_wp_auth():
            logging.info("abort: wp_auth_failed")
            return

        # 2) キーワード
        seeds = expand_keywords()
        if not seeds:
            alert("KW_EMPTY","error",{"kw":"","stage":"kw","reason":"キーワードが空"})
            return

        posted = 0
        max_posts = CONFIG["site"]["posts_per_run"]
        cats = CONFIG["site"].get("category_names", ["レビュー"])

        for kw in seeds:
            if posted >= max_posts: break

            logging.info(f"query kw='{kw}'")
            items = rakuten_items(kw, max_total=CONFIG["data_sources"]["rakuten"]["max_per_seed"])
            good = filter_items(items)
            logging.info(f"stats kw='{kw}': total={len(items)}, after_filters={len(good)}")

            if len(good) < CONFIG["content"]["min_items"]:
                logging.info(f"skip thin (<{CONFIG['content']['min_items']}) for '{kw}'")
                continue

            md = generate_article(kw, good)
            if not md:
                logging.info(f"skip no-md for '{kw}'")
                continue

            title = f"{kw}のおすすめ比較【失敗しにくい選び方と注意点】"
            ok = wp_post(title, md, slug_hint=kw, categories=cats)
            if ok:
                posted += 1

        logging.info(f"done, posted={posted}")
    except Exception as e:
        logging.exception("run_failed")
        alert("RUN_FAILED","error",{"kw":"","stage":"run","reason":"未捕捉の例外","exception":str(e)})
        raise

if __name__ == "__main__":
    main()
