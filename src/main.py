# -*- coding: utf-8 -*-
"""
Draft-only generator:
- 1キーワードにつき 1本の下書きMarkdownを生成し drafts/ に保存
- WP投稿はしない（完全に切り離し）
- Discordへ DRAFT_READY / NO_PUBLISH / RUN_SUMMARY を通知
- LLMは Responses API の gpt-5 を第一候補。失敗時に gpt-4o → gpt-4o-mini へ段階的フォールバック
- Rakuten API (IchibaItem/Search) で候補商品を収集（任意）。不足時も記事は生成継続

環境変数:
  OPENAI_API_KEY (必須)
  ALERT_WEBHOOK_URL (任意 / Discord Webhook)
  RAKUTEN_APP_ID (任意 / 商品情報強化用)

実行例:
  python -u src/main.py --config config/app.yaml
"""
import os
import sys
import json
import time
import math
import random
import argparse
import logging
import pathlib
import datetime
import traceback
from typing import List, Dict, Any, Optional

import yaml
import requests
from slugify import slugify

# ---------- ログ設定 ----------
LOG_PATH = "run.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

JST = datetime.timezone(datetime.timedelta(hours=9))

# ---------- 通知 ----------
def notify(event: str, severity: str = "info", **payload):
    """DiscordへJSONを投げる。内容は人間が読んで即状況把握できる形式。"""
    webhook = os.getenv("ALERT_WEBHOOK_URL", "").strip()
    ts = datetime.datetime.now(JST).isoformat()
    body = {
        "event": event,
        "severity": severity,
        "ts_jst": ts,
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
    body.update(payload)
    msg = f"[AUTO-REV][{severity.upper()}] EVENT={event}\n" + "```\n" + json.dumps(body, ensure_ascii=False, indent=2) + "\n```"
    logging.info("%s %s", event, payload)
    if webhook:
        try:
            requests.post(webhook, json={"content": msg}, timeout=30)
        except Exception as e:
            logging.error("notify failed: %s", e)

# ---------- 設定読込 ----------
def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg

# ---------- ユーティリティ ----------
def ensure_dir(p: str):
    pathlib.Path(p).mkdir(parents=True, exist_ok=True)

def jst_now():
    return datetime.datetime.now(JST)

def pick_keywords(cfg: Dict[str, Any]) -> List[str]:
    seeds: List[str] = cfg["keywords"].get("seeds", [])
    per_run: int = int(cfg["keywords"].get("per_run", 1))
    if not seeds:
        return []
    # 安定性重視：シードから先頭順で最大 per_run 件
    return seeds[:per_run]

# ---------- 楽天API ----------
def rakuten_items(app_id: Optional[str], kw: str, max_hits: int = 30) -> List[Dict[str, Any]]:
    if not app_id:
        return []
    url = "https://app.rakuten.co.jp/services/api/IchibaItem/Search/20220601"
    params = {
        "applicationId": app_id.strip(),
        "keyword": kw.strip(),
        "hits": min(max_hits, 30),
        "page": 1,
        "sort": "+reviewCount",  # レビュー多い順で安定
        "format": "json",
    }
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code >= 400:
            notify("rakuten_api_error", "error", kw=kw, reason=f"HTTP {r.status_code}", resp=r.text)
            logging.error("rakuten_api_error kw='%s': HTTP %s - %s", kw, r.status_code, r.text)
            return []
        data = r.json()
        items = []
        for it in data.get("Items", []):
            i = it.get("Item", {})
            items.append({
                "name": i.get("itemName"),
                "url": i.get("itemUrl"),
                "price": i.get("itemPrice"),
                "shop": i.get("shopName"),
                "image": i.get("mediumImageUrls",[{}])[0].get("imageUrl"),
                "review_count": i.get("reviewCount"),
                "review_average": i.get("reviewAverage"),
                "genre_id": i.get("genreId"),
            })
        return items
    except Exception as e:
        notify("rakuten_api_error", "error", kw=kw, reason=str(e))
        logging.exception("rakuten_api_error kw='%s'", kw)
        return []

def shortlist(items: List[Dict[str, Any]], limit: int = 5) -> List[Dict[str, Any]]:
    # レビュー数→評価→価格の簡易ソート
    def key(i):
        return (
            int(i.get("review_count") or 0),
            float(i.get("review_average") or 0.0),
            -float(i.get("price") or 0),  # 価格高い方が上にならないように後で逆転しないシンプル指標
        )
    items_sorted = sorted(items, key=key, reverse=True)
    # 重複名称を除去
    seen = set()
    uniq = []
    for i in items_sorted:
        n = (i.get("name") or "").strip()
        if not n or n in seen:
            continue
        seen.add(n)
        uniq.append(i)
        if len(uniq) >= limit:
            break
    return uniq

# ---------- LLM（Responses API優先） ----------
OPENAI_API = "https://api.openai.com/v1"

def _headers():
    key = os.getenv("OPENAI_API_KEY", "").strip()
    if not key:
        raise RuntimeError("OPENAI_API_KEY is empty")
    return {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}

def responses_api(model: str, prompt_text: str, max_completion_tokens: int = 2200, metadata: Optional[Dict[str,Any]]=None) -> str:
    """
    Responses API に合わせた最小パラメータのみ送信。
    注意: temperature / response_format / text.format は送らない（モデルによって非対応）
    """
    url = f"{OPENAI_API}/responses"
    payload = {
        "model": model,
        "input": prompt_text,
        "max_completion_tokens": max_completion_tokens,
    }
    if metadata:
        payload["metadata"] = metadata
    r = requests.post(url, headers=_headers(), json=payload, timeout=120)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} - {r.text}")
    data = r.json()
    # 取り出し：output_text / content[] / choices[] の順で吸収
    if "output_text" in data and isinstance(data["output_text"], str) and data["output_text"].strip():
        return data["output_text"]
    if "content" in data and isinstance(data["content"], list):
        texts = []
        for seg in data["content"]:
            # 仕様差異を吸収（text or output_textなど）
            if isinstance(seg, dict):
                if "text" in seg and isinstance(seg["text"], str):
                    texts.append(seg["text"])
                elif "output_text" in seg and isinstance(seg["output_text"], str):
                    texts.append(seg["output_text"])
        txt = "\n".join(t for t in texts if t)
        if txt.strip():
            return txt
    # Chat形式で返るケースも保険で処理
    if "choices" in data and data["choices"]:
        ch = data["choices"][0]
        if "message" in ch and isinstance(ch["message"], dict):
            return ch["message"].get("content","")
        if "text" in ch:
            return ch["text"]
    raise ValueError("empty_completion")

def chat_api(model: str, system: str, user: str, max_tokens: int = 2200) -> str:
    """従来のchat.completionsにフォールバック"""
    url = f"{OPENAI_API}/chat/completions"
    payload = {
        "model": model,
        "messages": [
            {"role":"system","content":system},
            {"role":"user","content":user},
        ],
        "max_tokens": max_tokens,
    }
    r = requests.post(url, headers=_headers(), json=payload, timeout=120)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} - {r.text}")
    data = r.json()
    return data["choices"][0]["message"]["content"]

def call_llm(spec_prompt: str, kw: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """gpt-5 → gpt-4o → gpt-4o-mini の順で試行。Responses優先。"""
    models = [cfg["llm"]["model_primary"]] + cfg["llm"].get("fallback_models", [])
    md = None
    used = None
    errors = []
    for m in models:
        try:
            # Responses APIを先に試す
            md = responses_api(m, spec_prompt, max_completion_tokens=int(cfg["llm"].get("max_completion_tokens", 2200)),
                               metadata={"kw": kw, "mode":"draft_only"})
            used = m
            break
        except Exception as e1:
            errors.append(f"{m}/responses:{e1}")
            notify("LLM_CALL_FAILED", "error", stage="llm_call", model=m, exception=str(e1))
            # Chat API フォールバック（gpt-4o系はchat対応が安定）
            if m.startswith("gpt-4"):
                try:
                    md = chat_api(m, "You are a careful Japanese writer.", spec_prompt, max_tokens= int(cfg["llm"].get("max_completion_tokens", 2200)))
                    used = m
                    notify("LLM_MODEL_FALLBACK", "warning", from_model=cfg["llm"]["model_primary"], to_model=m)
                    break
                except Exception as e2:
                    errors.append(f"{m}/chat:{e2}")
                    notify("LLM_CALL_FAILED", "error", stage="llm_call", model=m, exception=str(e2))
                    continue
            else:
                continue
    if md is None or not md.strip():
        raise RuntimeError("LLM all failed: " + " | ".join(errors))
    return {"text": md, "model_used": used}

# ---------- 検証 ----------
def validate_markdown(md: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    min_chars = int(cfg["validation"].get("min_chars", 1500))
    req_table = bool(cfg["validation"].get("require_table", True))
    req_cta = int(cfg["validation"].get("require_cta_count", 3))
    errors = []
    L = len(md)
    if L < min_chars:
        errors.append(f"too_short:{L}")
    # Markdown表（| が3本以上の行が存在）
    has_table = any(line.count("|") >= 3 for line in md.splitlines())
    if req_table and not has_table:
        errors.append("missing_table")
    # CTA見出し（3つ）
    cta_count = sum(1 for line in md.splitlines() if ("CTA" in line or "迷っている人" in line or "即決" in line))
    if cta_count < req_cta:
        errors.append("few_buttons")
    ok = len(errors) == 0
    return {"ok": ok, "errors": errors, "length": L}

# ---------- プロンプト作成 ----------
def build_prompt(kw: str, site: Dict[str, Any], items: List[Dict[str, Any]], cfg: Dict[str, Any]) -> str:
    """
    ユーザーの仕様書をベースに、一次情報優先/H2中心/表/CTA/脚注/JSON-LD雛形を強制。
    """
    # 競合比較の軸（設定 or デフォルト）
    axes = cfg.get("axes", ["耐久性","保証","カラーバリエーション","追加オプション","流通度合い"])
    disclosure = site.get("affiliate_disclosure") or "当サイトはアフィリエイト広告（Amazonアソシエイト含む）を利用しています。"
    # 楽天商品をJSONで渡す
    items_json = json.dumps(items, ensure_ascii=False, indent=2)

    spec = f"""
あなたは「一次情報最優先・法令順守のアフィリエイト記事ライター兼編集者」です。
日本語で、H2中心の構成・正確性重視・不必要に煽らないトーンで執筆してください。

【目的】
検索流入と指名流入の両方で読者の意思決定を助け、適切なCTAで離脱せずに比較→選択→購入へ導く記事を作る。

【読者像】
- 読者タイプ: 初心者
- 主要ニーズ: コスパが良く失敗しにくいものだけ知りたい
- シチュエーション: 一人暮らし/家族

【入力】
- 記事タイプ: 比較まとめ＋用途別おすすめ
- 注意点/除外: 医療・効果の断定表現を禁止、未確定の価格は書かない
- 競合比較の軸: {", ".join(axes)}
- 内部リンク予定: なし（プレースホルダでOK）

【厳守事項】
1) 一次情報（公式サイト/メーカー/正規販売ページ/公式X・プレス）を優先し、事実は必ず根拠リンクを脚注で示す。
2) 価格・在庫・キャンペーンは変動前提。「執筆時点」表現と注意書きを必ず入れる。最安や断定表現は禁止。
3) 比較は公正・具体。欠点も必ず記載。誇大・医療/効果効能の断定・体験の一般化をしない。
4) Amazonアソシエイト等の開示文を本文冒頭か直後に掲載。
5) 読みやすさ優先：H2中心、段落短め、箇条書きを多用。結論→理由→具体例の順で。

【出力仕様（Markdown）】
1. タイトル案 ×8（32〜48字）
2. メタ情報
   - slug（ローマ字短め）／meta description（全角80〜120字）
   - 主要見出しの想定検索意図マッピング（クエリ例も）
3. 記事アウトライン（H2/H3）
4. 本文（1500〜3000字）
   - 冒頭：読者の状況→結論の要約（誰に/なぜ/何を選べば良いか）
   - 開示文テンプレ：『{disclosure}』
   - セクション：選び方（評価軸3〜5個を短く定義）
   - セクション：比較（表＋短評）※同一条件で公平に
   - セクション：推しの1〜3製品の深掘り（長所/短所/向いている人）
   - セクション：よくある質問（FAQ 5問）
   - まとめ：重要ポイント3つ＋CTA（3パターン）
5. 比較表（Markdown）
   - 列：製品名 / ここが強い / 注意点 / 重さorサイズ / 主要指標 / 公式参考リンク
6. CTAブロック（3パターン / 迷っている人・即決したい人・さらに比較したい人）
7. 脚注（参照した一次情報URLを列挙）
8. 追加（任意）
   - 構造化データの雛形（FAQPage + ItemList のJSON-LD。価格は不明なら記載しない）
   - OGP/アイキャッチの画像プロンプト案 ×3

【制約】
- 断定は根拠がある時のみ。曖昧なときは条件付き表現。
- 文章は短文主体・改行多め・H2中心。
- 比較表とCTAは必ず含める（表はMarkdownの|を使う）。

【テーマ】
- キーワード: 「{kw}」

【候補商品データ（一次情報化のヒント / 使える範囲で）】
{items_json}

出力は**Markdownのみ**。余計な前置き・注釈は不要。
""".strip()
    return spec

# ---------- メイン ----------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    cfg = load_config(args.config)
    ensure_dir("drafts")

    site = cfg.get("site", {})
    keywords = pick_keywords(cfg)
    per_run = len(keywords)

    openai_key_present = bool(os.getenv("OPENAI_API_KEY", "").strip())
    if not openai_key_present:
        notify("LLM_KEY_MISSING", "error", reason="OPENAI_API_KEY empty")
        raise SystemExit(2)

    total_generated = 0
    failures = 0
    used_models = []

    rakuten_app_id = os.getenv("RAKUTEN_APP_ID", "").strip() or None

    for kw in keywords:
        # 1) 楽天で候補収集（任意）
        items = rakuten_items(rakuten_app_id, kw, max_hits=30)
        short = shortlist(items, limit=int(cfg.get("shortlist_limit", 5)))

        # 2) プロンプト作成
        prompt = build_prompt(kw, site, short, cfg)

        # 3) LLM呼び出し
        try:
            out = call_llm(prompt, kw, cfg)
            md = out["text"]
            model_used = out["model_used"] or cfg["llm"]["model_primary"]
            used_models.append(model_used)
        except Exception as e:
            failures += 1
            notify("LLM_FAILED_FINAL", "error", kw=kw, stage="llm_call", reason=str(e))
            continue

        # 4) 検証
        v = validate_markdown(md, cfg)
        if not v["ok"]:
            notify("VALIDATION_FAILED", "warning", kw=kw, stage="validation", errors=v["errors"])

        # 5) 保存（slugはkw基準）
        date_str = jst_now().strftime("%Y%m%d")
        slug_base = slugify(kw) or f"post-{date_str}"
        slug = f"{slug_base}-{date_str}"
        out_path = pathlib.Path("drafts") / f"{slug}.md"
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(md)

        # 6) デバッグ保存
        with open("llm_prompt.json", "w", encoding="utf-8") as f:
            json.dump({"kw": kw, "prompt": prompt}, f, ensure_ascii=False, indent=2)
        with open("llm_output.txt", "w", encoding="utf-8") as f:
            f.write(md)

        total_generated += 1
        notify("DRAFT_READY", "info", kw=kw, path=str(out_path), model=model_used, length=v["length"], validation_errors=v["errors"])

    # 7) サマリ
    if total_generated == 0:
        notify("NO_PUBLISH", "warning", reason="draft_only mode & 生成ゼロ", counts={"generated": total_generated, "failures": failures})
    notify("RUN_SUMMARY", "info", counts={"generated": total_generated, "failures": failures, "per_run": per_run}, models=used_models)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        notify("RUN_FAILED", "error", stage="run", reason="未捕捉の例外", exception=str(e))
        logging.exception("RUN_FAILED")
        sys.exit(1)
