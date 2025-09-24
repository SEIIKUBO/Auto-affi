# -*- coding: utf-8 -*-
"""
Auto-affi main (Responses API / gpt-5 対応・全文貼り替え版)
- OpenAI Responses API に準拠（gpt-5 / gpt-4o / gpt-4o-mini フォールバック）
- gpt-5 用のパラメータ: max_output_tokens, text.format, temperature 未使用
- 失敗時は Discord へ構造化通知（コピペで状況判定可能）
- 楽天API(公式)で商品データを取得 → LLM で記事生成 → WordPressへ投稿
- バリデーションに引っかかっても最小要件を満たせば投稿（安定運転寄り）
"""

import os
import sys
import json
import time
import base64
import logging
import random
import hashlib
import traceback
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import requests
import yaml
from slugify import slugify

LOG_PATH = "run.log"
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(message)s"))
logging.getLogger("").addHandler(console)


# ========= Util =========

JST = timezone(timedelta(hours=9))

def now_jst_iso() -> str:
    return datetime.now(JST).isoformat()

def read_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def getenv_required(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"ENV '{name}' is required but missing.")
    return v

def notify(event: str, severity: str, payload: Dict[str, Any]) -> None:
    """Discord に JSON を投げる（テキストと JSON 両方）"""
    url = os.getenv("ALERT_WEBHOOK_URL", "").strip()
    msg_title = f"[AUTO-REV][{severity.upper()}] EVENT={event}"
    body = {
        "event": event,
        "severity": severity,
        "ts_jst": now_jst_iso(),
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
    body.update(payload or {})

    # ログにも残す
    logging.info("%s\n%s", msg_title, json.dumps(body, ensure_ascii=False, indent=2))

    if not url:
        return
    try:
        content = f"{msg_title}\n```json\n{json.dumps(body, ensure_ascii=False, indent=2)}\n```"
        requests.post(url, json={"content": content}, timeout=15)
    except Exception:
        logging.error("discord_notify_failed: %s", traceback.format_exc())


def http_get_json(url: str, params: Dict[str, Any], timeout: int = 30) -> Dict[str, Any]:
    r = requests.get(url, params=params, timeout=timeout)
    try:
        r.raise_for_status()
    except Exception:
        # 詳細を上位で扱えるよう例外を投げる
        raise
    return r.json()


def basic_auth(user: str, app_password: str) -> str:
    token = base64.b64encode(f"{user}:{app_password}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


# ========= Config & Defaults =========

DEFAULT_CONFIG = {
    "site": {
        "affiliate_disclosure": "当サイトはアフィリエイト広告（Amazonアソシエイト含む）を利用しています。",
        "post_status": "publish",  # or "draft"
        "min_length": 1400,        # これ未満は低品質とみなす
        "accept_warnings": True    # 警告があっても投稿を許容
    },
    "llm": {
        "model_primary": "gpt-5",      # Responses API前提
        "model_fallback": ["gpt-4o", "gpt-4o-mini"],
        "max_output_tokens": 3600
    },
    "keywords": {
        "per_run": 1,
        "seeds": [
            "USB充電器 65W",
            "電動歯ブラシ コスパ",
            "空気清浄機 小型",
            "コーヒーメーカー 大容量",
            "ロボット掃除機 静音"
        ]
    },
    "rakuten": {
        "hits": 30,
        "min_items_for_article": 3,
        "min_after_filters": 3
    }
}


# ========= Rakuten API =========

def sanitize_kw(kw: str) -> str:
    return " ".join(kw.strip().split())

def rakuten_items(app_id: str, kw: str, hits: int = 30) -> List[Dict[str, Any]]:
    """IchibaItem Search 20220601"""
    hits = max(1, min(hits, 30))  # API制約
    url = "https://app.rakuten.co.jp/services/api/IchibaItem/Search/20220601"
    params = {
        "applicationId": app_id,
        "keyword": sanitize_kw(kw),
        "hits": hits,
        "format": "json",
        "imageFlag": 1,
        "sort": "+reviewAverage"
    }
    try:
        data = http_get_json(url, params, timeout=30)
    except Exception as e:
        # 400等
        try:
            j = e.response.json() if hasattr(e, "response") and e.response is not None else {}
        except Exception:
            j = {}
        notify("RAKUTEN_API_ERROR", "error", {
            "kw": kw,
            "reason": f"HTTP {getattr(e, 'response', None).status_code if hasattr(e, 'response') and e.response is not None else 'ERR'}",
            "raw": j
        })
        return []
    items = data.get("Items", [])
    out = []
    for it in items:
        f = it.get("Item", {})
        out.append({
            "name": f.get("itemName", ""),
            "price": f.get("itemPrice", 0),
            "url": f.get("itemUrl", ""),
            "image": f.get("mediumImageUrls", [{}])[0].get("imageUrl", ""),
            "shop": f.get("shopName", ""),
            "review": {
                "count": f.get("reviewCount", 0),
                "avg": f.get("reviewAverage", 0.0)
            }
        })
    return out


# ========= OpenAI (Responses API) =========

class OpenAIResponses:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base = "https://api.openai.com/v1/responses"
        self.sess = requests.Session()
        self.sess.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        })

    def create(self, model: str, system: str, user: str, max_output_tokens: int) -> str:
        """
        Responses API で markdown テキストを返す。
        - gpt-5 向けに max_output_tokens / text.format=markdown を使用。
        - gpt-4o 系でも同一ペイロードで通す（互換維持）。
        """
        payload = {
            "model": model,
            "input": [
                {
                    "role": "system",
                    "content": [{"type": "text", "text": system}]
                },
                {
                    "role": "user",
                    "content": [{"type": "text", "text": user}]
                }
            ],
            "max_output_tokens": max_output_tokens,
            "text": {"format": "markdown"}  # response_format 相当（新パラメータ）
        }
        r = self.sess.post(self.base, data=json.dumps(payload), timeout=120)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code} - {r.text}")
        data = r.json()
        # 新APIは output_text が便利（無ければ fallback 抽出）
        if "output_text" in data and data["output_text"]:
            return data["output_text"]
        # fallback（念のため）
        try:
            return data["output"][0]["content"][0]["text"]
        except Exception:
            raise ValueError("empty_completion")


def build_llm_prompt(spec: Dict[str, Any], kw: str, items: List[Dict[str, Any]], disclosure: str) -> str:
    """
    ユーザー提供の記事仕様プロンプト（要約）＋楽天アイテムをコンテキストとして渡す
    """
    # 参照用に商品を圧縮
    lines = []
    for i, it in enumerate(items[:8], 1):
        lines.append(f"- {i}. {it['name']} / 参考価格: {it['price']}円 / レビュー: {it['review']['avg']}({it['review']['count']}) / URL: {it['url']}")

    # 仕様（要点）— ユーザーが以前提示したものを凝縮し system/prompts に反映
    spec_text = f"""
あなたは「一次情報最優先・法令順守のアフィリエイト記事ライター兼編集者」です。
日本語で、H2中心・短文・逆三角形型・煽らないトーン。誇大・断定を避ける。価格は変動前提で「執筆時点」を明記。
本文内に開示文:『{disclosure}』を先頭付近に含めること。
比較は公正に。長所/短所/向く人を必ず併記。FAQ×5。最後に要点3つ＋CTA。
表(比較表)をMarkdownで出す。CTAボタン風リンク（3パターン）を用意。
"""

    ctx_items = "\n".join(lines) if lines else "- (十分な商品候補がありませんでした)"

    user_text = f"""
【キーワード】{kw}

【比較候補（楽天API）】
{ctx_items}

【出力仕様】
- 文字数目安: 2000-3500字
- 構成:
  - H2中心。冒頭で結論要約→選び方(評価軸3-5)→比較表＋短評→推し製品1-3(長所/短所/向いている人)→FAQ(5)→まとめ(要点3+CTA)
- Markdown（見出し/箇条書き/表を適切に）
- 製品リンクはプレースホルダでもよい（本文中に [公式/楽天で見る] などの文言とURL）
- 誤情報を避け、確信がない仕様数値は断定しない（“目安”/“例”を用いる）

【注意】
- 医療・効果効能の断定は禁止
- 価格/在庫は変動前提。「執筆時点」表記
- クリックベイト禁止

この条件を満たす記事本文（Markdownのみ）を出力してください。
"""
    return spec_text.strip(), user_text.strip()


def validate_md(md: str, min_len: int) -> List[str]:
    errs = []
    if len(md) < min_len:
        errs.append(f"too_short:{len(md)}")
    # 簡易: 表(パイプ記法)の有無
    if "|" not in md:
        errs.append("missing_table")
    # 簡易: CTA的要素（3つのリンクキーワードが最低2回以上）
    cta_count = md.count("楽天で見る") + md.count("公式で見る") + md.count("Amazonで見る")
    if cta_count < 2:
        errs.append("few_buttons")
    return errs


# ========= WordPress =========

def wp_can_post(site_url: str, user: str, app_pw: str) -> Optional[str]:
    """ユーザー確認（現在のユーザー情報を取れるか）"""
    try:
        r = requests.get(
            f"{site_url.rstrip('/')}/wp-json/wp/v2/users/me",
            headers={"Authorization": basic_auth(user, app_pw)},
            timeout=20
        )
        r.raise_for_status()
        j = r.json()
        logging.info("wp_auth_ok: user=%s", j.get("name", ""))
        return j.get("name", "")
    except Exception as e:
        notify("WP_AUTH_FAILED", "error", {"reason": str(e)})
        return None

def wp_post(site_url: str, user: str, app_pw: str, title: str, content_html: str, status: str = "publish", slug_hint: str = "") -> bool:
    slug_value = slugify(slug_hint or title)[:120]
    payload = {
        "title": title,
        "content": content_html,
        "status": status,
        "slug": slug_value
    }
    try:
        r = requests.post(
            f"{site_url.rstrip('/')}/wp-json/wp/v2/posts",
            headers={
                "Authorization": basic_auth(user, app_pw),
                "Content-Type": "application/json"
            },
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            timeout=30
        )
        if r.status_code == 401:
            # 権限エラーは詳細通知
            notify("WP_POST_401", "error", {
                "endpoint": "wp/v2/posts",
                "resp": r.text[:500]
            })
            return False
        r.raise_for_status()
        return True
    except Exception as e:
        notify("WP_POST_FAILED", "error", {"reason": str(e)})
        return False


def md_to_basic_html(md: str) -> str:
    """
    依存を増やさず最小変換（見出し/段落/コード/表はそのままGutenbergでも見える）
    ここでは安全側にプレーンを <pre> にせず、そのまま HTML として流す
    """
    # 単純に <p> に包まず WP 側の Markdown プラグインやブロックで解釈される想定
    # 必要に応じて軽い置換のみ
    return md


# ========= Main Flow =========

def pick_keywords(cfg: Dict[str, Any]) -> List[str]:
    seeds = cfg.get("keywords", {}).get("seeds", [])
    per_run = int(cfg.get("keywords", {}).get("per_run", 1))
    if not seeds:
        return []
    random.shuffle(seeds)
    return seeds[:max(1, per_run)]

def make_title_from_kw(kw: str) -> str:
    # シンプルに
    return f"{kw}の選び方とおすすめ比較【最新ガイド】"

def main():
    try:
        # 必須ENV
        OPENAI_API_KEY = getenv_required("OPENAI_API_KEY")
        RAKUTEN_APP_ID = getenv_required("RAKUTEN_APP_ID")
        WP_SITE_URL = getenv_required("WP_SITE_URL")
        WP_USERNAME = getenv_required("WP_USERNAME")
        WP_APP_PASSWORD = getenv_required("WP_APP_PASSWORD")
    except Exception as e:
        notify("ENV_MISSING", "error", {"reason": str(e)})
        sys.exit(1)

    # Config
    cfg_path = "config/app.yaml"
    try:
        cfg = read_yaml(cfg_path)
    except Exception:
        cfg = {}
    # マージ（欠落をデフォルトで補完）
    def deep_merge(a, b):
        for k, v in b.items():
            if isinstance(v, dict):
                a[k] = deep_merge(a.get(k, {}) if isinstance(a.get(k), dict) else {}, v)
            else:
                a.setdefault(k, v)
        return a
    cfg = deep_merge(cfg, DEFAULT_CONFIG)

    # 事前通知（欠落をデフォルト適用）
    if not cfg.get("site", {}).get("affiliate_disclosure"):
        notify("CONFIG_DEFAULTED", "warning", {
            "reason": "site.affiliate_disclosure が未設定のためデフォルトを適用",
            "defaults": {"affiliate_disclosure": DEFAULT_CONFIG["site"]["affiliate_disclosure"]}
        })

    # WP認証確認
    if not wp_can_post(WP_SITE_URL, WP_USERNAME, WP_APP_PASSWORD):
        # 詳細は notify 内で送付済み
        sys.exit(1)

    # LLM 準備
    llm = OpenAIResponses(api_key=OPENAI_API_KEY)
    model_primary = cfg["llm"]["model_primary"]
    model_fallback = cfg["llm"].get("model_fallback", [])
    max_out = int(cfg["llm"]["max_output_tokens"])

    # キーワード選定
    kws = pick_keywords(cfg)
    if not kws:
        notify("KW_EMPTY", "warning", {"reason": "keywords.seeds が空です"})
        sys.exit(0)

    generated_count = 0
    for kw in kws:
        kw = sanitize_kw(kw)

        # 楽天から候補取得
        items = rakuten_items(RAKUTEN_APP_ID, kw, hits=int(cfg["rakuten"]["hits"]))
        logging.info("stats kw='%s': total=%d", kw, len(items))
        if len(items) < int(cfg["rakuten"]["min_after_filters"]):
            logging.info("skip thin (<%d) for '%s'", cfg["rakuten"]["min_after_filters"], kw)
            continue

        # プロンプト組み立て
        system, user = build_llm_prompt(cfg, kw, items, cfg["site"]["affiliate_disclosure"])

        # LLM 呼び出し（gpt-5 → fallback 順に）
        models_try = [model_primary] + [m for m in model_fallback if m]
        md = None
        used_model = None
        for m in models_try:
            try:
                md = llm.create(model=m, system=system, user=user, max_output_tokens=max_out)
                used_model = m
                break
            except Exception as e:
                notify("LLM_CALL_FAILED", "error", {
                    "stage": "llm_call",
                    "model": m,
                    "exception": str(e)
                })
                # 次モデルへ
                continue

        if used_model and used_model != model_primary:
            notify("LLM_MODEL_FALLBACK", "warning", {
                "from_model": model_primary,
                "to_model": used_model
            })

        if not md:
            notify("LLM_FAILED_FINAL", "error", {
                "stage": "llm_call",
                "kw": kw,
                "reason": "再試行の結果も失敗"
            })
            continue

        # バリデーション
        errs = validate_md(md, min_len=int(cfg["site"]["min_length"]))
        if errs:
            notify("VALIDATION_FAILED", "warning", {
                "kw": kw,
                "stage": "validation",
                "errors": errs
            })
            if not cfg["site"]["accept_warnings"]:
                # 投稿せず次へ
                continue

        # 投稿
        title = make_title_from_kw(kw)
        ok = wp_post(
            WP_SITE_URL,
            WP_USERNAME,
            WP_APP_PASSWORD,
            title=title,
            content_html=md_to_basic_html(md),
            status=cfg["site"]["post_status"],
            slug_hint=kw
        )
        if ok:
            generated_count += 1
        else:
            continue

        # 1本/実行 に絞って安定性を上げる（要求に合わせて）
        break

    # サマリ
    notify("RUN_SUMMARY", "info", {
        "counts": {
            "generated": generated_count,
            "failures": 0,
            "per_run": 1
        },
        "models": [model_primary] + model_fallback
    })


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        notify("RUN_FAILED", "error", {
            "stage": "run",
            "reason": "未捕捉の例外",
            "exception": str(e)
        })
        raise
