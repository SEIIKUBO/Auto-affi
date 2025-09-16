import os, sys, json, time, base64, logging, math
disclosure = conf["site"]["affiliate_disclosure"]
body = f"<p>{disclosure}</p><h2>{kw}の要点</h2><ul>" \
f"<li>レビュー密度（⭐×件数の対数）で上位{n}件を抽出</li>" \
f"<li>在庫や価格は変動します。購入前にリンク先で最新情報を確認してください。</li></ul>{table}" \
f"<p>※本ページはAPIデータをもとに自動生成し、転記ではなく数値集計で付加価値を加えています。</p>"
return title, body


def notify(msg):
    if not ALERT:
        return
    try:
        requests.post(ALERT, json={"content": msg}, timeout=10)
    except Exception as e:
        LOG.error(f"alert failed: {e}")



def main():
with open(sys.argv[sys.argv.index("--config")+1], "r", encoding="utf-8") as f:
conf = yaml.safe_load(f)
conf["ab_tests"]= yaml.safe_load(open("src/ab_tests.yaml","r", encoding="utf-8")) if conf.get("content",{}).get("ab_test") else {}
rules = yaml.safe_load(open(conf["review"]["rules_file"],"r", encoding="utf-8"))
cats = ensure_categories(conf["site"]["category_names"])


posted=0
for kw in conf["keywords"]["seeds"]:
if posted >= conf["site"]["posts_per_run"]: break
slug = slugify(kw)
if wp_post_exists(slug):
LOG.info(f"skip exists: {kw}")
continue
arr = rakuten_items(os.getenv("RAKUTEN_APP_ID"), kw, conf["data_sources"]["rakuten"]["endpoint"], conf["data_sources"]["rakuten"]["max_per_seed"], conf["data_sources"]["rakuten"].get("genreId"))
items = [it for it in enrich(arr)
if it["price"]>=conf["content"]["price_floor"] and it["review_avg"]>=conf["content"]["review_floor"]]
if len(items)<3:
LOG.info(f"thin content for {kw}, skipping")
continue
title, html = render_html(kw, items, conf, cats)
for bad in rules["prohibited_phrases"]:
if bad in title or bad in html:
LOG.info(f"blocked phrase: {bad} in {kw}")
continue


payload = {
"title": title,
"slug": slug,
"status": "publish",
"content": html,
"categories": cats
}
headers={"Authorization": f"Basic {b64cred(WP_USER, WP_APP_PW)}","Content-Type":"application/json"}
try:
http_json("POST", f"{WP_URL}/wp-json/wp/v2/posts", data=json.dumps(payload), headers=headers)
posted+=1
LOG.info(f"posted: {kw}")
except Exception as e:
LOG.error(f"post failed: {e}")
notify(f"[AUTO-REV] post failed for {kw}: {e}")
time.sleep(3)
LOG.info(f"done, posted={posted}")


if __name__=="__main__":
try:
main()
except Exception as e:
notify(f"[AUTO-REV] job failed: {e}")
raise
