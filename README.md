# auto-affi (WP自動投稿ボット)


運用0分：Rakuten API→付加価値集計→WordPress REST自動投稿。


### 使い方要約
1. この一式をGitHub新規リポへ
2. リポSettings→Secretsに以下を追加：
- `RAKUTEN_APP_ID`
- `WP_SITE_URL`（例: https://simple-ni-yoku.net）
- `WP_USERNAME`
- `WP_APP_PASSWORD`
- （任意）`ALERT_WEBHOOK_URL`
3. `config/app.yaml`の`keywords.seeds`編集
4. Actionsの`publish`をRun（初回）→以後はJST 06:30に自動。
