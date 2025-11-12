# GitHub Dashboard

GitHubリポジトリのPR、code frequency、contributionsなどを分析し、美しいHTMLレポートを生成するツールです。

## 特徴

- 📊 **複数リポジトリ対応**: 複数のGitHubリポジトリを指定して分析可能
- 👥 **人ごとの集計**: コントリビューターごとの統計を表示
- 📅 **月ごとの集計**: 月単位での活動状況を可視化
- 📈 **Code Frequency**: コードの追加・削除行数を時系列で表示
- 🔒 **Private Repo対応**: GitHub PATを使用してprivateリポジトリも分析可能
- 🚀 **自動更新**: GitHub Actionsで定期的にデータ収集・更新
- 📱 **レスポンシブデザイン**: モダンで美しいUI

## セットアップ

### 1. リポジトリをFork

このリポジトリをForkして、あなたのアカウントにコピーします。

### 2. リポジトリ設定

`config/repos.json` を編集して、分析したいリポジトリを指定します。

```json
{
  "repositories": [
    {
      "owner": "your-username",
      "name": "your-repo"
    },
    {
      "owner": "another-owner",
      "name": "another-repo"
    }
  ],
  "options": {
    "collect_reviews": false,
    "collect_commit_stats": true,
    "max_workers": 3
  }
}
```

#### オプション設定

- `collect_reviews`: PRのレビュー情報を収集するか（デフォルト: `false`）
  - `true`にすると収集時間が大幅に増加します
  - レビュー統計が必要な場合のみ有効化してください
- `collect_commit_stats`: コミット統計（追加・削除行数）を収集するか（デフォルト: `true`）
  - `false`にすると収集時間が短縮されますが、Code Frequencyデータが取得できません
- `max_workers`: 並列処理の最大ワーカー数（デフォルト: `3`）
  - 複数リポジトリがある場合、並列処理で高速化されます
  - レート制限に注意して調整してください
- `days`: 分析対象期間（何日前から、デフォルト: `365` = 1年）
  - 例: `180` で6ヶ月、`90` で3ヶ月
- `start_date`: 開始日をISO形式で指定（例: `"2024-01-01T00:00:00Z"`）
  - `start_date`が指定されている場合は`days`より優先されます
  - 特定の日付から分析したい場合に使用

### 3. GitHub Pagesの有効化

1. リポジトリの Settings → Pages に移動
2. Source を "GitHub Actions" に設定

### 4. GitHub Actionsの設定

#### パターンA: Publicリポジトリのみ分析する場合

`.github/workflows/analyze.yml` を使用します（デフォルトで有効）。

GitHub Actionsの自動トークン（`GITHUB_TOKEN`）が使用されます。

#### パターンB: Privateリポジトリも分析する場合

`.github/workflows/analyze_with_pat.yml` を使用します。

1. GitHub Personal Access Token (PAT) を作成
   - [GitHub Settings → Developer settings → Personal access tokens → Tokens (classic)](https://github.com/settings/tokens)
   - "Generate new token (classic)" をクリック
   - 必要な権限（スコープ）を選択:
     - `repo` (privateリポジトリにアクセスする場合、すべてのリポジトリデータにアクセス)
     - `public_repo` (publicリポジトリのみの場合)
   - トークンを生成し、**必ずコピーして保存**してください（再表示されません）
2. リポジトリの Secrets に追加
   - Settings → Secrets and variables → Actions
   - "New repository secret" をクリック
   - Name: `PAT` (注意: `GITHUB_`で始まる名前は使用できません)
   - Value: 作成したPATを貼り付け
   - "Add secret" をクリック

**重要**: GitHubのシークレット名は`GITHUB_`で始まる名前は使用できません。`PAT`という名前を使用してください。

**公開リポジトリのみの場合**: プライベートリポジトリにアクセスする必要がない場合は、`secrets.GITHUB_TOKEN`（GitHubが自動提供）を使用することもできます。その場合は、ワークフローファイルの`secrets.PAT`を`secrets.GITHUB_TOKEN`に変更してください。

### 5. 初回実行

GitHub Actionsが自動的に実行されます。または、Actionsタブから手動で実行することもできます。

## ローカルでの実行

### 必要な環境

- Python 3.11以上
- pip

### セットアップ

```bash
# 依存関係のインストール
pip install -r requirements.txt

# 環境変数の設定
export GITHUB_TOKEN=your_github_token_here

# データ収集
python scripts/collect_data.py

# HTML生成
python scripts/generate_html.py
```

生成されたHTMLは `docs/index.html` に保存されます。

## 出力されるデータ

### 統計情報

- 総PR数
- マージ済みPR数
- 総コミット数
- 追加・削除行数
- コントリビューター数

### 月ごとの分析

- PR作成数・マージ数の推移
- コントリビューター数の推移
- Code Frequency（追加・削除行数）

### コントリビューター別統計

- コミット数
- PR作成数・マージ数
- PRレビュー数
- 追加・削除行数
- 関与リポジトリ数

## ファイル構成

```
githubDash/
├── .github/
│   └── workflows/
│       ├── analyze.yml              # Publicリポジトリ用ワークフロー
│       └── analyze_with_pat.yml    # Privateリポジトリ用ワークフロー
├── config/
│   ├── repos.json                  # 分析対象リポジトリ設定
│   └── repos.json.example         # 設定ファイルの例
├── scripts/
│   ├── collect_data.py            # データ収集スクリプト
│   └── generate_html.py           # HTML生成スクリプト
├── data/
│   └── collected_data.json        # 収集されたデータ（自動生成）
├── docs/
│   └── index.html                  # 生成されたHTMLレポート（自動生成）
├── requirements.txt                # Python依存関係
├── .gitignore
└── README.md
```

## 分析期間

デフォルトで**直近1年間**（365日）のデータを分析します。

期間を変更する場合は、`config/repos.json`の`options`セクションで設定できます：

```json
{
  "options": {
    "days": 180
  }
}
```

または、特定の開始日を指定することもできます：

```json
{
  "options": {
    "start_date": "2024-01-01T00:00:00Z"
  }
}
```

- `days`: 現在から何日前まで遡るか（デフォルト: `365`）
- `start_date`: 開始日をISO形式で指定（`days`より優先）

## パフォーマンス最適化

データ収集のパフォーマンスを改善するために、以下の最適化を実装しています：

### 実装済みの最適化

1. **レビュー取得のオプション化**
   - デフォルトで無効（`collect_reviews: false`）
   - レビュー統計が必要な場合のみ有効化

2. **並列処理**
   - 複数リポジトリを並列で処理（`max_workers`で制御）
   - I/O待機が多いAPI呼び出しを効率化

3. **レート制限の監視**
   - レート制限に達する前に自動的に待機
   - レート制限情報を表示

4. **エラーハンドリングの改善**
   - 個別のエラーが全体に影響しないように改善
   - 統計取得エラーが多い場合は自動的にスキップ

### パフォーマンス向上の目安

- **レビュー取得を無効化**: 約50-70%の時間短縮
- **並列処理（3リポジトリ）**: 約60-70%の時間短縮
- **両方を適用**: 約70-80%の時間短縮

## トラブルシューティング

### GitHub Actionsが失敗する

- `GITHUB_TOKEN` または `GITHUB_PAT` が正しく設定されているか確認
- リポジトリへのアクセス権限があるか確認
- Actions のログを確認してエラー内容を確認

### データが取得できない

- リポジトリ名とオーナー名が正しいか確認
- Privateリポジトリの場合は、PATに適切な権限があるか確認
- GitHub APIのレート制限に達していないか確認

### データ収集が遅い

- `collect_reviews`を`false`に設定（デフォルト）
- `max_workers`を調整（3-5が推奨）
- レート制限に達している場合は、待機時間が発生します

### HTMLが表示されない

- GitHub Pagesが有効になっているか確認
- `docs/index.html` が生成されているか確認
- Actions のデプロイステップが成功しているか確認

## ライセンス

MIT License

## 貢献

プルリクエストやイシューの報告を歓迎します！

## 開発

### ローカル開発環境のセットアップ

```bash
# リポジトリをクローン
git clone https://github.com/your-username/githubDash.git
cd githubDash

# 仮想環境を作成（推奨）
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 依存関係をインストール
make install
# または
python3 -m pip install -r requirements.txt

# 環境変数を設定
# GitHub Personal Access Tokenを取得:
# https://github.com/settings/tokens
export GITHUB_TOKEN=your_token_here

# データ収集とHTML生成
make all
# または
make collect
make generate
```

### テスト

現在、テストスイートは含まれていません。将来的に追加予定です。

## ライセンス

MIT License - 詳細は [LICENSE](LICENSE) ファイルを参照してください。

## 作者

このプロジェクトはOSSとして公開されています。

## 謝辞

- [PyGithub](https://github.com/PyGithub/PyGithub) - GitHub APIのPythonラッパー
- [Chart.js](https://www.chartjs.org/) - 美しいチャートライブラリ
- [Jinja2](https://jinja.palletsprojects.com/) - テンプレートエンジン
