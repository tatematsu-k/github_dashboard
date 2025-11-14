#!/usr/bin/env python3
"""
GitHubリポジトリのデータを収集するスクリプト（最適化版）
PR、code frequency、contributionsなどを取得し、人ごと・月ごとに集計
"""

import json
import os
import sys
import time
import requests
from datetime import datetime, timedelta
from dateutil import parser
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from github import Github
from github import Auth
from github.GithubException import GithubException, RateLimitExceededException
import pytz

# タイムゾーン設定（JST）
JST = pytz.timezone('Asia/Tokyo')

# キャッシュスキーマのバージョン
# データ構造が変更された場合はこのバージョンを上げる
# バージョンが異なるキャッシュは無視され、全て作り直される
# Version 2: 月ごとのチャンク構造に変更（start_date/end_date付き）
CACHE_SCHEMA_VERSION = 2

# 指定日数前の日付を取得
def get_start_date(days=365):
    """指定日数前の日付を取得（デフォルト: 365日 = 1年）"""
    return datetime.now(JST) - timedelta(days=days)

# 月のキーを生成（YYYY-MM形式）
def get_month_key(date):
    if isinstance(date, str):
        date = parser.parse(date)
    return date.strftime('%Y-%m')

# 週のキーを生成（YYYY-WW形式、ISO週番号）
def get_week_key(date):
    """週のキーを生成（YYYY-WW形式、ISO週番号）"""
    if isinstance(date, str):
        date = parser.parse(date)
    # ISO週番号を取得
    year, week, _ = date.isocalendar()
    return f"{year}-W{week:02d}"

# 現在の月の開始日を取得
def get_current_month_start():
    """現在の月の開始日を取得"""
    now = datetime.now(JST)
    return datetime(now.year, now.month, 1, tzinfo=JST)

# 現在の週の開始日を取得（月曜日）
def get_current_week_start():
    """現在の週の開始日を取得（月曜日）"""
    now = datetime.now(JST)
    # 月曜日を取得（0=月曜日、6=日曜日）
    days_since_monday = now.weekday()
    return now - timedelta(days=days_since_monday)

# 月の開始日と終了日を取得
def get_month_range(year, month):
    """指定された年月の開始日と終了日を取得"""
    if month == 12:
        next_month = datetime(year + 1, 1, 1, tzinfo=JST)
    else:
        next_month = datetime(year, month + 1, 1, tzinfo=JST)
    month_start = datetime(year, month, 1, tzinfo=JST)
    month_end = next_month - timedelta(seconds=1)
    return month_start, month_end

# 月キーから年月を取得
def parse_month_key(month_key):
    """月キー（YYYY-MM）から年と月を取得"""
    year, month = map(int, month_key.split('-'))
    return year, month

# キャッシュファイルのパスを取得
def get_cache_path(owner, repo_name):
    """キャッシュファイルのパスを取得"""
    cache_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'cache')
    os.makedirs(cache_dir, exist_ok=True)
    # ファイル名に特殊文字をエスケープ
    safe_name = f"{owner}_{repo_name}".replace('/', '_').replace('\\', '_')
    return os.path.join(cache_dir, f"{safe_name}.json")

# キャッシュを読み込み
def load_cache(cache_path):
    """キャッシュを読み込み（バージョンチェック付き）"""
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cached_data = json.load(f)

            # バージョンチェック
            cached_version = cached_data.get('schema_version', 0)
            if cached_version != CACHE_SCHEMA_VERSION:
                print(f"  ⚠️  Cache schema version mismatch (cached: {cached_version}, current: {CACHE_SCHEMA_VERSION})")
                print(f"  🔄 Cache will be ignored and rebuilt")
                return None

            return cached_data
        except Exception as e:
            print(f"  ⚠️  Failed to load cache: {e}")
    return None

# 月ごとのチャンクを保存
def save_monthly_chunk(cache_path, month_key, chunk_data):
    """月ごとのチャンクを保存（個別ファイル）"""
    try:
        cache_dir = os.path.dirname(cache_path)
        os.makedirs(cache_dir, exist_ok=True)
        base_name = os.path.basename(cache_path).replace('.json', '')
        chunk_file = os.path.join(cache_dir, f"{base_name}_chunk_{month_key}.json")
        chunk_data['schema_version'] = CACHE_SCHEMA_VERSION
        with open(chunk_file, 'w', encoding='utf-8') as f:
            json.dump(chunk_data, f, indent=2, ensure_ascii=False)
        print(f"  💾 Saved chunk for {month_key} to {chunk_file}")
    except Exception as e:
        print(f"  ⚠️  Failed to save monthly chunk for {month_key}: {e}")
        import traceback
        traceback.print_exc()

# 月ごとのチャンクを読み込み
def load_monthly_chunk(cache_path, month_key):
    """月ごとのチャンクを読み込み"""
    try:
        cache_dir = os.path.dirname(cache_path)
        chunk_file = os.path.join(cache_dir, f"{os.path.basename(cache_path).replace('.json', '')}_chunk_{month_key}.json")
        if os.path.exists(chunk_file):
            with open(chunk_file, 'r', encoding='utf-8') as f:
                chunk_data = json.load(f)
            # バージョンチェック
            cached_version = chunk_data.get('schema_version', 0)
            if cached_version != CACHE_SCHEMA_VERSION:
                return None
            return chunk_data
    except Exception as e:
        pass
    return None

# PRキャッシュを更新（確定分のPRを追加保存）
def update_pr_cache(cache_path, new_prs, start_date):
    """確定分のPRをキャッシュに追加保存（処理中断に備える）"""
    try:
        # 既存のキャッシュを読み込み
        cached_data = load_cache(cache_path)
        if not cached_data:
            # キャッシュがない場合は新規作成
            cached_data = {
                'schema_version': CACHE_SCHEMA_VERSION,
                'cached_at': datetime.now(JST).isoformat(),
                'start_date': start_date.isoformat(),
                'prs': [],
                'contributions': {},
                'monthly_stats': {},
                'monthly_contributions': {},
                'code_frequency': {},
                'devin_breakdown': {}
            }

        # 新しいPRを追加（重複チェック）
        existing_pr_numbers = {pr['number'] for pr in cached_data.get('prs', [])}
        for pr in new_prs:
            if pr['number'] not in existing_pr_numbers:
                cached_data['prs'].append(pr)

        # キャッシュを保存
        cached_data['cached_at'] = datetime.now(JST).isoformat()
        cached_data['schema_version'] = CACHE_SCHEMA_VERSION
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(cached_data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"  ⚠️  Failed to update PR cache: {e}")

# キャッシュを保存（後方互換性のため残す）
def save_cache(cache_path, data):
    """キャッシュを保存（バージョン情報付き）"""
    try:
        # バージョン情報を追加
        data['schema_version'] = CACHE_SCHEMA_VERSION
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"  ⚠️  Failed to save cache: {e}")

# レート制限をチェックして必要に応じて待機
def check_rate_limit(github, resource_type='core'):
    """レート制限をチェックし、必要に応じて待機"""
    rate_limit = github.get_rate_limit()

    # PyGithubのバージョンによって構造が異なるため、両方に対応
    if hasattr(rate_limit, 'resources'):
        # 新しいバージョン
        if resource_type == 'core':
            core_limit = rate_limit.resources.core
            remaining = core_limit.remaining
            reset_time = core_limit.reset
        else:
            search_limit = rate_limit.resources.search
            remaining = search_limit.remaining
            reset_time = search_limit.reset
    else:
        # 古いバージョン（後方互換性）
        if resource_type == 'core':
            remaining = rate_limit.core.remaining
            reset_time = rate_limit.core.reset
        else:
            remaining = rate_limit.search.remaining
            reset_time = rate_limit.search.reset

    if remaining < 10:  # 残りが10未満の場合
        wait_time = (reset_time - datetime.now(JST)).total_seconds() + 10
        if wait_time > 0:
            print(f"  ⚠️  Rate limit low ({remaining} remaining). Waiting {int(wait_time)} seconds...")
            time.sleep(wait_time)

    return remaining

# GraphQLでPRとレビューを一括取得
def fetch_prs_with_graphql(github_token, owner, repo_name, start_date, collect_reviews=True):
    """GraphQL APIを使用してPRとレビュー情報を一括取得"""
    graphql_url = "https://api.github.com/graphql"
    headers = {
        "Authorization": f"Bearer {github_token}",
        "Content-Type": "application/json"
    }

    all_prs = []
    cursor = None
    has_next_page = True

    # start_dateをISO形式に変換
    start_date_str = start_date.isoformat()

    while has_next_page:
        # GraphQLクエリ
        query = """
        query($owner: String!, $repo: String!, $cursor: String) {
          repository(owner: $owner, name: $repo) {
            pullRequests(
              first: 100
              states: [OPEN, CLOSED, MERGED]
              orderBy: {field: CREATED_AT, direction: DESC}
              after: $cursor
            ) {
              nodes {
                number
                title
                author {
                  login
                }
                state
                createdAt
                mergedAt
                mergedBy {
                  login
                }
                additions
                deletions
                updatedAt
                reviews(first: 100) {
                  nodes {
                    author {
                      login
                    }
                  }
                }
              }
              pageInfo {
                hasNextPage
                endCursor
              }
            }
          }
          rateLimit {
            remaining
            resetAt
          }
        }
        """

        variables = {
            "owner": owner,
            "repo": repo_name,
            "cursor": cursor
        }

        payload = {
            "query": query,
            "variables": variables
        }

        try:
            response = requests.post(graphql_url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()

            if "errors" in data:
                print(f"  ⚠️  GraphQL errors: {data['errors']}")
                break

            repository = data.get("data", {}).get("repository")
            if not repository:
                print(f"  ⚠️  Repository not found in GraphQL response")
                break

            pull_requests = repository.get("pullRequests", {})
            nodes = pull_requests.get("nodes", [])
            page_info = pull_requests.get("pageInfo", {})

            # デバッグ: 取得したノード数を出力
            if cursor is None:
                print(f"  🔍 GraphQL: Received {len(nodes)} PR nodes from API")

            # start_dateをUTCに変換して比較（start_dateがJSTの場合はUTCに変換）
            start_date_utc = start_date
            if start_date.tzinfo == JST:
                start_date_utc = start_date.astimezone(pytz.UTC)
            elif start_date.tzinfo is None:
                start_date_utc = pytz.UTC.localize(start_date)

            if cursor is None:
                print(f"  🔍 Start date (UTC): {start_date_utc}")

            # PRを処理
            nodes_processed = 0
            nodes_skipped_before_start = 0
            nodes_added = 0

            for pr_node in nodes:
                nodes_processed += 1
                created_at_str = pr_node.get("createdAt", "")
                if not created_at_str:
                    continue

                created_at = parser.parse(created_at_str)
                # タイムゾーンがNoneの場合はUTCとして扱う
                if created_at.tzinfo is None:
                    created_at = pytz.UTC.localize(created_at)

                # created_atがstart_dateより前の場合はスキップ
                if created_at < start_date_utc:
                    nodes_skipped_before_start += 1
                    # 最初のPRがstart_dateより前の場合は、以降も全て古いPRなのでfetchを停止
                    if nodes_processed == 1:
                        print(f"  ⚠️  First PR createdAt ({created_at}) < start_date_utc ({start_date_utc}), stopping pagination")
                        has_next_page = False
                        break
                    continue

                nodes_added += 1

                # レビュアーリストを取得
                reviewers = []
                if collect_reviews:
                    reviews = pr_node.get("reviews", {}).get("nodes", [])
                    reviewer_set = set()
                    for review in reviews:
                        author = review.get("author", {})
                        if author and author.get("login"):
                            reviewer_set.add(author["login"])
                    reviewers = list(reviewer_set)

                merged_at = pr_node.get("mergedAt")
                merged_by_node = pr_node.get("mergedBy")
                merged_by = merged_by_node.get("login") if merged_by_node and merged_by_node.get("login") else None

                # デバッグ: 最初の数件のPRのmergedAt情報を出力
                if nodes_added <= 3:
                    print(f"  🔍 PR #{pr_node.get('number')}: state={pr_node.get('state')}, mergedAt={merged_at}, mergedBy={merged_by}")

                # stateを小文字に変換（MERGEDも含む）
                state = pr_node.get("state", "").lower()

                pr_data = {
                    "number": pr_node.get("number"),
                    "title": pr_node.get("title", ""),
                    "author": pr_node.get("author", {}).get("login", "unknown") if pr_node.get("author") else "unknown",
                    "state": state,
                    "created_at": created_at_str,
                    "merged_at": merged_at,
                    "merged_by": merged_by,
                    "additions": pr_node.get("additions", 0),
                    "deletions": pr_node.get("deletions", 0),
                    "reviewers": reviewers
                }

                all_prs.append(pr_data)

            # デバッグ情報を出力（最初のページのみ）
            if cursor is None:
                print(f"  🔍 Debug: Processed {nodes_processed} nodes, added {nodes_added}, skipped {nodes_skipped_before_start} (before start_date)")
                if nodes_processed > 0 and nodes_added == 0:
                    # 最初のPRの情報を出力してデバッグ
                    first_pr = nodes[0] if nodes else None
                    if first_pr:
                        first_created = parser.parse(first_pr.get("createdAt", ""))
                        if first_created.tzinfo is None:
                            first_created = pytz.UTC.localize(first_created)
                        print(f"  🔍 First PR: created_at={first_created}, start_date_utc={start_date_utc}, diff={(first_created - start_date_utc).total_seconds() / 86400:.1f} days")

            # ページネーション（既にhas_next_pageがFalseに設定されている場合は上書きしない）
            if has_next_page:
                has_next_page = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")

            # レート制限チェック
            rate_limit = data.get("data", {}).get("rateLimit", {})
            remaining = rate_limit.get("remaining", 0)
            if remaining < 10:
                reset_at = rate_limit.get("resetAt")
                if reset_at:
                    reset_time = parser.parse(reset_at)
                    wait_time = (reset_time - datetime.now(JST)).total_seconds() + 10
                    if wait_time > 0:
                        print(f"  ⚠️  GraphQL rate limit low ({remaining} remaining). Waiting {int(wait_time)} seconds...")
                        time.sleep(wait_time)

        except requests.exceptions.RequestException as e:
            print(f"  ⚠️  GraphQL request error: {e}")
            import traceback
            print(f"  ⚠️  Traceback: {traceback.format_exc()}")
            break
        except Exception as e:
            print(f"  ⚠️  GraphQL error: {e}")
            import traceback
            print(f"  ⚠️  Traceback: {traceback.format_exc()}")
            break

    print(f"  🔍 GraphQL: Total PRs collected: {len(all_prs)}")
    if len(all_prs) == 0 and cursor is None:
        print(f"  ⚠️  WARNING: No PRs collected from GraphQL API. Check if repository has PRs or if filtering is too strict.")
    return all_prs

# PRのレビューを取得（並列処理用）- 後方互換性のため残す
def fetch_pr_reviews(github, pr_number, pr):
    """PRのレビューを取得してレビュアーリストを返す"""
    try:
        check_rate_limit(github)
        reviews = pr.get_reviews()
        reviewers = []
        for review in reviews:
            if review.user and review.user.login not in reviewers:
                reviewers.append(review.user.login)
        return pr_number, reviewers
    except RateLimitExceededException:
        return pr_number, []
    except Exception:
        return pr_number, []

# 月ごとのコミットをフェッチ（並列処理用）
def fetch_month_commits(github, owner, repo_name, month_key, month_start, month_end, cache_path, use_cache=True):
    """月ごとのコミットをフェッチして結果を返す"""
    print(f"  🔄 [{owner}/{repo_name} {month_key}] Starting commit fetch...")
    try:
        check_rate_limit(github)
        repo = github.get_repo(f"{owner}/{repo_name}")
        commits = repo.get_commits(since=month_start, until=month_end)

        month_code_frequency = defaultdict(lambda: {'additions': 0, 'deletions': 0})
        month_contributions = defaultdict(lambda: {
            'commits': 0,
            'additions': 0,
            'deletions': 0
        })
        month_monthly_contributions = defaultdict(lambda: {
            'commits': 0,
            'additions': 0,
            'deletions': 0
        })
        month_contributors = set()
        month_stats_errors = 0
        month_commit_count = 0

        for commit in commits:
            month_commit_count += 1
            try:
                commit_date = commit.commit.author.date

                # 月の範囲外の場合はスキップ
                if commit_date < month_start or commit_date > month_end:
                    continue

                # 統計情報を取得
                if month_stats_errors < 10:
                    try:
                        check_rate_limit(github)
                        stats = commit.stats
                        additions = stats.additions
                        deletions = stats.deletions
                    except RateLimitExceededException:
                        print(f"  ⚠️  [{owner}/{repo_name} {month_key}] Rate limit exceeded, stopping...")
                        break
                    except Exception:
                        month_stats_errors += 1
                        additions = 0
                        deletions = 0
                else:
                    additions = 0
                    deletions = 0

                month_code_frequency[month_key]['additions'] += additions
                month_code_frequency[month_key]['deletions'] += deletions

                # コミット作成者の統計
                if commit.author:
                    author = commit.author.login
                    month_contributions[author]['commits'] += 1
                    month_contributions[author]['additions'] += additions
                    month_contributions[author]['deletions'] += deletions
                    month_monthly_contributions[author]['commits'] += 1
                    month_monthly_contributions[author]['additions'] += additions
                    month_monthly_contributions[author]['deletions'] += deletions
                    month_contributors.add(author)

            except Exception as e:
                continue

        # 月ごとのチャンクを保存（コミットが1件以上ある場合のみ）
        if use_cache and month_commit_count > 0:
            chunk_data = {
                'start_date': month_start.isoformat(),
                'end_date': month_end.isoformat(),
                'code_frequency': {month_key: dict(month_code_frequency[month_key])},
                'monthly_stats': {month_key: {
                    'prs_created': 0,
                    'prs_merged': 0,
                    'additions': month_code_frequency[month_key]['additions'],
                    'deletions': month_code_frequency[month_key]['deletions'],
                    'contributors': list(month_contributors)
                }},
                'monthly_contributions': {month_key: {k: dict(v) for k, v in month_monthly_contributions.items()}},
                'contributions': {k: dict(v) for k, v in month_contributions.items()}
            }
            save_monthly_chunk(cache_path, month_key, chunk_data)
        elif month_commit_count == 0:
            print(f"  ℹ️  [{owner}/{repo_name} {month_key}] No commits found, skipping chunk save")

        return {
            'month_key': month_key,
            'commit_count': month_commit_count,
            'code_frequency': dict(month_code_frequency),
            'contributions': {k: dict(v) for k, v in month_contributions.items()},
            'monthly_contributions': {month_key: {k: dict(v) for k, v in month_monthly_contributions.items()}},
            'contributors': list(month_contributors)
        }
    except RateLimitExceededException:
        print(f"  ⚠️  [{owner}/{repo_name} {month_key}] Rate limit exceeded")
        return None
    except Exception as e:
        print(f"  ✗ [{owner}/{repo_name} {month_key}] Error: {e}")
        return None

# リポジトリのデータを収集（最適化版）
def collect_repo_data(github, owner, repo_name, start_date, collect_reviews=False, collect_commit_stats=True, use_cache=True, max_workers=3, github_token=None):
    """リポジトリのデータを収集（PRとキャッシュチェックのみ、コミットは別途並列処理）"""
    print(f"\n{'='*60}")
    print(f"Collecting data for {owner}/{repo_name}...")
    start_time = time.time()

    cache_path = get_cache_path(owner, repo_name)
    current_month_start = get_current_month_start()
    cached_data = None

    # キャッシュから確定分を読み込み
    if use_cache:
        cached_data = load_cache(cache_path)
        if cached_data:
            print(f"  📦 Loaded cache (last updated: {cached_data.get('cached_at', 'unknown')})")

    try:
        repo = github.get_repo(f"{owner}/{repo_name}")
    except GithubException as e:
        if e.status == 401:
            print(f"Error accessing {owner}/{repo_name}: Authentication failed (401)")
            print("  The token may not have access to this repository, or the token is invalid.")
        elif e.status == 403:
            print(f"Error accessing {owner}/{repo_name}: Access forbidden (403)")
            print("  The token may not have sufficient permissions, or the repository is private and the token lacks 'repo' scope.")
        elif e.status == 404:
            print(f"Error accessing {owner}/{repo_name}: Repository not found (404)")
            print("  Please check if the repository name and owner are correct.")
        else:
            print(f"Error accessing {owner}/{repo_name}: {e}")
        return None

    data = {
        'repository': f"{owner}/{repo_name}",
        'prs': [],
        'code_frequency': defaultdict(lambda: {'additions': 0, 'deletions': 0}),
        'contributions': defaultdict(lambda: {
            'commits': 0,
            'additions': 0,
            'deletions': 0,
            'prs_created': 0,
            'prs_merged': 0,
            'prs_reviewed': 0
        }),
        'monthly_stats': defaultdict(lambda: {
            'prs_created': 0,
            'prs_merged': 0,
            'additions': 0,
            'deletions': 0,
            'contributors': set()
        }),
        'monthly_contributions': defaultdict(lambda: defaultdict(lambda: {
            'commits': 0,
            'additions': 0,
            'deletions': 0,
            'prs_created': 0,
            'prs_merged': 0,
            'prs_reviewed': 0
        })),
        'devin_breakdown': defaultdict(lambda: {
            'prs_merged': 0,
            'additions': 0,
            'deletions': 0
        })
    }

    # キャッシュから確定分のPRを読み込み
    cached_prs = []
    cached_contributions = defaultdict(lambda: {
        'commits': 0,
        'additions': 0,
        'deletions': 0,
        'prs_created': 0,
        'prs_merged': 0,
        'prs_reviewed': 0
    })
    cached_monthly_stats = defaultdict(lambda: {
        'prs_created': 0,
        'prs_merged': 0,
        'additions': 0,
        'deletions': 0,
        'contributors': set()
    })
    cached_monthly_contributions = defaultdict(lambda: defaultdict(lambda: {
        'commits': 0,
        'additions': 0,
        'deletions': 0,
        'prs_created': 0,
        'prs_merged': 0,
        'prs_reviewed': 0
    }))
    cached_code_frequency = defaultdict(lambda: {'additions': 0, 'deletions': 0})
    cached_devin_breakdown = defaultdict(lambda: {
        'prs_merged': 0,
        'additions': 0,
        'deletions': 0
    })

    if cached_data and use_cache:
        # 確定分（当月より前）のPRをキャッシュから読み込み
        # start_dateより前のデータも含まれているか確認
        cache_has_old_data = False
        for cached_pr in cached_data.get('prs', []):
            pr_created = parser.parse(cached_pr['created_at'])
            if pr_created < current_month_start:
                cached_prs.append(cached_pr)
                # start_dateより前のデータがあるか確認
                if pr_created >= start_date:
                    cache_has_old_data = True

        # キャッシュにstart_dateより前のデータがない場合は、通常通り取得する
        if not cache_has_old_data and len(cached_prs) > 0:
            # キャッシュの最も古いPRの日付を確認
            oldest_cached_pr_date = min(parser.parse(pr['created_at']) for pr in cached_prs)
            if oldest_cached_pr_date > start_date:
                print(f"  ⚠️  Cache doesn't contain data before {start_date.strftime('%Y-%m-%d')}, will fetch from API")
                # キャッシュをクリアして通常通り取得
                cached_prs = []
                cached_contributions = defaultdict(lambda: {
                    'commits': 0,
                    'additions': 0,
                    'deletions': 0,
                    'prs_created': 0,
                    'prs_merged': 0,
                    'prs_reviewed': 0
                })
                cached_monthly_stats = defaultdict(lambda: {
                    'prs_created': 0,
                    'prs_merged': 0,
                    'additions': 0,
                    'deletions': 0,
                    'contributors': set()
                })
                cached_monthly_contributions = defaultdict(lambda: defaultdict(lambda: {
                    'commits': 0,
                    'additions': 0,
                    'deletions': 0,
                    'prs_created': 0,
                    'prs_merged': 0,
                    'prs_reviewed': 0
                }))
                cached_code_frequency = defaultdict(lambda: {'additions': 0, 'deletions': 0})
                cached_devin_breakdown = defaultdict(lambda: {
                    'prs_merged': 0,
                    'additions': 0,
                    'deletions': 0
                })

        # キャッシュがクリアされていない場合のみ統計を読み込み
        if len(cached_prs) > 0:
            # 確定分の統計をキャッシュから読み込み
            for month, stats in cached_data.get('monthly_stats', {}).items():
                month_date = parser.parse(f"{month}-01")
                if month_date.tzinfo is None:
                    month_date = JST.localize(month_date)
                if month_date < current_month_start:
                    cached_monthly_stats[month] = stats.copy()
                    if isinstance(stats.get('contributors'), int):
                        cached_monthly_stats[month]['contributors'] = set()

            for month, freq in cached_data.get('code_frequency', {}).items():
                month_date = parser.parse(f"{month}-01")
                if month_date.tzinfo is None:
                    month_date = JST.localize(month_date)
                if month_date < current_month_start:
                    cached_code_frequency[month] = freq.copy()

            # 確定分のコントリビューター統計をキャッシュから読み込み
            for contributor, stats in cached_data.get('contributions', {}).items():
                cached_contributions[contributor] = stats.copy()

            # 月別コントリビューター統計をキャッシュから読み込み
            for month, contributors in cached_data.get('monthly_contributions', {}).items():
                month_date = parser.parse(f"{month}-01")
                if month_date.tzinfo is None:
                    month_date = JST.localize(month_date)
                if month_date < current_month_start:
                    for contributor, stats in contributors.items():
                        cached_monthly_contributions[month][contributor] = stats.copy()

            # devin-botの内訳も読み込み
            for contributor, breakdown in cached_data.get('devin_breakdown', {}).items():
                cached_devin_breakdown[contributor] = breakdown.copy()

            print(f"  📦 Using {len(cached_prs)} cached PRs (before {current_month_start.strftime('%Y-%m')})")
        else:
            print(f"  📦 Cache cleared, will fetch all data from API")

    # PRデータを収集（GraphQLを使用するか、従来のREST APIを使用するか）
    use_graphql = os.getenv('USE_GRAPHQL', 'true').lower() == 'true'

    if use_graphql and github_token:
        # GraphQLでPRとレビューを一括取得
        print(f"  🔄 Fetching PRs with GraphQL...")
        print(f"  📅 Start date: {start_date} (timezone: {start_date.tzinfo})")
        try:
            graphql_prs = fetch_prs_with_graphql(github_token, owner, repo_name, start_date, collect_reviews)
            print(f"  ✓ Fetched {len(graphql_prs)} PRs with GraphQL")
            if len(graphql_prs) > 0:
                print(f"  📊 Sample PR: #{graphql_prs[0].get('number')} created_at={graphql_prs[0].get('created_at')}, state={graphql_prs[0].get('state')}")
            else:
                print(f"  ⚠️  No PRs fetched - checking if repository has PRs...")

            # GraphQLで取得したPRを処理
            pr_count = 0
            new_pr_count = 0
            pr_data_map = {}
            determined_prs = []
            last_cache_save_time = time.time()
            cache_save_interval = 30

            # start_dateをUTCに変換（処理ループ内で使用）
            start_date_utc_for_processing = start_date
            if start_date.tzinfo == JST:
                start_date_utc_for_processing = start_date.astimezone(pytz.UTC)
            elif start_date.tzinfo is None:
                start_date_utc_for_processing = pytz.UTC.localize(start_date)

            for pr_data in graphql_prs:
                pr_created = parser.parse(pr_data['created_at'])
                # タイムゾーンがNoneの場合はUTCとして扱う
                if pr_created.tzinfo is None:
                    pr_created = pytz.UTC.localize(pr_created)

                is_determined = pr_created < current_month_start

                if is_determined:
                    if len(cached_prs) > 0:
                        continue
                    if pr_created < start_date_utc_for_processing:
                        continue

                # 確定分のPRは順次保存用リストに追加
                if is_determined:
                    determined_prs.append(pr_data)

                # 確定分のPRを定期的にキャッシュに保存
                if is_determined and use_cache and len(determined_prs) > 0:
                    current_time = time.time()
                    if current_time - last_cache_save_time >= cache_save_interval:
                        update_pr_cache(cache_path, determined_prs, start_date)
                        print(f"  💾 Saved {len(determined_prs)} determined PRs to cache (interim save)")
                        determined_prs = []
                        last_cache_save_time = current_time

                pr_data_map[pr_data['number']] = pr_data
                data['prs'].append(pr_data)
                pr_count += 1
                new_pr_count += 1

                # 統計を更新
                month_key = get_month_key(pr_data['created_at'])
                data['monthly_stats'][month_key]['prs_created'] += 1
                if pr_data['merged_at']:
                    merge_month = get_month_key(pr_data['merged_at'])
                    data['monthly_stats'][merge_month]['prs_merged'] += 1

                # devin-ai-integration[bot]のPRがマージされた場合の処理
                author = pr_data['author']
                is_devin_bot = author == 'devin-ai-integration[bot]'
                merged_by = pr_data.get('merged_by')

                if is_devin_bot and pr_data['merged_at'] and merged_by:
                    merger = merged_by
                    data['contributions'][merger]['prs_merged'] += 1
                    data['contributions'][merger]['additions'] += pr_data['additions']
                    data['contributions'][merger]['deletions'] += pr_data['deletions']
                    merge_month = get_month_key(pr_data['merged_at'])
                    data['monthly_contributions'][merge_month][merger]['prs_merged'] += 1
                    data['monthly_contributions'][merge_month][merger]['additions'] += pr_data['additions']
                    data['monthly_contributions'][merge_month][merger]['deletions'] += pr_data['deletions']

                    if merger not in data['devin_breakdown']:
                        data['devin_breakdown'][merger] = {
                            'prs_merged': 0,
                            'additions': 0,
                            'deletions': 0
                        }
                    data['devin_breakdown'][merger]['prs_merged'] += 1
                    data['devin_breakdown'][merger]['additions'] += pr_data['additions']
                    data['devin_breakdown'][merger]['deletions'] += pr_data['deletions']
                else:
                    data['contributions'][author]['prs_created'] += 1
                    data['contributions'][author]['additions'] += pr_data['additions']
                    data['contributions'][author]['deletions'] += pr_data['deletions']
                    data['monthly_contributions'][month_key][author]['prs_created'] += 1
                    data['monthly_contributions'][month_key][author]['additions'] += pr_data['additions']
                    data['monthly_contributions'][month_key][author]['deletions'] += pr_data['deletions']

                    if pr_data['merged_at']:
                        merge_month = get_month_key(pr_data['merged_at'])
                        data['contributions'][author]['prs_merged'] += 1
                        data['monthly_contributions'][merge_month][author]['prs_merged'] += 1

                # レビュアーの統計を更新
                if collect_reviews and pr_data.get('reviewers'):
                    for reviewer in pr_data['reviewers']:
                        data['contributions'][reviewer]['prs_reviewed'] += 1
                        data['monthly_contributions'][month_key][reviewer]['prs_reviewed'] += 1

            # 残りの確定分のPRをキャッシュに保存
            if use_cache and len(determined_prs) > 0:
                update_pr_cache(cache_path, determined_prs, start_date)
                print(f"  💾 Saved {len(determined_prs)} determined PRs to cache (final save)")

            print(f"  ✓ Collected {new_pr_count} new PRs (total: {pr_count + len(cached_prs)} with cache)")

            # キャッシュから読み込んだPRを追加
            data['prs'].extend(cached_prs)

            # キャッシュから読み込んだPRの統計も更新
            for cached_pr in cached_prs:
                month_key = get_month_key(cached_pr['created_at'])
                data['monthly_stats'][month_key]['prs_created'] += 1
                if cached_pr.get('merged_at'):
                    merge_month = get_month_key(cached_pr['merged_at'])
                    data['monthly_stats'][merge_month]['prs_merged'] += 1

                author = cached_pr.get('author', 'unknown')
                is_devin_bot = author == 'devin-ai-integration[bot]'
                merged_by = cached_pr.get('merged_by')

                # devin-ai-integration[bot]のPRがマージされた場合の処理
                if is_devin_bot and cached_pr.get('merged_at') and merged_by:
                    merger = merged_by
                    data['contributions'][merger]['prs_merged'] += 1
                    data['contributions'][merger]['additions'] += cached_pr.get('additions', 0)
                    data['contributions'][merger]['deletions'] += cached_pr.get('deletions', 0)
                    merge_month = get_month_key(cached_pr['merged_at'])
                    data['monthly_contributions'][merge_month][merger]['prs_merged'] += 1
                    data['monthly_contributions'][merge_month][merger]['additions'] += cached_pr.get('additions', 0)
                    data['monthly_contributions'][merge_month][merger]['deletions'] += cached_pr.get('deletions', 0)

                    if merger not in data['devin_breakdown']:
                        data['devin_breakdown'][merger] = {
                            'prs_merged': 0,
                            'additions': 0,
                            'deletions': 0
                        }
                    data['devin_breakdown'][merger]['prs_merged'] += 1
                    data['devin_breakdown'][merger]['additions'] += cached_pr.get('additions', 0)
                    data['devin_breakdown'][merger]['deletions'] += cached_pr.get('deletions', 0)
                else:
                    # 通常のPRの統計
                    data['contributions'][author]['prs_created'] += 1
                    data['contributions'][author]['additions'] += cached_pr.get('additions', 0)
                    data['contributions'][author]['deletions'] += cached_pr.get('deletions', 0)
                    data['monthly_contributions'][month_key][author]['prs_created'] += 1
                    data['monthly_contributions'][month_key][author]['additions'] += cached_pr.get('additions', 0)
                    data['monthly_contributions'][month_key][author]['deletions'] += cached_pr.get('deletions', 0)

                    if cached_pr.get('merged_at'):
                        merge_month = get_month_key(cached_pr['merged_at'])
                        data['contributions'][author]['prs_merged'] += 1
                        data['monthly_contributions'][merge_month][author]['prs_merged'] += 1

                if collect_reviews and cached_pr.get('reviewers'):
                    for reviewer in cached_pr['reviewers']:
                        data['contributions'][reviewer]['prs_reviewed'] += 1
                        data['monthly_contributions'][month_key][reviewer]['prs_reviewed'] += 1

            # GraphQLで取得した場合は、従来のREST API処理をスキップ
            # （この後、コミット統計の処理に進む）
        except Exception as e:
            print(f"  ⚠️  GraphQL fetch failed, falling back to REST API: {e}")
            use_graphql = False  # フォールバック

    if not use_graphql:
        # 従来のREST APIを使用
        try:
            check_rate_limit(github)
            prs = repo.get_pulls(state='all', sort='updated', direction='desc')
        except Exception as e:
            print(f"  ✗ Error getting PRs: {e}")
            prs = []

        if prs:  # prsが空でない場合のみ処理
            try:
                pr_count = 0
                new_pr_count = 0
                last_progress_time = time.time()
                progress_interval = 60  # 60秒ごとに進捗表示
                total_checked = 0  # start_date以降のPRをチェックした数

                # PRの基本情報を先に収集（レビューは後で並列取得）
                prs_to_fetch_reviews = []  # レビュー取得が必要なPRのリスト
                pr_data_map = {}  # PR番号 -> PRデータのマッピング
                determined_prs = []  # 確定分のPR（順次保存用）
                last_cache_save_time = time.time()
                cache_save_interval = 30  # 30秒ごとに確定分のPRをキャッシュに保存

                for pr in prs:
                    # 直近1年間のPRのみ処理
                    if pr.updated_at < start_date:
                        break

                    total_checked += 1  # start_date以降のPRをチェック

                    # 確定分（当月より前）はスキップ（キャッシュから読み込む）
                    # ただし、キャッシュがない場合や、start_dateより前のデータでキャッシュにない場合は取得する
                    pr_created = pr.created_at
                    is_determined = pr_created < current_month_start
                    if is_determined:
                        # キャッシュがある場合はスキップ
                        if len(cached_prs) > 0:
                            continue
                        # キャッシュがない場合は、start_date以降のデータを取得
                        if pr_created < start_date:
                            continue

                    # マージした人を取得（devin-botのPRの場合に使用）
                    merged_by = None
                    if pr.merged_at and pr.merged_by:
                        merged_by = pr.merged_by.login

                    pr_data = {
                        'number': pr.number,
                        'title': pr.title,
                        'author': pr.user.login if pr.user else 'unknown',
                        'state': pr.state,
                        'created_at': pr.created_at.isoformat(),
                        'merged_at': pr.merged_at.isoformat() if pr.merged_at else None,
                        'merged_by': merged_by,
                        'additions': pr.additions,
                        'deletions': pr.deletions,
                        'reviewers': []
                    }

                    # レビュー取得が必要な場合は後で並列処理するため、リストに追加
                    if collect_reviews:
                        prs_to_fetch_reviews.append((pr.number, pr))

                    pr_data_map[pr.number] = pr_data
                    data['prs'].append(pr_data)
                    pr_count += 1
                    new_pr_count += 1

                    # 確定分のPRは順次保存用リストに追加
                    if is_determined:
                        determined_prs.append(pr_data)

                    # 確定分のPRを定期的にキャッシュに保存（処理中断に備える）
                    if is_determined and use_cache and len(determined_prs) > 0:
                        current_time = time.time()
                        if current_time - last_cache_save_time >= cache_save_interval:
                            update_pr_cache(cache_path, determined_prs, start_date)
                            print(f"  💾 Saved {len(determined_prs)} determined PRs to cache (interim save)")
                            determined_prs = []  # 保存済みのPRはクリア
                            last_cache_save_time = current_time

                    # 進捗表示（1分ごと）
                    current_time = time.time()
                    if current_time - last_progress_time >= progress_interval:
                        elapsed = int(current_time - start_time)
                        elapsed_min = elapsed // 60
                        elapsed_sec = elapsed % 60

                        # 処理速度を計算（PR/秒）
                        if elapsed > 0:
                            rate = new_pr_count / elapsed
                            # 進捗率を計算（total_checkedから推定）
                            if total_checked > 0:
                                progress_pct = min(100, int((new_pr_count / total_checked) * 100))
                                # 残り時間を推定
                                remaining = max(0, total_checked - new_pr_count)
                                if rate > 0 and remaining > 0:
                                    eta_seconds = int(remaining / rate)
                                    eta_min = eta_seconds // 60
                                    eta_sec = eta_seconds % 60
                                    if elapsed_min > 0:
                                        print(f"  ⏳ Progress: {new_pr_count}/{total_checked} PRs collected ({progress_pct}%, elapsed: {elapsed_min}m {elapsed_sec}s, rate: {rate:.2f} PRs/s, ETA: ~{eta_min}m {eta_sec}s)")
                                    else:
                                        print(f"  ⏳ Progress: {new_pr_count}/{total_checked} PRs collected ({progress_pct}%, elapsed: {elapsed_sec}s, rate: {rate:.2f} PRs/s, ETA: ~{eta_sec}s)")
                                else:
                                    if elapsed_min > 0:
                                        print(f"  ⏳ Progress: {new_pr_count}/{total_checked} PRs collected ({progress_pct}%, elapsed: {elapsed_min}m {elapsed_sec}s, rate: {rate:.2f} PRs/s)")
                                    else:
                                        print(f"  ⏳ Progress: {new_pr_count}/{total_checked} PRs collected ({progress_pct}%, elapsed: {elapsed_sec}s, rate: {rate:.2f} PRs/s)")
                            else:
                                # total_checkedが0の場合は従来の表示
                                if rate > 0:
                                    if elapsed_min > 0:
                                        print(f"  ⏳ Progress: {new_pr_count} PRs collected (elapsed: {elapsed_min}m {elapsed_sec}s, rate: {rate:.2f} PRs/s)")
                                    else:
                                        print(f"  ⏳ Progress: {new_pr_count} PRs collected (elapsed: {elapsed_sec}s, rate: {rate:.2f} PRs/s)")
                                else:
                                    if elapsed_min > 0:
                                        print(f"  ⏳ Progress: {new_pr_count} PRs collected (elapsed: {elapsed_min}m {elapsed_sec}s)")
                                    else:
                                        print(f"  ⏳ Progress: {new_pr_count} PRs collected (elapsed: {elapsed_sec}s)")
                        else:
                            print(f"  ⏳ Progress: {new_pr_count} PRs collected")
                        last_progress_time = current_time

                    # 月ごとの統計
                    month_key = get_month_key(pr.created_at)
                    data['monthly_stats'][month_key]['prs_created'] += 1
                    if pr.merged_at:
                        merge_month = get_month_key(pr.merged_at)
                        data['monthly_stats'][merge_month]['prs_merged'] += 1

                    # devin-ai-integration[bot]のPRがマージされた場合、実績をマージした人に計上
                    author = pr.user.login if pr.user else 'unknown'
                    is_devin_bot = author == 'devin-ai-integration[bot]'

                    if is_devin_bot and pr.merged_at and merged_by:
                        # devin-botのPRがマージされた場合、マージした人に実績を計上
                        merger = merged_by
                        data['contributions'][merger]['prs_merged'] += 1
                        data['contributions'][merger]['additions'] += pr.additions
                        data['contributions'][merger]['deletions'] += pr.deletions
                        data['monthly_stats'][merge_month]['contributors'].add(merger)
                        # 月別統計
                        data['monthly_contributions'][merge_month][merger]['prs_merged'] += 1
                        data['monthly_contributions'][merge_month][merger]['additions'] += pr.additions
                        data['monthly_contributions'][merge_month][merger]['deletions'] += pr.deletions

                        # devin-botの内訳も記録（括弧書き表示用）
                        if 'devin_breakdown' not in data:
                            data['devin_breakdown'] = defaultdict(lambda: {
                                'prs_merged': 0,
                                'additions': 0,
                                'deletions': 0
                            })
                        data['devin_breakdown'][merger]['prs_merged'] += 1
                        data['devin_breakdown'][merger]['additions'] += pr.additions
                        data['devin_breakdown'][merger]['deletions'] += pr.deletions
                    else:
                        # 通常のPRの統計
                        if pr.user:
                            data['contributions'][author]['prs_created'] += 1
                            if pr.merged_at:
                                data['contributions'][author]['prs_merged'] += 1
                            data['contributions'][author]['additions'] += pr.additions
                            data['contributions'][author]['deletions'] += pr.deletions
                            data['monthly_stats'][month_key]['contributors'].add(author)
                            # 月別統計
                            data['monthly_contributions'][month_key][author]['prs_created'] += 1
                            data['monthly_contributions'][month_key][author]['additions'] += pr.additions
                            data['monthly_contributions'][month_key][author]['deletions'] += pr.deletions
                            if pr.merged_at:
                                data['monthly_contributions'][merge_month][author]['prs_merged'] += 1

                # 残りの確定分のPRをキャッシュに保存
                if use_cache and len(determined_prs) > 0:
                    update_pr_cache(cache_path, determined_prs, start_date)
                    print(f"  💾 Saved {len(determined_prs)} determined PRs to cache (final save)")

                print(f"  ✓ Collected {new_pr_count} new PRs (total: {pr_count + len(cached_prs)} with cache)")

                # レビューを並列取得（レビュー取得が有効な場合、PRの基本情報収集後に実行）
                if collect_reviews and prs_to_fetch_reviews:
                    print(f"  🔄 Fetching reviews for {len(prs_to_fetch_reviews)} PRs in parallel...")
                    review_workers = min(max_workers, len(prs_to_fetch_reviews))
                    review_start_time = time.time()
                    with ThreadPoolExecutor(max_workers=review_workers) as executor:
                        future_to_pr = {
                            executor.submit(fetch_pr_reviews, github, pr_number, pr): pr_number
                            for pr_number, pr in prs_to_fetch_reviews
                        }

                        completed = 0
                        for future in as_completed(future_to_pr):
                            pr_number = future_to_pr[future]
                            completed += 1
                            try:
                                _, reviewers = future.result()
                                if pr_number in pr_data_map:
                                    pr_data_map[pr_number]['reviewers'] = reviewers
                                    # レビュアーの統計を更新
                                    pr_data = pr_data_map[pr_number]
                                    month_key = get_month_key(pr_data['created_at'])
                                    for reviewer in reviewers:
                                        data['contributions'][reviewer]['prs_reviewed'] += 1
                                        data['monthly_contributions'][month_key][reviewer]['prs_reviewed'] += 1
                            except Exception as e:
                                print(f"  ⚠️  Error fetching reviews for PR #{pr_number}: {e}")
                                if pr_number in pr_data_map:
                                    pr_data_map[pr_number]['reviewers'] = []

                            # 進捗表示（10件ごと）
                            if completed % 10 == 0:
                                elapsed = time.time() - review_start_time
                                rate = completed / elapsed if elapsed > 0 else 0
                                remaining = len(prs_to_fetch_reviews) - completed
                                eta = remaining / rate if rate > 0 else 0
                                print(f"  ⏳ Reviews: {completed}/{len(prs_to_fetch_reviews)} ({rate:.1f} PRs/s, ETA: {int(eta)}s)")

                    review_elapsed = time.time() - review_start_time
                    print(f"  ✓ Fetched reviews for {len(prs_to_fetch_reviews)} PRs in {review_elapsed:.1f}s")

            except RateLimitExceededException:
                print(f"  ⚠️  Rate limit exceeded while fetching PRs")
            except Exception as e:
                print(f"  ✗ Error collecting PRs: {e}")

        # キャッシュから読み込んだPRを追加
        data['prs'].extend(cached_prs)

    # Code frequencyデータの収集はmain関数で並列処理されるため、ここではキャッシュから読み込むだけ
    # コミット統計の収集はmain関数で月ごとに並列処理される

    # キャッシュから読み込んだ統計をマージ
    for contributor, stats in cached_contributions.items():
        for key, value in stats.items():
            data['contributions'][contributor][key] += value

    for month, stats in cached_monthly_stats.items():
        for key, value in stats.items():
            if key == 'contributors':
                if isinstance(value, set):
                    data['monthly_stats'][month]['contributors'].update(value)
                else:
                    # 数値の場合は無視（後で計算し直す）
                    pass
            else:
                data['monthly_stats'][month][key] += value

    # 月別コントリビューター統計をマージ
    for month, contributors in cached_monthly_contributions.items():
        for contributor, stats in contributors.items():
            for key, value in stats.items():
                data['monthly_contributions'][month][contributor][key] += value

    for contributor, breakdown in cached_devin_breakdown.items():
        if 'devin_breakdown' not in data:
            data['devin_breakdown'] = defaultdict(lambda: {
                'prs_merged': 0,
                'additions': 0,
                'deletions': 0
            })
        for key, value in breakdown.items():
            data['devin_breakdown'][contributor][key] += value

    # セットをリストに変換
    for month_key in data['monthly_stats']:
        if isinstance(data['monthly_stats'][month_key]['contributors'], set):
            data['monthly_stats'][month_key]['contributors'] = len(data['monthly_stats'][month_key]['contributors'])

    # 辞書を通常の辞書に変換
    data['code_frequency'] = dict(data['code_frequency'])
    data['contributions'] = dict(data['contributions'])
    data['monthly_stats'] = dict(data['monthly_stats'])
    # monthly_contributionsを通常の辞書に変換
    monthly_contributions_dict = {}
    for month, contributors in data['monthly_contributions'].items():
        monthly_contributions_dict[month] = dict(contributors)
    data['monthly_contributions'] = monthly_contributions_dict
    if 'devin_breakdown' in data:
        data['devin_breakdown'] = dict(data['devin_breakdown'])

    # キャッシュを保存（次回のために）
    if use_cache:
        cache_data = {
            'cached_at': datetime.now(JST).isoformat(),
            'start_date': start_date.isoformat(), # キャッシュの開始日を保存
            'repository': data['repository'],
            'prs': data['prs'],
            'contributions': data['contributions'],
            'monthly_stats': data['monthly_stats'],
            'monthly_contributions': data.get('monthly_contributions', {}),
            'code_frequency': data['code_frequency'],
            'devin_breakdown': data.get('devin_breakdown', {})
        }
        save_cache(cache_path, cache_data)
        print(f"  💾 Cache saved for next run")

    elapsed_time = time.time() - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)
    if minutes > 0:
        print(f"  ✓ Completed in {minutes}m {seconds}s")
    else:
        print(f"  ✓ Completed in {elapsed_time:.1f}s")
    print(f"{'='*60}\n")

    return data

def main():
    # GitHub PATを取得
    github_token = os.getenv('GITHUB_TOKEN')
    if not github_token:
        print("Error: GITHUB_TOKEN environment variable is not set")
        print("Please set GITHUB_TOKEN environment variable or use GitHub Actions secrets")
        print("You can create a token at: https://github.com/settings/tokens")
        sys.exit(1)

    # APIレート制限を考慮してGithubオブジェクトを作成（新しいAPIを使用）
    auth = Auth.Token(github_token)
    github = Github(auth=auth, per_page=100)

    # リポジトリ設定を読み込み
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'repos.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    # 設定オプションを読み込み
    options = config.get('options', {})
    collect_reviews = options.get('collect_reviews', False)
    collect_commit_stats = options.get('collect_commit_stats', True)
    max_workers = options.get('max_workers', 3)
    use_cache = options.get('use_cache', True)

    # 対象期間の設定
    # days: 何日前から（デフォルト: 365日 = 1年）
    # start_date: 開始日をISO形式で指定（例: "2024-01-01T00:00:00Z"）
    # start_dateが指定されている場合は優先
    if 'start_date' in options:
        try:
            start_date = parser.parse(options['start_date'])
            if start_date.tzinfo is None:
                start_date = JST.localize(start_date)
            print(f"Using custom start date: {start_date.isoformat()}")
        except Exception as e:
            print(f"Warning: Invalid start_date format, using days option instead: {e}")
            days = options.get('days', 365)
            start_date = get_start_date(days)
    else:
        days = options.get('days', 365)
        start_date = get_start_date(days)
        print(f"Using {days} days period (from {start_date.isoformat()})")

    all_data = []

    repos = config['repositories']
    total_repos = len(repos)

    # 最初のリポジトリで認証を確認
    if repos:
        first_repo = repos[0]
        try:
            test_repo = github.get_repo(f"{first_repo['owner']}/{first_repo['name']}")
            print(f"✓ Authentication successful (testing with {first_repo['owner']}/{first_repo['name']})")

            # レート制限情報を表示
            rate_limit = github.get_rate_limit()
            if hasattr(rate_limit, 'resources'):
                core_limit = rate_limit.resources.core
                print(f"Rate limit: {core_limit.remaining}/{core_limit.limit} (resets at {core_limit.reset})")
            else:
                print(f"Rate limit: {rate_limit.core.remaining}/{rate_limit.core.limit} (resets at {rate_limit.core.reset})")
        except GithubException as e:
            if e.status == 401:
                print("Error: Invalid GitHub token (401 Unauthorized)")
                print("Please check your GITHUB_TOKEN:")
                print("1. Token is valid and not expired")
                print("2. Token has necessary permissions (repo scope for private repos)")
                print("3. Token is correctly set in environment variable")
                print("You can create a new token at: https://github.com/settings/tokens")
            elif e.status == 403:
                print("Error: Access forbidden (403 Forbidden)")
                print("Please check your GITHUB_TOKEN:")
                print("1. Token has sufficient permissions (repo scope for private repos)")
                print("2. Token is not rate limited")
                print("3. Token has access to the requested resources")
                print(f"4. Token has access to {first_repo['owner']}/{first_repo['name']}")
                print("You can check token permissions at: https://github.com/settings/tokens")
            else:
                print(f"Error accessing {first_repo['owner']}/{first_repo['name']}: {e}")
            sys.exit(1)

    print(f"\n{'='*60}")
    print(f"Processing {total_repos} repository/repositories...")
    print(f"Options: collect_reviews={collect_reviews}, collect_commit_stats={collect_commit_stats}, max_workers={max_workers}, use_cache={use_cache}")
    print(f"Period: {start_date.isoformat()} to {datetime.now(JST).isoformat()}")
    print(f"{'='*60}\n")

    # まず各リポジトリのPRデータを収集（並列処理）
    repo_data_map = {}
    if total_repos > 1 and max_workers > 1:
        print(f"Using parallel processing for PRs (max {max_workers} workers)...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_repo = {
                executor.submit(
                    collect_repo_data,
                    github,
                    repo_config['owner'],
                    repo_config['name'],
                    start_date,
                    collect_reviews,
                    collect_commit_stats,
                    use_cache,
                    max_workers,
                    github_token
                ): repo_config
                for repo_config in repos
            }

            for future in as_completed(future_to_repo):
                repo_config = future_to_repo[future]
                try:
                    repo_data = future.result()
                    if repo_data:
                        repo_key = f"{repo_config['owner']}/{repo_config['name']}"
                        repo_data_map[repo_key] = repo_data
                except Exception as e:
                    print(f"Error processing {repo_config['owner']}/{repo_config['name']}: {e}")
    else:
        for repo_config in repos:
            owner = repo_config['owner']
            name = repo_config['name']
            repo_data = collect_repo_data(github, owner, name, start_date, collect_reviews, collect_commit_stats, use_cache, max_workers, github_token)
            if repo_data:
                repo_key = f"{owner}/{name}"
                repo_data_map[repo_key] = repo_data

    # コミット統計を収集する場合、月ごとの並列処理を実行
    if collect_commit_stats:
        # 全リポジトリ×全月のタスクリストを作成
        month_tasks = []
        for repo_key, repo_data in repo_data_map.items():
            owner, repo_name = repo_key.split('/')
            cache_path = get_cache_path(owner, repo_name)

            # 必要な月のリストを生成
            months_to_process = []
            current = datetime(start_date.year, start_date.month, 1, tzinfo=JST)
            now = datetime.now(JST)
            while current <= now:
                month_key = current.strftime('%Y-%m')
                year, month = current.year, current.month
                month_start, month_end = get_month_range(year, month)
                months_to_process.append((month_key, month_start, month_end))
                if month == 12:
                    current = datetime(year + 1, 1, 1, tzinfo=JST)
                else:
                    current = datetime(year, month + 1, 1, tzinfo=JST)

            # 各月のキャッシュをチェックして、完全なキャッシュを読み込む
            for month_key, month_start, month_end in months_to_process:
                chunk = load_monthly_chunk(cache_path, month_key) if use_cache else None
                if chunk:
                    chunk_start = parser.parse(chunk.get('start_date', ''))
                    chunk_end = parser.parse(chunk.get('end_date', ''))
                    if chunk_start <= month_start and chunk_end >= month_end:
                        # 完全なキャッシュがある場合は読み込む
                        print(f"  📦 Using cached chunk for {owner}/{repo_name} {month_key}")
                        if 'code_frequency' in chunk:
                            if month_key in chunk['code_frequency']:
                                repo_data['code_frequency'][month_key] = chunk['code_frequency'][month_key].copy()
                        if 'monthly_stats' in chunk:
                            if month_key in chunk['monthly_stats']:
                                stats = chunk['monthly_stats'][month_key]
                                if month_key not in repo_data['monthly_stats']:
                                    repo_data['monthly_stats'][month_key] = {
                                        'prs_created': 0,
                                        'prs_merged': 0,
                                        'additions': 0,
                                        'deletions': 0,
                                        'contributors': set()
                                    }
                                # contributorsが既に数値の場合はsetに変換
                                if isinstance(repo_data['monthly_stats'][month_key].get('contributors'), int):
                                    repo_data['monthly_stats'][month_key]['contributors'] = set()
                                if isinstance(stats.get('contributors'), list):
                                    repo_data['monthly_stats'][month_key]['contributors'].update(stats.get('contributors', []))
                                elif isinstance(stats.get('contributors'), int):
                                    # 既に数値の場合はスキップ（後で計算）
                                    pass
                                repo_data['monthly_stats'][month_key]['additions'] += stats.get('additions', 0)
                                repo_data['monthly_stats'][month_key]['deletions'] += stats.get('deletions', 0)
                        if 'monthly_contributions' in chunk:
                            if month_key in chunk['monthly_contributions']:
                                if month_key not in repo_data['monthly_contributions']:
                                    repo_data['monthly_contributions'][month_key] = defaultdict(lambda: {
                                        'commits': 0, 'additions': 0, 'deletions': 0, 'prs_created': 0, 'prs_merged': 0, 'prs_reviewed': 0
                                    })
                                for contributor, stats in chunk['monthly_contributions'][month_key].items():
                                    if not contributor:  # Noneや空文字列をスキップ
                                        continue
                                    if not isinstance(stats, dict):
                                        continue
                                    # contributorキーが存在しない場合は初期化
                                    if contributor not in repo_data['monthly_contributions'][month_key]:
                                        repo_data['monthly_contributions'][month_key][contributor] = {
                                            'commits': 0, 'additions': 0, 'deletions': 0, 'prs_created': 0, 'prs_merged': 0, 'prs_reviewed': 0
                                        }
                                    for key, value in stats.items():
                                        # 存在しないキーの場合は初期化してから加算
                                        if key not in repo_data['monthly_contributions'][month_key][contributor]:
                                            repo_data['monthly_contributions'][month_key][contributor][key] = 0
                                        repo_data['monthly_contributions'][month_key][contributor][key] += value
                        if 'contributions' in chunk:
                            for contributor, stats in chunk['contributions'].items():
                                if not contributor:  # Noneや空文字列をスキップ
                                    continue
                                if not isinstance(stats, dict):
                                    continue
                                # contributorキーが存在しない場合は初期化
                                if contributor not in repo_data['contributions']:
                                    repo_data['contributions'][contributor] = {
                                        'commits': 0, 'additions': 0, 'deletions': 0, 'prs_created': 0, 'prs_merged': 0, 'prs_reviewed': 0
                                    }
                                for key, value in stats.items():
                                    # 存在しないキーの場合は初期化してから加算
                                    if key not in repo_data['contributions'][contributor]:
                                        repo_data['contributions'][contributor][key] = 0
                                    repo_data['contributions'][contributor][key] += value
                        continue
                # フェッチが必要な月をタスクに追加
                month_tasks.append((owner, repo_name, month_key, month_start, month_end, cache_path))

        # 月ごとの並列処理を実行
        if month_tasks:
            print(f"\n🔄 Fetching commits for {len(month_tasks)} month(s) across {len(repo_data_map)} repository/repositories...")
            print(f"Using parallel processing (max {max_workers} workers)...")
            print(f"  Tasks: {[(owner, repo_name, month_key) for owner, repo_name, month_key, _, _, _ in month_tasks[:5]]}...")

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_task = {
                    executor.submit(
                        fetch_month_commits,
                        github,
                        owner,
                        repo_name,
                        month_key,
                        month_start,
                        month_end,
                        cache_path,
                        use_cache
                    ): (owner, repo_name, month_key)
                    for owner, repo_name, month_key, month_start, month_end, cache_path in month_tasks
                }

                # 完了したタスクの結果をマージ
                for future in as_completed(future_to_task):
                    owner, repo_name, month_key = future_to_task[future]
                    repo_key = f"{owner}/{repo_name}"
                    try:
                        result = future.result()
                        if result and repo_key in repo_data_map:
                            repo_data = repo_data_map[repo_key]
                            # データをマージ
                            if 'month_key' not in result:
                                print(f"  ⚠️  [{owner}/{repo_name}] Result missing 'month_key', skipping...")
                                continue
                            month_key_result = result['month_key']
                            # code_frequencyは{month_key: {...}}の形式
                            if month_key_result in result.get('code_frequency', {}):
                                if month_key_result not in repo_data['code_frequency']:
                                    repo_data['code_frequency'][month_key_result] = {'additions': 0, 'deletions': 0}
                                repo_data['code_frequency'][month_key_result]['additions'] += result['code_frequency'][month_key_result]['additions']
                                repo_data['code_frequency'][month_key_result]['deletions'] += result['code_frequency'][month_key_result]['deletions']

                            # contributionsが存在する場合のみ処理
                            if 'contributions' in result and result['contributions']:
                                for contributor, stats in result['contributions'].items():
                                    if not contributor:  # Noneや空文字列をスキップ
                                        continue
                                    if not isinstance(stats, dict):
                                        continue
                                    # contributorキーが存在しない場合は初期化
                                    if contributor not in repo_data['contributions']:
                                        repo_data['contributions'][contributor] = {
                                            'commits': 0, 'additions': 0, 'deletions': 0, 'prs_created': 0, 'prs_merged': 0, 'prs_reviewed': 0
                                        }
                                    for key, value in stats.items():
                                        # 存在しないキーの場合は初期化してから加算
                                        if key not in repo_data['contributions'][contributor]:
                                            repo_data['contributions'][contributor][key] = 0
                                        repo_data['contributions'][contributor][key] += value

                            # monthly_contributionsが存在する場合のみ処理
                            monthly_contributions = result.get('monthly_contributions', {})
                            if monthly_contributions and month_key_result in monthly_contributions:
                                month_contribs = monthly_contributions[month_key_result]
                                if isinstance(month_contribs, dict):
                                    # month_key_resultが存在しない場合は初期化
                                    if month_key_result not in repo_data['monthly_contributions']:
                                        repo_data['monthly_contributions'][month_key_result] = defaultdict(lambda: {
                                            'commits': 0, 'additions': 0, 'deletions': 0, 'prs_created': 0, 'prs_merged': 0, 'prs_reviewed': 0
                                        })
                                    for contributor, stats in month_contribs.items():
                                        if not contributor:  # Noneや空文字列をスキップ
                                            continue
                                        if not isinstance(stats, dict):
                                            continue
                                        # contributorキーが存在しない場合は初期化
                                        if contributor not in repo_data['monthly_contributions'][month_key_result]:
                                            repo_data['monthly_contributions'][month_key_result][contributor] = {
                                                'commits': 0, 'additions': 0, 'deletions': 0, 'prs_created': 0, 'prs_merged': 0, 'prs_reviewed': 0
                                            }
                                        for key, value in stats.items():
                                            # 存在しないキーの場合は初期化してから加算
                                            if key not in repo_data['monthly_contributions'][month_key_result][contributor]:
                                                repo_data['monthly_contributions'][month_key_result][contributor][key] = 0
                                            repo_data['monthly_contributions'][month_key_result][contributor][key] += value

                            # contributorsが存在する場合のみ処理
                            contributors = result.get('contributors', [])
                            if contributors and isinstance(contributors, list):
                                for contributor in contributors:
                                    if not contributor:  # Noneや空文字列をスキップ
                                        continue
                                if month_key_result not in repo_data['monthly_stats']:
                                    repo_data['monthly_stats'][month_key_result] = {
                                        'prs_created': 0, 'prs_merged': 0, 'additions': 0, 'deletions': 0, 'contributors': set()
                                    }
                                if isinstance(repo_data['monthly_stats'][month_key_result]['contributors'], set):
                                    repo_data['monthly_stats'][month_key_result]['contributors'].add(contributor)
                                else:
                                    repo_data['monthly_stats'][month_key_result]['contributors'] = set([contributor])

                            if month_key_result in result.get('code_frequency', {}):
                                if month_key_result not in repo_data['monthly_stats']:
                                    repo_data['monthly_stats'][month_key_result] = {
                                        'prs_created': 0, 'prs_merged': 0, 'additions': 0, 'deletions': 0, 'contributors': set()
                                    }
                                repo_data['monthly_stats'][month_key_result]['additions'] += result['code_frequency'][month_key_result]['additions']
                                repo_data['monthly_stats'][month_key_result]['deletions'] += result['code_frequency'][month_key_result]['deletions']

                            print(f"  ✓ [{owner}/{repo_name} {month_key_result}] {result['commit_count']} commits")
                    except Exception as e:
                        import traceback
                        print(f"  ✗ Error processing {owner}/{repo_name} {month_key}: {e}")
                        print(f"    Traceback: {traceback.format_exc()}")

    # データをリストに変換
    all_data = list(repo_data_map.values())

    # contributorsをsetから数値に変換（JSONシリアライズのため）
    for repo_data in all_data:
        for month_key in repo_data.get('monthly_stats', {}):
            if isinstance(repo_data['monthly_stats'][month_key].get('contributors'), set):
                repo_data['monthly_stats'][month_key]['contributors'] = len(repo_data['monthly_stats'][month_key]['contributors'])
        # monthly_contributionsを通常の辞書に変換
        if 'monthly_contributions' in repo_data:
            monthly_contributions_dict = {}
            for month, contributors in repo_data['monthly_contributions'].items():
                monthly_contributions_dict[month] = dict(contributors)
            repo_data['monthly_contributions'] = monthly_contributions_dict

    # データをJSONファイルに保存
    output_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'collected_data.json')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump({
            'collected_at': datetime.now(JST).isoformat(),
            'start_date': start_date.isoformat(),
            'repositories': all_data
        }, f, indent=2, ensure_ascii=False)

    print(f"\n{'='*60}")
    print(f"Data collection completed. Saved to {output_path}")
    print(f"Total repositories processed: {len(all_data)}/{total_repos}")

    # 最終的なレート制限情報を表示
    rate_limit = github.get_rate_limit()
    if hasattr(rate_limit, 'resources'):
        core_limit = rate_limit.resources.core
        print(f"Rate limit remaining: {core_limit.remaining}/{core_limit.limit}")
    else:
        print(f"Rate limit remaining: {rate_limit.core.remaining}/{rate_limit.core.limit}")

if __name__ == '__main__':
    main()
