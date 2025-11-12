#!/usr/bin/env python3
"""
GitHubリポジトリのデータを収集するスクリプト（最適化版）
PR、code frequency、contributionsなどを取得し、人ごと・月ごとに集計
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta
from dateutil import parser
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from github import Github
from github import Auth
from github.GithubException import GithubException, RateLimitExceededException
import pytz

# 指定日数前の日付を取得
def get_start_date(days=365):
    """指定日数前の日付を取得（デフォルト: 365日 = 1年）"""
    return datetime.now(pytz.UTC) - timedelta(days=days)

# 月のキーを生成（YYYY-MM形式）
def get_month_key(date):
    if isinstance(date, str):
        date = parser.parse(date)
    return date.strftime('%Y-%m')

# 現在の月の開始日を取得
def get_current_month_start():
    """現在の月の開始日を取得"""
    now = datetime.now(pytz.UTC)
    return datetime(now.year, now.month, 1, tzinfo=pytz.UTC)

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
    """キャッシュを読み込み"""
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"  ⚠️  Failed to load cache: {e}")
    return None

# キャッシュを保存
def save_cache(cache_path, data):
    """キャッシュを保存"""
    try:
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
        wait_time = (reset_time - datetime.now(pytz.UTC)).total_seconds() + 10
        if wait_time > 0:
            print(f"  ⚠️  Rate limit low ({remaining} remaining). Waiting {int(wait_time)} seconds...")
            time.sleep(wait_time)

    return remaining

# リポジトリのデータを収集（最適化版）
def collect_repo_data(github, owner, repo_name, start_date, collect_reviews=False, collect_commit_stats=True, use_cache=True):
    """リポジトリのデータを収集"""
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
                if month_date < current_month_start:
                    cached_monthly_stats[month] = stats.copy()
                    if isinstance(stats.get('contributors'), int):
                        cached_monthly_stats[month]['contributors'] = set()

            for month, freq in cached_data.get('code_frequency', {}).items():
                month_date = parser.parse(f"{month}-01")
                if month_date < current_month_start:
                    cached_code_frequency[month] = freq.copy()

            # 確定分のコントリビューター統計をキャッシュから読み込み
            for contributor, stats in cached_data.get('contributions', {}).items():
                cached_contributions[contributor] = stats.copy()

            # devin-botの内訳も読み込み
            for contributor, breakdown in cached_data.get('devin_breakdown', {}).items():
                cached_devin_breakdown[contributor] = breakdown.copy()

            print(f"  📦 Using {len(cached_prs)} cached PRs (before {current_month_start.strftime('%Y-%m')})")
        else:
            print(f"  📦 Cache cleared, will fetch all data from API")

    # PRデータを収集（当月分のみ）
    try:
        check_rate_limit(github)
        prs = repo.get_pulls(state='all', sort='updated', direction='desc')

        pr_count = 0
        new_pr_count = 0
        last_progress_time = time.time()
        progress_interval = 60  # 60秒ごとに進捗表示
        total_checked = 0  # start_date以降のPRをチェックした数

        for pr in prs:
            # 直近1年間のPRのみ処理
            if pr.updated_at < start_date:
                break

            total_checked += 1  # start_date以降のPRをチェック

            # 確定分（当月より前）はスキップ（キャッシュから読み込む）
            # ただし、キャッシュがない場合や、start_dateより前のデータでキャッシュにない場合は取得する
            pr_created = pr.created_at
            if pr_created < current_month_start:
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

            # レビュアーを取得（オプション、デフォルトで無効）
            if collect_reviews:
                try:
                    check_rate_limit(github)
                    reviews = pr.get_reviews()
                    for review in reviews:
                        if review.user and review.user.login not in pr_data['reviewers']:
                            pr_data['reviewers'].append(review.user.login)
                except RateLimitExceededException:
                    print(f"  ⚠️  Rate limit exceeded while fetching reviews for PR #{pr.number}, skipping...")
                    break
                except Exception:
                    pass  # レビュー取得エラーは無視

            data['prs'].append(pr_data)
            pr_count += 1
            new_pr_count += 1

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

            # レビュアーの統計
            for reviewer in pr_data['reviewers']:
                data['contributions'][reviewer]['prs_reviewed'] += 1

        print(f"  ✓ Collected {new_pr_count} new PRs (total: {pr_count + len(cached_prs)} with cache)")

        # キャッシュから読み込んだPRを追加
        data['prs'].extend(cached_prs)
    except RateLimitExceededException:
        print(f"  ⚠️  Rate limit exceeded while fetching PRs")
    except Exception as e:
        print(f"  ✗ Error collecting PRs: {e}")

    # Code frequencyデータを収集（コミット統計）
    if collect_commit_stats:
        # 確定分のコミット統計をキャッシュから読み込み
        if cached_data and use_cache:
            for month, freq in cached_code_frequency.items():
                data['code_frequency'][month] = freq.copy()

        try:
            check_rate_limit(github)
            commits = repo.get_commits(since=start_date)
            commit_count = 0
            new_commit_count = 0
            max_commits = 1000  # API制限を考慮
            stats_errors = 0
            last_commit_progress_time = time.time()
            commit_progress_interval = 60  # 60秒ごとに進捗表示

            for commit in commits:
                commit_count += 1
                if commit_count > max_commits:
                    print(f"  ⚠️  Reached commit limit ({max_commits}), stopping collection")
                    break

                try:
                    commit_date = commit.commit.author.date

                    # 日付が範囲外の場合はスキップ
                    if commit_date < start_date:
                        continue

                    # 確定分（当月より前）はスキップ（キャッシュから読み込む）
                    # ただし、キャッシュがない場合や、start_dateより前のデータでキャッシュにない場合は取得する
                    if commit_date < current_month_start:
                        # キャッシュがある場合はスキップ
                        if len(cached_code_frequency) > 0:
                            continue
                        # キャッシュがない場合は、start_date以降のデータを取得
                        if commit_date < start_date:
                            continue

                    # 統計情報を取得（重いAPI呼び出し）
                    # エラーが多すぎる場合はスキップ
                    if stats_errors < 10:
                        try:
                            check_rate_limit(github)
                            stats = commit.stats
                            additions = stats.additions
                            deletions = stats.deletions
                        except RateLimitExceededException:
                            print(f"  ⚠️  Rate limit exceeded while fetching commit stats, stopping...")
                            break
                        except Exception:
                            stats_errors += 1
                            additions = 0
                            deletions = 0
                    else:
                        # 統計取得エラーが多すぎる場合はスキップ
                        additions = 0
                        deletions = 0

                    month_key = get_month_key(commit_date)
                    data['code_frequency'][month_key]['additions'] += additions
                    data['code_frequency'][month_key]['deletions'] += deletions

                    # コミット作成者の統計
                    if commit.author:
                        author = commit.author.login
                        data['contributions'][author]['commits'] += 1
                        data['contributions'][author]['additions'] += additions
                        data['contributions'][author]['deletions'] += deletions
                        data['monthly_stats'][month_key]['contributors'].add(author)

                    # 月ごとの統計
                    data['monthly_stats'][month_key]['additions'] += additions
                    data['monthly_stats'][month_key]['deletions'] += deletions
                    new_commit_count += 1

                    # 進捗表示（1分ごと）
                    current_time = time.time()
                    if current_time - last_commit_progress_time >= commit_progress_interval:
                        elapsed = int(current_time - start_time)
                        elapsed_min = elapsed // 60
                        elapsed_sec = elapsed % 60

                        # 処理速度を計算（コミット/秒）
                        if elapsed > 0:
                            rate = new_commit_count / elapsed
                            # 残り時間を推定
                            if rate > 0:
                                # 残りのコミット数を推定（最大1000件まで）
                                remaining_estimate = max(0, max_commits - commit_count)
                                if remaining_estimate > 0:
                                    eta_seconds = int(remaining_estimate / rate) if rate > 0 else 0
                                    eta_min = eta_seconds // 60
                                    eta_sec = eta_seconds % 60
                                    progress_pct = min(100, int((commit_count / max_commits) * 100)) if max_commits > 0 else 0
                                    if elapsed_min > 0:
                                        print(f"  ⏳ Progress: {new_commit_count} commits processed ({progress_pct}%, elapsed: {elapsed_min}m {elapsed_sec}s, rate: {rate:.2f} commits/s, ETA: ~{eta_min}m {eta_sec}s)")
                                    else:
                                        print(f"  ⏳ Progress: {new_commit_count} commits processed ({progress_pct}%, elapsed: {elapsed_sec}s, rate: {rate:.2f} commits/s, ETA: ~{eta_sec}s)")
                                else:
                                    if elapsed_min > 0:
                                        print(f"  ⏳ Progress: {new_commit_count} commits processed (elapsed: {elapsed_min}m {elapsed_sec}s, rate: {rate:.2f} commits/s)")
                                    else:
                                        print(f"  ⏳ Progress: {new_commit_count} commits processed (elapsed: {elapsed_sec}s, rate: {rate:.2f} commits/s)")
                            else:
                                if elapsed_min > 0:
                                    print(f"  ⏳ Progress: {new_commit_count} commits processed (elapsed: {elapsed_min}m {elapsed_sec}s)")
                                else:
                                    print(f"  ⏳ Progress: {new_commit_count} commits processed (elapsed: {elapsed_sec}s)")
                        else:
                            print(f"  ⏳ Progress: {new_commit_count} commits processed")
                        last_commit_progress_time = current_time
                except Exception as e:
                    # 個別のコミットエラーは無視して続行
                    continue

            print(f"  ✓ Collected {new_commit_count} new commits (total: {commit_count} with cache)")
            if stats_errors > 0:
                print(f"  ⚠️  Skipped stats for {stats_errors} commits due to errors")
        except RateLimitExceededException:
            print(f"  ⚠️  Rate limit exceeded while fetching commits")
        except Exception as e:
            print(f"  ✗ Error collecting commits: {e}")

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
    if 'devin_breakdown' in data:
        data['devin_breakdown'] = dict(data['devin_breakdown'])

    # キャッシュを保存（次回のために）
    if use_cache:
        cache_data = {
            'cached_at': datetime.now(pytz.UTC).isoformat(),
            'repository': data['repository'],
            'prs': data['prs'],
            'contributions': data['contributions'],
            'monthly_stats': data['monthly_stats'],
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
    try:
        auth = Auth.Token(github_token)
        github = Github(auth=auth, per_page=100)

        # 認証をテスト
        user = github.get_user()
        print(f"Authenticated as: {user.login}")

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
        else:
            print(f"Error authenticating with GitHub: {e}")
        sys.exit(1)

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
                start_date = pytz.UTC.localize(start_date)
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

    print(f"\n{'='*60}")
    print(f"Processing {total_repos} repository/repositories...")
    print(f"Options: collect_reviews={collect_reviews}, collect_commit_stats={collect_commit_stats}, max_workers={max_workers}, use_cache={use_cache}")
    print(f"Period: {start_date.isoformat()} to {datetime.now(pytz.UTC).isoformat()}")
    print(f"{'='*60}\n")

    # 並列処理で各リポジトリのデータを収集
    if total_repos > 1 and max_workers > 1:
        print(f"Using parallel processing (max {max_workers} workers)...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # タスクを送信
            future_to_repo = {
                executor.submit(
                    collect_repo_data,
                    github,
                    repo_config['owner'],
                    repo_config['name'],
                    start_date,
                    collect_reviews,
                    collect_commit_stats,
                    use_cache
                ): repo_config
                for repo_config in repos
            }

            # 完了したタスクを処理
            for future in as_completed(future_to_repo):
                repo_config = future_to_repo[future]
                try:
                    repo_data = future.result()
                    if repo_data:
                        all_data.append(repo_data)
                except Exception as e:
                    print(f"Error processing {repo_config['owner']}/{repo_config['name']}: {e}")
    else:
        # 順次処理（単一リポジトリまたは並列処理が無効な場合）
        for repo_config in repos:
            owner = repo_config['owner']
            name = repo_config['name']
            repo_data = collect_repo_data(github, owner, name, start_date, collect_reviews, collect_commit_stats, use_cache)
            if repo_data:
                all_data.append(repo_data)

    # データをJSONファイルに保存
    output_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'collected_data.json')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump({
            'collected_at': datetime.now(pytz.UTC).isoformat(),
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
