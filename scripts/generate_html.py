#!/usr/bin/env python3
"""
収集したデータからHTMLレポートを生成するスクリプト
"""

import json
import os
from datetime import datetime
from collections import defaultdict
from jinja2 import Template

def aggregate_data(data):
    """全リポジトリのデータを集計"""
    aggregated = {
        'total_prs': 0,
        'total_merged_prs': 0,
        'total_additions': 0,
        'total_deletions': 0,
        'total_commits': 0,
        'contributors': defaultdict(lambda: {
            'commits': 0,
            'additions': 0,
            'deletions': 0,
            'prs_created': 0,
            'prs_merged': 0,
            'prs_reviewed': 0,
            'repositories': set()
        }),
        'monthly_stats': defaultdict(lambda: {
            'prs_created': 0,
            'prs_merged': 0,
            'additions': 0,
            'deletions': 0,
            'contributors': 0
        }),
        'code_frequency': defaultdict(lambda: {'additions': 0, 'deletions': 0})
    }

    for repo_data in data['repositories']:
        # PR統計
        aggregated['total_prs'] += len(repo_data['prs'])
        aggregated['total_merged_prs'] += sum(1 for pr in repo_data['prs'] if pr['state'] == 'closed' and pr['merged_at'])

        # コントリビューター統計
        for contributor, stats in repo_data['contributions'].items():
            aggregated['contributors'][contributor]['commits'] += stats['commits']
            aggregated['contributors'][contributor]['additions'] += stats['additions']
            aggregated['contributors'][contributor]['deletions'] += stats['deletions']
            aggregated['contributors'][contributor]['prs_created'] += stats['prs_created']
            aggregated['contributors'][contributor]['prs_merged'] += stats['prs_merged']
            aggregated['contributors'][contributor]['prs_reviewed'] += stats['prs_reviewed']
            aggregated['contributors'][contributor]['repositories'].add(repo_data['repository'])

        # 月ごとの統計
        for month, stats in repo_data['monthly_stats'].items():
            aggregated['monthly_stats'][month]['prs_created'] += stats['prs_created']
            aggregated['monthly_stats'][month]['prs_merged'] += stats['prs_merged']
            aggregated['monthly_stats'][month]['additions'] += stats['additions']
            aggregated['monthly_stats'][month]['deletions'] += stats['deletions']
            aggregated['monthly_stats'][month]['contributors'] = max(
                aggregated['monthly_stats'][month]['contributors'],
                stats['contributors']
            )

        # Code frequency
        for month, freq in repo_data['code_frequency'].items():
            aggregated['code_frequency'][month]['additions'] += freq['additions']
            aggregated['code_frequency'][month]['deletions'] += freq['deletions']

    # セットを数値に変換
    for contributor in aggregated['contributors']:
        aggregated['contributors'][contributor]['repositories'] = len(aggregated['contributors'][contributor]['repositories'])
        aggregated['total_commits'] += aggregated['contributors'][contributor]['commits']
        aggregated['total_additions'] += aggregated['contributors'][contributor]['additions']
        aggregated['total_deletions'] += aggregated['contributors'][contributor]['deletions']

    # 辞書を通常の辞書に変換
    aggregated['contributors'] = dict(aggregated['contributors'])
    aggregated['monthly_stats'] = dict(sorted(aggregated['monthly_stats'].items()))
    aggregated['code_frequency'] = dict(sorted(aggregated['code_frequency'].items()))

    return aggregated

def generate_html(data, aggregated):
    """HTMLを生成"""

    # devin-botの内訳を集計
    devin_breakdown_aggregated = defaultdict(lambda: {
        'prs_merged': 0,
        'additions': 0,
        'deletions': 0
    })
    for repo_data in data['repositories']:
        if 'devin_breakdown' in repo_data:
            for contributor, breakdown in repo_data['devin_breakdown'].items():
                devin_breakdown_aggregated[contributor]['prs_merged'] += breakdown['prs_merged']
                devin_breakdown_aggregated[contributor]['additions'] += breakdown['additions']
                devin_breakdown_aggregated[contributor]['deletions'] += breakdown['deletions']
    devin_breakdown_aggregated = dict(devin_breakdown_aggregated)

    # コントリビューターをソート（総合的な貢献度で）
    # 各コントリビューターが関与しているリポジトリのリストを作成
    contributor_repos = {}
    for repo_data in data['repositories']:
        for contributor_name in repo_data['contributions'].keys():
            if contributor_name not in contributor_repos:
                contributor_repos[contributor_name] = []
            contributor_repos[contributor_name].append(repo_data['repository'])

    contributors_list = []
    for contributor, stats in aggregated['contributors'].items():
        score = (
            stats['commits'] * 1 +
            stats['prs_created'] * 5 +
            stats['prs_merged'] * 10 +
            stats['prs_reviewed'] * 3 +
            (stats['additions'] + stats['deletions']) / 100
        )
        repos_list = contributor_repos.get(contributor, [])

        # devin-botの内訳を追加
        devin_breakdown = devin_breakdown_aggregated.get(contributor, {
            'prs_merged': 0,
            'additions': 0,
            'deletions': 0
        })

        contributors_list.append({
            'name': contributor,
            'score': score,
            'repos_list': repos_list,
            'devin_breakdown': devin_breakdown,
            **stats
        })
    contributors_list.sort(key=lambda x: x['score'], reverse=True)

    # 月ごとのデータを配列に変換（チャート用）
    monthly_data = []
    all_months = set(aggregated['monthly_stats'].keys()) | set(aggregated['code_frequency'].keys())
    for month in sorted(all_months):
        monthly_stats = aggregated['monthly_stats'].get(month, {
            'prs_created': 0,
            'prs_merged': 0,
            'additions': 0,
            'deletions': 0,
            'contributors': 0
        })
        code_freq = aggregated['code_frequency'].get(month, {'additions': 0, 'deletions': 0})
        monthly_data.append({
            'month': month,
            **monthly_stats,
            'additions': code_freq['additions'],
            'deletions': code_freq['deletions']
        })

    # グラフのフィルタリング用にPRデータを準備
    pr_data_for_charts = []
    for repo_data in data['repositories']:
        for pr in repo_data['prs']:
            pr_data_for_charts.append({
                'author': pr.get('author', 'unknown'),
                'merged_by': pr.get('merged_by'),
                'created_at': pr.get('created_at'),
                'merged_at': pr.get('merged_at'),
                'additions': pr.get('additions', 0),
                'deletions': pr.get('deletions', 0),
                'repository': repo_data['repository']
            })

    template_str = '''<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GitHub Dashboard - 分析レポート</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
            color: #333;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        .header {
            background: white;
            border-radius: 10px;
            padding: 30px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        .header h1 {
            color: #667eea;
            margin-bottom: 10px;
        }
        .header p {
            color: #666;
        }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        .stat-card {
            background: white;
            border-radius: 10px;
            padding: 25px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            transition: transform 0.2s;
        }
        .stat-card:hover {
            transform: translateY(-5px);
        }
        .stat-card h3 {
            color: #667eea;
            font-size: 14px;
            margin-bottom: 10px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        .stat-card .value {
            font-size: 36px;
            font-weight: bold;
            color: #333;
        }
        .section {
            background: white;
            border-radius: 10px;
            padding: 30px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        .section h2 {
            color: #667eea;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 2px solid #f0f0f0;
        }
        .chart-container {
            position: relative;
            height: 400px;
            margin-bottom: 30px;
        }
        .contributors-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }
        .contributors-table th,
        .contributors-table td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #f0f0f0;
        }
        .contributors-table th {
            background: #f8f9fa;
            color: #667eea;
            font-weight: 600;
        }
        .contributors-table tr:hover {
            background: #f8f9fa;
        }
        .badge {
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 600;
            margin-left: 5px;
        }
        .badge-primary {
            background: #667eea;
            color: white;
        }
        .badge-success {
            background: #10b981;
            color: white;
        }
        .repositories-list {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 15px;
            margin-top: 20px;
        }
        .repo-card {
            background: #f8f9fa;
            border-radius: 8px;
            padding: 15px;
            border-left: 4px solid #667eea;
        }
        .repo-card h4 {
            color: #333;
            margin-bottom: 10px;
        }
        .repo-stats {
            display: flex;
            gap: 15px;
            font-size: 14px;
            color: #666;
        }
        .repo-stat {
            display: flex;
            align-items: center;
            gap: 5px;
        }
        .filters {
            background: white;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        .filters h3 {
            color: #667eea;
            margin-bottom: 15px;
            font-size: 18px;
        }
        .filter-group {
            display: flex;
            gap: 15px;
            flex-wrap: wrap;
            align-items: center;
        }
        .filter-item {
            flex: 1;
            min-width: 200px;
        }
        .filter-item label {
            display: block;
            margin-bottom: 5px;
            color: #666;
            font-size: 14px;
            font-weight: 500;
        }
        .filter-item input,
        .filter-item select {
            width: 100%;
            padding: 10px;
            border: 2px solid #e0e0e0;
            border-radius: 6px;
            font-size: 14px;
            transition: border-color 0.2s;
        }
        .filter-item input:focus,
        .filter-item select:focus {
            outline: none;
            border-color: #667eea;
        }
        .filter-item input::placeholder {
            color: #999;
        }
        .filter-actions {
            display: flex;
            gap: 10px;
            align-items: flex-end;
        }
        .btn {
            padding: 10px 20px;
            border: none;
            border-radius: 6px;
            font-size: 14px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s;
        }
        .btn-primary {
            background: #667eea;
            color: white;
        }
        .btn-primary:hover {
            background: #5568d3;
        }
        .btn-secondary {
            background: #f0f0f0;
            color: #333;
        }
        .btn-secondary:hover {
            background: #e0e0e0;
        }
        .hidden {
            display: none !important;
        }
        .filter-info {
            margin-top: 10px;
            padding: 10px;
            background: #f8f9fa;
            border-radius: 6px;
            font-size: 14px;
            color: #666;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 GitHub Dashboard - 分析レポート</h1>
            <p>収集日時: {{ collected_at }}</p>
            <p>分析期間: 直近1年間 ({{ start_date }} ～ {{ collected_at }})</p>
        </div>

        <div class="filters">
            <h3>🔍 フィルタリング</h3>
            <div class="filter-group">
                <div class="filter-item">
                    <label for="contributorFilter">コントリビューター名</label>
                    <input type="text" id="contributorFilter" placeholder="ユーザー名で検索...">
                </div>
                <div class="filter-item">
                    <label for="repoFilter">リポジトリ名</label>
                    <select id="repoFilter">
                        <option value="">すべてのリポジトリ</option>
                        {% for repo_data in repositories %}
                        <option value="{{ repo_data.repository }}">{{ repo_data.repository }}</option>
                        {% endfor %}
                    </select>
                </div>
                <div class="filter-actions">
                    <button class="btn btn-primary" onclick="applyFilters()">適用</button>
                    <button class="btn btn-secondary" onclick="clearFilters()">クリア</button>
                </div>
            </div>
            <div class="filter-info" id="filterInfo"></div>
        </div>

        <div class="stats-grid">
            <div class="stat-card">
                <h3>総PR数</h3>
                <div class="value">{{ total_prs }}</div>
            </div>
            <div class="stat-card">
                <h3>マージ済みPR</h3>
                <div class="value">{{ total_merged_prs }}</div>
            </div>
            <div class="stat-card">
                <h3>総コミット数</h3>
                <div class="value">{{ total_commits }}</div>
            </div>
            <div class="stat-card">
                <h3>追加行数</h3>
                <div class="value">{{ "{:,}".format(total_additions) }}</div>
            </div>
            <div class="stat-card">
                <h3>削除行数</h3>
                <div class="value">{{ "{:,}".format(total_deletions) }}</div>
            </div>
            <div class="stat-card">
                <h3>コントリビューター数</h3>
                <div class="value">{{ contributors_list|length }}</div>
            </div>
        </div>

        <div class="section">
            <h2>📈 月ごとの活動状況</h2>
            <div class="chart-container">
                <canvas id="monthlyChart"></canvas>
            </div>
        </div>

        <div class="section">
            <h2>💻 Code Frequency (月ごと)</h2>
            <div class="chart-container">
                <canvas id="codeFrequencyChart"></canvas>
            </div>
        </div>

        <div class="section">
            <h2>👥 コントリビューター別統計</h2>
            <table class="contributors-table">
                <thead>
                    <tr>
                        <th>順位</th>
                        <th>ユーザー名</th>
                        <th>コミット</th>
                        <th>PR作成</th>
                        <th>PRマージ</th>
                        <th>PRレビュー</th>
                        <th>追加行数</th>
                        <th>削除行数</th>
                        <th>関与リポジトリ</th>
                    </tr>
                </thead>
                <tbody id="contributorsTableBody">
                    {% for contributor in contributors_list[:50] %}
                    <tr data-contributor="{{ contributor.name|lower }}" data-repos="{{ contributor.repos_list|join(',')|lower }}">
                        <td class="rank">{{ loop.index }}</td>
                        <td><strong>{{ contributor.name }}</strong>{% if contributor.devin_breakdown.prs_merged > 0 %}<br><span style="font-size: 12px; color: #666; font-weight: normal;">(devin: PR{{ contributor.devin_breakdown.prs_merged }}, +{{ "{:,}".format(contributor.devin_breakdown.additions) }}/-{{ "{:,}".format(contributor.devin_breakdown.deletions) }})</span>{% endif %}</td>
                        <td>{{ contributor.commits }}</td>
                        <td>{{ contributor.prs_created }}</td>
                        <td>{{ contributor.prs_merged }}</td>
                        <td>{{ contributor.prs_reviewed }}</td>
                        <td>{{ "{:,}".format(contributor.additions) }}</td>
                        <td>{{ "{:,}".format(contributor.deletions) }}</td>
                        <td>{{ contributor.repositories }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>

        <div class="section">
            <h2>📦 対象リポジトリ</h2>
            <div class="repositories-list" id="repositoriesList">
                {% for repo_data in repositories %}
                <div class="repo-card" data-repo="{{ repo_data.repository }}">
                    <h4>{{ repo_data.repository }}</h4>
                    <div class="repo-stats">
                        <div class="repo-stat">
                            <span>PR:</span>
                            <strong>{{ repo_data.prs|length }}</strong>
                        </div>
                        <div class="repo-stat">
                            <span>コントリビューター:</span>
                            <strong>{{ repo_data.contributions|length }}</strong>
                        </div>
                    </div>
                </div>
                {% endfor %}
            </div>
        </div>
    </div>

    <script>
        // グローバル変数としてチャートを保持
        let monthlyChart = null;
        let codeFrequencyChart = null;

        // 月ごとの活動状況チャート
        const monthlyCtx = document.getElementById('monthlyChart').getContext('2d');
        monthlyChart = new Chart(monthlyCtx, {
            type: 'line',
            data: {
                labels: {{ monthly_labels|tojson }},
                datasets: [
                    {
                        label: 'PR作成数',
                        data: {{ monthly_prs_created|tojson }},
                        borderColor: 'rgb(102, 126, 234)',
                        backgroundColor: 'rgba(102, 126, 234, 0.1)',
                        tension: 0.4
                    },
                    {
                        label: 'PRマージ数',
                        data: {{ monthly_prs_merged|tojson }},
                        borderColor: 'rgb(16, 185, 129)',
                        backgroundColor: 'rgba(16, 185, 129, 0.1)',
                        tension: 0.4
                    },
                    {
                        label: 'コントリビューター数',
                        data: {{ monthly_contributors|tojson }},
                        borderColor: 'rgb(245, 158, 11)',
                        backgroundColor: 'rgba(245, 158, 11, 0.1)',
                        tension: 0.4
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'top',
                    },
                    title: {
                        display: true,
                        text: '月ごとの活動状況'
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });

        // Code Frequencyチャート
        const codeFreqCtx = document.getElementById('codeFrequencyChart').getContext('2d');
        codeFrequencyChart = new Chart(codeFreqCtx, {
            type: 'bar',
            data: {
                labels: {{ monthly_labels|tojson }},
                datasets: [
                    {
                        label: '追加行数',
                        data: {{ monthly_additions|tojson }},
                        backgroundColor: 'rgba(16, 185, 129, 0.6)',
                    },
                    {
                        label: '削除行数',
                        data: {{ monthly_deletions|tojson }},
                        backgroundColor: 'rgba(239, 68, 68, 0.6)',
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'top',
                    },
                    title: {
                        display: true,
                        text: 'Code Frequency (追加・削除行数)'
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });

        // フィルタリング機能
        function applyFilters() {
            const contributorFilter = document.getElementById('contributorFilter').value.toLowerCase().trim();
            const repoFilter = document.getElementById('repoFilter').value;
            const filterInfo = document.getElementById('filterInfo');

            let visibleCount = 0;
            let totalCount = 0;

            // フィルタリングされたコントリビューターのリストを収集
            const visibleContributors = new Set();

            // コントリビューター テーブルのフィルタリング
            const tableRows = document.querySelectorAll('#contributorsTableBody tr');
            tableRows.forEach((row, index) => {
                totalCount++;
                const contributorName = row.getAttribute('data-contributor') || '';
                const contributorRepos = (row.getAttribute('data-repos') || '').toLowerCase();

                let show = true;

                // コントリビューター名でフィルタリング
                if (contributorFilter && !contributorName.includes(contributorFilter)) {
                    show = false;
                }

                // リポジトリでフィルタリング
                if (repoFilter) {
                    const repoFilterLower = repoFilter.toLowerCase();
                    if (!contributorRepos.includes(repoFilterLower)) {
                        show = false;
                    }
                }

                if (show) {
                    row.classList.remove('hidden');
                    visibleCount++;
                    // 順位を更新
                    const rankCell = row.querySelector('.rank');
                    if (rankCell) {
                        rankCell.textContent = visibleCount;
                    }
                    // 表示されているコントリビューターを記録
                    visibleContributors.add(contributorName);
                } else {
                    row.classList.add('hidden');
                }
            });

            // リポジトリカードのフィルタリング
            const repoCards = document.querySelectorAll('#repositoriesList .repo-card');
            repoCards.forEach(card => {
                const repoName = card.getAttribute('data-repo') || '';

                let show = true;

                // リポジトリ名でフィルタリング
                if (repoFilter && repoName !== repoFilter) {
                    show = false;
                }

                if (show) {
                    card.classList.remove('hidden');
                } else {
                    card.classList.add('hidden');
                }
            });

            // フィルタ情報を表示
            let infoText = '';
            if (contributorFilter || repoFilter) {
                infoText = `表示中: ${visibleCount} / ${totalCount} コントリビューター`;
                if (repoFilter) {
                    infoText += ` (リポジトリ: ${repoFilter})`;
                }
                if (contributorFilter) {
                    infoText += ` (検索: "${contributorFilter}")`;
                }
            } else {
                infoText = '';
            }
            filterInfo.textContent = infoText;
            filterInfo.style.display = infoText ? 'block' : 'none';

            // グラフを更新（フィルタリングされたコントリビューターのみ）
            updateCharts(visibleContributors, contributorFilter, repoFilter);
        }

        // PRデータをJavaScriptで利用可能にする
        const allPRData = {{ pr_data_for_charts|tojson }};

        // グラフを更新する関数
        function updateCharts(visibleContributors, contributorFilter, repoFilter) {
            // 元のデータを保持
            const originalMonthlyLabels = {{ monthly_labels|tojson }};
            const originalMonthlyPRsCreated = {{ monthly_prs_created|tojson }};
            const originalMonthlyPRsMerged = {{ monthly_prs_merged|tojson }};
            const originalMonthlyContributors = {{ monthly_contributors|tojson }};
            const originalMonthlyAdditions = {{ monthly_additions|tojson }};
            const originalMonthlyDeletions = {{ monthly_deletions|tojson }};

            // フィルタリングが適用されている場合
            if (contributorFilter || repoFilter) {
                // フィルタリングされたPRデータで月ごとの統計を再計算
                const filteredMonthlyStats = {};
                const filteredCodeFrequency = {};
                const contributorSet = new Set();

                allPRData.forEach(pr => {
                    const prAuthor = (pr.author || '').toLowerCase();
                    const prRepo = (pr.repository || '').toLowerCase();

                    // フィルタリング条件をチェック
                    let include = true;
                    if (contributorFilter && !prAuthor.includes(contributorFilter)) {
                        include = false;
                    }
                    if (repoFilter && !prRepo.includes(repoFilter.toLowerCase())) {
                        include = false;
                    }

                    if (!include) return;

                    // 月を取得
                    if (pr.created_at) {
                        const createdDate = new Date(pr.created_at);
                        const monthKey = createdDate.getFullYear() + '-' + String(createdDate.getMonth() + 1).padStart(2, '0');

                        if (!filteredMonthlyStats[monthKey]) {
                            filteredMonthlyStats[monthKey] = {
                                prs_created: 0,
                                prs_merged: 0,
                                additions: 0,
                                deletions: 0,
                                contributors: new Set()
                            };
                        }

                        filteredMonthlyStats[monthKey].prs_created += 1;
                        if (pr.merged_at) {
                            const mergedDate = new Date(pr.merged_at);
                            const mergeMonthKey = mergedDate.getFullYear() + '-' + String(mergedDate.getMonth() + 1).padStart(2, '0');

                            if (!filteredMonthlyStats[mergeMonthKey]) {
                                filteredMonthlyStats[mergeMonthKey] = {
                                    prs_created: 0,
                                    prs_merged: 0,
                                    additions: 0,
                                    deletions: 0,
                                    contributors: new Set()
                                };
                            }

                            filteredMonthlyStats[mergeMonthKey].prs_merged += 1;

                            // devin-botの場合はマージした人をカウント
                            const contributor = pr.author === 'devin-ai-integration[bot]' && pr.merged_by ? pr.merged_by : pr.author;
                            filteredMonthlyStats[mergeMonthKey].contributors.add(contributor);
                        }

                        filteredMonthlyStats[monthKey].additions += pr.additions || 0;
                        filteredMonthlyStats[monthKey].deletions += pr.deletions || 0;
                        filteredMonthlyStats[monthKey].contributors.add(prAuthor);
                    }

                    // Code frequency（簡易版：PRの追加・削除行数を使用）
                    if (pr.created_at) {
                        const createdDate = new Date(pr.created_at);
                        const monthKey = createdDate.getFullYear() + '-' + String(createdDate.getMonth() + 1).padStart(2, '0');

                        if (!filteredCodeFrequency[monthKey]) {
                            filteredCodeFrequency[monthKey] = { additions: 0, deletions: 0 };
                        }
                        filteredCodeFrequency[monthKey].additions += pr.additions || 0;
                        filteredCodeFrequency[monthKey].deletions += pr.deletions || 0;
                    }
                });

                // 月ごとのデータを配列に変換
                const allFilteredMonths = new Set([...Object.keys(filteredMonthlyStats), ...Object.keys(filteredCodeFrequency)]);
                const sortedFilteredMonths = Array.from(allFilteredMonths).sort();

                const filteredPRsCreated = [];
                const filteredPRsMerged = [];
                const filteredContributors = [];
                const filteredAdditions = [];
                const filteredDeletions = [];

                sortedFilteredMonths.forEach(month => {
                    const stats = filteredMonthlyStats[month] || { prs_created: 0, prs_merged: 0, contributors: new Set() };
                    const freq = filteredCodeFrequency[month] || { additions: 0, deletions: 0 };

                    filteredPRsCreated.push(stats.prs_created);
                    filteredPRsMerged.push(stats.prs_merged);
                    filteredContributors.push(stats.contributors instanceof Set ? stats.contributors.size : stats.contributors);
                    filteredAdditions.push(freq.additions);
                    filteredDeletions.push(freq.deletions);
                });

                // グラフを更新
                if (monthlyChart) {
                    monthlyChart.data.labels = sortedFilteredMonths;
                    monthlyChart.data.datasets[0].data = filteredPRsCreated;
                    monthlyChart.data.datasets[1].data = filteredPRsMerged;
                    monthlyChart.data.datasets[2].data = filteredContributors;
                    monthlyChart.options.plugins.title.text = '月ごとの活動状況 (フィルタリング適用中)';
                    monthlyChart.update();
                }
                if (codeFrequencyChart) {
                    codeFrequencyChart.data.labels = sortedFilteredMonths;
                    codeFrequencyChart.data.datasets[0].data = filteredAdditions;
                    codeFrequencyChart.data.datasets[1].data = filteredDeletions;
                    codeFrequencyChart.options.plugins.title.text = 'Code Frequency (フィルタリング適用中)';
                    codeFrequencyChart.update();
                }
            } else {
                // フィルタリングが解除された場合、元のデータに戻す
                if (monthlyChart) {
                    monthlyChart.data.labels = originalMonthlyLabels;
                    monthlyChart.data.datasets[0].data = originalMonthlyPRsCreated;
                    monthlyChart.data.datasets[1].data = originalMonthlyPRsMerged;
                    monthlyChart.data.datasets[2].data = originalMonthlyContributors;
                    monthlyChart.options.plugins.title.text = '月ごとの活動状況';
                    monthlyChart.update();
                }
                if (codeFrequencyChart) {
                    codeFrequencyChart.data.labels = originalMonthlyLabels;
                    codeFrequencyChart.data.datasets[0].data = originalMonthlyAdditions;
                    codeFrequencyChart.data.datasets[1].data = originalMonthlyDeletions;
                    codeFrequencyChart.options.plugins.title.text = 'Code Frequency (追加・削除行数)';
                    codeFrequencyChart.update();
                }
            }
        }

        function clearFilters() {
            document.getElementById('contributorFilter').value = '';
            document.getElementById('repoFilter').value = '';
            document.getElementById('filterInfo').textContent = '';
            document.getElementById('filterInfo').style.display = 'none';

            // すべての行とカードを表示
            document.querySelectorAll('#contributorsTableBody tr').forEach((row, index) => {
                row.classList.remove('hidden');
                const rankCell = row.querySelector('.rank');
                if (rankCell) {
                    rankCell.textContent = index + 1;
                }
            });
            document.querySelectorAll('#repositoriesList .repo-card').forEach(card => {
                card.classList.remove('hidden');
            });

            // グラフを元に戻す
            updateCharts(new Set(), '', '');
        }

        // Enterキーでフィルタリング
        document.getElementById('contributorFilter').addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                applyFilters();
            }
        });

        // リポジトリ選択時に自動でフィルタリング
        document.getElementById('repoFilter').addEventListener('change', function() {
            applyFilters();
        });
    </script>
</body>
</html>'''

    template = Template(template_str)

    # チャート用のデータを準備
    monthly_labels = [d['month'] for d in monthly_data]
    monthly_prs_created = [d['prs_created'] for d in monthly_data]
    monthly_prs_merged = [d['prs_merged'] for d in monthly_data]
    monthly_contributors = [d['contributors'] for d in monthly_data]
    monthly_additions = [d['additions'] for d in monthly_data]
    monthly_deletions = [d['deletions'] for d in monthly_data]

    html = template.render(
        collected_at=data['collected_at'],
        start_date=data['start_date'],
        total_prs=aggregated['total_prs'],
        total_merged_prs=aggregated['total_merged_prs'],
        total_commits=aggregated['total_commits'],
        total_additions=aggregated['total_additions'],
        total_deletions=aggregated['total_deletions'],
        contributors_list=contributors_list,
        monthly_labels=monthly_labels,
        monthly_prs_created=monthly_prs_created,
        monthly_prs_merged=monthly_prs_merged,
        monthly_contributors=monthly_contributors,
        monthly_additions=monthly_additions,
        monthly_deletions=monthly_deletions,
        repositories=data['repositories'],
        devin_breakdown=devin_breakdown_aggregated,
        pr_data_for_charts=pr_data_for_charts
    )

    return html

def main():
    # データファイルを読み込み
    data_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'collected_data.json')
    if not os.path.exists(data_path):
        print(f"Error: Data file not found: {data_path}")
        print("Please run collect_data.py first")
        return

    with open(data_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # データを集計
    aggregated = aggregate_data(data)

    # HTMLを生成
    html = generate_html(data, aggregated)

    # HTMLファイルを保存
    output_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'docs', 'index.html')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"HTML generated successfully: {output_path}")

if __name__ == '__main__':
    main()
