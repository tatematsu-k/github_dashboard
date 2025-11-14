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
        'code_frequency': defaultdict(lambda: {'additions': 0, 'deletions': 0}),
        'monthly_contributions': defaultdict(lambda: defaultdict(lambda: {
            'commits': 0,
            'additions': 0,
            'deletions': 0,
            'prs_created': 0,
            'prs_merged': 0,
            'prs_reviewed': 0
        }))
    }

    for repo_data in data['repositories']:
        # PR統計
        aggregated['total_prs'] += len(repo_data['prs'])
        # マージ済みPR: stateが'merged'、またはmerged_atが存在する、またはmerged_byが存在する場合
        aggregated['total_merged_prs'] += sum(1 for pr in repo_data['prs'] if pr.get('state') == 'merged' or pr.get('merged_at') or pr.get('merged_by'))

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
            # contributorsがセットの場合は数値に変換
            contributors_count = stats['contributors']
            if isinstance(contributors_count, (set, list)):
                contributors_count = len(contributors_count)
            elif not isinstance(contributors_count, (int, float)):
                contributors_count = 0
            aggregated['monthly_stats'][month]['contributors'] = max(
                aggregated['monthly_stats'][month]['contributors'],
                contributors_count
            )

        # Code frequency
        for month, freq in repo_data['code_frequency'].items():
            aggregated['code_frequency'][month]['additions'] += freq['additions']
            aggregated['code_frequency'][month]['deletions'] += freq['deletions']

        # 月別コントリビューター統計
        if 'monthly_contributions' in repo_data:
            for month, contributors in repo_data['monthly_contributions'].items():
                for contributor, stats in contributors.items():
                    aggregated['monthly_contributions'][month][contributor]['commits'] += stats.get('commits', 0)
                    aggregated['monthly_contributions'][month][contributor]['additions'] += stats.get('additions', 0)
                    aggregated['monthly_contributions'][month][contributor]['deletions'] += stats.get('deletions', 0)
                    aggregated['monthly_contributions'][month][contributor]['prs_created'] += stats.get('prs_created', 0)
                    aggregated['monthly_contributions'][month][contributor]['prs_merged'] += stats.get('prs_merged', 0)
                    aggregated['monthly_contributions'][month][contributor]['prs_reviewed'] += stats.get('prs_reviewed', 0)

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
    # monthly_contributionsを通常の辞書に変換
    monthly_contributions_dict = {}
    for month, contributors in aggregated['monthly_contributions'].items():
        monthly_contributions_dict[month] = dict(contributors)
    aggregated['monthly_contributions'] = monthly_contributions_dict

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

    # 合計値を計算
    total_stats = {
        'commits': sum(c['commits'] for c in contributors_list),
        'prs_created': sum(c['prs_created'] for c in contributors_list),
        'prs_merged': sum(c['prs_merged'] for c in contributors_list),
        'prs_reviewed': sum(c['prs_reviewed'] for c in contributors_list),
        'additions': sum(c['additions'] for c in contributors_list),
        'deletions': sum(c['deletions'] for c in contributors_list),
        'repositories': len(set(repo for c in contributors_list for repo in c['repos_list']))
    }

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
        # contributorsがセットの場合は数値に変換
        contributors_count = monthly_stats.get('contributors', 0)
        if isinstance(contributors_count, (set, list)):
            contributors_count = len(contributors_count)
        elif not isinstance(contributors_count, (int, float)):
            contributors_count = 0
        monthly_stats['contributors'] = contributors_count

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
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <script defer src="https://cdn.jsdelivr.net/npm/alpinejs@3.x.x/dist/cdn.min.js"></script>
    <script>
        tailwind.config = {
            theme: {
                extend: {
                    colors: {
                        primary: '#667eea',
                    }
                }
            }
        }
    </script>
    <style>
        body {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        }
        .sortable {
            cursor: pointer;
            user-select: none;
            position: relative;
        }
        .sortable:hover {
            background-color: #f1f5f9;
        }
    </style>
</head>
<body x-data="dashboard()" class="min-h-screen p-5 text-gray-800">
    <div class="max-w-7xl mx-auto">
        <div class="bg-white rounded-xl p-8 mb-5 shadow-lg">
            <h1 class="text-primary text-3xl font-bold mb-2">📊 GitHub Dashboard - 分析レポート</h1>
            <p class="text-gray-600">収集日時: {{ collected_at }}</p>
            <p class="text-gray-600">分析期間: 直近1年間 ({{ start_date }} ～ {{ collected_at }})</p>
        </div>


        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5 mb-5">
            <div class="bg-white rounded-xl p-6 shadow-lg transition-transform hover:-translate-y-1">
                <h3 class="text-primary text-xs uppercase tracking-wide mb-2">総PR数</h3>
                <div class="text-4xl font-bold text-gray-800">{{ "{:,}".format(total_prs) }}</div>
            </div>
            <div class="bg-white rounded-xl p-6 shadow-lg transition-transform hover:-translate-y-1">
                <h3 class="text-primary text-xs uppercase tracking-wide mb-2">マージ済みPR</h3>
                <div class="text-4xl font-bold text-gray-800">{{ "{:,}".format(total_merged_prs) }}</div>
            </div>
            <div class="bg-white rounded-xl p-6 shadow-lg transition-transform hover:-translate-y-1">
                <h3 class="text-primary text-xs uppercase tracking-wide mb-2">総コミット数</h3>
                <div class="text-4xl font-bold text-gray-800">{{ "{:,}".format(total_commits) }}</div>
            </div>
            <div class="bg-white rounded-xl p-6 shadow-lg transition-transform hover:-translate-y-1">
                <h3 class="text-primary text-xs uppercase tracking-wide mb-2">追加行数</h3>
                <div class="text-4xl font-bold text-gray-800">{{ "{:,}".format(total_additions) }}</div>
            </div>
            <div class="bg-white rounded-xl p-6 shadow-lg transition-transform hover:-translate-y-1">
                <h3 class="text-primary text-xs uppercase tracking-wide mb-2">削除行数</h3>
                <div class="text-4xl font-bold text-gray-800">{{ "{:,}".format(total_deletions) }}</div>
            </div>
            <div class="bg-white rounded-xl p-6 shadow-lg transition-transform hover:-translate-y-1">
                <h3 class="text-primary text-xs uppercase tracking-wide mb-2">コントリビューター数</h3>
                <div class="text-4xl font-bold text-gray-800">{{ "{:,}".format(contributors_list|length) }}</div>
            </div>
        </div>

        <div class="bg-white rounded-xl p-8 mb-5 shadow-lg">
            <h2 class="text-primary text-2xl font-semibold mb-5 pb-3 border-b-2 border-gray-100">📈 月ごとの活動状況</h2>
            <div class="relative h-96 mb-8">
                <canvas id="monthlyChart"></canvas>
            </div>
        </div>

        <div class="bg-white rounded-xl p-8 mb-5 shadow-lg">
            <h2 class="text-primary text-2xl font-semibold mb-5 pb-3 border-b-2 border-gray-100">💻 Code Frequency (月ごと)</h2>
            <div class="relative h-96 mb-8">
                <canvas id="codeFrequencyChart"></canvas>
            </div>
        </div>

        <div class="bg-white rounded-xl p-8 mb-5 shadow-lg">
            <h2 class="text-primary text-2xl font-semibold mb-5 pb-3 border-b-2 border-gray-100">👥 コントリビューター別統計</h2>
            <div class="mb-5 flex items-center gap-4 flex-wrap">
                <div class="flex items-center gap-2">
                    <label for="monthFilter" class="font-semibold text-primary">月を選択:</label>
                    <select id="monthFilter" x-model="filters.month" @change="updateContributorsByMonth()" class="px-3 py-2 border-2 border-primary rounded-lg text-sm bg-white cursor-pointer focus:outline-none focus:ring-2 focus:ring-primary">
                    <option value="">すべての期間（累計）</option>
                    {% for month in monthly_labels %}
                    <option value="{{ month }}">{{ month }}</option>
                    {% endfor %}
                </select>
            </div>
                <div class="flex items-center gap-2" x-show="filters.month">
                    <input type="checkbox" id="showMonthOverMonth" x-model="filters.showMonthOverMonth" @change="updateContributorsByMonth()" class="w-4 h-4 text-primary border-gray-300 rounded focus:ring-primary cursor-pointer">
                    <label for="showMonthOverMonth" class="text-sm text-gray-700 cursor-pointer">前月比を表示</label>
                </div>
            </div>
            <div class="overflow-x-auto">
                <table class="w-full border-collapse mt-5">
                <thead>
                    <tr>
                            <th class="px-3 py-3 text-left border-b border-gray-200 bg-gray-50 text-primary font-semibold">順位</th>
                            <th class="px-3 py-3 text-left border-b border-gray-200 bg-gray-50 text-primary font-semibold">ユーザー名</th>
                            <th @click="sortTable('commits')" class="sortable px-3 py-3 text-right border-b border-gray-200 bg-gray-50 text-primary font-semibold">コミット</th>
                            <th @click="sortTable('prs_created')" class="sortable px-3 py-3 text-right border-b border-gray-200 bg-gray-50 text-primary font-semibold">PR作成</th>
                            <th @click="sortTable('prs_merged')" class="sortable px-3 py-3 text-right border-b border-gray-200 bg-gray-50 text-primary font-semibold">PRマージ</th>
                            <th @click="sortTable('prs_reviewed')" class="sortable px-3 py-3 text-right border-b border-gray-200 bg-gray-50 text-primary font-semibold">PRレビュー</th>
                            <th @click="sortTable('additions')" class="sortable px-3 py-3 text-right border-b border-gray-200 bg-gray-50 text-primary font-semibold">追加行数</th>
                            <th @click="sortTable('deletions')" class="sortable px-3 py-3 text-right border-b border-gray-200 bg-gray-50 text-primary font-semibold">削除行数</th>
                            <th @click="sortTable('repositories')" class="sortable px-3 py-3 text-right border-b border-gray-200 bg-gray-50 text-primary font-semibold">関与リポジトリ</th>
                    </tr>
                </thead>
                <tbody id="contributorsTableBody">
                    {% for contributor in contributors_list[:50] %}
                        <tr data-contributor="{{ contributor.name|lower }}" data-repos="{{ contributor.repos_list|join(',')|lower }}" data-all-stats='{{ contributor|tojson }}' class="hover:bg-gray-50">
                            <td class="rank px-3 py-3 border-b border-gray-100">{{ loop.index }}</td>
                            <td class="px-3 py-3 border-b border-gray-100"><strong>{{ contributor.name }}</strong>{% if contributor.devin_breakdown.prs_merged > 0 %}<br><span class="text-xs text-gray-600 font-normal">(内Devin PR{{ contributor.devin_breakdown.prs_merged }}, +{{ "{:,}".format(contributor.devin_breakdown.additions) }}/-{{ "{:,}".format(contributor.devin_breakdown.deletions) }})</span>{% endif %}</td>
                            <td class="stat-commits px-3 py-3 text-right border-b border-gray-100">{{ "{:,}".format(contributor.commits) }}</td>
                            <td class="stat-prs-created px-3 py-3 text-right border-b border-gray-100">{{ "{:,}".format(contributor.prs_created) }}</td>
                            <td class="stat-prs-merged px-3 py-3 text-right border-b border-gray-100">{{ "{:,}".format(contributor.prs_merged) }}</td>
                            <td class="stat-prs-reviewed px-3 py-3 text-right border-b border-gray-100">{{ "{:,}".format(contributor.prs_reviewed) }}</td>
                            <td class="stat-additions px-3 py-3 text-right border-b border-gray-100">{{ "{:,}".format(contributor.additions) }}</td>
                            <td class="stat-deletions px-3 py-3 text-right border-b border-gray-100">{{ "{:,}".format(contributor.deletions) }}</td>
                            <td class="px-3 py-3 text-right border-b border-gray-100">{{ "{:,}".format(contributor.repositories) }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
                <tfoot id="contributorsTableFooter">
                    <tr class="bg-gray-100 font-bold border-t-2 border-gray-300">
                        <td class="px-3 py-3 text-center" colspan="2">合計</td>
                        <td id="total-commits" class="px-3 py-3 text-right">{{ "{:,}".format(total_stats.commits) }}</td>
                        <td id="total-prs-created" class="px-3 py-3 text-right">{{ "{:,}".format(total_stats.prs_created) }}</td>
                        <td id="total-prs-merged" class="px-3 py-3 text-right">{{ "{:,}".format(total_stats.prs_merged) }}</td>
                        <td id="total-prs-reviewed" class="px-3 py-3 text-right">{{ "{:,}".format(total_stats.prs_reviewed) }}</td>
                        <td id="total-additions" class="px-3 py-3 text-right">{{ "{:,}".format(total_stats.additions) }}</td>
                        <td id="total-deletions" class="px-3 py-3 text-right">{{ "{:,}".format(total_stats.deletions) }}</td>
                        <td id="total-repositories" class="px-3 py-3 text-right">{{ "{:,}".format(total_stats.repositories) }}</td>
                    </tr>
                </tfoot>
            </table>
            </div>
        </div>

        <div class="bg-white rounded-xl p-8 mb-5 shadow-lg">
            <h2 class="text-primary text-2xl font-semibold mb-5 pb-3 border-b-2 border-gray-100">📦 対象リポジトリ</h2>
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mt-5" id="repositoriesList">
                {% for repo_data in repositories %}
                <div class="repo-card bg-gray-50 rounded-lg p-4 border-l-4 border-primary" data-repo="{{ repo_data.repository }}">
                    <h4 class="text-gray-800 font-semibold mb-2">{{ repo_data.repository }}</h4>
                    <div class="flex gap-4 text-sm text-gray-600">
                        <div class="flex items-center gap-1">
                            <span>PR:</span>
                            <strong>{{ repo_data.prs|length }}</strong>
                        </div>
                        <div class="flex items-center gap-1">
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
        // Alpine.jsの状態管理
        function dashboard() {
            return {
                filters: {
                    month: '',
                    showMonthOverMonth: true
                },
                sortColumn: null,
                sortDirection: 'desc',
                monthlyChart: null,
                codeFrequencyChart: null,
                allContributors: [],
                monthlyContributionsData: {{ monthly_contributions_data|tojson }},
                allPRData: {{ pr_data_for_charts|tojson }},

                init() {
                    // グローバルインスタンスとして保存（updateChartsGlobalからアクセス可能にするため）
                    window.dashboardInstance = this;
                    // チャートを初期化
                    this.initCharts();
                    // コントリビューターリストを初期化
                    this.initContributors();
                },

                initCharts() {
        // 月ごとの活動状況チャート
        const monthlyCtx = document.getElementById('monthlyChart').getContext('2d');
                    this.monthlyChart = new Chart(monthlyCtx, {
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
                        label: '1人あたりのPR作成数',
                        data: {{ monthly_prs_created_per_contributor|tojson }},
                        borderColor: 'rgb(139, 92, 246)',
                        backgroundColor: 'rgba(139, 92, 246, 0.1)',
                        tension: 0.4,
                        borderDash: [5, 5]
                    },
                    {
                        label: '1人あたりのPRマージ数',
                        data: {{ monthly_prs_merged_per_contributor|tojson }},
                        borderColor: 'rgb(34, 197, 94)',
                        backgroundColor: 'rgba(34, 197, 94, 0.1)',
                        tension: 0.4,
                        borderDash: [5, 5]
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                                legend: { position: 'top' },
                                title: { display: true, text: '月ごとの活動状況' }
                            },
                            scales: { y: { beginAtZero: true } }
            }
        });

        // Code Frequencyチャート
        const codeFreqCtx = document.getElementById('codeFrequencyChart').getContext('2d');
                    this.codeFrequencyChart = new Chart(codeFreqCtx, {
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
                                legend: { position: 'top' },
                                title: { display: true, text: 'Code Frequency (追加・削除行数)' }
                            },
                            scales: { y: { beginAtZero: true } }
                        }
                    });
                },

                initContributors() {
                    const rows = document.querySelectorAll('#contributorsTableBody tr');
                    this.allContributors = Array.from(rows).map(row => ({
                        element: row,
                        name: row.getAttribute('data-contributor') || '',
                        repos: (row.getAttribute('data-repos') || '').toLowerCase(),
                        stats: JSON.parse(row.getAttribute('data-all-stats') || '{}')
                    }));

                    // 初期合計値を更新
                    this.updateContributorsByMonth();
                },

                sortTable(column) {
                    if (this.sortColumn === column) {
                        this.sortDirection = this.sortDirection === 'asc' ? 'desc' : 'asc';
                    } else {
                        this.sortColumn = column;
                        this.sortDirection = 'desc';
                    }

                    this.allContributors.sort((a, b) => {
                        const aStats = this.getStatsForMonth(a.stats, this.filters.month);
                        const bStats = this.getStatsForMonth(b.stats, this.filters.month);
                        let aVal, bVal;

                        if (column === 'repositories') {
                            aVal = aStats.repositories || (aStats.repos_list ? aStats.repos_list.length : 0);
                            bVal = bStats.repositories || (bStats.repos_list ? bStats.repos_list.length : 0);
                } else {
                            aVal = aStats[column] || 0;
                            bVal = bStats[column] || 0;
                        }

                        return this.sortDirection === 'asc' ? aVal - bVal : bVal - aVal;
                    });

                    // DOMを再配置
                    const tbody = document.querySelector('#contributorsTableBody');
                    this.allContributors.forEach(contributor => {
                        tbody.appendChild(contributor.element);
                    });

                    // 合計値を更新
                    this.updateContributorsByMonth();
                },

                getStatsForMonth(stats, month) {
                    if (!month || !this.monthlyContributionsData[month]) {
                        return stats;
                    }
                    const monthly = this.monthlyContributionsData[month][stats.name] || {};
                    return { ...stats, ...monthly };
                },

                getPreviousMonth(month) {
                    if (!month) return null;
                    const [year, monthNum] = month.split('-').map(Number);
                    let prevYear = year;
                    let prevMonth = monthNum - 1;
                    if (prevMonth < 1) {
                        prevMonth = 12;
                        prevYear -= 1;
                    }
                    return `${prevYear}-${String(prevMonth).padStart(2, '0')}`;
                },

                calculateMonthOverMonth(current, previous) {
                    if (previous === 0 && current > 0) {
                        return { value: '+∞', isPositive: true };
                    }
                    if (previous === 0 && current === 0) {
                        return null;
                    }
                    const diff = current - previous;
                    const percent = previous !== 0 ? ((diff / previous) * 100).toFixed(1) : '0.0';
                    const sign = diff >= 0 ? '+' : '';
                    return {
                        value: `${sign}${percent}%`,
                        isPositive: diff >= 0
                    };
                },

                updateContributorsByMonth() {
                    const selectedMonth = this.filters.month;
                    const previousMonth = selectedMonth ? this.getPreviousMonth(selectedMonth) : null;

                    // 合計値を計算するための変数
                    let totalCommits = 0;
                    let totalPRsCreated = 0;
                    let totalPRsMerged = 0;
                    let totalPRsReviewed = 0;
                    let totalAdditions = 0;
                    let totalDeletions = 0;
                    const uniqueRepos = new Set();

                    this.allContributors.forEach(contributor => {
                        const stats = this.getStatsForMonth(contributor.stats, selectedMonth);
                        const prevStats = previousMonth ? this.getStatsForMonth(contributor.stats, previousMonth) : null;
                        const row = contributor.element;

                        // 統計値を更新
                        const commitsCell = row.querySelector('.stat-commits');
                        const prsCreatedCell = row.querySelector('.stat-prs-created');
                        const prsMergedCell = row.querySelector('.stat-prs-merged');
                        const prsReviewedCell = row.querySelector('.stat-prs-reviewed');
                        const additionsCell = row.querySelector('.stat-additions');
                        const deletionsCell = row.querySelector('.stat-deletions');

                        const updateCellWithComparison = (cell, currentValue, prevValue, formatFn = (v) => v) => {
                            if (!cell) return;
                            const current = currentValue || 0;
                            const previous = prevValue || 0;
                            let html = formatFn(current);

                            if (selectedMonth && previousMonth && this.monthlyContributionsData[previousMonth] && this.filters.showMonthOverMonth) {
                                const comparison = this.calculateMonthOverMonth(current, previous);
                                if (comparison) {
                                    const colorClass = comparison.isPositive ? 'text-green-600' : 'text-red-600';
                                    html += `<br><span class="text-xs ${colorClass}">(${comparison.value})</span>`;
                                }
                            }
                            cell.innerHTML = html;
                        };

                        updateCellWithComparison(commitsCell, stats.commits, prevStats?.commits, (v) => v.toLocaleString());
                        updateCellWithComparison(prsCreatedCell, stats.prs_created, prevStats?.prs_created, (v) => v.toLocaleString());
                        updateCellWithComparison(prsMergedCell, stats.prs_merged, prevStats?.prs_merged, (v) => v.toLocaleString());
                        updateCellWithComparison(prsReviewedCell, stats.prs_reviewed, prevStats?.prs_reviewed, (v) => v.toLocaleString());
                        updateCellWithComparison(additionsCell, stats.additions, prevStats?.additions, (v) => v.toLocaleString());
                        updateCellWithComparison(deletionsCell, stats.deletions, prevStats?.deletions, (v) => v.toLocaleString());

                        // 合計値に加算
                        totalCommits += stats.commits || 0;
                        totalPRsCreated += stats.prs_created || 0;
                        totalPRsMerged += stats.prs_merged || 0;
                        totalPRsReviewed += stats.prs_reviewed || 0;
                        totalAdditions += stats.additions || 0;
                        totalDeletions += stats.deletions || 0;

                        // リポジトリのユニーク数を計算
                        if (stats.repos_list && Array.isArray(stats.repos_list)) {
                            stats.repos_list.forEach(repo => uniqueRepos.add(repo));
                        }
                    });

                    // 合計行を更新
                    this.updateTotalRow({
                        commits: totalCommits,
                        prs_created: totalPRsCreated,
                        prs_merged: totalPRsMerged,
                        prs_reviewed: totalPRsReviewed,
                        additions: totalAdditions,
                        deletions: totalDeletions,
                        repositories: uniqueRepos.size
                    });
                },

                updateTotalRow(totals) {
                    const commitsCell = document.getElementById('total-commits');
                    const prsCreatedCell = document.getElementById('total-prs-created');
                    const prsMergedCell = document.getElementById('total-prs-merged');
                    const prsReviewedCell = document.getElementById('total-prs-reviewed');
                    const additionsCell = document.getElementById('total-additions');
                    const deletionsCell = document.getElementById('total-deletions');
                    const repositoriesCell = document.getElementById('total-repositories');

                    if (commitsCell) commitsCell.textContent = (totals.commits || 0).toLocaleString();
                    if (prsCreatedCell) prsCreatedCell.textContent = (totals.prs_created || 0).toLocaleString();
                    if (prsMergedCell) prsMergedCell.textContent = (totals.prs_merged || 0).toLocaleString();
                    if (prsReviewedCell) prsReviewedCell.textContent = (totals.prs_reviewed || 0).toLocaleString();
                    if (additionsCell) additionsCell.textContent = (totals.additions || 0).toLocaleString();
                    if (deletionsCell) deletionsCell.textContent = (totals.deletions || 0).toLocaleString();
                    if (repositoriesCell) repositoriesCell.textContent = (totals.repositories || 0).toLocaleString();
                }
            }
        }

        // PRデータをJavaScriptで利用可能にする
        const allPRData = {{ pr_data_for_charts|tojson }};
        const monthlyContributionsData = {{ monthly_contributions_data|tojson }};
        const allContributorsData = {{ contributors_list|tojson }};

        // グラフを更新する関数（Alpine.jsから呼び出し可能）
        function updateChartsGlobal() {
            // 元のデータを保持
            const originalMonthlyLabels = {{ monthly_labels|tojson }};
            const originalMonthlyPRsCreated = {{ monthly_prs_created|tojson }};
            const originalMonthlyPRsMerged = {{ monthly_prs_merged|tojson }};
            const originalMonthlyAdditions = {{ monthly_additions|tojson }};
            const originalMonthlyDeletions = {{ monthly_deletions|tojson }};

            // 常に元のデータを表示
            if (window.dashboardInstance && window.dashboardInstance.monthlyChart) {
                const originalMonthlyPRsCreatedPerContributor = {{ monthly_prs_created_per_contributor|tojson }};
                const originalMonthlyPRsMergedPerContributor = {{ monthly_prs_merged_per_contributor|tojson }};
                window.dashboardInstance.monthlyChart.data.labels = originalMonthlyLabels;
                window.dashboardInstance.monthlyChart.data.datasets[0].data = originalMonthlyPRsCreated;
                window.dashboardInstance.monthlyChart.data.datasets[1].data = originalMonthlyPRsMerged;
                window.dashboardInstance.monthlyChart.data.datasets[2].data = originalMonthlyPRsCreatedPerContributor;
                window.dashboardInstance.monthlyChart.data.datasets[3].data = originalMonthlyPRsMergedPerContributor;
                window.dashboardInstance.monthlyChart.options.plugins.title.text = '月ごとの活動状況';
                window.dashboardInstance.monthlyChart.update();
            }
            if (window.dashboardInstance && window.dashboardInstance.codeFrequencyChart) {
                window.dashboardInstance.codeFrequencyChart.data.labels = originalMonthlyLabels;
                window.dashboardInstance.codeFrequencyChart.data.datasets[0].data = originalMonthlyAdditions;
                window.dashboardInstance.codeFrequencyChart.data.datasets[1].data = originalMonthlyDeletions;
                window.dashboardInstance.codeFrequencyChart.options.plugins.title.text = 'Code Frequency (追加・削除行数)';
                window.dashboardInstance.codeFrequencyChart.update();
            }
        }

    </script>
    </body>
    </html>'''

    template = Template(template_str)

    # チャート用のデータを準備
    monthly_labels = [d['month'] for d in monthly_data]
    monthly_prs_created = [d['prs_created'] for d in monthly_data]
    monthly_prs_merged = [d['prs_merged'] for d in monthly_data]
    monthly_contributors = [d['contributors'] for d in monthly_data]

    # monthly_contributionsから正確なコントリビューター数を計算
    monthly_contributors_from_contributions = []
    for month in monthly_labels:
        contributors_set = set()
        if month in aggregated['monthly_contributions']:
            contributors_set = set(aggregated['monthly_contributions'][month].keys())
        monthly_contributors_from_contributions.append(len(contributors_set))

    # コントリビューター1人あたりのPR作成数・マージ数を計算（monthly_contributionsから計算したコントリビューター数を使用）
    monthly_prs_created_per_contributor = [
        round(prs / contributors, 2) if contributors > 0 else 0
        for prs, contributors in zip(monthly_prs_created, monthly_contributors_from_contributions)
    ]
    monthly_prs_merged_per_contributor = [
        round(prs / contributors, 2) if contributors > 0 else 0
        for prs, contributors in zip(monthly_prs_merged, monthly_contributors_from_contributions)
    ]
    monthly_additions = [d['additions'] for d in monthly_data]
    monthly_deletions = [d['deletions'] for d in monthly_data]

    # 月別コントリビューター統計データを準備（JavaScript用）
    monthly_contributions_data = aggregated.get('monthly_contributions', {})

    html = template.render(
        collected_at=data['collected_at'],
        start_date=data['start_date'],
        total_prs=aggregated['total_prs'],
        total_merged_prs=aggregated['total_merged_prs'],
        total_commits=aggregated['total_commits'],
        total_additions=aggregated['total_additions'],
        total_deletions=aggregated['total_deletions'],
        contributors_list=contributors_list,
        total_stats=total_stats,
        monthly_labels=monthly_labels,
        monthly_prs_created=monthly_prs_created,
        monthly_prs_merged=monthly_prs_merged,
        monthly_contributors=monthly_contributors,
        monthly_prs_created_per_contributor=monthly_prs_created_per_contributor,
        monthly_prs_merged_per_contributor=monthly_prs_merged_per_contributor,
        monthly_additions=monthly_additions,
        monthly_deletions=monthly_deletions,
        repositories=data['repositories'],
        devin_breakdown=devin_breakdown_aggregated,
        pr_data_for_charts=pr_data_for_charts,
        monthly_contributions_data=monthly_contributions_data,
        monthly_stats_data=aggregated['monthly_stats']
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
