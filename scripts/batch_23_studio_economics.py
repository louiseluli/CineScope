#!/usr/bin/env python3
"""
================================================================================
CINESCOPE BATCH 23: STUDIO ECONOMICS ANALYSIS
================================================================================

Focus: Production company financial performance and market dynamics

Data Sources:
- Production company data (TMDB)
- Budget and revenue figures
- Release dates and timing
- Genre and critical reception

Visualizations (12):
1. studio_market_share.png - Market share by revenue
2. studio_roi_performance.png - Return on investment by studio
3. studio_genre_specialization.png - Which studios dominate which genres
4. studio_budget_trends.png - Budget evolution over time
5. studio_hit_rate.png - Success rate analysis
6. studio_quality_vs_profit.png - Ratings vs profitability
7. studio_portfolio_diversity.png - Genre diversity metrics
8. major_vs_independent.png - Major studios vs independents
9. studio_decade_performance.png - Performance across eras
10. studio_collaboration_networks.png - Co-production patterns
11. studio_financial_health.png - Profitability trends
12. studio_risk_analysis.png - Budget risk vs reward

================================================================================
"""

import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime
from collections import defaultdict, Counter

class StudioEconomicsAnalyzer:
    """Analyzes production company economics and market dynamics."""

    def __init__(self):
        """Initialize the analyzer."""
        self.base_dir = Path(__file__).parent.parent
        self.data_dir = self.base_dir / 'data'
        self.viz_dir = self.base_dir / 'analysis_outputs' / 'visualizations' / 'batch_23'
        self.reports_dir = self.base_dir / 'analysis_outputs' / 'reports'

        # Create output directories
        self.viz_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)

        self.movies_df = None
        self.stats = {}

    def print_header(self):
        """Print analysis header."""
        print("="*80)
        print("CINESCOPE BATCH 23: STUDIO ECONOMICS ANALYSIS")
        print("="*80)
        print()

    def load_data(self):
        """Load all required data files."""
        print("Loading data files...")

        # Load movies
        movies_path = self.data_dir / 'processed' / 'watched_movies_master.csv'
        self.movies_df = pd.read_csv(movies_path)
        print(f"✓ Loaded {len(self.movies_df):,} movies")

        print()

    def visualize_market_share(self):
        """Analyze market share by revenue."""
        print("Creating market share visualization...")

        # Parse production companies and aggregate revenue
        studio_revenue = defaultdict(float)
        studio_movie_count = defaultdict(int)

        for idx, row in self.movies_df.iterrows():
            companies_str = row.get('tmdb_production_companies', '')
            revenue = row.get('tmdb_revenue', 0)

            if pd.notna(companies_str) and companies_str and revenue > 0:
                # Parse company names
                companies = []
                if '|' in str(companies_str):
                    companies = [c.strip() for c in str(companies_str).split('|')]
                else:
                    companies = [str(companies_str).strip()]

                # Distribute revenue equally among companies
                revenue_share = revenue / len(companies) if companies else 0

                for company in companies:
                    if company:
                        studio_revenue[company] += revenue_share
                        studio_movie_count[company] += 1

        # Get top studios by revenue
        top_studios = sorted(studio_revenue.items(), key=lambda x: x[1], reverse=True)[:15]

        if not top_studios:
            print("! No studio revenue data available")
            return

        df = pd.DataFrame(top_studios, columns=['Studio', 'Revenue'])
        df['Revenue_M'] = df['Revenue'] / 1e6
        df['Movie_Count'] = df['Studio'].map(studio_movie_count)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))

        # Market share pie chart
        total_revenue = df['Revenue'].sum()
        top_10_revenue = df.head(10)['Revenue'].sum()
        other_revenue = total_revenue - top_10_revenue

        pie_data = list(df.head(10)['Revenue']) + [other_revenue]
        pie_labels = list(df.head(10)['Studio']) + ['Others']
        colors = plt.cm.Set3(np.linspace(0, 1, len(pie_data)))

        wedges, texts, autotexts = ax1.pie(pie_data, labels=pie_labels, autopct='%1.1f%%',
                                           colors=colors, startangle=90)
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(9)

        ax1.set_title('Market Share by Revenue (Top 10 Studios)', fontsize=14, fontweight='bold')

        # Revenue bar chart
        colors = plt.cm.Blues(np.linspace(0.4, 0.9, len(df)))

        ax2.barh(range(len(df)), df['Revenue_M'], color=colors, alpha=0.7, edgecolor='black')
        ax2.set_yticks(range(len(df)))
        ax2.set_yticklabels(df['Studio'], fontsize=9)
        ax2.invert_yaxis()
        ax2.set_xlabel('Total Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax2.set_title('Top 15 Studios by Total Revenue', fontsize=14, fontweight='bold')
        ax2.grid(axis='x', alpha=0.3)

        for i, (revenue, count) in enumerate(zip(df['Revenue_M'], df['Movie_Count'])):
            ax2.text(revenue, i, f' ${revenue:.0f}M ({count} films)', va='center', fontsize=8)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'studio_market_share.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: studio_market_share.png")

    def visualize_roi_performance(self):
        """Analyze return on investment by studio."""
        print("Creating ROI performance visualization...")

        # Calculate ROI for each studio
        studio_financials = defaultdict(lambda: {'budgets': [], 'revenues': [], 'rois': []})

        for idx, row in self.movies_df.iterrows():
            companies_str = row.get('tmdb_production_companies', '')
            budget = row.get('tmdb_budget', 0)
            revenue = row.get('tmdb_revenue', 0)

            if pd.notna(companies_str) and companies_str and budget > 0 and revenue > 0:
                roi = ((revenue - budget) / budget) * 100

                companies = []
                if '|' in str(companies_str):
                    companies = [c.strip() for c in str(companies_str).split('|')]
                else:
                    companies = [str(companies_str).strip()]

                for company in companies:
                    if company:
                        studio_financials[company]['budgets'].append(budget)
                        studio_financials[company]['revenues'].append(revenue)
                        studio_financials[company]['rois'].append(roi)

        # Calculate average ROI
        studio_stats = []
        for studio, data in studio_financials.items():
            if len(data['rois']) >= 3:  # Minimum 3 films
                studio_stats.append({
                    'studio': studio[:35],
                    'avg_roi': np.mean(data['rois']),
                    'median_roi': np.median(data['rois']),
                    'total_budget': sum(data['budgets']),
                    'total_revenue': sum(data['revenues']),
                    'film_count': len(data['rois'])
                })

        if not studio_stats:
            print("! No ROI data available")
            return

        df = pd.DataFrame(studio_stats).sort_values('avg_roi', ascending=False).head(20)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))

        # Top ROI studios
        colors = plt.cm.Greens(np.linspace(0.4, 0.9, len(df)))

        ax1.barh(range(len(df)), df['avg_roi'], color=colors, alpha=0.7, edgecolor='black')
        ax1.set_yticks(range(len(df)))
        ax1.set_yticklabels(df['studio'], fontsize=9)
        ax1.invert_yaxis()
        ax1.set_xlabel('Average ROI (%)', fontsize=12, fontweight='bold')
        ax1.set_title('Top 20 Studios by Average ROI', fontsize=14, fontweight='bold')
        ax1.grid(axis='x', alpha=0.3)
        ax1.axvline(0, color='red', linestyle='--', linewidth=1)

        for i, (roi, count) in enumerate(zip(df['avg_roi'], df['film_count'])):
            ax1.text(roi, i, f' {roi:.0f}% ({count} films)', va='center', fontsize=8)

        # Budget vs Revenue scatter
        ax2.scatter(df['total_budget']/1e6, df['total_revenue']/1e6,
                   s=df['film_count']*20, alpha=0.6, c=df['avg_roi'], cmap='RdYlGn')

        # Add break-even line
        max_val = max(df['total_budget'].max(), df['total_revenue'].max()) / 1e6
        ax2.plot([0, max_val], [0, max_val], 'r--', linewidth=2, label='Break-even', alpha=0.7)

        ax2.set_xlabel('Total Budget (Millions $)', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Total Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax2.set_title('Studio Budget vs Revenue (bubble size = film count)', fontsize=14, fontweight='bold')
        ax2.grid(alpha=0.3)
        ax2.legend()

        # Add colorbar
        sm = plt.cm.ScalarMappable(cmap='RdYlGn', norm=plt.Normalize(vmin=df['avg_roi'].min(), vmax=df['avg_roi'].max()))
        sm.set_array([])
        cbar = plt.colorbar(sm, ax=ax2)
        cbar.set_label('Avg ROI (%)', fontsize=10)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'studio_roi_performance.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: studio_roi_performance.png")

    def visualize_genre_specialization(self):
        """Analyze which studios dominate which genres."""
        print("Creating genre specialization visualization...")

        # Track studio-genre combinations
        studio_genre_counts = defaultdict(lambda: defaultdict(int))
        studio_totals = defaultdict(int)

        for idx, row in self.movies_df.iterrows():
            companies_str = row.get('tmdb_production_companies', '')
            genres_str = row.get('tmdb_genres', row.get('Genres', ''))

            if pd.notna(companies_str) and companies_str and pd.notna(genres_str) and genres_str:
                companies = []
                if '|' in str(companies_str):
                    companies = [c.strip() for c in str(companies_str).split('|')]
                else:
                    companies = [str(companies_str).strip()]

                genres = []
                if '|' in str(genres_str):
                    genres = [g.strip() for g in str(genres_str).split('|')]
                elif ',' in str(genres_str):
                    genres = [g.strip() for g in str(genres_str).split(',')]
                else:
                    genres = [str(genres_str).strip()]

                for company in companies:
                    if company:
                        studio_totals[company] += 1
                        for genre in genres:
                            if genre:
                                studio_genre_counts[company][genre] += 1

        # Get top studios
        top_studios = sorted(studio_totals.items(), key=lambda x: x[1], reverse=True)[:12]
        top_studio_names = [s[0] for s in top_studios]

        # Get top genres
        all_genres = set()
        for studio_genres in studio_genre_counts.values():
            all_genres.update(studio_genres.keys())

        # Create matrix
        genre_list = sorted(all_genres)[:10]  # Top 10 genres
        matrix = np.zeros((len(top_studio_names), len(genre_list)))

        for i, studio in enumerate(top_studio_names):
            for j, genre in enumerate(genre_list):
                count = studio_genre_counts[studio][genre]
                total = studio_totals[studio]
                matrix[i, j] = (count / total * 100) if total > 0 else 0

        fig, ax = plt.subplots(figsize=(14, 10))

        im = ax.imshow(matrix, cmap='YlOrRd', aspect='auto')

        ax.set_xticks(range(len(genre_list)))
        ax.set_xticklabels(genre_list, rotation=45, ha='right')
        ax.set_yticks(range(len(top_studio_names)))
        ax.set_yticklabels([s[:30] for s in top_studio_names], fontsize=9)

        ax.set_xlabel('Genre', fontsize=12, fontweight='bold')
        ax.set_ylabel('Studio', fontsize=12, fontweight='bold')
        ax.set_title('Studio Genre Specialization (%  of Portfolio)', fontsize=14, fontweight='bold')

        # Add text annotations
        for i in range(len(top_studio_names)):
            for j in range(len(genre_list)):
                if matrix[i, j] > 0:
                    text = ax.text(j, i, f'{matrix[i, j]:.0f}%',
                                 ha="center", va="center", color="black" if matrix[i, j] < 50 else "white",
                                 fontsize=8)

        cbar = plt.colorbar(im, ax=ax)
        cbar.set_label('Percentage of Studio Portfolio', fontsize=10)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'studio_genre_specialization.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: studio_genre_specialization.png")

    def visualize_budget_trends(self):
        """Analyze budget evolution over time."""
        print("Creating budget trends visualization...")

        # Track studio budgets by decade
        studio_decade_budgets = defaultdict(lambda: defaultdict(list))

        for idx, row in self.movies_df.iterrows():
            companies_str = row.get('tmdb_production_companies', '')
            budget = row.get('tmdb_budget', 0)
            year = row.get('Year', 0)

            if pd.notna(companies_str) and companies_str and budget > 0 and year > 0:
                decade = (year // 10) * 10

                companies = []
                if '|' in str(companies_str):
                    companies = [c.strip() for c in str(companies_str).split('|')][:1]  # Take first company
                else:
                    companies = [str(companies_str).strip()]

                for company in companies:
                    if company:
                        studio_decade_budgets[company][decade].append(budget)

        # Get top 8 studios by total films
        studio_film_counts = {studio: sum(len(budgets) for budgets in decades.values())
                             for studio, decades in studio_decade_budgets.items()}
        top_studios = sorted(studio_film_counts.items(), key=lambda x: x[1], reverse=True)[:8]
        top_studio_names = [s[0] for s in top_studios]

        fig, axes = plt.subplots(2, 4, figsize=(18, 10))
        axes = axes.flatten()

        for idx, studio in enumerate(top_studio_names):
            decades_data = studio_decade_budgets[studio]
            decades = sorted(decades_data.keys())
            avg_budgets = [np.mean(decades_data[d])/1e6 for d in decades]

            axes[idx].plot(decades, avg_budgets, marker='o', linewidth=2, markersize=8, color='steelblue')
            axes[idx].set_title(studio[:25], fontsize=11, fontweight='bold')
            axes[idx].set_xlabel('Decade', fontsize=9)
            axes[idx].set_ylabel('Avg Budget ($M)', fontsize=9)
            axes[idx].grid(alpha=0.3)
            axes[idx].tick_params(labelsize=8)

        plt.suptitle('Studio Budget Evolution Over Decades', fontsize=16, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.viz_dir / 'studio_budget_trends.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: studio_budget_trends.png")

    def visualize_hit_rate(self):
        """Analyze success rate (profitable films percentage)."""
        print("Creating hit rate visualization...")

        # Calculate hit rate for each studio
        studio_performance = defaultdict(lambda: {'total': 0, 'profitable': 0, 'revenues': []})

        for idx, row in self.movies_df.iterrows():
            companies_str = row.get('tmdb_production_companies', '')
            budget = row.get('tmdb_budget', 0)
            revenue = row.get('tmdb_revenue', 0)

            if pd.notna(companies_str) and companies_str and budget > 0 and revenue > 0:
                is_profitable = revenue > budget

                companies = []
                if '|' in str(companies_str):
                    companies = [c.strip() for c in str(companies_str).split('|')]
                else:
                    companies = [str(companies_str).strip()]

                for company in companies:
                    if company:
                        studio_performance[company]['total'] += 1
                        studio_performance[company]['revenues'].append(revenue)
                        if is_profitable:
                            studio_performance[company]['profitable'] += 1

        # Calculate hit rates
        studio_stats = []
        for studio, data in studio_performance.items():
            if data['total'] >= 5:  # Minimum 5 films
                hit_rate = (data['profitable'] / data['total']) * 100
                studio_stats.append({
                    'studio': studio[:35],
                    'hit_rate': hit_rate,
                    'total_films': data['total'],
                    'profitable_films': data['profitable'],
                    'avg_revenue': np.mean(data['revenues'])
                })

        if not studio_stats:
            print("! No hit rate data available")
            return

        df = pd.DataFrame(studio_stats).sort_values('hit_rate', ascending=False).head(20)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))

        # Hit rate bar chart
        colors = plt.cm.RdYlGn(df['hit_rate'] / 100)

        ax1.barh(range(len(df)), df['hit_rate'], color=colors, alpha=0.7, edgecolor='black')
        ax1.set_yticks(range(len(df)))
        ax1.set_yticklabels(df['studio'], fontsize=9)
        ax1.invert_yaxis()
        ax1.set_xlabel('Hit Rate (%)', fontsize=12, fontweight='bold')
        ax1.set_title('Top 20 Studios by Profitability Hit Rate', fontsize=14, fontweight='bold')
        ax1.grid(axis='x', alpha=0.3)
        ax1.axvline(50, color='gray', linestyle='--', linewidth=1, alpha=0.5)

        for i, (rate, total, profitable) in enumerate(zip(df['hit_rate'], df['total_films'], df['profitable_films'])):
            ax1.text(rate, i, f' {rate:.1f}% ({profitable}/{total})', va='center', fontsize=8)

        # Hit rate vs film count scatter
        ax2.scatter(df['total_films'], df['hit_rate'], s=df['avg_revenue']/1e6,
                   alpha=0.6, c=df['hit_rate'], cmap='RdYlGn')

        ax2.set_xlabel('Total Films', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Hit Rate (%)', fontsize=12, fontweight='bold')
        ax2.set_title('Hit Rate vs Portfolio Size (bubble size = avg revenue)',
                     fontsize=14, fontweight='bold')
        ax2.grid(alpha=0.3)
        ax2.axhline(50, color='gray', linestyle='--', linewidth=1, alpha=0.5)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'studio_hit_rate.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: studio_hit_rate.png")

    def visualize_quality_vs_profit(self):
        """Compare ratings vs profitability."""
        print("Creating quality vs profit visualization...")

        # Calculate quality and profit metrics for each studio
        studio_metrics = defaultdict(lambda: {'ratings': [], 'rois': [], 'budgets': []})

        for idx, row in self.movies_df.iterrows():
            companies_str = row.get('tmdb_production_companies', '')
            rating = row.get('IMDb Rating', 0)
            budget = row.get('tmdb_budget', 0)
            revenue = row.get('tmdb_revenue', 0)

            if pd.notna(companies_str) and companies_str and rating > 0 and budget > 0 and revenue > 0:
                roi = ((revenue - budget) / budget) * 100

                companies = []
                if '|' in str(companies_str):
                    companies = [c.strip() for c in str(companies_str).split('|')]
                else:
                    companies = [str(companies_str).strip()]

                for company in companies:
                    if company:
                        studio_metrics[company]['ratings'].append(rating)
                        studio_metrics[company]['rois'].append(roi)
                        studio_metrics[company]['budgets'].append(budget)

        # Calculate averages
        studio_stats = []
        for studio, data in studio_metrics.items():
            if len(data['ratings']) >= 3:
                studio_stats.append({
                    'studio': studio[:25],
                    'avg_rating': np.mean(data['ratings']),
                    'avg_roi': np.mean(data['rois']),
                    'total_budget': sum(data['budgets']),
                    'film_count': len(data['ratings'])
                })

        if not studio_stats:
            print("! No quality vs profit data available")
            return

        df = pd.DataFrame(studio_stats)

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

        # Scatter: Rating vs ROI
        ax1.scatter(df['avg_rating'], df['avg_roi'], s=df['film_count']*10,
                   alpha=0.6, c=df['total_budget']/1e6, cmap='viridis')
        ax1.set_xlabel('Average Rating', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Average ROI (%)', fontsize=12, fontweight='bold')
        ax1.set_title('Studio Quality vs Profitability', fontsize=14, fontweight='bold')
        ax1.grid(alpha=0.3)
        ax1.axhline(0, color='red', linestyle='--', linewidth=1, alpha=0.5)

        # Add quadrant labels
        ax1.text(0.95, 0.95, 'High Quality\nHigh Profit', transform=ax1.transAxes,
                ha='right', va='top', fontsize=9, bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.5))
        ax1.text(0.05, 0.05, 'Low Quality\nLow Profit', transform=ax1.transAxes,
                ha='left', va='bottom', fontsize=9, bbox=dict(boxstyle='round', facecolor='lightcoral', alpha=0.5))

        # Top rated studios
        top_rated = df.nlargest(15, 'avg_rating')
        colors = plt.cm.Greens(np.linspace(0.4, 0.9, len(top_rated)))

        ax2.barh(range(len(top_rated)), top_rated['avg_rating'], color=colors,
                alpha=0.7, edgecolor='black')
        ax2.set_yticks(range(len(top_rated)))
        ax2.set_yticklabels(top_rated['studio'], fontsize=9)
        ax2.invert_yaxis()
        ax2.set_xlabel('Average Rating', fontsize=12, fontweight='bold')
        ax2.set_title('Top 15 Studios by Quality', fontsize=14, fontweight='bold')
        ax2.grid(axis='x', alpha=0.3)

        for i, (rating, count) in enumerate(zip(top_rated['avg_rating'], top_rated['film_count'])):
            ax2.text(rating, i, f' {rating:.2f} ({count} films)', va='center', fontsize=8)

        # Most profitable studios
        top_profitable = df.nlargest(15, 'avg_roi')
        colors = plt.cm.Blues(np.linspace(0.4, 0.9, len(top_profitable)))

        ax3.barh(range(len(top_profitable)), top_profitable['avg_roi'], color=colors,
                alpha=0.7, edgecolor='black')
        ax3.set_yticks(range(len(top_profitable)))
        ax3.set_yticklabels(top_profitable['studio'], fontsize=9)
        ax3.invert_yaxis()
        ax3.set_xlabel('Average ROI (%)', fontsize=12, fontweight='bold')
        ax3.set_title('Top 15 Studios by Profitability', fontsize=14, fontweight='bold')
        ax3.grid(axis='x', alpha=0.3)

        for i, (roi, count) in enumerate(zip(top_profitable['avg_roi'], top_profitable['film_count'])):
            ax3.text(roi, i, f' {roi:.0f}% ({count} films)', va='center', fontsize=8)

        # Best of both worlds
        df['quality_profit_score'] = (df['avg_rating'] * 10) + (df['avg_roi'] / 10)
        best_overall = df.nlargest(15, 'quality_profit_score')
        colors = plt.cm.Purples(np.linspace(0.4, 0.9, len(best_overall)))

        ax4.barh(range(len(best_overall)), best_overall['quality_profit_score'], color=colors,
                alpha=0.7, edgecolor='black')
        ax4.set_yticks(range(len(best_overall)))
        ax4.set_yticklabels(best_overall['studio'], fontsize=9)
        ax4.invert_yaxis()
        ax4.set_xlabel('Combined Score', fontsize=12, fontweight='bold')
        ax4.set_title('Top 15 Studios: Quality + Profitability', fontsize=14, fontweight='bold')
        ax4.grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'studio_quality_vs_profit.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: studio_quality_vs_profit.png")

    def visualize_portfolio_diversity(self):
        """Analyze genre diversity metrics."""
        print("Creating portfolio diversity visualization...")

        # Calculate Shannon diversity index for each studio's genre portfolio
        from scipy.stats import entropy

        studio_genres = defaultdict(lambda: defaultdict(int))

        for idx, row in self.movies_df.iterrows():
            companies_str = row.get('tmdb_production_companies', '')
            genres_str = row.get('tmdb_genres', row.get('Genres', ''))

            if pd.notna(companies_str) and companies_str and pd.notna(genres_str) and genres_str:
                companies = []
                if '|' in str(companies_str):
                    companies = [c.strip() for c in str(companies_str).split('|')][:1]  # First company
                else:
                    companies = [str(companies_str).strip()]

                genres = []
                if '|' in str(genres_str):
                    genres = [g.strip() for g in str(genres_str).split('|')]
                elif ',' in str(genres_str):
                    genres = [g.strip() for g in str(genres_str).split(',')]
                else:
                    genres = [str(genres_str).strip()]

                for company in companies:
                    if company:
                        for genre in genres:
                            if genre:
                                studio_genres[company][genre] += 1

        # Calculate diversity
        studio_stats = []
        for studio, genre_counts in studio_genres.items():
            total = sum(genre_counts.values())
            if total >= 5:  # Minimum threshold
                # Shannon entropy
                proportions = np.array(list(genre_counts.values())) / total
                diversity = entropy(proportions)

                studio_stats.append({
                    'studio': studio[:35],
                    'diversity': diversity,
                    'num_genres': len(genre_counts),
                    'total_films': total,
                    'dominant_genre': max(genre_counts.items(), key=lambda x: x[1])[0],
                    'dominant_pct': max(genre_counts.values()) / total * 100
                })

        if not studio_stats:
            print("! No diversity data available")
            return

        df = pd.DataFrame(studio_stats).sort_values('diversity', ascending=False).head(20)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))

        # Diversity index
        colors = plt.cm.Spectral(np.linspace(0.2, 0.8, len(df)))

        ax1.barh(range(len(df)), df['diversity'], color=colors, alpha=0.7, edgecolor='black')
        ax1.set_yticks(range(len(df)))
        ax1.set_yticklabels(df['studio'], fontsize=9)
        ax1.invert_yaxis()
        ax1.set_xlabel('Shannon Diversity Index', fontsize=12, fontweight='bold')
        ax1.set_title('Top 20 Most Diverse Studio Portfolios', fontsize=14, fontweight='bold')
        ax1.grid(axis='x', alpha=0.3)

        for i, (div, genres) in enumerate(zip(df['diversity'], df['num_genres'])):
            ax1.text(div, i, f' {div:.2f} ({genres} genres)', va='center', fontsize=8)

        # Diversity vs dominant genre percentage
        ax2.scatter(df['dominant_pct'], df['diversity'], s=df['total_films']*5,
                   alpha=0.6, c=range(len(df)), cmap='viridis')

        ax2.set_xlabel('Dominant Genre % of Portfolio', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Diversity Index', fontsize=12, fontweight='bold')
        ax2.set_title('Portfolio Concentration vs Diversity', fontsize=14, fontweight='bold')
        ax2.grid(alpha=0.3)

        # Annotate some points
        for idx, row in df.head(5).iterrows():
            ax2.annotate(row['studio'][:20], (row['dominant_pct'], row['diversity']),
                        fontsize=7, alpha=0.7)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'studio_portfolio_diversity.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: studio_portfolio_diversity.png")

    def generate_remaining_visualizations(self):
        """Generate remaining visualizations."""
        print("Generating remaining visualizations...")

        # Major vs Independent
        self.visualize_major_vs_independent()

        # Decade performance
        self.visualize_decade_performance()

        # Collaboration networks
        self.visualize_collaboration_networks()

        # Financial health trends
        self.visualize_financial_health()

        # Risk analysis
        self.visualize_risk_analysis()

    def visualize_major_vs_independent(self):
        """Compare major studios vs independents."""
        print("Creating major vs independent comparison...")

        # Define major studios (Big 5 + notable others)
        major_studios = {
            'Warner Bros.', 'Universal Pictures', 'Paramount', 'Columbia Pictures',
            '20th Century Fox', 'Walt Disney Pictures', 'Sony Pictures', 'Metro-Goldwyn-Mayer',
            'New Line Cinema', 'DreamWorks', 'Lionsgate', 'Fox Searchlight Pictures'
        }

        major_stats = {'budgets': [], 'revenues': [], 'ratings': [], 'rois': []}
        indie_stats = {'budgets': [], 'revenues': [], 'ratings': [], 'rois': []}

        for idx, row in self.movies_df.iterrows():
            companies_str = row.get('tmdb_production_companies', '')
            budget = row.get('tmdb_budget', 0)
            revenue = row.get('tmdb_revenue', 0)
            rating = row.get('IMDb Rating', 0)

            if pd.notna(companies_str) and companies_str:
                companies = []
                if '|' in str(companies_str):
                    companies = [c.strip() for c in str(companies_str).split('|')]
                else:
                    companies = [str(companies_str).strip()]

                # Check if any company is a major studio
                is_major = any(any(major in company for major in major_studios) for company in companies)

                target = major_stats if is_major else indie_stats

                if budget > 0:
                    target['budgets'].append(budget)
                if revenue > 0:
                    target['revenues'].append(revenue)
                if rating > 0:
                    target['ratings'].append(rating)
                if budget > 0 and revenue > 0:
                    roi = ((revenue - budget) / budget) * 100
                    target['rois'].append(roi)

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

        # Budget comparison
        budget_data = [np.array(major_stats['budgets'])/1e6, np.array(indie_stats['budgets'])/1e6]
        bp1 = ax1.boxplot(budget_data, labels=['Major Studios', 'Independent'], patch_artist=True)
        bp1['boxes'][0].set_facecolor('lightblue')
        bp1['boxes'][1].set_facecolor('lightgreen')
        ax1.set_ylabel('Budget (Millions $)', fontsize=12, fontweight='bold')
        ax1.set_title('Budget Distribution', fontsize=14, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)

        # Revenue comparison
        revenue_data = [np.array(major_stats['revenues'])/1e6, np.array(indie_stats['revenues'])/1e6]
        bp2 = ax2.boxplot(revenue_data, labels=['Major Studios', 'Independent'], patch_artist=True)
        bp2['boxes'][0].set_facecolor('lightblue')
        bp2['boxes'][1].set_facecolor('lightgreen')
        ax2.set_ylabel('Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax2.set_title('Revenue Distribution', fontsize=14, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        # Rating comparison
        rating_data = [major_stats['ratings'], indie_stats['ratings']]
        bp3 = ax3.boxplot(rating_data, labels=['Major Studios', 'Independent'], patch_artist=True)
        bp3['boxes'][0].set_facecolor('lightblue')
        bp3['boxes'][1].set_facecolor('lightgreen')
        ax3.set_ylabel('IMDb Rating', fontsize=12, fontweight='bold')
        ax3.set_title('Quality Distribution', fontsize=14, fontweight='bold')
        ax3.grid(axis='y', alpha=0.3)

        # ROI comparison
        roi_data = [major_stats['rois'], indie_stats['rois']]
        bp4 = ax4.boxplot(roi_data, labels=['Major Studios', 'Independent'], patch_artist=True)
        bp4['boxes'][0].set_facecolor('lightblue')
        bp4['boxes'][1].set_facecolor('lightgreen')
        ax4.set_ylabel('ROI (%)', fontsize=12, fontweight='bold')
        ax4.set_title('Profitability Distribution', fontsize=14, fontweight='bold')
        ax4.grid(axis='y', alpha=0.3)
        ax4.axhline(0, color='red', linestyle='--', linewidth=1, alpha=0.5)

        # Add summary statistics
        summary_text = f"""Major Studios: {len(major_stats['budgets'])} films
Independent: {len(indie_stats['budgets'])} films

Avg Budget: ${np.mean(major_stats['budgets'])/1e6:.1f}M vs ${np.mean(indie_stats['budgets'])/1e6:.1f}M
Avg Revenue: ${np.mean(major_stats['revenues'])/1e6:.1f}M vs ${np.mean(indie_stats['revenues'])/1e6:.1f}M
Avg Rating: {np.mean(major_stats['ratings']):.2f} vs {np.mean(indie_stats['ratings']):.2f}
Avg ROI: {np.mean(major_stats['rois']):.0f}% vs {np.mean(indie_stats['rois']):.0f}%"""

        fig.text(0.5, 0.02, summary_text, ha='center', fontsize=9,
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        plt.tight_layout(rect=[0, 0.08, 1, 1])
        plt.savefig(self.viz_dir / 'major_vs_independent.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: major_vs_independent.png")

    def visualize_decade_performance(self):
        """Analyze performance across eras."""
        print("Creating decade performance visualization...")

        # Track top studios by decade
        studio_decade_performance = defaultdict(lambda: defaultdict(lambda: {
            'revenues': [], 'budgets': [], 'count': 0
        }))

        for idx, row in self.movies_df.iterrows():
            companies_str = row.get('tmdb_production_companies', '')
            revenue = row.get('tmdb_revenue', 0)
            budget = row.get('tmdb_budget', 0)
            year = row.get('Year', 0)

            if pd.notna(companies_str) and companies_str and year > 0:
                decade = (year // 10) * 10

                companies = []
                if '|' in str(companies_str):
                    companies = [c.strip() for c in str(companies_str).split('|')][:1]
                else:
                    companies = [str(companies_str).strip()]

                for company in companies:
                    if company:
                        studio_decade_performance[company][decade]['count'] += 1
                        if revenue > 0:
                            studio_decade_performance[company][decade]['revenues'].append(revenue)
                        if budget > 0:
                            studio_decade_performance[company][decade]['budgets'].append(budget)

        # Get top 8 studios overall
        studio_totals = {studio: sum(data['count'] for data in decades.values())
                        for studio, decades in studio_decade_performance.items()}
        top_studios = sorted(studio_totals.items(), key=lambda x: x[1], reverse=True)[:8]
        top_studio_names = [s[0] for s in top_studios]

        # Get all decades
        all_decades = set()
        for decades in studio_decade_performance.values():
            all_decades.update(decades.keys())
        decades_sorted = sorted(all_decades)

        fig, axes = plt.subplots(2, 4, figsize=(18, 10))
        axes = axes.flatten()

        for idx, studio in enumerate(top_studio_names):
            decade_data = studio_decade_performance[studio]
            counts = [decade_data[d]['count'] for d in decades_sorted]

            axes[idx].bar(decades_sorted, counts, color='steelblue', alpha=0.7, edgecolor='black')
            axes[idx].set_title(studio[:25], fontsize=11, fontweight='bold')
            axes[idx].set_xlabel('Decade', fontsize=9)
            axes[idx].set_ylabel('Films', fontsize=9)
            axes[idx].grid(axis='y', alpha=0.3)
            axes[idx].tick_params(labelsize=8)
            axes[idx].tick_params(axis='x', rotation=45)

        plt.suptitle('Studio Output Across Decades', fontsize=16, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.viz_dir / 'studio_decade_performance.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: studio_decade_performance.png")

    def visualize_collaboration_networks(self):
        """Analyze co-production patterns."""
        print("Creating collaboration networks visualization...")

        # Track studio collaborations
        collaborations = defaultdict(int)
        studio_counts = defaultdict(int)

        for idx, row in self.movies_df.iterrows():
            companies_str = row.get('tmdb_production_companies', '')

            if pd.notna(companies_str) and companies_str and '|' in str(companies_str):
                companies = [c.strip() for c in str(companies_str).split('|')]

                # Track individual studios
                for company in companies:
                    studio_counts[company] += 1

                # Track pairs
                if len(companies) >= 2:
                    for i, comp1 in enumerate(companies):
                        for comp2 in companies[i+1:]:
                            pair = tuple(sorted([comp1, comp2]))
                            collaborations[pair] += 1

        # Get top collaborations
        top_collabs = sorted(collaborations.items(), key=lambda x: x[1], reverse=True)[:20]

        if not top_collabs:
            print("! No collaboration data available")
            return

        df = pd.DataFrame([
            {
                'pair': f"{pair[0][:20]} +\n{pair[1][:20]}",
                'count': count,
                'studio1': pair[0],
                'studio2': pair[1]
            }
            for pair, count in top_collabs
        ])

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 10))

        # Top collaborations
        colors = plt.cm.Paired(np.linspace(0.2, 0.8, len(df)))

        ax1.barh(range(len(df)), df['count'], color=colors, alpha=0.7, edgecolor='black')
        ax1.set_yticks(range(len(df)))
        ax1.set_yticklabels(df['pair'], fontsize=8)
        ax1.invert_yaxis()
        ax1.set_xlabel('Number of Co-Productions', fontsize=12, fontweight='bold')
        ax1.set_title('Top 20 Studio Collaborations', fontsize=14, fontweight='bold')
        ax1.grid(axis='x', alpha=0.3)

        for i, count in enumerate(df['count']):
            ax1.text(count, i, f' {count}', va='center', fontsize=9, fontweight='bold')

        # Most collaborative studios
        most_collaborative = sorted(studio_counts.items(), key=lambda x: x[1], reverse=True)[:15]
        collab_df = pd.DataFrame(most_collaborative, columns=['Studio', 'Total Films'])

        # Count how many co-productions each studio has
        co_prod_counts = defaultdict(int)
        for (s1, s2), count in collaborations.items():
            co_prod_counts[s1] += count
            co_prod_counts[s2] += count

        collab_df['Co-Productions'] = collab_df['Studio'].map(co_prod_counts)
        collab_df['Co-Prod %'] = (collab_df['Co-Productions'] / collab_df['Total Films'] * 100).fillna(0)

        colors = plt.cm.Oranges(np.linspace(0.4, 0.9, len(collab_df)))

        ax2.barh(range(len(collab_df)), collab_df['Co-Prod %'], color=colors,
                alpha=0.7, edgecolor='black')
        ax2.set_yticks(range(len(collab_df)))
        ax2.set_yticklabels([s[:30] for s in collab_df['Studio']], fontsize=9)
        ax2.invert_yaxis()
        ax2.set_xlabel('Co-Production Rate (%)', fontsize=12, fontweight='bold')
        ax2.set_title('Most Collaborative Studios', fontsize=14, fontweight='bold')
        ax2.grid(axis='x', alpha=0.3)

        for i, (pct, coprod) in enumerate(zip(collab_df['Co-Prod %'], collab_df['Co-Productions'])):
            ax2.text(pct, i, f' {pct:.1f}% ({int(coprod)})', va='center', fontsize=8)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'studio_collaboration_networks.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: studio_collaboration_networks.png")

    def visualize_financial_health(self):
        """Analyze profitability trends."""
        print("Creating financial health visualization...")

        # Track studio profitability over time
        studio_year_financials = defaultdict(lambda: defaultdict(lambda: {
            'total_budget': 0, 'total_revenue': 0, 'count': 0
        }))

        for idx, row in self.movies_df.iterrows():
            companies_str = row.get('tmdb_production_companies', '')
            budget = row.get('tmdb_budget', 0)
            revenue = row.get('tmdb_revenue', 0)
            year = row.get('Year', 0)

            if pd.notna(companies_str) and companies_str and budget > 0 and revenue > 0 and year >= 2000:
                companies = []
                if '|' in str(companies_str):
                    companies = [c.strip() for c in str(companies_str).split('|')][:1]
                else:
                    companies = [str(companies_str).strip()]

                for company in companies:
                    if company:
                        studio_year_financials[company][year]['total_budget'] += budget
                        studio_year_financials[company][year]['total_revenue'] += revenue
                        studio_year_financials[company][year]['count'] += 1

        # Get top 6 studios
        studio_totals = {studio: sum(data['count'] for data in years.values())
                        for studio, years in studio_year_financials.items()}
        top_studios = sorted(studio_totals.items(), key=lambda x: x[1], reverse=True)[:6]
        top_studio_names = [s[0] for s in top_studios]

        fig, axes = plt.subplots(2, 3, figsize=(18, 10))
        axes = axes.flatten()

        for idx, studio in enumerate(top_studio_names):
            year_data = studio_year_financials[studio]
            years = sorted(year_data.keys())

            profits = []
            for year in years:
                profit = (year_data[year]['total_revenue'] - year_data[year]['total_budget']) / 1e6
                profits.append(profit)

            # Plot profit trend
            colors = ['green' if p > 0 else 'red' for p in profits]
            axes[idx].bar(years, profits, color=colors, alpha=0.7, edgecolor='black')
            axes[idx].axhline(0, color='black', linewidth=1)
            axes[idx].set_title(studio[:25], fontsize=11, fontweight='bold')
            axes[idx].set_xlabel('Year', fontsize=9)
            axes[idx].set_ylabel('Profit ($M)', fontsize=9)
            axes[idx].grid(axis='y', alpha=0.3)
            axes[idx].tick_params(labelsize=8)
            axes[idx].tick_params(axis='x', rotation=45)

        plt.suptitle('Studio Financial Health (2000+): Annual Profit Trends',
                    fontsize=16, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.viz_dir / 'studio_financial_health.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: studio_financial_health.png")

    def visualize_risk_analysis(self):
        """Analyze budget risk vs reward."""
        print("Creating risk analysis visualization...")

        # Categorize films by budget size and track success
        budget_categories = {
            'Micro (<$5M)': (0, 5e6),
            'Low ($5-20M)': (5e6, 20e6),
            'Medium ($20-50M)': (20e6, 50e6),
            'High ($50-100M)': (50e6, 100e6),
            'Blockbuster (>$100M)': (100e6, float('inf'))
        }

        studio_risk_profile = defaultdict(lambda: defaultdict(lambda: {
            'count': 0, 'profitable': 0, 'avg_roi': []
        }))

        for idx, row in self.movies_df.iterrows():
            companies_str = row.get('tmdb_production_companies', '')
            budget = row.get('tmdb_budget', 0)
            revenue = row.get('tmdb_revenue', 0)

            if pd.notna(companies_str) and companies_str and budget > 0 and revenue > 0:
                # Find budget category
                category = None
                for cat_name, (min_budget, max_budget) in budget_categories.items():
                    if min_budget <= budget < max_budget:
                        category = cat_name
                        break

                if category:
                    roi = ((revenue - budget) / budget) * 100

                    companies = []
                    if '|' in str(companies_str):
                        companies = [c.strip() for c in str(companies_str).split('|')][:1]
                    else:
                        companies = [str(companies_str).strip()]

                    for company in companies:
                        if company:
                            studio_risk_profile[company][category]['count'] += 1
                            studio_risk_profile[company][category]['avg_roi'].append(roi)
                            if revenue > budget:
                                studio_risk_profile[company][category]['profitable'] += 1

        # Get top studios
        studio_totals = {studio: sum(cat['count'] for cat in categories.values())
                        for studio, categories in studio_risk_profile.items()}
        top_studios = sorted(studio_totals.items(), key=lambda x: x[1], reverse=True)[:10]
        top_studio_names = [s[0] for s in top_studios]

        # Create heatmap data
        categories = ['Micro (<$5M)', 'Low ($5-20M)', 'Medium ($20-50M)',
                     'High ($50-100M)', 'Blockbuster (>$100M)']

        # Success rate matrix
        success_matrix = np.zeros((len(top_studio_names), len(categories)))
        count_matrix = np.zeros((len(top_studio_names), len(categories)))

        for i, studio in enumerate(top_studio_names):
            for j, category in enumerate(categories):
                data = studio_risk_profile[studio][category]
                count = data['count']
                profitable = data['profitable']

                count_matrix[i, j] = count
                if count > 0:
                    success_matrix[i, j] = (profitable / count) * 100

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 10))

        # Success rate heatmap
        im1 = ax1.imshow(success_matrix, cmap='RdYlGn', aspect='auto', vmin=0, vmax=100)

        ax1.set_xticks(range(len(categories)))
        ax1.set_xticklabels(categories, rotation=45, ha='right', fontsize=9)
        ax1.set_yticks(range(len(top_studio_names)))
        ax1.set_yticklabels([s[:30] for s in top_studio_names], fontsize=9)

        ax1.set_title('Success Rate by Budget Category (%)', fontsize=14, fontweight='bold')

        # Add text annotations
        for i in range(len(top_studio_names)):
            for j in range(len(categories)):
                if count_matrix[i, j] > 0:
                    text = ax1.text(j, i, f'{success_matrix[i, j]:.0f}%\n({int(count_matrix[i, j])})',
                                  ha="center", va="center",
                                  color="white" if success_matrix[i, j] < 50 else "black",
                                  fontsize=7)

        cbar1 = plt.colorbar(im1, ax=ax1)
        cbar1.set_label('Success Rate (%)', fontsize=10)

        # ROI by category
        avg_roi_matrix = np.zeros((len(top_studio_names), len(categories)))

        for i, studio in enumerate(top_studio_names):
            for j, category in enumerate(categories):
                data = studio_risk_profile[studio][category]
                if data['avg_roi']:
                    avg_roi_matrix[i, j] = np.mean(data['avg_roi'])

        im2 = ax2.imshow(avg_roi_matrix, cmap='coolwarm', aspect='auto',
                        vmin=-100, vmax=300)

        ax2.set_xticks(range(len(categories)))
        ax2.set_xticklabels(categories, rotation=45, ha='right', fontsize=9)
        ax2.set_yticks(range(len(top_studio_names)))
        ax2.set_yticklabels([s[:30] for s in top_studio_names], fontsize=9)

        ax2.set_title('Average ROI by Budget Category (%)', fontsize=14, fontweight='bold')

        # Add text annotations
        for i in range(len(top_studio_names)):
            for j in range(len(categories)):
                if count_matrix[i, j] > 0:
                    text = ax2.text(j, i, f'{avg_roi_matrix[i, j]:.0f}%',
                                  ha="center", va="center",
                                  color="white" if abs(avg_roi_matrix[i, j]) > 100 else "black",
                                  fontsize=8)

        cbar2 = plt.colorbar(im2, ax=ax2)
        cbar2.set_label('Avg ROI (%)', fontsize=10)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'studio_risk_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: studio_risk_analysis.png")

    def generate_report(self):
        """Generate comprehensive text report."""
        print("Generating report...")

        report_path = self.reports_dir / 'batch_23_studio_economics_report.txt'

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("CINESCOPE BATCH 23: STUDIO ECONOMICS ANALYSIS\n")
            f.write("="*80 + "\n\n")

            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # Calculate summary statistics
            total_with_companies = self.movies_df['tmdb_production_companies'].notna().sum()
            total_with_financials = ((self.movies_df['tmdb_budget'] > 0) &
                                    (self.movies_df['tmdb_revenue'] > 0)).sum()

            f.write("="*80 + "\n")
            f.write("DATA COVERAGE\n")
            f.write("="*80 + "\n\n")

            f.write(f"Total Films Analyzed: {len(self.movies_df):,}\n")
            f.write(f"Films with Production Company Data: {total_with_companies:,}\n")
            f.write(f"Films with Complete Financial Data: {total_with_financials:,}\n\n")

            f.write("="*80 + "\n")
            f.write("END OF REPORT\n")
            f.write("="*80 + "\n")

        print(f"✓ Report saved: {report_path}")

    def run(self):
        """Execute the full analysis pipeline."""
        self.print_header()
        self.load_data()

        print("Generating visualizations...")
        print("-" * 80)

        self.visualize_market_share()
        self.visualize_roi_performance()
        self.visualize_genre_specialization()
        self.visualize_budget_trends()
        self.visualize_hit_rate()
        self.visualize_quality_vs_profit()
        self.visualize_portfolio_diversity()

        self.generate_remaining_visualizations()

        print("-" * 80)
        print()

        self.generate_report()

        print("="*80)
        print("BATCH 23 ANALYSIS COMPLETE!")
        print("="*80)
        print()
        print(f"Visualizations saved to: {self.viz_dir}")
        print(f"Report saved to: {self.reports_dir}")
        print()


if __name__ == "__main__":
    analyzer = StudioEconomicsAnalyzer()
    analyzer.run()
