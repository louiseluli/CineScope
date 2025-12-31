#!/usr/bin/env python3
"""
================================================================================
CINESCOPE BATCH 24: BOX OFFICE PATTERNS ANALYSIS
================================================================================

Focus: Commercial success patterns by genre, era, director, and other factors

Data Sources:
- TMDB revenue and budget data
- OMDb box office figures
- Genre classifications
- Director and cast information
- Release dates and timing

Visualizations (12):
1. genre_box_office_performance.png - Revenue performance by genre
2. decade_commercial_trends.png - Box office evolution over time
3. director_commercial_success.png - Most commercially successful directors
4. budget_size_vs_revenue.png - Revenue by budget category
5. seasonal_box_office.png - Commercial performance by release season
6. star_power_revenue.png - Actor influence on box office
7. franchise_vs_original.png - Franchise vs original film revenue
8. runtime_revenue_correlation.png - Runtime impact on revenue
9. rating_vs_commercial.png - Quality vs commercial success
10. international_vs_domestic.png - Revenue distribution analysis
11. top_earners_timeline.png - Highest-grossing films over time
12. commercial_prediction_factors.png - Key predictors of box office success

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
import ast

class BoxOfficePatternAnalyzer:
    """Analyzes box office and commercial success patterns."""

    def __init__(self):
        """Initialize the analyzer."""
        self.base_dir = Path(__file__).parent.parent
        self.data_dir = self.base_dir / 'data'
        self.viz_dir = self.base_dir / 'analysis_outputs' / 'visualizations' / 'batch_24'
        self.reports_dir = self.base_dir / 'analysis_outputs' / 'reports'

        # Create output directories
        self.viz_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)

        self.movies_df = None
        self.people_cache = None
        self.stats = {}

    def print_header(self):
        """Print analysis header."""
        print("="*80)
        print("CINESCOPE BATCH 24: BOX OFFICE PATTERNS ANALYSIS")
        print("="*80)
        print()

    def load_data(self):
        """Load all required data files."""
        print("Loading data files...")

        # Load movies
        movies_path = self.data_dir / 'processed' / 'watched_movies_master.csv'
        self.movies_df = pd.read_csv(movies_path)
        print(f"✓ Loaded {len(self.movies_df):,} movies")

        # Load people cache
        people_cache_path = self.data_dir / 'processed' / 'people_cache.json'
        with open(people_cache_path, 'r', encoding='utf-8') as f:
            self.people_cache = json.load(f)
        print(f"✓ Loaded {len(self.people_cache):,} people from cache")

        print()

    def visualize_genre_box_office(self):
        """Analyze revenue performance by genre."""
        print("Creating genre box office visualization...")

        # Track genre revenues
        genre_revenues = defaultdict(list)
        genre_budgets = defaultdict(list)

        for idx, row in self.movies_df.iterrows():
            genres_str = row.get('tmdb_genres', row.get('Genres', ''))
            revenue = row.get('tmdb_revenue', 0)
            budget = row.get('tmdb_budget', 0)

            if pd.notna(genres_str) and genres_str and revenue > 0:
                # Parse genres
                genres = []
                if '|' in str(genres_str):
                    genres = [g.strip() for g in str(genres_str).split('|')]
                elif ',' in str(genres_str):
                    genres = [g.strip() for g in str(genres_str).split(',')]
                else:
                    genres = [str(genres_str).strip()]

                for genre in genres:
                    if genre:
                        genre_revenues[genre].append(revenue)
                        if budget > 0:
                            genre_budgets[genre].append(budget)

        # Calculate statistics
        genre_stats = []
        for genre, revenues in genre_revenues.items():
            if len(revenues) >= 5:  # Minimum threshold
                budgets = genre_budgets.get(genre, [])
                genre_stats.append({
                    'genre': genre,
                    'total_revenue': sum(revenues) / 1e6,
                    'avg_revenue': np.mean(revenues) / 1e6,
                    'median_revenue': np.median(revenues) / 1e6,
                    'avg_budget': np.mean(budgets) / 1e6 if budgets else 0,
                    'film_count': len(revenues)
                })

        if not genre_stats:
            print("! No genre revenue data available")
            return

        df = pd.DataFrame(genre_stats).sort_values('total_revenue', ascending=False)

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

        # Total revenue by genre
        top_total = df.head(12)
        colors = plt.cm.Blues(np.linspace(0.4, 0.9, len(top_total)))

        ax1.barh(range(len(top_total)), top_total['total_revenue'], color=colors,
                alpha=0.7, edgecolor='black')
        ax1.set_yticks(range(len(top_total)))
        ax1.set_yticklabels(top_total['genre'], fontsize=9)
        ax1.invert_yaxis()
        ax1.set_xlabel('Total Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax1.set_title('Genres by Total Box Office Revenue', fontsize=14, fontweight='bold')
        ax1.grid(axis='x', alpha=0.3)

        for i, (rev, count) in enumerate(zip(top_total['total_revenue'], top_total['film_count'])):
            ax1.text(rev, i, f' ${rev:.0f}M ({count} films)', va='center', fontsize=8)

        # Average revenue by genre
        top_avg = df.nlargest(12, 'avg_revenue')
        colors = plt.cm.Greens(np.linspace(0.4, 0.9, len(top_avg)))

        ax2.barh(range(len(top_avg)), top_avg['avg_revenue'], color=colors,
                alpha=0.7, edgecolor='black')
        ax2.set_yticks(range(len(top_avg)))
        ax2.set_yticklabels(top_avg['genre'], fontsize=9)
        ax2.invert_yaxis()
        ax2.set_xlabel('Average Revenue per Film (Millions $)', fontsize=12, fontweight='bold')
        ax2.set_title('Genres by Average Box Office', fontsize=14, fontweight='bold')
        ax2.grid(axis='x', alpha=0.3)

        for i, rev in enumerate(top_avg['avg_revenue']):
            ax2.text(rev, i, f' ${rev:.0f}M', va='center', fontsize=8)

        # Budget vs Revenue scatter
        ax3.scatter(df['avg_budget'], df['avg_revenue'], s=df['film_count']*5,
                   alpha=0.6, c=range(len(df)), cmap='viridis')

        # Break-even line
        max_val = max(df['avg_budget'].max(), df['avg_revenue'].max())
        ax3.plot([0, max_val], [0, max_val], 'r--', linewidth=2, label='Break-even', alpha=0.7)

        ax3.set_xlabel('Average Budget (Millions $)', fontsize=12, fontweight='bold')
        ax3.set_ylabel('Average Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax3.set_title('Genre Budget vs Revenue (bubble size = film count)',
                     fontsize=14, fontweight='bold')
        ax3.grid(alpha=0.3)
        ax3.legend()

        # Annotate some genres
        for idx, row in df.head(5).iterrows():
            ax3.annotate(row['genre'], (row['avg_budget'], row['avg_revenue']),
                        fontsize=8, alpha=0.7)

        # ROI by genre
        df['roi'] = ((df['avg_revenue'] - df['avg_budget']) / df['avg_budget'] * 100).replace([np.inf, -np.inf], 0)
        top_roi = df[df['avg_budget'] > 0].nlargest(12, 'roi')
        colors = plt.cm.RdYlGn(np.linspace(0.3, 0.9, len(top_roi)))

        ax4.barh(range(len(top_roi)), top_roi['roi'], color=colors,
                alpha=0.7, edgecolor='black')
        ax4.set_yticks(range(len(top_roi)))
        ax4.set_yticklabels(top_roi['genre'], fontsize=9)
        ax4.invert_yaxis()
        ax4.set_xlabel('Average ROI (%)', fontsize=12, fontweight='bold')
        ax4.set_title('Most Profitable Genres by ROI', fontsize=14, fontweight='bold')
        ax4.grid(axis='x', alpha=0.3)
        ax4.axvline(0, color='black', linewidth=1)

        for i, roi in enumerate(top_roi['roi']):
            ax4.text(roi, i, f' {roi:.0f}%', va='center', fontsize=8)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'genre_box_office_performance.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: genre_box_office_performance.png")

    def visualize_decade_trends(self):
        """Analyze box office evolution over time."""
        print("Creating decade commercial trends visualization...")

        # Track revenues by decade
        decade_revenues = defaultdict(list)
        decade_budgets = defaultdict(list)

        for idx, row in self.movies_df.iterrows():
            year = row.get('Year', 0)
            revenue = row.get('tmdb_revenue', 0)
            budget = row.get('tmdb_budget', 0)

            if year > 0 and revenue > 0:
                decade = (year // 10) * 10
                decade_revenues[decade].append(revenue)
                if budget > 0:
                    decade_budgets[decade].append(budget)

        # Calculate statistics
        decades = sorted(decade_revenues.keys())
        avg_revenues = [np.mean(decade_revenues[d]) / 1e6 for d in decades]
        median_revenues = [np.median(decade_revenues[d]) / 1e6 for d in decades]
        total_revenues = [sum(decade_revenues[d]) / 1e6 for d in decades]
        counts = [len(decade_revenues[d]) for d in decades]
        avg_budgets = [np.mean(decade_budgets[d]) / 1e6 if d in decade_budgets else 0 for d in decades]

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

        # Average revenue over time
        ax1.plot(decades, avg_revenues, marker='o', linewidth=2, markersize=8,
                color='steelblue', label='Average Revenue')
        ax1.plot(decades, median_revenues, marker='s', linewidth=2, markersize=8,
                color='coral', label='Median Revenue', linestyle='--')
        ax1.set_xlabel('Decade', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax1.set_title('Box Office Evolution Over Decades', fontsize=14, fontweight='bold')
        ax1.grid(alpha=0.3)
        ax1.legend()

        # Total revenue by decade
        colors = plt.cm.Blues(np.array(total_revenues) / max(total_revenues))
        ax2.bar(decades, total_revenues, color=colors, alpha=0.7, edgecolor='black')
        ax2.set_xlabel('Decade', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Total Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax2.set_title('Total Box Office by Decade', fontsize=14, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        for i, (decade, rev) in enumerate(zip(decades, total_revenues)):
            ax2.text(decade, rev, f'${rev:.0f}M', ha='center', va='bottom', fontsize=8)

        # Film count over time
        ax3.bar(decades, counts, color='lightgreen', alpha=0.7, edgecolor='black')
        ax3.set_xlabel('Decade', fontsize=12, fontweight='bold')
        ax3.set_ylabel('Number of Films', fontsize=12, fontweight='bold')
        ax3.set_title('Number of Films with Revenue Data', fontsize=14, fontweight='bold')
        ax3.grid(axis='y', alpha=0.3)

        for i, (decade, count) in enumerate(zip(decades, counts)):
            ax3.text(decade, count, str(count), ha='center', va='bottom', fontweight='bold')

        # Budget vs Revenue over time
        ax4.plot(decades, avg_budgets, marker='o', linewidth=2, markersize=8,
                color='red', label='Average Budget', alpha=0.7)
        ax4.plot(decades, avg_revenues, marker='s', linewidth=2, markersize=8,
                color='green', label='Average Revenue', alpha=0.7)
        ax4.set_xlabel('Decade', fontsize=12, fontweight='bold')
        ax4.set_ylabel('Amount (Millions $)', fontsize=12, fontweight='bold')
        ax4.set_title('Budget vs Revenue Trends', fontsize=14, fontweight='bold')
        ax4.grid(alpha=0.3)
        ax4.legend()

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'decade_commercial_trends.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: decade_commercial_trends.png")

    def visualize_director_commercial_success(self):
        """Analyze most commercially successful directors."""
        print("Creating director commercial success visualization...")

        # Track director revenues
        director_revenues = defaultdict(list)
        director_budgets = defaultdict(list)

        for idx, row in self.movies_df.iterrows():
            director_ids = row.get('imdb_director_ids', '')
            revenue = row.get('tmdb_revenue', 0)
            budget = row.get('tmdb_budget', 0)

            if pd.notna(director_ids) and str(director_ids) not in ['', 'nan', '[]'] and revenue > 0:
                try:
                    if isinstance(director_ids, str):
                        director_ids = director_ids.strip()
                        if director_ids.startswith('['):
                            directors = ast.literal_eval(director_ids)
                            if isinstance(directors, list):
                                director_ids = [str(d).strip().strip("'\"") for d in directors if d]
                        else:
                            director_ids = [d.strip() for d in director_ids.split('|')]

                    for director_id in director_ids:
                        if director_id:
                            director_revenues[director_id].append(revenue)
                            if budget > 0:
                                director_budgets[director_id].append(budget)
                except:
                    pass

        # Find director names
        director_stats = []
        for director_id, revenues in director_revenues.items():
            if len(revenues) >= 3:  # Minimum 3 films
                # Find name
                director_name = 'Unknown'
                for cache_id, person_data in self.people_cache.items():
                    if person_data.get('imdb_id') == director_id:
                        director_name = person_data.get('imdb_name', 'Unknown')
                        break

                budgets = director_budgets.get(director_id, [])
                director_stats.append({
                    'name': director_name[:30],
                    'total_revenue': sum(revenues) / 1e6,
                    'avg_revenue': np.mean(revenues) / 1e6,
                    'avg_budget': np.mean(budgets) / 1e6 if budgets else 0,
                    'film_count': len(revenues)
                })

        if not director_stats:
            print("! No director revenue data available")
            return

        df = pd.DataFrame(director_stats)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))

        # Top directors by total revenue
        top_total = df.nlargest(20, 'total_revenue')
        colors = plt.cm.Blues(np.linspace(0.4, 0.9, len(top_total)))

        ax1.barh(range(len(top_total)), top_total['total_revenue'], color=colors,
                alpha=0.7, edgecolor='black')
        ax1.set_yticks(range(len(top_total)))
        ax1.set_yticklabels(top_total['name'], fontsize=9)
        ax1.invert_yaxis()
        ax1.set_xlabel('Total Box Office Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax1.set_title('Top 20 Directors by Total Revenue', fontsize=14, fontweight='bold')
        ax1.grid(axis='x', alpha=0.3)

        for i, (rev, count) in enumerate(zip(top_total['total_revenue'], top_total['film_count'])):
            ax1.text(rev, i, f' ${rev:.0f}M ({count} films)', va='center', fontsize=8)

        # Top directors by average revenue
        top_avg = df.nlargest(20, 'avg_revenue')
        colors = plt.cm.Greens(np.linspace(0.4, 0.9, len(top_avg)))

        ax2.barh(range(len(top_avg)), top_avg['avg_revenue'], color=colors,
                alpha=0.7, edgecolor='black')
        ax2.set_yticks(range(len(top_avg)))
        ax2.set_yticklabels(top_avg['name'], fontsize=9)
        ax2.invert_yaxis()
        ax2.set_xlabel('Average Revenue per Film (Millions $)', fontsize=12, fontweight='bold')
        ax2.set_title('Top 20 Directors by Average Revenue', fontsize=14, fontweight='bold')
        ax2.grid(axis='x', alpha=0.3)

        for i, (rev, count) in enumerate(zip(top_avg['avg_revenue'], top_avg['film_count'])):
            ax2.text(rev, i, f' ${rev:.0f}M ({count} films)', va='center', fontsize=8)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'director_commercial_success.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: director_commercial_success.png")

    def visualize_budget_categories(self):
        """Analyze revenue by budget size."""
        print("Creating budget size vs revenue visualization...")

        # Define budget categories
        budget_categories = {
            'Micro (<$1M)': (0, 1e6),
            'Low ($1-10M)': (1e6, 10e6),
            'Medium ($10-50M)': (10e6, 50e6),
            'High ($50-100M)': (50e6, 100e6),
            'Blockbuster ($100-200M)': (100e6, 200e6),
            'Mega ($200M+)': (200e6, float('inf'))
        }

        category_data = defaultdict(lambda: {'revenues': [], 'count': 0})

        for idx, row in self.movies_df.iterrows():
            budget = row.get('tmdb_budget', 0)
            revenue = row.get('tmdb_revenue', 0)

            if budget > 0 and revenue > 0:
                for cat_name, (min_b, max_b) in budget_categories.items():
                    if min_b <= budget < max_b:
                        category_data[cat_name]['revenues'].append(revenue)
                        category_data[cat_name]['count'] += 1
                        break

        # Calculate statistics
        categories = list(budget_categories.keys())
        avg_revenues = []
        median_revenues = []
        counts = []

        for cat in categories:
            data = category_data[cat]
            if data['revenues']:
                avg_revenues.append(np.mean(data['revenues']) / 1e6)
                median_revenues.append(np.median(data['revenues']) / 1e6)
                counts.append(data['count'])
            else:
                avg_revenues.append(0)
                median_revenues.append(0)
                counts.append(0)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Average revenue by budget category
        colors = plt.cm.RdYlGn(np.linspace(0.3, 0.9, len(categories)))

        ax1.bar(range(len(categories)), avg_revenues, color=colors, alpha=0.7, edgecolor='black')
        ax1.set_xticks(range(len(categories)))
        ax1.set_xticklabels(categories, rotation=45, ha='right', fontsize=9)
        ax1.set_ylabel('Average Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax1.set_title('Average Box Office by Budget Category', fontsize=14, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)

        for i, (rev, count) in enumerate(zip(avg_revenues, counts)):
            if count > 0:
                ax1.text(i, rev, f'${rev:.0f}M\n({count})', ha='center', va='bottom', fontsize=8)

        # Film distribution by budget category
        ax2.bar(range(len(categories)), counts, color='steelblue', alpha=0.7, edgecolor='black')
        ax2.set_xticks(range(len(categories)))
        ax2.set_xticklabels(categories, rotation=45, ha='right', fontsize=9)
        ax2.set_ylabel('Number of Films', fontsize=12, fontweight='bold')
        ax2.set_title('Film Distribution by Budget Category', fontsize=14, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        for i, count in enumerate(counts):
            if count > 0:
                ax2.text(i, count, str(count), ha='center', va='bottom', fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'budget_size_vs_revenue.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: budget_size_vs_revenue.png")

    def generate_remaining_visualizations(self):
        """Generate remaining visualizations."""
        print("Generating remaining visualizations...")

        self.visualize_seasonal_box_office()
        self.visualize_star_power()
        self.visualize_franchise_vs_original()
        self.visualize_runtime_revenue()
        self.visualize_rating_vs_commercial()
        self.visualize_top_earners_timeline()
        self.visualize_commercial_factors()
        self.visualize_revenue_distribution()

    def visualize_seasonal_box_office(self):
        """Analyze commercial performance by release season."""
        print("Creating seasonal box office visualization...")

        # Parse release dates
        seasonal_revenues = defaultdict(list)

        for idx, row in self.movies_df.iterrows():
            release_date = row.get('tmdb_release_date', row.get('Release Date', ''))
            revenue = row.get('tmdb_revenue', 0)

            if pd.notna(release_date) and revenue > 0:
                try:
                    date = pd.to_datetime(release_date)
                    month = date.month

                    # Categorize season
                    if month in [5, 6, 7, 8]:
                        season = 'Summer\n(May-Aug)'
                    elif month in [11, 12]:
                        season = 'Holiday\n(Nov-Dec)'
                    elif month in [1, 2, 3]:
                        season = 'Winter\n(Jan-Mar)'
                    else:
                        season = 'Spring/Fall\n(Apr,Sep,Oct)'

                    seasonal_revenues[season].append(revenue)
                except:
                    pass

        # Calculate statistics
        seasons = ['Summer\n(May-Aug)', 'Holiday\n(Nov-Dec)', 'Winter\n(Jan-Mar)', 'Spring/Fall\n(Apr,Sep,Oct)']
        avg_revenues = []
        median_revenues = []
        counts = []

        for season in seasons:
            if seasonal_revenues[season]:
                avg_revenues.append(np.mean(seasonal_revenues[season]) / 1e6)
                median_revenues.append(np.median(seasonal_revenues[season]) / 1e6)
                counts.append(len(seasonal_revenues[season]))
            else:
                avg_revenues.append(0)
                median_revenues.append(0)
                counts.append(0)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Average revenue by season
        colors = ['#ff9800', '#2196f3', '#9e9e9e', '#4caf50']

        ax1.bar(range(len(seasons)), avg_revenues, color=colors, alpha=0.7, edgecolor='black')
        ax1.set_xticks(range(len(seasons)))
        ax1.set_xticklabels(seasons)
        ax1.set_ylabel('Average Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax1.set_title('Average Box Office by Release Season', fontsize=14, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)

        for i, (rev, count) in enumerate(zip(avg_revenues, counts)):
            if count > 0:
                ax1.text(i, rev, f'${rev:.0f}M\n({count} films)', ha='center', va='bottom', fontsize=9)

        # Revenue distribution by season
        box_data = [seasonal_revenues[s] for s in seasons if seasonal_revenues[s]]
        box_labels = [s for s in seasons if seasonal_revenues[s]]

        bp = ax2.boxplot([np.array(d)/1e6 for d in box_data], labels=box_labels, patch_artist=True)
        for patch, color in zip(bp['boxes'], colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)

        ax2.set_ylabel('Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax2.set_title('Revenue Distribution by Season', fontsize=14, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'seasonal_box_office.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: seasonal_box_office.png")

    def visualize_star_power(self):
        """Analyze actor influence on box office."""
        print("Creating star power revenue visualization...")

        # Track actor revenues
        actor_revenues = defaultdict(list)

        for idx, row in self.movies_df.iterrows():
            cast_ids = row.get('imdb_cast_ids', '')
            revenue = row.get('tmdb_revenue', 0)

            if pd.notna(cast_ids) and str(cast_ids) not in ['', 'nan', '[]'] and revenue > 0:
                try:
                    if isinstance(cast_ids, str):
                        cast_ids = cast_ids.strip()
                        if cast_ids.startswith('['):
                            actors = ast.literal_eval(cast_ids)
                            if isinstance(actors, list):
                                cast_ids = [str(a).strip().strip("'\"") for a in actors[:5] if a]  # Top 5 cast
                        else:
                            cast_ids = [a.strip() for a in cast_ids.split('|')[:5]]

                    for actor_id in cast_ids:
                        if actor_id:
                            actor_revenues[actor_id].append(revenue)
                except:
                    pass

        # Find actor names
        actor_stats = []
        for actor_id, revenues in actor_revenues.items():
            if len(revenues) >= 5:  # Minimum 5 films
                # Find name
                actor_name = 'Unknown'
                for cache_id, person_data in self.people_cache.items():
                    if person_data.get('imdb_id') == actor_id:
                        actor_name = person_data.get('imdb_name', 'Unknown')
                        break

                actor_stats.append({
                    'name': actor_name[:30],
                    'total_revenue': sum(revenues) / 1e6,
                    'avg_revenue': np.mean(revenues) / 1e6,
                    'film_count': len(revenues)
                })

        if not actor_stats:
            print("! No actor revenue data available")
            return

        df = pd.DataFrame(actor_stats).nlargest(25, 'total_revenue')

        fig, ax = plt.subplots(figsize=(14, 10))

        colors = plt.cm.Oranges(np.linspace(0.4, 0.9, len(df)))

        ax.barh(range(len(df)), df['total_revenue'], color=colors, alpha=0.7, edgecolor='black')
        ax.set_yticks(range(len(df)))
        ax.set_yticklabels(df['name'], fontsize=9)
        ax.invert_yaxis()
        ax.set_xlabel('Total Box Office Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax.set_title('Top 25 Actors by Total Box Office Revenue', fontsize=14, fontweight='bold')
        ax.grid(axis='x', alpha=0.3)

        for i, (rev, count) in enumerate(zip(df['total_revenue'], df['film_count'])):
            ax.text(rev, i, f' ${rev:.0f}M ({count} films)', va='center', fontsize=8)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'star_power_revenue.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: star_power_revenue.png")

    def visualize_franchise_vs_original(self):
        """Compare franchise vs original film revenue."""
        print("Creating franchise vs original visualization...")

        franchise_revenues = []
        original_revenues = []

        for idx, row in self.movies_df.iterrows():
            collection = row.get('tmdb_belongs_to_collection', '')
            revenue = row.get('tmdb_revenue', 0)

            if revenue > 0:
                is_franchise = pd.notna(collection) and str(collection).lower() != 'nan' and collection
                if is_franchise:
                    franchise_revenues.append(revenue)
                else:
                    original_revenues.append(revenue)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Box plot comparison
        if franchise_revenues and original_revenues:
            box_data = [np.array(franchise_revenues)/1e6, np.array(original_revenues)/1e6]
            bp = ax1.boxplot(box_data, labels=['Franchise Films', 'Original Films'], patch_artist=True)
            bp['boxes'][0].set_facecolor('lightblue')
            bp['boxes'][1].set_facecolor('lightgreen')

            ax1.set_ylabel('Revenue (Millions $)', fontsize=12, fontweight='bold')
            ax1.set_title('Revenue Distribution', fontsize=14, fontweight='bold')
            ax1.grid(axis='y', alpha=0.3)

        # Average comparison
        avg_franchise = np.mean(franchise_revenues) / 1e6 if franchise_revenues else 0
        avg_original = np.mean(original_revenues) / 1e6 if original_revenues else 0

        categories = ['Franchise Films', 'Original Films']
        averages = [avg_franchise, avg_original]
        counts = [len(franchise_revenues), len(original_revenues)]
        colors = ['lightblue', 'lightgreen']

        ax2.bar(categories, averages, color=colors, alpha=0.7, edgecolor='black')
        ax2.set_ylabel('Average Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax2.set_title('Average Box Office Comparison', fontsize=14, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        for i, (avg, count) in enumerate(zip(averages, counts)):
            ax2.text(i, avg, f'${avg:.0f}M\n({count} films)', ha='center', va='bottom', fontsize=10)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'franchise_vs_original.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: franchise_vs_original.png")

    def visualize_runtime_revenue(self):
        """Analyze runtime impact on revenue."""
        print("Creating runtime revenue correlation visualization...")

        runtimes = []
        revenues = []

        for idx, row in self.movies_df.iterrows():
            runtime = row.get('Runtime (mins)', 0)
            revenue = row.get('tmdb_revenue', 0)

            if runtime > 0 and revenue > 0:
                runtimes.append(runtime)
                revenues.append(revenue / 1e6)

        if len(runtimes) < 10:
            print("! Insufficient runtime-revenue data")
            return

        fig, ax = plt.subplots(figsize=(12, 8))

        ax.scatter(runtimes, revenues, alpha=0.5, s=30)
        ax.set_xlabel('Runtime (minutes)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax.set_title('Runtime vs Box Office Revenue', fontsize=14, fontweight='bold')
        ax.grid(alpha=0.3)

        # Add correlation
        from scipy import stats as sp_stats
        corr, p_value = sp_stats.pearsonr(runtimes, revenues)
        ax.text(0.05, 0.95, f'Correlation: {corr:.3f}\nP-value: {p_value:.4f}',
                transform=ax.transAxes, fontsize=11, fontweight='bold',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
                verticalalignment='top')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'runtime_revenue_correlation.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: runtime_revenue_correlation.png")

    def visualize_rating_vs_commercial(self):
        """Compare quality vs commercial success."""
        print("Creating rating vs commercial success visualization...")

        ratings = []
        revenues = []
        titles = []

        for idx, row in self.movies_df.iterrows():
            rating = row.get('IMDb Rating', 0)
            revenue = row.get('tmdb_revenue', 0)
            title = row.get('Title', 'Unknown')

            if rating > 0 and revenue > 0:
                ratings.append(rating)
                revenues.append(revenue / 1e6)
                titles.append(title)

        if len(ratings) < 10:
            print("! Insufficient rating-revenue data")
            return

        fig, ax = plt.subplots(figsize=(12, 8))

        scatter = ax.scatter(ratings, revenues, alpha=0.5, s=30, c=revenues,
                            cmap='viridis')
        ax.set_xlabel('IMDb Rating', fontsize=12, fontweight='bold')
        ax.set_ylabel('Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax.set_title('Film Quality vs Commercial Success', fontsize=14, fontweight='bold')
        ax.grid(alpha=0.3)

        # Add colorbar
        cbar = plt.colorbar(scatter, ax=ax)
        cbar.set_label('Revenue (Millions $)', fontsize=10)

        # Add correlation
        from scipy import stats as sp_stats
        corr, p_value = sp_stats.pearsonr(ratings, revenues)
        ax.text(0.05, 0.95, f'Correlation: {corr:.3f}\nP-value: {p_value:.4f}',
                transform=ax.transAxes, fontsize=11, fontweight='bold',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
                verticalalignment='top')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'rating_vs_commercial.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: rating_vs_commercial.png")

    def visualize_top_earners_timeline(self):
        """Show highest-grossing films over time."""
        print("Creating top earners timeline visualization...")

        # Get films with year and revenue
        films = []
        for idx, row in self.movies_df.iterrows():
            year = row.get('Year', 0)
            revenue = row.get('tmdb_revenue', 0)
            title = row.get('Title', 'Unknown')

            if year > 0 and revenue > 0:
                films.append({
                    'year': year,
                    'revenue': revenue / 1e6,
                    'title': title
                })

        if not films:
            print("! No timeline data available")
            return

        df = pd.DataFrame(films)

        # Get top 3 per decade
        df['decade'] = (df['year'] // 10) * 10
        top_per_decade = df.groupby('decade').apply(
            lambda x: x.nlargest(3, 'revenue')
        ).reset_index(drop=True)

        fig, ax = plt.subplots(figsize=(14, 10))

        decades = sorted(top_per_decade['decade'].unique())
        y_pos = 0

        for decade in decades:
            decade_films = top_per_decade[top_per_decade['decade'] == decade]

            for idx, film in decade_films.iterrows():
                ax.barh(y_pos, film['revenue'], color='steelblue', alpha=0.7, edgecolor='black')
                ax.text(film['revenue'], y_pos,
                       f" {film['title'][:40]} (${film['revenue']:.0f}M)",
                       va='center', fontsize=8)
                y_pos += 1

            y_pos += 0.5  # Gap between decades

        ax.set_yticks([])
        ax.set_xlabel('Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax.set_title('Top Box Office Films by Decade (Top 3 per decade)',
                    fontsize=14, fontweight='bold')
        ax.grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'top_earners_timeline.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: top_earners_timeline.png")

    def visualize_commercial_factors(self):
        """Analyze key predictors of box office success."""
        print("Creating commercial prediction factors visualization...")

        # Calculate correlations with revenue
        factors = {
            'IMDb Rating': [],
            'Runtime': [],
            'Budget': [],
            'IMDb Votes': []
        }

        revenues = []

        for idx, row in self.movies_df.iterrows():
            revenue = row.get('tmdb_revenue', 0)

            if revenue > 0:
                rating = row.get('IMDb Rating', 0)
                runtime = row.get('Runtime (mins)', 0)
                budget = row.get('tmdb_budget', 0)
                votes = row.get('Num Votes', 0)

                if rating > 0 and runtime > 0:
                    revenues.append(revenue)
                    factors['IMDb Rating'].append(rating)
                    factors['Runtime'].append(runtime)
                    factors['Budget'].append(budget if budget > 0 else 0)
                    factors['IMDb Votes'].append(votes if votes > 0 else 0)

        if len(revenues) < 10:
            print("! Insufficient data for factor analysis")
            return

        # Calculate correlations
        from scipy import stats as sp_stats

        correlations = []
        for factor_name, values in factors.items():
            if any(v != 0 for v in values):
                corr, p_value = sp_stats.pearsonr(values, revenues)
                correlations.append({
                    'factor': factor_name,
                    'correlation': corr,
                    'p_value': p_value
                })

        df = pd.DataFrame(correlations).sort_values('correlation', ascending=False)

        fig, ax = plt.subplots(figsize=(12, 6))

        colors = ['green' if c > 0 else 'red' for c in df['correlation']]

        ax.barh(range(len(df)), df['correlation'], color=colors, alpha=0.7, edgecolor='black')
        ax.set_yticks(range(len(df)))
        ax.set_yticklabels(df['factor'])
        ax.invert_yaxis()
        ax.set_xlabel('Correlation with Box Office Revenue', fontsize=12, fontweight='bold')
        ax.set_title('Key Predictors of Commercial Success', fontsize=14, fontweight='bold')
        ax.grid(axis='x', alpha=0.3)
        ax.axvline(0, color='black', linewidth=1)

        for i, (corr, p_val) in enumerate(zip(df['correlation'], df['p_value'])):
            ax.text(corr, i, f' {corr:.3f} (p={p_val:.4f})', va='center', fontsize=9)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'commercial_prediction_factors.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: commercial_prediction_factors.png")

    def visualize_revenue_distribution(self):
        """Analyze overall revenue distribution."""
        print("Creating revenue distribution visualization...")

        revenues = []
        for idx, row in self.movies_df.iterrows():
            revenue = row.get('tmdb_revenue', 0)
            if revenue > 0:
                revenues.append(revenue / 1e6)

        if not revenues:
            print("! No revenue data available")
            return

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Histogram
        ax1.hist(revenues, bins=50, color='steelblue', alpha=0.7, edgecolor='black')
        ax1.set_xlabel('Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Number of Films', fontsize=12, fontweight='bold')
        ax1.set_title('Box Office Revenue Distribution', fontsize=14, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)
        ax1.axvline(np.median(revenues), color='red', linestyle='--', linewidth=2,
                   label=f'Median: ${np.median(revenues):.0f}M')
        ax1.legend()

        # Log scale histogram
        ax2.hist(revenues, bins=50, color='green', alpha=0.7, edgecolor='black')
        ax2.set_xlabel('Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Number of Films (log scale)', fontsize=12, fontweight='bold')
        ax2.set_title('Revenue Distribution (Log Scale)', fontsize=14, fontweight='bold')
        ax2.set_yscale('log')
        ax2.grid(axis='y', alpha=0.3)

        # Add statistics
        stats_text = f"""Mean: ${np.mean(revenues):.0f}M
Median: ${np.median(revenues):.0f}M
Max: ${max(revenues):.0f}M
Min: ${min(revenues):.0f}M
Films: {len(revenues)}"""

        ax2.text(0.65, 0.65, stats_text, transform=ax2.transAxes,
                fontsize=10, bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'revenue_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: revenue_distribution.png")

    def generate_report(self):
        """Generate comprehensive text report."""
        print("Generating report...")

        report_path = self.reports_dir / 'batch_24_box_office_patterns_report.txt'

        # Calculate summary statistics
        total_films = len(self.movies_df)
        films_with_revenue = (self.movies_df['tmdb_revenue'] > 0).sum()
        total_revenue = self.movies_df[self.movies_df['tmdb_revenue'] > 0]['tmdb_revenue'].sum()
        avg_revenue = self.movies_df[self.movies_df['tmdb_revenue'] > 0]['tmdb_revenue'].mean()

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("CINESCOPE BATCH 24: BOX OFFICE PATTERNS ANALYSIS\n")
            f.write("="*80 + "\n\n")

            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            f.write("="*80 + "\n")
            f.write("DATA COVERAGE\n")
            f.write("="*80 + "\n\n")

            f.write(f"Total Films Analyzed: {total_films:,}\n")
            f.write(f"Films with Revenue Data: {films_with_revenue:,} ({films_with_revenue/total_films*100:.1f}%)\n\n")

            f.write("="*80 + "\n")
            f.write("BOX OFFICE SUMMARY\n")
            f.write("="*80 + "\n\n")

            f.write(f"Total Combined Revenue: ${total_revenue/1e9:.2f} Billion\n")
            f.write(f"Average Revenue per Film: ${avg_revenue/1e6:.1f} Million\n\n")

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

        self.visualize_genre_box_office()
        self.visualize_decade_trends()
        self.visualize_director_commercial_success()
        self.visualize_budget_categories()

        self.generate_remaining_visualizations()

        print("-" * 80)
        print()

        self.generate_report()

        print("="*80)
        print("BATCH 24 ANALYSIS COMPLETE!")
        print("="*80)
        print()
        print(f"Visualizations saved to: {self.viz_dir}")
        print(f"Report saved to: {self.reports_dir}")
        print()


if __name__ == "__main__":
    analyzer = BoxOfficePatternAnalyzer()
    analyzer.run()
