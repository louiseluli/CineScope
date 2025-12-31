#!/usr/bin/env python3
"""
================================================================================
CINESCOPE BATCH 17: RUNTIME & PACING ANALYSIS
================================================================================

Focus: How film length affects quality, genre patterns, and viewer engagement

Data Sources:
- Runtime data from movies dataset
- Genre, rating, year data
- Director preferences

Visualizations (12):
1. runtime_distribution.png - Overall runtime distribution
2. runtime_by_decade.png - How runtimes have evolved over time
3. runtime_vs_rating.png - Does length correlate with quality?
4. runtime_by_genre.png - Genre-specific runtime patterns
5. optimal_runtime_by_genre.png - Sweet spots for each genre
6. director_runtime_preferences.png - Directors' length tendencies
7. runtime_outliers.png - Unusually long/short films
8. pacing_categories.png - Short/medium/long film performance
9. runtime_rating_heatmap.png - 2D correlation view
10. runtime_efficiency.png - Rating per minute analysis
11. decade_genre_runtime.png - Evolution by genre over decades
12. runtime_completion_analysis.png - Personal watch patterns by length

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
from scipy import stats
import ast

class RuntimePacingAnalyzer:
    """Analyzes runtime and pacing patterns in cinema."""

    def __init__(self):
        """Initialize the analyzer."""
        self.base_dir = Path(__file__).parent.parent
        self.data_dir = self.base_dir / 'data'
        self.output_dir = self.base_dir / 'analysis_outputs'
        self.viz_dir = self.output_dir / 'visualizations' / 'batch_17'
        self.reports_dir = self.output_dir / 'reports'

        # Create output directories
        self.viz_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)

        # Set visualization style
        sns.set_style("whitegrid")
        plt.rcParams['figure.figsize'] = (12, 8)
        plt.rcParams['font.size'] = 10

        # Data containers
        self.movies_df = None
        self.people_cache = {}
        self.stats = {}

        print("="*80)
        print("CINESCOPE BATCH 17: RUNTIME & PACING ANALYSIS")
        print("="*80)
        print()

    def load_data(self):
        """Load all required data files."""
        print("Loading data files...")

        # Load movies
        movies_path = self.data_dir / 'processed' / 'watched_movies_master.csv'
        self.movies_df = pd.read_csv(movies_path)
        print(f"✓ Loaded {len(self.movies_df):,} movies")

        # Load people cache for director analysis
        people_cache_path = self.data_dir / 'processed' / 'people_cache.json'
        with open(people_cache_path, 'r', encoding='utf-8') as f:
            self.people_cache = json.load(f)
        print(f"✓ Loaded {len(self.people_cache):,} people from cache")

        print()

    def process_runtime_data(self):
        """Process and clean runtime data."""
        print("Processing runtime data...")

        # Get runtime column (try different possible column names)
        runtime_col = None
        for col in ['Runtime (mins)', 'Runtime', 'runtime', 'Runtime_mins']:
            if col in self.movies_df.columns:
                runtime_col = col
                break

        if runtime_col is None:
            print("! Runtime column not found")
            return

        # Create working dataframe
        self.df = self.movies_df.copy()

        # Clean runtime data
        self.df['runtime'] = pd.to_numeric(self.df[runtime_col], errors='coerce')

        # Filter out invalid runtimes (< 40 minutes or > 300 minutes)
        self.df = self.df[
            (self.df['runtime'] >= 40) &
            (self.df['runtime'] <= 300) &
            (self.df['runtime'].notna())
        ].copy()

        # Parse genres
        def parse_genres(genres_str):
            if not genres_str or pd.isna(genres_str):
                return []

            if '|' in str(genres_str):
                return [g.strip() for g in str(genres_str).split('|') if g.strip()]
            elif ',' in str(genres_str):
                genres_str = str(genres_str).strip("[]'\"")
                return [g.strip().strip("'\"") for g in genres_str.split(',') if g.strip()]
            else:
                return [str(genres_str).strip()]

        self.df['genres_list'] = self.df.get('Genres', self.df.get('genres', '')).apply(parse_genres)

        # Calculate stats
        self.stats['total_movies'] = len(self.df)
        self.stats['mean_runtime'] = self.df['runtime'].mean()
        self.stats['median_runtime'] = self.df['runtime'].median()
        self.stats['std_runtime'] = self.df['runtime'].std()
        self.stats['min_runtime'] = self.df['runtime'].min()
        self.stats['max_runtime'] = self.df['runtime'].max()

        # Categorize by length
        self.df['length_category'] = pd.cut(
            self.df['runtime'],
            bins=[0, 90, 120, 180, 999],
            labels=['Short (< 90min)', 'Medium (90-120min)', 'Long (120-180min)', 'Epic (> 180min)']
        )

        print(f"✓ Processed {len(self.df):,} films with valid runtime data")
        print(f"  Mean runtime: {self.stats['mean_runtime']:.1f} minutes")
        print(f"  Range: {self.stats['min_runtime']:.0f} - {self.stats['max_runtime']:.0f} minutes")
        print()

    def visualize_runtime_distribution(self):
        """Visualize overall runtime distribution."""
        print("Creating runtime distribution visualization...")

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        # Histogram
        ax1.hist(self.df['runtime'], bins=50, color='steelblue', alpha=0.7, edgecolor='black')
        ax1.axvline(self.stats['mean_runtime'], color='red', linestyle='--',
                   linewidth=2, label=f'Mean: {self.stats["mean_runtime"]:.1f} min')
        ax1.axvline(self.stats['median_runtime'], color='green', linestyle='--',
                   linewidth=2, label=f'Median: {self.stats["median_runtime"]:.1f} min')
        ax1.set_xlabel('Runtime (minutes)', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Frequency', fontsize=12, fontweight='bold')
        ax1.set_title('Runtime Distribution', fontsize=14, fontweight='bold')
        ax1.legend()
        ax1.grid(axis='y', alpha=0.3)

        # Box plot
        ax2.boxplot(self.df['runtime'], vert=True, patch_artist=True,
                   boxprops=dict(facecolor='lightblue', alpha=0.7),
                   medianprops=dict(color='red', linewidth=2))
        ax2.set_ylabel('Runtime (minutes)', fontsize=12, fontweight='bold')
        ax2.set_title('Runtime Box Plot', fontsize=14, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        # Add stats text
        stats_text = f'Mean: {self.stats["mean_runtime"]:.1f} min\n'
        stats_text += f'Median: {self.stats["median_runtime"]:.1f} min\n'
        stats_text += f'Std: {self.stats["std_runtime"]:.1f} min\n'
        stats_text += f'Range: {self.stats["min_runtime"]:.0f}-{self.stats["max_runtime"]:.0f} min'
        ax2.text(0.98, 0.98, stats_text, transform=ax2.transAxes,
                verticalalignment='top', horizontalalignment='right',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
                fontsize=10)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'runtime_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: runtime_distribution.png")

    def visualize_runtime_by_decade(self):
        """Visualize how runtimes have evolved over time."""
        print("Creating runtime by decade visualization...")

        # Extract year and decade
        self.df['year'] = pd.to_numeric(self.df.get('Year', 0), errors='coerce')
        self.df['decade'] = (self.df['year'] // 10) * 10

        # Filter valid decades
        decade_df = self.df[(self.df['decade'] >= 1920) & (self.df['decade'] <= 2020)].copy()

        if len(decade_df) == 0:
            print("! No decade data available")
            return

        # Calculate stats by decade
        decade_stats = decade_df.groupby('decade')['runtime'].agg(['mean', 'median', 'std', 'count'])

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))

        # Line plot of mean/median
        decades = decade_stats.index
        ax1.plot(decades, decade_stats['mean'], marker='o', linewidth=2,
                markersize=8, label='Mean', color='steelblue')
        ax1.plot(decades, decade_stats['median'], marker='s', linewidth=2,
                markersize=8, label='Median', color='darkgreen')
        ax1.fill_between(decades,
                         decade_stats['mean'] - decade_stats['std'],
                         decade_stats['mean'] + decade_stats['std'],
                         alpha=0.2, color='steelblue', label='±1 Std Dev')
        ax1.set_xlabel('Decade', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Runtime (minutes)', fontsize=12, fontweight='bold')
        ax1.set_title('Runtime Evolution Over Decades', fontsize=14, fontweight='bold')
        ax1.legend()
        ax1.grid(alpha=0.3)

        # Count by decade
        ax2.bar(decades, decade_stats['count'], color='coral', alpha=0.7, edgecolor='black')
        ax2.set_xlabel('Decade', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Number of Films', fontsize=12, fontweight='bold')
        ax2.set_title('Films per Decade', fontsize=14, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        # Add value labels
        for decade, count in zip(decades, decade_stats['count']):
            ax2.text(decade, count, f'{int(count)}', ha='center', va='bottom', fontsize=9)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'runtime_by_decade.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: runtime_by_decade.png")

    def visualize_runtime_vs_rating(self):
        """Analyze correlation between runtime and rating."""
        print("Creating runtime vs rating visualization...")

        # Get rating data
        rating_col = 'IMDb Rating'
        if rating_col not in self.df.columns:
            print("! No rating data available")
            return

        # Filter rated movies
        rated_df = self.df[self.df[rating_col].notna() & (self.df[rating_col] > 0)].copy()

        if len(rated_df) == 0:
            print("! No rated movies")
            return

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

        # Scatter plot
        ax1.scatter(rated_df['runtime'], rated_df[rating_col], alpha=0.5, s=30, color='steelblue')
        ax1.set_xlabel('Runtime (minutes)', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Rating', fontsize=12, fontweight='bold')
        ax1.set_title('Runtime vs Rating', fontsize=14, fontweight='bold')
        ax1.grid(alpha=0.3)

        # Add trendline
        z = np.polyfit(rated_df['runtime'], rated_df[rating_col], 1)
        p = np.poly1d(z)
        ax1.plot(sorted(rated_df['runtime']), p(sorted(rated_df['runtime'])),
                "r--", alpha=0.8, linewidth=2, label=f'Trend: y={z[0]:.4f}x+{z[1]:.2f}')

        # Calculate correlation
        corr, p_value = stats.pearsonr(rated_df['runtime'], rated_df[rating_col])
        ax1.text(0.05, 0.95, f'Correlation: {corr:.3f}\np-value: {p_value:.4f}',
                transform=ax1.transAxes, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
                fontsize=10)
        ax1.legend()

        self.stats['runtime_rating_correlation'] = corr
        self.stats['runtime_rating_pvalue'] = p_value

        # Box plot by length category
        rated_df.boxplot(column=rating_col, by='length_category', ax=ax2,
                        patch_artist=True)
        ax2.set_xlabel('Length Category', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Rating', fontsize=12, fontweight='bold')
        ax2.set_title('Rating by Length Category', fontsize=14, fontweight='bold')
        plt.suptitle('')  # Remove default title
        ax2.grid(axis='y', alpha=0.3)

        # Average rating by runtime bins
        runtime_bins = pd.cut(rated_df['runtime'], bins=10)
        avg_by_bin = rated_df.groupby(runtime_bins)[rating_col].mean()
        bin_centers = [interval.mid for interval in avg_by_bin.index]

        ax3.plot(bin_centers, avg_by_bin.values, marker='o', linewidth=2,
                markersize=8, color='darkgreen')
        ax3.set_xlabel('Runtime (minutes)', fontsize=12, fontweight='bold')
        ax3.set_ylabel('Average Rating', fontsize=12, fontweight='bold')
        ax3.set_title('Average Rating by Runtime Bins', fontsize=14, fontweight='bold')
        ax3.grid(alpha=0.3)

        # Histogram of ratings by category
        categories = rated_df['length_category'].unique()
        colors = plt.cm.viridis(np.linspace(0, 1, len(categories)))

        for i, category in enumerate(sorted(categories, key=str)):
            cat_data = rated_df[rated_df['length_category'] == category][rating_col]
            ax4.hist(cat_data, bins=10, alpha=0.5, label=category,
                    color=colors[i], edgecolor='black')

        ax4.set_xlabel('Rating', fontsize=12, fontweight='bold')
        ax4.set_ylabel('Frequency', fontsize=12, fontweight='bold')
        ax4.set_title('Rating Distribution by Length Category', fontsize=14, fontweight='bold')
        ax4.legend()
        ax4.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'runtime_vs_rating.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: runtime_vs_rating.png")
        print(f"  Correlation: {corr:.3f} (p={p_value:.4f})")

    def visualize_runtime_by_genre(self):
        """Visualize runtime patterns by genre."""
        print("Creating runtime by genre visualization...")

        # Explode genres
        genre_records = []
        for _, row in self.df.iterrows():
            for genre in row['genres_list']:
                if genre:
                    genre_records.append({
                        'genre': genre,
                        'runtime': row['runtime']
                    })

        if not genre_records:
            print("! No genre data")
            return

        genre_df = pd.DataFrame(genre_records)

        # Calculate stats by genre
        genre_stats = genre_df.groupby('genre')['runtime'].agg(['mean', 'median', 'std', 'count'])
        genre_stats = genre_stats[genre_stats['count'] >= 10].sort_values('mean', ascending=False)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 10))

        # Bar plot of mean runtime
        genres = genre_stats.index[:20]
        means = genre_stats.loc[genres, 'mean']
        stds = genre_stats.loc[genres, 'std']

        colors = plt.cm.viridis(np.linspace(0.3, 0.9, len(genres)))
        bars = ax1.barh(range(len(genres)), means, xerr=stds, color=colors,
                       alpha=0.7, edgecolor='black', capsize=5)

        ax1.set_yticks(range(len(genres)))
        ax1.set_yticklabels(genres)
        ax1.invert_yaxis()
        ax1.set_xlabel('Average Runtime (minutes)', fontsize=12, fontweight='bold')
        ax1.set_title('Average Runtime by Genre (Top 20)', fontsize=14, fontweight='bold', pad=20)
        ax1.grid(axis='x', alpha=0.3)

        # Add value labels
        for i, (mean, std) in enumerate(zip(means, stds)):
            ax1.text(mean, i, f' {mean:.1f}±{std:.1f}', va='center', fontsize=9)

        # Box plot by genre (top 10)
        top_genres = genre_stats.index[:10]
        genre_data = [genre_df[genre_df['genre'] == g]['runtime'].values for g in top_genres]

        bp = ax2.boxplot(genre_data, labels=top_genres, patch_artist=True, vert=False)

        for patch, color in zip(bp['boxes'], plt.cm.viridis(np.linspace(0.3, 0.9, len(top_genres)))):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)

        ax2.set_xlabel('Runtime (minutes)', fontsize=12, fontweight='bold')
        ax2.set_title('Runtime Distribution by Genre (Top 10)', fontsize=14, fontweight='bold', pad=20)
        ax2.grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'runtime_by_genre.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: runtime_by_genre.png")

    def visualize_optimal_runtime_by_genre(self):
        """Find optimal runtime for each genre based on ratings."""
        print("Creating optimal runtime by genre visualization...")

        rating_col = 'IMDb Rating'
        if rating_col not in self.df.columns:
            print("! No rating data")
            return

        # Explode genres with ratings
        genre_records = []
        for _, row in self.df.iterrows():
            rating = row.get(rating_col, 0)
            if pd.isna(rating) or rating == 0:
                continue

            for genre in row['genres_list']:
                if genre:
                    genre_records.append({
                        'genre': genre,
                        'runtime': row['runtime'],
                        'rating': rating
                    })

        if not genre_records:
            print("! No genre-rating data")
            return

        genre_df = pd.DataFrame(genre_records)

        # Get top genres
        top_genres = genre_df['genre'].value_counts().head(10).index

        fig, axes = plt.subplots(2, 5, figsize=(20, 10))
        axes = axes.flatten()

        for i, genre in enumerate(top_genres):
            genre_data = genre_df[genre_df['genre'] == genre]

            # Bin runtimes and calculate average rating
            runtime_bins = pd.cut(genre_data['runtime'], bins=8)
            avg_rating = genre_data.groupby(runtime_bins)['rating'].mean()
            bin_centers = [interval.mid for interval in avg_rating.index]

            axes[i].plot(bin_centers, avg_rating.values, marker='o', linewidth=2, markersize=8)
            axes[i].set_title(f'{genre} (n={len(genre_data)})', fontweight='bold')
            axes[i].set_xlabel('Runtime (min)', fontsize=9)
            axes[i].set_ylabel('Avg Rating', fontsize=9)
            axes[i].grid(alpha=0.3)

            # Highlight optimal runtime
            optimal_idx = avg_rating.argmax()
            optimal_runtime = bin_centers[optimal_idx]
            optimal_rating = avg_rating.values[optimal_idx]

            axes[i].scatter([optimal_runtime], [optimal_rating], color='red',
                          s=100, zorder=5, marker='*')
            axes[i].text(0.05, 0.95, f'Optimal: {optimal_runtime:.0f}min',
                        transform=axes[i].transAxes, verticalalignment='top',
                        bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.7),
                        fontsize=8)

        plt.suptitle('Optimal Runtime by Genre', fontsize=16, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.viz_dir / 'optimal_runtime_by_genre.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: optimal_runtime_by_genre.png")

    def visualize_director_runtime_preferences(self):
        """Analyze director runtime preferences."""
        print("Creating director runtime preferences visualization...")

        # Get director IDs
        director_runtimes = defaultdict(list)

        for _, movie in self.df.iterrows():
            runtime = movie['runtime']
            director_str = movie.get('imdb_director_ids', '')

            if not director_str or pd.isna(director_str):
                continue

            # Parse director IDs
            director_ids = []
            try:
                director_ids_list = ast.literal_eval(str(director_str))
                if isinstance(director_ids_list, list):
                    director_ids = [str(id).strip() for id in director_ids_list if id]
            except:
                director_ids = [id.strip() for id in str(director_str).split('|') if id.strip()]

            for director_id in director_ids:
                director_runtimes[director_id].append(runtime)

        # Calculate average runtime per director
        director_stats = []
        for director_id, runtimes in director_runtimes.items():
            if len(runtimes) >= 3:  # Minimum 3 films
                # Find name by IMDb ID
                director_name = 'Unknown'
                for cache_id, person_data in self.people_cache.items():
                    if person_data.get('imdb_id') == director_id:
                        director_name = person_data.get('imdb_name', 'Unknown')
                        break

                director_stats.append({
                    'name': director_name,
                    'avg_runtime': np.mean(runtimes),
                    'std_runtime': np.std(runtimes),
                    'count': len(runtimes)
                })

        if not director_stats:
            print("! No director runtime data")
            return

        director_df = pd.DataFrame(director_stats).sort_values('avg_runtime', ascending=False)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 10))

        # Top 20 longest runtimes
        top_20 = director_df.head(20)
        colors = plt.cm.Reds(np.linspace(0.4, 0.9, len(top_20)))

        bars = ax1.barh(range(len(top_20)), top_20['avg_runtime'],
                       xerr=top_20['std_runtime'], color=colors,
                       alpha=0.7, edgecolor='black', capsize=5)

        ax1.set_yticks(range(len(top_20)))
        ax1.set_yticklabels(top_20['name'], fontsize=9)
        ax1.invert_yaxis()
        ax1.set_xlabel('Average Runtime (minutes)', fontsize=12, fontweight='bold')
        ax1.set_title('Directors with Longest Average Runtimes', fontsize=14, fontweight='bold', pad=20)
        ax1.grid(axis='x', alpha=0.3)

        # Add labels
        for i, (runtime, count) in enumerate(zip(top_20['avg_runtime'], top_20['count'])):
            ax1.text(runtime, i, f' {runtime:.1f} (n={count})', va='center', fontsize=8)

        # Bottom 20 shortest runtimes
        bottom_20 = director_df.tail(20)
        colors = plt.cm.Blues(np.linspace(0.4, 0.9, len(bottom_20)))

        bars = ax2.barh(range(len(bottom_20)), bottom_20['avg_runtime'],
                       xerr=bottom_20['std_runtime'], color=colors,
                       alpha=0.7, edgecolor='black', capsize=5)

        ax2.set_yticks(range(len(bottom_20)))
        ax2.set_yticklabels(bottom_20['name'], fontsize=9)
        ax2.invert_yaxis()
        ax2.set_xlabel('Average Runtime (minutes)', fontsize=12, fontweight='bold')
        ax2.set_title('Directors with Shortest Average Runtimes', fontsize=14, fontweight='bold', pad=20)
        ax2.grid(axis='x', alpha=0.3)

        # Add labels
        for i, (runtime, count) in enumerate(zip(bottom_20['avg_runtime'], bottom_20['count'])):
            ax2.text(runtime, i, f' {runtime:.1f} (n={count})', va='center', fontsize=8)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'director_runtime_preferences.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: director_runtime_preferences.png")

    def visualize_runtime_outliers(self):
        """Identify and visualize runtime outliers."""
        print("Creating runtime outliers visualization...")

        rating_col = 'IMDb Rating'

        # Calculate z-scores
        self.df['runtime_zscore'] = np.abs(stats.zscore(self.df['runtime']))

        # Get outliers (z-score > 2)
        outliers = self.df[self.df['runtime_zscore'] > 2].copy()
        outliers = outliers.sort_values('runtime', ascending=False)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))

        # Show top 20 longest
        top_20 = outliers.head(20)

        y_pos = np.arange(len(top_20))
        colors = plt.cm.Reds(np.linspace(0.4, 0.9, len(top_20)))

        bars = ax1.barh(y_pos, top_20['runtime'], color=colors, alpha=0.7, edgecolor='black')

        ax1.set_yticks(y_pos)
        ax1.set_yticklabels([f"{row['Title'][:40]}..." if len(str(row['Title'])) > 40 else row['Title']
                            for _, row in top_20.iterrows()], fontsize=8)
        ax1.invert_yaxis()
        ax1.set_xlabel('Runtime (minutes)', fontsize=12, fontweight='bold')
        ax1.set_title('20 Longest Films (Outliers)', fontsize=14, fontweight='bold', pad=20)
        ax1.grid(axis='x', alpha=0.3)

        # Add runtime labels
        for i, (runtime, rating) in enumerate(zip(top_20['runtime'], top_20.get(rating_col, [0]*len(top_20)))):
            label = f' {runtime:.0f} min'
            if rating and not pd.isna(rating) and rating > 0:
                label += f' (★{rating:.1f})'
            ax1.text(runtime, i, label, va='center', fontsize=8)

        # Show bottom 20 shortest
        bottom_20 = outliers.tail(20)

        y_pos = np.arange(len(bottom_20))
        colors = plt.cm.Blues(np.linspace(0.4, 0.9, len(bottom_20)))

        bars = ax2.barh(y_pos, bottom_20['runtime'], color=colors, alpha=0.7, edgecolor='black')

        ax2.set_yticks(y_pos)
        ax2.set_yticklabels([f"{row['Title'][:40]}..." if len(str(row['Title'])) > 40 else row['Title']
                            for _, row in bottom_20.iterrows()], fontsize=8)
        ax2.invert_yaxis()
        ax2.set_xlabel('Runtime (minutes)', fontsize=12, fontweight='bold')
        ax2.set_title('20 Shortest Films (Outliers)', fontsize=14, fontweight='bold', pad=20)
        ax2.grid(axis='x', alpha=0.3)

        # Add runtime labels
        for i, (runtime, rating) in enumerate(zip(bottom_20['runtime'], bottom_20.get(rating_col, [0]*len(bottom_20)))):
            label = f' {runtime:.0f} min'
            if rating and not pd.isna(rating) and rating > 0:
                label += f' (★{rating:.1f})'
            ax2.text(runtime, i, label, va='center', fontsize=8)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'runtime_outliers.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: runtime_outliers.png")

    def visualize_pacing_categories(self):
        """Compare performance across pacing categories."""
        print("Creating pacing categories visualization...")

        rating_col = 'IMDb Rating'
        if rating_col not in self.df.columns:
            print("! No rating data")
            return

        rated_df = self.df[self.df[rating_col].notna() & (self.df[rating_col] > 0)].copy()

        # Calculate stats by category
        category_stats = rated_df.groupby('length_category')[rating_col].agg(['mean', 'median', 'std', 'count'])

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

        # Bar plot of average ratings
        categories = category_stats.index
        means = category_stats['mean']
        stds = category_stats['std']

        colors = ['#3498db', '#2ecc71', '#f39c12', '#e74c3c']
        bars = ax1.bar(range(len(categories)), means, yerr=stds, color=colors,
                      alpha=0.7, edgecolor='black', capsize=10)

        ax1.set_xticks(range(len(categories)))
        ax1.set_xticklabels(categories, rotation=15, ha='right')
        ax1.set_ylabel('Average Rating', fontsize=12, fontweight='bold')
        ax1.set_title('Average Rating by Length Category', fontsize=14, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)

        # Add value labels
        for i, (mean, std) in enumerate(zip(means, stds)):
            ax1.text(i, mean + std, f'{mean:.2f}', ha='center', va='bottom',
                    fontsize=10, fontweight='bold')

        # Count distribution
        counts = category_stats['count']
        ax2.bar(range(len(categories)), counts, color=colors, alpha=0.7, edgecolor='black')
        ax2.set_xticks(range(len(categories)))
        ax2.set_xticklabels(categories, rotation=15, ha='right')
        ax2.set_ylabel('Number of Films', fontsize=12, fontweight='bold')
        ax2.set_title('Films per Length Category', fontsize=14, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        # Add value labels
        for i, count in enumerate(counts):
            ax2.text(i, count, f'{int(count)}', ha='center', va='bottom',
                    fontsize=10, fontweight='bold')

        # Violin plot
        violin_data = [rated_df[rated_df['length_category'] == cat][rating_col].values
                      for cat in categories]

        parts = ax3.violinplot(violin_data, positions=range(len(categories)), showmeans=True,
                              showmedians=True)

        for i, pc in enumerate(parts['bodies']):
            pc.set_facecolor(colors[i])
            pc.set_alpha(0.7)

        ax3.set_xticks(range(len(categories)))
        ax3.set_xticklabels(categories, rotation=15, ha='right')
        ax3.set_ylabel('Rating', fontsize=12, fontweight='bold')
        ax3.set_title('Rating Distribution by Length Category', fontsize=14, fontweight='bold')
        ax3.grid(axis='y', alpha=0.3)

        # Percentage above average
        overall_avg = rated_df[rating_col].mean()
        pct_above = []
        for cat in categories:
            cat_data = rated_df[rated_df['length_category'] == cat][rating_col]
            pct = (cat_data > overall_avg).sum() / len(cat_data) * 100
            pct_above.append(pct)

        ax4.bar(range(len(categories)), pct_above, color=colors, alpha=0.7, edgecolor='black')
        ax4.axhline(50, color='red', linestyle='--', linewidth=2, label='50% threshold')
        ax4.set_xticks(range(len(categories)))
        ax4.set_xticklabels(categories, rotation=15, ha='right')
        ax4.set_ylabel('Percentage (%)', fontsize=12, fontweight='bold')
        ax4.set_title(f'% Films Above Overall Average ({overall_avg:.2f})', fontsize=14, fontweight='bold')
        ax4.legend()
        ax4.grid(axis='y', alpha=0.3)

        # Add value labels
        for i, pct in enumerate(pct_above):
            ax4.text(i, pct, f'{pct:.1f}%', ha='center', va='bottom',
                    fontsize=10, fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'pacing_categories.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: pacing_categories.png")

    def visualize_runtime_rating_heatmap(self):
        """Create 2D heatmap of runtime vs rating."""
        print("Creating runtime-rating heatmap...")

        rating_col = 'IMDb Rating'
        if rating_col not in self.df.columns:
            print("! No rating data")
            return

        rated_df = self.df[self.df[rating_col].notna() & (self.df[rating_col] > 0)].copy()

        # Create bins
        runtime_bins = pd.cut(rated_df['runtime'], bins=15)
        rating_bins = pd.cut(rated_df[rating_col], bins=10)

        # Create pivot table
        heatmap_data = pd.crosstab(rating_bins, runtime_bins)

        fig, ax = plt.subplots(figsize=(14, 10))

        sns.heatmap(heatmap_data, cmap='YlOrRd', annot=True, fmt='g',
                   cbar_kws={'label': 'Number of Films'}, ax=ax)

        ax.set_xlabel('Runtime (minutes)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Rating', fontsize=12, fontweight='bold')
        ax.set_title('Runtime vs Rating Heatmap', fontsize=14, fontweight='bold', pad=20)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'runtime_rating_heatmap.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: runtime_rating_heatmap.png")

    def visualize_runtime_efficiency(self):
        """Analyze rating per minute (runtime efficiency)."""
        print("Creating runtime efficiency visualization...")

        rating_col = 'IMDb Rating'
        if rating_col not in self.df.columns:
            print("! No rating data")
            return

        rated_df = self.df[self.df[rating_col].notna() & (self.df[rating_col] > 0)].copy()

        # Calculate efficiency
        rated_df['efficiency'] = rated_df[rating_col] / rated_df['runtime']

        # Get top and bottom 20
        top_20 = rated_df.nlargest(20, 'efficiency')
        bottom_20 = rated_df.nsmallest(20, 'efficiency')

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 10))

        # Top 20 most efficient
        colors = plt.cm.Greens(np.linspace(0.4, 0.9, len(top_20)))
        bars = ax1.barh(range(len(top_20)), top_20['efficiency'], color=colors,
                       alpha=0.7, edgecolor='black')

        ax1.set_yticks(range(len(top_20)))
        ax1.set_yticklabels([f"{row['Title'][:35]}..." if len(str(row['Title'])) > 35 else row['Title']
                            for _, row in top_20.iterrows()], fontsize=8)
        ax1.invert_yaxis()
        ax1.set_xlabel('Rating per Minute', fontsize=12, fontweight='bold')
        ax1.set_title('Most Efficient Films (Highest Rating/Minute)', fontsize=14, fontweight='bold', pad=20)
        ax1.grid(axis='x', alpha=0.3)

        # Add labels
        for i, (eff, runtime, rating) in enumerate(zip(top_20['efficiency'], top_20['runtime'], top_20[rating_col])):
            ax1.text(eff, i, f' {eff:.4f} (★{rating:.0f}/{runtime:.0f}min)',
                    va='center', fontsize=7)

        # Bottom 20 least efficient
        colors = plt.cm.Reds(np.linspace(0.4, 0.9, len(bottom_20)))
        bars = ax2.barh(range(len(bottom_20)), bottom_20['efficiency'], color=colors,
                       alpha=0.7, edgecolor='black')

        ax2.set_yticks(range(len(bottom_20)))
        ax2.set_yticklabels([f"{row['Title'][:35]}..." if len(str(row['Title'])) > 35 else row['Title']
                            for _, row in bottom_20.iterrows()], fontsize=8)
        ax2.invert_yaxis()
        ax2.set_xlabel('Rating per Minute', fontsize=12, fontweight='bold')
        ax2.set_title('Least Efficient Films (Lowest Rating/Minute)', fontsize=14, fontweight='bold', pad=20)
        ax2.grid(axis='x', alpha=0.3)

        # Add labels
        for i, (eff, runtime, rating) in enumerate(zip(bottom_20['efficiency'], bottom_20['runtime'], bottom_20[rating_col])):
            ax2.text(eff, i, f' {eff:.4f} (★{rating:.0f}/{runtime:.0f}min)',
                    va='center', fontsize=7)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'runtime_efficiency.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: runtime_efficiency.png")

    def visualize_decade_genre_runtime(self):
        """Evolution of runtime by genre across decades."""
        print("Creating decade-genre runtime evolution...")

        # Get top 6 genres
        genre_counts = Counter()
        for genres_list in self.df['genres_list']:
            for genre in genres_list:
                if genre:
                    genre_counts[genre] += 1

        top_genres = [genre for genre, _ in genre_counts.most_common(6)]

        # Filter by decade
        decade_df = self.df[(self.df.get('decade', 0) >= 1950) & (self.df.get('decade', 0) <= 2020)].copy()

        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        axes = axes.flatten()

        for i, genre in enumerate(top_genres):
            # Get films with this genre
            genre_films = []
            for _, row in decade_df.iterrows():
                if genre in row['genres_list']:
                    genre_films.append({
                        'decade': row.get('decade'),
                        'runtime': row['runtime']
                    })

            if not genre_films:
                continue

            genre_decade_df = pd.DataFrame(genre_films)

            # Calculate stats by decade
            decade_stats = genre_decade_df.groupby('decade')['runtime'].agg(['mean', 'std', 'count'])

            decades = decade_stats.index
            axes[i].plot(decades, decade_stats['mean'], marker='o', linewidth=2, markersize=8)
            axes[i].fill_between(decades,
                                decade_stats['mean'] - decade_stats['std'],
                                decade_stats['mean'] + decade_stats['std'],
                                alpha=0.2)
            axes[i].set_title(f'{genre} (n={len(genre_films)})', fontweight='bold', fontsize=12)
            axes[i].set_xlabel('Decade', fontsize=10)
            axes[i].set_ylabel('Avg Runtime (min)', fontsize=10)
            axes[i].grid(alpha=0.3)

        plt.suptitle('Runtime Evolution by Genre Across Decades', fontsize=16, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.viz_dir / 'decade_genre_runtime.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: decade_genre_runtime.png")

    def visualize_runtime_completion_analysis(self):
        """Analyze personal watch patterns by runtime length."""
        print("Creating runtime completion analysis...")

        # This would require watch date data - create placeholder analysis
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        # Distribution by category
        category_counts = self.df['length_category'].value_counts()

        colors = ['#3498db', '#2ecc71', '#f39c12', '#e74c3c']
        wedges, texts, autotexts = ax1.pie(category_counts.values, labels=category_counts.index,
                                           autopct='%1.1f%%', colors=colors, startangle=90)

        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(10)

        ax1.set_title('Personal Collection by Length Category', fontsize=14, fontweight='bold')

        # Cumulative runtime
        runtime_bins = [60, 90, 105, 120, 135, 150, 180, 240, 300]
        cumulative_counts = []

        for runtime_max in runtime_bins:
            count = len(self.df[self.df['runtime'] <= runtime_max])
            cumulative_counts.append(count)

        ax2.plot(runtime_bins, cumulative_counts, marker='o', linewidth=2, markersize=8, color='steelblue')
        ax2.fill_between(runtime_bins, cumulative_counts, alpha=0.3, color='steelblue')
        ax2.set_xlabel('Maximum Runtime (minutes)', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Cumulative Films Watched', fontsize=12, fontweight='bold')
        ax2.set_title('Cumulative Films by Runtime Threshold', fontsize=14, fontweight='bold')
        ax2.grid(alpha=0.3)

        # Add value labels
        for runtime, count in zip(runtime_bins, cumulative_counts):
            ax2.text(runtime, count, f'{count}', ha='center', va='bottom', fontsize=9)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'runtime_completion_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: runtime_completion_analysis.png")

    def generate_report(self):
        """Generate comprehensive text report."""
        print("Generating report...")

        report_path = self.reports_dir / 'batch_17_runtime_pacing_report.txt'

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("CINESCOPE BATCH 17: RUNTIME & PACING ANALYSIS\n")
            f.write("="*80 + "\n\n")

            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            f.write(f"Total Films Analyzed: {self.stats.get('total_movies', 0):,}\n\n")

            f.write("="*80 + "\n")
            f.write("RUNTIME STATISTICS\n")
            f.write("="*80 + "\n\n")

            f.write(f"Mean Runtime: {self.stats.get('mean_runtime', 0):.1f} minutes\n")
            f.write(f"Median Runtime: {self.stats.get('median_runtime', 0):.1f} minutes\n")
            f.write(f"Standard Deviation: {self.stats.get('std_runtime', 0):.1f} minutes\n")
            f.write(f"Range: {self.stats.get('min_runtime', 0):.0f} - {self.stats.get('max_runtime', 0):.0f} minutes\n\n")

            # Length category distribution
            f.write("Distribution by Length Category:\n")
            category_counts = self.df['length_category'].value_counts().sort_index()
            for category, count in category_counts.items():
                pct = count / len(self.df) * 100
                f.write(f"  {category}: {count:,} films ({pct:.1f}%)\n")

            f.write("\n")

            if 'runtime_rating_correlation' in self.stats:
                f.write("="*80 + "\n")
                f.write("RUNTIME-QUALITY CORRELATION\n")
                f.write("="*80 + "\n\n")

                corr = self.stats['runtime_rating_correlation']
                pval = self.stats['runtime_rating_pvalue']

                f.write(f"Correlation (Runtime vs Rating): {corr:.4f}\n")
                f.write(f"P-value: {pval:.6f}\n\n")

                if pval < 0.05:
                    if corr > 0:
                        f.write(f"Result: Significant POSITIVE correlation - longer films tend to be rated higher\n")
                    else:
                        f.write(f"Result: Significant NEGATIVE correlation - shorter films tend to be rated higher\n")
                else:
                    f.write(f"Result: No significant correlation between runtime and film quality\n")

                f.write("\n")

            f.write("="*80 + "\n")
            f.write("END OF REPORT\n")
            f.write("="*80 + "\n")

        print(f"✓ Report saved: {report_path}")
        print()

    def run(self):
        """Run the complete analysis pipeline."""
        try:
            self.load_data()
            self.process_runtime_data()

            print("\nGenerating visualizations...")
            print("-" * 80)

            self.visualize_runtime_distribution()
            self.visualize_runtime_by_decade()
            self.visualize_runtime_vs_rating()
            self.visualize_runtime_by_genre()
            self.visualize_optimal_runtime_by_genre()
            self.visualize_director_runtime_preferences()
            self.visualize_runtime_outliers()
            self.visualize_pacing_categories()
            self.visualize_runtime_rating_heatmap()
            self.visualize_runtime_efficiency()
            self.visualize_decade_genre_runtime()
            self.visualize_runtime_completion_analysis()

            print("-" * 80)
            print()

            self.generate_report()

            print("="*80)
            print("BATCH 17 ANALYSIS COMPLETE!")
            print("="*80)
            print(f"\nVisualizations saved to: {self.viz_dir}")
            print(f"Report saved to: {self.reports_dir}")
            print()

        except Exception as e:
            print(f"\n❌ Error during analysis: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

if __name__ == '__main__':
    analyzer = RuntimePacingAnalyzer()
    analyzer.run()
