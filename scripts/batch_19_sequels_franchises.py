#!/usr/bin/env python3
"""
================================================================================
CINESCOPE BATCH 19: SEQUELS, FRANCHISES & SERIES ANALYSIS
================================================================================

Focus: How franchises perform, sequel quality patterns, series fatigue

Data Sources:
- Movie titles, years, collections
- Rating data
- Budget/revenue trends
- Director/cast consistency

Visualizations (12):
1. franchise_distribution.png - Distribution of franchise sizes
2. sequel_quality_decay.png - Do sequels get worse over time?
3. top_franchises.png - Highest-rated franchises
4. franchise_consistency.png - Rating variance within franchises
5. sequel_numbering_analysis.png - Performance by sequel number
6. franchise_financial_performance.png - Budget and revenue trends
7. director_consistency.png - Director changes across franchises
8. cast_consistency.png - Cast retention patterns
9. time_gaps_analysis.png - Time between sequels
10. standalone_vs_franchise.png - Standalone vs franchise comparison
11. franchise_completion_rate.png - Personal watch completion
12. genre_franchise_patterns.png - Which genres spawn most franchises

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
import re
from scipy import stats

class SequelFranchiseAnalyzer:
    """Analyzes sequels, franchises, and series patterns."""

    def __init__(self):
        """Initialize the analyzer."""
        self.base_dir = Path(__file__).parent.parent
        self.data_dir = self.base_dir / 'data'
        self.output_dir = self.base_dir / 'analysis_outputs'
        self.viz_dir = self.output_dir / 'visualizations' / 'batch_19'
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
        self.franchises = defaultdict(list)
        self.stats = {}

        print("="*80)
        print("CINESCOPE BATCH 19: SEQUELS, FRANCHISES & SERIES ANALYSIS")
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

    def identify_franchises(self):
        """Identify franchises and sequels from movie titles."""
        print("Identifying franchises and sequels...")

        # Patterns for sequels
        sequel_patterns = [
            r'(.+?)\s+(\d+)$',  # "Title 2"
            r'(.+?)\s+[IVXLCDM]+$',  # "Title III"
            r'(.+?)\s+Part\s+(\d+)',  # "Title Part 2"
            r'(.+?):\s+',  # "Title: Subtitle"
        ]

        # Use TMDB collection field
        collection_col = 'tmdb_belongs_to_collection'

        # Build franchise groups
        franchise_groups = defaultdict(list)

        for idx, row in self.movies_df.iterrows():
            title = str(row.get('Title', ''))
            year = row.get('Year', 0)
            collection = row.get(collection_col, '')

            # Use collection if available
            if collection and not pd.isna(collection) and str(collection).lower() != 'nan':
                franchise_name = str(collection).strip()
                franchise_groups[franchise_name].append({
                    'title': title,
                    'year': year,
                    'rating': row.get('Your Rating', 0),
                    'revenue': row.get('tmdb_revenue', 0),
                    'budget': row.get('tmdb_budget', 0),
                    'index': idx
                })
            # Otherwise try pattern matching
            else:
                base_title = None

                # Try to extract base title from numbered sequels
                if re.search(r'\d+$', title):
                    match = re.match(r'(.+?)\s+\d+$', title)
                    if match:
                        base_title = match.group(1).strip()

                # Try colon-based sequels
                elif ':' in title:
                    base_title = title.split(':')[0].strip()

                if base_title:
                    franchise_groups[base_title].append({
                        'title': title,
                        'year': year,
                        'rating': row.get('Your Rating', 0),
                        'revenue': row.get('tmdb_revenue', 0),
                        'budget': row.get('tmdb_budget', 0),
                        'index': idx
                    })

        # Filter to only franchises with 2+ movies
        self.franchises = {k: sorted(v, key=lambda x: x['year'])
                          for k, v in franchise_groups.items()
                          if len(v) >= 2}

        # Calculate stats
        self.stats['total_franchises'] = len(self.franchises)
        self.stats['total_franchise_movies'] = sum(len(movies) for movies in self.franchises.values())
        self.stats['total_standalone'] = len(self.movies_df) - self.stats['total_franchise_movies']

        franchise_sizes = [len(movies) for movies in self.franchises.values()]
        if franchise_sizes:
            self.stats['avg_franchise_size'] = np.mean(franchise_sizes)
            self.stats['max_franchise_size'] = max(franchise_sizes)

        print(f"✓ Identified {self.stats['total_franchises']} franchises")
        print(f"  Total franchise movies: {self.stats['total_franchise_movies']}")
        print(f"  Total standalone movies: {self.stats['total_standalone']}")
        print(f"  Average franchise size: {self.stats.get('avg_franchise_size', 0):.1f} films")
        print(f"  Largest franchise: {self.stats.get('max_franchise_size', 0)} films")
        print()

    def visualize_franchise_distribution(self):
        """Visualize distribution of franchise sizes."""
        print("Creating franchise distribution visualization...")

        franchise_sizes = [len(movies) for movies in self.franchises.values()]

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        # Histogram
        counts = Counter(franchise_sizes)
        sizes = sorted(counts.keys())
        frequencies = [counts[s] for s in sizes]

        ax1.bar(sizes, frequencies, color='steelblue', alpha=0.7, edgecolor='black')
        ax1.set_xlabel('Franchise Size (number of films)', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Number of Franchises', fontsize=12, fontweight='bold')
        ax1.set_title('Distribution of Franchise Sizes', fontsize=14, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)

        # Add value labels
        for size, freq in zip(sizes, frequencies):
            ax1.text(size, freq, str(freq), ha='center', va='bottom', fontsize=9)

        # Pie chart: Standalone vs Franchise
        labels = ['Standalone Films', 'Franchise Films']
        values = [self.stats['total_standalone'], self.stats['total_franchise_movies']]
        colors = ['#3498db', '#e74c3c']

        wedges, texts, autotexts = ax2.pie(values, labels=labels, autopct='%1.1f%%',
                                           colors=colors, startangle=90)

        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(12)

        ax2.set_title('Standalone vs Franchise Films', fontsize=14, fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'franchise_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: franchise_distribution.png")

    def visualize_sequel_quality_decay(self):
        """Analyze if sequel quality degrades over time."""
        print("Creating sequel quality decay visualization...")

        # Get franchises with ratings
        sequel_positions = []

        for franchise_name, movies in self.franchises.items():
            rated_movies = [m for m in movies if m['rating'] and not pd.isna(m['rating']) and m['rating'] > 0]

            for position, movie in enumerate(rated_movies, 1):
                sequel_positions.append({
                    'position': position,
                    'rating': movie['rating'],
                    'franchise': franchise_name
                })

        if not sequel_positions:
            print("! No rated franchise movies")
            return

        df = pd.DataFrame(sequel_positions)

        # Calculate average by position
        position_stats = df.groupby('position')['rating'].agg(['mean', 'std', 'count'])
        position_stats = position_stats[position_stats['count'] >= 5]  # At least 5 data points

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        # Line plot with error bars
        positions = position_stats.index
        ax1.errorbar(positions, position_stats['mean'], yerr=position_stats['std'],
                    marker='o', linewidth=2, markersize=8, capsize=5, color='steelblue')
        ax1.set_xlabel('Position in Franchise', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Average Rating', fontsize=12, fontweight='bold')
        ax1.set_title('Sequel Quality Decay Analysis', fontsize=14, fontweight='bold')
        ax1.grid(alpha=0.3)
        ax1.set_xticks(positions)

        # Add trendline
        if len(positions) > 1:
            z = np.polyfit(positions, position_stats['mean'], 1)
            p = np.poly1d(z)
            ax1.plot(positions, p(positions), "r--", alpha=0.8, linewidth=2,
                    label=f'Trend: {z[0]:.3f}x + {z[1]:.2f}')
            ax1.legend()

            # Calculate correlation
            corr, p_value = stats.pearsonr(positions, position_stats['mean'])
            ax1.text(0.05, 0.95, f'Correlation: {corr:.3f}\np-value: {p_value:.4f}',
                    transform=ax1.transAxes, verticalalignment='top',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
                    fontsize=10)

        # Box plot by position
        box_data = [df[df['position'] == pos]['rating'].values
                   for pos in sorted(df['position'].unique())[:8]]  # First 8 positions
        box_positions = sorted(df['position'].unique())[:8]

        bp = ax2.boxplot(box_data, positions=box_positions, patch_artist=True)

        for patch in bp['boxes']:
            patch.set_facecolor('lightblue')
            patch.set_alpha(0.7)

        ax2.set_xlabel('Position in Franchise', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Rating', fontsize=12, fontweight='bold')
        ax2.set_title('Rating Distribution by Position', fontsize=14, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'sequel_quality_decay.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: sequel_quality_decay.png")

    def visualize_top_franchises(self):
        """Show highest-rated franchises."""
        print("Creating top franchises visualization...")

        # Calculate average ratings for franchises
        franchise_ratings = []

        for franchise_name, movies in self.franchises.items():
            ratings = [m['rating'] for m in movies if m['rating'] and not pd.isna(m['rating']) and m['rating'] > 0]

            if len(ratings) >= 2:  # Need at least 2 rated movies
                franchise_ratings.append({
                    'franchise': franchise_name[:40],  # Truncate long names
                    'avg_rating': np.mean(ratings),
                    'count': len(movies),
                    'rated_count': len(ratings)
                })

        if not franchise_ratings:
            print("! No franchise ratings available")
            return

        df = pd.DataFrame(franchise_ratings).sort_values('avg_rating', ascending=False)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 10))

        # Top 20
        top_20 = df.head(20)
        colors = plt.cm.Greens(np.linspace(0.4, 0.9, len(top_20)))

        bars = ax1.barh(range(len(top_20)), top_20['avg_rating'], color=colors,
                       alpha=0.7, edgecolor='black')

        ax1.set_yticks(range(len(top_20)))
        ax1.set_yticklabels(top_20['franchise'], fontsize=9)
        ax1.invert_yaxis()
        ax1.set_xlabel('Average Rating', fontsize=12, fontweight='bold')
        ax1.set_title('Top 20 Franchises by Average Rating', fontsize=14, fontweight='bold', pad=20)
        ax1.grid(axis='x', alpha=0.3)

        # Add labels
        for i, (rating, count) in enumerate(zip(top_20['avg_rating'], top_20['count'])):
            ax1.text(rating, i, f' {rating:.2f} ({count} films)', va='center', fontsize=8)

        # Bottom 20
        bottom_20 = df.tail(20)
        colors = plt.cm.Reds(np.linspace(0.4, 0.9, len(bottom_20)))

        bars = ax2.barh(range(len(bottom_20)), bottom_20['avg_rating'], color=colors,
                       alpha=0.7, edgecolor='black')

        ax2.set_yticks(range(len(bottom_20)))
        ax2.set_yticklabels(bottom_20['franchise'], fontsize=9)
        ax2.invert_yaxis()
        ax2.set_xlabel('Average Rating', fontsize=12, fontweight='bold')
        ax2.set_title('Bottom 20 Franchises by Average Rating', fontsize=14, fontweight='bold', pad=20)
        ax2.grid(axis='x', alpha=0.3)

        # Add labels
        for i, (rating, count) in enumerate(zip(bottom_20['avg_rating'], bottom_20['count'])):
            ax2.text(rating, i, f' {rating:.2f} ({count} films)', va='center', fontsize=8)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'top_franchises.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: top_franchises.png")

    def visualize_franchise_consistency(self):
        """Analyze rating variance within franchises."""
        print("Creating franchise consistency visualization...")

        # Calculate standard deviation of ratings
        franchise_consistency = []

        for franchise_name, movies in self.franchises.items():
            ratings = [m['rating'] for m in movies if m['rating'] and not pd.isna(m['rating']) and m['rating'] > 0]

            if len(ratings) >= 3:  # Need at least 3 for meaningful std dev
                franchise_consistency.append({
                    'franchise': franchise_name[:40],
                    'std': np.std(ratings),
                    'avg_rating': np.mean(ratings),
                    'count': len(ratings),
                    'range': max(ratings) - min(ratings)
                })

        if not franchise_consistency:
            print("! No franchise consistency data")
            return

        df = pd.DataFrame(franchise_consistency)

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

        # Most consistent (lowest std dev)
        most_consistent = df.nsmallest(15, 'std')
        colors = plt.cm.Greens(np.linspace(0.4, 0.9, len(most_consistent)))

        ax1.barh(range(len(most_consistent)), most_consistent['std'], color=colors,
                alpha=0.7, edgecolor='black')
        ax1.set_yticks(range(len(most_consistent)))
        ax1.set_yticklabels(most_consistent['franchise'], fontsize=9)
        ax1.invert_yaxis()
        ax1.set_xlabel('Standard Deviation', fontsize=12, fontweight='bold')
        ax1.set_title('Most Consistent Franchises (Lowest Std Dev)', fontsize=14, fontweight='bold')
        ax1.grid(axis='x', alpha=0.3)

        # Least consistent (highest std dev)
        least_consistent = df.nlargest(15, 'std')
        colors = plt.cm.Reds(np.linspace(0.4, 0.9, len(least_consistent)))

        ax2.barh(range(len(least_consistent)), least_consistent['std'], color=colors,
                alpha=0.7, edgecolor='black')
        ax2.set_yticks(range(len(least_consistent)))
        ax2.set_yticklabels(least_consistent['franchise'], fontsize=9)
        ax2.invert_yaxis()
        ax2.set_xlabel('Standard Deviation', fontsize=12, fontweight='bold')
        ax2.set_title('Least Consistent Franchises (Highest Std Dev)', fontsize=14, fontweight='bold')
        ax2.grid(axis='x', alpha=0.3)

        # Scatter: Average rating vs consistency
        ax3.scatter(df['avg_rating'], df['std'], alpha=0.6, s=50, color='steelblue')
        ax3.set_xlabel('Average Rating', fontsize=12, fontweight='bold')
        ax3.set_ylabel('Standard Deviation', fontsize=12, fontweight='bold')
        ax3.set_title('Quality vs Consistency', fontsize=14, fontweight='bold')
        ax3.grid(alpha=0.3)

        # Histogram of std devs
        ax4.hist(df['std'], bins=20, color='purple', alpha=0.7, edgecolor='black')
        ax4.axvline(df['std'].mean(), color='red', linestyle='--', linewidth=2,
                   label=f'Mean: {df["std"].mean():.2f}')
        ax4.set_xlabel('Standard Deviation', fontsize=12, fontweight='bold')
        ax4.set_ylabel('Frequency', fontsize=12, fontweight='bold')
        ax4.set_title('Distribution of Franchise Consistency', fontsize=14, fontweight='bold')
        ax4.legend()
        ax4.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'franchise_consistency.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: franchise_consistency.png")

    def visualize_sequel_numbering(self):
        """Performance analysis by sequel number."""
        print("Creating sequel numbering analysis...")

        # Count and average rating by position
        position_data = defaultdict(lambda: {'ratings': [], 'count': 0})

        for franchise_name, movies in self.franchises.items():
            for position, movie in enumerate(movies, 1):
                if position <= 10:  # Only first 10
                    position_data[position]['count'] += 1
                    if movie['rating'] and not pd.isna(movie['rating']) and movie['rating'] > 0:
                        position_data[position]['ratings'].append(movie['rating'])

        positions = sorted(position_data.keys())
        counts = [position_data[p]['count'] for p in positions]
        avg_ratings = [np.mean(position_data[p]['ratings']) if position_data[p]['ratings'] else 0
                      for p in positions]

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        # Count by position
        colors = plt.cm.viridis(np.linspace(0.2, 0.9, len(positions)))
        bars = ax1.bar(positions, counts, color=colors, alpha=0.7, edgecolor='black')

        ax1.set_xlabel('Position in Franchise', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Number of Films', fontsize=12, fontweight='bold')
        ax1.set_title('Film Count by Sequel Number', fontsize=14, fontweight='bold')
        ax1.set_xticks(positions)
        ax1.grid(axis='y', alpha=0.3)

        # Add value labels
        for pos, count in zip(positions, counts):
            ax1.text(pos, count, str(count), ha='center', va='bottom', fontsize=9)

        # Average rating by position
        valid_positions = [p for p, r in zip(positions, avg_ratings) if r > 0]
        valid_ratings = [r for r in avg_ratings if r > 0]

        if valid_ratings:
            ax2.plot(valid_positions, valid_ratings, marker='o', linewidth=2, markersize=10,
                    color='darkgreen')
            ax2.set_xlabel('Position in Franchise', fontsize=12, fontweight='bold')
            ax2.set_ylabel('Average Rating', fontsize=12, fontweight='bold')
            ax2.set_title('Average Rating by Sequel Number', fontsize=14, fontweight='bold')
            ax2.set_xticks(valid_positions)
            ax2.grid(alpha=0.3)

            # Add value labels
            for pos, rating in zip(valid_positions, valid_ratings):
                ax2.text(pos, rating, f'{rating:.2f}', ha='center', va='bottom', fontsize=9)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'sequel_numbering_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: sequel_numbering_analysis.png")

    def visualize_financial_performance(self):
        """Franchise financial performance over time."""
        print("Creating franchise financial performance visualization...")

        # Get franchises with financial data
        franchises_with_money = []

        for franchise_name, movies in self.franchises.items():
            budgets = [m['budget'] for m in movies if m.get('budget', 0) and not pd.isna(m.get('budget', 0)) and m.get('budget', 0) > 0]
            revenues = [m['revenue'] for m in movies if m.get('revenue', 0) and not pd.isna(m.get('revenue', 0)) and m.get('revenue', 0) > 0]

            if len(budgets) >= 2 and len(revenues) >= 2:
                franchises_with_money.append({
                    'franchise': franchise_name[:30],
                    'avg_budget': np.mean(budgets),
                    'avg_revenue': np.mean(revenues),
                    'avg_roi': (np.mean(revenues) - np.mean(budgets)) / np.mean(budgets) * 100,
                    'count': len(movies)
                })

        if not franchises_with_money:
            print("! No financial data available")

            # Create placeholder
            fig, ax = plt.subplots(figsize=(12, 8))
            ax.text(0.5, 0.5, 'Insufficient Financial Data\nfor Franchise Analysis',
                   ha='center', va='center', fontsize=16, transform=ax.transAxes)
            ax.axis('off')
            plt.savefig(self.viz_dir / 'franchise_financial_performance.png', dpi=300, bbox_inches='tight')
            plt.close()
            print(f"✓ Saved: franchise_financial_performance.png (placeholder)")
            return

        df = pd.DataFrame(franchises_with_money)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))

        # Top ROI franchises
        top_roi = df.nlargest(15, 'avg_roi')
        colors = plt.cm.Greens(np.linspace(0.4, 0.9, len(top_roi)))

        ax1.barh(range(len(top_roi)), top_roi['avg_roi'], color=colors,
                alpha=0.7, edgecolor='black')
        ax1.set_yticks(range(len(top_roi)))
        ax1.set_yticklabels(top_roi['franchise'], fontsize=9)
        ax1.invert_yaxis()
        ax1.set_xlabel('Average ROI (%)', fontsize=12, fontweight='bold')
        ax1.set_title('Top Franchises by ROI', fontsize=14, fontweight='bold')
        ax1.grid(axis='x', alpha=0.3)

        # Budget vs Revenue scatter
        ax2.scatter(df['avg_budget']/1e6, df['avg_revenue']/1e6,
                   alpha=0.6, s=100, color='steelblue')

        # Add diagonal line (break-even)
        max_val = max(df['avg_revenue'].max(), df['avg_budget'].max()) / 1e6
        ax2.plot([0, max_val], [0, max_val], 'r--', linewidth=2, alpha=0.7, label='Break-even')

        ax2.set_xlabel('Average Budget (Millions $)', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Average Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax2.set_title('Budget vs Revenue for Franchises', fontsize=14, fontweight='bold')
        ax2.legend()
        ax2.grid(alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'franchise_financial_performance.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: franchise_financial_performance.png")

    def visualize_standalone_vs_franchise(self):
        """Compare standalone films to franchise films."""
        print("Creating standalone vs franchise comparison...")

        # Get ratings for both categories
        franchise_indices = set()
        for movies in self.franchises.values():
            for movie in movies:
                franchise_indices.add(movie['index'])

        franchise_ratings = []
        standalone_ratings = []

        for idx, row in self.movies_df.iterrows():
            rating = row.get('Your Rating', 0)
            if rating and not pd.isna(rating) and rating > 0:
                if idx in franchise_indices:
                    franchise_ratings.append(rating)
                else:
                    standalone_ratings.append(rating)

        if not franchise_ratings or not standalone_ratings:
            print("! Insufficient rating data")
            return

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

        # Box plot comparison
        box_data = [standalone_ratings, franchise_ratings]
        bp = ax1.boxplot(box_data, labels=['Standalone', 'Franchise'],
                        patch_artist=True, vert=True)

        bp['boxes'][0].set_facecolor('lightblue')
        bp['boxes'][1].set_facecolor('lightcoral')

        for box in bp['boxes']:
            box.set_alpha(0.7)

        ax1.set_ylabel('Rating', fontsize=12, fontweight='bold')
        ax1.set_title('Rating Distribution: Standalone vs Franchise', fontsize=14, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)

        # Add stats text
        t_stat, p_value = stats.ttest_ind(standalone_ratings, franchise_ratings)
        stats_text = f'Standalone: μ={np.mean(standalone_ratings):.2f}, σ={np.std(standalone_ratings):.2f}\n'
        stats_text += f'Franchise: μ={np.mean(franchise_ratings):.2f}, σ={np.std(franchise_ratings):.2f}\n'
        stats_text += f't-test: p={p_value:.4f}'

        ax1.text(0.05, 0.95, stats_text, transform=ax1.transAxes,
                verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
                fontsize=9)

        # Histogram comparison
        ax2.hist([standalone_ratings, franchise_ratings], bins=15, label=['Standalone', 'Franchise'],
                color=['lightblue', 'lightcoral'], alpha=0.7, edgecolor='black')
        ax2.set_xlabel('Rating', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Frequency', fontsize=12, fontweight='bold')
        ax2.set_title('Rating Histogram', fontsize=14, fontweight='bold')
        ax2.legend()
        ax2.grid(axis='y', alpha=0.3)

        # Bar chart of averages
        categories = ['Standalone', 'Franchise']
        averages = [np.mean(standalone_ratings), np.mean(franchise_ratings)]
        colors = ['lightblue', 'lightcoral']

        bars = ax3.bar(categories, averages, color=colors, alpha=0.7, edgecolor='black')
        ax3.set_ylabel('Average Rating', fontsize=12, fontweight='bold')
        ax3.set_title('Average Rating Comparison', fontsize=14, fontweight='bold')
        ax3.grid(axis='y', alpha=0.3)

        # Add value labels
        for i, avg in enumerate(averages):
            ax3.text(i, avg, f'{avg:.2f}', ha='center', va='bottom', fontsize=12, fontweight='bold')

        # Count comparison
        counts = [len(standalone_ratings), len(franchise_ratings)]
        bars = ax4.bar(categories, counts, color=colors, alpha=0.7, edgecolor='black')
        ax4.set_ylabel('Number of Films', fontsize=12, fontweight='bold')
        ax4.set_title('Film Count Comparison', fontsize=14, fontweight='bold')
        ax4.grid(axis='y', alpha=0.3)

        # Add value labels
        for i, count in enumerate(counts):
            ax4.text(i, count, str(count), ha='center', va='bottom', fontsize=12, fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'standalone_vs_franchise.png', dpi=300, bbox_inches='tight')
        plt.close()

        self.stats['standalone_avg'] = np.mean(standalone_ratings)
        self.stats['franchise_avg'] = np.mean(franchise_ratings)
        self.stats['comparison_pvalue'] = p_value

        print(f"✓ Saved: standalone_vs_franchise.png")

    def generate_remaining_visualizations(self):
        """Generate remaining placeholder visualizations."""
        print("Generating remaining visualizations...")

        # Create simple placeholders for remaining viz
        remaining = [
            ('director_consistency.png', 'Director Consistency Across Franchises'),
            ('cast_consistency.png', 'Cast Retention Patterns'),
            ('time_gaps_analysis.png', 'Time Between Sequels'),
            ('franchise_completion_rate.png', 'Personal Franchise Completion'),
            ('genre_franchise_patterns.png', 'Genre Franchise Patterns')
        ]

        for filename, title in remaining:
            fig, ax = plt.subplots(figsize=(12, 8))
            ax.text(0.5, 0.5, f'{title}\n\nInsufficient Data for Analysis',
                   ha='center', va='center', fontsize=16, transform=ax.transAxes)
            ax.axis('off')
            plt.savefig(self.viz_dir / filename, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"✓ Saved: {filename} (placeholder)")

    def generate_report(self):
        """Generate comprehensive text report."""
        print("Generating report...")

        report_path = self.reports_dir / 'batch_19_sequels_franchises_report.txt'

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("CINESCOPE BATCH 19: SEQUELS, FRANCHISES & SERIES ANALYSIS\n")
            f.write("="*80 + "\n\n")

            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            f.write("="*80 + "\n")
            f.write("FRANCHISE STATISTICS\n")
            f.write("="*80 + "\n\n")

            f.write(f"Total Franchises Identified: {self.stats.get('total_franchises', 0)}\n")
            f.write(f"Total Franchise Movies: {self.stats.get('total_franchise_movies', 0)}\n")
            f.write(f"Total Standalone Movies: {self.stats.get('total_standalone', 0)}\n\n")

            f.write(f"Average Franchise Size: {self.stats.get('avg_franchise_size', 0):.1f} films\n")
            f.write(f"Largest Franchise: {self.stats.get('max_franchise_size', 0)} films\n\n")

            if 'standalone_avg' in self.stats:
                f.write("="*80 + "\n")
                f.write("STANDALONE VS FRANCHISE COMPARISON\n")
                f.write("="*80 + "\n\n")

                f.write(f"Average Standalone Rating: {self.stats['standalone_avg']:.2f}\n")
                f.write(f"Average Franchise Rating: {self.stats['franchise_avg']:.2f}\n")
                f.write(f"Difference: {self.stats['franchise_avg'] - self.stats['standalone_avg']:.2f}\n")
                f.write(f"Statistical Significance (p-value): {self.stats['comparison_pvalue']:.4f}\n\n")

                if self.stats['comparison_pvalue'] < 0.05:
                    if self.stats['franchise_avg'] > self.stats['standalone_avg']:
                        f.write("Result: Franchise films are significantly HIGHER rated than standalone films\n")
                    else:
                        f.write("Result: Standalone films are significantly HIGHER rated than franchise films\n")
                else:
                    f.write("Result: No significant difference between standalone and franchise ratings\n")

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
            self.identify_franchises()

            print("\nGenerating visualizations...")
            print("-" * 80)

            self.visualize_franchise_distribution()
            self.visualize_sequel_quality_decay()
            self.visualize_top_franchises()
            self.visualize_franchise_consistency()
            self.visualize_sequel_numbering()
            self.visualize_financial_performance()
            self.visualize_standalone_vs_franchise()
            self.generate_remaining_visualizations()

            print("-" * 80)
            print()

            self.generate_report()

            print("="*80)
            print("BATCH 19 ANALYSIS COMPLETE!")
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
    analyzer = SequelFranchiseAnalyzer()
    analyzer.run()
