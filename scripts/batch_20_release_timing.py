#!/usr/bin/env python3
"""
================================================================================
CINESCOPE BATCH 20: RELEASE TIMING & SEASONALITY ANALYSIS
================================================================================

Focus: When movies are released and how timing affects success

Data Sources:
- Release dates from TMDB and OMDb
- Box office revenue
- Ratings data

Visualizations (12):
1. monthly_release_distribution.png - Release patterns by month
2. seasonal_performance.png - Performance by season
3. day_of_week_releases.png - Preferred release days
4. holiday_releases.png - Holiday weekend performance
5. summer_vs_winter.png - Blockbuster seasons
6. monthly_revenue_patterns.png - Revenue by release month
7. rating_by_release_month.png - Quality by release timing
8. decade_release_patterns.png - How timing evolved
9. genre_release_timing.png - Genre-specific patterns
10. awards_season_analysis.png - Q4 release advantage
11. release_density_calendar.png - Heatmap calendar view
12. optimal_release_windows.png - Best times to release

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
import calendar

class ReleaseTimingAnalyzer:
    """Analyzes release timing and seasonality patterns."""

    def __init__(self):
        """Initialize the analyzer."""
        self.base_dir = Path(__file__).parent.parent
        self.data_dir = self.base_dir / 'data'
        self.output_dir = self.base_dir / 'analysis_outputs'
        self.viz_dir = self.output_dir / 'visualizations' / 'batch_20'
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
        self.stats = {}

        print("="*80)
        print("CINESCOPE BATCH 20: RELEASE TIMING & SEASONALITY ANALYSIS")
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

    def process_release_dates(self):
        """Process and clean release date data."""
        print("Processing release date data...")

        # Try multiple date columns
        self.movies_df['parsed_date'] = None

        for idx, row in self.movies_df.iterrows():
            date_val = None

            # Try TMDB release date first
            tmdb_date = row.get('tmdb_release_date', '')
            if tmdb_date and not pd.isna(tmdb_date) and str(tmdb_date) != 'nan':
                try:
                    date_val = pd.to_datetime(tmdb_date)
                except:
                    pass

            # Try OMDb released date
            if not date_val:
                omdb_date = row.get('omdb_released', '')
                if omdb_date and not pd.isna(omdb_date) and str(omdb_date) != 'nan':
                    try:
                        date_val = pd.to_datetime(omdb_date)
                    except:
                        pass

            # Try Release Date column
            if not date_val:
                release_date = row.get('Release Date', '')
                if release_date and not pd.isna(release_date) and str(release_date) != 'nan':
                    try:
                        date_val = pd.to_datetime(release_date)
                    except:
                        pass

            self.movies_df.at[idx, 'parsed_date'] = date_val

        # Convert to datetime and filter to valid dates
        self.movies_df['parsed_date'] = pd.to_datetime(self.movies_df['parsed_date'], errors='coerce')
        self.df = self.movies_df[self.movies_df['parsed_date'].notna()].copy()

        # Extract components
        self.df['month'] = self.df['parsed_date'].dt.month
        self.df['day_of_week'] = self.df['parsed_date'].dt.day_name()
        self.df['season'] = self.df['month'].apply(self.get_season)
        self.df['quarter'] = self.df['parsed_date'].dt.quarter

        self.stats['total_with_dates'] = len(self.df)
        self.stats['coverage'] = len(self.df) / len(self.movies_df) * 100

        print(f"✓ Processed {len(self.df):,} films with valid release dates ({self.stats['coverage']:.1f}% coverage)")
        print()

    def get_season(self, month):
        """Map month to season."""
        if month in [12, 1, 2]:
            return 'Winter'
        elif month in [3, 4, 5]:
            return 'Spring'
        elif month in [6, 7, 8]:
            return 'Summer'
        else:
            return 'Fall'

    def visualize_monthly_distribution(self):
        """Monthly release distribution."""
        print("Creating monthly distribution visualization...")

        month_counts = self.df['month'].value_counts().sort_index()

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Bar chart
        months = [calendar.month_abbr[m] for m in month_counts.index]
        colors = plt.cm.viridis(np.linspace(0.2, 0.9, 12))

        ax1.bar(range(12), month_counts.values, color=colors, alpha=0.7, edgecolor='black')
        ax1.set_xticks(range(12))
        ax1.set_xticklabels(months)
        ax1.set_xlabel('Month', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Number of Releases', fontsize=12, fontweight='bold')
        ax1.set_title('Release Distribution by Month', fontsize=14, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)

        # Add value labels
        for i, count in enumerate(month_counts.values):
            ax1.text(i, count, str(count), ha='center', va='bottom', fontsize=9)

        # Seasonal pie chart
        season_counts = self.df['season'].value_counts()
        season_colors = {'Winter': '#3498db', 'Spring': '#2ecc71', 'Summer': '#f39c12', 'Fall': '#e74c3c'}
        colors_ordered = [season_colors[s] for s in season_counts.index]

        wedges, texts, autotexts = ax2.pie(season_counts.values, labels=season_counts.index,
                                           autopct='%1.1f%%', colors=colors_ordered, startangle=90)

        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(12)

        ax2.set_title('Release Distribution by Season', fontsize=14, fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'monthly_release_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: monthly_release_distribution.png")

    def visualize_seasonal_performance(self):
        """Performance by season."""
        print("Creating seasonal performance visualization...")

        # Get revenue and rating data
        df_with_revenue = self.df[self.df['tmdb_revenue'].notna() & (self.df['tmdb_revenue'] > 0)].copy()

        if len(df_with_revenue) == 0:
            print("! No revenue data")
            return

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Revenue by season
        season_revenue = df_with_revenue.groupby('season')['tmdb_revenue'].mean() / 1e6
        season_order = ['Winter', 'Spring', 'Summer', 'Fall']
        season_revenue = season_revenue.reindex(season_order)

        colors = ['#3498db', '#2ecc71', '#f39c12', '#e74c3c']
        ax1.bar(range(4), season_revenue.values, color=colors, alpha=0.7, edgecolor='black')
        ax1.set_xticks(range(4))
        ax1.set_xticklabels(season_order)
        ax1.set_xlabel('Season', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Average Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax1.set_title('Average Revenue by Season', fontsize=14, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)

        # Add labels
        for i, val in enumerate(season_revenue.values):
            ax1.text(i, val, f'${val:.1f}M', ha='center', va='bottom', fontsize=10, fontweight='bold')

        # Count by season
        season_counts = self.df['season'].value_counts().reindex(season_order)
        ax2.bar(range(4), season_counts.values, color=colors, alpha=0.7, edgecolor='black')
        ax2.set_xticks(range(4))
        ax2.set_xticklabels(season_order)
        ax2.set_xlabel('Season', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Number of Releases', fontsize=12, fontweight='bold')
        ax2.set_title('Release Count by Season', fontsize=14, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        # Add labels
        for i, count in enumerate(season_counts.values):
            ax2.text(i, count, str(count), ha='center', va='bottom', fontsize=10, fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'seasonal_performance.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: seasonal_performance.png")

    def visualize_day_of_week(self):
        """Day of week release patterns."""
        print("Creating day of week visualization...")

        dow_counts = self.df['day_of_week'].value_counts()
        dow_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        dow_counts = dow_counts.reindex(dow_order, fill_value=0)

        fig, ax = plt.subplots(figsize=(12, 6))

        colors = plt.cm.coolwarm(np.linspace(0.2, 0.8, 7))
        bars = ax.bar(range(7), dow_counts.values, color=colors, alpha=0.7, edgecolor='black')

        ax.set_xticks(range(7))
        ax.set_xticklabels(dow_order, rotation=15, ha='right')
        ax.set_xlabel('Day of Week', fontsize=12, fontweight='bold')
        ax.set_ylabel('Number of Releases', fontsize=12, fontweight='bold')
        ax.set_title('Release Distribution by Day of Week', fontsize=14, fontweight='bold')
        ax.grid(axis='y', alpha=0.3)

        # Add labels
        for i, count in enumerate(dow_counts.values):
            if count > 0:
                ax.text(i, count, str(count), ha='center', va='bottom', fontsize=10)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'day_of_week_releases.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: day_of_week_releases.png")

    def visualize_monthly_revenue(self):
        """Revenue patterns by month."""
        print("Creating monthly revenue visualization...")

        df_revenue = self.df[self.df['tmdb_revenue'].notna() & (self.df['tmdb_revenue'] > 0)].copy()

        if len(df_revenue) == 0:
            print("! No revenue data")
            return

        month_revenue = df_revenue.groupby('month')['tmdb_revenue'].agg(['mean', 'sum', 'count'])

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))

        # Average revenue
        months = [calendar.month_abbr[m] for m in month_revenue.index]
        ax1.bar(range(12), month_revenue['mean'] / 1e6, color='steelblue', alpha=0.7, edgecolor='black')
        ax1.set_xticks(range(12))
        ax1.set_xticklabels(months)
        ax1.set_xlabel('Month', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Average Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax1.set_title('Average Revenue by Release Month', fontsize=14, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)

        # Total revenue
        ax2.bar(range(12), month_revenue['sum'] / 1e6, color='darkgreen', alpha=0.7, edgecolor='black')
        ax2.set_xticks(range(12))
        ax2.set_xticklabels(months)
        ax2.set_xlabel('Month', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Total Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax2.set_title('Total Revenue by Release Month', fontsize=14, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'monthly_revenue_patterns.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: monthly_revenue_patterns.png")

    def visualize_rating_by_month(self):
        """Analyze film quality by release month."""
        print("Creating rating by release month visualization...")

        monthly_ratings = self.df.groupby('month').agg({
            'IMDb Rating': ['mean', 'median', 'count']
        }).round(2)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Average and median by month
        months = range(1, 13)
        month_names = [calendar.month_abbr[i] for i in months]

        ax1.plot(months, monthly_ratings[('IMDb Rating', 'mean')], marker='o',
                linewidth=2, markersize=8, color='steelblue', label='Mean Rating')
        ax1.plot(months, monthly_ratings[('IMDb Rating', 'median')], marker='s',
                linewidth=2, markersize=8, color='coral', label='Median Rating', linestyle='--')
        ax1.set_xlabel('Release Month', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Rating', fontsize=12, fontweight='bold')
        ax1.set_title('Film Quality by Release Month', fontsize=14, fontweight='bold')
        ax1.set_xticks(months)
        ax1.set_xticklabels(month_names)
        ax1.grid(alpha=0.3)
        ax1.legend()

        # Box plot
        self.df.boxplot(column='IMDb Rating', by='month', ax=ax2)
        ax2.set_xlabel('Release Month', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Rating', fontsize=12, fontweight='bold')
        ax2.set_title('Rating Distribution by Month', fontsize=14, fontweight='bold')
        ax2.set_xticklabels(month_names)
        plt.suptitle('')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'rating_by_release_month.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: rating_by_release_month.png")

    def visualize_decade_patterns(self):
        """Analyze how release patterns evolved over time."""
        print("Creating decade release patterns visualization...")

        self.df['decade'] = (self.df['parsed_date'].dt.year // 10) * 10

        decade_month = self.df.groupby(['decade', 'month']).size().unstack(fill_value=0)

        # Normalize by decade totals to show percentage
        decade_month_pct = decade_month.div(decade_month.sum(axis=1), axis=0) * 100

        fig, ax = plt.subplots(figsize=(14, 8))

        im = ax.imshow(decade_month_pct.values, cmap='YlOrRd', aspect='auto')

        ax.set_xticks(range(12))
        ax.set_xticklabels([calendar.month_abbr[i] for i in range(1, 13)])
        ax.set_yticks(range(len(decade_month_pct)))
        ax.set_yticklabels([f"{int(d)}s" for d in decade_month_pct.index])
        ax.set_xlabel('Release Month', fontsize=12, fontweight='bold')
        ax.set_ylabel('Decade', fontsize=12, fontweight='bold')
        ax.set_title('Release Pattern Evolution by Decade', fontsize=14, fontweight='bold')

        cbar = plt.colorbar(im, ax=ax)
        cbar.set_label('Percentage of Releases', fontsize=10)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'decade_release_patterns.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: decade_release_patterns.png")

    def visualize_genre_timing(self):
        """Analyze genre-specific release patterns."""
        print("Creating genre release timing visualization...")

        # Parse genres
        genre_month_data = defaultdict(lambda: defaultdict(int))

        for idx, row in self.df.iterrows():
            month = row['month']
            genres_str = row.get('tmdb_genres', row.get('Genres', ''))

            if pd.notna(genres_str) and genres_str:
                genres = []
                if '|' in str(genres_str):
                    genres = [g.strip() for g in str(genres_str).split('|')]
                elif ',' in str(genres_str):
                    genres = [g.strip() for g in str(genres_str).split(',')]
                else:
                    genres = [str(genres_str).strip()]

                for genre in genres:
                    if genre:
                        genre_month_data[genre][month] += 1

        # Get top genres
        top_genres = sorted(genre_month_data.keys(),
                           key=lambda x: sum(genre_month_data[x].values()),
                           reverse=True)[:8]

        fig, axes = plt.subplots(2, 4, figsize=(18, 10))
        axes = axes.flatten()

        for idx, genre in enumerate(top_genres):
            months = range(1, 13)
            counts = [genre_month_data[genre][m] for m in months]
            month_names = [calendar.month_abbr[i] for i in months]

            axes[idx].bar(months, counts, color='steelblue', alpha=0.7, edgecolor='black')
            axes[idx].set_title(genre, fontsize=12, fontweight='bold')
            axes[idx].set_xticks(months)
            axes[idx].set_xticklabels(month_names, rotation=45, fontsize=8)
            axes[idx].grid(axis='y', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'genre_release_timing.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: genre_release_timing.png")

    def visualize_awards_season(self):
        """Analyze Q4 release advantage for awards."""
        print("Creating awards season visualization...")

        self.df['quarter'] = self.df['parsed_date'].dt.quarter

        quarterly_stats = self.df.groupby('quarter').agg({
            'IMDb Rating': ['mean', 'count']
        }).round(2)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Releases by quarter
        quarters = ['Q1\n(Jan-Mar)', 'Q2\n(Apr-Jun)', 'Q3\n(Jul-Sep)', 'Q4\n(Oct-Dec)']
        counts = quarterly_stats[('IMDb Rating', 'count')].values
        colors = ['#3498db', '#2ecc71', '#f39c12', '#e74c3c']

        ax1.bar(range(1, 5), counts, color=colors, alpha=0.7, edgecolor='black')
        ax1.set_xticks(range(1, 5))
        ax1.set_xticklabels(quarters)
        ax1.set_ylabel('Number of Releases', fontsize=12, fontweight='bold')
        ax1.set_title('Movie Releases by Quarter', fontsize=14, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)

        for i, count in enumerate(counts):
            ax1.text(i+1, count, str(int(count)), ha='center', va='bottom', fontweight='bold')

        # Average rating by quarter
        ratings = quarterly_stats[('IMDb Rating', 'mean')].values

        ax2.bar(range(1, 5), ratings, color=colors, alpha=0.7, edgecolor='black')
        ax2.set_xticks(range(1, 5))
        ax2.set_xticklabels(quarters)
        ax2.set_ylabel('Average Rating', fontsize=12, fontweight='bold')
        ax2.set_title('Film Quality by Quarter', fontsize=14, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)
        ax2.axhline(self.df['IMDb Rating'].mean(), color='red', linestyle='--',
                   linewidth=2, label='Overall Average')
        ax2.legend()

        for i, rating in enumerate(ratings):
            ax2.text(i+1, rating, f'{rating:.2f}', ha='center', va='bottom', fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'awards_season_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: awards_season_analysis.png")

    def visualize_summer_winter(self):
        """Compare summer vs winter blockbuster seasons."""
        print("Creating summer vs winter comparison...")

        # Define seasons
        self.df['is_summer'] = self.df['month'].isin([5, 6, 7, 8])  # May-Aug
        self.df['is_winter'] = self.df['month'].isin([11, 12])  # Nov-Dec

        summer = self.df[self.df['is_summer']]
        winter = self.df[self.df['is_winter']]

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

        # Release counts
        seasons = ['Summer\n(May-Aug)', 'Winter\n(Nov-Dec)', 'Other']
        counts = [len(summer), len(winter), len(self.df) - len(summer) - len(winter)]
        colors = ['#ff9800', '#2196f3', '#9e9e9e']

        ax1.bar(seasons, counts, color=colors, alpha=0.7, edgecolor='black')
        ax1.set_ylabel('Number of Releases', fontsize=12, fontweight='bold')
        ax1.set_title('Release Distribution by Season', fontsize=14, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)

        for i, count in enumerate(counts):
            ax1.text(i, count, str(count), ha='center', va='bottom', fontweight='bold')

        # Average ratings
        ratings = [summer['IMDb Rating'].mean(), winter['IMDb Rating'].mean(),
                  self.df[~(self.df['is_summer'] | self.df['is_winter'])]['IMDb Rating'].mean()]

        ax2.bar(seasons, ratings, color=colors, alpha=0.7, edgecolor='black')
        ax2.set_ylabel('Average Rating', fontsize=12, fontweight='bold')
        ax2.set_title('Average Film Quality by Season', fontsize=14, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        for i, rating in enumerate(ratings):
            ax2.text(i, rating, f'{rating:.2f}', ha='center', va='bottom', fontweight='bold')

        # Revenue comparison (if available)
        summer_revenue = summer[summer['tmdb_revenue'] > 0]
        winter_revenue = winter[winter['tmdb_revenue'] > 0]

        if len(summer_revenue) > 0 and len(winter_revenue) > 0:
            revenue_data = [summer_revenue['tmdb_revenue'].values / 1e6,
                           winter_revenue['tmdb_revenue'].values / 1e6]

            ax3.boxplot(revenue_data, labels=['Summer', 'Winter'])
            ax3.set_ylabel('Revenue (Millions $)', fontsize=12, fontweight='bold')
            ax3.set_title('Box Office Revenue Distribution', fontsize=14, fontweight='bold')
            ax3.grid(axis='y', alpha=0.3)
        else:
            ax3.text(0.5, 0.5, 'Insufficient Revenue Data', ha='center', va='center',
                    transform=ax3.transAxes, fontsize=14)
            ax3.axis('off')

        # Rating distribution comparison
        ax4.hist([summer['IMDb Rating'], winter['IMDb Rating']],
                label=['Summer', 'Winter'], bins=10, alpha=0.6, edgecolor='black')
        ax4.set_xlabel('Rating', fontsize=12, fontweight='bold')
        ax4.set_ylabel('Frequency', fontsize=12, fontweight='bold')
        ax4.set_title('Rating Distribution Comparison', fontsize=14, fontweight='bold')
        ax4.legend()
        ax4.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'summer_vs_winter.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: summer_vs_winter.png")

    def visualize_holiday_releases(self):
        """Analyze holiday weekend performance."""
        print("Creating holiday releases visualization...")

        # Define holiday months
        holiday_months = {
            12: 'December Holidays',
            7: 'Summer Peak',
            11: 'Thanksgiving',
            5: 'Memorial Day',
            10: 'Halloween'
        }

        holiday_data = []
        for month, label in holiday_months.items():
            month_data = self.df[self.df['month'] == month]
            holiday_data.append({
                'period': label,
                'count': len(month_data),
                'avg_rating': month_data['IMDb Rating'].mean(),
                'median_rating': month_data['IMDb Rating'].median()
            })

        df = pd.DataFrame(holiday_data).sort_values('count', ascending=False)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Release counts
        colors = plt.cm.Spectral(np.linspace(0.2, 0.8, len(df)))

        ax1.barh(range(len(df)), df['count'], color=colors, alpha=0.7, edgecolor='black')
        ax1.set_yticks(range(len(df)))
        ax1.set_yticklabels(df['period'])
        ax1.invert_yaxis()
        ax1.set_xlabel('Number of Releases', fontsize=12, fontweight='bold')
        ax1.set_title('Releases During Key Periods', fontsize=14, fontweight='bold')
        ax1.grid(axis='x', alpha=0.3)

        for i, count in enumerate(df['count']):
            ax1.text(count, i, f' {count}', va='center', fontsize=10, fontweight='bold')

        # Average ratings
        ax2.barh(range(len(df)), df['avg_rating'], color=colors, alpha=0.7, edgecolor='black')
        ax2.set_yticks(range(len(df)))
        ax2.set_yticklabels(df['period'])
        ax2.invert_yaxis()
        ax2.set_xlabel('Average Rating', fontsize=12, fontweight='bold')
        ax2.set_title('Film Quality During Key Periods', fontsize=14, fontweight='bold')
        ax2.grid(axis='x', alpha=0.3)
        ax2.axvline(self.df['IMDb Rating'].mean(), color='red', linestyle='--',
                   linewidth=2, label='Overall Average')
        ax2.legend()

        for i, rating in enumerate(df['avg_rating']):
            ax2.text(rating, i, f' {rating:.2f}', va='center', fontsize=10, fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'holiday_releases.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: holiday_releases.png")

    def visualize_release_calendar(self):
        """Create a calendar heatmap of releases."""
        print("Creating release density calendar...")

        # Count releases by month and decade
        self.df['decade'] = (self.df['parsed_date'].dt.year // 10) * 10
        month_counts = self.df.groupby('month').size()

        fig, ax = plt.subplots(figsize=(14, 6))

        # Create calendar-like view
        months = range(1, 13)
        month_names = [calendar.month_name[i] for i in months]
        counts = [month_counts.get(i, 0) for i in months]

        colors = plt.cm.Blues(np.array(counts) / max(counts))

        bars = ax.bar(months, counts, color=colors, edgecolor='black', alpha=0.8)
        ax.set_xticks(months)
        ax.set_xticklabels(month_names, rotation=45, ha='right')
        ax.set_ylabel('Number of Releases', fontsize=12, fontweight='bold')
        ax.set_title('Release Density Calendar', fontsize=14, fontweight='bold')
        ax.grid(axis='y', alpha=0.3)

        for i, (month, count) in enumerate(zip(months, counts)):
            ax.text(month, count, str(count), ha='center', va='bottom', fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'release_density_calendar.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: release_density_calendar.png")

    def visualize_optimal_windows(self):
        """Identify optimal release windows."""
        print("Creating optimal release windows visualization...")

        # Calculate composite score: rating * log(count + 1)
        month_stats = self.df.groupby('month').agg({
            'IMDb Rating': ['mean', 'count']
        })

        month_stats['score'] = (month_stats[('IMDb Rating', 'mean')] *
                               np.log(month_stats[('IMDb Rating', 'count')] + 1))

        fig, ax = plt.subplots(figsize=(14, 8))

        months = list(range(1, 13))
        month_names = [calendar.month_name[i] for i in months]

        # Extract scores as floats
        scores = []
        ratings = []
        for i in months:
            if i in month_stats.index:
                # Get value and convert to scalar
                score_val = month_stats.loc[i, 'score']
                rating_val = month_stats.loc[i, ('IMDb Rating', 'mean')]

                # Handle both scalar and Series types
                if isinstance(score_val, pd.Series):
                    score_val = score_val.iloc[0] if len(score_val) > 0 else 0
                if isinstance(rating_val, pd.Series):
                    rating_val = rating_val.iloc[0] if len(rating_val) > 0 else 0

                scores.append(float(score_val) if pd.notna(score_val) else 0)
                ratings.append(float(rating_val) if pd.notna(rating_val) else 0)
            else:
                scores.append(0)
                ratings.append(0)

        # Color by score
        scores_array = np.array(scores)
        max_score = scores_array.max() if scores_array.max() > 0 else 1
        colors = plt.cm.RdYlGn(scores_array / max_score)

        bars = ax.bar(months, scores_array, color=colors, alpha=0.7, edgecolor='black')
        ax.set_xticks(months)
        ax.set_xticklabels(month_names, rotation=45, ha='right')
        ax.set_ylabel('Composite Score (Quality × Activity)', fontsize=12, fontweight='bold')
        ax.set_title('Optimal Release Windows\n(Higher score = Better quality with sufficient volume)',
                    fontsize=14, fontweight='bold')
        ax.grid(axis='y', alpha=0.3)

        # Add rating annotations
        for i, (month, score, rating) in enumerate(zip(months, scores, ratings)):
            if score > 0:
                ax.text(month, score, f'{rating:.1f}', ha='center', va='bottom',
                       fontsize=9, fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'optimal_release_windows.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: optimal_release_windows.png")

    def generate_remaining_visualizations(self):
        """Generate all remaining visualizations with real data."""
        print("Generating remaining visualizations...")

        self.visualize_rating_by_month()
        self.visualize_decade_patterns()
        self.visualize_genre_timing()
        self.visualize_awards_season()
        self.visualize_summer_winter()
        self.visualize_holiday_releases()
        self.visualize_release_calendar()
        self.visualize_optimal_windows()

    def generate_report(self):
        """Generate comprehensive text report."""
        print("Generating report...")

        report_path = self.reports_dir / 'batch_20_release_timing_report.txt'

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("CINESCOPE BATCH 20: RELEASE TIMING & SEASONALITY ANALYSIS\n")
            f.write("="*80 + "\n\n")

            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            f.write(f"Total Films Analyzed: {self.stats.get('total_with_dates', 0):,}\n")
            f.write(f"Coverage: {self.stats.get('coverage', 0):.1f}%\n\n")

            f.write("="*80 + "\n")
            f.write("RELEASE PATTERNS\n")
            f.write("="*80 + "\n\n")

            # Most common month
            if len(self.df) > 0:
                month_counts = self.df['month'].value_counts()
                top_month = calendar.month_name[month_counts.index[0]]
                f.write(f"Most Common Release Month: {top_month} ({month_counts.values[0]} releases)\n")

                # Most common season
                season_counts = self.df['season'].value_counts()
                f.write(f"Most Common Season: {season_counts.index[0]} ({season_counts.values[0]} releases)\n\n")

            f.write("="*80 + "\n")
            f.write("END OF REPORT\n")
            f.write("="*80 + "\n")

        print(f"✓ Report saved: {report_path}")
        print()

    def run(self):
        """Run the complete analysis pipeline."""
        try:
            self.load_data()
            self.process_release_dates()

            print("\nGenerating visualizations...")
            print("-" * 80)

            self.visualize_monthly_distribution()
            self.visualize_seasonal_performance()
            self.visualize_day_of_week()
            self.visualize_monthly_revenue()
            self.generate_remaining_visualizations()

            print("-" * 80)
            print()

            self.generate_report()

            print("="*80)
            print("BATCH 20 ANALYSIS COMPLETE!")
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
    analyzer = ReleaseTimingAnalyzer()
    analyzer.run()
