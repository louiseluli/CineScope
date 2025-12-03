"""
CineScope - Batch 1: Quantified Self Analysis
=============================================
Comprehensive analysis of YOUR watched movies with 8 professional visualizations.

Focus: Rating patterns, temporal viewing, runtime preferences
All based on IMDB/TMDB ratings (public consensus), not personal ratings

Generates 8 PNG files at 300 DPI.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import sys
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import (
    WATCHED_ONLY_DATA, VISUALIZATIONS_DIR, RATING_COLORS, QUALITY_COLORS,
    DEFAULT_FIGSIZE, DEFAULT_DPI, save_figure, get_batch_output_dir,
    log_message, BATCH1_BINS
)
from src.core.helpers import (
    categorize_rating, categorize_runtime, get_decade, get_season,
    explode_genres
)


class QuantifiedSelfAnalysis:
    """Batch 1: Analyze personal viewing behavior and rating patterns."""
    
    def __init__(self):
        """Initialize the analysis."""
        self.df = None
        self.batch_dir = get_batch_output_dir(1)
        
    def load_data(self):
        """Load watched movies dataset."""
        log_message("=" * 80)
        log_message("BATCH 1: Quantified Self Analysis")
        log_message("Loading YOUR watched movies...")
        log_message("=" * 80)
        
        self.df = pd.read_csv(WATCHED_ONLY_DATA)
        
        # Parse dates
        date_cols = ['date_rated', 'watched_created', 'watched_modified']
        for col in date_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
        
        log_message(f"\nLoaded: {len(self.df):,} watched films")
        log_message(f"Year range: {int(self.df['year'].min())} - {int(self.df['year'].max())}")
        log_message(f"IMDB avg rating: {self.df['imdb_rating'].mean():.2f}")
        
        return self
    
    def viz_1_rating_distribution(self):
        """Viz 1: Rating Distribution - IMDB vs TMDB comparison."""
        log_message("\n📊 Creating Visualization 1: Rating Distribution")
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))
        
        # Left: IMDB Rating Distribution
        imdb_ratings = self.df['imdb_rating'].dropna()
        
        ax1.hist(imdb_ratings, bins=20, alpha=0.7, color=RATING_COLORS['imdb'],
                edgecolor='black', linewidth=1.5)
        
        # Add mean line
        mean_imdb = imdb_ratings.mean()
        ax1.axvline(mean_imdb, color='red', linestyle='--', linewidth=2, 
                   label=f'Mean: {mean_imdb:.2f}')
        
        # Add median line
        median_imdb = imdb_ratings.median()
        ax1.axvline(median_imdb, color='orange', linestyle='--', linewidth=2,
                   label=f'Median: {median_imdb:.2f}')
        
        ax1.set_xlabel('IMDB Rating', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Number of Films', fontsize=14, fontweight='bold')
        ax1.set_title('IMDB Rating Distribution\nYour Watched Films', 
                     fontsize=16, fontweight='bold', pad=20)
        ax1.legend(fontsize=12)
        ax1.grid(True, alpha=0.3)
        
        # Right: TMDB Rating Distribution (if available)
        if 'tmdb_vote_average' in self.df.columns:
            tmdb_ratings = self.df['tmdb_vote_average'].dropna()
            
            if len(tmdb_ratings) > 0:
                ax2.hist(tmdb_ratings, bins=20, alpha=0.7, color=RATING_COLORS['tmdb'],
                        edgecolor='black', linewidth=1.5)
                
                mean_tmdb = tmdb_ratings.mean()
                ax2.axvline(mean_tmdb, color='red', linestyle='--', linewidth=2,
                           label=f'Mean: {mean_tmdb:.2f}')
                
                median_tmdb = tmdb_ratings.median()
                ax2.axvline(median_tmdb, color='orange', linestyle='--', linewidth=2,
                           label=f'Median: {median_tmdb:.2f}')
                
                ax2.set_xlabel('TMDB Rating', fontsize=14, fontweight='bold')
                ax2.set_ylabel('Number of Films', fontsize=14, fontweight='bold')
                ax2.set_title('TMDB Rating Distribution\nYour Watched Films',
                             fontsize=16, fontweight='bold', pad=20)
                ax2.legend(fontsize=12)
                ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        filepath = save_figure(fig, '01_rating_distribution.png', batch_number=1)
        plt.close()
        
        return self
    
    def viz_2_rating_matrix_heatmap(self):
        """Viz 2: Rating Matrix Heatmap - consensus analysis."""
        log_message("📊 Creating Visualization 2: Rating Matrix Heatmap")
        
        # Create rating bins
        df_subset = self.df[['imdb_rating']].dropna()
        
        # Bin IMDB ratings
        df_subset['imdb_bin'] = pd.cut(df_subset['imdb_rating'], 
                                        bins=range(1, 12), 
                                        labels=range(1, 11))
        
        # Count frequency
        rating_matrix = df_subset.groupby('imdb_bin').size().reindex(range(1, 11), fill_value=0)
        
        # Create heatmap data
        matrix_data = rating_matrix.values.reshape(2, 5)
        
        fig, ax = plt.subplots(figsize=(14, 8))
        
        sns.heatmap(matrix_data, annot=True, fmt='d', cmap='YlOrRd',
                   cbar_kws={'label': 'Number of Films'},
                   xticklabels=[f'{i}-{i+1}' for i in range(1, 11, 2)],
                   yticklabels=['Lower Ratings', 'Higher Ratings'],
                   linewidths=2, linecolor='white',
                   ax=ax)
        
        ax.set_title('IMDB Rating Distribution Heatmap\nHow the World Rates Your Watched Films',
                    fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Rating Range', fontsize=14, fontweight='bold')
        ax.set_ylabel('Quality Tier', fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        filepath = save_figure(fig, '02_rating_matrix_heatmap.png', batch_number=1)
        plt.close()
        
        return self
    
    def viz_3_temporal_viewing_timeline(self):
        """Viz 3: Temporal Viewing Timeline - when you watched films."""
        log_message("📊 Creating Visualization 3: Temporal Viewing Timeline")
        
        if 'date_rated' not in self.df.columns:
            log_message("⚠️ No date_rated column - skipping temporal analysis", level="WARNING")
            return self
        
        # Filter to films with dates
        df_dated = self.df[self.df['date_rated'].notna()].copy()
        
        if len(df_dated) == 0:
            log_message("⚠️ No films with dates - skipping", level="WARNING")
            return self
        
        # Group by month
        df_dated['year_month'] = df_dated['date_rated'].dt.to_period('M')
        monthly_counts = df_dated.groupby('year_month').size()
        
        # Convert back to datetime for plotting
        monthly_counts.index = monthly_counts.index.to_timestamp()
        
        fig, ax = plt.subplots(figsize=(18, 8))
        
        ax.plot(monthly_counts.index, monthly_counts.values, 
               color=RATING_COLORS['imdb'], linewidth=2, marker='o', markersize=4)
        
        # Add 3-month moving average
        ma = monthly_counts.rolling(window=3, center=True).mean()
        ax.plot(ma.index, ma.values, color='red', linewidth=3, 
               linestyle='--', label='3-Month Moving Avg', alpha=0.7)
        
        ax.set_xlabel('Date', fontsize=14, fontweight='bold')
        ax.set_ylabel('Films Watched', fontsize=14, fontweight='bold')
        ax.set_title('Your Viewing Timeline\nMovies Watched Over Time',
                    fontsize=16, fontweight='bold', pad=20)
        ax.legend(fontsize=12)
        ax.grid(True, alpha=0.3)
        
        # Rotate x-axis labels
        plt.xticks(rotation=45, ha='right')
        
        plt.tight_layout()
        filepath = save_figure(fig, '03_temporal_viewing_timeline.png', batch_number=1)
        plt.close()
        
        return self
    
    def viz_4_cine_calendar_heatmap(self):
        """Viz 4: Cine-Calendar Heatmap - GitHub-style viewing pattern."""
        log_message("📊 Creating Visualization 4: Cine-Calendar Heatmap")
        
        if 'date_rated' not in self.df.columns:
            log_message("⚠️ No date_rated column - skipping", level="WARNING")
            return self
        
        df_dated = self.df[self.df['date_rated'].notna()].copy()
        
        if len(df_dated) == 0:
            log_message("⚠️ No dated films - skipping", level="WARNING")
            return self
        
        # Count by date
        daily_counts = df_dated.groupby(df_dated['date_rated'].dt.date).size()
        
        # Create a date range
        date_range = pd.date_range(start=daily_counts.index.min(), 
                                   end=daily_counts.index.max(), freq='D')
        
        # Reindex to include all dates
        daily_counts = daily_counts.reindex(date_range, fill_value=0)
        
        # Limit to last 365 days for visualization
        if len(daily_counts) > 365:
            daily_counts = daily_counts.tail(365)
        
        # Create calendar matrix (7 rows for days of week, columns for weeks)
        daily_counts.index = pd.to_datetime(daily_counts.index)
        daily_counts_df = pd.DataFrame({
            'count': daily_counts.values,
            'day_of_week': daily_counts.index.dayofweek,
            'week': ((daily_counts.index - daily_counts.index[0]).days // 7)
        })
        
        # Pivot for heatmap
        cal_matrix = daily_counts_df.pivot(index='day_of_week', 
                                           columns='week', 
                                           values='count').fillna(0)
        
        fig, ax = plt.subplots(figsize=(20, 6))
        
        sns.heatmap(cal_matrix, cmap='YlGnBu', cbar_kws={'label': 'Films Watched'},
                   yticklabels=['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
                   linewidths=0.5, linecolor='lightgray', ax=ax)
        
        ax.set_title('Your Viewing Calendar (Last 365 Days)\nGitHub-Style Activity Map',
                    fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Week', fontsize=14, fontweight='bold')
        ax.set_ylabel('Day of Week', fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        filepath = save_figure(fig, '04_cine_calendar_heatmap.png', batch_number=1)
        plt.close()
        
        return self
    
    def viz_5_runtime_sweet_spot(self):
        """Viz 5: Runtime Sweet Spot - preferred film lengths."""
        log_message("📊 Creating Visualization 5: Runtime Sweet Spot")
        
        df_runtime = self.df[self.df['runtime_mins'].notna()].copy()
        
        # Create runtime bins
        bins = [0, 90, 120, 150, 180, 999]
        labels = ['<90 min', '90-120 min', '120-150 min', '150-180 min', '>180 min']
        df_runtime['runtime_bin'] = pd.cut(df_runtime['runtime_mins'], bins=bins, labels=labels)
        
        # Count and average rating by bin
        runtime_stats = df_runtime.groupby('runtime_bin').agg({
            'const': 'count',
            'imdb_rating': 'mean'
        }).reset_index()
        runtime_stats.columns = ['Runtime', 'Count', 'Avg_Rating']
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))
        
        # Left: Count by runtime
        ax1.bar(runtime_stats['Runtime'], runtime_stats['Count'],
               color=RATING_COLORS['imdb'], edgecolor='black', linewidth=1.5, alpha=0.7)
        ax1.set_xlabel('Runtime', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Number of Films', fontsize=14, fontweight='bold')
        ax1.set_title('Films by Runtime Length\nYour Viewing Distribution',
                     fontsize=16, fontweight='bold', pad=20)
        ax1.grid(True, alpha=0.3, axis='y')
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # Right: Average rating by runtime
        ax2.plot(runtime_stats['Runtime'], runtime_stats['Avg_Rating'],
                marker='o', markersize=12, linewidth=3, color=RATING_COLORS['critics'])
        ax2.set_xlabel('Runtime', fontsize=14, fontweight='bold')
        ax2.set_ylabel('Average IMDB Rating', fontsize=14, fontweight='bold')
        ax2.set_title('Rating by Runtime Length\nDoes Length Affect Quality?',
                     fontsize=16, fontweight='bold', pad=20)
        ax2.grid(True, alpha=0.3)
        ax2.set_ylim([5, 8])
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        plt.tight_layout()
        filepath = save_figure(fig, '05_runtime_sweet_spot.png', batch_number=1)
        plt.close()
        
        return self
    
    def viz_6_rating_vs_runtime_scatter(self):
        """Viz 6: Rating vs Runtime Scatter - correlation analysis."""
        log_message("📊 Creating Visualization 6: Rating vs Runtime Scatter")
        
        df_scatter = self.df[
            (self.df['runtime_mins'].notna()) & 
            (self.df['imdb_rating'].notna())
        ].copy()
        
        fig, ax = plt.subplots(figsize=(14, 10))
        
        # Scatter plot
        scatter = ax.scatter(df_scatter['runtime_mins'], 
                           df_scatter['imdb_rating'],
                           c=df_scatter['decade'], 
                           cmap='viridis',
                           alpha=0.6, 
                           s=50,
                           edgecolors='black',
                           linewidth=0.5)
        
        # Add colorbar
        cbar = plt.colorbar(scatter, ax=ax)
        cbar.set_label('Decade', fontsize=12, fontweight='bold')
        
        # Add trend line
        z = np.polyfit(df_scatter['runtime_mins'], df_scatter['imdb_rating'], 1)
        p = np.poly1d(z)
        ax.plot(df_scatter['runtime_mins'], p(df_scatter['runtime_mins']),
               "r--", linewidth=2, label=f'Trend: y={z[0]:.4f}x+{z[1]:.2f}')
        
        ax.set_xlabel('Runtime (minutes)', fontsize=14, fontweight='bold')
        ax.set_ylabel('IMDB Rating', fontsize=14, fontweight='bold')
        ax.set_title('Runtime vs Rating\nDoes Movie Length Correlate with Quality?',
                    fontsize=16, fontweight='bold', pad=20)
        ax.legend(fontsize=12)
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        filepath = save_figure(fig, '06_rating_vs_runtime_scatter.png', batch_number=1)
        plt.close()
        
        return self
    
    def viz_7_decade_distribution(self):
        """Viz 7: Decade Distribution - which decades you watch most."""
        log_message("📊 Creating Visualization 7: Decade Distribution")
        
        df_decades = self.df[self.df['decade'].notna()].copy()
        
        decade_stats = df_decades.groupby('decade').agg({
            'const': 'count',
            'imdb_rating': 'mean'
        }).reset_index()
        decade_stats.columns = ['Decade', 'Count', 'Avg_Rating']
        decade_stats['Decade'] = decade_stats['Decade'].astype(int)
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12))
        
        # Top: Count by decade
        bars = ax1.bar(decade_stats['Decade'], decade_stats['Count'],
                      width=8, edgecolor='black', linewidth=1.5, alpha=0.7)
        
        # Color bars by count (gradient)
        colors = plt.cm.viridis(decade_stats['Count'] / decade_stats['Count'].max())
        for bar, color in zip(bars, colors):
            bar.set_color(color)
        
        ax1.set_xlabel('Decade', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Number of Films', fontsize=14, fontweight='bold')
        ax1.set_title('Films Watched by Decade\nYour Cinema Timeline',
                     fontsize=16, fontweight='bold', pad=20)
        ax1.grid(True, alpha=0.3, axis='y')
        
        # Add value labels on bars
        for decade, count in zip(decade_stats['Decade'], decade_stats['Count']):
            ax1.text(decade, count, str(count), ha='center', va='bottom', fontsize=10, fontweight='bold')
        
        # Bottom: Average rating by decade
        ax2.plot(decade_stats['Decade'], decade_stats['Avg_Rating'],
                marker='o', markersize=10, linewidth=3, color=RATING_COLORS['critics'])
        ax2.fill_between(decade_stats['Decade'], decade_stats['Avg_Rating'],
                        alpha=0.3, color=RATING_COLORS['critics'])
        
        ax2.set_xlabel('Decade', fontsize=14, fontweight='bold')
        ax2.set_ylabel('Average IMDB Rating', fontsize=14, fontweight='bold')
        ax2.set_title('Average Rating by Decade\nQuality Across Time',
                     fontsize=16, fontweight='bold', pad=20)
        ax2.grid(True, alpha=0.3)
        ax2.set_ylim([5, 8])
        
        plt.tight_layout()
        filepath = save_figure(fig, '07_decade_distribution.png', batch_number=1)
        plt.close()
        
        return self
    
    def viz_8_era_analysis(self):
        """Viz 8: Era Analysis - cinema periods you prefer."""
        log_message("📊 Creating Visualization 8: Era Analysis")
        
        df_era = self.df[self.df['era'].notna()].copy()
        
        # Define era order
        era_order = [
            'Silent Era (pre-1927)',
            'Pre-Code (1927-1934)',
            'Golden Age (1935-1959)',
            'New Hollywood (1960-1979)',
            'Blockbuster Era (1980-1999)',
            'Digital Age (2000-2009)',
            'Modern (2010-2019)',
            'Current (2020+)'
        ]
        
        era_stats = df_era.groupby('era').agg({
            'const': 'count',
            'imdb_rating': 'mean'
        }).reset_index()
        era_stats.columns = ['Era', 'Count', 'Avg_Rating']
        
        # Sort by era order
        era_stats['Era'] = pd.Categorical(era_stats['Era'], categories=era_order, ordered=True)
        era_stats = era_stats.sort_values('Era')
        
        fig, ax = plt.subplots(figsize=(16, 10))
        
        # Horizontal bar chart
        bars = ax.barh(era_stats['Era'], era_stats['Count'],
                      edgecolor='black', linewidth=1.5, alpha=0.7)
        
        # Color by count
        colors = plt.cm.plasma(era_stats['Count'] / era_stats['Count'].max())
        for bar, color in zip(bars, colors):
            bar.set_color(color)
        
        ax.set_xlabel('Number of Films', fontsize=14, fontweight='bold')
        ax.set_ylabel('Cinema Era', fontsize=14, fontweight='bold')
        ax.set_title('Films Watched by Cinema Era\nYour Journey Through Film History',
                    fontsize=16, fontweight='bold', pad=20)
        ax.grid(True, alpha=0.3, axis='x')
        
        # Add value labels
        for i, (era, count) in enumerate(zip(era_stats['Era'], era_stats['Count'])):
            ax.text(count, i, f'  {count}', va='center', fontsize=11, fontweight='bold')
        
        plt.tight_layout()
        filepath = save_figure(fig, '08_era_analysis.png', batch_number=1)
        plt.close()
        
        return self
    
    def generate_summary_report(self):
        """Generate text summary of findings."""
        log_message("\n" + "=" * 80)
        log_message("BATCH 1 ANALYSIS SUMMARY")
        log_message("=" * 80)
        
        log_message(f"\nTotal Films Analyzed: {len(self.df):,}")
        log_message(f"Year Range: {int(self.df['year'].min())} - {int(self.df['year'].max())}")
        log_message(f"Total Watch Time: {self.df['runtime_mins'].sum()/60:.0f} hours")
        
        log_message(f"\nRating Statistics:")
        log_message(f"  IMDB Avg: {self.df['imdb_rating'].mean():.2f}")
        log_message(f"  IMDB Median: {self.df['imdb_rating'].median():.2f}")
        
        if 'tmdb_vote_average' in self.df.columns:
            tmdb_avg = self.df['tmdb_vote_average'].mean()
            if not pd.isna(tmdb_avg):
                log_message(f"  TMDB Avg: {tmdb_avg:.2f}")
        
        log_message(f"\nTop 5 Decades:")
        top_decades = self.df['decade'].value_counts().head(5)
        for decade, count in top_decades.items():
            if not pd.isna(decade):
                log_message(f"  {int(decade)}s: {count} films")
        
        log_message(f"\nTop 5 Genres:")
        genre_exploded = explode_genres(self.df)
        top_genres = genre_exploded['genre'].value_counts().head(5)
        for genre, count in top_genres.items():
            log_message(f"  {genre}: {count} films")
        
        log_message("\n✅ Batch 1 Analysis Complete!")
        
        return self


def main():
    """Main execution."""
    print("\n" + "🎨" * 40)
    print("\n" + " " * 15 + "BATCH 1: QUANTIFIED SELF ANALYSIS")
    print(" " * 10 + "Analyzing YOUR Watched Movies with 8 Visualizations")
    print("\n" + "🎨" * 40 + "\n")
    
    try:
        analysis = QuantifiedSelfAnalysis()
        analysis.load_data()
        
        # Generate all 8 visualizations
        analysis.viz_1_rating_distribution()
        analysis.viz_2_rating_matrix_heatmap()
        analysis.viz_3_temporal_viewing_timeline()
        analysis.viz_4_cine_calendar_heatmap()
        analysis.viz_5_runtime_sweet_spot()
        analysis.viz_6_rating_vs_runtime_scatter()
        analysis.viz_7_decade_distribution()
        analysis.viz_8_era_analysis()
        
        # Generate summary
        analysis.generate_summary_report()
        
        print("\n" + "✨" * 40)
        print("\n" + " " * 10 + "BATCH 1 COMPLETE - 8 VISUALIZATIONS CREATED!")
        print(" " * 15 + "Check analysis_outputs/visualizations/batch_1/")
        print("\n" + "✨" * 40 + "\n")
        
        return 0
        
    except Exception as e:
        print("\n" + "❌" * 40)
        print(f"\nERROR: {str(e)}")
        print("\n" + "❌" * 40 + "\n")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)