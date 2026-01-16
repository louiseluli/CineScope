#!/usr/bin/env python3
"""
CineScope Batch 37: Personal Viewing Patterns
==============================================

Analyzes my personal viewing patterns, habits, preferences evolution,
and rating tendencies over time.

Author: CineScope Analytics
Date: 2025-12-31
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict
import warnings
warnings.filterwarnings('ignore')

class ViewingPatternsAnalyzer:
    """Analyze personal viewing patterns and preferences."""

    def __init__(self, data_path='data/processed/watched_movies_master.csv'):
        """Initialize analyzer with watched movies data."""
        self.data_path = Path(data_path)
        self.output_dir = Path('analysis_outputs/visualizations/batch_37')
        self.report_dir = Path('analysis_outputs/reports')

        # Create output directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)

        # Load data
        print("Loading watched movies data...")
        self.df = pd.read_csv(self.data_path)
        print(f"Loaded {len(self.df)} watched films")

        # Set up plotting
        plt.style.use('default')
        sns.set_palette("husl")

        # Prepare data
        self._prepare_data()

    def _prepare_data(self):
        """Prepare data for analysis."""
        print("Preparing viewing data...")

        # Convert date if available
        if 'Date Rated' in self.df.columns:
            self.df['watch_date'] = pd.to_datetime(self.df['Date Rated'], errors='coerce')
            self.has_watch_dates = self.df['watch_date'].notna().sum() > 0
        else:
            self.has_watch_dates = False

        # Decade
        self.df['decade'] = (self.df['year'] // 10) * 10

        # Rating categories
        self.df['rating_category'] = pd.cut(self.df['IMDb Rating'],
                                            bins=[0, 6, 7, 8, 9, 10],
                                            labels=['Poor (<6)', 'Good (6-7)',
                                                   'Great (7-8)', 'Excellent (8-9)',
                                                   'Masterpiece (9-10)'])

        print(f"  Has watch dates: {self.has_watch_dates}")

    def visualize_rating_patterns(self):
        """Visualization 1-4: My rating patterns."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('My Rating Patterns', fontsize=16, fontweight='bold')

        # 1. Rating distribution
        ax1 = axes[0, 0]
        ax1.hist(self.df['IMDb Rating'], bins=40, color='#3498DB',
                edgecolor='black', alpha=0.7)
        ax1.axvline(self.df['IMDb Rating'].mean(), color='red', linestyle='--',
                   linewidth=2, label=f"My Mean: {self.df['IMDb Rating'].mean():.2f}")
        ax1.axvline(self.df['IMDb Rating'].median(), color='blue', linestyle='--',
                   linewidth=2, label=f"My Median: {self.df['IMDb Rating'].median():.2f}")
        ax1.set_xlabel('My Rating', fontsize=11, fontweight='bold')
        ax1.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
        ax1.set_title('My Rating Distribution', fontsize=12, fontweight='bold')
        ax1.legend()
        ax1.grid(axis='y', alpha=0.3)

        # 2. Rating categories pie chart
        ax2 = axes[0, 1]
        category_counts = self.df['rating_category'].value_counts()

        colors = ['#E74C3C', '#F39C12', '#F1C40F', '#2ECC71', '#9B59B6']
        ax2.pie(category_counts.values, labels=category_counts.index,
               autopct='%1.1f%%', colors=colors, startangle=90,
               textprops={'fontsize': 10})
        ax2.set_title('Rating Category Distribution', fontsize=12, fontweight='bold')

        # 3. Rating statistics
        ax3 = axes[1, 0]
        ax3.axis('off')

        stats_text = "MY RATING STATISTICS\n" + "="*50 + "\n\n"
        stats_text += f"Total Films Watched: {len(self.df)}\n\n"

        stats_text += f"Mean Rating: {self.df['IMDb Rating'].mean():.2f}\n"
        stats_text += f"Median Rating: {self.df['IMDb Rating'].median():.2f}\n"
        stats_text += f"Std Deviation: {self.df['IMDb Rating'].std():.2f}\n\n"

        stats_text += f"Highest Rating: {self.df['IMDb Rating'].max():.1f}\n"
        stats_text += f"Lowest Rating: {self.df['IMDb Rating'].min():.1f}\n"
        stats_text += f"Range: {self.df['IMDb Rating'].max() - self.df['IMDb Rating'].min():.1f}\n\n"

        # Percentiles
        stats_text += "PERCENTILES:\n"
        for pct in [25, 50, 75, 90, 95]:
            val = self.df['IMDb Rating'].quantile(pct/100)
            stats_text += f"  {pct}th: {val:.2f}\n"

        stats_text += f"\n\nMost Common Rating: {self.df['IMDb Rating'].mode()[0]:.1f}\n"
        stats_text += f"Films Rated 8.0+: {len(self.df[self.df['IMDb Rating'] >= 8.0])} ({len(self.df[self.df['IMDb Rating'] >= 8.0])/len(self.df)*100:.1f}%)\n"
        stats_text += f"Films Rated 9.0+: {len(self.df[self.df['IMDb Rating'] >= 9.0])} ({len(self.df[self.df['IMDb Rating'] >= 9.0])/len(self.df)*100:.1f}%)\n"

        ax3.text(0.1, 0.9, stats_text, transform=ax3.transAxes,
                fontsize=10, verticalalignment='top',
                family='monospace',
                bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.5))
        ax3.set_title('Rating Statistics', fontsize=12, fontweight='bold')

        # 4. Rating by decade
        ax4 = axes[1, 1]
        decade_ratings = self.df.groupby('decade')['IMDb Rating'].agg(['mean', 'count'])
        decade_ratings = decade_ratings[decade_ratings['count'] >= 5]  # At least 5 films

        x_pos = np.arange(len(decade_ratings))
        bars = ax4.bar(x_pos, decade_ratings['mean'], color='#2ECC71',
                      edgecolor='black', alpha=0.7)
        ax4.set_xticks(x_pos)
        ax4.set_xticklabels([f"{int(d)}s" for d in decade_ratings.index], rotation=45)
        ax4.set_ylabel('Average Rating', fontsize=11, fontweight='bold')
        ax4.set_xlabel('Decade', fontsize=11, fontweight='bold')
        ax4.set_title('My Average Rating by Decade', fontsize=12, fontweight='bold')
        ax4.grid(axis='y', alpha=0.3)
        ax4.set_ylim(0, 10)

        # Annotate with count
        for i, (bar, count) in enumerate(zip(bars, decade_ratings['count'])):
            height = bar.get_height()
            ax4.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                    f'{height:.2f}\n({count})',
                    ha='center', va='bottom', fontsize=7)

        plt.tight_layout()
        output_path = self.output_dir / 'rating_patterns.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_genre_preferences(self):
        """Visualization 5-6: Genre preferences analysis."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('My Genre Preferences', fontsize=16, fontweight='bold')

        # Extract all genres
        all_genres = []
        genre_ratings = defaultdict(list)

        for idx, row in self.df.iterrows():
            if pd.notna(row['Genres']):
                genres = [g.strip() for g in str(row['Genres']).split(',')]
                all_genres.extend(genres)
                for genre in genres:
                    genre_ratings[genre].append(row['IMDb Rating'])

        # 1. Genre frequency
        ax1 = axes[0, 0]
        genre_counts = Counter(all_genres).most_common(15)
        genres, counts = zip(*genre_counts)

        bars = ax1.barh(range(len(genres)), counts,
                       color=plt.cm.tab10(np.arange(len(genres))),
                       edgecolor='black')
        ax1.set_yticks(range(len(genres)))
        ax1.set_yticklabels(genres, fontsize=9)
        ax1.set_xlabel('Number of Films Watched', fontsize=11, fontweight='bold')
        ax1.set_title('My Most Watched Genres', fontsize=12, fontweight='bold')
        ax1.invert_yaxis()
        ax1.grid(axis='x', alpha=0.3)

        # Annotate
        for i, count in enumerate(counts):
            ax1.text(count + 1, i, f'{count}', va='center', fontsize=9)

        # 2. Average rating by genre
        ax2 = axes[0, 1]
        genre_avg = {g: np.mean(ratings) for g, ratings in genre_ratings.items()
                     if len(ratings) >= 5}
        sorted_genres = sorted(genre_avg.items(), key=lambda x: x[1], reverse=True)[:15]
        genres_avg, ratings_avg = zip(*sorted_genres)

        bars = ax2.barh(range(len(genres_avg)), ratings_avg,
                       color=plt.cm.RdYlGn(np.linspace(0.3, 1, len(genres_avg))),
                       edgecolor='black')
        ax2.set_yticks(range(len(genres_avg)))
        ax2.set_yticklabels(genres_avg, fontsize=9)
        ax2.set_xlabel('Average Rating', fontsize=11, fontweight='bold')
        ax2.set_title('My Highest Rated Genres (5+ films)', fontsize=12, fontweight='bold')
        ax2.invert_yaxis()
        ax2.grid(axis='x', alpha=0.3)
        ax2.set_xlim(0, 10)

        # Annotate
        for i, rating in enumerate(ratings_avg):
            ax2.text(rating + 0.1, i, f'{rating:.2f}', va='center', fontsize=8)

        # 3. Genre rating distribution (top 6 genres)
        ax3 = axes[1, 0]
        top_6_genres = [g for g, _ in genre_counts[:6]]
        rating_data = [genre_ratings[g] for g in top_6_genres]

        bp = ax3.boxplot(rating_data, labels=top_6_genres, patch_artist=True)
        colors = plt.cm.Set3(np.linspace(0, 1, 6))
        for patch, color in zip(bp['boxes'], colors):
            patch.set_facecolor(color)

        ax3.set_xticklabels(top_6_genres, rotation=45, ha='right', fontsize=9)
        ax3.set_ylabel('Rating', fontsize=11, fontweight='bold')
        ax3.set_title('Rating Distribution by Top Genres', fontsize=12, fontweight='bold')
        ax3.grid(axis='y', alpha=0.3)

        # 4. Genre preferences over time (if dates available)
        ax4 = axes[1, 1]

        if self.has_watch_dates:
            # Group by year
            self.df['watch_year'] = self.df['watch_date'].dt.year
            yearly_genres = defaultdict(lambda: defaultdict(int))

            for idx, row in self.df[self.df['watch_date'].notna()].iterrows():
                year = row['watch_year']
                if pd.notna(row['Genres']):
                    genres = [g.strip() for g in str(row['Genres']).split(',')]
                    for genre in genres:
                        yearly_genres[year][genre] += 1

            # Plot top 5 genres over time
            top_5 = [g for g, _ in genre_counts[:5]]
            years = sorted(yearly_genres.keys())

            for genre in top_5:
                counts_by_year = [yearly_genres[year].get(genre, 0) for year in years]
                ax4.plot(years, counts_by_year, marker='o', linewidth=2,
                        markersize=6, label=genre, alpha=0.7)

            ax4.set_xlabel('Year', fontsize=11, fontweight='bold')
            ax4.set_ylabel('Films Watched', fontsize=11, fontweight='bold')
            ax4.set_title('Genre Preferences Evolution', fontsize=12, fontweight='bold')
            ax4.legend(fontsize=8)
            ax4.grid(True, alpha=0.3)
        else:
            # Alternative: genre count vs avg rating
            genre_scatter_data = [(g, len(ratings), np.mean(ratings))
                                 for g, ratings in genre_ratings.items()
                                 if len(ratings) >= 3]
            genres_s, counts_s, ratings_s = zip(*genre_scatter_data)

            scatter = ax4.scatter(counts_s, ratings_s, s=100, alpha=0.6,
                                c=range(len(counts_s)), cmap='viridis',
                                edgecolors='black', linewidth=1)
            ax4.set_xlabel('Films Watched', fontsize=11, fontweight='bold')
            ax4.set_ylabel('Average Rating', fontsize=11, fontweight='bold')
            ax4.set_title('Genre: Quantity vs Quality', fontsize=12, fontweight='bold')
            ax4.grid(True, alpha=0.3)

            # Annotate top genres
            for i, (genre, count, rating) in enumerate(genre_scatter_data[:10]):
                ax4.annotate(genre, (count, rating), fontsize=7, alpha=0.7)

        plt.tight_layout()
        output_path = self.output_dir / 'genre_preferences.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_director_preferences(self):
        """Visualization 7-8: Director preferences."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('My Director Preferences', fontsize=16, fontweight='bold')

        # Director stats
        director_films = defaultdict(list)
        for idx, row in self.df.iterrows():
            if pd.notna(row.get('Directors')):
                directors = [d.strip() for d in str(row['Directors']).split(',')]
                for director in directors:
                    director_films[director].append(row['IMDb Rating'])

        # 1. Most watched directors
        ax1 = axes[0, 0]
        director_counts = {d: len(films) for d, films in director_films.items()}
        top_directors = sorted(director_counts.items(), key=lambda x: x[1], reverse=True)[:15]
        directors, counts = zip(*top_directors)

        bars = ax1.barh(range(len(directors)), counts, color='#9B59B6', edgecolor='black')
        ax1.set_yticks(range(len(directors)))
        ax1.set_yticklabels([d[:30] for d in directors], fontsize=9)
        ax1.set_xlabel('Films Watched', fontsize=11, fontweight='bold')
        ax1.set_title('My Most Watched Directors', fontsize=12, fontweight='bold')
        ax1.invert_yaxis()
        ax1.grid(axis='x', alpha=0.3)

        # Annotate
        for i, count in enumerate(counts):
            ax1.text(count + 0.1, i, f'{count}', va='center', fontsize=9)

        # 2. Highest rated directors (3+ films)
        ax2 = axes[0, 1]
        director_avg = {d: np.mean(films) for d, films in director_films.items()
                       if len(films) >= 3}
        top_rated = sorted(director_avg.items(), key=lambda x: x[1], reverse=True)[:15]
        directors_rated, ratings = zip(*top_rated)

        bars = ax2.barh(range(len(directors_rated)), ratings,
                       color=plt.cm.RdYlGn(np.linspace(0.3, 1, len(directors_rated))),
                       edgecolor='black')
        ax2.set_yticks(range(len(directors_rated)))
        ax2.set_yticklabels([d[:30] for d in directors_rated], fontsize=9)
        ax2.set_xlabel('Average Rating', fontsize=11, fontweight='bold')
        ax2.set_title('Highest Rated Directors (3+ films)', fontsize=12, fontweight='bold')
        ax2.invert_yaxis()
        ax2.grid(axis='x', alpha=0.3)
        ax2.set_xlim(0, 10)

        # Annotate
        for i, rating in enumerate(ratings):
            ax2.text(rating + 0.1, i, f'{rating:.2f}', va='center', fontsize=8)

        # 3. Director consistency (std dev)
        ax3 = axes[1, 0]
        director_consistency = {d: np.std(films) for d, films in director_films.items()
                               if len(films) >= 3}
        most_consistent = sorted(director_consistency.items(), key=lambda x: x[1])[:12]
        directors_cons, stds = zip(*most_consistent)

        bars = ax3.barh(range(len(directors_cons)), stds, color='#2ECC71', edgecolor='black')
        ax3.set_yticks(range(len(directors_cons)))
        ax3.set_yticklabels([d[:30] for d in directors_cons], fontsize=9)
        ax3.set_xlabel('Rating Std Deviation (Lower = More Consistent)', fontsize=11, fontweight='bold')
        ax3.set_title('Most Consistent Directors (3+ films)', fontsize=12, fontweight='bold')
        ax3.invert_yaxis()
        ax3.grid(axis='x', alpha=0.3)

        # 4. Director statistics table
        ax4 = axes[1, 1]
        ax4.axis('off')

        dir_text = "DIRECTOR STATISTICS\n" + "="*50 + "\n\n"
        dir_text += f"Total Directors: {len(director_films)}\n"
        dir_text += f"Directors with 3+ films: {len([d for d, f in director_films.items() if len(f) >= 3])}\n"
        dir_text += f"Directors with 5+ films: {len([d for d, f in director_films.items() if len(f) >= 5])}\n\n"

        dir_text += "TOP 10 DIRECTOR-FILM COMBINATIONS:\n\n"
        top_10_dirs = sorted(director_counts.items(), key=lambda x: x[1], reverse=True)[:10]

        for i, (director, count) in enumerate(top_10_dirs, 1):
            avg_rating = np.mean(director_films[director])
            dir_text += f"{i:2d}. {director[:35]}\n"
            dir_text += f"    {count} films | Avg: {avg_rating:.2f}\n\n"

        ax4.text(0.1, 0.9, dir_text, transform=ax4.transAxes,
                fontsize=9, verticalalignment='top',
                family='monospace',
                bbox=dict(boxstyle='round', facecolor='#FFE6F0', alpha=0.7))
        ax4.set_title('Director Overview', fontsize=12, fontweight='bold')

        plt.tight_layout()
        output_path = self.output_dir / 'director_preferences.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_temporal_patterns(self):
        """Visualization 9-10: Viewing patterns over time."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Viewing Patterns Over Time', fontsize=16, fontweight='bold')

        # 1. Films by release decade
        ax1 = axes[0, 0]
        decade_counts = self.df['decade'].value_counts().sort_index()

        bars = ax1.bar(decade_counts.index, decade_counts.values, width=8,
                      color='#3498DB', edgecolor='black', alpha=0.7)
        ax1.set_xlabel('Decade', fontsize=11, fontweight='bold')
        ax1.set_ylabel('Films Watched', fontsize=11, fontweight='bold')
        ax1.set_title('Films Watched by Release Decade', fontsize=12, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)

        # Annotate
        for bar in bars:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(height)}', ha='center', va='bottom', fontsize=8)

        # 2. Rating evolution over decades
        ax2 = axes[0, 1]
        decade_rating_stats = self.df.groupby('decade')['IMDb Rating'].agg(['mean', 'std'])

        ax2.plot(decade_rating_stats.index, decade_rating_stats['mean'],
                marker='o', linewidth=2, markersize=8, color='#E74C3C',
                label='Average Rating')
        ax2.fill_between(decade_rating_stats.index,
                        decade_rating_stats['mean'] - decade_rating_stats['std'],
                        decade_rating_stats['mean'] + decade_rating_stats['std'],
                        alpha=0.3, color='#E74C3C')
        ax2.set_xlabel('Decade', fontsize=11, fontweight='bold')
        ax2.set_ylabel('Rating', fontsize=11, fontweight='bold')
        ax2.set_title('My Rating Trends by Decade', fontsize=12, fontweight='bold')
        ax2.legend()
        ax2.grid(True, alpha=0.3)

        # 3. Cumulative films watched (if dates available)
        ax3 = axes[1, 0]

        if self.has_watch_dates:
            dated_films = self.df[self.df['watch_date'].notna()].sort_values('watch_date')
            dated_films['cumulative'] = range(1, len(dated_films) + 1)

            ax3.plot(dated_films['watch_date'], dated_films['cumulative'],
                    linewidth=2, color='#2ECC71')
            ax3.fill_between(dated_films['watch_date'], dated_films['cumulative'],
                           alpha=0.3, color='#2ECC71')
            ax3.set_xlabel('Date', fontsize=11, fontweight='bold')
            ax3.set_ylabel('Cumulative Films', fontsize=11, fontweight='bold')
            ax3.set_title('Cumulative Viewing Progress', fontsize=12, fontweight='bold')
            ax3.grid(True, alpha=0.3)
        else:
            # Alternative: runtime distribution
            if 'Runtime (mins)' in self.df.columns:
                ax3.hist(self.df['Runtime (mins)'].dropna(), bins=30,
                        color='#F39C12', edgecolor='black', alpha=0.7)
                ax3.axvline(self.df['Runtime (mins)'].mean(), color='red',
                           linestyle='--', linewidth=2,
                           label=f"Mean: {self.df['Runtime (mins)'].mean():.0f} mins")
                ax3.set_xlabel('Runtime (minutes)', fontsize=11, fontweight='bold')
                ax3.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
                ax3.set_title('Runtime Preferences', fontsize=12, fontweight='bold')
                ax3.legend()
                ax3.grid(axis='y', alpha=0.3)

        # 4. Viewing summary
        ax4 = axes[1, 1]
        ax4.axis('off')

        summary_text = "VIEWING SUMMARY\n" + "="*50 + "\n\n"
        summary_text += f"Total Films: {len(self.df)}\n\n"

        # Decade coverage
        summary_text += "DECADE COVERAGE:\n"
        for decade in sorted(decade_counts.index):
            count = decade_counts[decade]
            pct = count / len(self.df) * 100
            summary_text += f"  {int(decade)}s: {count:4d} ({pct:5.1f}%)\n"

        summary_text += f"\n\nOldest Film: {self.df['year'].min():.0f}\n"
        summary_text += f"Newest Film: {self.df['year'].max():.0f}\n"
        summary_text += f"Span: {self.df['year'].max() - self.df['year'].min():.0f} years\n\n"

        if 'Runtime (mins)' in self.df.columns:
            total_runtime = self.df['Runtime (mins)'].sum()
            summary_text += f"Total Runtime: {total_runtime:,.0f} minutes\n"
            summary_text += f"             = {total_runtime/60:.0f} hours\n"
            summary_text += f"             = {total_runtime/60/24:.1f} days\n"

        ax4.text(0.1, 0.9, summary_text, transform=ax4.transAxes,
                fontsize=10, verticalalignment='top',
                family='monospace',
                bbox=dict(boxstyle='round', facecolor='#E8F8F5', alpha=0.7))
        ax4.set_title('Collection Summary', fontsize=12, fontweight='bold')

        plt.tight_layout()
        output_path = self.output_dir / 'temporal_patterns.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def generate_report(self):
        """Generate comprehensive viewing patterns report."""
        print("\nGenerating viewing patterns report...")

        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("CINESCOPE BATCH 37: PERSONAL VIEWING PATTERNS")
        report_lines.append("=" * 80)
        report_lines.append("")
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("")

        # Overall stats
        report_lines.append("=" * 80)
        report_lines.append("OVERALL VIEWING STATISTICS")
        report_lines.append("=" * 80)
        report_lines.append("")
        report_lines.append(f"Total Films Watched: {len(self.df)}")
        report_lines.append(f"Average Rating: {self.df['IMDb Rating'].mean():.2f}")
        report_lines.append(f"Median Rating: {self.df['IMDb Rating'].median():.2f}")
        report_lines.append(f"Rating Std Dev: {self.df['IMDb Rating'].std():.2f}")
        report_lines.append("")

        # Top rated
        report_lines.append("=" * 80)
        report_lines.append("MY TOP 30 RATED FILMS")
        report_lines.append("=" * 80)
        report_lines.append("")

        top_rated = self.df.nlargest(30, 'IMDb Rating')
        for i, (idx, row) in enumerate(top_rated.iterrows(), 1):
            report_lines.append(f"{i:2d}. {row['title']:50s} - {row['IMDb Rating']:.1f} ({row['year']:.0f})")

        report_lines.append("")

        # Genre preferences
        report_lines.append("=" * 80)
        report_lines.append("GENRE PREFERENCES")
        report_lines.append("=" * 80)
        report_lines.append("")

        all_genres = []
        genre_ratings = defaultdict(list)
        for idx, row in self.df.iterrows():
            if pd.notna(row['Genres']):
                genres = [g.strip() for g in str(row['Genres']).split(',')]
                all_genres.extend(genres)
                for genre in genres:
                    genre_ratings[genre].append(row['IMDb Rating'])

        genre_counts = Counter(all_genres).most_common(20)
        for i, (genre, count) in enumerate(genre_counts, 1):
            avg_rating = np.mean(genre_ratings[genre])
            report_lines.append(f"{i:2d}. {genre:20s} - {count:4d} films | Avg Rating: {avg_rating:.2f}")

        report_lines.append("")
        report_lines.append("=" * 80)
        report_lines.append("END OF REPORT")
        report_lines.append("=" * 80)

        # Write report
        report_path = self.report_dir / 'batch_37_viewing_patterns_report.txt'
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))

        print(f"Report saved: {report_path}")

    def run_all_analyses(self):
        """Execute all analyses and generate report."""
        print("\n" + "="*80)
        print("BATCH 37: PERSONAL VIEWING PATTERNS")
        print("="*80)

        print("\n[1/5] Analyzing rating patterns...")
        self.visualize_rating_patterns()

        print("\n[2/5] Analyzing genre preferences...")
        self.visualize_genre_preferences()

        print("\n[3/5] Analyzing director preferences...")
        self.visualize_director_preferences()

        print("\n[4/5] Analyzing temporal patterns...")
        self.visualize_temporal_patterns()

        print("\n[5/5] Generating comprehensive report...")
        self.generate_report()

        print("\n" + "="*80)
        print("BATCH 37 COMPLETE")
        print("="*80)
        print(f"\nVisualizations saved to: {self.output_dir}")
        print(f"Report saved to: {self.report_dir}/batch_37_viewing_patterns_report.txt")


def main():
    """Main execution function."""
    analyzer = ViewingPatternsAnalyzer()
    analyzer.run_all_analyses()


if __name__ == '__main__':
    main()
