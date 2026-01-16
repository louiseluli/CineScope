#!/usr/bin/env python3
"""
================================================================================
CINESCOPE BATCH 21: AWARDS & RECOGNITION ANALYSIS
================================================================================

Focus: Awards, nominations, and critical recognition patterns

Data Sources:
- OMDb awards data
- IMDb ratings
- Personal ratings

Visualizations (12):
1. awards_distribution.png - Distribution of award wins and nominations
2. oscar_winners.png - Oscar-winning films analysis
3. awards_vs_rating.png - Correlation between awards and quality
4. awards_by_decade.png - Awards over time
5. awards_by_genre.png - Genre performance in awards
6. nominated_vs_winner.png - Nomination vs win comparison
7. awards_word_cloud.png - Common award types
8. critic_vs_audience.png - Critical vs audience reception
9. awards_revenue_correlation.png - Do awards drive revenue?
10. major_vs_minor_awards.png - Prestigious awards analysis
11. personal_rating_vs_awards.png - Personal taste vs recognition
12. awards_coverage.png - Award data availability

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

class AwardsRecognitionAnalyzer:
    """Analyzes awards and recognition patterns."""

    def __init__(self):
        """Initialize the analyzer."""
        self.base_dir = Path(__file__).parent.parent
        self.data_dir = self.base_dir / 'data'
        self.output_dir = self.base_dir / 'analysis_outputs'
        self.viz_dir = self.output_dir / 'visualizations' / 'batch_21'
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
        print("CINESCOPE BATCH 21: AWARDS & RECOGNITION ANALYSIS")
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

    def process_awards_data(self):
        """Process awards data from OMDb."""
        print("Processing awards data...")

        self.movies_df['has_awards'] = False
        self.movies_df['num_wins'] = 0
        self.movies_df['num_nominations'] = 0
        self.movies_df['awards_text'] = ''

        for idx, row in self.movies_df.iterrows():
            awards_str = row.get('omdb_awards', '')

            if awards_str and not pd.isna(awards_str) and str(awards_str).lower() != 'n/a':
                self.movies_df.at[idx, 'has_awards'] = True
                self.movies_df.at[idx, 'awards_text'] = str(awards_str)

                # Parse wins
                wins_match = re.search(r'(\d+)\s+win', str(awards_str), re.IGNORECASE)
                if wins_match:
                    self.movies_df.at[idx, 'num_wins'] = int(wins_match.group(1))

                # Parse nominations
                noms_match = re.search(r'(\d+)\s+nomination', str(awards_str), re.IGNORECASE)
                if noms_match:
                    self.movies_df.at[idx, 'num_nominations'] = int(noms_match.group(1))

        self.df = self.movies_df[self.movies_df['has_awards']].copy()

        self.stats['total_with_awards'] = len(self.df)
        self.stats['coverage'] = len(self.df) / len(self.movies_df) * 100
        self.stats['total_wins'] = self.df['num_wins'].sum()
        self.stats['total_nominations'] = self.df['num_nominations'].sum()

        print(f"✓ Processed {len(self.df):,} films with awards data ({self.stats['coverage']:.1f}% coverage)")
        print(f"  Total wins: {self.stats['total_wins']:,}")
        print(f"  Total nominations: {self.stats['total_nominations']:,}")
        print()

    def visualize_awards_distribution(self):
        """Awards distribution."""
        print("Creating awards distribution visualization...")

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

        # Wins distribution
        ax1.hist(self.df['num_wins'], bins=30, color='gold', alpha=0.7, edgecolor='black')
        ax1.axvline(self.df['num_wins'].mean(), color='red', linestyle='--',
                   linewidth=2, label=f'Mean: {self.df["num_wins"].mean():.1f}')
        ax1.set_xlabel('Number of Wins', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Frequency', fontsize=12, fontweight='bold')
        ax1.set_title('Distribution of Award Wins', fontsize=14, fontweight='bold')
        ax1.legend()
        ax1.grid(axis='y', alpha=0.3)

        # Nominations distribution
        ax2.hist(self.df['num_nominations'], bins=30, color='silver', alpha=0.7, edgecolor='black')
        ax2.axvline(self.df['num_nominations'].mean(), color='red', linestyle='--',
                   linewidth=2, label=f'Mean: {self.df["num_nominations"].mean():.1f}')
        ax2.set_xlabel('Number of Nominations', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Frequency', fontsize=12, fontweight='bold')
        ax2.set_title('Distribution of Award Nominations', fontsize=14, fontweight='bold')
        ax2.legend()
        ax2.grid(axis='y', alpha=0.3)

        # Pie chart: Awards vs No Awards
        has_awards_count = len(self.df)
        no_awards_count = len(self.movies_df) - has_awards_count

        labels = ['With Awards', 'No Awards Data']
        values = [has_awards_count, no_awards_count]
        colors = ['#f39c12', '#95a5a6']

        wedges, texts, autotexts = ax3.pie(values, labels=labels, autopct='%1.1f%%',
                                           colors=colors, startangle=90)

        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(12)

        ax3.set_title('Award Data Coverage', fontsize=14, fontweight='bold')

        # Top 15 most awarded films
        top_films = self.df.nlargest(15, 'num_wins')[['Title', 'num_wins']]

        colors = plt.cm.YlOrBr(np.linspace(0.4, 0.9, len(top_films)))
        ax4.barh(range(len(top_films)), top_films['num_wins'], color=colors,
                alpha=0.7, edgecolor='black')
        ax4.set_yticks(range(len(top_films)))
        ax4.set_yticklabels([title[:30] + '...' if len(title) > 30 else title
                            for title in top_films['Title']], fontsize=9)
        ax4.invert_yaxis()
        ax4.set_xlabel('Number of Wins', fontsize=12, fontweight='bold')
        ax4.set_title('Top 15 Most Awarded Films', fontsize=14, fontweight='bold')
        ax4.grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'awards_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: awards_distribution.png")

    def visualize_awards_vs_rating(self):
        """Awards correlation with ratings."""
        print("Creating awards vs rating visualization...")

        # Filter to films with IMDb rating
        df_rated = self.df[self.df['IMDb Rating'].notna() & (self.df['IMDb Rating'] > 0)].copy()

        if len(df_rated) == 0:
            print("! No rating data")
            return

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Wins vs rating
        ax1.scatter(df_rated['num_wins'], df_rated['IMDb Rating'], alpha=0.5, s=50, color='gold')
        ax1.set_xlabel('Number of Wins', fontsize=12, fontweight='bold')
        ax1.set_ylabel('IMDb Rating', fontsize=12, fontweight='bold')
        ax1.set_title('Award Wins vs IMDb Rating', fontsize=14, fontweight='bold')
        ax1.grid(alpha=0.3)

        # Add trendline
        if len(df_rated) > 1:
            z = np.polyfit(df_rated['num_wins'], df_rated['IMDb Rating'], 1)
            p = np.poly1d(z)
            x_range = np.linspace(df_rated['num_wins'].min(), df_rated['num_wins'].max(), 100)
            ax1.plot(x_range, p(x_range), "r--", alpha=0.8, linewidth=2)

            # Correlation
            corr, p_value = stats.pearsonr(df_rated['num_wins'], df_rated['IMDb Rating'])
            ax1.text(0.05, 0.95, f'Correlation: {corr:.3f}\np-value: {p_value:.4f}',
                    transform=ax1.transAxes, verticalalignment='top',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
                    fontsize=10)

        # Nominations vs rating
        ax2.scatter(df_rated['num_nominations'], df_rated['IMDb Rating'], alpha=0.5, s=50, color='silver')
        ax2.set_xlabel('Number of Nominations', fontsize=12, fontweight='bold')
        ax2.set_ylabel('IMDb Rating', fontsize=12, fontweight='bold')
        ax2.set_title('Award Nominations vs IMDb Rating', fontsize=14, fontweight='bold')
        ax2.grid(alpha=0.3)

        # Add trendline
        if len(df_rated) > 1:
            z = np.polyfit(df_rated['num_nominations'], df_rated['IMDb Rating'], 1)
            p = np.poly1d(z)
            x_range = np.linspace(df_rated['num_nominations'].min(), df_rated['num_nominations'].max(), 100)
            ax2.plot(x_range, p(x_range), "r--", alpha=0.8, linewidth=2)

            # Correlation
            corr2, p_value2 = stats.pearsonr(df_rated['num_nominations'], df_rated['IMDb Rating'])
            ax2.text(0.05, 0.95, f'Correlation: {corr2:.3f}\np-value: {p_value2:.4f}',
                    transform=ax2.transAxes, verticalalignment='top',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
                    fontsize=10)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'awards_vs_rating.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: awards_vs_rating.png")

    def visualize_awards_by_decade(self):
        """Awards by decade."""
        print("Creating awards by decade visualization...")

        self.df['decade'] = (pd.to_numeric(self.df['Year'], errors='coerce') // 10) * 10
        decade_df = self.df[(self.df['decade'] >= 1920) & (self.df['decade'] <= 2020)].copy()

        if len(decade_df) == 0:
            print("! No decade data")
            return

        decade_stats = decade_df.groupby('decade').agg({
            'num_wins': ['mean', 'sum'],
            'num_nominations': ['mean', 'sum']
        })

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))

        decades = decade_stats.index

        # Average per film
        ax1.plot(decades, decade_stats[('num_wins', 'mean')], marker='o', linewidth=2,
                markersize=8, label='Wins', color='gold')
        ax1.plot(decades, decade_stats[('num_nominations', 'mean')], marker='s', linewidth=2,
                markersize=8, label='Nominations', color='silver')
        ax1.set_xlabel('Decade', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Average per Film', fontsize=12, fontweight='bold')
        ax1.set_title('Average Awards per Film by Decade', fontsize=14, fontweight='bold')
        ax1.legend()
        ax1.grid(alpha=0.3)

        # Total
        width = 3
        ax2.bar(decades - width/2, decade_stats[('num_wins', 'sum')], width=width,
               label='Wins', color='gold', alpha=0.7, edgecolor='black')
        ax2.bar(decades + width/2, decade_stats[('num_nominations', 'sum')], width=width,
               label='Nominations', color='silver', alpha=0.7, edgecolor='black')
        ax2.set_xlabel('Decade', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Total Count', fontsize=12, fontweight='bold')
        ax2.set_title('Total Awards by Decade', fontsize=14, fontweight='bold')
        ax2.legend()
        ax2.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'awards_by_decade.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: awards_by_decade.png")

    def visualize_oscar_winners(self):
        """Analyze Oscar-winning films."""
        print("Creating Oscar winners visualization...")

        # Identify likely Oscar winners from awards text
        self.movies_df['is_oscar'] = self.movies_df['awards_text'].str.contains(
            'Oscar|Academy Award', case=False, na=False)

        oscar_winners = self.movies_df[self.movies_df['is_oscar']]

        if len(oscar_winners) == 0:
            print("! No Oscar winners identified")
            return

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

        # Oscar winners count
        oscar_count = len(oscar_winners)
        non_oscar_count = len(self.movies_df) - oscar_count

        ax1.pie([oscar_count, non_oscar_count],
               labels=['Oscar Winners/Nominees', 'Others'],
               autopct='%1.1f%%', colors=['gold', 'lightgray'],
               explode=(0.1, 0))
        ax1.set_title('Oscar-Related Films in Collection', fontsize=14, fontweight='bold')

        # Rating comparison
        oscar_ratings = oscar_winners['IMDb Rating'].dropna()
        other_ratings = self.movies_df[~self.movies_df['is_oscar']]['IMDb Rating'].dropna()

        ax2.boxplot([oscar_ratings, other_ratings],
                   labels=['Oscar Films', 'Other Films'])
        ax2.set_ylabel('Rating', fontsize=12, fontweight='bold')
        ax2.set_title('Rating Comparison', fontsize=14, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        # Oscar films by decade
        oscar_winners['decade'] = (oscar_winners['Year'] // 10) * 10
        decade_counts = oscar_winners.groupby('decade').size()

        ax3.bar(decade_counts.index, decade_counts.values, color='gold',
               alpha=0.7, edgecolor='black')
        ax3.set_xlabel('Decade', fontsize=12, fontweight='bold')
        ax3.set_ylabel('Number of Oscar Films', fontsize=12, fontweight='bold')
        ax3.set_title('Oscar-Related Films by Decade', fontsize=14, fontweight='bold')
        ax3.grid(axis='y', alpha=0.3)

        # Top rated Oscar films
        top_oscar = oscar_winners.nlargest(10, 'IMDb Rating')[['Title', 'IMDb Rating', 'Year']]

        ax4.axis('off')
        ax4.text(0.5, 0.95, 'Top 10 Rated Oscar Films', ha='center', va='top',
                fontsize=14, fontweight='bold', transform=ax4.transAxes)

        for i, (idx, row) in enumerate(top_oscar.iterrows()):
            title = row['Title'][:35]
            rating = row['IMDb Rating']
            year = row['Year']
            ax4.text(0.1, 0.85 - i*0.08, f"{i+1}. {title} ({year}) - {rating}/10",
                    fontsize=10, transform=ax4.transAxes)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'oscar_winners.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: oscar_winners.png")

    def visualize_awards_by_genre(self):
        """Analyze awards performance by genre."""
        print("Creating awards by genre visualization...")

        genre_awards = defaultdict(lambda: {'wins': [], 'nominations': []})

        for idx, row in self.movies_df[self.movies_df['has_awards']].iterrows():
            genres_str = row.get('tmdb_genres', row.get('Genres', ''))
            wins = row['num_wins']
            noms = row['num_nominations']

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
                        genre_awards[genre]['wins'].append(wins)
                        genre_awards[genre]['nominations'].append(noms)

        # Calculate averages
        genre_stats = []
        for genre, data in genre_awards.items():
            if len(data['wins']) >= 5:  # Minimum threshold
                genre_stats.append({
                    'genre': genre,
                    'avg_wins': np.mean(data['wins']),
                    'avg_noms': np.mean(data['nominations']),
                    'count': len(data['wins'])
                })

        if not genre_stats:
            print("! No genre awards data available")
            return

        df = pd.DataFrame(genre_stats).sort_values('avg_wins', ascending=False)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))

        # Average wins by genre
        colors = plt.cm.YlOrRd(np.linspace(0.3, 0.9, len(df)))

        ax1.barh(range(len(df)), df['avg_wins'], color=colors, alpha=0.7, edgecolor='black')
        ax1.set_yticks(range(len(df)))
        ax1.set_yticklabels(df['genre'])
        ax1.invert_yaxis()
        ax1.set_xlabel('Average Awards Won', fontsize=12, fontweight='bold')
        ax1.set_title('Average Award Wins by Genre', fontsize=14, fontweight='bold')
        ax1.grid(axis='x', alpha=0.3)

        for i, (wins, count) in enumerate(zip(df['avg_wins'], df['count'])):
            ax1.text(wins, i, f' {wins:.1f} ({count} films)', va='center', fontsize=9)

        # Wins vs Nominations
        ax2.scatter(df['avg_noms'], df['avg_wins'], s=df['count']*10,
                   alpha=0.6, c=range(len(df)), cmap='viridis')

        for idx, row in df.iterrows():
            ax2.annotate(row['genre'], (row['avg_noms'], row['avg_wins']),
                        fontsize=8, alpha=0.7)

        ax2.set_xlabel('Average Nominations', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Average Wins', fontsize=12, fontweight='bold')
        ax2.set_title('Genre Awards Performance (bubble size = film count)',
                     fontsize=14, fontweight='bold')
        ax2.grid(alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'awards_by_genre.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: awards_by_genre.png")

    def visualize_nominations_vs_wins(self):
        """Compare nomination vs win patterns."""
        print("Creating nominations vs wins visualization...")

        awarded_films = self.movies_df[self.movies_df['has_awards']].copy()

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

        # Win rate distribution
        awarded_films['win_rate'] = awarded_films.apply(
            lambda x: (x['num_wins'] / (x['num_wins'] + x['num_nominations']) * 100)
            if (x['num_wins'] + x['num_nominations']) > 0 else 0, axis=1)

        ax1.hist(awarded_films['win_rate'], bins=20, color='green',
                alpha=0.7, edgecolor='black')
        ax1.set_xlabel('Win Rate (%)', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Number of Films', fontsize=12, fontweight='bold')
        ax1.set_title('Award Win Rate Distribution', fontsize=14, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)
        ax1.axvline(awarded_films['win_rate'].median(), color='red',
                   linestyle='--', label=f'Median: {awarded_films["win_rate"].median():.1f}%')
        ax1.legend()

        # Scatter: nominations vs wins
        ax2.scatter(awarded_films['num_nominations'], awarded_films['num_wins'],
                   alpha=0.5, s=50)
        ax2.set_xlabel('Number of Nominations', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Number of Wins', fontsize=12, fontweight='bold')
        ax2.set_title('Nominations vs Wins', fontsize=14, fontweight='bold')
        ax2.grid(alpha=0.3)

        # Add trend line
        if len(awarded_films) > 0:
            z = np.polyfit(awarded_films['num_nominations'], awarded_films['num_wins'], 1)
            p = np.poly1d(z)
            ax2.plot(awarded_films['num_nominations'], p(awarded_films['num_nominations']),
                    "r--", alpha=0.8, linewidth=2, label='Trend')
            ax2.legend()

        # Top wins
        top_wins = awarded_films.nlargest(15, 'num_wins')
        colors = plt.cm.Greens(np.linspace(0.4, 0.9, len(top_wins)))

        ax3.barh(range(len(top_wins)), top_wins['num_wins'], color=colors,
                alpha=0.7, edgecolor='black')
        ax3.set_yticks(range(len(top_wins)))
        ax3.set_yticklabels([t[:30] for t in top_wins['Title']], fontsize=9)
        ax3.invert_yaxis()
        ax3.set_xlabel('Number of Wins', fontsize=12, fontweight='bold')
        ax3.set_title('Top 15 Most Awarded Films', fontsize=14, fontweight='bold')
        ax3.grid(axis='x', alpha=0.3)

        # Top nominations
        top_noms = awarded_films.nlargest(15, 'num_nominations')
        colors = plt.cm.Blues(np.linspace(0.4, 0.9, len(top_noms)))

        ax4.barh(range(len(top_noms)), top_noms['num_nominations'], color=colors,
                alpha=0.7, edgecolor='black')
        ax4.set_yticks(range(len(top_noms)))
        ax4.set_yticklabels([t[:30] for t in top_noms['Title']], fontsize=9)
        ax4.invert_yaxis()
        ax4.set_xlabel('Number of Nominations', fontsize=12, fontweight='bold')
        ax4.set_title('Top 15 Most Nominated Films', fontsize=14, fontweight='bold')
        ax4.grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'nominated_vs_winner.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: nominated_vs_winner.png")

    def visualize_awards_word_cloud(self):
        """Visualize common award types."""
        print("Creating awards word cloud...")

        # Extract award types from awards text
        award_keywords = Counter()

        for awards_text in self.movies_df[self.movies_df['has_awards']]['awards_text']:
            if pd.notna(awards_text):
                # Common award keywords
                keywords = ['Oscar', 'Academy', 'Golden Globe', 'BAFTA', 'Cannes',
                           'Emmy', 'SAG', 'Critics', 'Venice', 'Berlin', 'Sundance',
                           'Independent Spirit', 'César', 'Goya']

                for keyword in keywords:
                    if keyword.lower() in str(awards_text).lower():
                        award_keywords[keyword] += 1

        if not award_keywords:
            print("! No award keywords found")
            return

        df = pd.DataFrame(award_keywords.most_common(15), columns=['Award', 'Count'])

        fig, ax = plt.subplots(figsize=(14, 8))

        colors = plt.cm.Spectral(np.linspace(0.2, 0.8, len(df)))

        bars = ax.barh(range(len(df)), df['Count'], color=colors, alpha=0.7, edgecolor='black')
        ax.set_yticks(range(len(df)))
        ax.set_yticklabels(df['Award'])
        ax.invert_yaxis()
        ax.set_xlabel('Frequency', fontsize=12, fontweight='bold')
        ax.set_title('Common Award Types Mentioned', fontsize=14, fontweight='bold')
        ax.grid(axis='x', alpha=0.3)

        for i, count in enumerate(df['Count']):
            ax.text(count, i, f' {count}', va='center', fontsize=10, fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'awards_word_cloud.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: awards_word_cloud.png")

    def visualize_critic_vs_audience(self):
        """Compare critical vs audience reception."""
        print("Creating critic vs audience visualization...")

        # Use OMDb Metascore and Rotten Tomatoes as critic scores
        df = self.movies_df.copy()

        # Parse metascore
        df['metascore'] = pd.to_numeric(df['omdb_metascore'], errors='coerce')

        # Parse Rotten Tomatoes
        df['rt_score'] = df['omdb_rating_rotten_tomatoes'].str.extract(r'(\d+)%').astype(float)

        # Filter to films with both scores
        valid_films = df[(df['metascore'].notna()) & (df['IMDb Rating'].notna())]

        if len(valid_films) < 10:
            print("! Insufficient critic/audience data")
            return

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

        # Metascore vs Personal Rating
        ax1.scatter(valid_films['metascore'], valid_films['IMDb Rating'],
                   alpha=0.5, s=50)
        ax1.set_xlabel('Metascore (Critics)', fontsize=12, fontweight='bold')
        ax1.set_ylabel('IMDb Rating', fontsize=12, fontweight='bold')
        ax1.set_title('Critic Score vs Personal Rating', fontsize=14, fontweight='bold')
        ax1.grid(alpha=0.3)

        # Correlation
        corr = valid_films['metascore'].corr(valid_films['IMDb Rating'])
        ax1.text(0.05, 0.95, f'Correlation: {corr:.3f}',
                transform=ax1.transAxes, fontsize=11, fontweight='bold',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        # IMDb vs Personal Rating
        valid_imdb = df[(df['IMDb Rating'].notna()) & (df['IMDb Rating'].notna())]

        ax2.scatter(valid_imdb['IMDb Rating'], valid_imdb['IMDb Rating'],
                   alpha=0.5, s=50, color='coral')
        ax2.set_xlabel('IMDb Rating (Audience)', fontsize=12, fontweight='bold')
        ax2.set_ylabel('IMDb Rating', fontsize=12, fontweight='bold')
        ax2.set_title('IMDb Rating vs Personal Rating', fontsize=14, fontweight='bold')
        ax2.grid(alpha=0.3)

        corr_imdb = valid_imdb['IMDb Rating'].corr(valid_imdb['IMDb Rating'])
        ax2.text(0.05, 0.95, f'Correlation: {corr_imdb:.3f}',
                transform=ax2.transAxes, fontsize=11, fontweight='bold',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        # Distribution comparison
        ax3.hist([valid_films['metascore']/10, valid_films['IMDb Rating']],
                label=['Metascore/10', 'IMDb Rating'], bins=15,
                alpha=0.6, edgecolor='black')
        ax3.set_xlabel('Rating (0-10 scale)', fontsize=12, fontweight='bold')
        ax3.set_ylabel('Frequency', fontsize=12, fontweight='bold')
        ax3.set_title('Rating Distribution Comparison', fontsize=14, fontweight='bold')
        ax3.legend()
        ax3.grid(axis='y', alpha=0.3)

        # Biggest disagreements
        valid_films['disagreement'] = abs(valid_films['metascore']/10 - valid_films['IMDb Rating'])
        top_disagreements = valid_films.nlargest(15, 'disagreement')

        colors = plt.cm.Reds(np.linspace(0.4, 0.9, len(top_disagreements)))

        ax4.barh(range(len(top_disagreements)), top_disagreements['disagreement'],
                color=colors, alpha=0.7, edgecolor='black')
        ax4.set_yticks(range(len(top_disagreements)))
        ax4.set_yticklabels([t[:25] for t in top_disagreements['Title']], fontsize=9)
        ax4.invert_yaxis()
        ax4.set_xlabel('Rating Difference', fontsize=12, fontweight='bold')
        ax4.set_title('Biggest Critic vs Personal Disagreements', fontsize=14, fontweight='bold')
        ax4.grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'critic_vs_audience.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: critic_vs_audience.png")

    def visualize_awards_revenue(self):
        """Analyze awards impact on revenue."""
        print("Creating awards revenue correlation...")

        df = self.movies_df[
            (self.movies_df['has_awards']) &
            (self.movies_df['tmdb_revenue'] > 0)
        ].copy()

        if len(df) < 10:
            print("! Insufficient revenue data for awarded films")
            return

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Total awards vs revenue
        df['total_awards'] = df['num_wins'] + df['num_nominations']

        ax1.scatter(df['total_awards'], df['tmdb_revenue'] / 1e6, alpha=0.5, s=50)
        ax1.set_xlabel('Total Awards (Wins + Nominations)', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax1.set_title('Awards vs Box Office Revenue', fontsize=14, fontweight='bold')
        ax1.grid(alpha=0.3)

        # Correlation
        corr = df['total_awards'].corr(df['tmdb_revenue'])
        ax1.text(0.05, 0.95, f'Correlation: {corr:.3f}',
                transform=ax1.transAxes, fontsize=11, fontweight='bold',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        # Revenue comparison: many awards vs few awards
        high_awards = df[df['total_awards'] >= df['total_awards'].median()]
        low_awards = df[df['total_awards'] < df['total_awards'].median()]

        revenue_comparison = [
            high_awards['tmdb_revenue'].values / 1e6,
            low_awards['tmdb_revenue'].values / 1e6
        ]

        ax2.boxplot(revenue_comparison, labels=['Many Awards', 'Few Awards'])
        ax2.set_ylabel('Revenue (Millions $)', fontsize=12, fontweight='bold')
        ax2.set_title('Revenue by Award Level', fontsize=14, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'awards_revenue_correlation.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: awards_revenue_correlation.png")

    def visualize_prestigious_awards(self):
        """Analyze prestigious awards patterns."""
        print("Creating prestigious awards analysis...")

        # Identify major vs minor awards
        self.movies_df['has_major_award'] = self.movies_df['awards_text'].str.contains(
            'Oscar|Academy Award|Golden Globe|BAFTA', case=False, na=False)

        major_films = self.movies_df[self.movies_df['has_major_award']]
        minor_films = self.movies_df[
            (self.movies_df['has_awards']) & (~self.movies_df['has_major_award'])
        ]

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

        # Distribution
        counts = [len(major_films), len(minor_films),
                 len(self.movies_df) - len(major_films) - len(minor_films)]
        labels = ['Major Awards', 'Other Awards', 'No Awards']
        colors = ['gold', 'silver', 'lightgray']

        ax1.pie(counts, labels=labels, autopct='%1.1f%%', colors=colors, explode=(0.1, 0, 0))
        ax1.set_title('Award Prestige Distribution', fontsize=14, fontweight='bold')

        # Rating comparison
        if len(major_films) > 0 and len(minor_films) > 0:
            ax2.boxplot([major_films['IMDb Rating'].dropna(),
                        minor_films['IMDb Rating'].dropna(),
                        self.movies_df[~self.movies_df['has_awards']]['IMDb Rating'].dropna()],
                       labels=['Major\nAwards', 'Other\nAwards', 'No\nAwards'])
            ax2.set_ylabel('Rating', fontsize=12, fontweight='bold')
            ax2.set_title('Quality by Award Prestige', fontsize=14, fontweight='bold')
            ax2.grid(axis='y', alpha=0.3)

        # Major awards by decade
        if len(major_films) > 0:
            major_films['decade'] = (major_films['Year'] // 10) * 10
            decade_counts = major_films.groupby('decade').size()

            ax3.bar(decade_counts.index, decade_counts.values, color='gold',
                   alpha=0.7, edgecolor='black')
            ax3.set_xlabel('Decade', fontsize=12, fontweight='bold')
            ax3.set_ylabel('Number of Major Award Films', fontsize=12, fontweight='bold')
            ax3.set_title('Major Awards Over Time', fontsize=14, fontweight='bold')
            ax3.grid(axis='y', alpha=0.3)

        # Top major award films
        if len(major_films) > 0:
            top_major = major_films.nlargest(10, 'IMDb Rating')[['Title', 'IMDb Rating', 'Year']]

            ax4.axis('off')
            ax4.text(0.5, 0.95, 'Top 10 Major Award Films', ha='center', va='top',
                    fontsize=14, fontweight='bold', transform=ax4.transAxes)

            for i, (idx, row) in enumerate(top_major.iterrows()):
                title = row['Title'][:30]
                rating = row['IMDb Rating']
                year = row['Year']
                ax4.text(0.1, 0.85 - i*0.08, f"{i+1}. {title} ({year}) - {rating}/10",
                        fontsize=10, transform=ax4.transAxes)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'major_vs_minor_awards.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: major_vs_minor_awards.png")

    def visualize_personal_vs_awards(self):
        """Analyze personal rating vs awards."""
        print("Creating personal rating vs awards visualization...")

        awarded_films = self.movies_df[self.movies_df['has_awards']].copy()
        awarded_films['total_awards'] = awarded_films['num_wins'] + awarded_films['num_nominations']

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Scatter: personal rating vs total awards
        ax1.scatter(awarded_films['IMDb Rating'], awarded_films['total_awards'],
                   alpha=0.5, s=50)
        ax1.set_xlabel('IMDb Rating', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Total Awards (Wins + Noms)', fontsize=12, fontweight='bold')
        ax1.set_title('Personal Taste vs Industry Recognition', fontsize=14, fontweight='bold')
        ax1.grid(alpha=0.3)

        corr = awarded_films['IMDb Rating'].corr(awarded_films['total_awards'])
        ax1.text(0.05, 0.95, f'Correlation: {corr:.3f}',
                transform=ax1.transAxes, fontsize=11, fontweight='bold',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        # Films I loved but critics didn't (low awards)
        high_personal_low_awards = awarded_films[
            (awarded_films['IMDb Rating'] >= 8) &
            (awarded_films['total_awards'] < awarded_films['total_awards'].median())
        ].nlargest(10, 'IMDb Rating')

        if len(high_personal_low_awards) > 0:
            ax2.axis('off')
            ax2.text(0.5, 0.95, 'Hidden Gems\n(High Personal Rating, Fewer Awards)',
                    ha='center', va='top', fontsize=14, fontweight='bold',
                    transform=ax2.transAxes)

            for i, (idx, row) in enumerate(high_personal_low_awards.iterrows()):
                title = row['Title'][:30]
                rating = row['IMDb Rating']
                awards = row['total_awards']
                ax2.text(0.1, 0.82 - i*0.08,
                        f"{i+1}. {title} - {rating}/10 ({int(awards)} awards)",
                        fontsize=10, transform=ax2.transAxes)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'personal_rating_vs_awards.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: personal_rating_vs_awards.png")

    def visualize_awards_coverage(self):
        """Analyze award data availability."""
        print("Creating awards coverage visualization...")

        coverage_stats = {
            'OMDb Awards': self.movies_df['omdb_awards'].notna().sum(),
            'Metascore': self.movies_df['omdb_metascore'].notna().sum(),
            'Rotten Tomatoes': self.movies_df['omdb_rating_rotten_tomatoes'].notna().sum(),
            'Box Office': self.movies_df['omdb_box_office'].notna().sum()
        }

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Coverage by data type
        labels = list(coverage_stats.keys())
        values = list(coverage_stats.values())
        total = len(self.movies_df)

        colors = ['#3498db', '#2ecc71', '#f39c12', '#e74c3c']

        bars = ax1.barh(labels, values, color=colors, alpha=0.7, edgecolor='black')
        ax1.set_xlabel('Number of Films', fontsize=12, fontweight='bold')
        ax1.set_title('Award & Recognition Data Coverage', fontsize=14, fontweight='bold')
        ax1.grid(axis='x', alpha=0.3)

        for i, (label, value) in enumerate(zip(labels, values)):
            pct = (value / total) * 100
            ax1.text(value, i, f' {value} ({pct:.1f}%)', va='center',
                    fontsize=10, fontweight='bold')

        # Pie chart
        has_awards = (self.movies_df['has_awards'].sum(),
                     len(self.movies_df) - self.movies_df['has_awards'].sum())

        ax2.pie(has_awards, labels=['With Award Data', 'No Award Data'],
               autopct='%1.1f%%', colors=['#2ecc71', '#e74c3c'],
               explode=(0.05, 0))
        ax2.set_title('Overall Award Data Coverage', fontsize=14, fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'awards_coverage.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Saved: awards_coverage.png")

    def generate_remaining_visualizations(self):
        """Generate all remaining visualizations with real data."""
        print("Generating remaining visualizations...")

        self.visualize_oscar_winners()
        self.visualize_awards_by_genre()
        self.visualize_nominations_vs_wins()
        self.visualize_awards_word_cloud()
        self.visualize_critic_vs_audience()
        self.visualize_awards_revenue()
        self.visualize_prestigious_awards()
        self.visualize_personal_vs_awards()
        self.visualize_awards_coverage()

    def generate_report(self):
        """Generate comprehensive text report."""
        print("Generating report...")

        report_path = self.reports_dir / 'batch_21_awards_recognition_report.txt'

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("CINESCOPE BATCH 21: AWARDS & RECOGNITION ANALYSIS\n")
            f.write("="*80 + "\n\n")

            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            f.write(f"Total Films Analyzed: {self.stats.get('total_with_awards', 0):,}\n")
            f.write(f"Coverage: {self.stats.get('coverage', 0):.1f}%\n\n")

            f.write("="*80 + "\n")
            f.write("AWARDS STATISTICS\n")
            f.write("="*80 + "\n\n")

            f.write(f"Total Award Wins: {self.stats.get('total_wins', 0):,}\n")
            f.write(f"Total Award Nominations: {self.stats.get('total_nominations', 0):,}\n\n")

            if len(self.df) > 0:
                f.write(f"Average Wins per Film: {self.df['num_wins'].mean():.1f}\n")
                f.write(f"Average Nominations per Film: {self.df['num_nominations'].mean():.1f}\n\n")

                # Most awarded film
                most_awarded = self.df.nlargest(1, 'num_wins').iloc[0]
                f.write(f"Most Awarded Film: {most_awarded['Title']} ({int(most_awarded['num_wins'])} wins)\n")

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
            self.process_awards_data()

            print("\nGenerating visualizations...")
            print("-" * 80)

            self.visualize_awards_distribution()
            self.visualize_awards_vs_rating()
            self.visualize_awards_by_decade()
            self.generate_remaining_visualizations()

            print("-" * 80)
            print()

            self.generate_report()

            print("="*80)
            print("BATCH 21 ANALYSIS COMPLETE!")
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
    analyzer = AwardsRecognitionAnalyzer()
    analyzer.run()
