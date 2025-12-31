#!/usr/bin/env python3
"""
CineScope Batch 29: Awards Deep-Dive Analysis
============================================

Comprehensive analysis of film awards beyond the Oscars, including BAFTA,
Golden Globes, Cannes, Venice, Berlin, SAG, and other prestigious ceremonies.

Author: CineScope Analytics
Date: 2025-12-30
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime
import re
from collections import Counter, defaultdict
from scipy import stats

class AwardsDeepDiveAnalyzer:
    """Analyze film awards across all major ceremonies and festivals."""

    def __init__(self, data_path='data/processed/watched_movies_master.csv'):
        """Initialize analyzer with watched movies dataset."""
        self.data_path = Path(data_path)
        self.output_dir = Path('analysis_outputs/visualizations/batch_29')
        self.report_dir = Path('analysis_outputs/reports')

        # Create output directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)

        # Load data
        print("Loading watched movies data...")
        self.df = pd.read_csv(self.data_path)

        print(f"Loaded {len(self.df)} watched films")

        # Award type patterns for extraction
        self.award_patterns = {
            'Oscar': r'(\d+)\s+Oscar',
            'BAFTA': r'(\d+)\s+BAFTA',
            'Golden Globe': r'(\d+)\s+Golden Globe',
            'Cannes': r'Cannes|Palme d\'Or',
            'Venice': r'Venice|Golden Lion',
            'Berlin': r'Berlin|Golden Bear',
            'SAG': r'(\d+)\s+Screen Actors Guild|SAG',
            'César': r'César',
            'Goya': r'Goya',
            'Critics Choice': r'Critics[\'\']?\s+Choice',
        }

        # Parse awards data
        self._parse_awards()

        # Set up plotting style
        plt.style.use('default')
        sns.set_palette("husl")

    def _parse_awards(self):
        """Use pre-enriched award columns from comprehensive parser."""
        print("Using enriched award data...")
        
        # Copy enriched columns to expected column names
        self.df['oscar_wins'] = self.df['enriched_oscar_wins']
        self.df['oscar_noms'] = self.df['enriched_oscar_noms']
        self.df['has_oscar'] = self.df['enriched_has_oscar']
        self.df['bafta_wins'] = self.df['enriched_bafta_wins']
        self.df['bafta_noms'] = self.df['enriched_bafta_noms']
        self.df['has_bafta'] = self.df['enriched_has_bafta']
        self.df['has_golden_globe'] = self.df['enriched_has_golden_globe']
        self.df['golden_globe_count'] = self.df['enriched_golden_globe_count']
        self.df['has_sag'] = self.df['enriched_has_sag']
        self.df['has_cesar'] = self.df['enriched_has_cesar']
        self.df['has_goya'] = self.df['enriched_has_goya']
        self.df['total_wins'] = self.df['enriched_total_wins']
        self.df['total_noms'] = self.df['enriched_total_noms']
        self.df['award_diversity'] = self.df['enriched_award_diversity']
        self.df['has_awards'] = self.df['omdb_awards'].notna() | self.df['wd_awards'].notna()
        
        # Parse festivals from enriched_festivals string
        self.df['has_festival_award'] = self.df['enriched_festivals'].str.len() > 0
        
        print(f"Loaded enriched awards for {self.df['has_awards'].sum()} films")

    def visualize_awards_overview(self):
        """Visualization 1: Overall awards landscape."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Awards Deep-Dive: Overall Landscape', fontsize=16, fontweight='bold')

        # 1. Award ceremony breakdown
        ax1 = axes[0, 0]
        award_counts = {
            'Oscar Winners': (self.df['oscar_wins'] > 0).sum(),
            'Oscar Nominees': ((self.df['oscar_noms'] > 0) & (self.df['oscar_wins'] == 0)).sum(),
            'BAFTA': self.df['has_bafta'].sum(),
            'Golden Globe': self.df['has_golden_globe'].sum(),
            'Festival Awards': self.df['has_festival_award'].sum(),
            'SAG Awards': self.df['has_sag'].sum(),
            'No Major Awards': (~self.df['has_oscar'] & ~self.df['has_bafta'] &
                               ~self.df['has_golden_globe'] & ~self.df['has_festival_award']).sum()
        }

        colors = ['gold', 'silver', '#CD7F32', '#FFD700', '#E74C3C', '#3498DB', '#95A5A6']
        bars = ax1.barh(list(award_counts.keys()), list(award_counts.values()), color=colors)
        ax1.set_xlabel('Number of Films', fontsize=11, fontweight='bold')
        ax1.set_title('Films by Award Category', fontsize=12, fontweight='bold')
        ax1.grid(axis='x', alpha=0.3)

        # Add value labels
        for i, (bar, val) in enumerate(zip(bars, award_counts.values())):
            pct = (val / len(self.df)) * 100
            ax1.text(val + 10, i, f'{val} ({pct:.1f}%)', va='center', fontsize=9)

        # 2. Oscar wins distribution
        ax2 = axes[0, 1]
        oscar_winners = self.df[self.df['oscar_wins'] > 0]
        if len(oscar_winners) > 0:
            oscar_dist = oscar_winners['oscar_wins'].value_counts().sort_index()
            ax2.bar(oscar_dist.index, oscar_dist.values, color='gold', edgecolor='black')
            ax2.set_xlabel('Number of Oscars Won', fontsize=11, fontweight='bold')
            ax2.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
            ax2.set_title(f'Oscar Wins Distribution ({len(oscar_winners)} winners)',
                         fontsize=12, fontweight='bold')
            ax2.grid(axis='y', alpha=0.3)
        else:
            ax2.text(0.5, 0.5, 'No Oscar Winners Found', ha='center', va='center',
                    transform=ax2.transAxes, fontsize=12)
            ax2.set_title('Oscar Wins Distribution', fontsize=12, fontweight='bold')

        # 3. Most awarded films
        ax3 = axes[1, 0]
        top_awarded = self.df.nlargest(15, 'total_wins')[['title', 'total_wins', 'oscar_wins']]
        if len(top_awarded) > 0:
            y_pos = np.arange(len(top_awarded))
            ax3.barh(y_pos, top_awarded['total_wins'].values, color='skyblue', edgecolor='black')
            ax3.set_yticks(y_pos)
            ax3.set_yticklabels([f"{title[:30]}..." if len(title) > 30 else title
                                for title in top_awarded['title'].values], fontsize=9)
            ax3.set_xlabel('Total Wins', fontsize=11, fontweight='bold')
            ax3.set_title('Most Awarded Films (All Ceremonies)', fontsize=12, fontweight='bold')
            ax3.invert_yaxis()
            ax3.grid(axis='x', alpha=0.3)

            # Highlight Oscar wins
            for i, (wins, oscar_wins) in enumerate(zip(top_awarded['total_wins'].values,
                                                       top_awarded['oscar_wins'].values)):
                if oscar_wins > 0:
                    ax3.text(wins + 0.5, i, f'{oscar_wins} Oscars', va='center',
                            fontsize=8, color='gold', fontweight='bold')
        else:
            ax3.text(0.5, 0.5, 'No Award Data Available', ha='center', va='center',
                    transform=ax3.transAxes, fontsize=12)
            ax3.set_title('Most Awarded Films', fontsize=12, fontweight='bold')

        # 4. Awards by decade
        ax4 = axes[1, 1]
        if 'release_year' in self.df.columns:
            self.df['decade'] = (self.df['release_year'] // 10) * 10
            decade_awards = self.df.groupby('decade').agg({
                'oscar_wins': 'sum',
                'has_bafta': 'sum',
                'has_golden_globe': 'sum',
                'has_festival_award': 'sum'
            }).sort_index()

            if len(decade_awards) > 0:
                x = np.arange(len(decade_awards))
                width = 0.2

                ax4.bar(x - 1.5*width, decade_awards['oscar_wins'], width,
                       label='Oscar Wins', color='gold')
                ax4.bar(x - 0.5*width, decade_awards['has_bafta'], width,
                       label='BAFTA', color='#CD7F32')
                ax4.bar(x + 0.5*width, decade_awards['has_golden_globe'], width,
                       label='Golden Globe', color='#FFD700')
                ax4.bar(x + 1.5*width, decade_awards['has_festival_award'], width,
                       label='Festival', color='#E74C3C')

                ax4.set_xlabel('Decade', fontsize=11, fontweight='bold')
                ax4.set_ylabel('Award Count', fontsize=11, fontweight='bold')
                ax4.set_title('Awards by Decade', fontsize=12, fontweight='bold')
                ax4.set_xticks(x)
                ax4.set_xticklabels([f"{int(d)}s" for d in decade_awards.index], rotation=45)
                ax4.legend(fontsize=9)
                ax4.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        output_path = self.output_dir / 'awards_overview.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_oscar_analysis(self):
        """Visualization 2: Deep dive into Oscar performance."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Oscar Performance Deep-Dive', fontsize=16, fontweight='bold')

        # 1. Oscar wins vs nominations
        ax1 = axes[0, 0]
        oscar_films = self.df[(self.df['oscar_wins'] > 0) | (self.df['oscar_noms'] > 0)]
        if len(oscar_films) > 0:
            scatter = ax1.scatter(oscar_films['oscar_noms'], oscar_films['oscar_wins'],
                                 c=oscar_films['imdb_rating'], cmap='YlOrRd',
                                 s=100, alpha=0.6, edgecolors='black')
            ax1.set_xlabel('Oscar Nominations', fontsize=11, fontweight='bold')
            ax1.set_ylabel('Oscar Wins', fontsize=11, fontweight='bold')
            ax1.set_title(f'Oscar Wins vs Nominations ({len(oscar_films)} films)',
                         fontsize=12, fontweight='bold')
            ax1.grid(True, alpha=0.3)

            cbar = plt.colorbar(scatter, ax=ax1)
            cbar.set_label('IMDb Rating', fontsize=10)

            # Add diagonal line
            max_val = max(oscar_films['oscar_noms'].max(), oscar_films['oscar_wins'].max())
            ax1.plot([0, max_val], [0, max_val], 'r--', alpha=0.5, label='Win rate = 100%')
            ax1.legend(fontsize=9)

        # 2. Top Oscar winners
        ax2 = axes[0, 1]
        top_oscar = self.df[self.df['oscar_wins'] > 0].nlargest(12, 'oscar_wins')
        if len(top_oscar) > 0:
            y_pos = np.arange(len(top_oscar))
            bars = ax2.barh(y_pos, top_oscar['oscar_wins'].values, color='gold', edgecolor='black')
            ax2.set_yticks(y_pos)
            ax2.set_yticklabels([f"{title[:35]}..." if len(title) > 35 else title
                                for title in top_oscar['title'].values], fontsize=9)
            ax2.set_xlabel('Oscar Wins', fontsize=11, fontweight='bold')
            ax2.set_title('Most Oscar-Winning Films', fontsize=12, fontweight='bold')
            ax2.invert_yaxis()
            ax2.grid(axis='x', alpha=0.3)

            # Add nomination count
            for i, (wins, noms) in enumerate(zip(top_oscar['oscar_wins'].values,
                                                 top_oscar['oscar_noms'].values)):
                ax2.text(wins + 0.2, i, f'({noms} noms)', va='center', fontsize=8)

        # 3. Oscar nominees that didn't win
        ax3 = axes[1, 0]
        nominees_no_win = self.df[(self.df['oscar_noms'] > 0) & (self.df['oscar_wins'] == 0)]
        if len(nominees_no_win) > 0:
            top_losers = nominees_no_win.nlargest(12, 'oscar_noms')
            y_pos = np.arange(len(top_losers))
            bars = ax3.barh(y_pos, top_losers['oscar_noms'].values, color='silver', edgecolor='black')
            ax3.set_yticks(y_pos)
            ax3.set_yticklabels([f"{title[:35]}..." if len(title) > 35 else title
                                for title in top_losers['title'].values], fontsize=9)
            ax3.set_xlabel('Oscar Nominations', fontsize=11, fontweight='bold')
            ax3.set_title('Most Nominated Without Wins', fontsize=12, fontweight='bold')
            ax3.invert_yaxis()
            ax3.grid(axis='x', alpha=0.3)

            # Add ratings
            for i, rating in enumerate(top_losers['imdb_rating'].values):
                if not pd.isna(rating) and rating > 0:
                    ax3.text(top_losers['oscar_noms'].values[i] + 0.2, i,
                            f'IMDb: {rating:.1f}', va='center', fontsize=8)

        # 4. Oscar win rate by genre
        ax4 = axes[1, 1]
        if 'tmdb_genres' in self.df.columns:
            genre_oscar_data = []

            for idx, row in self.df.iterrows():
                if pd.notna(row['tmdb_genres']):
                    try:
                        import ast
                        genres = ast.literal_eval(row['tmdb_genres'])
                        if isinstance(genres, list):
                            for genre in genres:
                                genre_oscar_data.append({
                                    'genre': genre,
                                    'has_oscar': row['oscar_wins'] > 0,
                                    'oscar_wins': row['oscar_wins']
                                })
                    except:
                        pass

            if genre_oscar_data:
                genre_df = pd.DataFrame(genre_oscar_data)
                genre_stats = genre_df.groupby('genre').agg({
                    'has_oscar': ['sum', 'count'],
                    'oscar_wins': 'sum'
                })
                genre_stats.columns = ['oscar_films', 'total_films', 'total_oscars']
                genre_stats = genre_stats[genre_stats['total_films'] >= 10]
                genre_stats['win_rate'] = (genre_stats['oscar_films'] / genre_stats['total_films']) * 100
                genre_stats = genre_stats.sort_values('win_rate', ascending=True).tail(12)

                y_pos = np.arange(len(genre_stats))
                bars = ax4.barh(y_pos, genre_stats['win_rate'].values,
                               color=plt.cm.YlOrRd(genre_stats['win_rate'].values / 100))
                ax4.set_yticks(y_pos)
                ax4.set_yticklabels(genre_stats.index, fontsize=10)
                ax4.set_xlabel('% of Films with Oscars', fontsize=11, fontweight='bold')
                ax4.set_title('Oscar Win Rate by Genre (min 10 films)', fontsize=12, fontweight='bold')
                ax4.grid(axis='x', alpha=0.3)

                for i, (rate, count) in enumerate(zip(genre_stats['win_rate'].values,
                                                     genre_stats['total_films'].values)):
                    ax4.text(rate + 0.5, i, f'{rate:.1f}% (n={int(count)})',
                            va='center', fontsize=8)

        plt.tight_layout()
        output_path = self.output_dir / 'oscar_deep_dive.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_bafta_analysis(self):
        """Visualization 3: BAFTA awards analysis."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('BAFTA Awards Analysis', fontsize=16, fontweight='bold')

        # 1. BAFTA vs Oscar comparison
        ax1 = axes[0, 0]
        bafta_oscar_overlap = pd.crosstab(
            self.df['has_bafta'],
            self.df['has_oscar'],
            margins=True
        )

        categories = ['BAFTA Only', 'Oscar Only', 'Both', 'Neither']
        values = [
            ((self.df['has_bafta']) & (~self.df['has_oscar'])).sum(),
            ((~self.df['has_bafta']) & (self.df['has_oscar'])).sum(),
            ((self.df['has_bafta']) & (self.df['has_oscar'])).sum(),
            ((~self.df['has_bafta']) & (~self.df['has_oscar'])).sum()
        ]

        colors_pie = ['#CD7F32', 'gold', '#FFD700', '#E0E0E0']
        wedges, texts, autotexts = ax1.pie(values, labels=categories, autopct='%1.1f%%',
                                           colors=colors_pie, startangle=90)
        ax1.set_title('BAFTA vs Oscar Distribution', fontsize=12, fontweight='bold')

        for autotext in autotexts:
            autotext.set_color('black')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(10)

        # 2. Top BAFTA films
        ax2 = axes[0, 1]
        bafta_films = self.df[self.df['has_bafta']]
        if len(bafta_films) > 0:
            top_bafta = bafta_films.nlargest(12, 'bafta_noms')
            if top_bafta['bafta_noms'].sum() > 0:
                y_pos = np.arange(len(top_bafta))
                bars = ax2.barh(y_pos, top_bafta['bafta_noms'].values,
                               color='#CD7F32', edgecolor='black')
                ax2.set_yticks(y_pos)
                ax2.set_yticklabels([f"{title[:35]}..." if len(title) > 35 else title
                                    for title in top_bafta['title'].values], fontsize=9)
                ax2.set_xlabel('BAFTA Nominations', fontsize=11, fontweight='bold')
                ax2.set_title('Top BAFTA Nominated Films', fontsize=12, fontweight='bold')
                ax2.invert_yaxis()
                ax2.grid(axis='x', alpha=0.3)
            else:
                ax2.text(0.5, 0.5, 'BAFTA nomination counts not available',
                        ha='center', va='center', transform=ax2.transAxes, fontsize=11)

        # 3. BAFTA by decade
        ax3 = axes[1, 0]
        if 'decade' in self.df.columns:
            decade_bafta = self.df.groupby('decade')['has_bafta'].sum().sort_index()
            decade_total = self.df.groupby('decade').size().sort_index()
            decade_pct = (decade_bafta / decade_total * 100)

            x = np.arange(len(decade_bafta))
            bars = ax3.bar(x, decade_bafta.values, color='#CD7F32', edgecolor='black')
            ax3.set_xticks(x)
            ax3.set_xticklabels([f"{int(d)}s" for d in decade_bafta.index], rotation=45)
            ax3.set_xlabel('Decade', fontsize=11, fontweight='bold')
            ax3.set_ylabel('BAFTA Films', fontsize=11, fontweight='bold')
            ax3.set_title('BAFTA Awards by Decade', fontsize=12, fontweight='bold')
            ax3.grid(axis='y', alpha=0.3)

            # Add percentage labels
            for i, (count, pct) in enumerate(zip(decade_bafta.values, decade_pct.values)):
                ax3.text(i, count + 0.5, f'{pct:.1f}%', ha='center', fontsize=8)

        # 4. BAFTA vs rating
        ax4 = axes[1, 1]
        bafta_ratings = self.df[self.df['has_bafta'] & (self.df['imdb_rating'] > 0)]['imdb_rating']
        no_bafta_ratings = self.df[(~self.df['has_bafta']) & (self.df['imdb_rating'] > 0)]['imdb_rating']

        if len(bafta_ratings) > 0 and len(no_bafta_ratings) > 0:
            box_data = [no_bafta_ratings, bafta_ratings]
            bp = ax4.boxplot(box_data, tick_labels=['No BAFTA', 'BAFTA'],
                            patch_artist=True, notch=True)

            for patch, color in zip(bp['boxes'], ['lightgray', '#CD7F32']):
                patch.set_facecolor(color)

            ax4.set_ylabel('IMDb Rating', fontsize=11, fontweight='bold')
            ax4.set_title('Rating Distribution: BAFTA vs Non-BAFTA', fontsize=12, fontweight='bold')
            ax4.grid(axis='y', alpha=0.3)

            # Add statistical test
            t_stat, p_value = stats.ttest_ind(bafta_ratings, no_bafta_ratings)
            ax4.text(0.05, 0.95, f'BAFTA mean: {bafta_ratings.mean():.2f}\n'
                                f'Non-BAFTA mean: {no_bafta_ratings.mean():.2f}\n'
                                f'p-value: {p_value:.4f}',
                    transform=ax4.transAxes, fontsize=9, verticalalignment='top',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        plt.tight_layout()
        output_path = self.output_dir / 'bafta_analysis.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_golden_globe_analysis(self):
        """Visualization 4: Golden Globe awards analysis."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Golden Globe Awards Analysis', fontsize=16, fontweight='bold')

        # 1. Golden Globe vs Oscar overlap
        ax1 = axes[0, 0]
        categories = ['GG Only', 'Oscar Only', 'Both', 'Neither']
        values = [
            ((self.df['has_golden_globe']) & (~self.df['has_oscar'])).sum(),
            ((~self.df['has_golden_globe']) & (self.df['has_oscar'])).sum(),
            ((self.df['has_golden_globe']) & (self.df['has_oscar'])).sum(),
            ((~self.df['has_golden_globe']) & (~self.df['has_oscar'])).sum()
        ]

        colors_pie = ['#FFD700', 'gold', 'darkgoldenrod', '#E0E0E0']
        wedges, texts, autotexts = ax1.pie(values, labels=categories, autopct='%1.1f%%',
                                           colors=colors_pie, startangle=90)
        ax1.set_title('Golden Globe vs Oscar Distribution', fontsize=12, fontweight='bold')

        for autotext in autotexts:
            autotext.set_color('black')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(10)

        # 2. Top Golden Globe films (by rating)
        ax2 = axes[0, 1]
        gg_films = self.df[self.df['has_golden_globe'] & (self.df['imdb_rating'] > 0)]
        if len(gg_films) > 0:
            top_gg = gg_films.nlargest(12, 'imdb_rating')
            y_pos = np.arange(len(top_gg))
            bars = ax2.barh(y_pos, top_gg['imdb_rating'].values,
                           color='#FFD700', edgecolor='black')
            ax2.set_yticks(y_pos)
            ax2.set_yticklabels([f"{title[:35]}..." if len(title) > 35 else title
                                for title in top_gg['title'].values], fontsize=9)
            ax2.set_xlabel('IMDb Rating', fontsize=11, fontweight='bold')
            ax2.set_title('Top-Rated Golden Globe Films', fontsize=12, fontweight='bold')
            ax2.invert_yaxis()
            ax2.grid(axis='x', alpha=0.3)
            ax2.set_xlim(0, 10)

            # Add Oscar indicator
            for i, (rating, has_oscar) in enumerate(zip(top_gg['imdb_rating'].values,
                                                        top_gg['has_oscar'].values)):
                if has_oscar:
                    ax2.text(rating - 0.3, i, '', va='center', fontsize=12, color='gold')

        # 3. Golden Globe by decade
        ax3 = axes[1, 0]
        if 'decade' in self.df.columns:
            decade_gg = self.df.groupby('decade')['has_golden_globe'].sum().sort_index()
            decade_total = self.df.groupby('decade').size().sort_index()
            decade_pct = (decade_gg / decade_total * 100)

            x = np.arange(len(decade_gg))
            bars = ax3.bar(x, decade_gg.values, color='#FFD700', edgecolor='black')
            ax3.set_xticks(x)
            ax3.set_xticklabels([f"{int(d)}s" for d in decade_gg.index], rotation=45)
            ax3.set_xlabel('Decade', fontsize=11, fontweight='bold')
            ax3.set_ylabel('Golden Globe Films', fontsize=11, fontweight='bold')
            ax3.set_title('Golden Globe Awards by Decade', fontsize=12, fontweight='bold')
            ax3.grid(axis='y', alpha=0.3)

        # 4. Three-way comparison
        ax4 = axes[1, 1]
        oscar_gg = self.df[(self.df['has_oscar']) & (self.df['has_golden_globe']) &
                          (self.df['imdb_rating'] > 0)]['imdb_rating']
        gg_only = self.df[(self.df['has_golden_globe']) & (~self.df['has_oscar']) &
                         (self.df['imdb_rating'] > 0)]['imdb_rating']
        neither = self.df[(~self.df['has_golden_globe']) & (~self.df['has_oscar']) &
                         (self.df['imdb_rating'] > 0)]['imdb_rating']

        if len(oscar_gg) > 0 and len(gg_only) > 0 and len(neither) > 0:
            box_data = [neither, gg_only, oscar_gg]
            bp = ax4.boxplot(box_data, tick_labels=['Neither', 'GG Only', 'Both'],
                            patch_artist=True, notch=True)

            for patch, color in zip(bp['boxes'], ['lightgray', '#FFD700', 'gold']):
                patch.set_facecolor(color)

            ax4.set_ylabel('IMDb Rating', fontsize=11, fontweight='bold')
            ax4.set_title('Rating by Award Category', fontsize=12, fontweight='bold')
            ax4.grid(axis='y', alpha=0.3)

            # Add means
            means = [neither.mean(), gg_only.mean(), oscar_gg.mean()]
            ax4.plot([1, 2, 3], means, 'ro-', linewidth=2, markersize=8, label='Mean')
            ax4.legend(fontsize=9)

        plt.tight_layout()
        output_path = self.output_dir / 'golden_globe_analysis.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_festival_awards(self):
        """Visualization 5: Film festival awards (Cannes, Venice, Berlin)."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Major Film Festival Awards', fontsize=16, fontweight='bold')

        # 1. Festival award distribution
        ax1 = axes[0, 0]
        festival_films = self.df[self.df['has_festival_award']]

        if len(festival_films) > 0:
            # Count mentions of each festival
            festival_counts = {
                'Cannes/Palme': 0,
                'Venice/Lion': 0,
                'Berlin/Bear': 0,
                'Other Festival': 0
            }

            for awards_text in festival_films['omdb_awards']:
                if pd.notna(awards_text):
                    awards_str = str(awards_text)
                    if 'Cannes' in awards_str or 'Palme' in awards_str:
                        festival_counts['Cannes/Palme'] += 1
                    elif 'Venice' in awards_str or 'Lion' in awards_str:
                        festival_counts['Venice/Lion'] += 1
                    elif 'Berlin' in awards_str or 'Bear' in awards_str:
                        festival_counts['Berlin/Bear'] += 1
                    else:
                        festival_counts['Other Festival'] += 1

            colors_pie = ['#E74C3C', '#3498DB', '#2ECC71', '#95A5A6']
            wedges, texts, autotexts = ax1.pie(festival_counts.values(),
                                               labels=festival_counts.keys(),
                                               autopct='%1.1f%%', colors=colors_pie,
                                               startangle=90)
            ax1.set_title(f'Festival Award Distribution ({len(festival_films)} films)',
                         fontsize=12, fontweight='bold')

            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontweight('bold')
                autotext.set_fontsize(10)
        else:
            ax1.text(0.5, 0.5, 'No Festival Awards Found', ha='center', va='center',
                    transform=ax1.transAxes, fontsize=12)
            ax1.set_title('Festival Award Distribution', fontsize=12, fontweight='bold')

        # 2. Festival vs Oscar overlap
        ax2 = axes[0, 1]
        categories = ['Festival Only', 'Oscar Only', 'Both', 'Neither']
        values = [
            ((self.df['has_festival_award']) & (~self.df['has_oscar'])).sum(),
            ((~self.df['has_festival_award']) & (self.df['has_oscar'])).sum(),
            ((self.df['has_festival_award']) & (self.df['has_oscar'])).sum(),
            ((~self.df['has_festival_award']) & (~self.df['has_oscar'])).sum()
        ]

        colors_bar = ['#E74C3C', 'gold', '#FF6347', '#E0E0E0']
        bars = ax2.bar(categories, values, color=colors_bar, edgecolor='black')
        ax2.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
        ax2.set_title('Festival vs Oscar Awards', fontsize=12, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        for bar, val in zip(bars, values):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + 5,
                    f'{val}\n({val/len(self.df)*100:.1f}%)',
                    ha='center', va='bottom', fontsize=9)

        # 3. Top-rated festival films
        ax3 = axes[1, 0]
        if len(festival_films) > 0:
            top_festival = festival_films[festival_films['imdb_rating'] > 0].nlargest(12, 'imdb_rating')
            if len(top_festival) > 0:
                y_pos = np.arange(len(top_festival))
                bars = ax3.barh(y_pos, top_festival['imdb_rating'].values,
                               color='#E74C3C', edgecolor='black')
                ax3.set_yticks(y_pos)
                ax3.set_yticklabels([f"{title[:35]}..." if len(title) > 35 else title
                                    for title in top_festival['title'].values], fontsize=9)
                ax3.set_xlabel('IMDb Rating', fontsize=11, fontweight='bold')
                ax3.set_title('Top-Rated Festival Award Winners', fontsize=12, fontweight='bold')
                ax3.invert_yaxis()
                ax3.grid(axis='x', alpha=0.3)
                ax3.set_xlim(0, 10)

        # 4. Festival awards by genre
        ax4 = axes[1, 1]
        if 'tmdb_genres' in self.df.columns and len(festival_films) > 0:
            genre_festival_data = []

            for idx, row in self.df.iterrows():
                if pd.notna(row['tmdb_genres']):
                    try:
                        import ast
                        genres = ast.literal_eval(row['tmdb_genres'])
                        if isinstance(genres, list):
                            for genre in genres:
                                genre_festival_data.append({
                                    'genre': genre,
                                    'has_festival': row['has_festival_award']
                                })
                    except:
                        pass

            if genre_festival_data:
                genre_df = pd.DataFrame(genre_festival_data)
                genre_stats = genre_df.groupby('genre').agg({
                    'has_festival': ['sum', 'count']
                })
                genre_stats.columns = ['festival_films', 'total_films']
                genre_stats = genre_stats[genre_stats['total_films'] >= 10]
                genre_stats['festival_rate'] = (genre_stats['festival_films'] /
                                                genre_stats['total_films']) * 100
                genre_stats = genre_stats.sort_values('festival_rate', ascending=True).tail(10)

                y_pos = np.arange(len(genre_stats))
                bars = ax4.barh(y_pos, genre_stats['festival_rate'].values,
                               color=plt.cm.RdYlGn(genre_stats['festival_rate'].values / 100))
                ax4.set_yticks(y_pos)
                ax4.set_yticklabels(genre_stats.index, fontsize=10)
                ax4.set_xlabel('% with Festival Awards', fontsize=11, fontweight='bold')
                ax4.set_title('Festival Award Rate by Genre (min 10 films)',
                             fontsize=12, fontweight='bold')
                ax4.grid(axis='x', alpha=0.3)

                for i, (rate, count) in enumerate(zip(genre_stats['festival_rate'].values,
                                                     genre_stats['total_films'].values)):
                    ax4.text(rate + 0.3, i, f'{rate:.1f}% (n={int(count)})',
                            va='center', fontsize=8)

        plt.tight_layout()
        output_path = self.output_dir / 'festival_awards.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_sag_critics_awards(self):
        """Visualization 6: SAG and Critics' Choice awards."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('SAG & Critics Awards Analysis', fontsize=16, fontweight='bold')

        # 1. SAG award presence
        ax1 = axes[0, 0]
        sag_counts = {
            'Has SAG': self.df['has_sag'].sum(),
            'No SAG': (~self.df['has_sag']).sum()
        }

        colors_pie = ['#3498DB', '#E0E0E0']
        wedges, texts, autotexts = ax1.pie(sag_counts.values(), labels=sag_counts.keys(),
                                           autopct='%1.1f%%', colors=colors_pie,
                                           startangle=90)
        ax1.set_title(f'SAG Award Distribution ({len(self.df)} films)',
                     fontsize=12, fontweight='bold')

        for autotext in autotexts:
            autotext.set_color('black')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(11)

        # 2. SAG vs Oscar correlation
        ax2 = axes[0, 1]
        categories = ['SAG Only', 'Oscar Only', 'Both', 'Neither']
        values = [
            ((self.df['has_sag']) & (~self.df['has_oscar'])).sum(),
            ((~self.df['has_sag']) & (self.df['has_oscar'])).sum(),
            ((self.df['has_sag']) & (self.df['has_oscar'])).sum(),
            ((~self.df['has_sag']) & (~self.df['has_oscar'])).sum()
        ]

        colors_bar = ['#3498DB', 'gold', '#5DADE2', '#E0E0E0']
        bars = ax2.bar(categories, values, color=colors_bar, edgecolor='black')
        ax2.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
        ax2.set_title('SAG vs Oscar Awards', fontsize=12, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        for bar, val in zip(bars, values):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + 3,
                    f'{val}', ha='center', va='bottom', fontsize=10, fontweight='bold')

        # 3. Top SAG films by rating
        ax3 = axes[1, 0]
        sag_films = self.df[self.df['has_sag'] & (self.df['imdb_rating'] > 0)]
        if len(sag_films) > 0:
            top_sag = sag_films.nlargest(12, 'imdb_rating')
            y_pos = np.arange(len(top_sag))
            bars = ax3.barh(y_pos, top_sag['imdb_rating'].values,
                           color='#3498DB', edgecolor='black')
            ax3.set_yticks(y_pos)
            ax3.set_yticklabels([f"{title[:35]}..." if len(title) > 35 else title
                                for title in top_sag['title'].values], fontsize=9)
            ax3.set_xlabel('IMDb Rating', fontsize=11, fontweight='bold')
            ax3.set_title('Top-Rated SAG Award Films', fontsize=12, fontweight='bold')
            ax3.invert_yaxis()
            ax3.grid(axis='x', alpha=0.3)
            ax3.set_xlim(0, 10)
        else:
            ax3.text(0.5, 0.5, 'No SAG Award Films Found', ha='center', va='center',
                    transform=ax3.transAxes, fontsize=12)
            ax3.set_title('Top-Rated SAG Award Films', fontsize=12, fontweight='bold')

        # 4. Award combination heatmap
        ax4 = axes[1, 1]
        award_combos = pd.DataFrame({
            'Oscar': self.df['has_oscar'].astype(int),
            'BAFTA': self.df['has_bafta'].astype(int),
            'Golden Globe': self.df['has_golden_globe'].astype(int),
            'Festival': self.df['has_festival_award'].astype(int),
            'SAG': self.df['has_sag'].astype(int)
        })

        corr_matrix = award_combos.corr()

        im = ax4.imshow(corr_matrix, cmap='YlOrRd', aspect='auto', vmin=0, vmax=1)
        ax4.set_xticks(np.arange(len(corr_matrix.columns)))
        ax4.set_yticks(np.arange(len(corr_matrix.columns)))
        ax4.set_xticklabels(corr_matrix.columns, rotation=45, ha='right', fontsize=10)
        ax4.set_yticklabels(corr_matrix.columns, fontsize=10)
        ax4.set_title('Award Correlation Matrix', fontsize=12, fontweight='bold')

        # Add correlation values
        for i in range(len(corr_matrix)):
            for j in range(len(corr_matrix)):
                text = ax4.text(j, i, f'{corr_matrix.iloc[i, j]:.2f}',
                               ha="center", va="center", color="black", fontsize=9,
                               fontweight='bold')

        cbar = plt.colorbar(im, ax=ax4)
        cbar.set_label('Correlation Coefficient', fontsize=10)

        plt.tight_layout()
        output_path = self.output_dir / 'sag_critics_awards.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_award_sweeps(self):
        """Visualization 7: Films that swept multiple major awards."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Award Sweep Analysis', fontsize=16, fontweight='bold')

        # Calculate award diversity score
        self.df['award_diversity'] = (
            self.df['has_oscar'].astype(int) +
            self.df['has_bafta'].astype(int) +
            self.df['has_golden_globe'].astype(int) +
            self.df['has_festival_award'].astype(int) +
            self.df['has_sag'].astype(int)
        )

        # 1. Award diversity distribution
        ax1 = axes[0, 0]
        diversity_counts = self.df['award_diversity'].value_counts().sort_index()

        colors_bar = ['#E0E0E0', '#D5DBDB', '#AEB6BF', '#85929E', '#5D6D7E', '#34495E']
        bars = ax1.bar(diversity_counts.index, diversity_counts.values,
                      color=[colors_bar[i] if i < len(colors_bar) else colors_bar[-1]
                            for i in diversity_counts.index],
                      edgecolor='black')
        ax1.set_xlabel('Number of Award Types', fontsize=11, fontweight='bold')
        ax1.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
        ax1.set_title('Award Diversity Distribution', fontsize=12, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)

        # Add percentages
        for bar, val in zip(bars, diversity_counts.values):
            height = bar.get_height()
            pct = (val / len(self.df)) * 100
            ax1.text(bar.get_x() + bar.get_width()/2., height + 5,
                    f'{val}\n({pct:.1f}%)', ha='center', va='bottom', fontsize=9)

        # 2. Top award sweep films
        ax2 = axes[0, 1]
        sweep_films = self.df[self.df['award_diversity'] >= 3]
        if len(sweep_films) > 0:
            top_sweeps = sweep_films.nlargest(12, 'award_diversity')
            y_pos = np.arange(len(top_sweeps))
            bars = ax2.barh(y_pos, top_sweeps['award_diversity'].values,
                           color=plt.cm.YlOrRd(top_sweeps['award_diversity'].values / 5),
                           edgecolor='black')
            ax2.set_yticks(y_pos)
            ax2.set_yticklabels([f"{title[:30]}..." if len(title) > 30 else title
                                for title in top_sweeps['title'].values], fontsize=9)
            ax2.set_xlabel('Award Types Won', fontsize=11, fontweight='bold')
            ax2.set_title(f'Films with Most Award Types ({len(sweep_films)} films with 3+)',
                         fontsize=12, fontweight='bold')
            ax2.invert_yaxis()
            ax2.grid(axis='x', alpha=0.3)
            ax2.set_xticks([0, 1, 2, 3, 4, 5])

            # Add ratings
            for i, rating in enumerate(top_sweeps['imdb_rating'].values):
                if not pd.isna(rating) and rating > 0:
                    ax2.text(top_sweeps['award_diversity'].values[i] + 0.1, i,
                            f'{rating:.1f}⭐', va='center', fontsize=8)
        else:
            ax2.text(0.5, 0.5, 'No films with 3+ award types', ha='center', va='center',
                    transform=ax2.transAxes, fontsize=12)
            ax2.set_title('Films with Most Award Types', fontsize=12, fontweight='bold')

        # 3. Award diversity vs rating
        ax3 = axes[1, 0]
        diversity_ratings = []
        for diversity in range(6):
            ratings = self.df[(self.df['award_diversity'] == diversity) &
                            (self.df['imdb_rating'] > 0)]['imdb_rating']
            if len(ratings) > 0:
                diversity_ratings.append(ratings)

        if len(diversity_ratings) > 0:
            bp = ax3.boxplot(diversity_ratings, tick_labels=range(len(diversity_ratings)),
                            patch_artist=True, notch=True)

            colors_box = plt.cm.YlOrRd(np.linspace(0.2, 0.9, len(diversity_ratings)))
            for patch, color in zip(bp['boxes'], colors_box):
                patch.set_facecolor(color)

            ax3.set_xlabel('Number of Award Types', fontsize=11, fontweight='bold')
            ax3.set_ylabel('IMDb Rating', fontsize=11, fontweight='bold')
            ax3.set_title('Rating vs Award Diversity', fontsize=12, fontweight='bold')
            ax3.grid(axis='y', alpha=0.3)

            # Add mean line
            means = [ratings.mean() for ratings in diversity_ratings]
            ax3.plot(range(1, len(means) + 1), means, 'ro-', linewidth=2,
                    markersize=8, label='Mean', zorder=10)
            ax3.legend(fontsize=9)

        # 4. Award sweep by decade
        ax4 = axes[1, 1]
        if 'decade' in self.df.columns:
            decade_sweep = self.df[self.df['award_diversity'] >= 3].groupby('decade').size()
            decade_total = self.df.groupby('decade').size()
            decade_pct = (decade_sweep / decade_total * 100).fillna(0)

            x = np.arange(len(decade_total))
            bars = ax4.bar(x, decade_sweep.reindex(decade_total.index, fill_value=0).values,
                          color='#E74C3C', edgecolor='black')
            ax4.set_xticks(x)
            ax4.set_xticklabels([f"{int(d)}s" for d in decade_total.index], rotation=45)
            ax4.set_xlabel('Decade', fontsize=11, fontweight='bold')
            ax4.set_ylabel('Films with 3+ Award Types', fontsize=11, fontweight='bold')
            ax4.set_title('Award Sweeps by Decade', fontsize=12, fontweight='bold')
            ax4.grid(axis='y', alpha=0.3)

            # Add percentage labels
            for i, (count, pct) in enumerate(zip(decade_sweep.reindex(decade_total.index, fill_value=0).values,
                                                 decade_pct.reindex(decade_total.index, fill_value=0).values)):
                if count > 0:
                    ax4.text(i, count + 0.3, f'{pct:.1f}%', ha='center', fontsize=8)

        plt.tight_layout()
        output_path = self.output_dir / 'award_sweeps.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_award_success_factors(self):
        """Visualization 8: Factors correlated with award success."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Award Success Factors', fontsize=16, fontweight='bold')

        # 1. Rating vs total awards
        ax1 = axes[0, 0]
        rated_films = self.df[(self.df['imdb_rating'] > 0) & (self.df['total_wins'] > 0)]
        if len(rated_films) > 0:
            scatter = ax1.scatter(rated_films['imdb_rating'], rated_films['total_wins'],
                                 c=rated_films['oscar_wins'], cmap='YlOrRd',
                                 s=100, alpha=0.6, edgecolors='black')
            ax1.set_xlabel('IMDb Rating', fontsize=11, fontweight='bold')
            ax1.set_ylabel('Total Wins', fontsize=11, fontweight='bold')
            ax1.set_title('Rating vs Award Success', fontsize=12, fontweight='bold')
            ax1.grid(True, alpha=0.3)

            cbar = plt.colorbar(scatter, ax=ax1)
            cbar.set_label('Oscar Wins', fontsize=10)

            # Add correlation
            if len(rated_films) >= 2:
                corr, p_val = stats.pearsonr(rated_films['imdb_rating'],
                                            rated_films['total_wins'])
                ax1.text(0.05, 0.95, f'Correlation: {corr:.3f}\np-value: {p_val:.4f}',
                        transform=ax1.transAxes, fontsize=10, verticalalignment='top',
                        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        # 2. Genre success rates
        ax2 = axes[0, 1]
        if 'tmdb_genres' in self.df.columns:
            genre_award_data = []

            for idx, row in self.df.iterrows():
                if pd.notna(row['tmdb_genres']):
                    try:
                        import ast
                        genres = ast.literal_eval(row['tmdb_genres'])
                        if isinstance(genres, list):
                            for genre in genres:
                                genre_award_data.append({
                                    'genre': genre,
                                    'has_major_award': row['award_diversity'] >= 1,
                                    'total_wins': row['total_wins']
                                })
                    except:
                        pass

            if genre_award_data:
                genre_df = pd.DataFrame(genre_award_data)
                genre_stats = genre_df.groupby('genre').agg({
                    'has_major_award': ['sum', 'count'],
                    'total_wins': 'sum'
                })
                genre_stats.columns = ['award_films', 'total_films', 'total_wins']
                genre_stats = genre_stats[genre_stats['total_films'] >= 15]
                genre_stats['award_rate'] = (genre_stats['award_films'] /
                                            genre_stats['total_films']) * 100
                genre_stats = genre_stats.sort_values('award_rate', ascending=True).tail(12)

                y_pos = np.arange(len(genre_stats))
                bars = ax2.barh(y_pos, genre_stats['award_rate'].values,
                               color=plt.cm.RdYlGn(genre_stats['award_rate'].values / 100))
                ax2.set_yticks(y_pos)
                ax2.set_yticklabels(genre_stats.index, fontsize=10)
                ax2.set_xlabel('% with Major Awards', fontsize=11, fontweight='bold')
                ax2.set_title('Award Success Rate by Genre (min 15 films)',
                             fontsize=12, fontweight='bold')
                ax2.grid(axis='x', alpha=0.3)

                for i, (rate, wins) in enumerate(zip(genre_stats['award_rate'].values,
                                                    genre_stats['total_wins'].values)):
                    ax2.text(rate + 1, i, f'{rate:.1f}% ({int(wins)} wins)',
                            va='center', fontsize=8)

        # 3. Runtime vs awards
        ax3 = axes[1, 0]
        if 'runtime' in self.df.columns:
            runtime_award = self.df[(self.df['runtime'] > 0) & (self.df['total_wins'] > 0)]
            runtime_no_award = self.df[(self.df['runtime'] > 0) & (self.df['total_wins'] == 0)]

            if len(runtime_award) > 0 and len(runtime_no_award) > 0:
                box_data = [runtime_no_award['runtime'], runtime_award['runtime']]
                bp = ax3.boxplot(box_data, tick_labels=['No Awards', 'Has Awards'],
                                patch_artist=True, notch=True)

                for patch, color in zip(bp['boxes'], ['lightgray', 'gold']):
                    patch.set_facecolor(color)

                ax3.set_ylabel('Runtime (minutes)', fontsize=11, fontweight='bold')
                ax3.set_title('Runtime: Award Winners vs Others', fontsize=12, fontweight='bold')
                ax3.grid(axis='y', alpha=0.3)

                # Add statistical test
                t_stat, p_value = stats.ttest_ind(runtime_award['runtime'],
                                                  runtime_no_award['runtime'])
                ax3.text(0.05, 0.95,
                        f'Award winners: {runtime_award["runtime"].mean():.0f} min\n'
                        f'Non-winners: {runtime_no_award["runtime"].mean():.0f} min\n'
                        f'p-value: {p_value:.4f}',
                        transform=ax3.transAxes, fontsize=9, verticalalignment='top',
                        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        # 4. Year vs award density
        ax4 = axes[1, 1]
        if 'release_year' in self.df.columns:
            # Group by year
            year_awards = self.df.groupby('release_year').agg({
                'award_diversity': 'mean',
                'title': 'count'
            })
            year_awards.columns = ['avg_diversity', 'film_count']
            year_awards = year_awards[year_awards['film_count'] >= 5]

            if len(year_awards) > 0:
                # Plot recent years only (last 50 years)
                recent_years = year_awards[year_awards.index >= year_awards.index.max() - 50]

                ax4.plot(recent_years.index, recent_years['avg_diversity'],
                        linewidth=2, color='#E74C3C', marker='o', markersize=4)
                ax4.set_xlabel('Year', fontsize=11, fontweight='bold')
                ax4.set_ylabel('Avg Award Diversity', fontsize=11, fontweight='bold')
                ax4.set_title('Award Diversity Over Time (min 5 films/year)',
                             fontsize=12, fontweight='bold')
                ax4.grid(True, alpha=0.3)

                # Add trend line
                z = np.polyfit(recent_years.index, recent_years['avg_diversity'], 1)
                p = np.poly1d(z)
                ax4.plot(recent_years.index, p(recent_years.index),
                        "--", color='blue', alpha=0.5, linewidth=2, label='Trend')
                ax4.legend(fontsize=9)

        plt.tight_layout()
        output_path = self.output_dir / 'award_success_factors.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_international_awards(self):
        """Visualization 9: International awards (César, Goya, etc.)."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('International Awards Analysis', fontsize=16, fontweight='bold')

        # Parse international awards
        self.df['has_cesar'] = self.df['omdb_awards'].str.contains('César', case=False, na=False)
        self.df['has_goya'] = self.df['omdb_awards'].str.contains('Goya', case=False, na=False)

        # 1. International award breakdown
        ax1 = axes[0, 0]
        intl_counts = {
            'César Awards': self.df['has_cesar'].sum(),
            'Goya Awards': self.df['has_goya'].sum(),
            'Cannes': sum('Cannes' in str(x) or 'Palme' in str(x)
                         for x in self.df['omdb_awards'].dropna()),
            'Venice': sum('Venice' in str(x) or 'Lion' in str(x)
                         for x in self.df['omdb_awards'].dropna()),
            'Berlin': sum('Berlin' in str(x) or 'Bear' in str(x)
                         for x in self.df['omdb_awards'].dropna())
        }

        # Filter to non-zero counts
        intl_counts = {k: v for k, v in intl_counts.items() if v > 0}

        if intl_counts:
            colors_bar = ['#E74C3C', '#3498DB', '#2ECC71', '#F39C12', '#9B59B6']
            bars = ax1.bar(list(intl_counts.keys()), list(intl_counts.values()),
                          color=colors_bar[:len(intl_counts)], edgecolor='black')
            ax1.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
            ax1.set_title('International Award Distribution', fontsize=12, fontweight='bold')
            ax1.grid(axis='y', alpha=0.3)
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')

            for bar, val in zip(bars, intl_counts.values()):
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                        f'{val}', ha='center', va='bottom', fontsize=10, fontweight='bold')
        else:
            ax1.text(0.5, 0.5, 'No International Awards Found', ha='center', va='center',
                    transform=ax1.transAxes, fontsize=12)
            ax1.set_title('International Award Distribution', fontsize=12, fontweight='bold')

        # 2. International vs Oscar overlap
        ax2 = axes[0, 1]
        has_intl = (self.df['has_cesar'] | self.df['has_goya'] |
                   self.df['has_festival_award'])

        categories = ['Intl Only', 'Oscar Only', 'Both', 'Neither']
        values = [
            (has_intl & ~self.df['has_oscar']).sum(),
            (~has_intl & self.df['has_oscar']).sum(),
            (has_intl & self.df['has_oscar']).sum(),
            (~has_intl & ~self.df['has_oscar']).sum()
        ]

        colors_pie = ['#E74C3C', 'gold', '#FF6347', '#E0E0E0']
        wedges, texts, autotexts = ax2.pie(values, labels=categories, autopct='%1.1f%%',
                                           colors=colors_pie, startangle=90)
        ax2.set_title('International vs Oscar Awards', fontsize=12, fontweight='bold')

        for autotext in autotexts:
            autotext.set_color('black')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(10)

        # 3. Top international award films
        ax3 = axes[1, 0]
        intl_films = self.df[has_intl & (self.df['imdb_rating'] > 0)]
        if len(intl_films) > 0:
            top_intl = intl_films.nlargest(12, 'imdb_rating')
            y_pos = np.arange(len(top_intl))
            bars = ax3.barh(y_pos, top_intl['imdb_rating'].values,
                           color='#E74C3C', edgecolor='black')
            ax3.set_yticks(y_pos)
            ax3.set_yticklabels([f"{title[:35]}..." if len(title) > 35 else title
                                for title in top_intl['title'].values], fontsize=9)
            ax3.set_xlabel('IMDb Rating', fontsize=11, fontweight='bold')
            ax3.set_title('Top-Rated International Award Films',
                         fontsize=12, fontweight='bold')
            ax3.invert_yaxis()
            ax3.grid(axis='x', alpha=0.3)
            ax3.set_xlim(0, 10)
        else:
            ax3.text(0.5, 0.5, 'No International Award Films', ha='center', va='center',
                    transform=ax3.transAxes, fontsize=12)
            ax3.set_title('Top-Rated International Award Films',
                         fontsize=12, fontweight='bold')

        # 4. Geographic distribution (if country data available)
        ax4 = axes[1, 1]
        if 'tmdb_production_countries' in self.df.columns:
            country_intl_data = []

            for idx, row in self.df.iterrows():
                if pd.notna(row['tmdb_production_countries']) and has_intl[idx]:
                    try:
                        import ast
                        countries = ast.literal_eval(row['tmdb_production_countries'])
                        if isinstance(countries, list):
                            for country in countries[:1]:  # Primary country only
                                country_intl_data.append(country)
                    except:
                        pass

            if country_intl_data:
                country_counts = Counter(country_intl_data).most_common(10)
                countries, counts = zip(*country_counts)

                y_pos = np.arange(len(countries))
                bars = ax4.barh(y_pos, counts, color='#3498DB', edgecolor='black')
                ax4.set_yticks(y_pos)
                ax4.set_yticklabels(countries, fontsize=10)
                ax4.set_xlabel('Number of Films', fontsize=11, fontweight='bold')
                ax4.set_title('Top Countries: International Awards',
                             fontsize=12, fontweight='bold')
                ax4.invert_yaxis()
                ax4.grid(axis='x', alpha=0.3)
            else:
                ax4.text(0.5, 0.5, 'Country data not available', ha='center', va='center',
                        transform=ax4.transAxes, fontsize=11)
                ax4.set_title('Top Countries: International Awards',
                             fontsize=12, fontweight='bold')
        else:
            ax4.text(0.5, 0.5, 'Country data not available', ha='center', va='center',
                    transform=ax4.transAxes, fontsize=11)
            ax4.set_title('Top Countries: International Awards',
                         fontsize=12, fontweight='bold')

        plt.tight_layout()
        output_path = self.output_dir / 'international_awards.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_controversial_awards(self):
        """Visualization 10: Award winners with surprisingly low ratings."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Controversial Award Winners', fontsize=16, fontweight='bold')

        # 1. Oscar winners with low ratings
        ax1 = axes[0, 0]
        oscar_winners = self.df[(self.df['oscar_wins'] > 0) & (self.df['imdb_rating'] > 0)]
        if len(oscar_winners) > 0:
            low_rated_oscars = oscar_winners.nsmallest(12, 'imdb_rating')
            y_pos = np.arange(len(low_rated_oscars))
            bars = ax1.barh(y_pos, low_rated_oscars['imdb_rating'].values,
                           color='#E74C3C', edgecolor='black')
            ax1.set_yticks(y_pos)
            ax1.set_yticklabels([f"{title[:35]}..." if len(title) > 35 else title
                                for title in low_rated_oscars['title'].values], fontsize=9)
            ax1.set_xlabel('IMDb Rating', fontsize=11, fontweight='bold')
            ax1.set_title('Lowest-Rated Oscar Winners', fontsize=12, fontweight='bold')
            ax1.invert_yaxis()
            ax1.grid(axis='x', alpha=0.3)
            ax1.set_xlim(0, 10)

            # Add Oscar count
            for i, oscars in enumerate(low_rated_oscars['oscar_wins'].values):
                ax1.text(low_rated_oscars['imdb_rating'].values[i] + 0.1, i,
                        f'{int(oscars)} Oscar{"s" if oscars > 1 else ""}',
                        va='center', fontsize=8, color='gold', fontweight='bold')
        else:
            ax1.text(0.5, 0.5, 'No Oscar Winners Found', ha='center', va='center',
                    transform=ax1.transAxes, fontsize=12)
            ax1.set_title('Lowest-Rated Oscar Winners', fontsize=12, fontweight='bold')

        # 2. High awards but low ratings
        ax2 = axes[0, 1]
        high_award_films = self.df[(self.df['total_wins'] >= 5) & (self.df['imdb_rating'] > 0)]
        if len(high_award_films) > 0:
            controversial = high_award_films.nsmallest(12, 'imdb_rating')
            y_pos = np.arange(len(controversial))
            bars = ax2.barh(y_pos, controversial['total_wins'].values,
                           color=plt.cm.RdYlGn_r(controversial['imdb_rating'].values / 10),
                           edgecolor='black')
            ax2.set_yticks(y_pos)
            ax2.set_yticklabels([f"{title[:35]}..." if len(title) > 35 else title
                                for title in controversial['title'].values], fontsize=9)
            ax2.set_xlabel('Total Awards', fontsize=11, fontweight='bold')
            ax2.set_title('Most Awarded Despite Low Ratings (5+ wins)',
                         fontsize=12, fontweight='bold')
            ax2.invert_yaxis()
            ax2.grid(axis='x', alpha=0.3)

            # Add ratings
            for i, rating in enumerate(controversial['imdb_rating'].values):
                ax2.text(controversial['total_wins'].values[i] + 0.5, i,
                        f'IMDb: {rating:.1f}', va='center', fontsize=8)
        else:
            ax2.text(0.5, 0.5, 'No highly awarded films found', ha='center', va='center',
                    transform=ax2.transAxes, fontsize=12)
            ax2.set_title('Most Awarded Despite Low Ratings', fontsize=12, fontweight='bold')

        # 3. Rating distribution: award winners vs all
        ax3 = axes[1, 0]
        award_winners = self.df[(self.df['total_wins'] > 0) & (self.df['imdb_rating'] > 0)]
        non_winners = self.df[(self.df['total_wins'] == 0) & (self.df['imdb_rating'] > 0)]

        if len(award_winners) > 0 and len(non_winners) > 0:
            ax3.hist([non_winners['imdb_rating'], award_winners['imdb_rating']],
                    bins=20, label=['No Awards', 'Award Winners'],
                    color=['lightgray', 'gold'], edgecolor='black', alpha=0.7)
            ax3.set_xlabel('IMDb Rating', fontsize=11, fontweight='bold')
            ax3.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
            ax3.set_title('Rating Distribution Comparison', fontsize=12, fontweight='bold')
            ax3.legend(fontsize=10)
            ax3.grid(axis='y', alpha=0.3)

            # Add means
            ax3.axvline(non_winners['imdb_rating'].mean(), color='gray',
                       linestyle='--', linewidth=2, label='Mean (No Awards)')
            ax3.axvline(award_winners['imdb_rating'].mean(), color='goldenrod',
                       linestyle='--', linewidth=2, label='Mean (Winners)')

        # 4. Award vs rating scatter with outliers
        ax4 = axes[1, 1]
        rated_awarded = self.df[(self.df['imdb_rating'] > 0) & (self.df['total_wins'] > 0)]

        if len(rated_awarded) > 0:
            scatter = ax4.scatter(rated_awarded['imdb_rating'], rated_awarded['total_wins'],
                                 c=rated_awarded['oscar_wins'], cmap='YlOrRd',
                                 s=100, alpha=0.6, edgecolors='black')
            ax4.set_xlabel('IMDb Rating', fontsize=11, fontweight='bold')
            ax4.set_ylabel('Total Wins', fontsize=11, fontweight='bold')
            ax4.set_title('Awards vs Rating with Outliers Highlighted',
                         fontsize=12, fontweight='bold')
            ax4.grid(True, alpha=0.3)

            cbar = plt.colorbar(scatter, ax=ax4)
            cbar.set_label('Oscar Wins', fontsize=10)

            # Highlight controversial films (high awards, low rating)
            controversial_threshold = rated_awarded['imdb_rating'].quantile(0.25)
            award_threshold = rated_awarded['total_wins'].quantile(0.75)
            controversial_films = rated_awarded[
                (rated_awarded['imdb_rating'] < controversial_threshold) &
                (rated_awarded['total_wins'] > award_threshold)
            ]

            if len(controversial_films) > 0:
                ax4.scatter(controversial_films['imdb_rating'],
                          controversial_films['total_wins'],
                          s=200, facecolors='none', edgecolors='red',
                          linewidths=3, label='Controversial', zorder=10)
                ax4.legend(fontsize=9)

        plt.tight_layout()
        output_path = self.output_dir / 'controversial_awards.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_nomination_analysis(self):
        """Visualization 11: Analysis of nominations vs wins."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Nominations vs Wins Analysis', fontsize=16, fontweight='bold')

        # 1. Win rate distribution
        ax1 = axes[0, 0]
        nominated_films = self.df[(self.df['total_noms'] > 0) & (self.df['total_wins'] >= 0)]
        if len(nominated_films) > 0:
            nominated_films['win_rate'] = (nominated_films['total_wins'] /
                                          nominated_films['total_noms'] * 100)

            ax1.hist(nominated_films['win_rate'], bins=20, color='skyblue',
                    edgecolor='black', alpha=0.7)
            ax1.set_xlabel('Win Rate (%)', fontsize=11, fontweight='bold')
            ax1.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
            ax1.set_title(f'Award Win Rate Distribution ({len(nominated_films)} films)',
                         fontsize=12, fontweight='bold')
            ax1.grid(axis='y', alpha=0.3)

            # Add mean line
            mean_rate = nominated_films['win_rate'].mean()
            ax1.axvline(mean_rate, color='red', linestyle='--', linewidth=2,
                       label=f'Mean: {mean_rate:.1f}%')
            ax1.legend(fontsize=10)
        else:
            ax1.text(0.5, 0.5, 'No nomination data available', ha='center', va='center',
                    transform=ax1.transAxes, fontsize=12)
            ax1.set_title('Award Win Rate Distribution', fontsize=12, fontweight='bold')

        # 2. Best win rates
        ax2 = axes[0, 1]
        if len(nominated_films) > 0:
            high_noms = nominated_films[nominated_films['total_noms'] >= 3]
            if len(high_noms) > 0:
                top_rates = high_noms.nlargest(12, 'win_rate')
                y_pos = np.arange(len(top_rates))
                bars = ax2.barh(y_pos, top_rates['win_rate'].values,
                               color=plt.cm.RdYlGn(top_rates['win_rate'].values / 100),
                               edgecolor='black')
                ax2.set_yticks(y_pos)
                ax2.set_yticklabels([f"{title[:30]}..." if len(title) > 30 else title
                                    for title in top_rates['title'].values], fontsize=9)
                ax2.set_xlabel('Win Rate (%)', fontsize=11, fontweight='bold')
                ax2.set_title('Best Win Rates (min 3 nominations)',
                             fontsize=12, fontweight='bold')
                ax2.invert_yaxis()
                ax2.grid(axis='x', alpha=0.3)

                # Add win/nom ratio
                for i, (rate, wins, noms) in enumerate(zip(top_rates['win_rate'].values,
                                                           top_rates['total_wins'].values,
                                                           top_rates['total_noms'].values)):
                    ax2.text(rate + 2, i, f'{int(wins)}/{int(noms)}',
                            va='center', fontsize=8)

        # 3. Nominations vs wins scatter
        ax3 = axes[1, 0]
        films_with_both = self.df[(self.df['total_noms'] > 0) & (self.df['total_wins'] >= 0)]
        if len(films_with_both) > 0:
            scatter = ax3.scatter(films_with_both['total_noms'],
                                 films_with_both['total_wins'],
                                 c=films_with_both['imdb_rating'],
                                 cmap='YlOrRd', s=100, alpha=0.6, edgecolors='black')
            ax3.set_xlabel('Total Nominations', fontsize=11, fontweight='bold')
            ax3.set_ylabel('Total Wins', fontsize=11, fontweight='bold')
            ax3.set_title('Nominations vs Wins Relationship', fontsize=12, fontweight='bold')
            ax3.grid(True, alpha=0.3)

            cbar = plt.colorbar(scatter, ax=ax3)
            cbar.set_label('IMDb Rating', fontsize=10)

            # Add diagonal reference lines
            max_val = max(films_with_both['total_noms'].max(),
                         films_with_both['total_wins'].max())
            ax3.plot([0, max_val], [0, max_val], 'r--', alpha=0.5,
                    linewidth=2, label='100% win rate')
            ax3.plot([0, max_val], [0, max_val/2], 'y--', alpha=0.5,
                    linewidth=2, label='50% win rate')
            ax3.legend(fontsize=9)

        # 4. Oscar nomination efficiency
        ax4 = axes[1, 1]
        oscar_noms = self.df[(self.df['oscar_noms'] > 0)]
        if len(oscar_noms) > 0:
            oscar_noms['oscar_win_rate'] = (oscar_noms['oscar_wins'] /
                                           oscar_noms['oscar_noms'] * 100).fillna(0)

            # Group by nomination count
            nom_groups = oscar_noms.groupby('oscar_noms')['oscar_win_rate'].mean().sort_index()

            if len(nom_groups) > 0:
                bars = ax4.bar(nom_groups.index, nom_groups.values,
                              color='gold', edgecolor='black')
                ax4.set_xlabel('Number of Oscar Nominations', fontsize=11, fontweight='bold')
                ax4.set_ylabel('Average Win Rate (%)', fontsize=11, fontweight='bold')
                ax4.set_title('Oscar Win Rate by Nomination Count',
                             fontsize=12, fontweight='bold')
                ax4.grid(axis='y', alpha=0.3)

                # Add counts
                nom_counts = oscar_noms.groupby('oscar_noms').size()
                for i, (noms, rate) in enumerate(nom_groups.items()):
                    count = nom_counts[noms]
                    ax4.text(noms, rate + 1, f'{rate:.1f}%\n(n={count})',
                            ha='center', fontsize=8)

        plt.tight_layout()
        output_path = self.output_dir / 'nomination_analysis.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_decade_trends(self):
        """Visualization 12: Award trends across decades."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Award Trends Across Decades', fontsize=16, fontweight='bold')

        if 'decade' not in self.df.columns:
            fig.text(0.5, 0.5, 'Decade information not available',
                    ha='center', va='center', fontsize=14)
            plt.tight_layout()
            output_path = self.output_dir / 'decade_trends.png'
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"Saved: {output_path}")
            return

        # 1. Award coverage by decade
        ax1 = axes[0, 0]
        decade_stats = self.df.groupby('decade').agg({
            'has_awards': 'sum',
            'title': 'count'
        })
        decade_stats.columns = ['films_with_awards', 'total_films']
        decade_stats['coverage_pct'] = (decade_stats['films_with_awards'] /
                                       decade_stats['total_films'] * 100)

        x = np.arange(len(decade_stats))
        bars = ax1.bar(x, decade_stats['coverage_pct'].values,
                      color='skyblue', edgecolor='black')
        ax1.set_xticks(x)
        ax1.set_xticklabels([f"{int(d)}s" for d in decade_stats.index], rotation=45)
        ax1.set_xlabel('Decade', fontsize=11, fontweight='bold')
        ax1.set_ylabel('% with Award Data', fontsize=11, fontweight='bold')
        ax1.set_title('Award Data Coverage by Decade', fontsize=12, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)

        # 2. Average awards per film by decade
        ax2 = axes[0, 1]
        decade_avg = self.df.groupby('decade')['total_wins'].mean()

        x = np.arange(len(decade_avg))
        bars = ax2.bar(x, decade_avg.values, color='#E74C3C', edgecolor='black')
        ax2.set_xticks(x)
        ax2.set_xticklabels([f"{int(d)}s" for d in decade_avg.index], rotation=45)
        ax2.set_xlabel('Decade', fontsize=11, fontweight='bold')
        ax2.set_ylabel('Average Wins per Film', fontsize=11, fontweight='bold')
        ax2.set_title('Average Award Wins by Decade', fontsize=12, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        # Add trend line
        z = np.polyfit(x, decade_avg.values, 1)
        p = np.poly1d(z)
        ax2.plot(x, p(x), "--", color='blue', alpha=0.7, linewidth=2, label='Trend')
        ax2.legend(fontsize=9)

        # 3. Oscar concentration by decade
        ax3 = axes[1, 0]
        decade_oscar = self.df.groupby('decade').agg({
            'oscar_wins': 'sum',
            'title': 'count'
        })
        decade_oscar.columns = ['total_oscars', 'total_films']
        decade_oscar['oscars_per_film'] = (decade_oscar['total_oscars'] /
                                          decade_oscar['total_films'])

        x = np.arange(len(decade_oscar))
        bars = ax3.bar(x, decade_oscar['oscars_per_film'].values,
                      color='gold', edgecolor='black')
        ax3.set_xticks(x)
        ax3.set_xticklabels([f"{int(d)}s" for d in decade_oscar.index], rotation=45)
        ax3.set_xlabel('Decade', fontsize=11, fontweight='bold')
        ax3.set_ylabel('Oscars per Film', fontsize=11, fontweight='bold')
        ax3.set_title('Oscar Concentration by Decade', fontsize=12, fontweight='bold')
        ax3.grid(axis='y', alpha=0.3)

        # 4. Award type evolution
        ax4 = axes[1, 1]
        decade_award_types = self.df.groupby('decade').agg({
            'has_oscar': 'sum',
            'has_bafta': 'sum',
            'has_golden_globe': 'sum',
            'has_festival_award': 'sum'
        })

        x = np.arange(len(decade_award_types))
        width = 0.2

        ax4.bar(x - 1.5*width, decade_award_types['has_oscar'], width,
               label='Oscar', color='gold')
        ax4.bar(x - 0.5*width, decade_award_types['has_bafta'], width,
               label='BAFTA', color='#CD7F32')
        ax4.bar(x + 0.5*width, decade_award_types['has_golden_globe'], width,
               label='Golden Globe', color='#FFD700')
        ax4.bar(x + 1.5*width, decade_award_types['has_festival_award'], width,
               label='Festival', color='#E74C3C')

        ax4.set_xticks(x)
        ax4.set_xticklabels([f"{int(d)}s" for d in decade_award_types.index], rotation=45)
        ax4.set_xlabel('Decade', fontsize=11, fontweight='bold')
        ax4.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
        ax4.set_title('Award Type Evolution', fontsize=12, fontweight='bold')
        ax4.legend(fontsize=9)
        ax4.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        output_path = self.output_dir / 'decade_trends.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_award_specificity(self):
        """Visualization 13: Which awards are most selective."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Award Selectivity Analysis', fontsize=16, fontweight='bold')

        # 1. Award type selectivity
        ax1 = axes[0, 0]
        total_films = len(self.df)

        selectivity_data = {
            'Oscar Win': (self.df['oscar_wins'] > 0).sum(),
            'BAFTA': self.df['has_bafta'].sum(),
            'Golden Globe': self.df['has_golden_globe'].sum(),
            'Festival': self.df['has_festival_award'].sum(),
            'SAG': self.df['has_sag'].sum()
        }

        # Convert to percentages
        selectivity_pct = {k: (v / total_films * 100) for k, v in selectivity_data.items()}
        selectivity_pct = dict(sorted(selectivity_pct.items(), key=lambda x: x[1]))

        y_pos = np.arange(len(selectivity_pct))
        bars = ax1.barh(y_pos, list(selectivity_pct.values()),
                       color=plt.cm.RdYlGn_r(np.array(list(selectivity_pct.values())) / 100))
        ax1.set_yticks(y_pos)
        ax1.set_yticklabels(list(selectivity_pct.keys()), fontsize=10)
        ax1.set_xlabel('% of Films with Award', fontsize=11, fontweight='bold')
        ax1.set_title('Award Selectivity (Lower = More Exclusive)',
                     fontsize=12, fontweight='bold')
        ax1.grid(axis='x', alpha=0.3)

        # Add values
        for i, (pct, count) in enumerate(zip(selectivity_pct.values(), selectivity_data.values())):
            ax1.text(pct + 0.3, i, f'{pct:.2f}% ({count} films)',
                    va='center', fontsize=9)

        # 2. Average rating by award type
        ax2 = axes[0, 1]
        award_quality = {
            'Oscar Winners': self.df[self.df['oscar_wins'] > 0]['imdb_rating'].mean(),
            'BAFTA': self.df[self.df['has_bafta']]['imdb_rating'].mean(),
            'Golden Globe': self.df[self.df['has_golden_globe']]['imdb_rating'].mean(),
            'Festival': self.df[self.df['has_festival_award']]['imdb_rating'].mean(),
            'SAG': self.df[self.df['has_sag']]['imdb_rating'].mean(),
            'No Awards': self.df[~self.df['has_awards']]['imdb_rating'].mean()
        }

        # Remove NaN values
        award_quality = {k: v for k, v in award_quality.items() if not pd.isna(v)}
        award_quality = dict(sorted(award_quality.items(), key=lambda x: x[1], reverse=True))

        y_pos = np.arange(len(award_quality))
        bars = ax2.barh(y_pos, list(award_quality.values()),
                       color=plt.cm.RdYlGn(np.array(list(award_quality.values())) / 10))
        ax2.set_yticks(y_pos)
        ax2.set_yticklabels(list(award_quality.keys()), fontsize=10)
        ax2.set_xlabel('Average IMDb Rating', fontsize=11, fontweight='bold')
        ax2.set_title('Quality Indicator by Award Type', fontsize=12, fontweight='bold')
        ax2.invert_yaxis()
        ax2.grid(axis='x', alpha=0.3)
        ax2.set_xlim(0, 10)

        # Add values
        for i, rating in enumerate(award_quality.values()):
            ax2.text(rating + 0.1, i, f'{rating:.2f}', va='center', fontsize=9)

        # 3. Oscar nomination to win ratio
        ax3 = axes[1, 0]
        oscar_films = self.df[self.df['oscar_noms'] > 0]

        if len(oscar_films) > 0:
            # Group by number of nominations
            nom_analysis = []
            for nom_count in range(1, min(13, oscar_films['oscar_noms'].max() + 1)):
                films = oscar_films[oscar_films['oscar_noms'] == nom_count]
                if len(films) > 0:
                    win_rate = (films['oscar_wins'].sum() /
                               (films['oscar_noms'].sum())) * 100
                    nom_analysis.append({
                        'noms': nom_count,
                        'films': len(films),
                        'win_rate': win_rate
                    })

            if nom_analysis:
                nom_df = pd.DataFrame(nom_analysis)

                bars = ax3.bar(nom_df['noms'], nom_df['win_rate'],
                              color='gold', edgecolor='black')
                ax3.set_xlabel('Number of Nominations', fontsize=11, fontweight='bold')
                ax3.set_ylabel('Win Rate (%)', fontsize=11, fontweight='bold')
                ax3.set_title('Oscar Win Efficiency by Nomination Count',
                             fontsize=12, fontweight='bold')
                ax3.grid(axis='y', alpha=0.3)

                # Add film counts
                for i, row in nom_df.iterrows():
                    ax3.text(row['noms'], row['win_rate'] + 1,
                            f'n={row["films"]}', ha='center', fontsize=8)

        # 4. Award prestige score
        ax4 = axes[1, 1]
        # Create prestige score based on rarity and quality
        prestige_scores = []

        for award_name, has_award_col in [
            ('Oscar Win', 'oscar_wins'),
            ('BAFTA', 'has_bafta'),
            ('Golden Globe', 'has_golden_globe'),
            ('Festival', 'has_festival_award'),
            ('SAG', 'has_sag')
        ]:
            if award_name == 'Oscar Win':
                award_films = self.df[self.df[has_award_col] > 0]
            else:
                award_films = self.df[self.df[has_award_col]]

            if len(award_films) > 0:
                rarity = 1 - (len(award_films) / len(self.df))
                quality = award_films['imdb_rating'].mean()

                if not pd.isna(quality):
                    # Prestige = rarity * quality (normalized)
                    prestige = rarity * (quality / 10) * 100
                    prestige_scores.append({
                        'award': award_name,
                        'prestige': prestige,
                        'rarity': rarity * 100,
                        'quality': quality
                    })

        if prestige_scores:
            prestige_df = pd.DataFrame(prestige_scores).sort_values('prestige', ascending=True)

            y_pos = np.arange(len(prestige_df))
            bars = ax4.barh(y_pos, prestige_df['prestige'].values,
                           color=plt.cm.YlOrRd(prestige_df['prestige'].values /
                                              prestige_df['prestige'].max()))
            ax4.set_yticks(y_pos)
            ax4.set_yticklabels(prestige_df['award'].values, fontsize=10)
            ax4.set_xlabel('Prestige Score', fontsize=11, fontweight='bold')
            ax4.set_title('Award Prestige (Rarity × Quality)', fontsize=12, fontweight='bold')
            ax4.grid(axis='x', alpha=0.3)

            # Add component scores
            for i, row in prestige_df.iterrows():
                idx = prestige_df.index.get_loc(i)
                ax4.text(row['prestige'] + 1, idx,
                        f"R:{row['rarity']:.0f}% Q:{row['quality']:.1f}",
                        va='center', fontsize=8)

        plt.tight_layout()
        output_path = self.output_dir / 'award_specificity.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_coverage_analysis(self):
        """Visualization 14: Award data coverage and completeness."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Award Data Coverage Analysis', fontsize=16, fontweight='bold')

        # 1. Overall coverage metrics
        ax1 = axes[0, 0]
        coverage_metrics = {
            'Has Award Data': self.df['has_awards'].sum(),
            'Oscar Info': ((self.df['oscar_wins'] > 0) | (self.df['oscar_noms'] > 0)).sum(),
            'BAFTA Info': self.df['has_bafta'].sum(),
            'Golden Globe Info': self.df['has_golden_globe'].sum(),
            'Festival Info': self.df['has_festival_award'].sum(),
            'SAG Info': self.df['has_sag'].sum()
        }

        coverage_pct = {k: (v / len(self.df) * 100) for k, v in coverage_metrics.items()}

        y_pos = np.arange(len(coverage_pct))
        bars = ax1.barh(y_pos, list(coverage_pct.values()),
                       color=plt.cm.RdYlGn(np.array(list(coverage_pct.values())) / 100))
        ax1.set_yticks(y_pos)
        ax1.set_yticklabels(list(coverage_pct.keys()), fontsize=10)
        ax1.set_xlabel('Coverage (%)', fontsize=11, fontweight='bold')
        ax1.set_title(f'Award Data Coverage ({len(self.df)} films)',
                     fontsize=12, fontweight='bold')
        ax1.grid(axis='x', alpha=0.3)

        # Add values
        for i, (pct, count) in enumerate(zip(coverage_pct.values(), coverage_metrics.values())):
            ax1.text(pct + 1, i, f'{pct:.1f}% ({count})',
                    va='center', fontsize=9)

        # 2. Coverage by decade
        ax2 = axes[0, 1]
        if 'decade' in self.df.columns:
            decade_coverage = self.df.groupby('decade')['has_awards'].agg(['sum', 'count'])
            decade_coverage['pct'] = (decade_coverage['sum'] / decade_coverage['count'] * 100)

            x = np.arange(len(decade_coverage))
            bars = ax2.bar(x, decade_coverage['pct'].values,
                          color='skyblue', edgecolor='black')
            ax2.set_xticks(x)
            ax2.set_xticklabels([f"{int(d)}s" for d in decade_coverage.index], rotation=45)
            ax2.set_xlabel('Decade', fontsize=11, fontweight='bold')
            ax2.set_ylabel('% with Award Data', fontsize=11, fontweight='bold')
            ax2.set_title('Award Data Coverage by Decade', fontsize=12, fontweight='bold')
            ax2.grid(axis='y', alpha=0.3)

            # Add counts
            for i, (pct, count) in enumerate(zip(decade_coverage['pct'].values,
                                                 decade_coverage['sum'].values)):
                ax2.text(i, pct + 1, f'{int(count)}', ha='center', fontsize=8)

        # 3. Award text length distribution
        ax3 = axes[1, 0]
        award_texts = self.df[self.df['has_awards']]['omdb_awards'].dropna()
        if len(award_texts) > 0:
            text_lengths = award_texts.str.len()

            ax3.hist(text_lengths, bins=30, color='#3498DB', edgecolor='black', alpha=0.7)
            ax3.set_xlabel('Award Text Length (characters)', fontsize=11, fontweight='bold')
            ax3.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
            ax3.set_title('Award Information Detail Distribution', fontsize=12, fontweight='bold')
            ax3.grid(axis='y', alpha=0.3)

            # Add statistics
            ax3.axvline(text_lengths.mean(), color='red', linestyle='--',
                       linewidth=2, label=f'Mean: {text_lengths.mean():.0f}')
            ax3.axvline(text_lengths.median(), color='green', linestyle='--',
                       linewidth=2, label=f'Median: {text_lengths.median():.0f}')
            ax3.legend(fontsize=9)

        # 4. Data completeness summary
        ax4 = axes[1, 1]

        # Create completeness score
        completeness_data = []
        for idx, row in self.df.iterrows():
            score = 0
            max_score = 7

            # Award data present
            if row['has_awards']:
                score += 1
            # Oscar data
            if row['oscar_wins'] > 0 or row['oscar_noms'] > 0:
                score += 1
            # BAFTA
            if row['has_bafta']:
                score += 1
            # Golden Globe
            if row['has_golden_globe']:
                score += 1
            # Festival
            if row['has_festival_award']:
                score += 1
            # SAG
            if row['has_sag']:
                score += 1
            # Total wins/noms
            if row['total_wins'] > 0 or row['total_noms'] > 0:
                score += 1

            completeness_data.append(score)

        self.df['completeness_score'] = completeness_data

        # Plot distribution
        completeness_counts = pd.Series(completeness_data).value_counts().sort_index()

        colors_bar = plt.cm.RdYlGn(completeness_counts.index / 7)
        bars = ax4.bar(completeness_counts.index, completeness_counts.values,
                      color=colors_bar, edgecolor='black')
        ax4.set_xlabel('Completeness Score (0-7)', fontsize=11, fontweight='bold')
        ax4.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
        ax4.set_title('Award Data Completeness Distribution', fontsize=12, fontweight='bold')
        ax4.grid(axis='y', alpha=0.3)

        # Add percentages
        for score, count in completeness_counts.items():
            pct = (count / len(self.df)) * 100
            ax4.text(score, count + 5, f'{count}\n({pct:.1f}%)',
                    ha='center', fontsize=8)

        plt.tight_layout()
        output_path = self.output_dir / 'coverage_analysis.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def generate_report(self):
        """Generate comprehensive analysis report."""
        report_path = self.report_dir / 'batch_29_awards_deep_dive_report.txt'

        with open(report_path, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("CINESCOPE BATCH 29: AWARDS DEEP-DIVE\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # Overall statistics
            f.write("=" * 80 + "\n")
            f.write("OVERALL STATISTICS\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Total Films: {len(self.df)}\n\n")
            f.write(f"Films with Award Data: {self.df['has_awards'].sum()} "
                   f"({self.df['has_awards'].sum() / len(self.df) * 100:.1f}%)\n\n")

            # Oscar statistics
            f.write("=" * 80 + "\n")
            f.write("OSCAR AWARDS\n")
            f.write("=" * 80 + "\n\n")
            oscar_winners = (self.df['oscar_wins'] > 0).sum()
            oscar_nominees = ((self.df['oscar_noms'] > 0) & (self.df['oscar_wins'] == 0)).sum()
            f.write(f"Oscar Winners: {oscar_winners} ({oscar_winners / len(self.df) * 100:.1f}%)\n")
            f.write(f"Oscar Nominees (no wins): {oscar_nominees} "
                   f"({oscar_nominees / len(self.df) * 100:.1f}%)\n")
            f.write(f"Total Oscars Won: {self.df['oscar_wins'].sum()}\n")
            f.write(f"Total Oscar Nominations: {self.df['oscar_noms'].sum()}\n\n")

            # Top Oscar winners
            if oscar_winners > 0:
                f.write("Top Oscar-Winning Films:\n")
                top_oscar = self.df[self.df['oscar_wins'] > 0].nlargest(10, 'oscar_wins')
                for i, row in enumerate(top_oscar.itertuples(), 1):
                    f.write(f"{i}. {row.title} - {int(row.oscar_wins)} Oscars "
                           f"({int(row.oscar_noms)} nominations)\n")
                f.write("\n")

            # Other major awards
            f.write("=" * 80 + "\n")
            f.write("OTHER MAJOR AWARDS\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"BAFTA: {self.df['has_bafta'].sum()} "
                   f"({self.df['has_bafta'].sum() / len(self.df) * 100:.1f}%)\n")
            f.write(f"Golden Globe: {self.df['has_golden_globe'].sum()} "
                   f"({self.df['has_golden_globe'].sum() / len(self.df) * 100:.1f}%)\n")
            f.write(f"Festival Awards: {self.df['has_festival_award'].sum()} "
                   f"({self.df['has_festival_award'].sum() / len(self.df) * 100:.1f}%)\n")
            f.write(f"SAG Awards: {self.df['has_sag'].sum()} "
                   f"({self.df['has_sag'].sum() / len(self.df) * 100:.1f}%)\n\n")

            # Award diversity
            f.write("=" * 80 + "\n")
            f.write("AWARD DIVERSITY\n")
            f.write("=" * 80 + "\n\n")
            diversity_dist = self.df['award_diversity'].value_counts().sort_index()
            for diversity, count in diversity_dist.items():
                pct = (count / len(self.df)) * 100
                f.write(f"{int(diversity)} award types: {count} ({pct:.1f}%)\n")
            f.write("\n")

            # Award sweeps (3+ types)
            sweep_films = self.df[self.df['award_diversity'] >= 3]
            if len(sweep_films) > 0:
                f.write(f"Films with 3+ Award Types: {len(sweep_films)}\n\n")
                f.write("Top Award Sweep Films:\n")
                top_sweeps = sweep_films.nlargest(10, 'award_diversity')
                for i, row in enumerate(top_sweeps.itertuples(), 1):
                    rating_str = f" - {row.imdb_rating:.1f}" if not pd.isna(row.imdb_rating) and row.imdb_rating > 0 else ""
                    f.write(f"{i}. {row.title} - {int(row.award_diversity)} types{rating_str}\n")
                f.write("\n")

            f.write("=" * 80 + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * 80 + "\n")

        print(f"Report generated: {report_path}")

    def run_all_analyses(self):
        """Execute all visualizations and generate report."""
        print("\n" + "=" * 80)
        print("CINESCOPE BATCH 29: AWARDS DEEP-DIVE ANALYSIS")
        print("=" * 80 + "\n")

        visualizations = [
            ("Awards Overview", self.visualize_awards_overview),
            ("Oscar Analysis", self.visualize_oscar_analysis),
            ("BAFTA Analysis", self.visualize_bafta_analysis),
            ("Golden Globe Analysis", self.visualize_golden_globe_analysis),
            ("Festival Awards", self.visualize_festival_awards),
            ("SAG & Critics Awards", self.visualize_sag_critics_awards),
            ("Award Sweeps", self.visualize_award_sweeps),
            ("Award Success Factors", self.visualize_award_success_factors),
            ("International Awards", self.visualize_international_awards),
            ("Controversial Awards", self.visualize_controversial_awards),
            ("Nomination Analysis", self.visualize_nomination_analysis),
            ("Decade Trends", self.visualize_decade_trends),
            ("Award Specificity", self.visualize_award_specificity),
            ("Coverage Analysis", self.visualize_coverage_analysis),
        ]

        for name, func in visualizations:
            print(f"Generating {name}...")
            func()

        print("\nGenerating comprehensive report...")
        self.generate_report()

        print("\n" + "=" * 80)
        print("ANALYSIS COMPLETE")
        print("=" * 80)
        print(f"\nOutputs saved to:")
        print(f"  Visualizations: {self.output_dir}")
        print(f"  Report: {self.report_dir / 'batch_29_awards_deep_dive_report.txt'}")


def main():
    """Main execution function."""
    analyzer = AwardsDeepDiveAnalyzer()
    analyzer.run_all_analyses()


if __name__ == '__main__':
    main()
