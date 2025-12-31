#!/usr/bin/env python3
"""
CineScope Batch 30: Festival Circuit Analysis
============================================

Analysis of film festival participation, winners, and the festival-to-mainstream
pipeline for indie films and art cinema.

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

class FestivalCircuitAnalyzer:
    """Analyze film festival participation and success patterns."""

    def __init__(self, data_path='data/processed/watched_movies_master.csv'):
        """Initialize analyzer with watched movies dataset."""
        self.data_path = Path(data_path)
        self.output_dir = Path('analysis_outputs/visualizations/batch_30')
        self.report_dir = Path('analysis_outputs/reports')

        # Create output directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)

        # Load data
        print("Loading watched movies data...")
        self.df = pd.read_csv(self.data_path)

        print(f"Loaded {len(self.df)} watched films")

        # Major film festivals with prestige ranking
        self.festival_prestige = {
            'Cannes': 10,
            'Venice': 9,
            'Berlin': 9,
            'Sundance': 8,
            'Toronto': 8,
            'Tribeca': 7,
            'Telluride': 7,
            'SXSW': 6,
            'Rotterdam': 6,
            'Locarno': 6,
            'San Sebastian': 6,
            'Karlovy Vary': 5,
            'Edinburgh': 5,
            'AFI Fest': 5,
            'New York Film Festival': 7,
            'London Film Festival': 7,
        }

        # Festival patterns for extraction
        self.festival_patterns = {
            'Cannes': r'Cannes|Palme d\'?Or',
            'Venice': r'Venice|Golden Lion',
            'Berlin': r'Berlin|Golden Bear',
            'Sundance': r'Sundance',
            'Toronto': r'Toronto|TIFF',
            'Tribeca': r'Tribeca',
            'Telluride': r'Telluride',
            'SXSW': r'SXSW|South by Southwest',
            'Rotterdam': r'Rotterdam',
            'Locarno': r'Locarno',
            'San Sebastian': r'San Sebastian',
            'Karlovy Vary': r'Karlovy Vary',
            'Edinburgh': r'Edinburgh',
            'AFI Fest': r'AFI',
            'New York Film Festival': r'New York Film Festival|NYFF',
            'London Film Festival': r'London Film Festival|BFI',
        }

        # Parse festival data
        self._parse_festival_data()

        # Set up plotting style
        plt.style.use('default')
        sns.set_palette("husl")

    def _parse_festival_data(self):
        """Use pre-enriched festival data from comprehensive parser."""
        print("Using enriched festival data...")
        
        # Use enriched columns
        self.df['has_festival'] = self.df['enriched_has_festival']
        self.df['has_cannes'] = self.df['enriched_festivals'].str.contains('Cannes', na=False)
        self.df['has_venice'] = self.df['enriched_festivals'].str.contains('Venice', na=False)
        self.df['has_berlin'] = self.df['enriched_festivals'].str.contains('Berlin', na=False)
        self.df['has_sundance'] = self.df['enriched_festivals'].str.contains('Sundance', na=False)
        self.df['has_toronto'] = self.df['enriched_festivals'].str.contains('Toronto', na=False)
        
        # Parse festival list
        self.df['festival_list'] = self.df['enriched_festivals'].apply(
            lambda x: x.split(',') if pd.notna(x) and x else []
        )
        self.df['festival_count'] = self.df['festival_list'].apply(len)
        
        # Calculate prestige score
        self.df['festival_prestige_score'] = 0
        for idx, row in self.df[self.df['has_festival']].iterrows():
            score = 0
            for festival in row['festival_list']:
                score += self.festival_prestige.get(festival, 3)
            self.df.at[idx, 'festival_prestige_score'] = score
        
        print(f"Loaded enriched festival data for {self.df['has_festival'].sum()} films")

    def visualize_festival_participation(self):
        """Visualization 1: Overall festival participation landscape."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Film Festival Participation Overview', fontsize=16, fontweight='bold')

        # 1. Festival representation
        ax1 = axes[0, 0]
        festival_counts = Counter()
        for festivals in self.df['festival_list']:
            festival_counts.update(festivals)

        if festival_counts:
            top_festivals = dict(festival_counts.most_common(15))
            y_pos = np.arange(len(top_festivals))
            colors = plt.cm.Spectral(np.linspace(0, 1, len(top_festivals)))

            bars = ax1.barh(y_pos, list(top_festivals.values()), color=colors, edgecolor='black')
            ax1.set_yticks(y_pos)
            ax1.set_yticklabels(list(top_festivals.keys()), fontsize=10)
            ax1.set_xlabel('Number of Films', fontsize=11, fontweight='bold')
            ax1.set_title('Most Represented Festivals', fontsize=12, fontweight='bold')
            ax1.invert_yaxis()
            ax1.grid(axis='x', alpha=0.3)

            # Add percentages
            for i, (festival, count) in enumerate(top_festivals.items()):
                pct = (count / len(self.df)) * 100
                ax1.text(count + 0.5, i, f'{count} ({pct:.1f}%)',
                        va='center', fontsize=8)
        else:
            ax1.text(0.5, 0.5, 'No Festival Data Found', ha='center', va='center',
                    transform=ax1.transAxes, fontsize=12)
            ax1.set_title('Most Represented Festivals', fontsize=12, fontweight='bold')

        # 2. Festival coverage
        ax2 = axes[0, 1]
        coverage_data = {
            'Has Festival Data': self.df['has_festival'].sum(),
            'No Festival Data': (~self.df['has_festival']).sum()
        }

        colors_pie = ['#E74C3C', '#E0E0E0']
        wedges, texts, autotexts = ax2.pie(coverage_data.values(), labels=coverage_data.keys(),
                                           autopct='%1.1f%%', colors=colors_pie, startangle=90)
        ax2.set_title(f'Festival Coverage ({len(self.df)} films)',
                     fontsize=12, fontweight='bold')

        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(11)

        # 3. Festival count distribution
        ax3 = axes[1, 0]
        festival_films = self.df[self.df['has_festival']]
        if len(festival_films) > 0:
            count_dist = festival_films['festival_count'].value_counts().sort_index()

            bars = ax3.bar(count_dist.index, count_dist.values,
                          color='#3498DB', edgecolor='black')
            ax3.set_xlabel('Number of Festivals', fontsize=11, fontweight='bold')
            ax3.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
            ax3.set_title(f'Festival Participation Distribution ({len(festival_films)} films)',
                         fontsize=12, fontweight='bold')
            ax3.grid(axis='y', alpha=0.3)

            # Add counts
            for i, (count, val) in enumerate(zip(count_dist.index, count_dist.values)):
                ax3.text(count, val + 1, f'{val}', ha='center', fontsize=9)
        else:
            ax3.text(0.5, 0.5, 'No Festival Films Found', ha='center', va='center',
                    transform=ax3.transAxes, fontsize=12)
            ax3.set_title('Festival Participation Distribution', fontsize=12, fontweight='bold')

        # 4. Big 3 festivals (Cannes, Venice, Berlin)
        ax4 = axes[1, 1]
        big3_data = {
            'Cannes': self.df['has_cannes'].sum(),
            'Venice': self.df['has_venice'].sum(),
            'Berlin': self.df['has_berlin'].sum(),
            'Sundance': self.df['has_sundance'].sum(),
            'Toronto': self.df['has_toronto'].sum()
        }

        colors_bar = ['#E74C3C', '#3498DB', '#2ECC71', '#F39C12', '#9B59B6']
        bars = ax4.bar(list(big3_data.keys()), list(big3_data.values()),
                      color=colors_bar, edgecolor='black')
        ax4.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
        ax4.set_title('Top 5 Prestigious Festivals', fontsize=12, fontweight='bold')
        ax4.grid(axis='y', alpha=0.3)

        for bar, val in zip(bars, big3_data.values()):
            height = bar.get_height()
            pct = (val / len(self.df)) * 100
            ax4.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                    f'{val}\n({pct:.1f}%)', ha='center', va='bottom', fontsize=9)

        plt.tight_layout()
        output_path = self.output_dir / 'festival_participation.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_festival_prestige(self):
        """Visualization 2: Festival prestige ranking and analysis."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Festival Prestige Analysis', fontsize=16, fontweight='bold')

        # 1. Prestige score distribution
        ax1 = axes[0, 0]
        festival_films = self.df[self.df['has_festival'] & (self.df['festival_prestige_score'] > 0)]

        if len(festival_films) > 0:
            ax1.hist(festival_films['festival_prestige_score'], bins=20,
                    color='#E74C3C', edgecolor='black', alpha=0.7)
            ax1.set_xlabel('Festival Prestige Score', fontsize=11, fontweight='bold')
            ax1.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
            ax1.set_title(f'Prestige Score Distribution ({len(festival_films)} films)',
                         fontsize=12, fontweight='bold')
            ax1.grid(axis='y', alpha=0.3)

            # Add mean and median
            mean_score = festival_films['festival_prestige_score'].mean()
            median_score = festival_films['festival_prestige_score'].median()
            ax1.axvline(mean_score, color='red', linestyle='--', linewidth=2,
                       label=f'Mean: {mean_score:.1f}')
            ax1.axvline(median_score, color='green', linestyle='--', linewidth=2,
                       label=f'Median: {median_score:.1f}')
            ax1.legend(fontsize=9)

        # 2. Top prestige films
        ax2 = axes[0, 1]
        if len(festival_films) > 0:
            top_prestige = festival_films.nlargest(15, 'festival_prestige_score')
            y_pos = np.arange(len(top_prestige))
            bars = ax2.barh(y_pos, top_prestige['festival_prestige_score'].values,
                           color=plt.cm.YlOrRd(top_prestige['festival_prestige_score'].values /
                                              top_prestige['festival_prestige_score'].max()),
                           edgecolor='black')
            ax2.set_yticks(y_pos)
            ax2.set_yticklabels([f"{title[:30]}..." if len(title) > 30 else title
                                for title in top_prestige['title'].values], fontsize=9)
            ax2.set_xlabel('Prestige Score', fontsize=11, fontweight='bold')
            ax2.set_title('Highest Prestige Films', fontsize=12, fontweight='bold')
            ax2.invert_yaxis()
            ax2.grid(axis='x', alpha=0.3)

            # Add festival count
            for i, (score, count) in enumerate(zip(top_prestige['festival_prestige_score'].values,
                                                   top_prestige['festival_count'].values)):
                ax2.text(score + 0.5, i, f'{int(count)} fest.',
                        va='center', fontsize=8)

        # 3. Prestige vs rating
        ax3 = axes[1, 0]
        if len(festival_films) > 0:
            rated_fest = festival_films[festival_films['imdb_rating'] > 0]
            if len(rated_fest) > 0:
                scatter = ax3.scatter(rated_fest['festival_prestige_score'],
                                     rated_fest['imdb_rating'],
                                     c=rated_fest['festival_count'], cmap='viridis',
                                     s=100, alpha=0.6, edgecolors='black')
                ax3.set_xlabel('Festival Prestige Score', fontsize=11, fontweight='bold')
                ax3.set_ylabel('IMDb Rating', fontsize=11, fontweight='bold')
                ax3.set_title('Prestige vs Quality Correlation', fontsize=12, fontweight='bold')
                ax3.grid(True, alpha=0.3)

                cbar = plt.colorbar(scatter, ax=ax3)
                cbar.set_label('Festival Count', fontsize=10)

                # Add correlation
                if len(rated_fest) >= 2:
                    corr, p_val = stats.pearsonr(rated_fest['festival_prestige_score'],
                                                rated_fest['imdb_rating'])
                    ax3.text(0.05, 0.95, f'Correlation: {corr:.3f}\np-value: {p_val:.4f}',
                            transform=ax3.transAxes, fontsize=10, verticalalignment='top',
                            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        # 4. Prestige by decade
        ax4 = axes[1, 1]
        if 'release_year' in self.df.columns and len(festival_films) > 0:
            self.df['decade'] = (self.df['release_year'] // 10) * 10
            decade_prestige = festival_films.groupby(
                festival_films['release_year'] // 10 * 10
            )['festival_prestige_score'].mean().sort_index()

            if len(decade_prestige) > 0:
                x = np.arange(len(decade_prestige))
                bars = ax4.bar(x, decade_prestige.values, color='#E74C3C', edgecolor='black')
                ax4.set_xticks(x)
                ax4.set_xticklabels([f"{int(d)}s" for d in decade_prestige.index], rotation=45)
                ax4.set_xlabel('Decade', fontsize=11, fontweight='bold')
                ax4.set_ylabel('Average Prestige Score', fontsize=11, fontweight='bold')
                ax4.set_title('Festival Prestige Trends', fontsize=12, fontweight='bold')
                ax4.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        output_path = self.output_dir / 'festival_prestige.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_festival_genre_preferences(self):
        """Visualization 3: Genre preferences at different festivals."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Festival Genre Preferences', fontsize=16, fontweight='bold')

        if 'tmdb_genres' not in self.df.columns:
            fig.text(0.5, 0.5, 'Genre data not available',
                    ha='center', va='center', fontsize=14)
            plt.tight_layout()
            output_path = self.output_dir / 'festival_genre_preferences.png'
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"Saved: {output_path}")
            return

        # Extract genre-festival data
        genre_festival_data = []

        for idx, row in self.df.iterrows():
            if row['has_festival'] and pd.notna(row['tmdb_genres']):
                try:
                    import ast
                    genres = ast.literal_eval(row['tmdb_genres'])
                    if isinstance(genres, list):
                        for genre in genres:
                            for festival in row['festival_list']:
                                genre_festival_data.append({
                                    'genre': genre,
                                    'festival': festival
                                })
                except:
                    pass

        if not genre_festival_data:
            fig.text(0.5, 0.5, 'No genre-festival data available',
                    ha='center', va='center', fontsize=14)
            plt.tight_layout()
            output_path = self.output_dir / 'festival_genre_preferences.png'
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"Saved: {output_path}")
            return

        gf_df = pd.DataFrame(genre_festival_data)

        # 1. Top genres at festivals overall
        ax1 = axes[0, 0]
        genre_counts = gf_df['genre'].value_counts().head(12)

        y_pos = np.arange(len(genre_counts))
        bars = ax1.barh(y_pos, genre_counts.values,
                       color=plt.cm.Spectral(np.linspace(0, 1, len(genre_counts))))
        ax1.set_yticks(y_pos)
        ax1.set_yticklabels(genre_counts.index, fontsize=10)
        ax1.set_xlabel('Number of Festival Appearances', fontsize=11, fontweight='bold')
        ax1.set_title('Most Common Genres at Festivals', fontsize=12, fontweight='bold')
        ax1.invert_yaxis()
        ax1.grid(axis='x', alpha=0.3)

        # 2. Cannes genre preferences
        ax2 = axes[0, 1]
        cannes_genres = gf_df[gf_df['festival'] == 'Cannes']['genre'].value_counts().head(10)

        if len(cannes_genres) > 0:
            y_pos = np.arange(len(cannes_genres))
            bars = ax2.barh(y_pos, cannes_genres.values, color='#E74C3C', edgecolor='black')
            ax2.set_yticks(y_pos)
            ax2.set_yticklabels(cannes_genres.index, fontsize=10)
            ax2.set_xlabel('Number of Films', fontsize=11, fontweight='bold')
            ax2.set_title('Cannes Film Festival - Top Genres', fontsize=12, fontweight='bold')
            ax2.invert_yaxis()
            ax2.grid(axis='x', alpha=0.3)
        else:
            ax2.text(0.5, 0.5, 'No Cannes Data', ha='center', va='center',
                    transform=ax2.transAxes, fontsize=12)
            ax2.set_title('Cannes Film Festival - Top Genres', fontsize=12, fontweight='bold')

        # 3. Sundance genre preferences
        ax3 = axes[1, 0]
        sundance_genres = gf_df[gf_df['festival'] == 'Sundance']['genre'].value_counts().head(10)

        if len(sundance_genres) > 0:
            y_pos = np.arange(len(sundance_genres))
            bars = ax3.barh(y_pos, sundance_genres.values, color='#F39C12', edgecolor='black')
            ax3.set_yticks(y_pos)
            ax3.set_yticklabels(sundance_genres.index, fontsize=10)
            ax3.set_xlabel('Number of Films', fontsize=11, fontweight='bold')
            ax3.set_title('Sundance Film Festival - Top Genres', fontsize=12, fontweight='bold')
            ax3.invert_yaxis()
            ax3.grid(axis='x', alpha=0.3)
        else:
            ax3.text(0.5, 0.5, 'No Sundance Data', ha='center', va='center',
                    transform=ax3.transAxes, fontsize=12)
            ax3.set_title('Sundance Film Festival - Top Genres', fontsize=12, fontweight='bold')

        # 4. Genre-festival heatmap (top festivals x top genres)
        ax4 = axes[1, 1]
        top_festivals = gf_df['festival'].value_counts().head(8).index
        top_genres = gf_df['genre'].value_counts().head(8).index

        # Create cross-tabulation
        heatmap_data = pd.crosstab(
            gf_df[gf_df['festival'].isin(top_festivals)]['festival'],
            gf_df[gf_df['genre'].isin(top_genres)]['genre']
        )

        if not heatmap_data.empty:
            im = ax4.imshow(heatmap_data.values, cmap='YlOrRd', aspect='auto')
            ax4.set_xticks(np.arange(len(heatmap_data.columns)))
            ax4.set_yticks(np.arange(len(heatmap_data.index)))
            ax4.set_xticklabels(heatmap_data.columns, rotation=45, ha='right', fontsize=9)
            ax4.set_yticklabels(heatmap_data.index, fontsize=9)
            ax4.set_title('Genre Distribution Across Festivals', fontsize=12, fontweight='bold')

            # Add values
            for i in range(len(heatmap_data.index)):
                for j in range(len(heatmap_data.columns)):
                    val = heatmap_data.values[i, j]
                    if val > 0:
                        text = ax4.text(j, i, int(val),
                                       ha="center", va="center",
                                       color="white" if val > heatmap_data.values.max()/2 else "black",
                                       fontsize=8, fontweight='bold')

            cbar = plt.colorbar(im, ax=ax4)
            cbar.set_label('Film Count', fontsize=10)

        plt.tight_layout()
        output_path = self.output_dir / 'festival_genre_preferences.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_festival_to_oscar_pipeline(self):
        """Visualization 4: Festival success to Oscar pipeline analysis."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Festival → Oscar Pipeline Analysis', fontsize=16, fontweight='bold')

        # Parse Oscar data if not already done
        if 'has_oscar' not in self.df.columns:
            self.df['has_oscar'] = self.df['omdb_awards'].str.contains('Oscar', case=False, na=False)
        if 'oscar_wins' not in self.df.columns:
            self.df['oscar_wins'] = 0
            for idx, row in self.df.iterrows():
                if pd.notna(row['omdb_awards']):
                    oscar_won = re.search(r'Won\s+(\d+)\s+Oscar', str(row['omdb_awards']))
                    if oscar_won:
                        self.df.at[idx, 'oscar_wins'] = int(oscar_won.group(1))

        # 1. Festival vs Oscar overlap
        ax1 = axes[0, 0]
        categories = ['Festival Only', 'Oscar Only', 'Both', 'Neither']
        values = [
            ((self.df['has_festival']) & (~self.df['has_oscar'])).sum(),
            ((~self.df['has_festival']) & (self.df['has_oscar'])).sum(),
            ((self.df['has_festival']) & (self.df['has_oscar'])).sum(),
            ((~self.df['has_festival']) & (~self.df['has_oscar'])).sum()
        ]

        colors_bar = ['#E74C3C', 'gold', '#FF6347', '#E0E0E0']
        bars = ax1.bar(categories, values, color=colors_bar, edgecolor='black')
        ax1.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
        ax1.set_title('Festival vs Oscar Distribution', fontsize=12, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)

        for bar, val in zip(bars, values):
            height = bar.get_height()
            pct = (val / len(self.df)) * 100
            ax1.text(bar.get_x() + bar.get_width()/2., height + 5,
                    f'{val}\n({pct:.1f}%)', ha='center', va='bottom', fontsize=9)

        # 2. Festival prestige vs Oscar wins
        ax2 = axes[0, 1]
        festival_oscar_films = self.df[(self.df['has_festival']) & (self.df['oscar_wins'] > 0)]

        if len(festival_oscar_films) > 0:
            scatter = ax2.scatter(festival_oscar_films['festival_prestige_score'],
                                 festival_oscar_films['oscar_wins'],
                                 c=festival_oscar_films['imdb_rating'],
                                 cmap='YlOrRd', s=100, alpha=0.6, edgecolors='black')
            ax2.set_xlabel('Festival Prestige Score', fontsize=11, fontweight='bold')
            ax2.set_ylabel('Oscar Wins', fontsize=11, fontweight='bold')
            ax2.set_title(f'Festival Prestige vs Oscar Success ({len(festival_oscar_films)} films)',
                         fontsize=12, fontweight='bold')
            ax2.grid(True, alpha=0.3)

            cbar = plt.colorbar(scatter, ax=ax2)
            cbar.set_label('IMDb Rating', fontsize=10)
        else:
            ax2.text(0.5, 0.5, 'No Films with Both Festival & Oscar', ha='center', va='center',
                    transform=ax2.transAxes, fontsize=12)
            ax2.set_title('Festival Prestige vs Oscar Success', fontsize=12, fontweight='bold')

        # 3. Oscar success rate by festival
        ax3 = axes[1, 0]
        festival_oscar_rates = []

        for festival in ['Cannes', 'Venice', 'Berlin', 'Sundance', 'Toronto']:
            festival_col = f'has_{festival.lower()}'
            if festival_col in self.df.columns:
                festival_films = self.df[self.df[festival_col]]
                if len(festival_films) > 0:
                    oscar_rate = (festival_films['has_oscar'].sum() / len(festival_films)) * 100
                    festival_oscar_rates.append({
                        'festival': festival,
                        'oscar_rate': oscar_rate,
                        'total_films': len(festival_films)
                    })

        if festival_oscar_rates:
            fest_df = pd.DataFrame(festival_oscar_rates)
            colors_bar = ['#E74C3C', '#3498DB', '#2ECC71', '#F39C12', '#9B59B6']

            bars = ax3.bar(fest_df['festival'], fest_df['oscar_rate'],
                          color=colors_bar[:len(fest_df)], edgecolor='black')
            ax3.set_ylabel('Oscar Win Rate (%)', fontsize=11, fontweight='bold')
            ax3.set_title('Oscar Success Rate by Festival', fontsize=12, fontweight='bold')
            ax3.grid(axis='y', alpha=0.3)

            # Add film counts
            for i, (bar, row) in enumerate(zip(bars, fest_df.itertuples())):
                height = bar.get_height()
                ax3.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                        f'{row.oscar_rate:.1f}%\n(n={row.total_films})',
                        ha='center', va='bottom', fontsize=8)

        # 4. Top festival-to-Oscar successes
        ax4 = axes[1, 1]
        if len(festival_oscar_films) > 0:
            top_pipeline = festival_oscar_films.nlargest(12, 'oscar_wins')
            y_pos = np.arange(len(top_pipeline))
            bars = ax4.barh(y_pos, top_pipeline['oscar_wins'].values,
                           color='gold', edgecolor='black')
            ax4.set_yticks(y_pos)
            ax4.set_yticklabels([f"{title[:30]}..." if len(title) > 30 else title
                                for title in top_pipeline['title'].values], fontsize=9)
            ax4.set_xlabel('Oscar Wins', fontsize=11, fontweight='bold')
            ax4.set_title('Top Festival → Oscar Success Stories', fontsize=12, fontweight='bold')
            ax4.invert_yaxis()
            ax4.grid(axis='x', alpha=0.3)

            # Add prestige scores
            for i, prestige in enumerate(top_pipeline['festival_prestige_score'].values):
                ax4.text(top_pipeline['oscar_wins'].values[i] + 0.2, i,
                        f'P:{int(prestige)}', va='center', fontsize=8, color='red')

        plt.tight_layout()
        output_path = self.output_dir / 'festival_oscar_pipeline.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_festival_darlings(self):
        """Visualization 5: Directors who frequently appear at festivals."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Festival Darlings: Directors & Repeated Success', fontsize=16, fontweight='bold')

        if 'tmdb_director' not in self.df.columns:
            fig.text(0.5, 0.5, 'Director data not available',
                    ha='center', va='center', fontsize=14)
            plt.tight_layout()
            output_path = self.output_dir / 'festival_darlings.png'
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"Saved: {output_path}")
            return

        # Extract director-festival data
        director_festival_data = []
        festival_films = self.df[self.df['has_festival']]

        for idx, row in festival_films.iterrows():
            if pd.notna(row['tmdb_director']):
                director = str(row['tmdb_director']).strip()
                if director and director != 'Unknown':
                    director_festival_data.append({
                        'director': director,
                        'festival_count': row['festival_count'],
                        'prestige_score': row['festival_prestige_score'],
                        'rating': row['imdb_rating'] if 'imdb_rating' in row and pd.notna(row['imdb_rating']) else 0
                    })

        if not director_festival_data:
            fig.text(0.5, 0.5, 'No director-festival data available',
                    ha='center', va='center', fontsize=14)
            plt.tight_layout()
            output_path = self.output_dir / 'festival_darlings.png'
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"Saved: {output_path}")
            return

        dir_df = pd.DataFrame(director_festival_data)

        # 1. Directors with most festival films
        ax1 = axes[0, 0]
        director_counts = dir_df['director'].value_counts().head(15)

        y_pos = np.arange(len(director_counts))
        bars = ax1.barh(y_pos, director_counts.values,
                       color=plt.cm.Spectral(np.linspace(0, 1, len(director_counts))))
        ax1.set_yticks(y_pos)
        ax1.set_yticklabels(director_counts.index, fontsize=9)
        ax1.set_xlabel('Number of Festival Films', fontsize=11, fontweight='bold')
        ax1.set_title('Directors with Most Festival Presence', fontsize=12, fontweight='bold')
        ax1.invert_yaxis()
        ax1.grid(axis='x', alpha=0.3)

        # 2. Directors with highest total prestige
        ax2 = axes[0, 1]
        director_prestige = dir_df.groupby('director')['prestige_score'].sum().sort_values(ascending=False).head(15)

        y_pos = np.arange(len(director_prestige))
        bars = ax2.barh(y_pos, director_prestige.values,
                       color=plt.cm.YlOrRd(director_prestige.values / director_prestige.max()),
                       edgecolor='black')
        ax2.set_yticks(y_pos)
        ax2.set_yticklabels(director_prestige.index, fontsize=9)
        ax2.set_xlabel('Total Prestige Score', fontsize=11, fontweight='bold')
        ax2.set_title('Directors with Highest Festival Prestige', fontsize=12, fontweight='bold')
        ax2.invert_yaxis()
        ax2.grid(axis='x', alpha=0.3)

        # 3. Average prestige per film by director
        ax3 = axes[1, 0]
        director_stats = dir_df.groupby('director').agg({
            'prestige_score': ['sum', 'count', 'mean']
        })
        director_stats.columns = ['total_prestige', 'film_count', 'avg_prestige']
        director_stats = director_stats[director_stats['film_count'] >= 3]  # Min 3 films
        director_stats = director_stats.sort_values('avg_prestige', ascending=False).head(12)

        if len(director_stats) > 0:
            y_pos = np.arange(len(director_stats))
            bars = ax3.barh(y_pos, director_stats['avg_prestige'].values,
                           color='#3498DB', edgecolor='black')
            ax3.set_yticks(y_pos)
            ax3.set_yticklabels(director_stats.index, fontsize=9)
            ax3.set_xlabel('Average Prestige per Film', fontsize=11, fontweight='bold')
            ax3.set_title('Highest Quality Festival Directors (min 3 films)',
                         fontsize=12, fontweight='bold')
            ax3.invert_yaxis()
            ax3.grid(axis='x', alpha=0.3)

            # Add film counts
            for i, (prestige, count) in enumerate(zip(director_stats['avg_prestige'].values,
                                                     director_stats['film_count'].values)):
                ax3.text(prestige + 0.2, i, f'n={int(count)}',
                        va='center', fontsize=8)

        # 4. Director festival success vs quality
        ax4 = axes[1, 1]
        director_quality = dir_df.groupby('director').agg({
            'rating': 'mean',
            'prestige_score': 'sum',
            'director': 'count'
        })
        director_quality.columns = ['avg_rating', 'total_prestige', 'film_count']
        director_quality = director_quality[
            (director_quality['film_count'] >= 2) &
            (director_quality['avg_rating'] > 0)
        ]

        if len(director_quality) > 0:
            scatter = ax4.scatter(director_quality['avg_rating'],
                                 director_quality['total_prestige'],
                                 c=director_quality['film_count'],
                                 cmap='viridis', s=100, alpha=0.6, edgecolors='black')
            ax4.set_xlabel('Average IMDb Rating', fontsize=11, fontweight='bold')
            ax4.set_ylabel('Total Prestige Score', fontsize=11, fontweight='bold')
            ax4.set_title('Director Quality vs Festival Success (min 2 films)',
                         fontsize=12, fontweight='bold')
            ax4.grid(True, alpha=0.3)

            cbar = plt.colorbar(scatter, ax=ax4)
            cbar.set_label('Film Count', fontsize=10)

        plt.tight_layout()
        output_path = self.output_dir / 'festival_darlings.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_festival_quality_indicators(self):
        """Visualization 6: Quality indicators for festival films."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Festival Film Quality Analysis', fontsize=16, fontweight='bold')

        # 1. Rating distribution: festival vs non-festival
        ax1 = axes[0, 0]
        festival_ratings = self.df[self.df['has_festival'] & (self.df['imdb_rating'] > 0)]['imdb_rating']
        non_festival_ratings = self.df[(~self.df['has_festival']) & (self.df['imdb_rating'] > 0)]['imdb_rating']

        if len(festival_ratings) > 0 and len(non_festival_ratings) > 0:
            ax1.hist([non_festival_ratings, festival_ratings], bins=20,
                    label=['Non-Festival', 'Festival'], color=['lightgray', '#E74C3C'],
                    edgecolor='black', alpha=0.7)
            ax1.set_xlabel('IMDb Rating', fontsize=11, fontweight='bold')
            ax1.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
            ax1.set_title('Rating Distribution Comparison', fontsize=12, fontweight='bold')
            ax1.legend(fontsize=10)
            ax1.grid(axis='y', alpha=0.3)

            # Add means
            ax1.axvline(non_festival_ratings.mean(), color='gray',
                       linestyle='--', linewidth=2)
            ax1.axvline(festival_ratings.mean(), color='darkred',
                       linestyle='--', linewidth=2)

        # 2. Boxplot comparison
        ax2 = axes[0, 1]
        if len(festival_ratings) > 0 and len(non_festival_ratings) > 0:
            box_data = [non_festival_ratings, festival_ratings]
            bp = ax2.boxplot(box_data, tick_labels=['Non-Festival', 'Festival'],
                            patch_artist=True, notch=True)

            for patch, color in zip(bp['boxes'], ['lightgray', '#E74C3C']):
                patch.set_facecolor(color)

            ax2.set_ylabel('IMDb Rating', fontsize=11, fontweight='bold')
            ax2.set_title('Quality Comparison: Festival vs Non-Festival',
                         fontsize=12, fontweight='bold')
            ax2.grid(axis='y', alpha=0.3)

            # Add statistical test
            t_stat, p_value = stats.ttest_ind(festival_ratings, non_festival_ratings)
            ax2.text(0.05, 0.95,
                    f'Festival mean: {festival_ratings.mean():.2f}\n'
                    f'Non-festival mean: {non_festival_ratings.mean():.2f}\n'
                    f'p-value: {p_value:.4f}',
                    transform=ax2.transAxes, fontsize=9, verticalalignment='top',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        # 3. Rating by prestige tier
        ax3 = axes[1, 0]
        festival_films_rated = self.df[self.df['has_festival'] & (self.df['imdb_rating'] > 0)]

        if len(festival_films_rated) > 0:
            # Create prestige tiers
            festival_films_rated = festival_films_rated.copy()
            festival_films_rated['prestige_tier'] = pd.cut(
                festival_films_rated['festival_prestige_score'],
                bins=[0, 5, 10, 20, 100],
                labels=['Low (0-5)', 'Medium (6-10)', 'High (11-20)', 'Very High (20+)']
            )

            tier_ratings = [
                festival_films_rated[festival_films_rated['prestige_tier'] == tier]['imdb_rating']
                for tier in ['Low (0-5)', 'Medium (6-10)', 'High (11-20)', 'Very High (20+)']
                if len(festival_films_rated[festival_films_rated['prestige_tier'] == tier]) > 0
            ]

            if tier_ratings:
                bp = ax3.boxplot(tier_ratings,
                                tick_labels=['Low', 'Medium', 'High', 'Very High'][:len(tier_ratings)],
                                patch_artist=True, notch=True)

                colors = plt.cm.YlOrRd(np.linspace(0.3, 0.9, len(tier_ratings)))
                for patch, color in zip(bp['boxes'], colors):
                    patch.set_facecolor(color)

                ax3.set_xlabel('Prestige Tier', fontsize=11, fontweight='bold')
                ax3.set_ylabel('IMDb Rating', fontsize=11, fontweight='bold')
                ax3.set_title('Rating by Festival Prestige Tier', fontsize=12, fontweight='bold')
                ax3.grid(axis='y', alpha=0.3)

        # 4. Festival count vs rating
        ax4 = axes[1, 1]
        multi_festival = self.df[(self.df['festival_count'] > 0) & (self.df['imdb_rating'] > 0)]

        if len(multi_festival) > 0:
            scatter = ax4.scatter(multi_festival['festival_count'],
                                 multi_festival['imdb_rating'],
                                 c=multi_festival['festival_prestige_score'],
                                 cmap='YlOrRd', s=100, alpha=0.6, edgecolors='black')
            ax4.set_xlabel('Number of Festivals', fontsize=11, fontweight='bold')
            ax4.set_ylabel('IMDb Rating', fontsize=11, fontweight='bold')
            ax4.set_title('Festival Participation vs Quality', fontsize=12, fontweight='bold')
            ax4.grid(True, alpha=0.3)

            cbar = plt.colorbar(scatter, ax=ax4)
            cbar.set_label('Prestige Score', fontsize=10)

            # Add correlation
            if len(multi_festival) >= 2:
                corr, p_val = stats.pearsonr(multi_festival['festival_count'],
                                            multi_festival['imdb_rating'])
                ax4.text(0.05, 0.95, f'Correlation: {corr:.3f}\np-value: {p_val:.4f}',
                        transform=ax4.transAxes, fontsize=10, verticalalignment='top',
                        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        plt.tight_layout()
        output_path = self.output_dir / 'festival_quality_indicators.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_festival_trends(self):
        """Visualization 7: Festival participation trends over time."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Festival Circuit Trends Over Time', fontsize=16, fontweight='bold')

        if 'release_year' not in self.df.columns:
            fig.text(0.5, 0.5, 'Year data not available',
                    ha='center', va='center', fontsize=14)
            plt.tight_layout()
            output_path = self.output_dir / 'festival_trends.png'
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"Saved: {output_path}")
            return

        # 1. Festival participation rate by decade
        ax1 = axes[0, 0]
        if 'decade' not in self.df.columns:
            self.df['decade'] = (self.df['release_year'] // 10) * 10

        decade_festival = self.df.groupby('decade').agg({
            'has_festival': ['sum', 'count']
        })
        decade_festival.columns = ['festival_films', 'total_films']
        decade_festival['festival_rate'] = (decade_festival['festival_films'] /
                                           decade_festival['total_films'] * 100)

        x = np.arange(len(decade_festival))
        bars = ax1.bar(x, decade_festival['festival_rate'].values,
                      color='#E74C3C', edgecolor='black')
        ax1.set_xticks(x)
        ax1.set_xticklabels([f"{int(d)}s" for d in decade_festival.index], rotation=45)
        ax1.set_xlabel('Decade', fontsize=11, fontweight='bold')
        ax1.set_ylabel('% with Festival Presence', fontsize=11, fontweight='bold')
        ax1.set_title('Festival Participation Rate by Decade', fontsize=12, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)

        # Add counts
        for i, (rate, count) in enumerate(zip(decade_festival['festival_rate'].values,
                                              decade_festival['festival_films'].values)):
            ax1.text(i, rate + 0.5, f'{int(count)}', ha='center', fontsize=8)

        # 2. Average festival count per film over time
        ax2 = axes[0, 1]
        festival_films_only = self.df[self.df['has_festival']]
        decade_avg_count = festival_films_only.groupby('decade')['festival_count'].mean()

        x = np.arange(len(decade_avg_count))
        bars = ax2.bar(x, decade_avg_count.values, color='#3498DB', edgecolor='black')
        ax2.set_xticks(x)
        ax2.set_xticklabels([f"{int(d)}s" for d in decade_avg_count.index], rotation=45)
        ax2.set_xlabel('Decade', fontsize=11, fontweight='bold')
        ax2.set_ylabel('Average Festivals per Film', fontsize=11, fontweight='bold')
        ax2.set_title('Festival Intensity Over Time', fontsize=12, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        # 3. Major festival trends
        ax3 = axes[1, 0]
        decade_big_festivals = self.df.groupby('decade').agg({
            'has_cannes': 'sum',
            'has_venice': 'sum',
            'has_berlin': 'sum',
            'has_sundance': 'sum'
        })

        x = np.arange(len(decade_big_festivals))
        width = 0.2

        ax3.bar(x - 1.5*width, decade_big_festivals['has_cannes'], width,
               label='Cannes', color='#E74C3C')
        ax3.bar(x - 0.5*width, decade_big_festivals['has_venice'], width,
               label='Venice', color='#3498DB')
        ax3.bar(x + 0.5*width, decade_big_festivals['has_berlin'], width,
               label='Berlin', color='#2ECC71')
        ax3.bar(x + 1.5*width, decade_big_festivals['has_sundance'], width,
               label='Sundance', color='#F39C12')

        ax3.set_xticks(x)
        ax3.set_xticklabels([f"{int(d)}s" for d in decade_big_festivals.index], rotation=45)
        ax3.set_xlabel('Decade', fontsize=11, fontweight='bold')
        ax3.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
        ax3.set_title('Major Festival Trends by Decade', fontsize=12, fontweight='bold')
        ax3.legend(fontsize=9)
        ax3.grid(axis='y', alpha=0.3)

        # 4. Recent years trend (last 30 years)
        ax4 = axes[1, 1]
        recent_years = self.df[self.df['release_year'] >= self.df['release_year'].max() - 30]
        year_festival = recent_years.groupby('release_year').agg({
            'has_festival': ['sum', 'count']
        })
        year_festival.columns = ['festival_films', 'total_films']
        year_festival = year_festival[year_festival['total_films'] >= 5]  # Min 5 films/year
        year_festival['festival_rate'] = (year_festival['festival_films'] /
                                         year_festival['total_films'] * 100)

        if len(year_festival) > 0:
            ax4.plot(year_festival.index, year_festival['festival_rate'].values,
                    linewidth=2, color='#E74C3C', marker='o', markersize=4)
            ax4.set_xlabel('Year', fontsize=11, fontweight='bold')
            ax4.set_ylabel('% with Festival Presence', fontsize=11, fontweight='bold')
            ax4.set_title('Recent Festival Participation Trend (min 5 films/year)',
                         fontsize=12, fontweight='bold')
            ax4.grid(True, alpha=0.3)

            # Add trend line
            z = np.polyfit(year_festival.index, year_festival['festival_rate'].values, 1)
            p = np.poly1d(z)
            ax4.plot(year_festival.index, p(year_festival.index),
                    "--", color='blue', alpha=0.5, linewidth=2, label='Trend')
            ax4.legend(fontsize=9)

        plt.tight_layout()
        output_path = self.output_dir / 'festival_trends.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_coverage_analysis(self):
        """Visualization 8: Festival data coverage and completeness."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Festival Data Coverage Analysis', fontsize=16, fontweight='bold')

        # 1. Overall coverage
        ax1 = axes[0, 0]
        coverage_data = {
            'Has Festival Data': self.df['has_festival'].sum(),
            'No Festival Data': (~self.df['has_festival']).sum()
        }

        colors_pie = ['#E74C3C', '#E0E0E0']
        wedges, texts, autotexts = ax1.pie(coverage_data.values(), labels=coverage_data.keys(),
                                           autopct='%1.1f%%', colors=colors_pie, startangle=90)
        ax1.set_title(f'Overall Festival Coverage ({len(self.df)} films)',
                     fontsize=12, fontweight='bold')

        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(11)

        # 2. Coverage by decade
        ax2 = axes[0, 1]
        if 'decade' in self.df.columns:
            decade_coverage = self.df.groupby('decade')['has_festival'].agg(['sum', 'count'])
            decade_coverage['pct'] = (decade_coverage['sum'] / decade_coverage['count'] * 100)

            x = np.arange(len(decade_coverage))
            bars = ax2.bar(x, decade_coverage['pct'].values, color='skyblue', edgecolor='black')
            ax2.set_xticks(x)
            ax2.set_xticklabels([f"{int(d)}s" for d in decade_coverage.index], rotation=45)
            ax2.set_xlabel('Decade', fontsize=11, fontweight='bold')
            ax2.set_ylabel('% with Festival Data', fontsize=11, fontweight='bold')
            ax2.set_title('Festival Coverage by Decade', fontsize=12, fontweight='bold')
            ax2.grid(axis='y', alpha=0.3)

            # Add counts
            for i, (pct, count) in enumerate(zip(decade_coverage['pct'].values,
                                                 decade_coverage['sum'].values)):
                ax2.text(i, pct + 1, f'{int(count)}', ha='center', fontsize=8)

        # 3. Data source breakdown
        ax3 = axes[1, 0]
        # Count films with festivals found in awards vs keywords
        from_awards = 0
        from_keywords = 0
        from_both = 0

        for idx, row in self.df[self.df['has_festival']].iterrows():
            in_awards = False
            in_keywords = False

            if pd.notna(row['omdb_awards']):
                for pattern in self.festival_patterns.values():
                    if re.search(pattern, str(row['omdb_awards']), re.IGNORECASE):
                        in_awards = True
                        break

            if 'tmdb_keywords' in self.df.columns and pd.notna(row['tmdb_keywords']):
                try:
                    import ast
                    keywords = ast.literal_eval(row['tmdb_keywords'])
                    if isinstance(keywords, list):
                        keywords_str = ' '.join([str(k).lower() for k in keywords])
                        for pattern in self.festival_patterns.values():
                            if re.search(pattern, keywords_str, re.IGNORECASE):
                                in_keywords = True
                                break
                except:
                    pass

            if in_awards and in_keywords:
                from_both += 1
            elif in_awards:
                from_awards += 1
            elif in_keywords:
                from_keywords += 1

        source_data = {
            'Awards Only': from_awards,
            'Keywords Only': from_keywords,
            'Both Sources': from_both
        }

        colors_bar = ['gold', '#3498DB', '#2ECC71']
        bars = ax3.bar(list(source_data.keys()), list(source_data.values()),
                      color=colors_bar, edgecolor='black')
        ax3.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
        ax3.set_title('Festival Data Source Distribution', fontsize=12, fontweight='bold')
        ax3.grid(axis='y', alpha=0.3)

        for bar, val in zip(bars, source_data.values()):
            height = bar.get_height()
            pct = (val / self.df['has_festival'].sum() * 100) if self.df['has_festival'].sum() > 0 else 0
            ax3.text(bar.get_x() + bar.get_width()/2., height + 1,
                    f'{val}\n({pct:.1f}%)', ha='center', va='bottom', fontsize=9)

        # 4. Festival detail completeness
        ax4 = axes[1, 1]
        festival_films = self.df[self.df['has_festival']]

        if len(festival_films) > 0:
            completeness_data = {
                '1 Festival': (festival_films['festival_count'] == 1).sum(),
                '2-3 Festivals': ((festival_films['festival_count'] >= 2) &
                                (festival_films['festival_count'] <= 3)).sum(),
                '4+ Festivals': (festival_films['festival_count'] >= 4).sum()
            }

            colors_pie = ['#95A5A6', '#3498DB', '#E74C3C']
            wedges, texts, autotexts = ax4.pie(completeness_data.values(),
                                               labels=completeness_data.keys(),
                                               autopct='%1.1f%%', colors=colors_pie,
                                               startangle=90)
            ax4.set_title(f'Festival Count Distribution ({len(festival_films)} films)',
                         fontsize=12, fontweight='bold')

            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontweight('bold')
                autotext.set_fontsize(10)

        plt.tight_layout()
        output_path = self.output_dir / 'coverage_analysis.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def generate_report(self):
        """Generate comprehensive analysis report."""
        report_path = self.report_dir / 'batch_30_festival_circuit_report.txt'

        with open(report_path, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("CINESCOPE BATCH 30: FESTIVAL CIRCUIT ANALYSIS\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # Overall statistics
            f.write("=" * 80 + "\n")
            f.write("OVERALL STATISTICS\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Total Films: {len(self.df)}\n\n")
            f.write(f"Films with Festival Data: {self.df['has_festival'].sum()} "
                   f"({self.df['has_festival'].sum() / len(self.df) * 100:.1f}%)\n\n")

            # Major festivals
            f.write("=" * 80 + "\n")
            f.write("MAJOR FESTIVAL REPRESENTATION\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Cannes: {self.df['has_cannes'].sum()} "
                   f"({self.df['has_cannes'].sum() / len(self.df) * 100:.1f}%)\n")
            f.write(f"Venice: {self.df['has_venice'].sum()} "
                   f"({self.df['has_venice'].sum() / len(self.df) * 100:.1f}%)\n")
            f.write(f"Berlin: {self.df['has_berlin'].sum()} "
                   f"({self.df['has_berlin'].sum() / len(self.df) * 100:.1f}%)\n")
            f.write(f"Sundance: {self.df['has_sundance'].sum()} "
                   f"({self.df['has_sundance'].sum() / len(self.df) * 100:.1f}%)\n")
            f.write(f"Toronto: {self.df['has_toronto'].sum()} "
                   f"({self.df['has_toronto'].sum() / len(self.df) * 100:.1f}%)\n\n")

            # Festival participation stats
            festival_films = self.df[self.df['has_festival']]
            if len(festival_films) > 0:
                f.write("=" * 80 + "\n")
                f.write("FESTIVAL PARTICIPATION PATTERNS\n")
                f.write("=" * 80 + "\n\n")

                count_dist = festival_films['festival_count'].value_counts().sort_index()
                for count, num_films in count_dist.items():
                    pct = (num_films / len(festival_films)) * 100
                    f.write(f"{int(count)} festival(s): {num_films} ({pct:.1f}%)\n")
                f.write("\n")

                f.write(f"Average festivals per film: {festival_films['festival_count'].mean():.2f}\n")
                f.write(f"Average prestige score: {festival_films['festival_prestige_score'].mean():.2f}\n\n")

            # Top prestige films
            if len(festival_films) > 0:
                f.write("=" * 80 + "\n")
                f.write("HIGHEST PRESTIGE FESTIVAL FILMS\n")
                f.write("=" * 80 + "\n\n")

                top_prestige = festival_films.nlargest(15, 'festival_prestige_score')
                for i, row in enumerate(top_prestige.itertuples(), 1):
                    festivals_str = ', '.join(row.festival_list) if row.festival_list else 'Unknown'
                    rating_str = f" - {row.imdb_rating:.1f}" if not pd.isna(row.imdb_rating) and row.imdb_rating > 0 else ""
                    f.write(f"{i}. {row.title} (Score: {int(row.festival_prestige_score)}) "
                           f"- {festivals_str}{rating_str}\n")
                f.write("\n")

            f.write("=" * 80 + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * 80 + "\n")

        print(f"Report generated: {report_path}")

    def run_all_analyses(self):
        """Execute all visualizations and generate report."""
        print("\n" + "=" * 80)
        print("CINESCOPE BATCH 30: FESTIVAL CIRCUIT ANALYSIS")
        print("=" * 80 + "\n")

        visualizations = [
            ("Festival Participation", self.visualize_festival_participation),
            ("Festival Prestige", self.visualize_festival_prestige),
            ("Festival Genre Preferences", self.visualize_festival_genre_preferences),
            ("Festival → Oscar Pipeline", self.visualize_festival_to_oscar_pipeline),
            ("Festival Darlings", self.visualize_festival_darlings),
            ("Festival Quality Indicators", self.visualize_festival_quality_indicators),
            ("Festival Trends", self.visualize_festival_trends),
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
        print(f"  Report: {self.report_dir / 'batch_30_festival_circuit_report.txt'}")


def main():
    """Main execution function."""
    analyzer = FestivalCircuitAnalyzer()
    analyzer.run_all_analyses()


if __name__ == '__main__':
    main()
