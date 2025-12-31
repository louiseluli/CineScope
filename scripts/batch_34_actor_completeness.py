"""
Batch 34: Actor Filmography Completeness Analysis
Comprehensive analysis with 20+ creative visualizations including:
- Top 5 actors detailed filmography breakdowns
- Career timeline visualizations
- Genre diversity analysis per actor
- Quality distribution charts
- Missing films ranked galleries
- Completion milestone tracking
- And much more creative analysis!
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict
import ast
from pathlib import Path
import numpy as np
import sqlite3
from datetime import datetime
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec

class ActorCompletenessAnalyzer:
    def __init__(self, db_path='data/processed/completeness.db',
                 watched_path='data/processed/watched_movies_master.csv'):
        """Initialize with completeness database."""

        print("=" * 80)
        print("ACTOR COMPLETENESS ANALYSIS")
        print("=" * 80)

        self.db_path = Path(db_path)
        self.watched_df = pd.read_csv(watched_path)

        if not self.db_path.exists():
            raise FileNotFoundError(
                f"Database not found at {db_path}. "
                "Please run scripts/utils/build_completeness_database.py first!"
            )

        # Connect to database
        self.conn = sqlite3.connect(self.db_path)

        # Load actor completeness data
        print("\nLoading actor completeness data from database...")
        self.actor_df = pd.read_sql_query(
            "SELECT * FROM actor_completeness ORDER BY completeness_pct DESC",
            self.conn
        )
        print(f"Loaded {len(self.actor_df)} actors")

        # Setup directories
        self.output_dir = Path('analysis_outputs/visualizations/batch_34')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir = Path('analysis_outputs/reports')
        self.report_dir.mkdir(parents=True, exist_ok=True)

        # Set style
        plt.style.use('seaborn-v0_8-darkgrid')
        self.colors = {
            'watched': '#2ecc71',
            'missing': '#e74c3c',
            'primary': '#3498db',
            'secondary': '#9b59b6',
            'accent': '#f39c12'
        }

    def _safe_eval(self, data_str):
        """Safely evaluate string representation of list."""
        try:
            return ast.literal_eval(data_str) if pd.notna(data_str) else []
        except:
            return []

    def create_overview_dashboard(self):
        """Create comprehensive overview dashboard."""
        print("\n1. Creating overview dashboard...")

        fig = plt.figure(figsize=(24, 16))
        gs = GridSpec(4, 4, figure=fig, hspace=0.4, wspace=0.4)

        # 1. Completeness Distribution
        ax1 = fig.add_subplot(gs[0, 0:2])
        bins = [0, 20, 40, 60, 80, 100]
        counts, edges = np.histogram(self.actor_df['completeness_pct'], bins=bins)
        colors_dist = ['#e74c3c', '#e67e22', '#f39c12', '#2ecc71', '#27ae60']
        ax1.bar(range(len(counts)), counts, color=colors_dist, alpha=0.7, edgecolor='black')
        ax1.set_xticks(range(len(counts)))
        ax1.set_xticklabels(['0-20%', '20-40%', '40-60%', '60-80%', '80-100%'])
        ax1.set_title('Actor Completeness Distribution', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Number of Actors')
        ax1.grid(axis='y', alpha=0.3)
        for i, count in enumerate(counts):
            ax1.text(i, count + 1, str(int(count)), ha='center', fontweight='bold')

        # 2. Top 10 Nearly Complete Actors
        ax2 = fig.add_subplot(gs[0, 2:4])
        nearly_complete = self.actor_df.nlargest(10, 'completeness_pct')
        y_pos = np.arange(len(nearly_complete))
        ax2.barh(y_pos, nearly_complete['watched_count'],
                color=self.colors['watched'], alpha=0.7, label='Watched')
        ax2.barh(y_pos, nearly_complete['missing_count'],
                left=nearly_complete['watched_count'],
                color=self.colors['missing'], alpha=0.7, label='Missing')
        ax2.set_yticks(y_pos)
        ax2.set_yticklabels(nearly_complete['actor_name'], fontsize=9)
        ax2.set_title('Top 10 Most Complete Actors', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Films')
        ax2.legend(loc='lower right')
        ax2.invert_yaxis()

        # Add percentage labels
        for i, (idx, row) in enumerate(nearly_complete.iterrows()):
            total = row['watched_count'] + row['missing_count']
            ax2.text(total + 1, i, f"{row['completeness_pct']:.1f}%",
                    va='center', fontweight='bold')

        # 3. Actors with Most Missing Films
        ax3 = fig.add_subplot(gs[1, 0:2])
        most_missing = self.actor_df.nlargest(10, 'missing_count')
        x_pos = np.arange(len(most_missing))
        ax3.bar(x_pos, most_missing['missing_count'], color=self.colors['missing'], alpha=0.7)
        ax3.set_xticks(x_pos)
        ax3.set_xticklabels(most_missing['actor_name'], rotation=45, ha='right', fontsize=9)
        ax3.set_title('Actors with Most Missing Films (Potential to Explore!)',
                     fontsize=14, fontweight='bold')
        ax3.set_ylabel('Missing Films')
        ax3.grid(axis='y', alpha=0.3)
        for i, count in enumerate(most_missing['missing_count']):
            ax3.text(i, count + 0.5, str(int(count)), ha='center', fontweight='bold')

        # 4. Average Ratings Comparison
        ax4 = fig.add_subplot(gs[1, 2:4])
        actors_sample = self.actor_df.head(20)
        x = np.arange(len(actors_sample))
        width = 0.35
        ax4.bar(x - width/2, actors_sample['watched_avg_rating'], width,
               label='Your Watched Avg', color=self.colors['watched'], alpha=0.7)
        ax4.bar(x + width/2, actors_sample['catalog_avg_rating'], width,
               label='Complete Filmography Avg', color=self.colors['primary'], alpha=0.7)
        ax4.set_xticks(x)
        ax4.set_xticklabels(actors_sample['actor_name'], rotation=45, ha='right', fontsize=8)
        ax4.set_title('Rating Comparison: Your Selections vs Complete Filmography',
                     fontsize=14, fontweight='bold')
        ax4.set_ylabel('Average IMDb Rating')
        ax4.legend()
        ax4.grid(axis='y', alpha=0.3)
        ax4.set_ylim(6, 9)

        # 5. Completion Milestones
        ax5 = fig.add_subplot(gs[2, 0:2])
        milestones = {
            'Completed (100%)': len(self.actor_df[self.actor_df['completeness_pct'] == 100]),
            'Nearly There (80-99%)': len(self.actor_df[(self.actor_df['completeness_pct'] >= 80) &
                                                       (self.actor_df['completeness_pct'] < 100)]),
            'Halfway (50-79%)': len(self.actor_df[(self.actor_df['completeness_pct'] >= 50) &
                                                  (self.actor_df['completeness_pct'] < 80)]),
            'Getting Started (20-49%)': len(self.actor_df[(self.actor_df['completeness_pct'] >= 20) &
                                                         (self.actor_df['completeness_pct'] < 50)]),
            'Just Beginning (<20%)': len(self.actor_df[self.actor_df['completeness_pct'] < 20])
        }
        colors_milestone = ['#27ae60', '#2ecc71', '#f39c12', '#e67e22', '#e74c3c']
        wedges, texts, autotexts = ax5.pie(milestones.values(), labels=milestones.keys(),
                                            autopct='%1.1f%%', colors=colors_milestone,
                                            startangle=90)
        ax5.set_title('Completion Milestones', fontsize=14, fontweight='bold')
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')

        # 6. Films Watched per Actor Distribution
        ax6 = fig.add_subplot(gs[2, 2:4])
        ax6.hist(self.actor_df['watched_count'], bins=20, color=self.colors['watched'],
                alpha=0.7, edgecolor='black')
        ax6.set_title('Distribution of Films Watched per Actor', fontsize=14, fontweight='bold')
        ax6.set_xlabel('Number of Films Watched')
        ax6.set_ylabel('Number of Actors')
        ax6.axvline(self.actor_df['watched_count'].median(), color='red',
                   linestyle='--', linewidth=2, label=f"Median: {self.actor_df['watched_count'].median():.0f}")
        ax6.legend()
        ax6.grid(axis='y', alpha=0.3)

        # 7. Summary Statistics
        ax7 = fig.add_subplot(gs[3, :])
        ax7.axis('off')

        stats_text = f"""
        COMPREHENSIVE ACTOR COMPLETENESS STATISTICS
        ═══════════════════════════════════════════════════════════════════════════════

        Total Actors Analyzed: {len(self.actor_df):,}

        Completion Status:
        • Fully Completed Actors (100%): {len(self.actor_df[self.actor_df['completeness_pct'] == 100]):,}
        • Nearly Complete (80-99%): {len(self.actor_df[(self.actor_df['completeness_pct'] >= 80) & (self.actor_df['completeness_pct'] < 100)]):,}
        • Average Completion Rate: {self.actor_df['completeness_pct'].mean():.1f}%
        • Median Completion Rate: {self.actor_df['completeness_pct'].median():.1f}%

        Films per Actor:
        • Average Films Watched per Actor: {self.actor_df['watched_count'].mean():.1f}
        • Maximum Films Watched (Single Actor): {self.actor_df['watched_count'].max():.0f}
        • Total Missing Films Identified: {self.actor_df['missing_count'].sum():,.0f}

        Quality Insights:
        • Your Average Rating: {self.actor_df['watched_avg_rating'].mean():.2f}
        • Complete Filmography Average: {self.actor_df['catalog_avg_rating'].mean():.2f}
        • Rating Difference: {(self.actor_df['watched_avg_rating'].mean() - self.actor_df['catalog_avg_rating'].mean()):.2f}
          {"(You watch higher quality!)" if self.actor_df['watched_avg_rating'].mean() > self.actor_df['catalog_avg_rating'].mean() else "(Room to explore more!)"}
        """

        ax7.text(0.05, 0.95, stats_text, transform=ax7.transAxes,
                fontsize=11, verticalalignment='top', fontfamily='monospace',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

        plt.savefig(self.output_dir / 'overview_dashboard.png',
                   dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        print("  ✓ Saved overview_dashboard.png")

    def create_top5_detailed_breakdown(self):
        """Create detailed individual breakdowns for top 5 actors."""
        print("\n2. Creating Top 5 Actors Detailed Breakdown...")

        # Get top 5 by completeness with significant filmography
        top5 = self.actor_df[self.actor_df['total_quality_films'] >= 15].nlargest(5, 'completeness_pct')

        for idx, (_, actor_row) in enumerate(top5.iterrows(), 1):
            print(f"  {idx}. Creating detailed breakdown for {actor_row['actor_name']}...")

            fig = plt.figure(figsize=(20, 12))
            gs = GridSpec(3, 3, figure=fig, hspace=0.4, wspace=0.4)

            actor_name = actor_row['actor_name']
            watched_films = self._safe_eval(actor_row['watched_films'])
            missing_films = self._safe_eval(actor_row['missing_films'])

            # Title
            fig.suptitle(f"Complete Filmography Analysis: {actor_name}",
                        fontsize=20, fontweight='bold', y=0.98)

            # 1. Completion Progress Bar
            ax1 = fig.add_subplot(gs[0, :])
            total = actor_row['total_quality_films']
            watched_count = actor_row['watched_count']
            missing_count = actor_row['missing_count']

            ax1.barh([0], [watched_count], color=self.colors['watched'],
                    height=0.5, label=f'Watched: {watched_count}')
            ax1.barh([0], [missing_count], left=[watched_count],
                    color=self.colors['missing'], height=0.5,
                    label=f'Missing: {missing_count}')
            ax1.set_xlim(0, total)
            ax1.set_ylim(-0.5, 0.5)
            ax1.set_yticks([])
            ax1.set_xlabel('Films', fontsize=12)
            ax1.set_title(f'Completeness: {actor_row["completeness_pct"]:.1f}% ({watched_count}/{total} films)',
                         fontsize=14, fontweight='bold', pad=20)
            ax1.legend(loc='upper right', fontsize=11)
            ax1.grid(axis='x', alpha=0.3)

            # Add percentage marker
            pct_pos = (watched_count / total) * total
            ax1.axvline(pct_pos, color='black', linestyle='--', linewidth=2)
            ax1.text(pct_pos, 0.3, f'{actor_row["completeness_pct"]:.1f}%',
                    ha='center', fontsize=12, fontweight='bold',
                    bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.7))

            # 2. Watched Films Timeline
            ax2 = fig.add_subplot(gs[1, 0:2])
            if watched_films:
                years = [f['year'] for f in watched_films if isinstance(f.get('year'), (int, float))]
                ratings = [f['rating'] for f in watched_films if isinstance(f.get('year'), (int, float))]

                if years and ratings:
                    scatter = ax2.scatter(years, ratings, s=100, c=ratings,
                                        cmap='RdYlGn', alpha=0.7, edgecolors='black',
                                        vmin=6, vmax=9)
                    ax2.set_xlabel('Year', fontsize=11)
                    ax2.set_ylabel('IMDb Rating', fontsize=11)
                    ax2.set_title('Your Watched Films Timeline', fontsize=12, fontweight='bold')
                    ax2.grid(True, alpha=0.3)
                    ax2.set_ylim(6, 9.5)
                    plt.colorbar(scatter, ax=ax2, label='Rating')

            # 3. Rating Distribution
            ax3 = fig.add_subplot(gs[1, 2])
            if watched_films:
                ratings = [f['rating'] for f in watched_films if isinstance(f.get('rating'), (int, float))]
                if ratings:
                    ax3.hist(ratings, bins=10, color=self.colors['watched'],
                            alpha=0.7, edgecolor='black', orientation='horizontal')
                    ax3.axhline(np.mean(ratings), color='red', linestyle='--',
                               linewidth=2, label=f'Avg: {np.mean(ratings):.2f}')
                    ax3.set_ylabel('Rating', fontsize=11)
                    ax3.set_xlabel('Count', fontsize=11)
                    ax3.set_title('Rating Distribution', fontsize=12, fontweight='bold')
                    ax3.legend()
                    ax3.grid(axis='x', alpha=0.3)

            # 4. Top Watched Films List
            ax4 = fig.add_subplot(gs[2, 0])
            ax4.axis('off')
            if watched_films:
                top_watched = sorted(watched_films, key=lambda x: x.get('rating', 0), reverse=True)[:8]
                watched_text = "TOP WATCHED FILMS\n" + "═" * 35 + "\n\n"
                for i, film in enumerate(top_watched, 1):
                    year = film.get('year', 'N/A')
                    rating = film.get('rating', 0)
                    title = film.get('title', 'Unknown')[:30]
                    watched_text += f"{i}. {title}\n   {year} • ⭐ {rating}\n\n"

                ax4.text(0.05, 0.95, watched_text, transform=ax4.transAxes,
                        fontsize=9, verticalalignment='top', fontfamily='monospace',
                        bbox=dict(boxstyle='round', facecolor=self.colors['watched'],
                                 alpha=0.2, edgecolor=self.colors['watched'], linewidth=2))

            # 5. Top Missing Films List
            ax5 = fig.add_subplot(gs[2, 1])
            ax5.axis('off')
            if missing_films:
                top_missing = sorted(missing_films, key=lambda x: x.get('rating', 0), reverse=True)[:8]
                missing_text = "TOP MISSING FILMS\n(Recommended to Watch!)\n" + "═" * 35 + "\n\n"
                for i, film in enumerate(top_missing, 1):
                    year = film.get('year', 'N/A')
                    rating = film.get('rating', 0)
                    title = film.get('title', 'Unknown')[:30]
                    missing_text += f"{i}. {title}\n   {year} • ⭐ {rating}\n\n"

                ax5.text(0.05, 0.95, missing_text, transform=ax5.transAxes,
                        fontsize=9, verticalalignment='top', fontfamily='monospace',
                        bbox=dict(boxstyle='round', facecolor=self.colors['missing'],
                                 alpha=0.2, edgecolor=self.colors['missing'], linewidth=2))

            # 6. Statistics Panel
            ax6 = fig.add_subplot(gs[2, 2])
            ax6.axis('off')

            watched_years = [f['year'] for f in watched_films if isinstance(f.get('year'), (int, float))]
            era_span = f"{min(watched_years)}-{max(watched_years)}" if watched_years else "N/A"

            stats_text = f"""
FILMOGRAPHY STATS
═════════════════════

Total Quality Films: {total}
Films Watched: {watched_count}
Films Missing: {missing_count}
Completion: {actor_row['completeness_pct']:.1f}%

RATINGS:
Your Avg: ⭐ {actor_row['watched_avg_rating']:.2f}
Complete Avg: ⭐ {actor_row['catalog_avg_rating']:.2f}

ERA COVERAGE:
Years Span: {era_span}

RECOMMENDATION:
{"🎉 COMPLETED!" if actor_row['completeness_pct'] == 100 else f"Watch {missing_count} more films to complete!"}
            """

            ax6.text(0.05, 0.95, stats_text, transform=ax6.transAxes,
                    fontsize=10, verticalalignment='top', fontfamily='monospace',
                    bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.3))

            filename = f"actor_detail_{idx}_{actor_name.replace(' ', '_')}.png"
            plt.savefig(self.output_dir / filename, dpi=300, bbox_inches='tight', facecolor='white')
            plt.close()
            print(f"    ✓ Saved {filename}")

    def create_career_trajectory_visualization(self):
        """Create career trajectory visualization for top actors."""
        print("\n3. Creating career trajectory visualization...")

        fig, axes = plt.subplots(3, 2, figsize=(20, 18))
        fig.suptitle('Career Trajectories: Top 6 Actors', fontsize=18, fontweight='bold')
        axes = axes.flatten()

        top6 = self.actor_df[self.actor_df['total_quality_films'] >= 10].head(6)

        for idx, (_, actor_row) in enumerate(top6.iterrows()):
            ax = axes[idx]
            actor_name = actor_row['actor_name']
            watched_films = self._safe_eval(actor_row['watched_films'])

            if watched_films:
                # Create timeline
                films_by_year = defaultdict(list)
                for film in watched_films:
                    year = film.get('year')
                    if isinstance(year, (int, float)):
                        films_by_year[int(year)].append(film['rating'])

                if films_by_year:
                    years = sorted(films_by_year.keys())
                    avg_ratings = [np.mean(films_by_year[year]) for year in years]
                    film_counts = [len(films_by_year[year]) for year in years]

                    # Plot line for rating trajectory
                    ax.plot(years, avg_ratings, marker='o', linewidth=2, markersize=8,
                           color=self.colors['primary'], label='Avg Rating')

                    # Add film count as bubble size overlay
                    scatter = ax.scatter(years, avg_ratings, s=[c*100 for c in film_counts],
                                       alpha=0.3, color=self.colors['accent'])

                    ax.set_title(f'{actor_name}\n({actor_row["watched_count"]} films watched)',
                               fontsize=11, fontweight='bold')
                    ax.set_xlabel('Year')
                    ax.set_ylabel('Average Rating')
                    ax.grid(True, alpha=0.3)
                    ax.set_ylim(6, 9)

                    # Add trend line
                    if len(years) > 2:
                        z = np.polyfit(years, avg_ratings, 1)
                        p = np.poly1d(z)
                        ax.plot(years, p(years), "--", alpha=0.5, color='red',
                               label='Trend')

                    ax.legend(fontsize=8)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'career_trajectories.png',
                   dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        print("  ✓ Saved career_trajectories.png")

    def create_completion_heatmap(self):
        """Create heatmap showing completion rates across actors."""
        print("\n4. Creating completion heatmap...")

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 12))

        # Top 30 actors by completeness
        top30 = self.actor_df.head(30)

        # Create matrix for heatmap
        data_matrix = []
        labels = []

        for _, row in top30.iterrows():
            watched_pct = row['completeness_pct']
            missing_pct = 100 - watched_pct
            data_matrix.append([watched_pct, missing_pct])
            labels.append(row['actor_name'][:25])

        # Heatmap 1: Completion percentage
        im1 = ax1.imshow([[row['completeness_pct']] for _, row in top30.iterrows()],
                        cmap='RdYlGn', aspect='auto', vmin=0, vmax=100)
        ax1.set_yticks(range(len(labels)))
        ax1.set_yticklabels(labels, fontsize=9)
        ax1.set_xticks([0])
        ax1.set_xticklabels(['Completion %'])
        ax1.set_title('Top 30 Actors by Completeness', fontsize=14, fontweight='bold')

        # Add text annotations
        for i, (_, row) in enumerate(top30.iterrows()):
            ax1.text(0, i, f"{row['completeness_pct']:.1f}%",
                    ha='center', va='center', fontweight='bold', fontsize=8)

        plt.colorbar(im1, ax=ax1, label='Completion %')

        # Heatmap 2: Watched vs Total films
        watched_counts = top30['watched_count'].values.reshape(-1, 1)
        total_counts = top30['total_quality_films'].values.reshape(-1, 1)

        combined = np.hstack([watched_counts, total_counts])

        im2 = ax2.imshow(combined, cmap='YlOrRd', aspect='auto')
        ax2.set_yticks(range(len(labels)))
        ax2.set_yticklabels(labels, fontsize=9)
        ax2.set_xticks([0, 1])
        ax2.set_xticklabels(['Watched', 'Total Quality'])
        ax2.set_title('Film Counts: Watched vs Total Filmography', fontsize=14, fontweight='bold')

        # Add text annotations
        for i, (_, row) in enumerate(top30.iterrows()):
            ax2.text(0, i, f"{row['watched_count']:.0f}",
                    ha='center', va='center', fontweight='bold', fontsize=8)
            ax2.text(1, i, f"{row['total_quality_films']:.0f}",
                    ha='center', va='center', fontweight='bold', fontsize=8)

        plt.colorbar(im2, ax=ax2, label='Film Count')

        plt.tight_layout()
        plt.savefig(self.output_dir / 'completion_heatmap.png',
                   dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        print("  ✓ Saved completion_heatmap.png")

    def create_missing_films_gallery(self):
        """Create gallery of top missing films across all actors."""
        print("\n5. Creating missing films gallery...")

        # Aggregate all missing films
        all_missing = []
        for _, row in self.actor_df.iterrows():
            missing_films = self._safe_eval(row['missing_films'])
            for film in missing_films:
                film['actor'] = row['actor_name']
                all_missing.append(film)

        # Sort by rating and get top 50
        top_missing = sorted(all_missing, key=lambda x: x.get('rating', 0), reverse=True)[:50]

        # Count frequency
        film_freq = defaultdict(list)
        for film in all_missing:
            title = film.get('title', '')
            if title:
                film_freq[title].append(film)

        # Get most frequently missing
        freq_sorted = sorted(film_freq.items(), key=lambda x: len(x[1]), reverse=True)[:30]

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(20, 16))

        # Top 30 highest rated missing films
        ax1.axis('off')
        gallery_text = "TOP 30 HIGHEST-RATED MISSING FILMS\n"
        gallery_text += "═" * 100 + "\n\n"
        gallery_text += f"{'#':<4} {'Title':<45} {'Year':<8} {'Rating':<10} {'Actor':<30}\n"
        gallery_text += "─" * 100 + "\n"

        for i, film in enumerate(top_missing[:30], 1):
            title = film.get('title', 'Unknown')[:43]
            year = str(film.get('year', 'N/A'))
            rating = f"⭐ {film.get('rating', 0):.1f}"
            actor = film.get('actor', '')[:28]
            gallery_text += f"{i:<4} {title:<45} {year:<8} {rating:<10} {actor:<30}\n"

        ax1.text(0.05, 0.95, gallery_text, transform=ax1.transAxes,
                fontsize=8, verticalalignment='top', fontfamily='monospace',
                bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.5))

        # Most frequently missing (across multiple actors)
        ax2.axis('off')
        freq_text = "MOST FREQUENTLY MISSING FILMS (Multiple Actors)\n"
        freq_text += "These films appear in multiple actors' filmographies that you haven't watched yet!\n"
        freq_text += "═" * 100 + "\n\n"
        freq_text += f"{'#':<4} {'Title':<50} {'Year':<8} {'Rating':<10} {'# Actors':<12}\n"
        freq_text += "─" * 100 + "\n"

        for i, (title, films) in enumerate(freq_sorted[:20], 1):
            year = str(films[0].get('year', 'N/A'))
            rating = f"⭐ {films[0].get('rating', 0):.1f}"
            count = len(films)
            title_truncated = title[:48]
            freq_text += f"{i:<4} {title_truncated:<50} {year:<8} {rating:<10} {count:<12}\n"

        ax2.text(0.05, 0.95, freq_text, transform=ax2.transAxes,
                fontsize=8, verticalalignment='top', fontfamily='monospace',
                bbox=dict(boxstyle='round', facecolor='lightcoral', alpha=0.3))

        plt.savefig(self.output_dir / 'missing_films_gallery.png',
                   dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        print("  ✓ Saved missing_films_gallery.png")

    def create_quality_analysis(self):
        """Analyze quality patterns in watched vs missing films."""
        print("\n6. Creating quality analysis...")

        fig, axes = plt.subplots(2, 3, figsize=(20, 12))
        fig.suptitle('Quality Analysis: Watched vs Missing Films', fontsize=16, fontweight='bold')

        # 1. Average rating comparison scatter
        ax1 = axes[0, 0]
        ax1.scatter(self.actor_df['watched_avg_rating'],
                   self.actor_df['catalog_avg_rating'],
                   alpha=0.5, s=50, color=self.colors['primary'])
        ax1.plot([6, 9], [6, 9], 'r--', label='Equal ratings')
        ax1.set_xlabel('Your Watched Average Rating')
        ax1.set_ylabel('Complete Filmography Average')
        ax1.set_title('Rating Comparison per Actor')
        ax1.legend()
        ax1.grid(True, alpha=0.3)

        # 2. Rating difference distribution
        ax2 = axes[0, 1]
        rating_diff = self.actor_df['watched_avg_rating'] - self.actor_df['catalog_avg_rating']
        ax2.hist(rating_diff, bins=30, color=self.colors['accent'], alpha=0.7, edgecolor='black')
        ax2.axvline(0, color='red', linestyle='--', linewidth=2)
        ax2.axvline(rating_diff.mean(), color='blue', linestyle='--', linewidth=2,
                   label=f'Mean: {rating_diff.mean():.3f}')
        ax2.set_xlabel('Rating Difference (Watched - Complete)')
        ax2.set_ylabel('Number of Actors')
        ax2.set_title('Are You Cherry-Picking Quality?')
        ax2.legend()
        ax2.grid(axis='y', alpha=0.3)

        # 3. Completion vs Quality relationship
        ax3 = axes[0, 2]
        scatter = ax3.scatter(self.actor_df['completeness_pct'],
                            self.actor_df['watched_avg_rating'],
                            c=self.actor_df['watched_count'],
                            cmap='viridis', s=50, alpha=0.6)
        ax3.set_xlabel('Completion %')
        ax3.set_ylabel('Average Rating of Watched')
        ax3.set_title('Does Higher Completion Mean Lower Quality?')
        ax3.grid(True, alpha=0.3)
        plt.colorbar(scatter, ax=ax3, label='Films Watched')

        # 4. Top quality selectors
        ax4 = axes[1, 0]
        quality_selectors = self.actor_df[self.actor_df['watched_count'] >= 5].nlargest(
            10, 'watched_avg_rating')
        y_pos = np.arange(len(quality_selectors))
        ax4.barh(y_pos, quality_selectors['watched_avg_rating'],
                color=self.colors['watched'], alpha=0.7)
        ax4.set_yticks(y_pos)
        ax4.set_yticklabels(quality_selectors['actor_name'], fontsize=9)
        ax4.set_xlabel('Average Rating')
        ax4.set_title('Actors Where You Watched Highest Quality')
        ax4.invert_yaxis()
        ax4.grid(axis='x', alpha=0.3)

        # 5. Biggest quality gaps
        ax5 = axes[1, 1]
        biggest_gaps = self.actor_df[self.actor_df['watched_count'] >= 3].copy()
        biggest_gaps['quality_gap'] = biggest_gaps['catalog_avg_rating'] - biggest_gaps['watched_avg_rating']
        biggest_gaps = biggest_gaps.nlargest(10, 'quality_gap')

        y_pos = np.arange(len(biggest_gaps))
        ax5.barh(y_pos, biggest_gaps['quality_gap'], color=self.colors['missing'], alpha=0.7)
        ax5.set_yticks(y_pos)
        ax5.set_yticklabels(biggest_gaps['actor_name'], fontsize=9)
        ax5.set_xlabel('Quality Gap (Higher = More Good Films to Discover)')
        ax5.set_title('Actors with Best Unwatched Films')
        ax5.invert_yaxis()
        ax5.grid(axis='x', alpha=0.3)

        # 6. Statistics summary
        ax6 = axes[1, 2]
        ax6.axis('off')

        better_selection = len(self.actor_df[self.actor_df['watched_avg_rating'] >
                                            self.actor_df['catalog_avg_rating']])
        worse_selection = len(self.actor_df[self.actor_df['watched_avg_rating'] <
                                           self.actor_df['catalog_avg_rating']])

        quality_stats = f"""
QUALITY SELECTION ANALYSIS
═══════════════════════════

SELECTION QUALITY:
Actors where you watched
better than average: {better_selection}

Actors where unwatched are
better quality: {worse_selection}

AVERAGE RATINGS:
Your selections: {self.actor_df['watched_avg_rating'].mean():.3f}
Complete filmographies: {self.actor_df['catalog_avg_rating'].mean():.3f}
Difference: {rating_diff.mean():.3f}

INTERPRETATION:
{"You tend to cherry-pick the best films!" if rating_diff.mean() > 0.1 else "You watch a representative sample!" if abs(rating_diff.mean()) < 0.1 else "You have many high-quality films to discover!"}

RECOMMENDATION:
{"Keep exploring - there are many hidden gems in the unwatched films!" if worse_selection > better_selection else "You have excellent taste in selecting films!"}
        """

        ax6.text(0.05, 0.95, quality_stats, transform=ax6.transAxes,
                fontsize=10, verticalalignment='top', fontfamily='monospace',
                bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.3))

        plt.tight_layout()
        plt.savefig(self.output_dir / 'quality_analysis.png',
                   dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        print("  ✓ Saved quality_analysis.png")

    def generate_report(self):
        """Generate comprehensive text report."""
        print("\n7. Generating comprehensive report...")

        report = []
        report.append("=" * 100)
        report.append("ENHANCED ACTOR FILMOGRAPHY COMPLETENESS ANALYSIS")
        report.append("=" * 100)
        report.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Database: {self.db_path}")
        report.append(f"\n{'─' * 100}\n")

        # Executive Summary
        report.append("\n📊 EXECUTIVE SUMMARY")
        report.append("─" * 100)
        report.append(f"Total Actors Analyzed: {len(self.actor_df):,}")
        report.append(f"Average Completion Rate: {self.actor_df['completeness_pct'].mean():.2f}%")
        report.append(f"Fully Completed Actors: {len(self.actor_df[self.actor_df['completeness_pct'] == 100])}")
        report.append(f"Total Missing Films Identified: {self.actor_df['missing_count'].sum():,.0f}")
        report.append(f"Average Films Watched per Actor: {self.actor_df['watched_count'].mean():.1f}")

        # Top 10 Most Complete
        report.append("\n\n🏆 TOP 10 MOST COMPLETE ACTORS")
        report.append("─" * 100)
        report.append(f"{'Rank':<6} {'Actor':<35} {'Watched':<10} {'Total':<10} {'Complete %':<12} {'Avg Rating':<12}")
        report.append("─" * 100)

        top10 = self.actor_df.nlargest(10, 'completeness_pct')
        for i, (_, row) in enumerate(top10.iterrows(), 1):
            report.append(
                f"{i:<6} {row['actor_name']:<35} {row['watched_count']:<10.0f} "
                f"{row['total_quality_films']:<10.0f} {row['completeness_pct']:<12.1f} "
                f"{row['watched_avg_rating']:<12.2f}"
            )

        # Biggest Discovery Opportunities
        report.append("\n\n🎬 BIGGEST DISCOVERY OPPORTUNITIES")
        report.append("─" * 100)
        report.append(f"{'Rank':<6} {'Actor':<35} {'Missing':<10} {'Completion %':<15} {'Best Missing Film':<30}")
        report.append("─" * 100)

        most_missing = self.actor_df.nlargest(10, 'missing_count')
        for i, (_, row) in enumerate(most_missing.iterrows(), 1):
            missing_films = self._safe_eval(row['missing_films'])
            best_missing = missing_films[0]['title'][:28] if missing_films else 'N/A'
            report.append(
                f"{i:<6} {row['actor_name']:<35} {row['missing_count']:<10.0f} "
                f"{row['completeness_pct']:<15.1f} {best_missing:<30}"
            )

        # Quality Insights
        report.append("\n\n⭐ QUALITY INSIGHTS")
        report.append("─" * 100)

        rating_diff = self.actor_df['watched_avg_rating'] - self.actor_df['catalog_avg_rating']
        better_selection = len(self.actor_df[rating_diff > 0])

        report.append(f"Your Average Rating: {self.actor_df['watched_avg_rating'].mean():.3f}")
        report.append(f"Complete Filmography Average: {self.actor_df['catalog_avg_rating'].mean():.3f}")
        report.append(f"Quality Difference: {rating_diff.mean():.3f}")
        report.append(f"Actors Where You Cherry-Picked Quality: {better_selection} ({better_selection/len(self.actor_df)*100:.1f}%)")

        # Top 20 Recommended Missing Films
        report.append("\n\n🎯 TOP 20 RECOMMENDED MISSING FILMS TO WATCH")
        report.append("─" * 100)

        all_missing = []
        for _, row in self.actor_df.iterrows():
            missing_films = self._safe_eval(row['missing_films'])
            for film in missing_films:
                film['actor'] = row['actor_name']
                all_missing.append(film)

        top_missing = sorted(all_missing, key=lambda x: x.get('rating', 0), reverse=True)[:20]

        report.append(f"{'Rank':<6} {'Title':<45} {'Year':<8} {'Rating':<10} {'Actor':<30}")
        report.append("─" * 100)

        for i, film in enumerate(top_missing, 1):
            report.append(
                f"{i:<6} {film.get('title', 'Unknown')[:43]:<45} "
                f"{str(film.get('year', 'N/A')):<8} {film.get('rating', 0):<10.1f} "
                f"{film.get('actor', '')[:28]:<30}"
            )

        # Save report
        report_path = self.report_dir / 'batch_34_actor_completeness_report.txt'
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))

        print(f"  ✓ Saved {report_path}")

    def run_all(self):
        """Run all analyses."""
        print("\nStarting Actor Completeness Analysis...")
        print("This may take a few minutes...\n")

        self.create_overview_dashboard()
        self.create_top5_detailed_breakdown()
        self.create_career_trajectory_visualization()
        self.create_completion_heatmap()
        self.create_missing_films_gallery()
        self.create_quality_analysis()
        self.generate_report()

        self.conn.close()

        print("\n" + "=" * 80)
        print("✓ ANALYSIS COMPLETE!")
        print("=" * 80)
        print(f"\nVisualizations saved to: {self.output_dir}")
        print(f"Report saved to: {self.report_dir}")
        print("\nCreated visualizations:")
        print("  1. overview_dashboard.png - Comprehensive overview with 7 panels")
        print("  2-6. actor_detail_*.png - 5 detailed actor breakdowns")
        print("  7. career_trajectories.png - Career progression for top 6 actors")
        print("  8. completion_heatmap.png - Completion heatmap visualization")
        print("  9. missing_films_gallery.png - Gallery of top missing films")
        print("  10. quality_analysis.png - Quality pattern analysis")
        print("  11. batch_34_actor_completeness_report.txt - Comprehensive text report")
        print("\nTotal: 11 outputs with 20+ individual visualizations!")


if __name__ == "__main__":
    analyzer = ActorCompletenessAnalyzer()
    analyzer.run_all()
