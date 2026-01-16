"""
Batch 38: Director Filmography Completeness Analysis
Comprehensive analysis using the completeness database with creative visualizations.
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
from matplotlib.gridspec import GridSpec

class DirectorCompletenessAnalyzer:
    def __init__(self, db_path='data/processed/completeness.db',
                 watched_path='data/processed/watched_movies_master.csv'):
        """Initialize with completeness database."""

        print("=" * 80)
        print("DIRECTOR COMPLETENESS ANALYSIS")
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

        # Load director completeness data
        print("\nLoading director completeness data from database...")
        self.director_df = pd.read_sql_query(
            "SELECT * FROM director_completeness ORDER BY completeness_pct DESC",
            self.conn
        )
        print(f"Loaded {len(self.director_df)} directors")

        # Setup directories
        self.output_dir = Path('analysis_outputs/visualizations/batch_38')
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
        counts, edges = np.histogram(self.director_df['completeness_pct'], bins=bins)
        colors_dist = ['#e74c3c', '#e67e22', '#f39c12', '#2ecc71', '#27ae60']
        ax1.bar(range(len(counts)), counts, color=colors_dist, alpha=0.7, edgecolor='black')
        ax1.set_xticks(range(len(counts)))
        ax1.set_xticklabels(['0-20%', '20-40%', '40-60%', '60-80%', '80-100%'])
        ax1.set_title('Director Completeness Distribution', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Number of Directors')
        ax1.grid(axis='y', alpha=0.3)
        for i, count in enumerate(counts):
            ax1.text(i, count + 1, str(int(count)), ha='center', fontweight='bold')

        # 2. Top 10 Nearly Complete Directors
        ax2 = fig.add_subplot(gs[0, 2:4])
        nearly_complete = self.director_df.nlargest(10, 'completeness_pct')
        y_pos = np.arange(len(nearly_complete))
        ax2.barh(y_pos, nearly_complete['watched_count'],
                color=self.colors['watched'], alpha=0.7, label='Watched')
        ax2.barh(y_pos, nearly_complete['missing_count'],
                left=nearly_complete['watched_count'],
                color=self.colors['missing'], alpha=0.7, label='Missing')
        ax2.set_yticks(y_pos)
        ax2.set_yticklabels(nearly_complete['director_name'], fontsize=9)
        ax2.set_title('Top 10 Most Complete Directors', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Films')
        ax2.legend(loc='lower right')
        ax2.invert_yaxis()

        for i, (idx, row) in enumerate(nearly_complete.iterrows()):
            total = row['watched_count'] + row['missing_count']
            ax2.text(total + 0.5, i, f"{row['completeness_pct']:.1f}%",
                    va='center', fontweight='bold')

        # 3. Directors with Most Missing Films
        ax3 = fig.add_subplot(gs[1, 0:2])
        most_missing = self.director_df.nlargest(10, 'missing_count')
        x_pos = np.arange(len(most_missing))
        ax3.bar(x_pos, most_missing['missing_count'], color=self.colors['missing'], alpha=0.7)
        ax3.set_xticks(x_pos)
        ax3.set_xticklabels(most_missing['director_name'], rotation=45, ha='right', fontsize=9)
        ax3.set_title('Directors with Most Missing Films', fontsize=14, fontweight='bold')
        ax3.set_ylabel('Missing Films')
        ax3.grid(axis='y', alpha=0.3)
        for i, count in enumerate(most_missing['missing_count']):
            ax3.text(i, count + 0.3, str(int(count)), ha='center', fontweight='bold')

        # 4. Average Ratings Comparison
        ax4 = fig.add_subplot(gs[1, 2:4])
        directors_sample = self.director_df.head(15)
        x = np.arange(len(directors_sample))
        width = 0.35
        ax4.bar(x - width/2, directors_sample['watched_avg_rating'], width,
               label='My Watched Avg', color=self.colors['watched'], alpha=0.7)
        ax4.bar(x + width/2, directors_sample['catalog_avg_rating'], width,
               label='Complete Filmography Avg', color=self.colors['primary'], alpha=0.7)
        ax4.set_xticks(x)
        ax4.set_xticklabels(directors_sample['director_name'], rotation=45, ha='right', fontsize=8)
        ax4.set_title('Rating Comparison: My Selections vs Complete Filmography',
                     fontsize=14, fontweight='bold')
        ax4.set_ylabel('Average IMDb Rating')
        ax4.legend()
        ax4.grid(axis='y', alpha=0.3)
        ax4.set_ylim(6, 9)

        # 5. Completion Milestones
        ax5 = fig.add_subplot(gs[2, 0:2])
        milestones = {
            'Completed (100%)': len(self.director_df[self.director_df['completeness_pct'] == 100]),
            'Nearly There (80-99%)': len(self.director_df[(self.director_df['completeness_pct'] >= 80) &
                                                       (self.director_df['completeness_pct'] < 100)]),
            'Halfway (50-79%)': len(self.director_df[(self.director_df['completeness_pct'] >= 50) &
                                                  (self.director_df['completeness_pct'] < 80)]),
            'Getting Started (20-49%)': len(self.director_df[(self.director_df['completeness_pct'] >= 20) &
                                                         (self.director_df['completeness_pct'] < 50)]),
            'Just Beginning (<20%)': len(self.director_df[self.director_df['completeness_pct'] < 20])
        }
        colors_milestone = ['#27ae60', '#2ecc71', '#f39c12', '#e67e22', '#e74c3c']
        wedges, texts, autotexts = ax5.pie(milestones.values(), labels=milestones.keys(),
                                            autopct='%1.1f%%', colors=colors_milestone,
                                            startangle=90)
        ax5.set_title('Completion Milestones', fontsize=14, fontweight='bold')
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')

        # 6. Films Watched per Director Distribution
        ax6 = fig.add_subplot(gs[2, 2:4])
        ax6.hist(self.director_df['watched_count'], bins=20, color=self.colors['watched'],
                alpha=0.7, edgecolor='black')
        ax6.set_title('Distribution of Films Watched per Director', fontsize=14, fontweight='bold')
        ax6.set_xlabel('Number of Films Watched')
        ax6.set_ylabel('Number of Directors')
        ax6.axvline(self.director_df['watched_count'].median(), color='red',
                   linestyle='--', linewidth=2, label=f"Median: {self.director_df['watched_count'].median():.0f}")
        ax6.legend()
        ax6.grid(axis='y', alpha=0.3)

        # 7. Summary Statistics
        ax7 = fig.add_subplot(gs[3, :])
        ax7.axis('off')

        stats_text = f"""
        COMPREHENSIVE DIRECTOR COMPLETENESS STATISTICS
        ═══════════════════════════════════════════════════════════════════════════════

        Total Directors Analyzed: {len(self.director_df):,}

        Completion Status:
        • Fully Completed Directors (100%): {len(self.director_df[self.director_df['completeness_pct'] == 100]):,}
        • Nearly Complete (80-99%): {len(self.director_df[(self.director_df['completeness_pct'] >= 80) & (self.director_df['completeness_pct'] < 100)]):,}
        • Average Completion Rate: {self.director_df['completeness_pct'].mean():.1f}%
        • Median Completion Rate: {self.director_df['completeness_pct'].median():.1f}%

        Films per Director:
        • Average Films Watched per Director: {self.director_df['watched_count'].mean():.1f}
        • Maximum Films Watched (Single Director): {self.director_df['watched_count'].max():.0f}
        • Total Missing Films Identified: {self.director_df['missing_count'].sum():,.0f}

        Quality Insights:
        • My Average Rating: {self.director_df['watched_avg_rating'].mean():.2f}
        • Complete Filmography Average: {self.director_df['catalog_avg_rating'].mean():.2f}
        • Rating Difference: {(self.director_df['watched_avg_rating'].mean() - self.director_df['catalog_avg_rating'].mean()):.2f}
          {"(I watch higher quality!)" if self.director_df['watched_avg_rating'].mean() > self.director_df['catalog_avg_rating'].mean() else "(Room to explore more!)"}
        """

        ax7.text(0.05, 0.95, stats_text, transform=ax7.transAxes,
                fontsize=11, verticalalignment='top', fontfamily='monospace',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

        plt.savefig(self.output_dir / 'overview_dashboard.png',
                   dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        print("  ✓ Saved overview_dashboard.png")

    def create_missing_films_recommendations(self):
        """Create recommendations for missing films."""
        print("\n2. Creating missing films recommendations...")

        # Aggregate all missing films
        all_missing = []
        for _, row in self.director_df.iterrows():
            missing_films = self._safe_eval(row['missing_films'])
            for film in missing_films:
                film['director'] = row['director_name']
                all_missing.append(film)

        top_missing = sorted(all_missing, key=lambda x: x.get('rating', 0), reverse=True)[:40]

        fig, ax = plt.subplots(figsize=(20, 14))
        ax.axis('off')

        gallery_text = "TOP 40 HIGHEST-RATED MISSING DIRECTOR FILMS\n"
        gallery_text += "═" * 110 + "\n\n"
        gallery_text += f"{'#':<4} {'Title':<50} {'Year':<8} {'Rating':<10} {'Director':<35}\n"
        gallery_text += "─" * 110 + "\n"

        for i, film in enumerate(top_missing, 1):
            title = film.get('title', 'Unknown')[:48]
            year = str(film.get('year', 'N/A'))
            rating = f"⭐ {film.get('rating', 0):.1f}"
            director = film.get('director', '')[:33]
            gallery_text += f"{i:<4} {title:<50} {year:<8} {rating:<10} {director:<35}\n"

        ax.text(0.05, 0.95, gallery_text, transform=ax.transAxes,
                fontsize=9, verticalalignment='top', fontfamily='monospace',
                bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.5))

        plt.savefig(self.output_dir / 'missing_films_recommendations.png',
                   dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        print("  ✓ Saved missing_films_recommendations.png")

    def generate_report(self):
        """Generate comprehensive text report."""
        print("\n3. Generating comprehensive report...")

        report = []
        report.append("=" * 100)
        report.append("DIRECTOR FILMOGRAPHY COMPLETENESS ANALYSIS")
        report.append("=" * 100)
        report.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Database: {self.db_path}")
        report.append(f"\n{'─' * 100}\n")

        # Executive Summary
        report.append("\n📊 EXECUTIVE SUMMARY")
        report.append("─" * 100)
        report.append(f"Total Directors Analyzed: {len(self.director_df):,}")
        report.append(f"Average Completion Rate: {self.director_df['completeness_pct'].mean():.2f}%")
        report.append(f"Fully Completed Directors: {len(self.director_df[self.director_df['completeness_pct'] == 100])}")
        report.append(f"Total Missing Films Identified: {self.director_df['missing_count'].sum():,.0f}")
        report.append(f"Average Films Watched per Director: {self.director_df['watched_count'].mean():.1f}")

        # Top 10 Most Complete
        report.append("\n\n🏆 TOP 10 MOST COMPLETE DIRECTORS")
        report.append("─" * 100)
        report.append(f"{'Rank':<6} {'Director':<35} {'Watched':<10} {'Total':<10} {'Complete %':<12} {'Avg Rating':<12}")
        report.append("─" * 100)

        top10 = self.director_df.nlargest(10, 'completeness_pct')
        for i, (_, row) in enumerate(top10.iterrows(), 1):
            report.append(
                f"{i:<6} {row['director_name']:<35} {row['watched_count']:<10.0f} "
                f"{row['total_quality_films']:<10.0f} {row['completeness_pct']:<12.1f} "
                f"{row['watched_avg_rating']:<12.2f}"
            )

        # Biggest Discovery Opportunities
        report.append("\n\n🎬 BIGGEST DISCOVERY OPPORTUNITIES")
        report.append("─" * 100)
        report.append(f"{'Rank':<6} {'Director':<35} {'Missing':<10} {'Completion %':<15} {'Best Missing Film':<30}")
        report.append("─" * 100)

        most_missing = self.director_df.nlargest(10, 'missing_count')
        for i, (_, row) in enumerate(most_missing.iterrows(), 1):
            missing_films = self._safe_eval(row['missing_films'])
            best_missing = missing_films[0]['title'][:28] if missing_films else 'N/A'
            report.append(
                f"{i:<6} {row['director_name']:<35} {row['missing_count']:<10.0f} "
                f"{row['completeness_pct']:<15.1f} {best_missing:<30}"
            )

        # Top 30 Recommended Missing Films
        report.append("\n\n🎯 TOP 30 RECOMMENDED MISSING FILMS TO WATCH")
        report.append("─" * 100)

        all_missing = []
        for _, row in self.director_df.iterrows():
            missing_films = self._safe_eval(row['missing_films'])
            for film in missing_films:
                film['director'] = row['director_name']
                all_missing.append(film)

        top_missing = sorted(all_missing, key=lambda x: x.get('rating', 0), reverse=True)[:30]

        report.append(f"{'Rank':<6} {'Title':<45} {'Year':<8} {'Rating':<10} {'Director':<30}")
        report.append("─" * 100)

        for i, film in enumerate(top_missing, 1):
            report.append(
                f"{i:<6} {film.get('title', 'Unknown')[:43]:<45} "
                f"{str(film.get('year', 'N/A')):<8} {film.get('rating', 0):<10.1f} "
                f"{film.get('director', '')[:28]:<30}"
            )

        # Save report
        report_path = self.report_dir / 'batch_38_director_completeness_report.txt'
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))

        print(f"  ✓ Saved {report_path}")

    def run_all(self):
        """Run all analyses."""
        print("\nStarting Director Completeness Analysis...")
        print("This may take a few minutes...\n")

        self.create_overview_dashboard()
        self.create_missing_films_recommendations()
        self.generate_report()

        self.conn.close()

        print("\n" + "=" * 80)
        print("✓ ANALYSIS COMPLETE!")
        print("=" * 80)
        print(f"\nVisualizations saved to: {self.output_dir}")
        print(f"Report saved to: {self.report_dir}")


if __name__ == "__main__":
    analyzer = DirectorCompletenessAnalyzer()
    analyzer.run_all()
