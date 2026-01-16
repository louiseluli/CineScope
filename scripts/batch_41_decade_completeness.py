"""
Batch 41: Decade/Era Completeness Analysis
Analyzes which decades and cinema eras I've explored thoroughly.
Shows what notable films I'm missing from each era.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict
from pathlib import Path
import numpy as np

class DecadeCompletenessAnalyzer:
    def __init__(self,
                 watched_path='data/processed/watched_movies_master.csv',
                 catalog_path='data/processed/master_cinema_data.csv'):
        """Initialize with watched movies and catalog data."""

        print("Loading my watched movies...")
        self.watched_df = pd.read_csv(watched_path)
        print(f"Loaded {len(self.watched_df)} watched films")

        print("\nLoading catalog...")
        self.catalog_df = pd.read_csv(catalog_path)
        print(f"Loaded {len(self.catalog_df)} catalog films")

        # Setup directories
        self.output_dir = Path('analysis_outputs/visualizations/batch_41')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir = Path('analysis_outputs/reports')
        self.report_dir.mkdir(parents=True, exist_ok=True)

        # Define cinema eras
        self.eras = {
            'Silent Era': (1900, 1929),
            'Golden Age': (1930, 1949),
            'Post-War': (1950, 1969),
            'New Hollywood': (1970, 1989),
            'Modern Era': (1990, 2009),
            'Contemporary': (2010, 2030)
        }

        # Analyze
        self._analyze_decade_completeness()

    def _get_decade(self, year):
        """Get decade from year."""
        try:
            year = int(year)
            return (year // 10) * 10
        except:
            return None

    def _get_era(self, year):
        """Get cinema era from year."""
        try:
            year = int(year)
            for era, (start, end) in self.eras.items():
                if start <= year <= end:
                    return era
            return 'Other'
        except:
            return None

    def _analyze_decade_completeness(self):
        """Analyze decade and era completeness."""
        print("\nAnalyzing decade completeness...")

        # Add decade and era columns
        self.watched_df['decade'] = self.watched_df['Year'].apply(self._get_decade)
        self.catalog_df['decade'] = self.catalog_df['year'].apply(self._get_decade)
        self.watched_df['era'] = self.watched_df['Year'].apply(self._get_era)
        self.catalog_df['era'] = self.catalog_df['year'].apply(self._get_era)

        # Filter catalog for quality
        quality_catalog = self.catalog_df[
            (self.catalog_df['imdb_rating'] >= 6.5) &
            (self.catalog_df['decade'].notna())
        ].copy()

        # Calculate decade completeness
        decade_data = []

        for decade in sorted(quality_catalog['decade'].dropna().unique()):
            decade = int(decade)

            # Get films from this decade
            watched_decade = self.watched_df[self.watched_df['decade'] == decade]
            catalog_decade = quality_catalog[quality_catalog['decade'] == decade]

            if len(catalog_decade) < 5:  # Skip decades with few films
                continue

            watched_count = len(watched_decade)
            total_count = len(catalog_decade)
            completeness_pct = (watched_count / total_count * 100) if total_count > 0 else 0

            # Find missing films
            watched_titles_lower = set(watched_decade['Title'].str.lower())
            missing_films = catalog_decade[~catalog_decade['title'].str.lower().isin(watched_titles_lower)]
            missing_films = missing_films.nlargest(10, 'imdb_rating')

            missing_list = []
            for _, film in missing_films.iterrows():
                missing_list.append({
                    'title': film['title'],
                    'year': film['year'],
                    'rating': film['imdb_rating']
                })

            # Calculate averages
            watched_avg = watched_decade['IMDb Rating'].mean() if len(watched_decade) > 0 else 0
            catalog_avg = catalog_decade['imdb_rating'].mean()

            decade_data.append({
                'decade': f"{decade}s",
                'decade_num': decade,
                'watched': watched_count,
                'total_quality_films': total_count,
                'completeness_pct': completeness_pct,
                'missing_count': len(catalog_decade) - watched_count,
                'watched_avg_rating': watched_avg,
                'catalog_avg_rating': catalog_avg,
                'missing_films': missing_list,
                'era': self._get_era(decade)
            })

        self.decade_df = pd.DataFrame(decade_data)
        self.decade_df = self.decade_df.sort_values('decade_num')

        # Calculate era completeness
        self._analyze_era_completeness(quality_catalog)

        print(f"\nAnalyzed {len(self.decade_df)} decades")
        print(f"Average completeness: {self.decade_df['completeness_pct'].mean():.1f}%")

    def _analyze_era_completeness(self, quality_catalog):
        """Analyze cinema era completeness."""
        era_data = []

        for era in self.eras.keys():
            watched_era = self.watched_df[self.watched_df['era'] == era]
            catalog_era = quality_catalog[quality_catalog['era'] == era]

            if len(catalog_era) == 0:
                continue

            watched_count = len(watched_era)
            total_count = len(catalog_era)
            completeness_pct = (watched_count / total_count * 100) if total_count > 0 else 0

            watched_avg = watched_era['IMDb Rating'].mean() if len(watched_era) > 0 else 0
            catalog_avg = catalog_era['imdb_rating'].mean()

            era_data.append({
                'era': era,
                'watched': watched_count,
                'total_quality_films': total_count,
                'completeness_pct': completeness_pct,
                'missing_count': total_count - watched_count,
                'watched_avg_rating': watched_avg,
                'catalog_avg_rating': catalog_avg
            })

        self.era_df = pd.DataFrame(era_data)

    def visualize_completeness_overview(self):
        """Visualization 1-4: Overview."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Decade/Era Cinema Completeness', fontsize=16, fontweight='bold')

        # 1. Decade completeness timeline
        x = range(len(self.decade_df))
        width = 0.7

        axes[0, 0].bar(x, self.decade_df['watched'],
                      color='green', alpha=0.7, label='Watched')
        axes[0, 0].bar(x, self.decade_df['missing_count'],
                      bottom=self.decade_df['watched'],
                      color='red', alpha=0.7, label='Missing')

        axes[0, 0].set_xticks(x)
        axes[0, 0].set_xticklabels(self.decade_df['decade'], rotation=45, ha='right')
        axes[0, 0].set_ylabel('Film Count')
        axes[0, 0].set_title('Collection by Decade', fontweight='bold')
        axes[0, 0].legend()
        axes[0, 0].grid(True, alpha=0.3, axis='y')

        # Add completeness percentages
        for i, pct in enumerate(self.decade_df['completeness_pct']):
            total = self.decade_df.iloc[i]['total_quality_films']
            axes[0, 0].text(i, total + 5, f'{pct:.0f}%',
                          ha='center', fontsize=8, fontweight='bold')

        # 2. Completeness percentage by decade
        axes[0, 1].plot(self.decade_df['decade'], self.decade_df['completeness_pct'],
                       marker='o', linewidth=2, markersize=8, color='steelblue')
        axes[0, 1].fill_between(range(len(self.decade_df)),
                                self.decade_df['completeness_pct'],
                                alpha=0.3, color='steelblue')
        axes[0, 1].axhline(y=50, color='red', linestyle='--', alpha=0.5, label='50% threshold')
        axes[0, 1].set_xlabel('Decade')
        axes[0, 1].set_ylabel('Completeness %')
        axes[0, 1].set_title('Completeness Trend Over Time', fontweight='bold')
        axes[0, 1].grid(True, alpha=0.3)
        axes[0, 1].legend()
        axes[0, 1].set_ylim([0, 105])

        # 3. Era completeness
        if len(self.era_df) > 0:
            x = range(len(self.era_df))
            axes[1, 0].barh(x, self.era_df['watched'],
                          color='green', alpha=0.7, label='Watched')
            axes[1, 0].barh(x, self.era_df['missing_count'],
                          left=self.era_df['watched'],
                          color='red', alpha=0.7, label='Missing')

            axes[1, 0].set_yticks(x)
            axes[1, 0].set_yticklabels(self.era_df['era'], fontsize=9)
            axes[1, 0].set_xlabel('Film Count')
            axes[1, 0].set_title('Collection by Cinema Era', fontweight='bold')
            axes[1, 0].legend()
            axes[1, 0].invert_yaxis()

            for i, pct in enumerate(self.era_df['completeness_pct']):
                total = self.era_df.iloc[i]['total_quality_films']
                axes[1, 0].text(total + 5, i, f'{pct:.0f}%',
                              va='center', fontsize=8, fontweight='bold')

        # 4. Quality by decade
        x = range(len(self.decade_df))
        width = 0.35

        axes[1, 1].bar([i - width/2 for i in x], self.decade_df['watched_avg_rating'],
                      width, label='My Avg Rating', color='gold', alpha=0.7)
        axes[1, 1].bar([i + width/2 for i in x], self.decade_df['catalog_avg_rating'],
                      width, label='Catalog Avg', color='silver', alpha=0.7)

        axes[1, 1].set_xticks(x)
        axes[1, 1].set_xticklabels(self.decade_df['decade'], rotation=45, ha='right')
        axes[1, 1].set_ylabel('Average Rating')
        axes[1, 1].set_title('Quality by Decade', fontweight='bold')
        axes[1, 1].legend()
        axes[1, 1].grid(True, alpha=0.3, axis='y')
        axes[1, 1].set_ylim([6, 9])

        plt.tight_layout()
        plt.savefig(self.output_dir / 'completeness_overview.png', dpi=300, bbox_inches='tight')
        print(f"Saved: completeness_overview.png")
        plt.close()

    def visualize_missing_analysis(self):
        """Visualization 5-8: Missing films analysis."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Missing Films by Decade/Era', fontsize=16, fontweight='bold')

        # 1. Top missing films across all decades
        all_missing = []
        for _, row in self.decade_df.iterrows():
            for film in row['missing_films']:
                all_missing.append({
                    'title': film['title'],
                    'decade': row['decade'],
                    'rating': film['rating'],
                    'year': film['year']
                })

        if all_missing:
            missing_df = pd.DataFrame(all_missing).nlargest(20, 'rating')
            axes[0, 0].barh(range(len(missing_df)), missing_df['rating'], color='coral')
            axes[0, 0].set_yticks(range(len(missing_df)))
            labels = [f"{row['title'][:30]} ({row['decade']})"
                     for _, row in missing_df.iterrows()]
            axes[0, 0].set_yticklabels(labels, fontsize=7)
            axes[0, 0].set_xlabel('IMDB Rating')
            axes[0, 0].set_title('Top 20 Missing Films', fontweight='bold')
            axes[0, 0].invert_yaxis()
            axes[0, 0].axvline(x=8.0, color='red', linestyle='--', alpha=0.5)

        # 2. Decades with lowest completeness
        lowest_complete = self.decade_df.nsmallest(10, 'completeness_pct')

        axes[0, 1].barh(range(len(lowest_complete)), lowest_complete['completeness_pct'],
                       color='orange', alpha=0.7)
        axes[0, 1].set_yticks(range(len(lowest_complete)))
        axes[0, 1].set_yticklabels(lowest_complete['decade'], fontsize=9)
        axes[0, 1].set_xlabel('Completeness %')
        axes[0, 1].set_title('Decades Needing Exploration', fontweight='bold')
        axes[0, 1].invert_yaxis()

        for i, (pct, watched) in enumerate(zip(lowest_complete['completeness_pct'],
                                               lowest_complete['watched'])):
            axes[0, 1].text(pct + 1, i, f'{watched} watched',
                          va='center', fontsize=8)

        # 3. Missing count by decade
        axes[1, 0].bar(range(len(self.decade_df)), self.decade_df['missing_count'],
                      color='salmon', alpha=0.7)
        axes[1, 0].set_xticks(range(len(self.decade_df)))
        axes[1, 0].set_xticklabels(self.decade_df['decade'], rotation=45, ha='right')
        axes[1, 0].set_ylabel('Missing Films Count')
        axes[1, 0].set_title('Missing Films by Decade', fontweight='bold')
        axes[1, 0].grid(True, alpha=0.3, axis='y')

        # 4. Sample missing films table
        axes[1, 1].axis('off')

        # Get one missing film from each era
        sample_missing = []
        for _, row in self.decade_df.iterrows():
            if row['missing_films']:
                film = row['missing_films'][0]
                sample_missing.append([
                    row['decade'],
                    film['title'][:30],
                    f"{film['rating']:.1f}"
                ])

        if sample_missing:
            table = axes[1, 1].table(cellText=sample_missing[:10],
                                    colLabels=['Decade', 'Top Missing Film', 'Rating'],
                                    cellLoc='left',
                                    loc='center',
                                    colWidths=[0.2, 0.6, 0.2])
            table.auto_set_font_size(False)
            table.set_fontsize(7)
            table.scale(1, 2)

            for i in range(3):
                table[(0, i)].set_facecolor('#4CAF50')
                table[(0, i)].set_text_props(weight='bold', color='white')

        axes[1, 1].set_title('Sample Missing Films by Decade', fontweight='bold', pad=20)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'missing_analysis.png', dpi=300, bbox_inches='tight')
        print(f"Saved: missing_analysis.png")
        plt.close()

    def visualize_recommendations(self):
        """Visualization 9-12: Recommendations."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Decade Collection Recommendations', fontsize=16, fontweight='bold')

        # 1. Priority decades (low coverage, high quality)
        priority = self.decade_df[self.decade_df['completeness_pct'] < 50].copy()
        priority = priority.sort_values('catalog_avg_rating', ascending=False)

        if len(priority) > 0:
            axes[0, 0].barh(range(len(priority)), priority['catalog_avg_rating'],
                          color='purple', alpha=0.6)
            axes[0, 0].set_yticks(range(len(priority)))
            labels = [f"{row['decade']} ({row['completeness_pct']:.0f}%)"
                     for _, row in priority.iterrows()]
            axes[0, 0].set_yticklabels(labels, fontsize=9)
            axes[0, 0].set_xlabel('Catalog Avg Rating')
            axes[0, 0].set_title('Priority Decades to Explore (<50% coverage)', fontweight='bold')
            axes[0, 0].invert_yaxis()

        # 2. Watched vs Total by decade
        scatter = axes[0, 1].scatter(self.decade_df['watched'],
                                    self.decade_df['total_quality_films'],
                                    s=self.decade_df['completeness_pct'] * 3,
                                    alpha=0.6,
                                    c=self.decade_df['decade_num'],
                                    cmap='viridis')

        axes[0, 1].plot([0, self.decade_df['total_quality_films'].max()],
                       [0, self.decade_df['total_quality_films'].max()],
                       'r--', alpha=0.5, label='100% line')

        axes[0, 1].set_xlabel('Films Watched')
        axes[0, 1].set_ylabel('Total Quality Films')
        axes[0, 1].set_title('Coverage by Decade (size = completeness %)', fontweight='bold')
        axes[0, 1].legend()
        axes[0, 1].grid(True, alpha=0.3)

        for _, row in self.decade_df.iterrows():
            axes[0, 1].annotate(row['decade'],
                              (row['watched'], row['total_quality_films']),
                              fontsize=7, alpha=0.7)

        # 3. Era comparison
        if len(self.era_df) > 0:
            x = range(len(self.era_df))
            axes[1, 0].bar(x, self.era_df['completeness_pct'],
                          color=plt.cm.viridis(np.linspace(0, 1, len(self.era_df))),
                          alpha=0.7)
            axes[1, 0].set_xticks(x)
            axes[1, 0].set_xticklabels(self.era_df['era'], rotation=45, ha='right')
            axes[1, 0].set_ylabel('Completeness %')
            axes[1, 0].set_title('Completeness by Cinema Era', fontweight='bold')
            axes[1, 0].grid(True, alpha=0.3, axis='y')

            for i, pct in enumerate(self.era_df['completeness_pct']):
                axes[1, 0].text(i, pct + 2, f'{pct:.0f}%',
                              ha='center', fontsize=8, fontweight='bold')

        # 4. Statistics table
        axes[1, 1].axis('off')

        stats_data = [
            ['Total Decades Analyzed', len(self.decade_df)],
            ['Best Covered Decade', f"{self.decade_df.iloc[self.decade_df['completeness_pct'].idxmax()]['decade']} ({self.decade_df['completeness_pct'].max():.0f}%)"],
            ['Least Covered Decade', f"{self.decade_df.iloc[self.decade_df['completeness_pct'].idxmin()]['decade']} ({self.decade_df['completeness_pct'].min():.0f}%)"],
            ['Avg Completeness', f"{self.decade_df['completeness_pct'].mean():.1f}%"],
            ['Median Completeness', f"{self.decade_df['completeness_pct'].median():.1f}%"],
            ['Decades >50% Complete', len(self.decade_df[self.decade_df['completeness_pct'] >= 50])]
        ]

        table = axes[1, 1].table(cellText=stats_data,
                                colLabels=['Metric', 'Value'],
                                cellLoc='left',
                                loc='center',
                                colWidths=[0.6, 0.4])
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 2)

        for i in range(2):
            table[(0, i)].set_facecolor('#4CAF50')
            table[(0, i)].set_text_props(weight='bold', color='white')

        axes[1, 1].set_title('Statistics', fontweight='bold', pad=20)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'recommendations.png', dpi=300, bbox_inches='tight')
        print(f"Saved: recommendations.png")
        plt.close()

    def generate_report(self):
        """Generate comprehensive report."""
        report_path = self.report_dir / 'batch_41_decade_completeness_report.txt'

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("DECADE/ERA CINEMA COMPLETENESS ANALYSIS\n")
            f.write("=" * 80 + "\n\n")

            f.write("OVERALL STATISTICS\n")
            f.write("-" * 80 + "\n")
            f.write(f"Decades Analyzed: {len(self.decade_df)}\n")
            f.write(f"Average Completeness: {self.decade_df['completeness_pct'].mean():.1f}%\n")
            f.write(f"Median Completeness: {self.decade_df['completeness_pct'].median():.1f}%\n\n")

            # By decade
            f.write("\nCOMPLETENESS BY DECADE\n")
            f.write("-" * 80 + "\n")
            for _, row in self.decade_df.iterrows():
                f.write(f"{row['decade']}: {row['watched']}/{row['total_quality_films']} ")
                f.write(f"({row['completeness_pct']:.1f}%)\n")
                f.write(f"  Avg Rating: {row['catalog_avg_rating']:.2f}\n")
                if row['missing_films']:
                    f.write(f"  Top Missing: {row['missing_films'][0]['title']} ")
                    f.write(f"({row['missing_films'][0]['year']}) - {row['missing_films'][0]['rating']:.1f}\n")
                f.write("\n")

            # By era
            if len(self.era_df) > 0:
                f.write("\nCOMPLETENESS BY CINEMA ERA\n")
                f.write("-" * 80 + "\n")
                for _, row in self.era_df.iterrows():
                    f.write(f"{row['era']}: {row['watched']}/{row['total_quality_films']} ")
                    f.write(f"({row['completeness_pct']:.1f}%)\n")
                    f.write(f"  Avg Rating: {row['catalog_avg_rating']:.2f}\n\n")

            # Top missing
            f.write("\nTOP MISSING FILMS (All Decades)\n")
            f.write("-" * 80 + "\n")
            all_missing = []
            for _, row in self.decade_df.iterrows():
                for film in row['missing_films']:
                    all_missing.append({
                        'title': film['title'],
                        'decade': row['decade'],
                        'rating': film['rating'],
                        'year': film['year']
                    })

            missing_df = pd.DataFrame(all_missing).nlargest(30, 'rating')
            for idx, (_, row) in enumerate(missing_df.iterrows(), 1):
                f.write(f"{idx}. {row['title']} ({row['year']}) - {row['rating']:.1f}\n")
                f.write(f"   Decade: {row['decade']}\n\n")

        print(f"Report saved: {report_path}")

    def run_all_analyses(self):
        """Execute all analyses."""
        print("\n" + "=" * 80)
        print("BATCH 41: DECADE/ERA COMPLETENESS ANALYSIS")
        print("=" * 80 + "\n")

        print("Generating visualizations...")
        self.visualize_completeness_overview()
        self.visualize_missing_analysis()
        self.visualize_recommendations()

        print("\nGenerating report...")
        self.generate_report()

        print("\n" + "=" * 80)
        print("ANALYSIS COMPLETE!")
        print("=" * 80)
        print(f"\nVisualizations: {self.output_dir}")
        print(f"Report: {self.report_dir / 'batch_41_decade_completeness_report.txt'}")

if __name__ == "__main__":
    analyzer = DecadeCompletenessAnalyzer()
    analyzer.run_all_analyses()
