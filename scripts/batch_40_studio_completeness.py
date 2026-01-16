"""
Batch 40: Studio/Production Company Completeness Analysis
Extracts production companies from my WATCHED movies, finds their complete filmographies from TMDB,
and shows what I've watched vs what I'm missing from each studio.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict
import ast
from pathlib import Path
import numpy as np

class StudioCompletenessAnalyzer:
    def __init__(self,
                 watched_path='data/processed/watched_movies_master.csv',
                 catalog_path='data/processed/master_cinema_data.csv'):
        """Initialize with watched movies and catalog data."""

        print("Loading my watched movies...")
        self.watched_df = pd.read_csv(watched_path)
        print(f"Loaded {len(self.watched_df)} watched films")

        print("\nLoading catalog (with production companies)...")
        self.catalog_df = pd.read_csv(catalog_path)
        print(f"Loaded {len(self.catalog_df)} catalog films")

        # Setup directories
        self.output_dir = Path('analysis_outputs/visualizations/batch_40')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir = Path('analysis_outputs/reports')
        self.report_dir.mkdir(parents=True, exist_ok=True)

        # Analyze
        self._analyze_studio_completeness()

    def _extract_production_companies(self, df, source='watched'):
        """Extract production companies from dataframe."""
        studio_films = defaultdict(list)

        for idx, row in df.iterrows():
            # Try tmdb_production_companies first
            if 'tmdb_production_companies' in df.columns and pd.notna(row.get('tmdb_production_companies')):
                try:
                    companies = ast.literal_eval(row['tmdb_production_companies'])
                    for company in companies[:5]:  # Top 5 production companies
                        company_name = company.get('name', '')
                        if company_name:
                            if source == 'watched':
                                studio_films[company_name].append({
                                    'title': row.get('Title', row.get('title', '')),
                                    'year': row.get('Year', row.get('year', 0)),
                                    'rating': row.get('IMDb Rating', row.get('imdb_rating', 0))
                                })
                            else:
                                studio_films[company_name].append({
                                    'title': row.get('title', ''),
                                    'year': row.get('year', 0),
                                    'rating': row.get('imdb_rating', 0),
                                    'const': row.get('const', '')
                                })
                except:
                    continue

        return studio_films

    def _analyze_studio_completeness(self):
        """Analyze studio filmography completeness."""
        print("\nExtracting production companies from watched movies...")
        watched_studios = self._extract_production_companies(self.watched_df, 'watched')
        print(f"Found {len(watched_studios)} unique production companies in watched movies")

        print("\nExtracting production companies from catalog...")
        catalog_studios = self._extract_production_companies(self.catalog_df, 'catalog')
        print(f"Found {len(catalog_studios)} unique production companies in catalog")

        # Calculate completeness
        print("\nCalculating completeness...")
        completeness_data = []

        for studio, watched_films in watched_studios.items():
            if studio not in catalog_studios:
                continue

            catalog_films = catalog_studios[studio]

            # Filter catalog for quality films (rating >= 6.5)
            quality_catalog = [f for f in catalog_films if f['rating'] >= 6.5]

            if len(quality_catalog) < 5:  # Skip studios with few quality films
                continue

            # Find missing films
            watched_titles_lower = set(f['title'].lower() for f in watched_films)
            missing_films = [f for f in quality_catalog
                           if f['title'].lower() not in watched_titles_lower]

            # Sort by rating
            missing_films = sorted(missing_films, key=lambda x: x['rating'], reverse=True)

            total_count = len(quality_catalog)
            watched_count = len(watched_films)
            completeness_pct = (watched_count / total_count * 100) if total_count > 0 else 0

            # Calculate average ratings
            watched_avg = np.mean([f['rating'] for f in watched_films]) if watched_films else 0
            catalog_avg = np.mean([f['rating'] for f in quality_catalog])

            completeness_data.append({
                'studio': studio,
                'total_quality_films': total_count,
                'watched': watched_count,
                'completeness_pct': completeness_pct,
                'missing_count': len(missing_films),
                'watched_films': watched_films,
                'missing_films': missing_films[:10],
                'watched_avg_rating': watched_avg,
                'catalog_avg_rating': catalog_avg
            })

        self.completeness_df = pd.DataFrame(completeness_data)
        self.completeness_df = self.completeness_df.sort_values('completeness_pct', ascending=False)

        print(f"\nAnalyzed {len(self.completeness_df)} production companies")
        print(f"Average completeness: {self.completeness_df['completeness_pct'].mean():.1f}%")

    def visualize_completeness_overview(self):
        """Visualization 1-4: Overview with watched vs missing films shown."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Studio/Production Company Completeness (Watched vs Catalog)',
                     fontsize=16, fontweight='bold')

        # 1. Top studios by collection size
        top_studios = self.completeness_df.nlargest(15, 'watched')

        x = range(len(top_studios))
        axes[0, 0].barh(x, top_studios['watched'],
                       color='green', alpha=0.7, label='Watched')
        axes[0, 0].barh(x, top_studios['missing_count'],
                       left=top_studios['watched'],
                       color='red', alpha=0.7, label='Missing')

        axes[0, 0].set_yticks(x)
        axes[0, 0].set_yticklabels(top_studios['studio'], fontsize=8)
        axes[0, 0].set_xlabel('Films')
        axes[0, 0].set_title('Top 15 Studios by Collection Size', fontweight='bold')
        axes[0, 0].legend()
        axes[0, 0].invert_yaxis()

        for i, pct in enumerate(top_studios['completeness_pct']):
            total = top_studios.iloc[i]['total_quality_films']
            axes[0, 0].text(total + 1, i, f'{pct:.0f}%',
                          va='center', fontsize=7, fontweight='bold')

        # 2. Nearly complete studios (70-99%)
        nearly_complete = self.completeness_df[
            (self.completeness_df['completeness_pct'] >= 70) &
            (self.completeness_df['completeness_pct'] < 100)
        ].nlargest(15, 'completeness_pct')

        if len(nearly_complete) > 0:
            x = range(len(nearly_complete))
            axes[0, 1].barh(x, nearly_complete['watched'],
                          color='green', alpha=0.7, label='Watched')
            axes[0, 1].barh(x, nearly_complete['missing_count'],
                          left=nearly_complete['watched'],
                          color='red', alpha=0.7, label='Missing')

            axes[0, 1].set_yticks(x)
            axes[0, 1].set_yticklabels(nearly_complete['studio'], fontsize=8)
            axes[0, 1].set_xlabel('Films')
            axes[0, 1].set_title('Nearly Complete Studios (70-99%)', fontweight='bold')
            axes[0, 1].legend()
            axes[0, 1].invert_yaxis()

            for i, (pct, total) in enumerate(zip(nearly_complete['completeness_pct'],
                                                 nearly_complete['total_quality_films'])):
                axes[0, 1].text(total + 0.5, i, f'{pct:.1f}%',
                              va='center', fontsize=7)

        # 3. Completeness vs Quality
        scatter = axes[1, 0].scatter(self.completeness_df['completeness_pct'],
                                    self.completeness_df['catalog_avg_rating'],
                                    s=self.completeness_df['watched'] * 3,
                                    alpha=0.6,
                                    c=self.completeness_df['total_quality_films'],
                                    cmap='plasma')

        axes[1, 0].set_xlabel('Completeness %')
        axes[1, 0].set_ylabel('Catalog Avg Rating')
        axes[1, 0].set_title('Completeness vs Quality (size = films watched)', fontweight='bold')
        axes[1, 0].grid(True, alpha=0.3)

        # Annotate top studios
        for _, row in self.completeness_df.nlargest(5, 'watched').iterrows():
            axes[1, 0].annotate(row['studio'][:15],
                              (row['completeness_pct'], row['catalog_avg_rating']),
                              fontsize=7, alpha=0.7)

        cbar = plt.colorbar(scatter, ax=axes[1, 0])
        cbar.set_label('Total Quality Films', rotation=270, labelpad=15)

        # 4. Statistics
        stats_data = [
            ['Total Studios Analyzed', len(self.completeness_df)],
            ['Avg Completeness', f"{self.completeness_df['completeness_pct'].mean():.1f}%"],
            ['Median Completeness', f"{self.completeness_df['completeness_pct'].median():.1f}%"],
            ['Studios 70%+ Complete', len(self.completeness_df[self.completeness_df['completeness_pct'] >= 70])],
            ['Studios 50-69% Complete', len(self.completeness_df[(self.completeness_df['completeness_pct'] >= 50) &
                                                                 (self.completeness_df['completeness_pct'] < 70)])],
            ['Studios <50% Complete', len(self.completeness_df[self.completeness_df['completeness_pct'] < 50])]
        ]

        axes[1, 1].axis('tight')
        axes[1, 1].axis('off')
        table = axes[1, 1].table(cellText=stats_data,
                                colLabels=['Metric', 'Value'],
                                cellLoc='left',
                                loc='center',
                                colWidths=[0.7, 0.3])
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 2)

        for i in range(2):
            table[(0, i)].set_facecolor('#4CAF50')
            table[(0, i)].set_text_props(weight='bold', color='white')

        axes[1, 1].set_title('Statistics', fontweight='bold', pad=20)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'completeness_overview.png', dpi=300, bbox_inches='tight')
        print(f"Saved: completeness_overview.png")
        plt.close()

    def visualize_missing_films(self):
        """Visualization 5-8: Missing films analysis."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Missing Films Analysis (What I Haven\'t Watched)', fontsize=16, fontweight='bold')

        # 1. Top missing films
        all_missing = []
        for _, row in self.completeness_df.iterrows():
            for film in row['missing_films']:
                all_missing.append({
                    'title': film['title'],
                    'studio': row['studio'],
                    'rating': film['rating'],
                    'year': film['year']
                })

        if all_missing:
            missing_df = pd.DataFrame(all_missing).nlargest(20, 'rating')
            axes[0, 0].barh(range(len(missing_df)), missing_df['rating'], color='coral')
            axes[0, 0].set_yticks(range(len(missing_df)))
            labels = [f"{row['title'][:28]} - {row['studio'][:12]}"
                     for _, row in missing_df.iterrows()]
            axes[0, 0].set_yticklabels(labels, fontsize=7)
            axes[0, 0].set_xlabel('IMDB Rating')
            axes[0, 0].set_title('Top 20 Missing Films (Highest Rated)', fontweight='bold')
            axes[0, 0].invert_yaxis()
            axes[0, 0].axvline(x=8.0, color='red', linestyle='--', alpha=0.5)

            for i, v in enumerate(missing_df['rating']):
                axes[0, 0].text(v + 0.05, i, f'{v:.1f}', va='center', fontsize=7)

        # 2. Studios with most high-rated missing films
        high_rated_missing = []
        for _, row in self.completeness_df.iterrows():
            high_rated = [f for f in row['missing_films'] if f['rating'] >= 8.0]
            if high_rated:
                high_rated_missing.append({
                    'studio': row['studio'],
                    'count': len(high_rated),
                    'avg_rating': np.mean([f['rating'] for f in high_rated])
                })

        if high_rated_missing:
            hr_df = pd.DataFrame(high_rated_missing).nlargest(15, 'count')
            bars = axes[0, 1].barh(range(len(hr_df)), hr_df['count'])
            axes[0, 1].set_yticks(range(len(hr_df)))
            axes[0, 1].set_yticklabels(hr_df['studio'], fontsize=8)
            axes[0, 1].set_xlabel('High-Rated Missing Films (8.0+)')
            axes[0, 1].set_title('Studios with Most Missing Gems', fontweight='bold')
            axes[0, 1].invert_yaxis()

            norm = plt.Normalize(vmin=8.0, vmax=hr_df['avg_rating'].max())
            colors = plt.cm.YlOrRd(norm(hr_df['avg_rating']))
            for bar, color in zip(bars, colors):
                bar.set_color(color)

        # 3. Watched vs Total scatter
        scatter = axes[1, 0].scatter(self.completeness_df['watched'],
                                    self.completeness_df['total_quality_films'],
                                    s=self.completeness_df['completeness_pct'] * 2,
                                    alpha=0.6,
                                    c=self.completeness_df['completeness_pct'],
                                    cmap='RdYlGn')

        axes[1, 0].plot([0, self.completeness_df['total_quality_films'].max()],
                       [0, self.completeness_df['total_quality_films'].max()],
                       'r--', alpha=0.5, label='100% line')

        axes[1, 0].set_xlabel('Films Watched')
        axes[1, 0].set_ylabel('Total Quality Films (Catalog)')
        axes[1, 0].set_title('Watched vs Total (size = completeness %)', fontweight='bold')
        axes[1, 0].legend()
        axes[1, 0].grid(True, alpha=0.3)

        cbar = plt.colorbar(scatter, ax=axes[1, 0])
        cbar.set_label('Completeness %', rotation=270, labelpad=15)

        # 4. Sample watched films
        axes[1, 1].axis('off')

        top_studios = self.completeness_df.nlargest(5, 'watched')
        sample_text = "SAMPLE: Top Studios - What I've Watched\n" + "="*50 + "\n\n"

        for _, row in top_studios.iterrows():
            sample_text += f"{row['studio'][:30]} ({row['watched']}/{row['total_quality_films']})\n"
            sample_text += f"Watched: {', '.join([f['title'][:20] for f in row['watched_films'][:2]])}\n"
            if row['missing_count'] > 0:
                sample_text += f"Missing: {', '.join([f['title'][:20] for f in row['missing_films'][:2]])}\n"
            sample_text += "\n"

        axes[1, 1].text(0.1, 0.9, sample_text, fontsize=7,
                       verticalalignment='top', fontfamily='monospace')
        axes[1, 1].set_title('Sample: Watched vs Missing Films', fontweight='bold', pad=20)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'missing_films_analysis.png', dpi=300, bbox_inches='tight')
        print(f"Saved: missing_films_analysis.png")
        plt.close()

    def visualize_recommendations(self):
        """Visualization 9-12: Recommendations."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Studio Collection Recommendations', fontsize=16, fontweight='bold')

        # 1. Priority recommendations
        recommendations = []
        for _, row in self.completeness_df.iterrows():
            if row['missing_count'] > 0 and row['watched'] >= 3:
                if row['missing_films']:
                    top_missing = row['missing_films'][0]
                    recommendations.append({
                        'film': top_missing['title'],
                        'studio': row['studio'],
                        'rating': top_missing['rating'],
                        'year': top_missing['year']
                    })

        if recommendations:
            rec_df = pd.DataFrame(recommendations).nlargest(20, 'rating')
            axes[0, 0].barh(range(len(rec_df)), rec_df['rating'], color='teal', alpha=0.7)
            axes[0, 0].set_yticks(range(len(rec_df)))
            labels = [f"{row['film'][:22]} - {row['studio'][:12]}"
                     for _, row in rec_df.iterrows()]
            axes[0, 0].set_yticklabels(labels, fontsize=7)
            axes[0, 0].set_xlabel('IMDB Rating')
            axes[0, 0].set_title('Top Recommended Films', fontweight='bold')
            axes[0, 0].invert_yaxis()

        # 2. Major studios overview
        major_studios = ['Warner Bros.', 'Universal Pictures', 'Paramount Pictures',
                        'Columbia Pictures', '20th Century Fox', 'Walt Disney Pictures',
                        'Metro-Goldwyn-Mayer', 'DreamWorks Pictures']

        major_studio_data = []
        for studio in major_studios:
            matches = self.completeness_df[self.completeness_df['studio'].str.contains(studio, case=False, na=False)]
            if len(matches) > 0:
                row = matches.iloc[0]
                major_studio_data.append({
                    'studio': studio,
                    'watched': row['watched'],
                    'total': row['total_quality_films'],
                    'completeness': row['completeness_pct']
                })

        if major_studio_data:
            ms_df = pd.DataFrame(major_studio_data)
            x = range(len(ms_df))
            width = 0.35

            axes[0, 1].bar([i - width/2 for i in x], ms_df['watched'],
                          width, label='Watched', color='steelblue', alpha=0.7)
            axes[0, 1].bar([i + width/2 for i in x], ms_df['total'] - ms_df['watched'],
                          width, label='Missing', color='lightcoral', alpha=0.7)

            axes[0, 1].set_xticks(x)
            axes[0, 1].set_xticklabels([s[:15] for s in ms_df['studio']], rotation=45, ha='right', fontsize=7)
            axes[0, 1].set_ylabel('Film Count')
            axes[0, 1].set_title('Major Studios Collection Status', fontweight='bold')
            axes[0, 1].legend()
            axes[0, 1].grid(True, alpha=0.3, axis='y')

        # 3. Completeness distribution
        axes[1, 0].hist(self.completeness_df['completeness_pct'], bins=20,
                       color='skyblue', edgecolor='black')
        axes[1, 0].axvline(self.completeness_df['completeness_pct'].median(),
                          color='red', linestyle='--',
                          label=f"Median: {self.completeness_df['completeness_pct'].median():.1f}%")
        axes[1, 0].set_xlabel('Completeness %')
        axes[1, 0].set_ylabel('Number of Studios')
        axes[1, 0].set_title('Completeness Distribution', fontweight='bold')
        axes[1, 0].legend()
        axes[1, 0].grid(True, alpha=0.3)

        # 4. Recommendation table
        axes[1, 1].axis('off')
        if recommendations:
            top_recs = rec_df.head(10)
            table_data = [[row['studio'][:18], row['film'][:28], f"{row['rating']:.1f}"]
                         for _, row in top_recs.iterrows()]

            table = axes[1, 1].table(cellText=table_data,
                                    colLabels=['Studio', 'Film', 'Rating'],
                                    cellLoc='left',
                                    loc='center',
                                    colWidths=[0.3, 0.5, 0.2])
            table.auto_set_font_size(False)
            table.set_fontsize(7)
            table.scale(1, 2)

            for i in range(3):
                table[(0, i)].set_facecolor('#4CAF50')
                table[(0, i)].set_text_props(weight='bold', color='white')

        axes[1, 1].set_title('Top 10 Recommended Films', fontweight='bold', pad=20)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'recommendations.png', dpi=300, bbox_inches='tight')
        print(f"Saved: recommendations.png")
        plt.close()

    def generate_report(self):
        """Generate comprehensive report."""
        report_path = self.report_dir / 'batch_40_studio_completeness_report.txt'

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("STUDIO/PRODUCTION COMPANY COMPLETENESS ANALYSIS\n")
            f.write("(My Watched Movies vs Catalog)\n")
            f.write("=" * 80 + "\n\n")

            f.write("OVERALL STATISTICS\n")
            f.write("-" * 80 + "\n")
            f.write(f"Total Studios Analyzed: {len(self.completeness_df)}\n")
            f.write(f"Average Completeness: {self.completeness_df['completeness_pct'].mean():.1f}%\n")
            f.write(f"Median Completeness: {self.completeness_df['completeness_pct'].median():.1f}%\n\n")

            # Top collected studios
            f.write("\nTOP COLLECTED STUDIOS\n")
            f.write("-" * 80 + "\n")
            top_studios = self.completeness_df.nlargest(30, 'watched')

            for idx, (_, row) in enumerate(top_studios.iterrows(), 1):
                f.write(f"{idx}. {row['studio']}\n")
                f.write(f"   Watched: {row['watched']}/{row['total_quality_films']} ")
                f.write(f"({row['completeness_pct']:.1f}%)\n")
                f.write(f"   Avg Rating: {row['watched_avg_rating']:.2f}\n\n")

            # High-rated missing films
            f.write("\nTOP MISSING FILMS (Highest Rated)\n")
            f.write("-" * 80 + "\n")
            all_missing = []
            for _, row in self.completeness_df.iterrows():
                for film in row['missing_films']:
                    all_missing.append({
                        'title': film['title'],
                        'studio': row['studio'],
                        'rating': film['rating'],
                        'year': film['year']
                    })

            missing_df = pd.DataFrame(all_missing).nlargest(30, 'rating')
            for idx, (_, row) in enumerate(missing_df.iterrows(), 1):
                f.write(f"{idx}. {row['title']} ({row['year']}) - {row['rating']:.1f}\n")
                f.write(f"   Studio: {row['studio']}\n\n")

        print(f"Report saved: {report_path}")

    def run_all_analyses(self):
        """Execute all analyses."""
        print("\n" + "=" * 80)
        print("BATCH 40: STUDIO/PRODUCTION COMPANY COMPLETENESS ANALYSIS")
        print("=" * 80 + "\n")

        print("Generating visualizations...")
        self.visualize_completeness_overview()
        self.visualize_missing_films()
        self.visualize_recommendations()

        print("\nGenerating report...")
        self.generate_report()

        print("\n" + "=" * 80)
        print("ANALYSIS COMPLETE!")
        print("=" * 80)
        print(f"\nVisualizations: {self.output_dir}")
        print(f"Report: {self.report_dir / 'batch_40_studio_completeness_report.txt'}")

if __name__ == "__main__":
    analyzer = StudioCompletenessAnalyzer()
    analyzer.run_all_analyses()
