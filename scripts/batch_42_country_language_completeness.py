"""
Batch 42: Country/Language Completeness Analysis
Analyzes which countries and languages I've explored in my film collection.
Shows what notable films I'm missing from different world cinemas.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict, Counter
import ast
from pathlib import Path
import numpy as np

class CountryLanguageCompletenessAnalyzer:
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
        self.output_dir = Path('analysis_outputs/visualizations/batch_42')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir = Path('analysis_outputs/reports')
        self.report_dir.mkdir(parents=True, exist_ok=True)

        # Analyze
        self._analyze_country_completeness()
        self._analyze_language_completeness()

    def _extract_countries(self, df, source='watched'):
        """Extract countries from dataframe."""
        country_films = defaultdict(list)

        for idx, row in df.iterrows():
            if 'tmdb_production_countries' in df.columns and pd.notna(row.get('tmdb_production_countries')):
                try:
                    countries = ast.literal_eval(row['tmdb_production_countries'])
                    for country in countries[:3]:  # Top 3 countries
                        country_name = country.get('name', '')
                        if country_name:
                            if source == 'watched':
                                country_films[country_name].append({
                                    'title': row.get('Title', row.get('title', '')),
                                    'year': row.get('Year', row.get('year', 0)),
                                    'rating': row.get('IMDb Rating', row.get('imdb_rating', 0))
                                })
                            else:
                                country_films[country_name].append({
                                    'title': row.get('title', ''),
                                    'year': row.get('year', 0),
                                    'rating': row.get('imdb_rating', 0)
                                })
                except:
                    continue

        return country_films

    def _extract_languages(self, df, source='watched'):
        """Extract languages from dataframe."""
        language_films = defaultdict(list)

        for idx, row in df.iterrows():
            if 'tmdb_spoken_languages' in df.columns and pd.notna(row.get('tmdb_spoken_languages')):
                try:
                    languages = ast.literal_eval(row['tmdb_spoken_languages'])
                    for language in languages[:2]:  # Top 2 languages
                        lang_name = language.get('english_name', '')
                        if lang_name:
                            if source == 'watched':
                                language_films[lang_name].append({
                                    'title': row.get('Title', row.get('title', '')),
                                    'year': row.get('Year', row.get('year', 0)),
                                    'rating': row.get('IMDb Rating', row.get('imdb_rating', 0))
                                })
                            else:
                                language_films[lang_name].append({
                                    'title': row.get('title', ''),
                                    'year': row.get('year', 0),
                                    'rating': row.get('imdb_rating', 0)
                                })
                except:
                    continue

        return language_films

    def _analyze_country_completeness(self):
        """Analyze country completeness."""
        print("\nAnalyzing country completeness...")

        watched_countries = self._extract_countries(self.watched_df, 'watched')
        catalog_countries = self._extract_countries(self.catalog_df, 'catalog')

        completeness_data = []

        for country, watched_films in watched_countries.items():
            if country not in catalog_countries:
                continue

            catalog_films = catalog_countries[country]
            quality_catalog = [f for f in catalog_films if f['rating'] >= 6.5]

            if len(quality_catalog) < 5:
                continue

            watched_titles_lower = set(f['title'].lower() for f in watched_films)
            missing_films = [f for f in quality_catalog
                           if f['title'].lower() not in watched_titles_lower]
            missing_films = sorted(missing_films, key=lambda x: x['rating'], reverse=True)

            total_count = len(quality_catalog)
            watched_count = len(watched_films)
            completeness_pct = (watched_count / total_count * 100) if total_count > 0 else 0

            watched_avg = np.mean([f['rating'] for f in watched_films]) if watched_films else 0
            catalog_avg = np.mean([f['rating'] for f in quality_catalog])

            completeness_data.append({
                'country': country,
                'total_quality_films': total_count,
                'watched': watched_count,
                'completeness_pct': completeness_pct,
                'missing_count': len(missing_films),
                'watched_avg_rating': watched_avg,
                'catalog_avg_rating': catalog_avg,
                'missing_films': missing_films[:10]
            })

        self.country_df = pd.DataFrame(completeness_data)
        self.country_df = self.country_df.sort_values('watched', ascending=False)

        print(f"Analyzed {len(self.country_df)} countries")

    def _analyze_language_completeness(self):
        """Analyze language completeness."""
        print("Analyzing language completeness...")

        watched_languages = self._extract_languages(self.watched_df, 'watched')
        catalog_languages = self._extract_languages(self.catalog_df, 'catalog')

        completeness_data = []

        for language, watched_films in watched_languages.items():
            if language not in catalog_languages:
                continue

            catalog_films = catalog_languages[language]
            quality_catalog = [f for f in catalog_films if f['rating'] >= 6.5]

            if len(quality_catalog) < 5:
                continue

            watched_titles_lower = set(f['title'].lower() for f in watched_films)
            missing_films = [f for f in quality_catalog
                           if f['title'].lower() not in watched_titles_lower]
            missing_films = sorted(missing_films, key=lambda x: x['rating'], reverse=True)

            total_count = len(quality_catalog)
            watched_count = len(watched_films)
            completeness_pct = (watched_count / total_count * 100) if total_count > 0 else 0

            watched_avg = np.mean([f['rating'] for f in watched_films]) if watched_films else 0
            catalog_avg = np.mean([f['rating'] for f in quality_catalog])

            completeness_data.append({
                'language': language,
                'total_quality_films': total_count,
                'watched': watched_count,
                'completeness_pct': completeness_pct,
                'missing_count': len(missing_films),
                'watched_avg_rating': watched_avg,
                'catalog_avg_rating': catalog_avg,
                'missing_films': missing_films[:10]
            })

        self.language_df = pd.DataFrame(completeness_data)
        self.language_df = self.language_df.sort_values('watched', ascending=False)

        print(f"Analyzed {len(self.language_df)} languages")

    def visualize_completeness_overview(self):
        """Visualization 1-4: Overview."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Country/Language Cinema Completeness', fontsize=16, fontweight='bold')

        # 1. Top countries by collection
        top_countries = self.country_df.nlargest(15, 'watched')

        x = range(len(top_countries))
        axes[0, 0].barh(x, top_countries['watched'],
                       color='green', alpha=0.7, label='Watched')
        axes[0, 0].barh(x, top_countries['missing_count'],
                       left=top_countries['watched'],
                       color='red', alpha=0.7, label='Missing')

        axes[0, 0].set_yticks(x)
        axes[0, 0].set_yticklabels(top_countries['country'], fontsize=8)
        axes[0, 0].set_xlabel('Films')
        axes[0, 0].set_title('Top 15 Countries by Collection', fontweight='bold')
        axes[0, 0].legend()
        axes[0, 0].invert_yaxis()

        for i, pct in enumerate(top_countries['completeness_pct']):
            total = top_countries.iloc[i]['total_quality_films']
            axes[0, 0].text(total + 2, i, f'{pct:.0f}%',
                          va='center', fontsize=7, fontweight='bold')

        # 2. Top languages by collection
        top_languages = self.language_df.nlargest(15, 'watched')

        x = range(len(top_languages))
        axes[0, 1].barh(x, top_languages['watched'],
                       color='steelblue', alpha=0.7, label='Watched')
        axes[0, 1].barh(x, top_languages['missing_count'],
                       left=top_languages['watched'],
                       color='lightcoral', alpha=0.7, label='Missing')

        axes[0, 1].set_yticks(x)
        axes[0, 1].set_yticklabels(top_languages['language'], fontsize=8)
        axes[0, 1].set_xlabel('Films')
        axes[0, 1].set_title('Top 15 Languages by Collection', fontweight='bold')
        axes[0, 1].legend()
        axes[0, 1].invert_yaxis()

        for i, pct in enumerate(top_languages['completeness_pct']):
            total = top_languages.iloc[i]['total_quality_films']
            axes[0, 1].text(total + 2, i, f'{pct:.0f}%',
                          va='center', fontsize=7, fontweight='bold')

        # 3. Country diversity (collection distribution)
        country_pct = (self.country_df['watched'] / self.country_df['watched'].sum() * 100)
        top_10_countries = country_pct.nlargest(10)

        axes[1, 0].pie(top_10_countries, labels=self.country_df.iloc[top_10_countries.index]['country'],
                      autopct='%1.1f%%', startangle=90)
        axes[1, 0].set_title('Collection Distribution (Top 10 Countries)', fontweight='bold')

        # 4. Language diversity
        lang_pct = (self.language_df['watched'] / self.language_df['watched'].sum() * 100)
        top_8_languages = lang_pct.nlargest(8)

        axes[1, 1].pie(top_8_languages, labels=self.language_df.iloc[top_8_languages.index]['language'],
                      autopct='%1.1f%%', startangle=90)
        axes[1, 1].set_title('Collection Distribution (Top 8 Languages)', fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.output_dir / 'completeness_overview.png', dpi=300, bbox_inches='tight')
        print(f"Saved: completeness_overview.png")
        plt.close()

    def visualize_missing_analysis(self):
        """Visualization 5-8: Missing films analysis."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Missing Films - World Cinema', fontsize=16, fontweight='bold')

        # 1. Countries with most high-rated missing films
        high_rated_missing = []
        for _, row in self.country_df.iterrows():
            high_rated = [f for f in row['missing_films'] if f['rating'] >= 8.0]
            if high_rated:
                high_rated_missing.append({
                    'country': row['country'],
                    'count': len(high_rated),
                    'avg_rating': np.mean([f['rating'] for f in high_rated])
                })

        if high_rated_missing:
            hr_df = pd.DataFrame(high_rated_missing).nlargest(15, 'count')
            bars = axes[0, 0].barh(range(len(hr_df)), hr_df['count'])
            axes[0, 0].set_yticks(range(len(hr_df)))
            axes[0, 0].set_yticklabels(hr_df['country'], fontsize=8)
            axes[0, 0].set_xlabel('High-Rated Missing Films (8.0+)')
            axes[0, 0].set_title('Countries with Most Missing Gems', fontweight='bold')
            axes[0, 0].invert_yaxis()

            norm = plt.Normalize(vmin=8.0, vmax=hr_df['avg_rating'].max())
            colors = plt.cm.YlOrRd(norm(hr_df['avg_rating']))
            for bar, color in zip(bars, colors):
                bar.set_color(color)

        # 2. Underexplored high-quality countries
        underexplored = self.country_df[
            (self.country_df['completeness_pct'] < 30) &
            (self.country_df['catalog_avg_rating'] >= 7.0) &
            (self.country_df['total_quality_films'] >= 10)
        ].nlargest(15, 'catalog_avg_rating')

        if len(underexplored) > 0:
            axes[0, 1].barh(range(len(underexplored)), underexplored['catalog_avg_rating'],
                          color='orange', alpha=0.7)
            axes[0, 1].set_yticks(range(len(underexplored)))
            labels = [f"{row['country']} ({row['watched']}/{row['total_quality_films']})"
                     for _, row in underexplored.iterrows()]
            axes[0, 1].set_yticklabels(labels, fontsize=8)
            axes[0, 1].set_xlabel('Catalog Avg Rating')
            axes[0, 1].set_title('Underexplored Countries (High Quality)', fontweight='bold')
            axes[0, 1].invert_yaxis()

        # 3. Top missing films (all countries)
        all_missing = []
        for _, row in self.country_df.iterrows():
            for film in row['missing_films'][:3]:
                all_missing.append({
                    'title': film['title'],
                    'country': row['country'],
                    'rating': film['rating'],
                    'year': film['year']
                })

        if all_missing:
            missing_df = pd.DataFrame(all_missing).nlargest(20, 'rating')
            axes[1, 0].barh(range(len(missing_df)), missing_df['rating'], color='coral')
            axes[1, 0].set_yticks(range(len(missing_df)))
            labels = [f"{row['title'][:25]} - {row['country'][:12]}"
                     for _, row in missing_df.iterrows()]
            axes[1, 0].set_yticklabels(labels, fontsize=7)
            axes[1, 0].set_xlabel('IMDB Rating')
            axes[1, 0].set_title('Top 20 Missing Films (World Cinema)', fontweight='bold')
            axes[1, 0].invert_yaxis()
            axes[1, 0].axvline(x=8.0, color='red', linestyle='--', alpha=0.5)

        # 4. Completeness vs Quality (countries)
        scatter = axes[1, 1].scatter(self.country_df['completeness_pct'],
                                    self.country_df['catalog_avg_rating'],
                                    s=self.country_df['watched'] * 3,
                                    alpha=0.6,
                                    c=self.country_df['total_quality_films'],
                                    cmap='plasma')

        axes[1, 1].set_xlabel('Completeness %')
        axes[1, 1].set_ylabel('Catalog Avg Rating')
        axes[1, 1].set_title('Completeness vs Quality (size = films watched)', fontweight='bold')
        axes[1, 1].grid(True, alpha=0.3)

        # Annotate top countries
        for _, row in self.country_df.nlargest(5, 'watched').iterrows():
            axes[1, 1].annotate(row['country'][:12],
                              (row['completeness_pct'], row['catalog_avg_rating']),
                              fontsize=7, alpha=0.7)

        cbar = plt.colorbar(scatter, ax=axes[1, 1])
        cbar.set_label('Total Quality Films', rotation=270, labelpad=15)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'missing_analysis.png', dpi=300, bbox_inches='tight')
        print(f"Saved: missing_analysis.png")
        plt.close()

    def visualize_recommendations(self):
        """Visualization 9-12: Recommendations."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('World Cinema Recommendations', fontsize=16, fontweight='bold')

        # 1. Priority countries to explore
        priority = self.country_df[
            (self.country_df['completeness_pct'] < 40) &
            (self.country_df['total_quality_films'] >= 10)
        ].nlargest(15, 'catalog_avg_rating')

        if len(priority) > 0:
            axes[0, 0].barh(range(len(priority)), priority['catalog_avg_rating'],
                          color='purple', alpha=0.6)
            axes[0, 0].set_yticks(range(len(priority)))
            labels = [f"{row['country']} ({row['completeness_pct']:.0f}%)"
                     for _, row in priority.iterrows()]
            axes[0, 0].set_yticklabels(labels, fontsize=8)
            axes[0, 0].set_xlabel('Catalog Avg Rating')
            axes[0, 0].set_title('Priority Countries to Explore', fontweight='bold')
            axes[0, 0].invert_yaxis()

        # 2. Non-English language recommendations
        non_english = self.language_df[
            (~self.language_df['language'].isin(['English'])) &
            (self.language_df['total_quality_films'] >= 10)
        ].nlargest(15, 'watched')

        if len(non_english) > 0:
            x = range(len(non_english))
            width = 0.35

            axes[0, 1].bar([i - width/2 for i in x], non_english['watched'],
                          width, label='Watched', color='steelblue', alpha=0.7)
            axes[0, 1].bar([i + width/2 for i in x], non_english['missing_count'],
                          width, label='Missing', color='lightcoral', alpha=0.7)

            axes[0, 1].set_xticks(x)
            axes[0, 1].set_xticklabels(non_english['language'], rotation=45, ha='right', fontsize=7)
            axes[0, 1].set_ylabel('Film Count')
            axes[0, 1].set_title('Top Non-English Languages', fontweight='bold')
            axes[0, 1].legend()
            axes[0, 1].grid(True, alpha=0.3, axis='y')

        # 3. Geographic diversity score
        axes[1, 0].axis('off')

        # Calculate diversity metrics
        total_countries = len(self.country_df)
        countries_50plus = len(self.country_df[self.country_df['completeness_pct'] >= 50])
        total_languages = len(self.language_df)
        languages_10plus = len(self.language_df[self.language_df['watched'] >= 10])

        diversity_text = "GEOGRAPHIC DIVERSITY METRICS\n" + "="*50 + "\n\n"
        diversity_text += f"Countries Explored: {total_countries}\n"
        diversity_text += f"Countries 50%+ Complete: {countries_50plus}\n"
        diversity_text += f"Languages Explored: {total_languages}\n"
        diversity_text += f"Languages with 10+ Films: {languages_10plus}\n\n"

        diversity_text += "TOP COUNTRIES:\n"
        for _, row in self.country_df.nlargest(5, 'watched').iterrows():
            diversity_text += f"  {row['country']}: {row['watched']} films\n"

        diversity_text += "\nTOP LANGUAGES:\n"
        for _, row in self.language_df.nlargest(5, 'watched').iterrows():
            diversity_text += f"  {row['language']}: {row['watched']} films\n"

        axes[1, 0].text(0.1, 0.9, diversity_text, fontsize=9,
                       verticalalignment='top', fontfamily='monospace')
        axes[1, 0].set_title('Diversity Overview', fontweight='bold', pad=20)

        # 4. Recommendation table
        axes[1, 1].axis('off')

        recommendations = []
        for _, row in self.country_df.nlargest(8, 'catalog_avg_rating').iterrows():
            if row['missing_films']:
                film = row['missing_films'][0]
                recommendations.append([
                    row['country'][:15],
                    film['title'][:25],
                    f"{film['rating']:.1f}"
                ])

        if recommendations:
            table = axes[1, 1].table(cellText=recommendations,
                                    colLabels=['Country', 'Top Missing Film', 'Rating'],
                                    cellLoc='left',
                                    loc='center',
                                    colWidths=[0.25, 0.55, 0.2])
            table.auto_set_font_size(False)
            table.set_fontsize(7)
            table.scale(1, 2)

            for i in range(3):
                table[(0, i)].set_facecolor('#4CAF50')
                table[(0, i)].set_text_props(weight='bold', color='white')

        axes[1, 1].set_title('Top Recommendations by Country', fontweight='bold', pad=20)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'recommendations.png', dpi=300, bbox_inches='tight')
        print(f"Saved: recommendations.png")
        plt.close()

    def generate_report(self):
        """Generate comprehensive report."""
        report_path = self.report_dir / 'batch_42_country_language_completeness_report.txt'

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("COUNTRY/LANGUAGE CINEMA COMPLETENESS ANALYSIS\n")
            f.write("=" * 80 + "\n\n")

            # Countries
            f.write("TOP COUNTRIES BY COLLECTION\n")
            f.write("-" * 80 + "\n")
            for idx, (_, row) in enumerate(self.country_df.nlargest(30, 'watched').iterrows(), 1):
                f.write(f"{idx}. {row['country']}\n")
                f.write(f"   Watched: {row['watched']}/{row['total_quality_films']} ")
                f.write(f"({row['completeness_pct']:.1f}%)\n")
                f.write(f"   Avg Rating: {row['catalog_avg_rating']:.2f}\n")
                if row['missing_films']:
                    f.write(f"   Top Missing: {row['missing_films'][0]['title']} - {row['missing_films'][0]['rating']:.1f}\n")
                f.write("\n")

            # Languages
            f.write("\nTOP LANGUAGES BY COLLECTION\n")
            f.write("-" * 80 + "\n")
            for idx, (_, row) in enumerate(self.language_df.nlargest(20, 'watched').iterrows(), 1):
                f.write(f"{idx}. {row['language']}\n")
                f.write(f"   Watched: {row['watched']}/{row['total_quality_films']} ")
                f.write(f"({row['completeness_pct']:.1f}%)\n")
                f.write(f"   Avg Rating: {row['catalog_avg_rating']:.2f}\n\n")

            # Priority countries
            f.write("\nPRIORITY COUNTRIES TO EXPLORE\n")
            f.write("-" * 80 + "\n")
            priority = self.country_df[
                (self.country_df['completeness_pct'] < 40) &
                (self.country_df['total_quality_films'] >= 10)
            ].nlargest(15, 'catalog_avg_rating')

            for idx, (_, row) in enumerate(priority.iterrows(), 1):
                f.write(f"{idx}. {row['country']} - {row['catalog_avg_rating']:.2f} avg rating\n")
                f.write(f"   Watched: {row['watched']}/{row['total_quality_films']} ({row['completeness_pct']:.1f}%)\n")
                f.write(f"   Top Missing Films:\n")
                for film in row['missing_films'][:5]:
                    f.write(f"   - {film['title']} ({film['year']}) - {film['rating']:.1f}\n")
                f.write("\n")

        print(f"Report saved: {report_path}")

    def run_all_analyses(self):
        """Execute all analyses."""
        print("\n" + "=" * 80)
        print("BATCH 42: COUNTRY/LANGUAGE COMPLETENESS ANALYSIS")
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
        print(f"Report: {self.report_dir / 'batch_42_country_language_completeness_report.txt'}")

if __name__ == "__main__":
    analyzer = CountryLanguageCompletenessAnalyzer()
    analyzer.run_all_analyses()
