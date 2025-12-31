"""
Batch 39: Genre Completeness Analysis
Uses full IMDB data to analyze genre collection completeness against your watchlist.
Shows which genres you've explored thoroughly and what notable films you're missing per genre.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict, Counter
from pathlib import Path
import numpy as np

class GenreCompletenessAnalyzer:
    def __init__(self,
                 watchlist_path='data/raw/Watchlist_IMDB.csv',
                 title_basics_path='data/raw/title.basics.tsv',
                 title_ratings_path='data/raw/title.ratings.tsv'):
        """Initialize with IMDB raw data and your watchlist."""

        self.watchlist_path = watchlist_path
        print("Loading your watchlist...")
        self.watchlist_df = pd.read_csv(watchlist_path)
        print(f"Loaded {len(self.watchlist_df)} films from your watchlist")

        # Get watchlist title IDs
        self.watchlist_ids = set(self.watchlist_df['Const'].str.replace('tt', ''))

        print("\nLoading IMDB title basics...")
        self.titles_df = pd.read_csv(title_basics_path, sep='\t', low_memory=False)
        # Filter to movies only
        self.titles_df = self.titles_df[
            (self.titles_df['titleType'].isin(['movie', 'tvMovie'])) &
            (self.titles_df['isAdult'] == '0')
        ]
        print(f"Loaded {len(self.titles_df)} movie titles")

        print("\nLoading IMDB ratings...")
        self.ratings_df = pd.read_csv(title_ratings_path, sep='\t')
        # Filter for well-rated films (>= 6.5 and 5000+ votes for genre analysis)
        self.ratings_df = self.ratings_df[
            (self.ratings_df['averageRating'] >= 6.5) &
            (self.ratings_df['numVotes'] >= 5000)
        ]
        print(f"Loaded {len(self.ratings_df)} quality-rated titles")

        # Setup directories
        self.output_dir = Path('analysis_outputs/visualizations/batch_39')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir = Path('analysis_outputs/reports')
        self.report_dir.mkdir(parents=True, exist_ok=True)

        # Analyze
        self._analyze_genre_completeness()

    def _analyze_genre_completeness(self):
        """Analyze genre completeness against watchlist."""
        print("\nAnalyzing genre completeness...")

        # Merge ratings with titles
        quality_titles = pd.merge(
            self.titles_df,
            self.ratings_df,
            on='tconst',
            how='inner'
        )

        # Build genre collections
        print("Building genre filmographies from quality films...")
        genre_filmographies = defaultdict(lambda: {'total': [], 'on_watchlist': []})

        for _, row in quality_titles.iterrows():
            title_id = row['tconst']
            genres = row.get('genres', '')

            if pd.isna(genres) or genres == '\\N':
                continue

            # Split multiple genres
            genre_list = genres.split(',')
            title_id_clean = title_id.replace('tt', '')

            for genre in genre_list:
                genre = genre.strip()
                if not genre:
                    continue

                # Add to total filmography
                genre_filmographies[genre]['total'].append({
                    'tconst': title_id,
                    'title': row['primaryTitle'],
                    'year': row.get('startYear', 'N/A'),
                    'rating': row.get('averageRating', 0),
                    'votes': row.get('numVotes', 0)
                })

                # Check if on watchlist
                if title_id_clean in self.watchlist_ids:
                    genre_filmographies[genre]['on_watchlist'].append({
                        'tconst': title_id,
                        'title': row['primaryTitle'],
                        'year': row.get('startYear', 'N/A'),
                        'rating': row.get('averageRating', 0)
                    })

        print(f"Found {len(genre_filmographies)} genres with quality films")

        # Calculate completeness
        completeness_data = []

        for genre, data in genre_filmographies.items():
            total_count = len(data['total'])
            watchlist_count = len(data['on_watchlist'])

            if total_count < 10:  # Skip genres with very few films
                continue

            completeness_pct = (watchlist_count / total_count) * 100
            missing_count = total_count - watchlist_count

            # Get missing films
            watchlist_ids_set = set(f['tconst'] for f in data['on_watchlist'])
            missing_films = [f for f in data['total'] if f['tconst'] not in watchlist_ids_set]

            # Sort by rating and votes
            missing_films = sorted(missing_films,
                                  key=lambda x: (x['rating'], x['votes']),
                                  reverse=True)

            # Get watchlist average rating
            watchlist_avg = np.mean([f['rating'] for f in data['on_watchlist']]) if data['on_watchlist'] else 0

            # Get total average rating
            total_avg = np.mean([f['rating'] for f in data['total']])

            completeness_data.append({
                'genre': genre,
                'total_quality_films': total_count,
                'on_watchlist': watchlist_count,
                'completeness_pct': completeness_pct,
                'missing_count': missing_count,
                'missing_films': missing_films[:20],  # Top 20
                'watchlist_avg_rating': watchlist_avg,
                'total_avg_rating': total_avg
            })

        # Convert to DataFrame
        self.completeness_df = pd.DataFrame(completeness_data)
        self.completeness_df = self.completeness_df.sort_values('completeness_pct', ascending=False)

        print(f"\nAnalyzed {len(self.completeness_df)} genres")
        print(f"Average completeness: {self.completeness_df['completeness_pct'].mean():.1f}%")

    def visualize_completeness_overview(self):
        """Visualization 1-4: Overall genre completeness overview."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Genre Collection Completeness (Watchlist vs Full IMDB)',
                     fontsize=16, fontweight='bold')

        # 1. Genre completeness bars
        top_genres = self.completeness_df.nlargest(20, 'on_watchlist')

        axes[0, 0].barh(range(len(top_genres)), top_genres['completeness_pct'],
                       color=plt.cm.viridis(np.linspace(0, 1, len(top_genres))))
        axes[0, 0].set_yticks(range(len(top_genres)))
        axes[0, 0].set_yticklabels(top_genres['genre'], fontsize=9)
        axes[0, 0].set_xlabel('Completeness %')
        axes[0, 0].set_title('Top 20 Genres by Collection Size', fontweight='bold')
        axes[0, 0].axvline(x=50, color='red', linestyle='--', alpha=0.5, label='50%')
        axes[0, 0].invert_yaxis()
        axes[0, 0].legend()

        for i, (pct, count) in enumerate(zip(top_genres['completeness_pct'],
                                             top_genres['on_watchlist'])):
            axes[0, 0].text(pct + 1, i, f'{pct:.1f}% ({count})',
                          va='center', fontsize=7)

        # 2. Collection size comparison
        x = range(len(top_genres))
        width = 0.35

        axes[0, 1].bar([i - width/2 for i in x], top_genres['on_watchlist'],
                      width, label='On Watchlist', color='green', alpha=0.7)
        axes[0, 1].bar([i + width/2 for i in x], top_genres['missing_count'],
                      width, label='Missing (Quality)', color='red', alpha=0.7)

        axes[0, 1].set_xticks(x)
        axes[0, 1].set_xticklabels(top_genres['genre'], rotation=45, ha='right', fontsize=8)
        axes[0, 1].set_ylabel('Film Count')
        axes[0, 1].set_title('Collection Status by Genre', fontweight='bold')
        axes[0, 1].legend()
        axes[0, 1].grid(True, alpha=0.3, axis='y')

        # 3. Completeness distribution
        axes[1, 0].hist(self.completeness_df['completeness_pct'], bins=15,
                       color='skyblue', edgecolor='black')
        axes[1, 0].axvline(self.completeness_df['completeness_pct'].median(),
                          color='red', linestyle='--',
                          label=f"Median: {self.completeness_df['completeness_pct'].median():.1f}%")
        axes[1, 0].set_xlabel('Completeness %')
        axes[1, 0].set_ylabel('Number of Genres')
        axes[1, 0].set_title('Genre Completeness Distribution', fontweight='bold')
        axes[1, 0].legend()
        axes[1, 0].grid(True, alpha=0.3)

        # 4. Quality comparison
        scatter = axes[1, 1].scatter(self.completeness_df['completeness_pct'],
                                    self.completeness_df['watchlist_avg_rating'],
                                    s=self.completeness_df['on_watchlist'] * 2,
                                    alpha=0.6,
                                    c=self.completeness_df['total_quality_films'],
                                    cmap='plasma')

        axes[1, 1].set_xlabel('Completeness %')
        axes[1, 1].set_ylabel('Avg Rating (Watchlist)')
        axes[1, 1].set_title('Completeness vs Quality (size = collection size)', fontweight='bold')
        axes[1, 1].grid(True, alpha=0.3)

        # Annotate interesting genres
        for _, row in self.completeness_df.nlargest(5, 'on_watchlist').iterrows():
            axes[1, 1].annotate(row['genre'],
                              (row['completeness_pct'], row['watchlist_avg_rating']),
                              fontsize=7, alpha=0.7)

        cbar = plt.colorbar(scatter, ax=axes[1, 1])
        cbar.set_label('Total Quality Films', rotation=270, labelpad=15)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'completeness_overview.png', dpi=300, bbox_inches='tight')
        print(f"Saved: completeness_overview.png")
        plt.close()

    def visualize_missing_analysis(self):
        """Visualization 5-8: Missing films analysis."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Missing Films Analysis by Genre', fontsize=16, fontweight='bold')

        # 1. Genres with most high-rated missing films
        high_rated_missing = []
        for _, row in self.completeness_df.iterrows():
            high_rated = [f for f in row['missing_films'] if f['rating'] >= 8.0]
            if high_rated:
                high_rated_missing.append({
                    'genre': row['genre'],
                    'count': len(high_rated),
                    'avg_rating': np.mean([f['rating'] for f in high_rated])
                })

        if high_rated_missing:
            hr_df = pd.DataFrame(high_rated_missing).nlargest(15, 'count')
            bars = axes[0, 0].barh(range(len(hr_df)), hr_df['count'])
            axes[0, 0].set_yticks(range(len(hr_df)))
            axes[0, 0].set_yticklabels(hr_df['genre'], fontsize=9)
            axes[0, 0].set_xlabel('High-Rated Missing Films (8.0+)')
            axes[0, 0].set_title('Genres with Most Missing Gems', fontweight='bold')
            axes[0, 0].invert_yaxis()

            norm = plt.Normalize(vmin=8.0, vmax=hr_df['avg_rating'].max())
            colors = plt.cm.YlOrRd(norm(hr_df['avg_rating']))
            for bar, color in zip(bars, colors):
                bar.set_color(color)

        # 2. Coverage gaps (low completeness, high quality)
        coverage_gaps = self.completeness_df[
            (self.completeness_df['completeness_pct'] < 30) &
            (self.completeness_df['total_quality_films'] >= 50)
        ].nlargest(15, 'total_avg_rating')

        if len(coverage_gaps) > 0:
            axes[0, 1].barh(range(len(coverage_gaps)), coverage_gaps['total_avg_rating'],
                          color='orange', alpha=0.7)
            axes[0, 1].set_yticks(range(len(coverage_gaps)))
            labels = [f"{row['genre']} ({row['on_watchlist']}/{row['total_quality_films']})"
                     for _, row in coverage_gaps.iterrows()]
            axes[0, 1].set_yticklabels(labels, fontsize=8)
            axes[0, 1].set_xlabel('Avg Rating')
            axes[0, 1].set_title('High-Quality Genres to Explore (<30% coverage)', fontweight='bold')
            axes[0, 1].invert_yaxis()

        # 3. Top missing films across all genres
        all_missing = []
        for _, row in self.completeness_df.iterrows():
            for film in row['missing_films'][:5]:  # Top 5 per genre
                all_missing.append({
                    'title': film['title'],
                    'genre': row['genre'],
                    'rating': film['rating'],
                    'year': film['year'],
                    'votes': film['votes']
                })

        if all_missing:
            # Sort by rating and votes
            missing_df = pd.DataFrame(all_missing)
            missing_df = missing_df.sort_values(['rating', 'votes'], ascending=[False, False]).head(20)

            axes[1, 0].barh(range(len(missing_df)), missing_df['rating'], color='coral')
            axes[1, 0].set_yticks(range(len(missing_df)))
            labels = [f"{row['title'][:30]} ({row['year']}) - {row['genre']}"
                     for _, row in missing_df.iterrows()]
            axes[1, 0].set_yticklabels(labels, fontsize=7)
            axes[1, 0].set_xlabel('IMDB Rating')
            axes[1, 0].set_title('Top 20 Missing Films (All Genres)', fontweight='bold')
            axes[1, 0].invert_yaxis()
            axes[1, 0].axvline(x=8.0, color='red', linestyle='--', alpha=0.5)

        # 4. Completeness heatmap (top genres)
        top_10_genres = self.completeness_df.nlargest(10, 'on_watchlist')

        heatmap_data = []
        for _, row in top_10_genres.iterrows():
            heatmap_data.append([
                row['on_watchlist'],
                row['missing_count'],
                row['completeness_pct']
            ])

        im = axes[1, 1].imshow(heatmap_data, cmap='RdYlGn', aspect='auto')
        axes[1, 1].set_xticks([0, 1, 2])
        axes[1, 1].set_xticklabels(['On Watchlist', 'Missing', 'Complete %'], fontsize=9)
        axes[1, 1].set_yticks(range(len(top_10_genres)))
        axes[1, 1].set_yticklabels(top_10_genres['genre'], fontsize=9)
        axes[1, 1].set_title('Top 10 Genres: Collection Status', fontweight='bold')

        # Annotate cells
        for i in range(len(top_10_genres)):
            for j in range(3):
                text = axes[1, 1].text(j, i, f'{heatmap_data[i][j]:.0f}',
                                      ha="center", va="center", color="black", fontsize=8)

        plt.colorbar(im, ax=axes[1, 1])

        plt.tight_layout()
        plt.savefig(self.output_dir / 'missing_analysis.png', dpi=300, bbox_inches='tight')
        print(f"Saved: missing_analysis.png")
        plt.close()

    def visualize_recommendations(self):
        """Visualization 9-12: Genre-based recommendations."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Genre Collection Recommendations', fontsize=16, fontweight='bold')

        # 1. Priority genres (high quality, low coverage)
        priority = self.completeness_df[
            (self.completeness_df['completeness_pct'] < 40) &
            (self.completeness_df['total_avg_rating'] >= 7.0)
        ].nlargest(15, 'total_avg_rating')

        if len(priority) > 0:
            axes[0, 0].barh(range(len(priority)), priority['total_avg_rating'],
                          color='purple', alpha=0.6)
            axes[0, 0].set_yticks(range(len(priority)))
            labels = [f"{row['genre']} ({row['completeness_pct']:.0f}%)"
                     for _, row in priority.iterrows()]
            axes[0, 0].set_yticklabels(labels, fontsize=9)
            axes[0, 0].set_xlabel('Average Rating')
            axes[0, 0].set_title('Priority Genres to Explore', fontweight='bold')
            axes[0, 0].invert_yaxis()

        # 2. Well-covered genres
        well_covered = self.completeness_df[
            self.completeness_df['completeness_pct'] >= 40
        ].nlargest(15, 'completeness_pct')

        axes[0, 1].barh(range(len(well_covered)), well_covered['completeness_pct'],
                       color='green', alpha=0.6)
        axes[0, 1].set_yticks(range(len(well_covered)))
        labels = [f"{row['genre']} ({row['on_watchlist']} films)"
                 for _, row in well_covered.iterrows()]
        axes[0, 1].set_yticklabels(labels, fontsize=9)
        axes[0, 1].set_xlabel('Completeness %')
        axes[0, 1].set_title('Well-Covered Genres (40%+)', fontweight='bold')
        axes[0, 1].invert_yaxis()

        # 3. Genre diversity score
        # Calculate how balanced the collection is
        total_films = self.completeness_df['on_watchlist'].sum()
        genre_percentages = (self.completeness_df['on_watchlist'] / total_films * 100).sort_values(ascending=False)

        top_15_genres = genre_percentages.head(15)

        axes[1, 0].bar(range(len(top_15_genres)), top_15_genres.values,
                      color=plt.cm.tab20(np.arange(len(top_15_genres))))
        axes[1, 0].set_xticks(range(len(top_15_genres)))
        axes[1, 0].set_xticklabels(top_15_genres.index, rotation=45, ha='right', fontsize=8)
        axes[1, 0].set_ylabel('% of Collection')
        axes[1, 0].set_title('Collection Distribution Across Genres', fontweight='bold')
        axes[1, 0].grid(True, alpha=0.3, axis='y')

        # 4. Recommended films table
        axes[1, 1].axis('off')

        recommendations = []
        for _, row in self.completeness_df.nlargest(5, 'total_avg_rating').iterrows():
            if row['missing_films']:
                top_film = row['missing_films'][0]
                recommendations.append([
                    row['genre'],
                    top_film['title'][:25],
                    f"{top_film['rating']:.1f}"
                ])

        if recommendations:
            table = axes[1, 1].table(cellText=recommendations,
                                    colLabels=['Genre', 'Top Missing Film', 'Rating'],
                                    cellLoc='left',
                                    loc='center',
                                    colWidths=[0.3, 0.5, 0.2])
            table.auto_set_font_size(False)
            table.set_fontsize(8)
            table.scale(1, 2)

            for i in range(3):
                table[(0, i)].set_facecolor('#4CAF50')
                table[(0, i)].set_text_props(weight='bold', color='white')

            axes[1, 1].set_title('Top Recommended Films by Genre', fontweight='bold', pad=20)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'recommendations.png', dpi=300, bbox_inches='tight')
        print(f"Saved: recommendations.png")
        plt.close()

    def generate_report(self):
        """Generate comprehensive report."""
        report_path = self.report_dir / 'batch_39_genre_completeness_report.txt'

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("GENRE COLLECTION COMPLETENESS ANALYSIS\n")
            f.write("(Watchlist vs Full IMDB Data)\n")
            f.write("=" * 80 + "\n\n")

            # Stats
            f.write("OVERALL STATISTICS\n")
            f.write("-" * 80 + "\n")
            f.write(f"Total Genres Analyzed: {len(self.completeness_df)}\n")
            f.write(f"Average Completeness: {self.completeness_df['completeness_pct'].mean():.1f}%\n")
            f.write(f"Median Completeness: {self.completeness_df['completeness_pct'].median():.1f}%\n\n")

            # Most collected genres
            f.write("\nMOST COLLECTED GENRES\n")
            f.write("-" * 80 + "\n")
            top_collected = self.completeness_df.nlargest(20, 'on_watchlist')

            for idx, (_, row) in enumerate(top_collected.iterrows(), 1):
                f.write(f"{idx}. {row['genre']}\n")
                f.write(f"   On Watchlist: {row['on_watchlist']} / {row['total_quality_films']} ")
                f.write(f"({row['completeness_pct']:.1f}%)\n")
                f.write(f"   Avg Rating (Watchlist): {row['watchlist_avg_rating']:.2f}\n")
                f.write(f"   Avg Rating (All Quality): {row['total_avg_rating']:.2f}\n\n")

            # Priority genres to explore
            f.write("\nPRIORITY GENRES TO EXPLORE (<40% coverage, high quality)\n")
            f.write("-" * 80 + "\n")
            priority = self.completeness_df[
                (self.completeness_df['completeness_pct'] < 40) &
                (self.completeness_df['total_avg_rating'] >= 7.0)
            ].nlargest(15, 'total_avg_rating')

            for idx, (_, row) in enumerate(priority.iterrows(), 1):
                f.write(f"{idx}. {row['genre']} - {row['total_avg_rating']:.2f} avg rating\n")
                f.write(f"   Coverage: {row['on_watchlist']}/{row['total_quality_films']} ")
                f.write(f"({row['completeness_pct']:.1f}%)\n")
                f.write(f"   Top Missing Films:\n")
                for film in row['missing_films'][:5]:
                    f.write(f"   - {film['title']} ({film['year']}) - {film['rating']:.1f}\n")
                f.write("\n")

            # Top missing films
            f.write("\nTOP MISSING FILMS (Highest Rated, All Genres)\n")
            f.write("-" * 80 + "\n")
            all_missing = []
            for _, row in self.completeness_df.iterrows():
                for film in row['missing_films'][:3]:
                    all_missing.append({
                        'title': film['title'],
                        'genre': row['genre'],
                        'rating': film['rating'],
                        'year': film['year'],
                        'votes': film['votes']
                    })

            missing_df = pd.DataFrame(all_missing)
            missing_df = missing_df.sort_values(['rating', 'votes'], ascending=[False, False]).head(30)

            for idx, (_, row) in enumerate(missing_df.iterrows(), 1):
                f.write(f"{idx}. {row['title']} ({row['year']}) - {row['rating']:.1f}\n")
                f.write(f"   Genre: {row['genre']}\n\n")

        print(f"Report saved: {report_path}")

    def run_all_analyses(self):
        """Execute all analyses."""
        print("\n" + "=" * 80)
        print("BATCH 39: GENRE COLLECTION COMPLETENESS ANALYSIS")
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
        print(f"Report: {self.report_dir / 'batch_39_genre_completeness_report.txt'}")

if __name__ == "__main__":
    analyzer = GenreCompletenessAnalyzer()
    analyzer.run_all_analyses()
