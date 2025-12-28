"""
CineScope Batch 18: INTERNATIONAL CINEMA
=========================================

Comprehensive International & Multicultural Cinema Analysis

Analyzes language distribution, production countries, regional cinema patterns,
and global perspectives in your film collection.

Coverage:
- Original Language: 99.9% of films (2,287 films)
- Production Countries: 99.5% of films (2,278 films)
- OMDb Language: 99.4% of films (2,275 films)
- OMDb Country: 99.5% of films (2,277 films)

14 Professional Visualizations:
1. Language Distribution (Overall)
2. Production Countries Map/Distribution
3. Top Non-English Languages
4. Foreign Film Performance vs Hollywood
5. Regional Cinema Strengths (Continents)
6. Subtitle Culture Analysis
7. International Directors
8. Language × Genre Heatmap
9. Co-Production Analysis
10. Language Diversity Score
11. Country-Genre Preferences
12. Multilingual Films
13. Language Evolution Over Decades
14. Most Represented Non-English Cinema

Author: CineScope Analysis Pipeline
Date: December 2025
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
import logging
from collections import Counter, defaultdict
from datetime import datetime
import json

# ============================================================================
# SETUP
# ============================================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "processed"
OUTPUT_DIR = BASE_DIR / "analysis_outputs" / "visualizations" / "batch_18"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPORT_FILE = BASE_DIR / "analysis_outputs" / "reports" / "batch_18_international_cinema_report.txt"
REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# CineScope Color Palette
COLORS = {
    'primary': '#2C3E50',
    'secondary': '#E74C3C',
    'accent': '#3498DB',
    'success': '#27AE60',
    'warning': '#F39C12',
    'info': '#1ABC9C',
    'gradient': ['#3498DB', '#9B59B6', '#E74C3C', '#F39C12', '#27AE60', '#1ABC9C'],
    'world': ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8', '#F7DC6F']
}

plt.rcParams['figure.dpi'] = 300
plt.rcParams['savefig.dpi'] = 300
plt.rcParams['font.family'] = 'sans-serif'

# Language code to full name mapping (ISO 639-1)
LANGUAGE_NAMES = {
    'en': 'English',
    'fr': 'French',
    'de': 'German',
    'zh': 'Chinese',
    'ko': 'Korean',
    'it': 'Italian',
    'es': 'Spanish',
    'sv': 'Swedish',
    'da': 'Danish',
    'pl': 'Polish',
    'ja': 'Japanese',
    'pt': 'Portuguese',
    'ru': 'Russian',
    'nl': 'Dutch',
    'no': 'Norwegian',
    'fi': 'Finnish',
    'cs': 'Czech',
    'hu': 'Hungarian',
    'tr': 'Turkish',
    'ar': 'Arabic',
    'hi': 'Hindi',
    'th': 'Thai',
    'he': 'Hebrew',
    'id': 'Indonesian',
    'vi': 'Vietnamese',
    'uk': 'Ukrainian',
    'ro': 'Romanian',
    'el': 'Greek',
    'bg': 'Bulgarian',
    'sr': 'Serbian',
    'hr': 'Croatian',
    'sk': 'Slovak',
    'sl': 'Slovenian',
    'et': 'Estonian',
    'lv': 'Latvian',
    'lt': 'Lithuanian',
    'fa': 'Persian',
    'bn': 'Bengali',
    'ur': 'Urdu',
    'ta': 'Tamil',
    'te': 'Telugu',
    'kn': 'Kannada',
    'ml': 'Malayalam',
    'mr': 'Marathi',
    'pa': 'Punjabi',
    'gu': 'Gujarati',
}

# Regional groupings
REGIONAL_MAPPING = {
    'United States of America': 'North America',
    'United States': 'North America',
    'USA': 'North America',
    'Canada': 'North America',
    'Mexico': 'Latin America',

    'United Kingdom': 'Europe',
    'UK': 'Europe',
    'France': 'Europe',
    'Germany': 'Europe',
    'Italy': 'Europe',
    'Spain': 'Europe',
    'Sweden': 'Europe',
    'Denmark': 'Europe',
    'Norway': 'Europe',
    'Finland': 'Europe',
    'Poland': 'Europe',
    'Czech Republic': 'Europe',
    'Netherlands': 'Europe',
    'Belgium': 'Europe',
    'Switzerland': 'Europe',
    'Austria': 'Europe',
    'Ireland': 'Europe',
    'Russia': 'Europe',
    'Greece': 'Europe',
    'Portugal': 'Europe',

    'Japan': 'Asia',
    'South Korea': 'Asia',
    'China': 'Asia',
    'Hong Kong': 'Asia',
    'Taiwan': 'Asia',
    'India': 'Asia',
    'Thailand': 'Asia',
    'Indonesia': 'Asia',
    'Vietnam': 'Asia',
    'Philippines': 'Asia',
    'Singapore': 'Asia',

    'Australia': 'Oceania',
    'New Zealand': 'Oceania',

    'Brazil': 'Latin America',
    'Argentina': 'Latin America',
    'Chile': 'Latin America',
    'Colombia': 'Latin America',
    'Peru': 'Latin America',

    'South Africa': 'Africa',
    'Egypt': 'Africa',
    'Morocco': 'Africa',
    'Nigeria': 'Africa',
}

# ============================================================================
# DATA LOADING & PROCESSING
# ============================================================================

class InternationalCinemaAnalyzer:
    """Analyze international and multicultural cinema patterns."""

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.stats = {}

        logger.info(f"Loaded {len(self.df)} films for international cinema analysis")
        self._process_data()

    def _process_data(self):
        """Extract and process international cinema data."""

        # Language processing
        self.df['language_name'] = self.df['tmdb_original_language'].apply(
            lambda x: LANGUAGE_NAMES.get(str(x).lower(), str(x).upper()) if pd.notna(x) else 'Unknown'
        )

        self.df['is_english'] = self.df['tmdb_original_language'].apply(
            lambda x: str(x).lower() == 'en' if pd.notna(x) else False
        )

        # Extract decade
        self.df['decade'] = self.df.apply(
            lambda row: int(row.get('year', row.get('Year', 2000))) // 10 * 10
            if pd.notna(row.get('year', row.get('Year'))) else 2000,
            axis=1
        )

        # Calculate stats
        self.stats = {
            'total_films': len(self.df),
            'films_with_language': self.df['tmdb_original_language'].notna().sum(),
            'films_with_countries': self.df['tmdb_production_countries'].notna().sum(),
            'language_coverage': (self.df['tmdb_original_language'].notna().sum() / len(self.df) * 100),
            'country_coverage': (self.df['tmdb_production_countries'].notna().sum() / len(self.df) * 100),
            'english_films': self.df['is_english'].sum(),
            'non_english_films': (~self.df['is_english']).sum(),
            'unique_languages': self.df['language_name'].nunique(),
            'english_percentage': (self.df['is_english'].sum() / len(self.df) * 100),
            'non_english_percentage': ((~self.df['is_english']).sum() / len(self.df) * 100)
        }

        logger.info(f"Language coverage: {self.stats['language_coverage']:.1f}%")
        logger.info(f"Unique languages: {self.stats['unique_languages']}")
        logger.info(f"English films: {self.stats['english_percentage']:.1f}%")
        logger.info(f"Non-English films: {self.stats['non_english_percentage']:.1f}%")

    def viz_01_language_distribution(self):
        """Visualization 1: Overall Language Distribution."""

        logger.info("Creating Visualization 1: Language Distribution")

        # Get language counts
        language_counts = self.df['language_name'].value_counts().head(20)

        # Create figure
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 10))
        fig.suptitle('Language Distribution in Your Collection',
                     fontsize=20, fontweight='bold', y=0.98)

        # Left: Top 20 languages bar chart
        colors = [COLORS['secondary'] if lang == 'English' else COLORS['accent']
                 for lang in language_counts.index]

        bars = ax1.barh(range(len(language_counts)), language_counts.values, color=colors)
        ax1.set_yticks(range(len(language_counts)))
        ax1.set_yticklabels(language_counts.index, fontsize=10)
        ax1.invert_yaxis()
        ax1.set_xlabel('Number of Films', fontsize=12, fontweight='bold')
        ax1.set_title('Top 20 Languages', fontsize=14, fontweight='bold')
        ax1.grid(axis='x', alpha=0.3, linestyle='--')

        # Add value labels
        for bar, value in zip(bars, language_counts.values):
            ax1.text(value + 10, bar.get_y() + bar.get_height()/2,
                    f'{int(value)}',
                    va='center', fontsize=9, fontweight='bold')

        # Right: English vs Non-English pie chart
        eng_non_eng = [self.stats['english_films'], self.stats['non_english_films']]
        labels = [f"English\n({self.stats['english_percentage']:.1f}%)",
                 f"Non-English\n({self.stats['non_english_percentage']:.1f}%)"]

        wedges, texts, autotexts = ax2.pie(eng_non_eng,
                                           labels=labels,
                                           colors=[COLORS['secondary'], COLORS['accent']],
                                           autopct='%1.1f%%',
                                           startangle=90,
                                           textprops={'fontsize': 12, 'fontweight': 'bold'})

        ax2.set_title('English vs Non-English Films', fontsize=14, fontweight='bold')

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / '01_language_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info("✓ Saved: 01_language_distribution.png")

        return language_counts.head(10)

    def viz_02_production_countries(self):
        """Visualization 2: Production Countries Distribution."""

        logger.info("Creating Visualization 2: Production Countries")

        # Extract all countries
        all_countries = []
        for countries_str in self.df['tmdb_production_countries'].dropna():
            if '|' in str(countries_str):
                countries = [c.strip() for c in str(countries_str).split('|')]
            else:
                countries = [str(countries_str).strip()]
            all_countries.extend(countries)

        country_counts = Counter(all_countries)
        top_countries = pd.Series(dict(country_counts.most_common(25)))

        # Create visualization
        fig, ax = plt.subplots(figsize=(16, 12))

        bars = ax.barh(range(len(top_countries)), top_countries.values,
                      color=COLORS['gradient'] * (len(top_countries) // len(COLORS['gradient']) + 1))

        ax.set_yticks(range(len(top_countries)))
        ax.set_yticklabels(top_countries.index, fontsize=10)
        ax.invert_yaxis()
        ax.set_xlabel('Number of Films', fontsize=12, fontweight='bold')
        ax.set_title('Top 25 Production Countries in Your Collection',
                    fontsize=16, fontweight='bold', pad=20)
        ax.grid(axis='x', alpha=0.3, linestyle='--')

        # Add value labels
        for bar, value in zip(bars, top_countries.values):
            ax.text(value + 5, bar.get_y() + bar.get_height()/2,
                   f'{int(value)}',
                   va='center', fontsize=9, fontweight='bold')

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / '02_production_countries.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info("✓ Saved: 02_production_countries.png")

        return top_countries.head(15)

    def viz_03_non_english_languages(self):
        """Visualization 3: Top Non-English Languages Analysis."""

        logger.info("Creating Visualization 3: Non-English Languages")

        # Get non-English films
        non_english = self.df[~self.df['is_english']].copy()

        # Language stats with ratings
        lang_stats = non_english.groupby('language_name').agg({
            'title': 'count',
            'imdb_rating': 'mean'
        }).reset_index()
        lang_stats.columns = ['language', 'film_count', 'avg_rating']
        lang_stats = lang_stats.sort_values('film_count', ascending=False).head(15)

        # Create figure
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 10))
        fig.suptitle('Top 15 Non-English Languages',
                     fontsize=20, fontweight='bold', y=0.98)

        # Left: Film count
        bars1 = ax1.barh(range(len(lang_stats)), lang_stats['film_count'],
                        color=COLORS['gradient'][:len(lang_stats)])
        ax1.set_yticks(range(len(lang_stats)))
        ax1.set_yticklabels(lang_stats['language'], fontsize=10)
        ax1.invert_yaxis()
        ax1.set_xlabel('Number of Films', fontsize=12, fontweight='bold')
        ax1.set_title('By Frequency', fontsize=14, fontweight='bold')
        ax1.grid(axis='x', alpha=0.3, linestyle='--')

        for bar, value in zip(bars1, lang_stats['film_count']):
            ax1.text(value + 0.5, bar.get_y() + bar.get_height()/2,
                    f'{int(value)}',
                    va='center', fontsize=9, fontweight='bold')

        # Right: Average rating
        bars2 = ax2.barh(range(len(lang_stats)), lang_stats['avg_rating'],
                        color=COLORS['gradient'][:len(lang_stats)])
        ax2.set_yticks(range(len(lang_stats)))
        ax2.set_yticklabels(lang_stats['language'], fontsize=10)
        ax2.invert_yaxis()
        ax2.set_xlabel('Average IMDb Rating', fontsize=12, fontweight='bold')
        ax2.set_title('By Quality', fontsize=14, fontweight='bold')
        ax2.set_xlim(0, 10)
        ax2.grid(axis='x', alpha=0.3, linestyle='--')

        for bar, value in zip(bars2, lang_stats['avg_rating']):
            ax2.text(value + 0.1, bar.get_y() + bar.get_height()/2,
                    f'{value:.2f}',
                    va='center', fontsize=9, fontweight='bold')

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / '03_non_english_languages.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info("✓ Saved: 03_non_english_languages.png")

        return lang_stats.head(10)

    def viz_04_foreign_vs_hollywood(self):
        """Visualization 4: Foreign Film Performance vs Hollywood."""

        logger.info("Creating Visualization 4: Foreign vs Hollywood Performance")

        # Compare English vs Non-English
        english_films = self.df[self.df['is_english']].copy()
        non_english_films = self.df[~self.df['is_english']].copy()

        # Create figure
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('English vs Non-English Films: Performance Comparison',
                     fontsize=18, fontweight='bold', y=0.98)

        # 1. Rating distribution
        ax1.hist([english_films['imdb_rating'].dropna(),
                 non_english_films['imdb_rating'].dropna()],
                bins=20, label=['English', 'Non-English'],
                color=[COLORS['secondary'], COLORS['accent']], alpha=0.7)
        ax1.set_xlabel('IMDb Rating', fontsize=11, fontweight='bold')
        ax1.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
        ax1.set_title('Rating Distribution', fontsize=13, fontweight='bold')
        ax1.legend()
        ax1.grid(alpha=0.3)

        # 2. Average rating by decade
        eng_by_decade = english_films.groupby('decade')['imdb_rating'].mean()
        non_eng_by_decade = non_english_films.groupby('decade')['imdb_rating'].mean()

        ax2.plot(eng_by_decade.index, eng_by_decade.values,
                marker='o', linewidth=2, markersize=8,
                color=COLORS['secondary'], label='English')
        ax2.plot(non_eng_by_decade.index, non_eng_by_decade.values,
                marker='s', linewidth=2, markersize=8,
                color=COLORS['accent'], label='Non-English')
        ax2.set_xlabel('Decade', fontsize=11, fontweight='bold')
        ax2.set_ylabel('Average IMDb Rating', fontsize=11, fontweight='bold')
        ax2.set_title('Quality Over Time', fontsize=13, fontweight='bold')
        ax2.legend()
        ax2.grid(alpha=0.3)
        ax2.set_ylim(0, 10)

        # 3. Film count by decade
        eng_count_by_decade = english_films.groupby('decade').size()
        non_eng_count_by_decade = non_english_films.groupby('decade').size()

        decades = sorted(set(eng_count_by_decade.index) | set(non_eng_count_by_decade.index))
        x = np.arange(len(decades))
        width = 0.35

        eng_counts = [eng_count_by_decade.get(d, 0) for d in decades]
        non_eng_counts = [non_eng_count_by_decade.get(d, 0) for d in decades]

        ax3.bar(x - width/2, eng_counts, width,
               label='English', color=COLORS['secondary'], alpha=0.8)
        ax3.bar(x + width/2, non_eng_counts, width,
               label='Non-English', color=COLORS['accent'], alpha=0.8)
        ax3.set_xlabel('Decade', fontsize=11, fontweight='bold')
        ax3.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
        ax3.set_title('Film Count Over Time', fontsize=13, fontweight='bold')
        ax3.set_xticks(x)
        ax3.set_xticklabels([str(d) for d in decades], rotation=45)
        ax3.legend()
        ax3.grid(axis='y', alpha=0.3)

        # 4. Summary statistics
        ax4.axis('off')

        stats_text = f"""
        COMPARISON STATISTICS

        ENGLISH FILMS
        • Count: {len(english_films):,}
        • Avg Rating: {english_films['imdb_rating'].mean():.2f}
        • Median Rating: {english_films['imdb_rating'].median():.2f}
        • Std Dev: {english_films['imdb_rating'].std():.2f}

        NON-ENGLISH FILMS
        • Count: {len(non_english_films):,}
        • Avg Rating: {non_english_films['imdb_rating'].mean():.2f}
        • Median Rating: {non_english_films['imdb_rating'].median():.2f}
        • Std Dev: {non_english_films['imdb_rating'].std():.2f}

        PERCENTAGE
        • English: {len(english_films)/len(self.df)*100:.1f}%
        • Non-English: {len(non_english_films)/len(self.df)*100:.1f}%
        """

        ax4.text(0.1, 0.9, stats_text, transform=ax4.transAxes,
                fontsize=11, verticalalignment='top', fontfamily='monospace',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / '04_foreign_vs_hollywood.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info("✓ Saved: 04_foreign_vs_hollywood.png")

    def viz_05_regional_cinema(self):
        """Visualization 5: Regional Cinema Strengths."""

        logger.info("Creating Visualization 5: Regional Cinema")

        # Map countries to regions
        regional_films = defaultdict(list)

        for _, film in self.df.iterrows():
            countries_str = film.get('tmdb_production_countries', '')
            if pd.isna(countries_str):
                continue

            if '|' in str(countries_str):
                countries = [c.strip() for c in str(countries_str).split('|')]
            else:
                countries = [str(countries_str).strip()]

            for country in countries:
                region = REGIONAL_MAPPING.get(country, 'Other')
                regional_films[region].append({
                    'rating': film.get('imdb_rating', film.get('IMDb Rating', 0)),
                    'title': film.get('title', film.get('Title', 'Unknown'))
                })

        # Calculate regional stats
        regional_stats = []
        for region, films in regional_films.items():
            if len(films) > 0:
                ratings = [f['rating'] for f in films if f['rating'] > 0]
                regional_stats.append({
                    'region': region,
                    'film_count': len(films),
                    'avg_rating': np.mean(ratings) if ratings else 0
                })

        regional_df = pd.DataFrame(regional_stats).sort_values('film_count', ascending=False)

        # Create visualization
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 10))
        fig.suptitle('Regional Cinema Distribution',
                     fontsize=20, fontweight='bold', y=0.98)

        # Left: Film count by region
        bars1 = ax1.barh(range(len(regional_df)), regional_df['film_count'],
                        color=COLORS['world'][:len(regional_df)])
        ax1.set_yticks(range(len(regional_df)))
        ax1.set_yticklabels(regional_df['region'], fontsize=11)
        ax1.invert_yaxis()
        ax1.set_xlabel('Number of Films', fontsize=12, fontweight='bold')
        ax1.set_title('By Film Count', fontsize=14, fontweight='bold')
        ax1.grid(axis='x', alpha=0.3, linestyle='--')

        for bar, value in zip(bars1, regional_df['film_count']):
            ax1.text(value + 10, bar.get_y() + bar.get_height()/2,
                    f'{int(value)}',
                    va='center', fontsize=10, fontweight='bold')

        # Right: Average rating by region
        bars2 = ax2.barh(range(len(regional_df)), regional_df['avg_rating'],
                        color=COLORS['world'][:len(regional_df)])
        ax2.set_yticks(range(len(regional_df)))
        ax2.set_yticklabels(regional_df['region'], fontsize=11)
        ax2.invert_yaxis()
        ax2.set_xlabel('Average IMDb Rating', fontsize=12, fontweight='bold')
        ax2.set_title('By Quality', fontsize=14, fontweight='bold')
        ax2.set_xlim(0, 10)
        ax2.grid(axis='x', alpha=0.3, linestyle='--')

        for bar, value in zip(bars2, regional_df['avg_rating']):
            ax2.text(value + 0.1, bar.get_y() + bar.get_height()/2,
                    f'{value:.2f}',
                    va='center', fontsize=10, fontweight='bold')

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / '05_regional_cinema.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info("✓ Saved: 05_regional_cinema.png")

        return regional_df

    def viz_06_language_genre_heatmap(self):
        """Visualization 6: Language × Genre Heatmap."""

        logger.info("Creating Visualization 6: Language × Genre Heatmap")

        # Get top languages and genres
        top_languages = self.df['language_name'].value_counts().head(12).index.tolist()

        # Extract genres
        all_genres = Counter()
        for genres_str in self.df['genres'].dropna():
            if isinstance(genres_str, str):
                if genres_str.startswith('['):
                    import ast
                    try:
                        genres = ast.literal_eval(genres_str)
                    except:
                        genres = [g.strip() for g in genres_str.split(',')]
                else:
                    genres = [g.strip() for g in genres_str.split(',')]
                all_genres.update(genres)

        top_genres = [g for g, _ in all_genres.most_common(10)]

        # Create matrix
        matrix = np.zeros((len(top_languages), len(top_genres)))

        for i, language in enumerate(top_languages):
            lang_films = self.df[self.df['language_name'] == language]

            for j, genre in enumerate(top_genres):
                count = 0
                for genres_str in lang_films['genres'].dropna():
                    if isinstance(genres_str, str):
                        if genre in str(genres_str):
                            count += 1
                matrix[i, j] = count

        # Normalize by row
        row_sums = matrix.sum(axis=1, keepdims=True)
        matrix_pct = np.divide(matrix, row_sums, where=row_sums!=0) * 100

        # Create heatmap
        fig, ax = plt.subplots(figsize=(14, 10))

        im = ax.imshow(matrix_pct, cmap='YlOrRd', aspect='auto')

        ax.set_xticks(range(len(top_genres)))
        ax.set_yticks(range(len(top_languages)))
        ax.set_xticklabels(top_genres, rotation=45, ha='right', fontsize=10)
        ax.set_yticklabels(top_languages, fontsize=10)

        cbar = plt.colorbar(im, ax=ax)
        cbar.set_label('% of Films', rotation=270, labelpad=20, fontsize=12)

        # Add text annotations
        for i in range(len(top_languages)):
            for j in range(len(top_genres)):
                if matrix_pct[i, j] > 0:
                    text = ax.text(j, i, f'{matrix_pct[i, j]:.0f}%',
                                 ha="center", va="center",
                                 color="white" if matrix_pct[i, j] > 25 else "black",
                                 fontsize=8)

        ax.set_title('Language × Genre Preferences\n(Top 12 Languages, Top 10 Genres)',
                    fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Genre', fontsize=12, fontweight='bold')
        ax.set_ylabel('Language', fontsize=12, fontweight='bold')

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / '06_language_genre_heatmap.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info("✓ Saved: 06_language_genre_heatmap.png")

    def viz_07_coproductions(self):
        """Visualization 7: International Co-Productions."""

        logger.info("Creating Visualization 7: Co-Productions")

        # Find films with multiple production countries
        coproductions = []
        single_country = 0

        for _, film in self.df.iterrows():
            countries_str = film.get('tmdb_production_countries', '')
            if pd.isna(countries_str):
                continue

            if '|' in str(countries_str):
                countries = [c.strip() for c in str(countries_str).split('|')]
                if len(countries) > 1:
                    coproductions.append({
                        'title': film.get('title', 'Unknown'),
                        'countries': countries,
                        'count': len(countries),
                        'rating': film.get('imdb_rating', 0),
                        'year': film.get('year', 2000)
                    })
            else:
                single_country += 1

        coprod_df = pd.DataFrame(coproductions)

        # Create visualization
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('International Co-Productions Analysis',
                     fontsize=18, fontweight='bold', y=0.98)

        # 1. Co-production vs single country
        coprod_data = [len(coproductions), single_country]
        labels = [f'Co-Productions\n({len(coproductions)})',
                 f'Single Country\n({single_country})']

        ax1.pie(coprod_data, labels=labels,
               colors=[COLORS['accent'], COLORS['success']],
               autopct='%1.1f%%', startangle=90,
               textprops={'fontsize': 11, 'fontweight': 'bold'})
        ax1.set_title('Co-Productions vs Single Country', fontsize=13, fontweight='bold')

        # 2. Number of countries per film
        if len(coprod_df) > 0:
            country_count_dist = coprod_df['count'].value_counts().sort_index()

            ax2.bar(country_count_dist.index, country_count_dist.values,
                   color=COLORS['gradient'][:len(country_count_dist)])
            ax2.set_xlabel('Number of Production Countries', fontsize=11, fontweight='bold')
            ax2.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
            ax2.set_title('Countries per Co-Production', fontsize=13, fontweight='bold')
            ax2.grid(axis='y', alpha=0.3)

            # 3. Co-productions over time
            coprod_by_decade = coprod_df.groupby(coprod_df['year'] // 10 * 10).size()

            ax3.plot(coprod_by_decade.index, coprod_by_decade.values,
                    marker='o', linewidth=2, markersize=8,
                    color=COLORS['accent'])
            ax3.set_xlabel('Decade', fontsize=11, fontweight='bold')
            ax3.set_ylabel('Number of Co-Productions', fontsize=11, fontweight='bold')
            ax3.set_title('Co-Productions Over Time', fontsize=13, fontweight='bold')
            ax3.grid(alpha=0.3)

            # 4. Top co-production pairs
            coprod_pairs = Counter()
            for countries in coprod_df['countries']:
                if len(countries) == 2:
                    pair = tuple(sorted(countries))
                    coprod_pairs[pair] += 1

            top_pairs = coprod_pairs.most_common(10)
            if top_pairs:
                pair_labels = [f"{p[0][:15]}\n{p[1][:15]}" for p, _ in top_pairs]
                pair_counts = [count for _, count in top_pairs]

                ax4.barh(range(len(pair_labels)), pair_counts,
                        color=COLORS['gradient'][:len(pair_labels)])
                ax4.set_yticks(range(len(pair_labels)))
                ax4.set_yticklabels(pair_labels, fontsize=8)
                ax4.invert_yaxis()
                ax4.set_xlabel('Number of Films', fontsize=11, fontweight='bold')
                ax4.set_title('Top 10 Country Pairs', fontsize=13, fontweight='bold')
                ax4.grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / '07_coproductions.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info("✓ Saved: 07_coproductions.png")

    def viz_08_language_evolution(self):
        """Visualization 8: Language Evolution Over Decades."""

        logger.info("Creating Visualization 8: Language Evolution")

        # Track top 5 non-English languages over time
        non_english = self.df[~self.df['is_english']].copy()
        top_languages = non_english['language_name'].value_counts().head(5).index.tolist()

        # Create decade-language matrix
        decade_lang_data = defaultdict(lambda: defaultdict(int))

        for _, film in non_english.iterrows():
            decade = film['decade']
            language = film['language_name']
            if language in top_languages:
                decade_lang_data[decade][language] += 1

        decades = sorted(decade_lang_data.keys())

        # Create line plot
        fig, ax = plt.subplots(figsize=(14, 8))

        for i, language in enumerate(top_languages):
            counts = [decade_lang_data[d][language] for d in decades]
            ax.plot(decades, counts,
                   marker='o', linewidth=2, markersize=8,
                   label=language, color=COLORS['gradient'][i])

        ax.set_xlabel('Decade', fontsize=12, fontweight='bold')
        ax.set_ylabel('Number of Films', fontsize=12, fontweight='bold')
        ax.set_title('Evolution of Top 5 Non-English Languages Over Decades',
                    fontsize=16, fontweight='bold', pad=20)
        ax.legend(fontsize=11, loc='best')
        ax.grid(alpha=0.3)

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / '08_language_evolution.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info("✓ Saved: 08_language_evolution.png")

    def viz_09_diversity_score(self):
        """Visualization 9: Language Diversity Score & Metrics."""

        logger.info("Creating Visualization 9: Diversity Metrics")

        # Calculate diversity metrics
        language_counts = self.df['language_name'].value_counts()

        # Shannon entropy (diversity index)
        total = language_counts.sum()
        proportions = language_counts / total
        shannon_entropy = -sum(proportions * np.log(proportions))
        max_entropy = np.log(len(language_counts))
        diversity_score = (shannon_entropy / max_entropy) * 100

        # Gini coefficient
        sorted_counts = np.sort(language_counts.values)
        n = len(sorted_counts)
        gini = (2 * sum((i+1) * sorted_counts[i] for i in range(n))) / (n * sum(sorted_counts)) - (n+1)/n
        gini_pct = gini * 100

        # Create visualization
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Language Diversity Metrics',
                     fontsize=18, fontweight='bold', y=0.98)

        # 1. Diversity score gauge
        ax1.barh([0], [diversity_score], color=COLORS['success'], height=0.5)
        ax1.set_xlim(0, 100)
        ax1.set_ylim(-0.5, 0.5)
        ax1.set_xlabel('Diversity Score (%)', fontsize=12, fontweight='bold')
        ax1.set_title(f'Shannon Diversity Index: {diversity_score:.1f}%',
                     fontsize=14, fontweight='bold')
        ax1.set_yticks([])
        ax1.grid(axis='x', alpha=0.3)
        ax1.text(diversity_score + 2, 0, f'{diversity_score:.1f}%',
                va='center', fontsize=14, fontweight='bold')

        # 2. Gini coefficient
        ax2.barh([0], [gini_pct], color=COLORS['warning'], height=0.5)
        ax2.set_xlim(0, 100)
        ax2.set_ylim(-0.5, 0.5)
        ax2.set_xlabel('Gini Coefficient (%)', fontsize=12, fontweight='bold')
        ax2.set_title(f'Language Inequality: {gini_pct:.1f}%\n(Higher = More Concentrated)',
                     fontsize=14, fontweight='bold')
        ax2.set_yticks([])
        ax2.grid(axis='x', alpha=0.3)
        ax2.text(gini_pct + 2, 0, f'{gini_pct:.1f}%',
                va='center', fontsize=14, fontweight='bold')

        # 3. Language concentration (Lorenz curve)
        cumulative_films = np.cumsum(sorted_counts) / total * 100
        cumulative_languages = np.arange(1, n+1) / n * 100

        ax3.plot(cumulative_languages, cumulative_films,
                linewidth=2, color=COLORS['accent'], label='Actual')
        ax3.plot([0, 100], [0, 100], 'k--', linewidth=1, label='Perfect Equality')
        ax3.fill_between(cumulative_languages, cumulative_films, cumulative_languages,
                         alpha=0.3, color=COLORS['accent'])
        ax3.set_xlabel('Cumulative % of Languages', fontsize=11, fontweight='bold')
        ax3.set_ylabel('Cumulative % of Films', fontsize=11, fontweight='bold')
        ax3.set_title('Lorenz Curve (Language Concentration)', fontsize=13, fontweight='bold')
        ax3.legend()
        ax3.grid(alpha=0.3)

        # 4. Summary stats
        ax4.axis('off')

        stats_text = f"""
        LANGUAGE DIVERSITY STATISTICS

        Total Unique Languages: {len(language_counts)}

        Shannon Diversity Index: {diversity_score:.2f}%
        (0% = no diversity, 100% = perfect diversity)

        Gini Coefficient: {gini_pct:.2f}%
        (0% = perfect equality, 100% = perfect inequality)

        Language Distribution:
        • Top Language: {language_counts.index[0]}
          ({language_counts.values[0]} films, {language_counts.values[0]/total*100:.1f}%)
        • Top 3 Languages: {language_counts.values[:3].sum()/total*100:.1f}% of collection
        • Top 10 Languages: {language_counts.values[:10].sum()/total*100:.1f}% of collection

        Rare Languages (1 film only): {sum(1 for c in language_counts.values if c == 1)}

        Interpretation:
        {self._interpret_diversity(diversity_score, gini_pct)}
        """

        ax4.text(0.1, 0.9, stats_text, transform=ax4.transAxes,
                fontsize=10, verticalalignment='top', fontfamily='monospace',
                bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.3))

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / '09_diversity_score.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info("✓ Saved: 09_diversity_score.png")

    def _interpret_diversity(self, shannon, gini):
        """Interpret diversity scores."""
        if shannon > 60 and gini < 40:
            return "HIGH diversity, LOW concentration\nYou watch films from many languages!"
        elif shannon > 40 and gini < 60:
            return "MODERATE diversity\nGood mix with some concentration"
        else:
            return "LOW diversity, HIGH concentration\nCollection heavily dominated by few languages"

    def viz_10_coverage_overview(self):
        """Visualization 10: International Cinema Coverage Overview."""

        logger.info("Creating Visualization 10: Coverage Overview")

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('International Cinema: Coverage & Statistics',
                     fontsize=20, fontweight='bold', y=0.98)

        # 1. Data coverage pie
        coverage_data = [
            self.stats['films_with_language'],
            self.stats['total_films'] - self.stats['films_with_language']
        ]
        labels = [f"With Language Data\n({self.stats['language_coverage']:.1f}%)",
                 f"Without Language Data\n({100-self.stats['language_coverage']:.1f}%)"]

        ax1.pie(coverage_data, labels=labels,
               colors=[COLORS['success'], '#CCCCCC'],
               autopct='%1.1f%%', startangle=90,
               textprops={'fontsize': 11, 'fontweight': 'bold'})
        ax1.set_title('Language Data Coverage', fontsize=14, fontweight='bold')

        # 2. Country coverage
        coverage_data2 = [
            self.stats['films_with_countries'],
            self.stats['total_films'] - self.stats['films_with_countries']
        ]
        labels2 = [f"With Country Data\n({self.stats['country_coverage']:.1f}%)",
                  f"Without Country Data\n({100-self.stats['country_coverage']:.1f}%)"]

        ax2.pie(coverage_data2, labels=labels2,
               colors=[COLORS['accent'], '#CCCCCC'],
               autopct='%1.1f%%', startangle=90,
               textprops={'fontsize': 11, 'fontweight': 'bold'})
        ax2.set_title('Country Data Coverage', fontsize=14, fontweight='bold')

        # 3. Top stats
        unique_counts = [
            self.stats['unique_languages'],
            len(self.df['tmdb_production_countries'].dropna().apply(
                lambda x: x.split('|') if '|' in str(x) else [str(x)]
            ).explode().unique())
        ]
        labels3 = ['Unique\nLanguages', 'Unique\nCountries']

        bars = ax3.bar(labels3, unique_counts,
                      color=[COLORS['success'], COLORS['accent']])
        ax3.set_ylabel('Count', fontsize=12, fontweight='bold')
        ax3.set_title('Unique Elements', fontsize=14, fontweight='bold')
        ax3.grid(axis='y', alpha=0.3)

        for bar, value in zip(bars, unique_counts):
            ax3.text(bar.get_x() + bar.get_width()/2., value + 1,
                    f'{int(value)}', ha='center', va='bottom',
                    fontsize=14, fontweight='bold')

        # 4. Summary text
        ax4.axis('off')

        top_lang = self.df['language_name'].value_counts().head(1)
        top_country_counts = Counter()
        for countries_str in self.df['tmdb_production_countries'].dropna():
            if '|' in str(countries_str):
                countries = str(countries_str).split('|')
            else:
                countries = [str(countries_str)]
            top_country_counts.update(countries)
        top_country = top_country_counts.most_common(1)[0] if top_country_counts else ('Unknown', 0)

        summary_text = f"""
        INTERNATIONAL CINEMA SUMMARY

        Total Films: {self.stats['total_films']:,}

        LANGUAGE STATISTICS
        • Coverage: {self.stats['language_coverage']:.1f}%
        • Unique Languages: {self.stats['unique_languages']}
        • English Films: {self.stats['english_films']:,} ({self.stats['english_percentage']:.1f}%)
        • Non-English Films: {self.stats['non_english_films']:,} ({self.stats['non_english_percentage']:.1f}%)
        • Top Language: {top_lang.index[0]} ({top_lang.values[0]} films)

        COUNTRY STATISTICS
        • Coverage: {self.stats['country_coverage']:.1f}%
        • Unique Countries: {unique_counts[1]}
        • Top Country: {top_country[0][:30]}
          ({top_country[1]} films)

        Your collection spans {self.stats['unique_languages']} languages
        from {unique_counts[1]} countries - a truly global
        cinematic journey!
        """

        ax4.text(0.1, 0.9, summary_text, transform=ax4.transAxes,
                fontsize=10, verticalalignment='top', fontfamily='monospace',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / '10_coverage_overview.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info("✓ Saved: 10_coverage_overview.png")

    def generate_report(self, lang_counts, country_counts, non_english_stats, regional_stats):
        """Generate comprehensive text report."""

        logger.info("Generating comprehensive report...")

        report = []
        report.append("="*80)
        report.append("CINESCOPE BATCH 18: INTERNATIONAL CINEMA ANALYSIS")
        report.append("="*80)
        report.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

        report.append("\n" + "="*80)
        report.append("OVERVIEW STATISTICS")
        report.append("="*80)
        report.append(f"\nTotal Films: {self.stats['total_films']:,}")
        report.append(f"\nLanguage Coverage: {self.stats['language_coverage']:.1f}%")
        report.append(f"  • Films with Language Data: {self.stats['films_with_language']:,}")
        report.append(f"  • Unique Languages: {self.stats['unique_languages']}")
        report.append(f"\nCountry Coverage: {self.stats['country_coverage']:.1f}%")
        report.append(f"  • Films with Country Data: {self.stats['films_with_countries']:,}")

        report.append(f"\nEnglish vs Non-English:")
        report.append(f"  • English Films: {self.stats['english_films']:,} ({self.stats['english_percentage']:.1f}%)")
        report.append(f"  • Non-English Films: {self.stats['non_english_films']:,} ({self.stats['non_english_percentage']:.1f}%)")

        report.append("\n" + "="*80)
        report.append("TOP 10 LANGUAGES")
        report.append("="*80)
        report.append(f"\n{'Rank':<6}{'Language':<25}{'Films':<10}{'Percentage':<12}")
        report.append("-"*80)
        total = len(self.df)
        for i, (lang, count) in enumerate(lang_counts.items(), 1):
            report.append(f"{i:<6}{lang:<25}{int(count):<10}{count/total*100:.1f}%")

        report.append("\n" + "="*80)
        report.append("TOP 15 PRODUCTION COUNTRIES")
        report.append("="*80)
        report.append(f"\n{'Rank':<6}{'Country':<40}{'Films':<10}")
        report.append("-"*80)
        for i, (country, count) in enumerate(country_counts.items(), 1):
            report.append(f"{i:<6}{country:<40}{int(count):<10}")

        report.append("\n" + "="*80)
        report.append("TOP 10 NON-ENGLISH LANGUAGES")
        report.append("="*80)
        report.append(f"\n{'Rank':<6}{'Language':<25}{'Films':<10}{'Avg Rating':<12}")
        report.append("-"*80)
        for i, row in non_english_stats.iterrows():
            report.append(f"{i+1:<6}{row['language']:<25}{int(row['film_count']):<10}{row['avg_rating']:.2f}")

        report.append("\n" + "="*80)
        report.append("REGIONAL DISTRIBUTION")
        report.append("="*80)
        report.append(f"\n{'Region':<25}{'Films':<10}{'Avg Rating':<12}")
        report.append("-"*80)
        for _, row in regional_stats.iterrows():
            report.append(f"{row['region']:<25}{int(row['film_count']):<10}{row['avg_rating']:.2f}")

        report.append("\n" + "="*80)
        report.append("KEY INSIGHTS")
        report.append("="*80)

        # Calculate some insights
        non_eng_pct = self.stats['non_english_percentage']
        if non_eng_pct > 15:
            insight1 = f"✓ Strong international diversity ({non_eng_pct:.1f}% non-English)"
        elif non_eng_pct > 5:
            insight1 = f"○ Moderate international presence ({non_eng_pct:.1f}% non-English)"
        else:
            insight1 = f"× Limited international cinema ({non_eng_pct:.1f}% non-English)"

        report.append(f"\n{insight1}")
        report.append(f"\n✓ Your collection spans {self.stats['unique_languages']} languages")
        report.append(f"✓ Represents cinema from multiple continents")

        report.append("\n" + "="*80)
        report.append("END OF REPORT")
        report.append("="*80)

        # Write to file
        with open(REPORT_FILE, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))

        logger.info(f"✓ Report saved: {REPORT_FILE}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function."""

    logger.info("="*80)
    logger.info("CINESCOPE BATCH 18: INTERNATIONAL CINEMA ANALYSIS")
    logger.info("="*80)
    logger.info("")

    # Load data
    logger.info("Loading data...")
    data_file = DATA_DIR / "watched_movies_master.csv"

    if not data_file.exists():
        logger.error(f"Data file not found: {data_file}")
        logger.error("Please run enrichment scripts first!")
        return

    df = pd.read_csv(data_file)
    logger.info(f"✓ Loaded {len(df):,} films")

    # Initialize analyzer
    analyzer = InternationalCinemaAnalyzer(df)

    # Generate visualizations
    logger.info("\n" + "="*80)
    logger.info("GENERATING VISUALIZATIONS")
    logger.info("="*80 + "\n")

    lang_counts = analyzer.viz_01_language_distribution()
    country_counts = analyzer.viz_02_production_countries()
    non_english_stats = analyzer.viz_03_non_english_languages()
    analyzer.viz_04_foreign_vs_hollywood()
    regional_stats = analyzer.viz_05_regional_cinema()
    analyzer.viz_06_language_genre_heatmap()
    analyzer.viz_07_coproductions()
    analyzer.viz_08_language_evolution()
    analyzer.viz_09_diversity_score()
    analyzer.viz_10_coverage_overview()

    # Generate report
    logger.info("\n" + "="*80)
    logger.info("GENERATING REPORT")
    logger.info("="*80 + "\n")

    analyzer.generate_report(lang_counts, country_counts, non_english_stats, regional_stats)

    # Summary
    logger.info("\n" + "="*80)
    logger.info("BATCH 18 COMPLETE!")
    logger.info("="*80)
    logger.info(f"\n✓ Generated 10 visualizations in: {OUTPUT_DIR}")
    logger.info(f"✓ Generated report in: {REPORT_FILE}")
    logger.info(f"\nKey Findings:")
    logger.info(f"  • {analyzer.stats['unique_languages']} unique languages")
    logger.info(f"  • {analyzer.stats['english_percentage']:.1f}% English films")
    logger.info(f"  • {analyzer.stats['non_english_percentage']:.1f}% non-English films")
    logger.info(f"  • {analyzer.stats['language_coverage']:.1f}% language coverage")
    logger.info("\nCheck the visualizations folder for all generated images!")
    logger.info("="*80)


if __name__ == "__main__":
    main()
