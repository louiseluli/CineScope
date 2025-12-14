"""
CineScope Batch 10: Keywords & Themes Analysis

COMPREHENSIVE KEYWORD AND THEMATIC ANALYSIS
============================================
This batch analyzes:

1. Keyword Distribution
   - Most common keywords across all films
   - Keyword frequency trends over decades
   - Genre-specific keywords

2. Thematic Patterns
   - Theme categories (love, revenge, survival, etc.)
   - Setting analysis (cities, environments)
   - Character archetypes frequency

3. Keyword Correlations
   - Keywords that appear together
   - Keyword-rating correlations
   - Keyword-genre mapping

4. Word Clouds
   - Overall keyword cloud
   - Genre-specific clouds
   - Decade-specific clouds

5. Emotional/Content Analysis
   - Mood distribution
   - Content warnings frequency
   - Tone analysis by genre

Usage:
    python scripts/batch_10_keywords_themes.py
"""
import sys
import json
import logging
import csv
from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict
from typing import Dict, List, Optional, Tuple
import re

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
import numpy as np
from tqdm import tqdm

# Optional imports
try:
    from wordcloud import WordCloud
    HAS_WORDCLOUD = True
except ImportError:
    HAS_WORDCLOUD = False
    print("Note: Install 'wordcloud' package for word cloud visualizations")

from src.core.config import settings

settings.ensure_directories()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(settings.LOG_FILE)
    ]
)
logger = logging.getLogger(__name__)

# Color palettes
THEME_COLORS = {
    'themes': '#E74C3C',
    'settings': '#3498DB',
    'character_types': '#2ECC71',
    'narrative': '#9B59B6',
    'emotional': '#F39C12',
    'content': '#E67E22'
}

GENRE_COLORS = {
    'Action': '#E74C3C', 'Adventure': '#F39C12', 'Animation': '#3498DB',
    'Comedy': '#2ECC71', 'Crime': '#95A5A6', 'Documentary': '#1ABC9C',
    'Drama': '#9B59B6', 'Family': '#E91E63', 'Fantasy': '#673AB7',
    'History': '#795548', 'Horror': '#212121', 'Music': '#FF5722',
    'Mystery': '#607D8B', 'Romance': '#E91E63', 'Science Fiction': '#00BCD4',
    'TV Movie': '#FFEB3B', 'Thriller': '#F44336', 'War': '#4CAF50', 'Western': '#8D6E63'
}


class KeywordsAnalyzer:
    """
    Analyzes keywords and themes across cinema data.
    """
    
    def __init__(self):
        self.keywords_cache_file = settings.PROCESSED_DATA_DIR / "keywords_cache.json"
        # Use watched_movies_master.csv (~2,300 watched films)
        # NOT master_cinema_data.csv (includes unwatched catalog)
        self.master_csv = settings.PROCESSED_DATA_DIR / "watched_movies_master.csv"
        self.output_dir = settings.VISUALIZATIONS_DIR / "batch_10"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load data
        self.movies = self._load_movies()
        self.keywords_cache = self._load_keywords_cache()
        
        # Analysis storage
        self.all_keywords: Counter = Counter()
        self.keywords_by_genre: Dict[str, Counter] = defaultdict(Counter)
        self.keywords_by_decade: Dict[int, Counter] = defaultdict(Counter)
        self.keyword_cooccurrence: Dict[str, Counter] = defaultdict(Counter)
        self.keywords_by_rating: Dict[str, List[float]] = defaultdict(list)
        self.category_stats = defaultdict(Counter)
        
        # Theme keyword mapping
        self.theme_keywords = {
            'love': ['love', 'romance', 'relationship', 'affair', 'passion', 'marriage', 'wedding', 'couple'],
            'revenge': ['revenge', 'vengeance', 'retribution', 'payback'],
            'survival': ['survival', 'stranded', 'disaster', 'apocalypse', 'post-apocalyptic'],
            'family': ['family', 'father', 'mother', 'son', 'daughter', 'parent', 'sibling', 'brother', 'sister'],
            'crime': ['crime', 'murder', 'robbery', 'heist', 'gangster', 'mafia', 'corruption'],
            'war': ['war', 'soldier', 'military', 'battle', 'combat', 'army', 'veteran'],
            'coming_of_age': ['coming of age', 'teenager', 'adolescence', 'youth', 'growing up'],
            'supernatural': ['supernatural', 'ghost', 'haunted', 'paranormal', 'demon', 'witch', 'vampire', 'zombie'],
            'technology': ['technology', 'computer', 'artificial intelligence', 'robot', 'hacker', 'virtual reality'],
            'nature': ['nature', 'animal', 'wildlife', 'environmental', 'climate', 'ocean', 'forest', 'mountain']
        }
        
        # Set style
        plt.style.use('seaborn-v0_8-darkgrid')
        plt.rcParams['figure.facecolor'] = '#f8f9fa'
        plt.rcParams['axes.facecolor'] = '#ffffff'
        plt.rcParams['font.family'] = 'sans-serif'
    
    def _load_movies(self) -> List[Dict]:
        """Load movies from master CSV."""
        movies = []
        if self.master_csv.exists():
            with open(self.master_csv, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    movies.append(row)
            logger.info(f"Loaded {len(movies):,} movies")
        return movies
    
    def _load_keywords_cache(self) -> Dict:
        """Load keywords cache."""
        if self.keywords_cache_file.exists():
            with open(self.keywords_cache_file, 'r') as f:
                return json.load(f)
        return {}
    
    def analyze(self):
        """Run full keywords analysis."""
        logger.info("=" * 80)
        logger.info("BATCH 10: KEYWORDS & THEMES ANALYSIS")
        logger.info("=" * 80)
        
        # Process all keywords
        has_keywords = self._process_keywords()
        
        if not has_keywords:
            return  # Exit early if no keywords
        
        # Generate visualizations
        self._viz_top_keywords()
        self._viz_keywords_by_decade()
        self._viz_keywords_by_genre()
        self._viz_theme_distribution()
        self._viz_keyword_heatmap()
        self._viz_category_breakdown()
        
        if HAS_WORDCLOUD:
            self._viz_wordcloud_overall()
            self._viz_wordcloud_by_genre()
        
        # Generate report
        self._generate_report()
        
        logger.info(f"\nVisualizations saved to {self.output_dir}")
    
    def _process_keywords(self):
        """Process all keywords from movies."""
        logger.info("Processing keywords...")
        
        for movie in tqdm(self.movies, desc="Processing movies"):
            tmdb_id = str(movie.get('tmdb_id') or movie.get('TMDB_ID') or '')
            if not tmdb_id or tmdb_id not in self.keywords_cache:
                continue
            kw_data = self.keywords_cache[tmdb_id]
            keywords = kw_data.get('keywords', [])
            if not keywords:
                continue
            # Overall counts
            for kw in keywords:
                kw_lower = kw.lower()
                self.all_keywords[kw_lower] += 1
            # By genre (normalize genre names)
            genres = movie.get('genres', '').split('|') if movie.get('genres') else []
            genres = [g.strip().title() for g in genres if g.strip()]
            for genre in genres:
                for kw in keywords:
                    self.keywords_by_genre[genre][kw.lower()] += 1
            # By decade
            try:
                year = int(movie.get('release_year') or movie.get('Year') or 0)
                if year > 0:
                    decade = (year // 10) * 10
                    for kw in keywords:
                        self.keywords_by_decade[decade][kw.lower()] += 1
            except:
                pass
            
            # By rating
            try:
                rating = float(movie.get('vote_average') or movie.get('imdb_rating') or 0)
                if rating > 0:
                    for kw in keywords:
                        self.keywords_by_rating[kw.lower()].append(rating)
            except:
                pass
            
            # Co-occurrence
            for i, kw1 in enumerate(keywords):
                for kw2 in keywords[i+1:]:
                    self.keyword_cooccurrence[kw1.lower()][kw2.lower()] += 1
                    self.keyword_cooccurrence[kw2.lower()][kw1.lower()] += 1
            
            # Category stats
            categories = kw_data.get('categories', {})
            for category, cat_keywords in categories.items():
                for kw in cat_keywords:
                    self.category_stats[category][kw.lower()] += 1
        
        logger.info(f"Processed {len(self.all_keywords):,} unique keywords")
        
        # Early exit if no keywords found
        if len(self.all_keywords) == 0:
            logger.warning("=" * 60)
            logger.warning("NO KEYWORDS FOUND IN DATA!")
            logger.warning("=" * 60)
            logger.warning("Please run the keywords enrichment script first:")
            logger.warning("  python scripts/enrich/08_enrich_keywords.py")
            logger.warning("")
            logger.warning("This will fetch keywords from TMDB for your watched movies.")
            return False
        
        return True
    
    def _viz_top_keywords(self):
        """Visualize top keywords."""
        logger.info("Creating top keywords visualization...")
        
        top_30 = self.all_keywords.most_common(30)
        
        if not top_30:
            logger.warning("No keywords found - skipping visualization")
            logger.warning("Hint: Run 'python scripts/enrich/08_enrich_keywords.py' first")
            return
        
        keywords, counts = zip(*top_30)
        
        fig, ax = plt.subplots(figsize=(14, 10))
        
        y_pos = np.arange(len(keywords))
        colors = plt.cm.viridis(np.linspace(0.2, 0.8, len(keywords)))
        
        bars = ax.barh(y_pos, counts, color=colors, edgecolor='white')
        
        ax.set_yticks(y_pos)
        ax.set_yticklabels(keywords, fontsize=10)
        ax.invert_yaxis()
        ax.set_xlabel('Occurrences', fontsize=12)
        ax.set_title('Top 30 Movie Keywords', fontsize=16, fontweight='bold', pad=20)
        
        # Add count labels
        for bar, count in zip(bars, counts):
            ax.text(bar.get_width() + max(counts)*0.01, bar.get_y() + bar.get_height()/2,
                   f'{count:,}', va='center', fontsize=9)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'top_keywords.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def _viz_keywords_by_decade(self):
        """Visualize keyword trends by decade."""
        logger.info("Creating keywords by decade visualization...")
        
        # Get decades with data
        decades = sorted([d for d in self.keywords_by_decade.keys() if d >= 1920 and d <= 2020])
        
        if not decades:
            logger.warning("No decade data available")
            return
        
        # Track certain keywords over time
        tracked_keywords = ['love', 'murder', 'based on novel', 'friendship', 'revenge', 'family']
        
        fig, ax = plt.subplots(figsize=(14, 8))
        
        for kw in tracked_keywords:
            values = []
            for decade in decades:
                total_in_decade = sum(self.keywords_by_decade[decade].values())
                kw_count = self.keywords_by_decade[decade].get(kw, 0)
                # Normalize as percentage
                pct = (kw_count / total_in_decade * 100) if total_in_decade else 0
                values.append(pct)
            
            ax.plot(decades, values, marker='o', linewidth=2, markersize=8, label=kw.title())
        
        ax.set_xlabel('Decade', fontsize=12)
        ax.set_ylabel('Percentage of Films', fontsize=12)
        ax.set_title('Keyword Trends Over Decades', fontsize=16, fontweight='bold', pad=20)
        ax.legend(loc='upper left', bbox_to_anchor=(1, 1))
        ax.set_xticks(decades)
        ax.set_xticklabels([f"{d}s" for d in decades], rotation=45)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'keywords_by_decade.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def _viz_keywords_by_genre(self):
        """Visualize top keywords per genre."""
        logger.info("Creating keywords by genre visualization...")
        
        # Select top genres
        top_genres = ['Drama', 'Comedy', 'Action', 'Thriller', 'Horror', 'Romance']
        
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        axes = axes.flatten()
        
        for idx, genre in enumerate(top_genres):
            ax = axes[idx]
            genre_keywords = self.keywords_by_genre.get(genre, None)
            if genre_keywords and len(genre_keywords) > 0:
                top_10 = genre_keywords.most_common(10)
                keywords, counts = zip(*top_10)
                color = GENRE_COLORS.get(genre, '#3498DB')
                y_pos = np.arange(len(keywords))
                ax.barh(y_pos, counts, color=color, edgecolor='white', alpha=0.8)
                ax.set_yticks(y_pos)
                ax.set_yticklabels(keywords, fontsize=9)
                ax.invert_yaxis()
            else:
                ax.text(0.5, 0.5, "No data", ha='center', va='center', fontsize=14)
                ax.set_xticks([])
                ax.set_yticks([])
            ax.set_title(f'{genre}', fontsize=12, fontweight='bold')
            ax.set_xlabel('Count')
        
        plt.suptitle('Top Keywords by Genre', fontsize=18, fontweight='bold', y=1.02)
        plt.tight_layout()
        plt.savefig(self.output_dir / 'keywords_by_genre.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def _viz_theme_distribution(self):
        """Visualize theme distribution."""
        logger.info("Creating theme distribution visualization...")
        
        # Count themes
        theme_counts = {}
        for theme, theme_kws in self.theme_keywords.items():
            count = sum(self.all_keywords.get(kw, 0) for kw in theme_kws)
            theme_counts[theme] = count
        
        # Sort by count
        sorted_themes = sorted(theme_counts.items(), key=lambda x: x[1], reverse=True)
        
        if not sorted_themes or all(c == 0 for _, c in sorted_themes):
            logger.warning("No theme data to visualize")
            return
        
        themes, counts = zip(*sorted_themes)
        
        fig, ax = plt.subplots(figsize=(12, 8))
        
        colors = plt.cm.Spectral(np.linspace(0.1, 0.9, len(themes)))
        
        bars = ax.bar(themes, counts, color=colors, edgecolor='white', linewidth=2)
        
        ax.set_xlabel('Theme', fontsize=12)
        ax.set_ylabel('Keyword Occurrences', fontsize=12)
        ax.set_title('Thematic Distribution in Cinema', fontsize=16, fontweight='bold', pad=20)
        
        plt.xticks(rotation=45, ha='right')
        
        # Add count labels
        for bar, count in zip(bars, counts):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(counts)*0.02,
                   f'{count:,}', ha='center', va='bottom', fontsize=10, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'theme_distribution.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def _viz_keyword_heatmap(self):
        """Create keyword co-occurrence heatmap."""
        logger.info("Creating keyword co-occurrence heatmap...")
        
        # Get top 15 keywords
        top_keywords = [kw for kw, _ in self.all_keywords.most_common(15)]
        
        if not top_keywords:
            logger.warning("No keywords for heatmap visualization")
            return
        
        # Build co-occurrence matrix
        matrix = np.zeros((len(top_keywords), len(top_keywords)))
        
        for i, kw1 in enumerate(top_keywords):
            for j, kw2 in enumerate(top_keywords):
                if i != j:
                    matrix[i][j] = self.keyword_cooccurrence.get(kw1, {}).get(kw2, 0)
        
        # Normalize
        if matrix.max() > 0:
            matrix = matrix / matrix.max()
        
        fig, ax = plt.subplots(figsize=(12, 10))
        
        sns.heatmap(matrix, xticklabels=top_keywords, yticklabels=top_keywords,
                   cmap='YlOrRd', annot=False, square=True, ax=ax,
                   cbar_kws={'label': 'Co-occurrence (normalized)'})
        
        ax.set_title('Keyword Co-occurrence Heatmap', fontsize=16, fontweight='bold', pad=20)
        plt.xticks(rotation=45, ha='right')
        plt.yticks(rotation=0)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'keyword_cooccurrence.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def _viz_category_breakdown(self):
        """Visualize keyword categories."""
        logger.info("Creating category breakdown visualization...")
        
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        axes = axes.flatten()
        
        categories = ['themes', 'settings', 'character_types', 'narrative', 'emotional', 'content']
        
        for idx, category in enumerate(categories):
            ax = axes[idx]
            
            if category in self.category_stats:
                top_10 = self.category_stats[category].most_common(10)
                if top_10:
                    keywords, counts = zip(*top_10)
                    
                    color = THEME_COLORS.get(category, '#3498DB')
                    y_pos = np.arange(len(keywords))
                    
                    ax.barh(y_pos, counts, color=color, edgecolor='white', alpha=0.8)
                    ax.set_yticks(y_pos)
                    ax.set_yticklabels(keywords, fontsize=9)
                    ax.invert_yaxis()
            
            ax.set_title(f'{category.replace("_", " ").title()}', fontsize=12, fontweight='bold')
            ax.set_xlabel('Count')
        
        plt.suptitle('Keywords by Category', fontsize=18, fontweight='bold', y=1.02)
        plt.tight_layout()
        plt.savefig(self.output_dir / 'category_breakdown.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def _viz_wordcloud_overall(self):
        """Create overall word cloud."""
        if not HAS_WORDCLOUD:
            return
        
        logger.info("Creating overall word cloud...")
        
        wordcloud = WordCloud(
            width=1600, height=800,
            background_color='white',
            colormap='viridis',
            max_words=200,
            min_font_size=10,
            max_font_size=150
        ).generate_from_frequencies(self.all_keywords)
        
        fig, ax = plt.subplots(figsize=(16, 8))
        ax.imshow(wordcloud, interpolation='bilinear')
        ax.axis('off')
        ax.set_title('Movie Keywords Word Cloud', fontsize=20, fontweight='bold', pad=20)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'wordcloud_overall.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def _viz_wordcloud_by_genre(self):
        """Create word clouds for top genres."""
        if not HAS_WORDCLOUD:
            return
        
        logger.info("Creating genre-specific word clouds...")
        
        top_genres = ['Drama', 'Comedy', 'Action', 'Horror']
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        axes = axes.flatten()
        
        for idx, genre in enumerate(top_genres):
            ax = axes[idx]
            
            if genre in self.keywords_by_genre and self.keywords_by_genre[genre]:
                wordcloud = WordCloud(
                    width=800, height=600,
                    background_color='white',
                    colormap='plasma',
                    max_words=100,
                    min_font_size=8
                ).generate_from_frequencies(self.keywords_by_genre[genre])
                
                ax.imshow(wordcloud, interpolation='bilinear')
                ax.axis('off')
                ax.set_title(genre, fontsize=14, fontweight='bold')
            else:
                ax.text(0.5, 0.5, 'No data', ha='center', va='center', fontsize=14)
                ax.axis('off')
        
        plt.suptitle('Keywords by Genre', fontsize=18, fontweight='bold', y=1.02)
        plt.tight_layout()
        plt.savefig(self.output_dir / 'wordcloud_by_genre.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def _generate_report(self):
        """Generate analysis report."""
        logger.info("Generating analysis report...")
        
        report_file = self.output_dir / 'keywords_analysis_report.txt'
        
        with open(report_file, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("CINESCOPE BATCH 10: KEYWORDS & THEMES ANALYSIS\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")
            
            # Summary stats
            f.write(f"Total Unique Keywords: {len(self.all_keywords):,}\n")
            f.write(f"Total Keyword Occurrences: {sum(self.all_keywords.values()):,}\n")
            f.write(f"Movies with Keywords: {len(self.keywords_cache):,}\n\n")
            
            # Top 50 keywords
            f.write("TOP 50 KEYWORDS\n")
            f.write("-" * 40 + "\n")
            for rank, (kw, count) in enumerate(self.all_keywords.most_common(50), 1):
                f.write(f"{rank:3}. {kw:30} {count:>6,}\n")
            
            # Keywords by genre
            f.write("\n\nTOP 10 KEYWORDS BY GENRE\n")
            f.write("-" * 40 + "\n")
            for genre in sorted(self.keywords_by_genre.keys()):
                f.write(f"\n{genre}:\n")
                for kw, count in self.keywords_by_genre[genre].most_common(10):
                    f.write(f"  {kw}: {count:,}\n")
            
            # Theme breakdown
            f.write("\n\nTHEME ANALYSIS\n")
            f.write("-" * 40 + "\n")
            for theme, theme_kws in self.theme_keywords.items():
                count = sum(self.all_keywords.get(kw, 0) for kw in theme_kws)
                f.write(f"{theme.replace('_', ' ').title():20} {count:>6,}\n")
            
            # Keyword rating correlations
            f.write("\n\nKEYWORDS WITH HIGHEST AVERAGE RATINGS\n")
            f.write("-" * 40 + "\n")
            keyword_ratings = []
            for kw, ratings in self.keywords_by_rating.items():
                if len(ratings) >= 10:  # At least 10 occurrences
                    avg = np.mean(ratings)
                    keyword_ratings.append((kw, avg, len(ratings)))
            
            keyword_ratings.sort(key=lambda x: x[1], reverse=True)
            for kw, avg, count in keyword_ratings[:20]:
                f.write(f"{kw:30} Avg: {avg:.2f}  (n={count})\n")
        
        logger.info(f"Report saved to {report_file}")


def main():
    analyzer = KeywordsAnalyzer()
    analyzer.analyze()


if __name__ == '__main__':
    main()
