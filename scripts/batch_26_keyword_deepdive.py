"""
CineScope Batch 26: Keyword Deep-Dive Analysis

ADVANCED KEYWORD INTELLIGENCE
==============================
This batch performs advanced keyword analysis that COMPLEMENTS Batch 10:

Batch 10 covered:
- Basic keyword frequency
- Keywords by genre/decade
- Theme distribution
- Word clouds
- Simple co-occurrence

Batch 26 adds:
1. Keyword Rarity & Uniqueness
   - TF-IDF analysis
   - Rare/unique keywords
   - Genre-distinctive keywords

2. Keyword Prediction Power
   - Which keywords predict high ratings?
   - Statistical significance testing
   - Keyword quality correlation

3. Advanced Co-Occurrence Networks
   - Network graph visualization
   - Keyword communities/clusters
   - Central keywords analysis

4. Keyword Evolution & Trends
   - Trending keywords (rising/falling)
   - Era-specific signatures
   - Keyword lifecycle analysis

5. Keyword Diversity & Distribution
   - Shannon entropy analysis
   - Keyword concentration metrics
   - Genre diversity scores

6. Keyword Sentiment & Emotional Mapping
   - Positive/negative keywords
   - Emotional tone analysis
   - Mood prediction

7. Cross-Genre Keyword Analysis
   - Most versatile keywords
   - Genre-exclusive keywords
   - Cross-pollination patterns

8. Keyword Clustering
   - Semantic groupings
   - Thematic clusters
   - Keyword families

Data Source: keywords_cache.json (2,287 movies)

Usage:
    python scripts/batch_26_keyword_deepdive.py
"""
import sys
import json
import logging
import csv
from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict
from typing import Dict, List, Optional, Tuple, Set

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
import numpy as np
import pandas as pd
from scipy import stats
from scipy.cluster.hierarchy import dendrogram, linkage
from sklearn.feature_extraction.text import TfidfVectorizer

# Optional imports
try:
    import networkx as nx
    HAS_NETWORKX = True
except ImportError:
    HAS_NETWORKX = False
    print("Note: Install 'networkx' for network graph visualizations")

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


class KeywordDeepDiveAnalyzer:
    """
    Advanced keyword analysis for deeper insights.
    """

    def __init__(self):
        self.keywords_cache_file = settings.PROCESSED_DATA_DIR / "keywords_cache.json"
        self.master_csv = settings.PROCESSED_DATA_DIR / "watched_movies_master.csv"
        self.output_dir = settings.VISUALIZATIONS_DIR / "batch_26"
        self.report_dir = settings.BASE_DIR / "analysis_outputs" / "reports"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)

        # Load data
        self.movies = self._load_movies()
        self.keywords_cache = self._load_keywords_cache()

        # Analysis storage
        self.keyword_stats: Dict[str, Dict] = {}
        self.genre_keywords: Dict[str, Set[str]] = defaultdict(set)
        self.decade_keywords: Dict[int, Set[str]] = defaultdict(set)
        self.keyword_ratings: Dict[str, List[float]] = defaultdict(list)
        self.keyword_cooccurrence: Dict[str, Counter] = defaultdict(Counter)
        self.all_keywords: Counter = Counter()

        # Emotional keyword mappings
        self.emotional_keywords = {
            'positive': [
                'love', 'friendship', 'hope', 'celebration', 'wedding', 'success',
                'victory', 'redemption', 'hero', 'rescue', 'happy ending', 'romance',
                'joy', 'triumph', 'freedom', 'peace', 'family', 'loyalty'
            ],
            'negative': [
                'death', 'murder', 'revenge', 'tragedy', 'loss', 'betrayal',
                'war', 'violence', 'crime', 'fear', 'suffering', 'corruption',
                'destruction', 'apocalypse', 'dystopia', 'despair', 'isolation'
            ],
            'neutral': [
                'investigation', 'journey', 'discovery', 'technology', 'politics',
                'business', 'school', 'city', 'based on novel', 'flashback'
            ]
        }

        # Stats tracking
        self.stats = {}

        # Set matplotlib style
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
                cache = json.load(f)
                logger.info(f"Loaded keywords for {len(cache):,} movies")
                return cache
        return {}

    def _parse_genres(self, genres_str: str) -> List[str]:
        """Parse genre string into list."""
        if not genres_str or pd.isna(genres_str):
            return []

        if '|' in str(genres_str):
            genres = [g.strip() for g in str(genres_str).split('|') if g.strip()]
        elif ',' in str(genres_str):
            genres_str = str(genres_str).strip("[]'\"")
            genres = [g.strip().strip("'\"") for g in genres_str.split(',') if g.strip()]
        else:
            genres = [str(genres_str).strip()]

        return [g.title() for g in genres if g]

    def analyze(self):
        """Run full keyword deep-dive analysis."""
        logger.info("=" * 80)
        logger.info("CINESCOPE BATCH 26: KEYWORD DEEP-DIVE ANALYSIS")
        logger.info("=" * 80)
        logger.info("")

        # Process keywords
        logger.info("Processing keywords...")
        self._process_keywords()

        if not self.all_keywords:
            logger.error("No keywords found! Please run keyword enrichment first.")
            return

        logger.info(f"Processed {len(self.all_keywords):,} unique keywords")
        logger.info("")

        logger.info("=" * 80)
        logger.info("GENERATING VISUALIZATIONS")
        logger.info("=" * 80)
        logger.info("")

        # Generate all visualizations
        self._viz_01_tfidf_rarity()
        self._viz_02_prediction_power()
        self._viz_03_cooccurrence_network()
        self._viz_04_trending_keywords()
        self._viz_05_diversity_metrics()
        self._viz_06_emotional_mapping()
        self._viz_07_cross_genre_analysis()
        self._viz_08_era_signatures()
        self._viz_09_keyword_lifecycle()
        self._viz_10_quality_correlation()

        # Generate report
        self._generate_report()

        logger.info("")
        logger.info("=" * 80)
        logger.info("BATCH 26 COMPLETE!")
        logger.info("=" * 80)
        logger.info("")
        logger.info(f"✓ Generated 10 visualizations in: {self.output_dir}")
        logger.info(f"✓ Generated report in: {self.report_dir / 'batch_26_keyword_deepdive_report.txt'}")
        logger.info("")
        logger.info("Key Findings:")
        logger.info(f"  • {len(self.all_keywords):,} unique keywords analyzed")
        logger.info(f"  • {len(self.keyword_stats):,} keywords with full statistics")
        logger.info(f"  • {len([k for k, v in self.keyword_stats.items() if v.get('avg_rating', 0) > 7.5]):,} keywords correlated with high ratings")
        logger.info("")
        logger.info("Check the visualizations folder for all generated images!")
        logger.info("=" * 80)

    def _process_keywords(self):
        """Process all keywords and build statistics."""
        for movie in self.movies:
            tmdb_id = str(movie.get('tmdb_id') or movie.get('TMDB_ID') or '')
            if not tmdb_id or tmdb_id not in self.keywords_cache:
                continue

            kw_data = self.keywords_cache[tmdb_id]
            keywords = kw_data.get('keywords', [])
            if not keywords:
                continue

            # Get movie metadata
            rating = float(movie.get('imdb_rating') or movie.get('IMDb Rating') or 0)
            year = int(movie.get('release_year') or movie.get('Year') or 2000)
            decade = (year // 10) * 10
            genres = self._parse_genres(movie.get('genres', ''))

            # Process each keyword
            for kw in keywords:
                kw_lower = kw.lower()
                self.all_keywords[kw_lower] += 1

                # Initialize stats if needed
                if kw_lower not in self.keyword_stats:
                    self.keyword_stats[kw_lower] = {
                        'count': 0,
                        'ratings': [],
                        'decades': set(),
                        'genres': set(),
                        'first_seen': year,
                        'last_seen': year
                    }

                # Update stats
                stats = self.keyword_stats[kw_lower]
                stats['count'] += 1
                if rating > 0:
                    stats['ratings'].append(rating)
                stats['decades'].add(decade)
                for genre in genres:
                    stats['genres'].add(genre)
                    self.genre_keywords[genre].add(kw_lower)
                stats['first_seen'] = min(stats['first_seen'], year)
                stats['last_seen'] = max(stats['last_seen'], year)

                self.decade_keywords[decade].add(kw_lower)

                if rating > 0:
                    self.keyword_ratings[kw_lower].append(rating)

            # Co-occurrence
            for i, kw1 in enumerate(keywords):
                for kw2 in keywords[i+1:]:
                    self.keyword_cooccurrence[kw1.lower()][kw2.lower()] += 1
                    self.keyword_cooccurrence[kw2.lower()][kw1.lower()] += 1

        # Calculate derived stats
        for kw, stats in self.keyword_stats.items():
            if stats['ratings']:
                stats['avg_rating'] = np.mean(stats['ratings'])
                stats['std_rating'] = np.std(stats['ratings'])
            else:
                stats['avg_rating'] = 0
                stats['std_rating'] = 0

            stats['genre_diversity'] = len(stats['genres'])
            stats['decade_span'] = stats['last_seen'] - stats['first_seen']
            stats['versatility'] = len(stats['genres']) * len(stats['decades'])

    def _viz_01_tfidf_rarity(self):
        """Visualize keyword rarity using TF-IDF-like scoring."""
        logger.info("Creating Visualization 1: Keyword Rarity Analysis")

        # Calculate rarity scores
        total_movies = len(self.keywords_cache)
        keyword_rarity = {}

        for kw, count in self.all_keywords.items():
            # IDF-like score: log(total_movies / keyword_frequency)
            rarity_score = np.log(total_movies / count) if count > 0 else 0
            keyword_rarity[kw] = rarity_score

        # Sort by rarity
        sorted_rare = sorted(keyword_rarity.items(), key=lambda x: x[1], reverse=True)
        sorted_common = sorted(keyword_rarity.items(), key=lambda x: x[1])

        fig, axes = plt.subplots(2, 2, figsize=(18, 14))

        # 1. Top 20 rarest keywords
        ax = axes[0, 0]
        rare_keywords = sorted_rare[:20]
        keywords, scores = zip(*rare_keywords)
        y_pos = np.arange(len(keywords))
        colors_grad = plt.cm.Purples(np.linspace(0.4, 0.9, len(keywords)))
        ax.barh(y_pos, scores, color=colors_grad, edgecolor='white')
        ax.set_yticks(y_pos)
        ax.set_yticklabels(keywords, fontsize=9)
        ax.invert_yaxis()
        ax.set_xlabel('Rarity Score (IDF)', fontsize=11)
        ax.set_title('Top 20 Rarest Keywords', fontsize=12, fontweight='bold')

        # 2. Top 20 most common (low rarity)
        ax = axes[0, 1]
        common_keywords = sorted_common[:20]
        keywords, scores = zip(*common_keywords)
        counts = [self.all_keywords[kw] for kw in keywords]
        y_pos = np.arange(len(keywords))
        colors_grad = plt.cm.Greens(np.linspace(0.4, 0.9, len(keywords)))
        ax.barh(y_pos, counts, color=colors_grad, edgecolor='white')
        ax.set_yticks(y_pos)
        ax.set_yticklabels(keywords, fontsize=9)
        ax.invert_yaxis()
        ax.set_xlabel('Frequency', fontsize=11)
        ax.set_title('Top 20 Most Common Keywords', fontsize=12, fontweight='bold')

        # 3. Rarity distribution
        ax = axes[1, 0]
        rarity_values = list(keyword_rarity.values())
        ax.hist(rarity_values, bins=50, color='#9B59B6', edgecolor='white', alpha=0.8)
        ax.set_xlabel('Rarity Score', fontsize=11)
        ax.set_ylabel('Number of Keywords', fontsize=11)
        ax.set_title('Rarity Score Distribution', fontsize=12, fontweight='bold')
        ax.axvline(np.median(rarity_values), color='red', linestyle='--', linewidth=2,
                   label=f'Median: {np.median(rarity_values):.2f}')
        ax.legend()

        # 4. Statistics
        ax = axes[1, 1]
        ax.axis('off')

        # Find genre-distinctive keywords
        genre_distinctive = {}
        for genre in ['Drama', 'Comedy', 'Action', 'Horror', 'Thriller']:
            if genre in self.genre_keywords:
                genre_kws = self.genre_keywords[genre]
                other_genres_kws = set()
                for g, kws in self.genre_keywords.items():
                    if g != genre:
                        other_genres_kws.update(kws)

                distinctive = genre_kws - other_genres_kws
                if distinctive:
                    genre_distinctive[genre] = len(distinctive)

        stats_text = f"""
KEYWORD RARITY STATISTICS

Total Unique Keywords: {len(keyword_rarity):,}

Rarity Scores:
  Mean: {np.mean(rarity_values):.2f}
  Median: {np.median(rarity_values):.2f}
  Min: {min(rarity_values):.2f}
  Max: {max(rarity_values):.2f}

Ultra-Rare Keywords (seen once):
  Count: {len([k for k, c in self.all_keywords.items() if c == 1]):,}

Most Common Keywords (>100 films):
  Count: {len([k for k, c in self.all_keywords.items() if c > 100]):,}

Genre-Distinctive Keywords:
  Drama: {genre_distinctive.get('Drama', 0)} unique
  Comedy: {genre_distinctive.get('Comedy', 0)} unique
  Action: {genre_distinctive.get('Action', 0)} unique
  Horror: {genre_distinctive.get('Horror', 0)} unique
  Thriller: {genre_distinctive.get('Thriller', 0)} unique
"""
        ax.text(0.1, 0.9, stats_text, fontsize=10, verticalalignment='top',
                fontfamily='monospace', bbox=dict(boxstyle='round', facecolor='lavender', alpha=0.3))

        plt.suptitle('Keyword Rarity & Uniqueness Analysis', fontsize=18, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.output_dir / '01_keyword_rarity.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 01_keyword_rarity.png")

    def _viz_02_prediction_power(self):
        """Analyze which keywords predict high ratings."""
        logger.info("Creating Visualization 2: Keyword Prediction Power")

        # Filter keywords with at least 15 occurrences for statistical significance
        significant_keywords = {}
        for kw, ratings in self.keyword_ratings.items():
            if len(ratings) >= 15:
                avg_rating = np.mean(ratings)
                std_rating = np.std(ratings)
                # T-test against overall mean
                overall_mean = np.mean([r for ratings_list in self.keyword_ratings.values()
                                       for r in ratings_list])
                t_stat, p_value = stats.ttest_1samp(ratings, overall_mean)

                significant_keywords[kw] = {
                    'avg_rating': avg_rating,
                    'std_rating': std_rating,
                    'count': len(ratings),
                    't_stat': t_stat,
                    'p_value': p_value,
                    'significant': p_value < 0.05
                }

        # Sort by average rating
        sorted_by_rating = sorted(significant_keywords.items(),
                                 key=lambda x: x[1]['avg_rating'],
                                 reverse=True)

        fig, axes = plt.subplots(2, 2, figsize=(18, 14))

        # 1. Top 20 quality predictors (high rating)
        ax = axes[0, 0]
        top_quality = sorted_by_rating[:20]
        keywords = [kw for kw, _ in top_quality]
        ratings = [stats['avg_rating'] for _, stats in top_quality]
        y_pos = np.arange(len(keywords))

        # Color by significance
        colors = ['#2ECC71' if top_quality[i][1]['significant'] else '#95A5A6'
                 for i in range(len(top_quality))]

        ax.barh(y_pos, ratings, color=colors, edgecolor='white')
        ax.set_yticks(y_pos)
        ax.set_yticklabels(keywords, fontsize=9)
        ax.invert_yaxis()
        ax.set_xlabel('Average IMDb Rating', fontsize=11)
        ax.set_title('Top 20 Keywords Predicting High Quality', fontsize=12, fontweight='bold')
        ax.axvline(np.mean([r for ratings_list in self.keyword_ratings.values() for r in ratings_list]),
                   color='red', linestyle='--', linewidth=2, alpha=0.5, label='Overall Mean')
        ax.legend()

        # Create legend for significance
        green_patch = mpatches.Patch(color='#2ECC71', label='Statistically Significant')
        gray_patch = mpatches.Patch(color='#95A5A6', label='Not Significant')
        ax.legend(handles=[green_patch, gray_patch], loc='lower right', fontsize=9)

        # 2. Bottom 20 (low rating predictors)
        ax = axes[0, 1]
        bottom_quality = sorted_by_rating[-20:]
        keywords = [kw for kw, _ in bottom_quality]
        ratings = [stats['avg_rating'] for _, stats in bottom_quality]
        y_pos = np.arange(len(keywords))

        colors = ['#E74C3C' if bottom_quality[i][1]['significant'] else '#95A5A6'
                 for i in range(len(bottom_quality))]

        ax.barh(y_pos, ratings, color=colors, edgecolor='white')
        ax.set_yticks(y_pos)
        ax.set_yticklabels(keywords, fontsize=9)
        ax.invert_yaxis()
        ax.set_xlabel('Average IMDb Rating', fontsize=11)
        ax.set_title('Top 20 Keywords Predicting Low Quality', fontsize=12, fontweight='bold')
        ax.axvline(np.mean([r for ratings_list in self.keyword_ratings.values() for r in ratings_list]),
                   color='red', linestyle='--', linewidth=2, alpha=0.5, label='Overall Mean')

        red_patch = mpatches.Patch(color='#E74C3C', label='Statistically Significant')
        ax.legend(handles=[red_patch, gray_patch], loc='lower right', fontsize=9)

        # 3. Rating prediction scatter (avg rating vs count)
        ax = axes[1, 0]
        counts = [stats['count'] for _, stats in significant_keywords.items()]
        avg_ratings = [stats['avg_rating'] for _, stats in significant_keywords.items()]

        ax.scatter(counts, avg_ratings, alpha=0.5, s=50, c='#3498DB', edgecolors='white')
        ax.set_xlabel('Keyword Frequency', fontsize=11)
        ax.set_ylabel('Average Rating', fontsize=11)
        ax.set_title('Frequency vs Rating Correlation', fontsize=12, fontweight='bold')
        ax.grid(True, alpha=0.3)

        # Calculate correlation
        if counts and avg_ratings:
            corr = np.corrcoef(counts, avg_ratings)[0, 1]
            ax.text(0.05, 0.95, f'Correlation: {corr:.3f}', transform=ax.transAxes,
                   fontsize=11, verticalalignment='top',
                   bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        # 4. Statistics
        ax = axes[1, 1]
        ax.axis('off')

        sig_positive = len([k for k, s in significant_keywords.items()
                           if s['significant'] and s['avg_rating'] > np.mean(avg_ratings)])
        sig_negative = len([k for k, s in significant_keywords.items()
                           if s['significant'] and s['avg_rating'] < np.mean(avg_ratings)])

        stats_text = f"""
PREDICTION POWER STATISTICS

Keywords Analyzed: {len(significant_keywords):,}
  (Minimum 15 occurrences)

Statistical Significance (p < 0.05):
  High-Quality Predictors: {sig_positive}
  Low-Quality Predictors: {sig_negative}
  Not Significant: {len(significant_keywords) - sig_positive - sig_negative}

Top Quality Predictors:
  1. {sorted_by_rating[0][0]}: {sorted_by_rating[0][1]['avg_rating']:.2f}
  2. {sorted_by_rating[1][0]}: {sorted_by_rating[1][1]['avg_rating']:.2f}
  3. {sorted_by_rating[2][0]}: {sorted_by_rating[2][1]['avg_rating']:.2f}

Bottom Quality Predictors:
  1. {sorted_by_rating[-1][0]}: {sorted_by_rating[-1][1]['avg_rating']:.2f}
  2. {sorted_by_rating[-2][0]}: {sorted_by_rating[-2][1]['avg_rating']:.2f}
  3. {sorted_by_rating[-3][0]}: {sorted_by_rating[-3][1]['avg_rating']:.2f}

Insight:
  Frequency-Rating Correlation: {corr:.3f}
  {"Common keywords don't predict quality" if abs(corr) < 0.3 else "Frequency affects perceived quality"}
"""
        ax.text(0.1, 0.9, stats_text, fontsize=10, verticalalignment='top',
                fontfamily='monospace', bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.3))

        plt.suptitle('Which Keywords Predict Quality?', fontsize=18, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.output_dir / '02_prediction_power.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 02_prediction_power.png")

    def _viz_03_cooccurrence_network(self):
        """Create keyword co-occurrence network graph."""
        logger.info("Creating Visualization 3: Co-occurrence Network")

        if not HAS_NETWORKX:
            logger.warning("Skipping network visualization (networkx not installed)")
            # Create placeholder
            fig, ax = plt.subplots(figsize=(14, 10))
            ax.text(0.5, 0.5, 'Network visualization requires networkx\n\npip install networkx',
                   ha='center', va='center', fontsize=16)
            ax.axis('off')
            plt.savefig(self.output_dir / '03_cooccurrence_network.png', dpi=300, bbox_inches='tight')
            plt.close()
            logger.info(f"✓ Saved: 03_cooccurrence_network.png (placeholder)")
            return

        # Get top 40 keywords
        top_keywords = [kw for kw, _ in self.all_keywords.most_common(40)]

        # Build network
        G = nx.Graph()

        # Add nodes
        for kw in top_keywords:
            G.add_node(kw, size=self.all_keywords[kw])

        # Add edges (co-occurrence > threshold)
        threshold = 5
        for kw1 in top_keywords:
            for kw2 in top_keywords:
                if kw1 < kw2:  # Avoid duplicates
                    weight = self.keyword_cooccurrence[kw1].get(kw2, 0)
                    if weight >= threshold:
                        G.add_edge(kw1, kw2, weight=weight)

        fig, ax = plt.subplots(figsize=(16, 16))

        # Layout
        pos = nx.spring_layout(G, k=2, iterations=50, seed=42)

        # Draw nodes
        node_sizes = [G.nodes[node]['size'] * 10 for node in G.nodes()]
        nx.draw_networkx_nodes(G, pos, node_size=node_sizes, node_color='#3498DB',
                               alpha=0.7, ax=ax)

        # Draw edges
        edge_weights = [G[u][v]['weight'] for u, v in G.edges()]
        nx.draw_networkx_edges(G, pos, width=[w/5 for w in edge_weights],
                               alpha=0.3, edge_color='gray', ax=ax)

        # Draw labels
        nx.draw_networkx_labels(G, pos, font_size=8, font_weight='bold', ax=ax)

        ax.set_title('Keyword Co-occurrence Network (Top 40 Keywords)',
                    fontsize=18, fontweight='bold', pad=20)
        ax.axis('off')

        plt.tight_layout()
        plt.savefig(self.output_dir / '03_cooccurrence_network.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 03_cooccurrence_network.png")

    def _viz_04_trending_keywords(self):
        """Analyze trending keywords (rising/falling over time)."""
        logger.info("Creating Visualization 4: Trending Keywords")

        # Calculate trend scores
        keyword_trends = {}

        for kw, stats in self.keyword_stats.items():
            if stats['count'] < 10:  # Need enough data
                continue

            decades = sorted(stats['decades'])
            if len(decades) < 2:
                continue

            # Count per decade
            decade_counts = {}
            for movie in self.movies:
                tmdb_id = str(movie.get('tmdb_id') or movie.get('TMDB_ID') or '')
                if tmdb_id not in self.keywords_cache:
                    continue

                keywords = self.keywords_cache[tmdb_id].get('keywords', [])
                if kw not in [k.lower() for k in keywords]:
                    continue

                year = int(movie.get('release_year') or movie.get('Year') or 2000)
                decade = (year // 10) * 10
                decade_counts[decade] = decade_counts.get(decade, 0) + 1

            # Calculate trend (linear regression slope)
            decades_list = sorted(decade_counts.keys())
            counts_list = [decade_counts[d] for d in decades_list]

            if len(decades_list) >= 3:
                slope, intercept = np.polyfit(decades_list, counts_list, 1)
                keyword_trends[kw] = {
                    'slope': slope,
                    'recent_count': decade_counts.get(2020, 0) + decade_counts.get(2010, 0),
                    'old_count': decade_counts.get(1990, 0) + decade_counts.get(2000, 0),
                    'total_count': stats['count']
                }

        # Sort by slope
        rising = sorted([(k, v) for k, v in keyword_trends.items() if v['slope'] > 0],
                       key=lambda x: x[1]['slope'], reverse=True)[:20]
        falling = sorted([(k, v) for k, v in keyword_trends.items() if v['slope'] < 0],
                        key=lambda x: x[1]['slope'])[:20]

        fig, axes = plt.subplots(2, 2, figsize=(18, 14))

        # 1. Rising keywords
        ax = axes[0, 0]
        if rising:
            keywords, trends = zip(*rising)
            slopes = [t['slope'] for t in trends]
            y_pos = np.arange(len(keywords))
            colors_grad = plt.cm.Greens(np.linspace(0.4, 0.9, len(keywords)))
            ax.barh(y_pos, slopes, color=colors_grad, edgecolor='white')
            ax.set_yticks(y_pos)
            ax.set_yticklabels(keywords, fontsize=9)
            ax.invert_yaxis()
            ax.set_xlabel('Trend Slope (Rising)', fontsize=11)
            ax.set_title('Top 20 Rising Keywords (Gaining Popularity)', fontsize=12, fontweight='bold')

        # 2. Falling keywords
        ax = axes[0, 1]
        if falling:
            keywords, trends = zip(*falling)
            slopes = [abs(t['slope']) for t in trends]
            y_pos = np.arange(len(keywords))
            colors_grad = plt.cm.Reds(np.linspace(0.4, 0.9, len(keywords)))
            ax.barh(y_pos, slopes, color=colors_grad, edgecolor='white')
            ax.set_yticks(y_pos)
            ax.set_yticklabels(keywords, fontsize=9)
            ax.invert_yaxis()
            ax.set_xlabel('Trend Slope (Falling)', fontsize=11)
            ax.set_title('Top 20 Falling Keywords (Losing Popularity)', fontsize=12, fontweight='bold')

        # 3. Era comparison (2010s-2020s vs 1990s-2000s)
        ax = axes[1, 0]
        if keyword_trends:
            growth_ratios = []
            growth_keywords = []

            for kw, trend in keyword_trends.items():
                if trend['old_count'] > 0:
                    ratio = (trend['recent_count'] / trend['old_count']) - 1
                    if abs(ratio) > 0.5 and trend['total_count'] > 15:  # Significant change
                        growth_ratios.append(ratio * 100)
                        growth_keywords.append(kw)

            # Top 15 growth
            if growth_ratios:
                combined = list(zip(growth_keywords, growth_ratios))
                combined_sorted = sorted(combined, key=lambda x: x[1], reverse=True)[:15]
                keywords, ratios = zip(*combined_sorted)

                y_pos = np.arange(len(keywords))
                colors = ['#2ECC71' if r > 0 else '#E74C3C' for r in ratios]
                ax.barh(y_pos, ratios, color=colors, edgecolor='white')
                ax.set_yticks(y_pos)
                ax.set_yticklabels(keywords, fontsize=9)
                ax.invert_yaxis()
                ax.set_xlabel('Growth Rate (%)', fontsize=11)
                ax.set_title('Modern Era (2010s) vs Classic Era (1990s) Growth', fontsize=12, fontweight='bold')
                ax.axvline(0, color='black', linestyle='-', linewidth=1, alpha=0.3)

        # 4. Statistics
        ax = axes[1, 1]
        ax.axis('off')

        stats_text = f"""
TRENDING KEYWORDS STATISTICS

Rising Keywords: {len(rising)}
Falling Keywords: {len(falling)}

Top 3 Rising:
  1. {rising[0][0] if rising else 'N/A'}
  2. {rising[1][0] if len(rising) > 1 else 'N/A'}
  3. {rising[2][0] if len(rising) > 2 else 'N/A'}

Top 3 Falling:
  1. {falling[0][0] if falling else 'N/A'}
  2. {falling[1][0] if len(falling) > 1 else 'N/A'}
  3. {falling[2][0] if len(falling) > 2 else 'N/A'}

Insight:
  Modern cinema (2010s-2020s) shows shift
  towards new thematic elements while
  traditional keywords decline in usage.
"""
        ax.text(0.1, 0.9, stats_text, fontsize=11, verticalalignment='top',
                fontfamily='monospace', bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.3))

        plt.suptitle('Keyword Trends Over Time', fontsize=18, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.output_dir / '04_trending_keywords.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 04_trending_keywords.png")

    def _viz_05_diversity_metrics(self):
        """Calculate and visualize keyword diversity."""
        logger.info("Creating Visualization 5: Diversity Metrics")

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))

        # 1. Shannon entropy (overall diversity)
        ax = axes[0, 0]

        # Calculate Shannon entropy
        total_kw = sum(self.all_keywords.values())
        proportions = np.array([count/total_kw for count in self.all_keywords.values()])
        shannon_entropy = -np.sum(proportions * np.log2(proportions + 1e-10))
        max_entropy = np.log2(len(self.all_keywords))
        diversity_score = (shannon_entropy / max_entropy) * 100

        # Visualization
        ax.bar(['Observed\nDiversity', 'Maximum\nPossible'],
               [diversity_score, 100],
               color=['#3498DB', '#95A5A6'],
               edgecolor='white', width=0.5)
        ax.set_ylabel('Diversity Score (%)', fontsize=12)
        ax.set_title('Keyword Diversity (Shannon Entropy)', fontsize=12, fontweight='bold')
        ax.set_ylim(0, 110)

        # Add value labels
        for i, v in enumerate([diversity_score, 100]):
            ax.text(i, v + 2, f'{v:.1f}%', ha='center', fontsize=12, fontweight='bold')

        # 2. Gini coefficient (concentration)
        ax = axes[0, 1]

        sorted_counts = np.sort(list(self.all_keywords.values()))
        n = len(sorted_counts)
        gini = (2 * sum((i+1) * sorted_counts[i] for i in range(n))) / (n * sum(sorted_counts)) - (n+1)/n
        concentration_pct = gini * 100

        ax.bar(['Concentration\n(Gini)', 'Perfect\nEquality'],
               [concentration_pct, 0],
               color=['#E74C3C', '#2ECC71'],
               edgecolor='white', width=0.5)
        ax.set_ylabel('Concentration (%)', fontsize=12)
        ax.set_title('Keyword Concentration (Gini Coefficient)', fontsize=12, fontweight='bold')
        ax.set_ylim(0, 110)

        for i, v in enumerate([concentration_pct, 0]):
            ax.text(i, v + 2, f'{v:.1f}%', ha='center', fontsize=12, fontweight='bold')

        # 3. Lorenz curve
        ax = axes[1, 0]

        sorted_counts = np.sort(list(self.all_keywords.values()))
        cumsum = np.cumsum(sorted_counts)
        cumsum_pct = cumsum / cumsum[-1] * 100
        x_pct = np.arange(1, len(sorted_counts)+1) / len(sorted_counts) * 100

        ax.plot(x_pct, cumsum_pct, linewidth=3, color='#3498DB', label='Lorenz Curve')
        ax.plot([0, 100], [0, 100], 'r--', linewidth=2, alpha=0.5, label='Perfect Equality')
        ax.fill_between(x_pct, cumsum_pct, x_pct, alpha=0.3, color='#E74C3C', label='Inequality Area')
        ax.set_xlabel('Cumulative % of Keywords', fontsize=11)
        ax.set_ylabel('Cumulative % of Occurrences', fontsize=11)
        ax.set_title('Lorenz Curve (Keyword Distribution)', fontsize=12, fontweight='bold')
        ax.legend()
        ax.grid(True, alpha=0.3)

        # 4. Genre diversity comparison
        ax = axes[1, 1]

        genre_diversity = {}
        for genre, keywords in self.genre_keywords.items():
            if len(keywords) > 10:  # Enough data
                genre_diversity[genre] = len(keywords)

        if genre_diversity:
            sorted_genres = sorted(genre_diversity.items(), key=lambda x: x[1], reverse=True)[:10]
            genres, diversities = zip(*sorted_genres)

            colors_grad = plt.cm.viridis(np.linspace(0.2, 0.8, len(genres)))
            bars = ax.bar(range(len(genres)), diversities, color=colors_grad, edgecolor='white')
            ax.set_xticks(range(len(genres)))
            ax.set_xticklabels(genres, rotation=45, ha='right', fontsize=10)
            ax.set_ylabel('Unique Keywords', fontsize=11)
            ax.set_title('Keyword Diversity by Genre', fontsize=12, fontweight='bold')
            ax.grid(True, alpha=0.3, axis='y')

        plt.suptitle('Keyword Diversity & Distribution Metrics', fontsize=18, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.output_dir / '05_diversity_metrics.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 05_diversity_metrics.png")

        self.stats['shannon_entropy'] = shannon_entropy
        self.stats['diversity_score'] = diversity_score
        self.stats['gini_coefficient'] = gini

    def _viz_06_emotional_mapping(self):
        """Map keywords to emotional categories."""
        logger.info("Creating Visualization 6: Emotional Keyword Mapping")

        # Count emotional keywords
        emotional_counts = {
            'positive': 0,
            'negative': 0,
            'neutral': 0
        }

        emotional_ratings = {
            'positive': [],
            'negative': [],
            'neutral': []
        }

        for kw, count in self.all_keywords.items():
            for emotion, emotion_kws in self.emotional_keywords.items():
                if kw in emotion_kws:
                    emotional_counts[emotion] += count
                    if kw in self.keyword_ratings:
                        emotional_ratings[emotion].extend(self.keyword_ratings[kw])

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))

        # 1. Emotional distribution pie
        ax = axes[0, 0]
        colors = ['#2ECC71', '#E74C3C', '#95A5A6']
        ax.pie(emotional_counts.values(), labels=emotional_counts.keys(), autopct='%1.1f%%',
               colors=colors, startangle=90, textprops={'fontsize': 12})
        ax.set_title('Emotional Keyword Distribution', fontsize=12, fontweight='bold')

        # 2. Emotional ratings comparison
        ax = axes[0, 1]
        emotions = ['Positive', 'Negative', 'Neutral']
        avg_ratings = [
            np.mean(emotional_ratings['positive']) if emotional_ratings['positive'] else 0,
            np.mean(emotional_ratings['negative']) if emotional_ratings['negative'] else 0,
            np.mean(emotional_ratings['neutral']) if emotional_ratings['neutral'] else 0
        ]

        bars = ax.bar(emotions, avg_ratings, color=colors, edgecolor='white')
        ax.set_ylabel('Average IMDb Rating', fontsize=11)
        ax.set_title('Average Rating by Emotional Tone', fontsize=12, fontweight='bold')
        ax.grid(True, alpha=0.3, axis='y')

        # Add value labels
        for bar, val in zip(bars, avg_ratings):
            ax.text(bar.get_x() + bar.get_width()/2, val + 0.1,
                   f'{val:.2f}', ha='center', fontsize=11, fontweight='bold')

        # 3. Top positive keywords
        ax = axes[1, 0]
        positive_kws = [(kw, self.all_keywords[kw]) for kw in self.emotional_keywords['positive']
                        if kw in self.all_keywords]
        if positive_kws:
            positive_kws = sorted(positive_kws, key=lambda x: x[1], reverse=True)[:12]
            keywords, counts = zip(*positive_kws)
            y_pos = np.arange(len(keywords))
            ax.barh(y_pos, counts, color='#2ECC71', edgecolor='white', alpha=0.8)
            ax.set_yticks(y_pos)
            ax.set_yticklabels(keywords, fontsize=9)
            ax.invert_yaxis()
            ax.set_xlabel('Frequency', fontsize=11)
            ax.set_title('Top Positive Keywords', fontsize=12, fontweight='bold')

        # 4. Top negative keywords
        ax = axes[1, 1]
        negative_kws = [(kw, self.all_keywords[kw]) for kw in self.emotional_keywords['negative']
                        if kw in self.all_keywords]
        if negative_kws:
            negative_kws = sorted(negative_kws, key=lambda x: x[1], reverse=True)[:12]
            keywords, counts = zip(*negative_kws)
            y_pos = np.arange(len(keywords))
            ax.barh(y_pos, counts, color='#E74C3C', edgecolor='white', alpha=0.8)
            ax.set_yticks(y_pos)
            ax.set_yticklabels(keywords, fontsize=9)
            ax.invert_yaxis()
            ax.set_xlabel('Frequency', fontsize=11)
            ax.set_title('Top Negative Keywords', fontsize=12, fontweight='bold')

        plt.suptitle('Emotional Keyword Analysis', fontsize=18, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.output_dir / '06_emotional_mapping.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 06_emotional_mapping.png")

    def _viz_07_cross_genre_analysis(self):
        """Analyze keywords across multiple genres."""
        logger.info("Creating Visualization 7: Cross-Genre Keyword Analysis")

        # Calculate versatility scores
        versatile_keywords = {}
        exclusive_keywords = defaultdict(list)

        for kw, stats in self.keyword_stats.items():
            genre_count = len(stats['genres'])
            if stats['count'] >= 10:  # Minimum threshold
                versatile_keywords[kw] = {
                    'genre_count': genre_count,
                    'total_count': stats['count'],
                    'versatility': genre_count * stats['count']
                }

            # Genre-exclusive (appears in only one genre)
            if genre_count == 1 and stats['count'] >= 5:
                genre = list(stats['genres'])[0]
                exclusive_keywords[genre].append((kw, stats['count']))

        # Sort
        most_versatile = sorted(versatile_keywords.items(),
                               key=lambda x: x[1]['versatility'],
                               reverse=True)[:20]

        fig, axes = plt.subplots(2, 2, figsize=(18, 14))

        # 1. Most versatile keywords
        ax = axes[0, 0]
        if most_versatile:
            keywords, stats_dict = zip(*most_versatile)
            genre_counts = [s['genre_count'] for s in stats_dict]
            y_pos = np.arange(len(keywords))
            colors_grad = plt.cm.viridis(np.linspace(0.2, 0.8, len(keywords)))
            ax.barh(y_pos, genre_counts, color=colors_grad, edgecolor='white')
            ax.set_yticks(y_pos)
            ax.set_yticklabels(keywords, fontsize=9)
            ax.invert_yaxis()
            ax.set_xlabel('Number of Genres', fontsize=11)
            ax.set_title('Top 20 Most Versatile Keywords', fontsize=12, fontweight='bold')

        # 2. Versatility distribution
        ax = axes[0, 1]
        genre_counts_all = [s['genre_count'] for s in versatile_keywords.values()]
        ax.hist(genre_counts_all, bins=range(1, max(genre_counts_all)+2),
               color='#9B59B6', edgecolor='white', alpha=0.8)
        ax.set_xlabel('Number of Genres', fontsize=11)
        ax.set_ylabel('Number of Keywords', fontsize=11)
        ax.set_title('Keyword Versatility Distribution', fontsize=12, fontweight='bold')
        ax.grid(True, alpha=0.3, axis='y')

        # 3. Genre-exclusive keywords (Drama)
        ax = axes[1, 0]
        if 'Drama' in exclusive_keywords:
            drama_exclusive = sorted(exclusive_keywords['Drama'], key=lambda x: x[1], reverse=True)[:15]
            keywords, counts = zip(*drama_exclusive)
            y_pos = np.arange(len(keywords))
            ax.barh(y_pos, counts, color='#9B59B6', edgecolor='white', alpha=0.8)
            ax.set_yticks(y_pos)
            ax.set_yticklabels(keywords, fontsize=9)
            ax.invert_yaxis()
            ax.set_xlabel('Frequency', fontsize=11)
            ax.set_title('Drama-Exclusive Keywords', fontsize=12, fontweight='bold')

        # 4. Genre-exclusive keywords (Action)
        ax = axes[1, 1]
        if 'Action' in exclusive_keywords:
            action_exclusive = sorted(exclusive_keywords['Action'], key=lambda x: x[1], reverse=True)[:15]
            keywords, counts = zip(*action_exclusive)
            y_pos = np.arange(len(keywords))
            ax.barh(y_pos, counts, color='#E74C3C', edgecolor='white', alpha=0.8)
            ax.set_yticks(y_pos)
            ax.set_yticklabels(keywords, fontsize=9)
            ax.invert_yaxis()
            ax.set_xlabel('Frequency', fontsize=11)
            ax.set_title('Action-Exclusive Keywords', fontsize=12, fontweight='bold')

        plt.suptitle('Cross-Genre Keyword Analysis', fontsize=18, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.output_dir / '07_cross_genre_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 07_cross_genre_analysis.png")

    def _viz_08_era_signatures(self):
        """Identify era-specific keyword signatures."""
        logger.info("Creating Visualization 8: Era Signatures")

        # Define eras
        eras = {
            '1970s': (1970, 1979),
            '1980s': (1980, 1989),
            '1990s': (1990, 1999),
            '2000s': (2000, 2009),
            '2010s': (2010, 2019),
            '2020s': (2020, 2029)
        }

        era_keywords = defaultdict(Counter)

        # Count keywords per era
        for movie in self.movies:
            tmdb_id = str(movie.get('tmdb_id') or movie.get('TMDB_ID') or '')
            if tmdb_id not in self.keywords_cache:
                continue

            year = int(movie.get('release_year') or movie.get('Year') or 2000)
            keywords = self.keywords_cache[tmdb_id].get('keywords', [])

            for era_name, (start, end) in eras.items():
                if start <= year <= end:
                    for kw in keywords:
                        era_keywords[era_name][kw.lower()] += 1

        fig, axes = plt.subplots(3, 2, figsize=(18, 18))
        axes = axes.flatten()

        for idx, (era_name, (start, end)) in enumerate(eras.items()):
            ax = axes[idx]

            if era_name in era_keywords and era_keywords[era_name]:
                top_kws = era_keywords[era_name].most_common(15)
                keywords, counts = zip(*top_kws)

                y_pos = np.arange(len(keywords))

                # Color by era
                colors_map = {
                    '1970s': '#8B4513',
                    '1980s': '#FF1493',
                    '1990s': '#00CED1',
                    '2000s': '#FFD700',
                    '2010s': '#00FF00',
                    '2020s': '#9400D3'
                }
                color = colors_map.get(era_name, '#3498DB')

                ax.barh(y_pos, counts, color=color, edgecolor='white', alpha=0.8)
                ax.set_yticks(y_pos)
                ax.set_yticklabels(keywords, fontsize=8)
                ax.invert_yaxis()
                ax.set_xlabel('Frequency', fontsize=10)
                ax.set_title(f'{era_name} Signature Keywords', fontsize=11, fontweight='bold')
            else:
                ax.text(0.5, 0.5, 'No data', ha='center', va='center', fontsize=12)
                ax.axis('off')

        plt.suptitle('Era-Specific Keyword Signatures', fontsize=18, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.output_dir / '08_era_signatures.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 08_era_signatures.png")

    def _viz_09_keyword_lifecycle(self):
        """Analyze keyword lifecycle (birth, peak, decline)."""
        logger.info("Creating Visualization 9: Keyword Lifecycle")

        # Identify keywords with interesting lifecycles
        lifecycle_keywords = {}

        for kw, stats in self.keyword_stats.items():
            if stats['count'] < 15 or len(stats['decades']) < 3:
                continue

            lifecycle_keywords[kw] = {
                'first_year': stats['first_seen'],
                'last_year': stats['last_seen'],
                'span': stats['decade_span'],
                'count': stats['count']
            }

        # Sort by span and count
        sorted_lifecycle = sorted(lifecycle_keywords.items(),
                                 key=lambda x: (x[1]['span'], x[1]['count']),
                                 reverse=True)[:30]

        fig, axes = plt.subplots(2, 1, figsize=(16, 12))

        # 1. Timeline visualization
        ax = axes[0]

        if sorted_lifecycle:
            for idx, (kw, stats) in enumerate(sorted_lifecycle[:20]):
                start = stats['first_year']
                end = stats['last_year']

                ax.plot([start, end], [idx, idx], linewidth=3, marker='o', markersize=6)
                ax.text(start - 2, idx, kw, ha='right', fontsize=8)

            ax.set_xlabel('Year', fontsize=12)
            ax.set_ylabel('Keyword', fontsize=12)
            ax.set_title('Keyword Lifecycle Timeline (Top 20 Longest-Spanning)',
                        fontsize=14, fontweight='bold')
            ax.set_yticks([])
            ax.grid(True, alpha=0.3, axis='x')

        # 2. Lifecycle span distribution
        ax = axes[1]
        spans = [stats['span'] for _, stats in lifecycle_keywords.items()]
        ax.hist(spans, bins=30, color='#3498DB', edgecolor='white', alpha=0.8)
        ax.set_xlabel('Lifecycle Span (Years)', fontsize=12)
        ax.set_ylabel('Number of Keywords', fontsize=12)
        ax.set_title('Keyword Lifecycle Span Distribution', fontsize=14, fontweight='bold')
        ax.axvline(np.median(spans), color='red', linestyle='--', linewidth=2,
                   label=f'Median: {np.median(spans):.0f} years')
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')

        plt.tight_layout()
        plt.savefig(self.output_dir / '09_keyword_lifecycle.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 09_keyword_lifecycle.png")

    def _viz_10_quality_correlation(self):
        """Deep dive into keyword-quality correlations."""
        logger.info("Creating Visualization 10: Quality Correlation Matrix")

        # Select interesting keywords for correlation analysis
        interesting_keywords = [
            'based on novel', 'murder', 'love', 'revenge', 'friendship',
            'based on true story', 'superhero', 'sequel', 'based on comic',
            'dystopia', 'coming of age', 'black and white', 'redemption',
            'betrayal', 'mentor', 'haunted house', 'heist', 'ensemble cast'
        ]

        # Build correlation matrix
        correlation_data = []
        for kw in interesting_keywords:
            if kw in self.keyword_ratings and len(self.keyword_ratings[kw]) >= 10:
                correlation_data.append({
                    'keyword': kw,
                    'avg_rating': np.mean(self.keyword_ratings[kw]),
                    'count': len(self.keyword_ratings[kw]),
                    'std': np.std(self.keyword_ratings[kw])
                })

        if not correlation_data:
            logger.warning("Not enough data for quality correlation")
            return

        df_corr = pd.DataFrame(correlation_data)
        df_corr = df_corr.sort_values('avg_rating', ascending=False)

        fig, axes = plt.subplots(2, 2, figsize=(18, 14))

        # 1. Rating comparison
        ax = axes[0, 0]
        keywords = df_corr['keyword'].tolist()
        ratings = df_corr['avg_rating'].tolist()
        y_pos = np.arange(len(keywords))

        # Color gradient
        colors = plt.cm.RdYlGn(np.linspace(0.3, 0.9, len(keywords)))

        ax.barh(y_pos, ratings, color=colors, edgecolor='white')
        ax.set_yticks(y_pos)
        ax.set_yticklabels(keywords, fontsize=9)
        ax.invert_yaxis()
        ax.set_xlabel('Average IMDb Rating', fontsize=11)
        ax.set_title('Keyword Quality Ranking', fontsize=12, fontweight='bold')

        # Overall mean line
        overall_mean = np.mean([r for ratings_list in self.keyword_ratings.values() for r in ratings_list])
        ax.axvline(overall_mean, color='red', linestyle='--', linewidth=2,
                   label=f'Overall Mean: {overall_mean:.2f}')
        ax.legend()

        # 2. Rating vs Frequency scatter
        ax = axes[0, 1]
        ax.scatter(df_corr['count'], df_corr['avg_rating'],
                  s=100, alpha=0.6, c=df_corr['avg_rating'],
                  cmap='RdYlGn', edgecolors='white')
        ax.set_xlabel('Keyword Frequency', fontsize=11)
        ax.set_ylabel('Average Rating', fontsize=11)
        ax.set_title('Frequency vs Quality', fontsize=12, fontweight='bold')
        ax.grid(True, alpha=0.3)

        # Add labels for notable keywords
        for _, row in df_corr.head(5).iterrows():
            ax.annotate(row['keyword'], (row['count'], row['avg_rating']),
                       fontsize=8, alpha=0.7)

        # 3. Rating standard deviation
        ax = axes[1, 0]
        keywords = df_corr.nsmallest(15, 'std')['keyword'].tolist()
        stds = df_corr.nsmallest(15, 'std')['std'].tolist()
        y_pos = np.arange(len(keywords))

        ax.barh(y_pos, stds, color='#3498DB', edgecolor='white', alpha=0.8)
        ax.set_yticks(y_pos)
        ax.set_yticklabels(keywords, fontsize=9)
        ax.invert_yaxis()
        ax.set_xlabel('Rating Standard Deviation', fontsize=11)
        ax.set_title('Most Consistent Keywords (Low Variance)', fontsize=12, fontweight='bold')

        # 4. Statistics summary
        ax = axes[1, 1]
        ax.axis('off')

        top_quality = df_corr.nlargest(5, 'avg_rating')
        bottom_quality = df_corr.nsmallest(5, 'avg_rating')

        stats_text = f"""
QUALITY CORRELATION SUMMARY

Keywords Analyzed: {len(df_corr)}

Highest Quality Predictors:
  1. {top_quality.iloc[0]['keyword']}: {top_quality.iloc[0]['avg_rating']:.2f}
  2. {top_quality.iloc[1]['keyword']}: {top_quality.iloc[1]['avg_rating']:.2f}
  3. {top_quality.iloc[2]['keyword']}: {top_quality.iloc[2]['avg_rating']:.2f}

Lowest Quality Predictors:
  1. {bottom_quality.iloc[0]['keyword']}: {bottom_quality.iloc[0]['avg_rating']:.2f}
  2. {bottom_quality.iloc[1]['keyword']}: {bottom_quality.iloc[1]['avg_rating']:.2f}
  3. {bottom_quality.iloc[2]['keyword']}: {bottom_quality.iloc[2]['avg_rating']:.2f}

Most Consistent:
  {df_corr.nsmallest(1, 'std').iloc[0]['keyword']}
  (std: {df_corr.nsmallest(1, 'std').iloc[0]['std']:.2f})

Overall Mean Rating: {overall_mean:.2f}
"""
        ax.text(0.1, 0.9, stats_text, fontsize=10, verticalalignment='top',
                fontfamily='monospace', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

        plt.suptitle('Keyword-Quality Correlation Deep-Dive', fontsize=18, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.output_dir / '10_quality_correlation.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 10_quality_correlation.png")

    def _generate_report(self):
        """Generate comprehensive text report."""
        logger.info("Generating comprehensive report...")

        report_file = self.report_dir / 'batch_26_keyword_deepdive_report.txt'

        with open(report_file, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("CINESCOPE BATCH 26: KEYWORD DEEP-DIVE ANALYSIS\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # Overview
            f.write("=" * 80 + "\n")
            f.write("OVERVIEW STATISTICS\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Total Unique Keywords: {len(self.all_keywords):,}\n")
            f.write(f"Total Keyword Occurrences: {sum(self.all_keywords.values()):,}\n")
            f.write(f"Movies with Keywords: {len(self.keywords_cache):,}\n\n")

            # Diversity metrics
            if 'shannon_entropy' in self.stats:
                f.write("=" * 80 + "\n")
                f.write("DIVERSITY METRICS\n")
                f.write("=" * 80 + "\n\n")
                f.write(f"Shannon Entropy: {self.stats['shannon_entropy']:.2f}\n")
                f.write(f"Diversity Score: {self.stats['diversity_score']:.1f}%\n")
                f.write(f"Gini Coefficient: {self.stats['gini_coefficient']:.3f}\n\n")

            # Top keywords by quality
            f.write("=" * 80 + "\n")
            f.write("TOP 30 KEYWORDS BY AVERAGE RATING\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"{'Rank':<6}{'Keyword':<35}{'Avg Rating':<12}{'Count':<10}\n")
            f.write("-" * 80 + "\n")

            quality_ranked = sorted(
                [(kw, stats['avg_rating'], stats['count'])
                 for kw, stats in self.keyword_stats.items() if stats['count'] >= 15],
                key=lambda x: x[1],
                reverse=True
            )[:30]

            for i, (kw, rating, count) in enumerate(quality_ranked, 1):
                f.write(f"{i:<6}{kw:<35}{rating:<12.2f}{count:<10}\n")

            # Most versatile keywords
            f.write("\n" + "=" * 80 + "\n")
            f.write("TOP 30 MOST VERSATILE KEYWORDS (Cross-Genre)\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"{'Rank':<6}{'Keyword':<35}{'Genres':<10}{'Count':<10}\n")
            f.write("-" * 80 + "\n")

            versatile_ranked = sorted(
                [(kw, stats['genre_diversity'], stats['count'])
                 for kw, stats in self.keyword_stats.items() if stats['count'] >= 10],
                key=lambda x: (x[1], x[2]),
                reverse=True
            )[:30]

            for i, (kw, genres, count) in enumerate(versatile_ranked, 1):
                f.write(f"{i:<6}{kw:<35}{genres:<10}{count:<10}\n")

            # Rare keywords
            f.write("\n" + "=" * 80 + "\n")
            f.write("TOP 30 RAREST KEYWORDS (Ultra-Unique)\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"{'Rank':<6}{'Keyword':<50}{'Count':<10}\n")
            f.write("-" * 80 + "\n")

            rare_ranked = sorted(
                [(kw, count) for kw, count in self.all_keywords.items() if count <= 3],
                key=lambda x: x[1]
            )[:30]

            for i, (kw, count) in enumerate(rare_ranked, 1):
                f.write(f"{i:<6}{kw:<50}{count:<10}\n")

        logger.info(f"✓ Report saved: {report_file}")


def main():
    analyzer = KeywordDeepDiveAnalyzer()
    analyzer.analyze()


if __name__ == '__main__':
    main()
