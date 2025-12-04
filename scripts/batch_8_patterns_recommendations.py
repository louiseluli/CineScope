"""
CineScope Batch 8: PATTERN DISCOVERY & RECOMMENDATIONS
=======================================================

Advanced pattern analysis and content-based recommendations
covering Questions 281-320:

VIEWING PATTERNS (Q281-290):
- Q281: What are my dominant viewing patterns?
- Q282: Which combinations of features do I prefer?
- Q283: Do I have genre-decade preferences?
- Q284: Which actor-director combinations do I watch?
- Q285: Do I prefer certain runtime-genre combinations?
- Q286: What are my collection "clusters"?
- Q287: Which patterns predict quality for me?
- Q288: Do I watch similar groups of films?
- Q289: What are my "signature" preferences?
- Q290: Which patterns are unique to my collection?

SIMILARITY ANALYSIS (Q291-300):
- Q291: Which films are most similar in my collection?
- Q292: Can I find film clusters?
- Q293: Which films share the most features?
- Q294: What are natural groupings in my collection?
- Q295: Which films are outliers?
- Q296: Do quality films cluster together?
- Q297: Which genres cluster with which eras?
- Q298: Do certain actors/directors cluster?
- Q299: What are the strongest feature correlations?
- Q300: Which combinations are overrepresented?

RECOMMENDATIONS (Q301-320):
- Q301: Based on patterns, what genres/eras should I explore?
- Q302: Which patterns am I underrepresented in?
- Q303: Which high-quality actors/directors do I barely know?
- Q304: Which directors am I missing from?
- Q305: Which genre-era combinations should I try?
- Q306: What gaps exist in my collection?
- Q307: Which patterns would diversify my collection?
- Q308: What are gaps in my viewing history?
- Q309: Which successful patterns am I ignoring?
- Q310: What would expand my horizons?
- Q311-320: Collection expansion strategies

12 Professional Visualizations
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from pathlib import Path
import logging
from collections import Counter, defaultdict
from itertools import combinations
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity
from scipy.cluster.hierarchy import dendrogram, linkage
import sys
import warnings
warnings.filterwarnings('ignore')

# Setup
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "processed"
OUTPUT_DIR = BASE_DIR / "analysis_outputs" / "visualizations" / "batch_8"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

sys.path.insert(0, str(BASE_DIR))
from src.core.helpers import parse_genres, parse_directors, explode_genres

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Professional color palette
COLORS = {
    'primary': '#2C3E50',
    'secondary': '#E74C3C',
    'accent': '#3498DB',
    'success': '#27AE60',
    'warning': '#F39C12',
    'info': '#9B59B6',
    'teal': '#1ABC9C',
    'gradient': ['#3498DB', '#9B59B6', '#E74C3C', '#F39C12', '#27AE60', '#1ABC9C']
}

plt.rcParams['figure.dpi'] = 300
plt.rcParams['savefig.dpi'] = 300
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica']


class PatternAnalyzer:
    """Discover patterns and generate recommendations."""
    
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self._prepare_features()
        logger.info(f"Analyzing patterns in {len(self.df)} films")
        
    def _prepare_features(self):
        """Prepare feature data for analysis."""
        # Ensure decade
        if 'decade' not in self.df.columns and 'year' in self.df.columns:
            self.df['decade'] = (self.df['year'] // 10) * 10
        
        # Parse genres properly using helpers
        if 'genres' in self.df.columns:
            genre_df = explode_genres(
                self.df, 
                genres_col='genres', 
                id_col='const',
                keep_cols=['imdb_rating', 'runtime_mins', 'year', 'title', 'decade']
            )
            self.genre_data = genre_df
        else:
            self.genre_data = pd.DataFrame()
        
        # Extract primary genre for each film using proper parsing
        self.df['primary_genre'] = None
        self.df['genre_count'] = 0
        
        for idx, row in self.df.iterrows():
            genres_str = row.get('genres', '')
            if pd.notna(genres_str):
                genres = parse_genres(genres_str)  # Use proper helper
                if genres:
                    self.df.at[idx, 'primary_genre'] = genres[0]
                    self.df.at[idx, 'genre_count'] = len(genres)
        
        # Runtime categories
        self.df['runtime_category'] = pd.cut(
            self.df['runtime_mins'],
            bins=[0, 90, 120, 150, 999],
            labels=['Short', 'Standard', 'Long', 'Epic']
        )
        
        # Rating tiers
        self.df['rating_tier'] = pd.cut(
            self.df['imdb_rating'],
            bins=[0, 6, 7, 8, 10],
            labels=['Mixed', 'Good', 'Great', 'Masterpiece']
        )
    
    def get_feature_combinations(self):
        """Analyze common feature combinations."""
        combos = []
        
        for _, film in self.df.iterrows():
            combo = {
                'decade': film.get('decade'),
                'primary_genre': film.get('primary_genre'),
                'runtime_cat': film.get('runtime_category'),
                'rating_tier': film.get('rating_tier'),
                'rating': film.get('imdb_rating')
            }
            if all(pd.notna(v) for v in combo.values() if v != 'rating'):
                combos.append(combo)
        
        return pd.DataFrame(combos)
    
    def calculate_film_similarity(self):
        """Calculate film-to-film similarity matrix."""
        # Create feature matrix
        features = []
        film_ids = []
        
        for idx, film in self.df.iterrows():
            # Genre one-hot encoding
            genres = []
            genres_str = film.get('genres', '')
            if pd.notna(genres_str):
                genres = [g.strip() for g in str(genres_str).split(',') if g.strip()]
            
            # Features: decade, rating, runtime, genre presence
            decade_norm = (film['decade'] - 1900) / 100 if pd.notna(film.get('decade')) else 0
            rating_norm = film['imdb_rating'] / 10 if pd.notna(film.get('imdb_rating')) else 0
            runtime_norm = film['runtime_mins'] / 200 if pd.notna(film.get('runtime_mins')) else 0
            
            feature_vec = [decade_norm, rating_norm, runtime_norm]
            features.append(feature_vec)
            film_ids.append(film.get('const', f'film_{idx}'))
        
        features_array = np.array(features)
        similarity_matrix = cosine_similarity(features_array)
        
        return similarity_matrix, film_ids


# =============================================================================
# VISUALIZATIONS
# =============================================================================

def viz_1_pattern_overview(analyzer: PatternAnalyzer):
    """Overview of dominant patterns in collection."""
    logger.info("Creating Viz 1: Pattern Overview...")
    
    combos = analyzer.get_feature_combinations()
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Genre-Decade heatmap
    if 'primary_genre' in combos.columns and 'decade' in combos.columns:
        pivot = pd.crosstab(combos['decade'], combos['primary_genre'])
        top_genres = pivot.sum().nlargest(8).index
        pivot = pivot[top_genres]
        
        im = ax1.imshow(pivot.values, cmap='YlOrRd', aspect='auto')
        ax1.set_xticks(range(len(pivot.columns)))
        ax1.set_yticks(range(len(pivot.index)))
        ax1.set_xticklabels(pivot.columns, rotation=45, ha='right')
        ax1.set_yticklabels([f"{int(d)}s" for d in pivot.index])
        ax1.set_title('Genre × Decade Pattern', fontsize=12, fontweight='bold', pad=15)
        plt.colorbar(im, ax=ax1, label='Count')
    
    # 2. Runtime-Rating distribution
    if 'runtime_cat' in combos.columns and 'rating_tier' in combos.columns:
        runtime_rating = pd.crosstab(combos['runtime_cat'], combos['rating_tier'])
        
        runtime_rating.plot(kind='bar', stacked=True, ax=ax2,
                           color=COLORS['gradient'][:len(runtime_rating.columns)],
                           alpha=0.8, edgecolor='black')
        ax2.set_xlabel('Runtime Category', fontsize=11, fontweight='bold')
        ax2.set_ylabel('Count', fontsize=11, fontweight='bold')
        ax2.set_title('Runtime × Quality Pattern', fontsize=12, fontweight='bold', pad=15)
        ax2.legend(title='Quality', bbox_to_anchor=(1.05, 1))
        ax2.grid(axis='y', alpha=0.3)
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=0)
    
    # 3. Top combinations
    combo_counts = combos.groupby(['primary_genre', 'decade']).size().nlargest(15)
    
    y_pos = range(len(combo_counts))
    ax3.barh(y_pos, combo_counts.values, color=COLORS['accent'], alpha=0.8, edgecolor='black')
    
    labels = [f"{idx[0]} ({int(idx[1])}s)" for idx in combo_counts.index]
    ax3.set_yticks(y_pos)
    ax3.set_yticklabels(labels, fontsize=9)
    ax3.set_xlabel('Film Count', fontsize=11, fontweight='bold')
    ax3.set_title('Top 15 Genre-Decade Combinations', fontsize=12, fontweight='bold', pad=15)
    ax3.grid(axis='x', alpha=0.3)
    
    # 4. Quality by pattern
    quality_by_genre = combos.groupby('primary_genre')['rating'].mean().nlargest(10)
    
    ax4.barh(quality_by_genre.index, quality_by_genre.values,
            color=COLORS['success'], alpha=0.8, edgecolor='black')
    for i, (genre, rating) in enumerate(zip(quality_by_genre.index, quality_by_genre.values)):
        ax4.text(rating + 0.05, i, f'{rating:.2f}', va='center', fontsize=9)
    
    ax4.set_xlabel('Average Rating', fontsize=11, fontweight='bold')
    ax4.set_title('Average Quality by Genre', fontsize=12, fontweight='bold', pad=15)
    ax4.set_xlim(0, 10)
    ax4.axvline(x=7.0, color='red', linestyle='--', alpha=0.5)
    ax4.grid(axis='x', alpha=0.3)
    
    plt.suptitle('Collection Pattern Overview', fontsize=15, fontweight='bold', y=0.98)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "01_pattern_overview.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 1 complete")


def viz_2_actor_director_patterns(analyzer: PatternAnalyzer):
    """Actor and director collaboration patterns."""
    logger.info("Creating Viz 2: Actor-Director Patterns...")
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    # 1. Top directors by film count
    director_counts = Counter()
    for _, film in analyzer.df.iterrows():
        directors_str = film.get('directors', '')
        if pd.notna(directors_str):
            directors = parse_directors(directors_str)  # Use proper helper
            director_counts.update(directors)
    
    top_directors = dict(director_counts.most_common(15))
    
    ax1.barh(list(top_directors.keys()), list(top_directors.values()),
            color=COLORS['accent'], alpha=0.8, edgecolor='black')
    ax1.set_xlabel('Film Count', fontsize=11, fontweight='bold')
    ax1.set_title('Top 15 Directors in Collection', fontsize=12, fontweight='bold', pad=15)
    ax1.grid(axis='x', alpha=0.3)
    
    # 2. Director quality
    director_ratings = defaultdict(list)
    for _, film in analyzer.df.iterrows():
        directors_str = film.get('directors', '')
        rating = film.get('imdb_rating')
        if pd.notna(directors_str) and pd.notna(rating):
            directors = parse_directors(directors_str)  # Use proper helper
            for director in directors:
                director_ratings[director].append(rating)
    
    director_avg = {d: np.mean(ratings) for d, ratings in director_ratings.items() 
                   if len(ratings) >= 3}
    top_quality_directors = dict(sorted(director_avg.items(), 
                                       key=lambda x: x[1], reverse=True)[:15])
    
    ax2.barh(list(top_quality_directors.keys()), list(top_quality_directors.values()),
            color=COLORS['success'], alpha=0.8, edgecolor='black')
    for i, (director, rating) in enumerate(top_quality_directors.items()):
        ax2.text(rating + 0.05, i, f'{rating:.2f}', va='center', fontsize=9)
    
    ax2.set_xlabel('Average Rating', fontsize=11, fontweight='bold')
    ax2.set_title('Highest Quality Directors (3+ films)', fontsize=12, fontweight='bold', pad=15)
    ax2.set_xlim(0, 10)
    ax2.axvline(x=7.0, color='red', linestyle='--', alpha=0.5)
    ax2.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "02_actor_director_patterns.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 2 complete")


def viz_3_similarity_clusters(analyzer: PatternAnalyzer):
    """Film similarity and clustering."""
    logger.info("Creating Viz 3: Similarity Clusters...")
    
    # Get similarity matrix
    sim_matrix, film_ids = analyzer.calculate_film_similarity()
    
    # Sample for visualization (too many films to show all)
    n_sample = min(50, len(film_ids))
    sample_indices = np.random.choice(len(film_ids), n_sample, replace=False)
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    # 1. Similarity heatmap (sample)
    sim_sample = sim_matrix[sample_indices][:, sample_indices]
    
    im = ax1.imshow(sim_sample, cmap='RdYlGn', vmin=0, vmax=1, aspect='auto')
    ax1.set_title(f'Film Similarity Matrix (Random {n_sample} Films)',
                 fontsize=12, fontweight='bold', pad=15)
    plt.colorbar(im, ax=ax1, label='Similarity')
    
    # 2. Similarity distribution
    # Get upper triangle (avoid diagonal and duplicates)
    triu_indices = np.triu_indices_from(sim_matrix, k=1)
    similarities = sim_matrix[triu_indices]
    
    ax2.hist(similarities, bins=50, color=COLORS['accent'], alpha=0.7, edgecolor='black')
    ax2.axvline(np.mean(similarities), color='red', linestyle='--', linewidth=2,
               label=f'Mean: {np.mean(similarities):.3f}')
    ax2.axvline(np.median(similarities), color='orange', linestyle='--', linewidth=2,
               label=f'Median: {np.median(similarities):.3f}')
    
    ax2.set_xlabel('Similarity Score', fontsize=11, fontweight='bold')
    ax2.set_ylabel('Frequency', fontsize=11, fontweight='bold')
    ax2.set_title('Film-to-Film Similarity Distribution', fontsize=12, fontweight='bold', pad=15)
    ax2.legend()
    ax2.grid(alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "03_similarity_clusters.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 3 complete")


def viz_4_feature_correlations(analyzer: PatternAnalyzer):
    """Feature correlation analysis - fixed to avoid NaN."""
    logger.info("Creating Viz 4: Feature Correlations...")
    
    # Build correlation matrix with proper handling
    corr_features = []
    
    for _, film in analyzer.df.iterrows():
        # Basic numeric features
        decade = film.get('decade', 2000)
        rating = film.get('imdb_rating', 0)
        runtime = film.get('runtime_mins', 0)
        genre_count = film.get('genre_count', 0)
        
        # Skip if missing critical data
        if pd.isna(decade) or pd.isna(rating) or pd.isna(runtime):
            continue
            
        features = {
            'decade': float(decade),
            'rating': float(rating),
            'runtime': float(runtime),
            'genre_count': int(genre_count)
        }
        
        # Add genre indicators for top genres
        genres_str = film.get('genres', '')
        if pd.notna(genres_str):
            genres = parse_genres(genres_str)
            for genre in ['Drama', 'Comedy', 'Action', 'Thriller', 'Horror']:
                features[f'is_{genre}'] = 1 if genre in genres else 0
        else:
            for genre in ['Drama', 'Comedy', 'Action', 'Thriller', 'Horror']:
                features[f'is_{genre}'] = 0
        
        corr_features.append(features)
    
    corr_df = pd.DataFrame(corr_features)
    
    # Drop any columns that are all zeros
    corr_df = corr_df.loc[:, (corr_df != 0).any(axis=0)]
    
    corr_matrix = corr_df.corr()
    
    fig, ax = plt.subplots(figsize=(12, 10))
    
    im = ax.imshow(corr_matrix, cmap='RdBu_r', vmin=-1, vmax=1, aspect='auto')
    
    ax.set_xticks(range(len(corr_matrix.columns)))
    ax.set_yticks(range(len(corr_matrix.columns)))
    ax.set_xticklabels(corr_matrix.columns, rotation=45, ha='right', fontsize=10)
    ax.set_yticklabels(corr_matrix.columns, fontsize=10)
    
    # Add correlation values
    for i in range(len(corr_matrix)):
        for j in range(len(corr_matrix.columns)):
            val = corr_matrix.iloc[i, j]
            text = ax.text(j, i, f'{val:.2f}',
                         ha="center", va="center", 
                         color="white" if abs(val) > 0.5 else "black", 
                         fontsize=9, fontweight='bold')
    
    ax.set_title('Feature Correlation Matrix\n(How different attributes relate to each other)',
                fontsize=14, fontweight='bold', pad=20)
    
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label('Correlation', fontsize=11, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "04_feature_correlations.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 4 complete")


def viz_5_collection_segments(analyzer: PatternAnalyzer):
    """Segment collection into natural groups."""
    logger.info("Creating Viz 5: Collection Segments...")
    
    # Use decade, genre, rating, runtime for segmentation
    segments = analyzer.df.groupby(['decade', 'primary_genre']).agg({
        'const': 'count',
        'imdb_rating': 'mean',
        'runtime_mins': 'mean'
    }).reset_index()
    segments.columns = ['decade', 'genre', 'count', 'avg_rating', 'avg_runtime']
    segments = segments[segments['count'] >= 3].sort_values('count', ascending=False)
    
    top_20_segments = segments.head(20)
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    # 1. Segment sizes
    labels = [f"{row['genre'][:15]} ({int(row['decade'])}s)" 
             for _, row in top_20_segments.iterrows()]
    
    y_pos = range(len(top_20_segments))
    bars = ax1.barh(y_pos, top_20_segments['count'],
                   color=COLORS['gradient'] * 4, alpha=0.8, edgecolor='black')
    
    ax1.set_yticks(y_pos)
    ax1.set_yticklabels(labels, fontsize=9)
    ax1.set_xlabel('Film Count', fontsize=11, fontweight='bold')
    ax1.set_title('Top 20 Collection Segments', fontsize=12, fontweight='bold', pad=15)
    ax1.grid(axis='x', alpha=0.3)
    
    # 2. Segment quality vs size
    scatter = ax2.scatter(top_20_segments['count'], top_20_segments['avg_rating'],
                         s=top_20_segments['count']*10, alpha=0.6,
                         c=range(len(top_20_segments)), cmap='viridis',
                         edgecolors='black', linewidth=1)
    
    for _, row in top_20_segments.head(10).iterrows():
        ax2.annotate(f"{row['genre'][:10]}",
                    (row['count'], row['avg_rating']),
                    xytext=(5, 5), textcoords='offset points', fontsize=8)
    
    ax2.set_xlabel('Segment Size (Film Count)', fontsize=11, fontweight='bold')
    ax2.set_ylabel('Average Rating', fontsize=11, fontweight='bold')
    ax2.set_title('Segment Size vs Quality', fontsize=12, fontweight='bold', pad=15)
    ax2.grid(alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "05_collection_segments.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 5 complete")


def viz_6_recommendations_similar_films(analyzer: PatternAnalyzer):
    """Show ACTUAL similar films with titles - meaningful similarity analysis."""
    logger.info("Creating Viz 6: Similar Films Analysis...")
    
    # Get similarity matrix
    sim_matrix, film_ids = analyzer.calculate_film_similarity()
    
    # Pick 5 diverse seed films (high, medium, low rated + different genres)
    diverse_films = []
    
    # High rated
    high_rated = analyzer.df.nlargest(1, 'imdb_rating').iloc[0]
    diverse_films.append(high_rated)
    
    # Medium rated
    medium_rated = analyzer.df[
        (analyzer.df['imdb_rating'] > 6.5) & 
        (analyzer.df['imdb_rating'] < 7.5)
    ].sample(1).iloc[0]
    diverse_films.append(medium_rated)
    
    # Recent film
    recent = analyzer.df.nlargest(1, 'year').iloc[0]
    diverse_films.append(recent)
    
    # Classic film
    classic = analyzer.df.nsmallest(1, 'year').iloc[0]
    diverse_films.append(classic)
    
    # Random popular genre
    if 'primary_genre' in analyzer.df.columns:
        top_genre = analyzer.df['primary_genre'].value_counts().index[0]
        genre_film = analyzer.df[analyzer.df['primary_genre'] == top_genre].sample(1).iloc[0]
        diverse_films.append(genre_film)
    
    fig, axes = plt.subplots(3, 2, figsize=(18, 16))
    axes = axes.flatten()
    
    for idx, seed_film in enumerate(diverse_films[:6]):
        ax = axes[idx]
        
        # Find seed film's index
        seed_idx = analyzer.df[analyzer.df['const'] == seed_film['const']].index[0]
        
        # Get similarities for this film
        similarities = sim_matrix[seed_idx]
        
        # Exclude self
        similarities[seed_idx] = -1
        
        # Get top 8 most similar
        top_similar_indices = np.argsort(similarities)[-8:][::-1]
        
        # Build table of similar films
        similar_films = []
        for sim_idx in top_similar_indices:
            similar_film = analyzer.df.iloc[sim_idx]
            similar_films.append({
                'title': similar_film.get('title', 'Unknown')[:35],
                'year': int(similar_film.get('year', 0)),
                'rating': similar_film.get('imdb_rating', 0),
                'similarity': similarities[sim_idx]
            })
        
        # Display as text table
        ax.axis('off')
        
        # Title
        seed_title = seed_film.get('title', 'Unknown')[:40]
        seed_year = int(seed_film.get('year', 0))
        seed_rating = seed_film.get('imdb_rating', 0)
        seed_genre = seed_film.get('primary_genre', 'Unknown')
        
        ax.text(0.5, 0.98, f"Films Similar To:",
               ha='center', va='top', fontsize=10, fontweight='bold',
               transform=ax.transAxes)
        
        ax.text(0.5, 0.92, f"{seed_title}",
               ha='center', va='top', fontsize=11, fontweight='bold',
               transform=ax.transAxes, color=COLORS['primary'])
        
        ax.text(0.5, 0.87, f"({seed_year}) • {seed_genre} • ★{seed_rating:.1f}",
               ha='center', va='top', fontsize=9,
               transform=ax.transAxes, color='#666')
        
        # Header
        y_pos = 0.80
        ax.text(0.05, y_pos, "Title", fontsize=8, fontweight='bold',
               transform=ax.transAxes)
        ax.text(0.65, y_pos, "Year", fontsize=8, fontweight='bold',
               transform=ax.transAxes)
        ax.text(0.75, y_pos, "Rating", fontsize=8, fontweight='bold',
               transform=ax.transAxes)
        ax.text(0.88, y_pos, "Similarity", fontsize=8, fontweight='bold',
               transform=ax.transAxes)
        
        # Add line
        ax.plot([0.02, 0.98], [y_pos - 0.02, y_pos - 0.02],
               color='black', linewidth=1, transform=ax.transAxes)
        
        # Similar films
        y_pos = 0.75
        for i, film in enumerate(similar_films):
            # Alternate row colors
            if i % 2 == 0:
                ax.add_patch(plt.Rectangle((0.02, y_pos - 0.04), 0.96, 0.055,
                                          transform=ax.transAxes, 
                                          facecolor='#f0f0f0', edgecolor='none'))
            
            ax.text(0.05, y_pos, f"{i+1}. {film['title']}", 
                   fontsize=7, va='center', transform=ax.transAxes)
            ax.text(0.65, y_pos, f"{film['year']}", 
                   fontsize=7, va='center', transform=ax.transAxes)
            ax.text(0.75, y_pos, f"{film['rating']:.1f}", 
                   fontsize=7, va='center', transform=ax.transAxes)
            ax.text(0.88, y_pos, f"{film['similarity']:.3f}", 
                   fontsize=7, va='center', transform=ax.transAxes,
                   color=COLORS['success'] if film['similarity'] > 0.98 else COLORS['accent'])
            
            y_pos -= 0.065
        
        # Add border
        ax.add_patch(plt.Rectangle((0.02, 0.02), 0.96, 0.96,
                                   transform=ax.transAxes, 
                                   fill=False, edgecolor='#ddd', linewidth=1.5))
    
    plt.suptitle('Film Similarity Analysis: What Films Are Similar in Your Collection?',
                fontsize=14, fontweight='bold', y=0.98)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "06_similar_films_recommendations.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 6 complete")


def viz_7_pattern_strength(analyzer: PatternAnalyzer):
    """Strength of different patterns."""
    logger.info("Creating Viz 7: Pattern Strength Analysis...")
    
    combos = analyzer.get_feature_combinations()
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Genre consistency
    genre_variance = combos.groupby('primary_genre')['rating'].agg(['mean', 'std', 'count'])
    genre_variance = genre_variance[genre_variance['count'] >= 5].sort_values('std')
    
    top_consistent = genre_variance.head(10)
    ax1.barh(top_consistent.index, top_consistent['std'],
            color=COLORS['success'], alpha=0.8, edgecolor='black')
    ax1.set_xlabel('Rating Std Dev', fontsize=11, fontweight='bold')
    ax1.set_title('Most Consistent Genres\n(Lower = More Consistent)',
                 fontsize=12, fontweight='bold', pad=15)
    ax1.grid(axis='x', alpha=0.3)
    
    # 2. Decade consistency
    decade_variance = combos.groupby('decade')['rating'].agg(['mean', 'std', 'count'])
    decade_variance = decade_variance[decade_variance['count'] >= 5]
    
    ax2.bar([f"{int(d)}s" for d in decade_variance.index], decade_variance['std'],
           color=COLORS['warning'], alpha=0.8, edgecolor='black')
    ax2.set_xlabel('Decade', fontsize=11, fontweight='bold')
    ax2.set_ylabel('Rating Std Dev', fontsize=11, fontweight='bold')
    ax2.set_title('Quality Variance by Decade', fontsize=12, fontweight='bold', pad=15)
    ax2.tick_params(axis='x', rotation=45)
    ax2.grid(axis='y', alpha=0.3)
    
    # 3. Runtime pattern strength
    runtime_pattern = combos.groupby('runtime_cat')['rating'].agg(['mean', 'count'])
    
    ax3.bar(runtime_pattern.index, runtime_pattern['mean'],
           color=COLORS['accent'], alpha=0.8, edgecolor='black')
    for i, (cat, row) in enumerate(runtime_pattern.iterrows()):
        ax3.text(i, row['mean'] + 0.1, f"{row['mean']:.2f}\n({int(row['count'])})",
                ha='center', fontsize=9)
    
    ax3.set_ylabel('Average Rating', fontsize=11, fontweight='bold')
    ax3.set_title('Quality by Runtime Category', fontsize=12, fontweight='bold', pad=15)
    ax3.set_ylim(0, 10)
    ax3.axhline(y=7.0, color='red', linestyle='--', alpha=0.5)
    ax3.grid(axis='y', alpha=0.3)
    
    # 4. Multi-feature pattern
    multi_pattern = combos.groupby(['decade', 'runtime_cat']).size().nlargest(15)
    
    y_pos = range(len(multi_pattern))
    ax4.barh(y_pos, multi_pattern.values,
            color=COLORS['info'], alpha=0.8, edgecolor='black')
    
    labels = [f"{int(idx[0])}s - {idx[1]}" for idx in multi_pattern.index]
    ax4.set_yticks(y_pos)
    ax4.set_yticklabels(labels, fontsize=9)
    ax4.set_xlabel('Film Count', fontsize=11, fontweight='bold')
    ax4.set_title('Top Decade-Runtime Patterns', fontsize=12, fontweight='bold', pad=15)
    ax4.grid(axis='x', alpha=0.3)
    
    plt.suptitle('Pattern Strength Analysis', fontsize=15, fontweight='bold', y=0.98)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "07_pattern_strength.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 7 complete")


def viz_8_interactive_pattern_explorer(analyzer: PatternAnalyzer):
    """Interactive pattern exploration dashboard."""
    logger.info("Creating Viz 8: Interactive Pattern Explorer...")
    
    combos = analyzer.get_feature_combinations()
    
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Genre Distribution', 'Decade Evolution',
                       'Runtime Patterns', 'Quality Landscape'),
        specs=[[{'type': 'bar'}, {'type': 'scatter'}],
               [{'type': 'bar'}, {'type': 'scatter'}]]
    )
    
    # 1. Genre distribution
    genre_counts = combos['primary_genre'].value_counts().head(10)
    fig.add_trace(
        go.Bar(x=genre_counts.index, y=genre_counts.values,
              marker_color=COLORS['accent'], name='Genre Count'),
        row=1, col=1
    )
    
    # 2. Decade evolution
    decade_avg = combos.groupby('decade')['rating'].mean().reset_index()
    fig.add_trace(
        go.Scatter(x=decade_avg['decade'], y=decade_avg['rating'],
                  mode='lines+markers', name='Decade Quality',
                  line=dict(color=COLORS['success'], width=3),
                  marker=dict(size=10)),
        row=1, col=2
    )
    
    # 3. Runtime patterns
    runtime_counts = combos['runtime_cat'].value_counts()
    fig.add_trace(
        go.Bar(x=runtime_counts.index, y=runtime_counts.values,
              marker_color=COLORS['warning'], name='Runtime Count'),
        row=2, col=1
    )
    
    # 4. Quality landscape (decade vs rating)
    fig.add_trace(
        go.Scatter(x=combos['decade'], y=combos['rating'],
                  mode='markers', marker=dict(size=5, opacity=0.5),
                  name='Films'),
        row=2, col=2
    )
    
    fig.update_layout(
        height=900,
        title_text="Interactive Pattern Explorer",
        title_font_size=18,
        showlegend=False,
        template='plotly_white'
    )
    
    fig.write_html(OUTPUT_DIR / "08_interactive_pattern_explorer.html")
    logger.info("✅ Viz 8 complete")


def viz_9_outlier_detection(analyzer: PatternAnalyzer):
    """Detect outlier films."""
    logger.info("Creating Viz 9: Outlier Detection...")
    
    # Calculate z-scores for rating, runtime, decade
    df_numeric = analyzer.df[['imdb_rating', 'runtime_mins', 'decade']].dropna()
    
    from scipy import stats
    z_scores = np.abs(stats.zscore(df_numeric.values))  # Convert to numpy array
    outliers = (z_scores > 2).any(axis=1)
    
    outlier_films = analyzer.df.loc[df_numeric[outliers].index]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    # 1. Outlier counts by dimension
    outlier_dims = {
        'Rating Outlier': (z_scores[:, 0] > 2).sum(),  # Column 0 = rating
        'Extreme Runtime': (z_scores[:, 1] > 2).sum(),  # Column 1 = runtime
        'Unusual Era': (z_scores[:, 2] > 2).sum()  # Column 2 = decade
    }
    
    ax1.bar(outlier_dims.keys(), outlier_dims.values(),
           color=[COLORS['accent'], COLORS['secondary'], COLORS['warning']],
           alpha=0.8, edgecolor='black')
    
    for i, (dim, count) in enumerate(outlier_dims.items()):
        pct = count / len(analyzer.df) * 100
        ax1.text(i, count + 5, f'{count}\n({pct:.1f}%)',
                ha='center', fontsize=10, fontweight='bold')
    
    ax1.set_ylabel('Count', fontsize=11, fontweight='bold')
    ax1.set_title('Outlier Films by Dimension', fontsize=12, fontweight='bold', pad=15)
    ax1.grid(axis='y', alpha=0.3)
    
    # 2. Example outliers
    sample_outliers = outlier_films.head(15)
    
    y_pos = range(len(sample_outliers))
    outlier_labels = [f"{row['title'][:40]}..." if len(str(row['title'])) > 40
                     else row['title']
                     for _, row in sample_outliers.iterrows()]
    
    outlier_values = [row['imdb_rating'] for _, row in sample_outliers.iterrows()]
    
    bars = ax2.barh(y_pos, outlier_values,
                   color=COLORS['secondary'], alpha=0.7, edgecolor='black')
    
    ax2.set_yticks(y_pos)
    ax2.set_yticklabels(outlier_labels, fontsize=8)
    ax2.set_xlabel('IMDb Rating', fontsize=11, fontweight='bold')
    ax2.set_title('Sample Outlier Films', fontsize=12, fontweight='bold', pad=15)
    ax2.set_xlim(0, 10)
    ax2.axvline(x=7.0, color='red', linestyle='--', alpha=0.5)
    ax2.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "09_outlier_detection.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 9 complete")


def viz_10_genre_decade_deep_dive(analyzer: PatternAnalyzer):
    """Deep dive into genre-decade patterns."""
    logger.info("Creating Viz 10: Genre-Decade Deep Dive...")
    
    if analyzer.genre_data.empty:
        logger.warning("No genre data available")
        return
    
    # Create comprehensive pivot
    pivot = pd.crosstab(
        pd.cut(analyzer.genre_data['year'], bins=range(1920, 2030, 10)),
        analyzer.genre_data['genre']
    )
    
    # Get top 12 genres
    top_genres = pivot.sum().nlargest(12).index
    pivot = pivot[top_genres]
    
    fig, ax = plt.subplots(figsize=(16, 10))
    
    im = ax.imshow(pivot.values, cmap='YlGnBu', aspect='auto')
    
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_yticks(range(len(pivot.index)))
    ax.set_xticklabels(pivot.columns, rotation=45, ha='right')
    ax.set_yticklabels([f"{interval.left}s" for interval in pivot.index])
    
    # Add counts
    for i in range(len(pivot.index)):
        for j in range(len(pivot.columns)):
            val = pivot.iloc[i, j]
            if val > 0:
                text = ax.text(j, i, int(val),
                             ha="center", va="center", color="black", fontsize=8)
    
    ax.set_xlabel('Genre', fontsize=12, fontweight='bold')
    ax.set_ylabel('Decade', fontsize=12, fontweight='bold')
    ax.set_title('Comprehensive Genre × Decade Heatmap',
                fontsize=14, fontweight='bold', pad=20)
    
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label('Film Count', fontsize=11)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "10_genre_decade_deep_dive.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 10 complete")


def viz_11_recommendation_engine_output(analyzer: PatternAnalyzer):
    """Gap analysis - what patterns should you explore more?"""
    logger.info("Creating Viz 11: Collection Gaps & Expansion Opportunities...")
    
    combos = analyzer.get_feature_combinations()
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(18, 14))
    
    # 1. Underrepresented high-quality patterns
    pattern_stats = combos.groupby(['primary_genre', 'decade']).agg({
        'rating': ['mean', 'count']
    }).reset_index()
    pattern_stats.columns = ['genre', 'decade', 'avg_rating', 'count']
    
    # High quality (>7.5) but underrepresented (<10 films)
    gaps = pattern_stats[
        (pattern_stats['avg_rating'] > 7.5) &
        (pattern_stats['count'] < 10) &
        (pattern_stats['count'] >= 2)
    ].sort_values('avg_rating', ascending=False).head(15)
    
    y_pos = range(len(gaps))
    labels = [f"{row['genre']} ({int(row['decade'])}s) - Only {int(row['count'])} films"
             for _, row in gaps.iterrows()]
    
    bars = ax1.barh(y_pos, gaps['avg_rating'],
                   color=COLORS['warning'], alpha=0.8, edgecolor='black')
    
    for i, (_, row) in enumerate(gaps.iterrows()):
        ax1.text(row['avg_rating'] + 0.05, i, f"{row['avg_rating']:.2f}",
                va='center', fontsize=9, fontweight='bold')
    
    ax1.set_yticks(y_pos)
    ax1.set_yticklabels(labels, fontsize=9)
    ax1.set_xlabel('Average Rating', fontsize=11, fontweight='bold')
    ax1.set_title('HIGH-QUALITY GAPS\n(Great patterns you barely explore)',
                 fontsize=12, fontweight='bold', pad=15)
    ax1.set_xlim(0, 10)
    ax1.axvline(x=7.5, color='red', linestyle='--', alpha=0.5)
    ax1.grid(axis='x', alpha=0.3)
    
    # 2. Missing decades by genre
    genre_decade = pd.crosstab(combos['decade'], combos['primary_genre'])
    top_genres = genre_decade.sum().nlargest(8).index
    genre_decade = genre_decade[top_genres]
    
    # Find sparse cells (genres you don't explore in certain decades)
    missing = []
    for genre in top_genres:
        for decade in genre_decade.index:
            count = genre_decade.loc[decade, genre]
            if count < 5 and count > 0:
                missing.append({
                    'genre': genre,
                    'decade': int(decade),
                    'count': int(count)
                })
    
    missing_df = pd.DataFrame(missing).head(15)
    
    y_pos = range(len(missing_df))
    labels = [f"{row['genre']} from {int(row['decade'])}s"
             for _, row in missing_df.iterrows()]
    
    ax2.barh(y_pos, missing_df['count'],
            color=COLORS['secondary'], alpha=0.8, edgecolor='black')
    
    ax2.set_yticks(y_pos)
    ax2.set_yticklabels(labels, fontsize=9)
    ax2.set_xlabel('Current Count', fontsize=11, fontweight='bold')
    ax2.set_title('ERA GAPS\n(Decades you underexplore by genre)',
                 fontsize=12, fontweight='bold', pad=15)
    ax2.grid(axis='x', alpha=0.3)
    
    # 3. Directors you should explore more
    director_counts = Counter()
    for _, film in analyzer.df.iterrows():
        directors_str = film.get('directors', '')
        if pd.notna(directors_str):
            directors = parse_directors(directors_str)
            director_counts.update(directors)
    
    # Directors with 1-3 films (entry points for deeper dives)
    shallow_directors = {d: c for d, c in director_counts.items() if 1 <= c <= 3}
    
    # Get their average rating
    director_ratings = {}
    for director in shallow_directors:
        ratings = []
        for _, film in analyzer.df.iterrows():
            directors_str = film.get('directors', '')
            if pd.notna(directors_str) and director in parse_directors(directors_str):
                if pd.notna(film.get('imdb_rating')):
                    ratings.append(film['imdb_rating'])
        if ratings:
            director_ratings[director] = np.mean(ratings)
    
    top_shallow = dict(sorted(director_ratings.items(), 
                             key=lambda x: x[1], reverse=True)[:15])
    
    y_pos = range(len(top_shallow))
    ax3.barh(y_pos, list(top_shallow.values()),
            color=COLORS['success'], alpha=0.8, edgecolor='black')
    
    for i, (director, rating) in enumerate(top_shallow.items()):
        count = shallow_directors[director]
        ax3.text(rating + 0.05, i, f"{rating:.2f} ({count})",
                va='center', fontsize=8)
    
    ax3.set_yticks(y_pos)
    ax3.set_yticklabels(list(top_shallow.keys()), fontsize=9)
    ax3.set_xlabel('Average Rating', fontsize=11, fontweight='bold')
    ax3.set_title('DIRECTORS TO EXPLORE DEEPER\n(High-quality but only 1-3 films watched)',
                 fontsize=12, fontweight='bold', pad=15)
    ax3.set_xlim(0, 10)
    ax3.grid(axis='x', alpha=0.3)
    
    # 4. Runtime gaps
    runtime_genre = combos.groupby(['runtime_cat', 'primary_genre']).size().reset_index()
    runtime_genre.columns = ['runtime', 'genre', 'count']
    
    # Find sparse combinations
    sparse_runtime = runtime_genre[runtime_genre['count'] < 10].nlargest(15, 'count')
    
    labels = [f"{row['genre']} ({row['runtime']})"
             for _, row in sparse_runtime.iterrows()]
    
    y_pos = range(len(sparse_runtime))
    ax4.barh(y_pos, sparse_runtime['count'],
            color=COLORS['info'], alpha=0.8, edgecolor='black')
    
    ax4.set_yticks(y_pos)
    ax4.set_yticklabels(labels, fontsize=9)
    ax4.set_xlabel('Current Count', fontsize=11, fontweight='bold')
    ax4.set_title('RUNTIME DIVERSITY GAPS\n(Genre-length combinations to try)',
                 fontsize=12, fontweight='bold', pad=15)
    ax4.grid(axis='x', alpha=0.3)
    
    plt.suptitle('Collection Gap Analysis: Where to Expand Your Horizons',
                fontsize=15, fontweight='bold', y=0.98)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "11_collection_gaps_analysis.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 11 complete")


def viz_12_collection_diversity_score(analyzer: PatternAnalyzer):
    """Calculate and visualize collection diversity."""
    logger.info("Creating Viz 12: Collection Diversity Score...")
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Genre diversity (entropy)
    genre_counts = Counter()
    for _, film in analyzer.df.iterrows():
        genres_str = film.get('genres', '')
        if pd.notna(genres_str):
            genres = parse_genres(genres_str)  # Use proper helper
            genre_counts.update(genres)
    
    total = sum(genre_counts.values())
    genre_props = {g: c/total for g, c in genre_counts.items()}
    genre_entropy = -sum(p * np.log2(p) for p in genre_props.values() if p > 0)
    max_entropy = np.log2(len(genre_counts))
    genre_diversity = genre_entropy / max_entropy * 100
    
    ax1.text(0.5, 0.6, f"{genre_diversity:.1f}%",
            ha='center', fontsize=48, fontweight='bold',
            transform=ax1.transAxes, color=COLORS['success'])
    ax1.text(0.5, 0.4, "Genre Diversity Score",
            ha='center', fontsize=14, transform=ax1.transAxes)
    ax1.text(0.5, 0.25, f"({len(genre_counts)} unique genres)",
            ha='center', fontsize=10, transform=ax1.transAxes, style='italic')
    ax1.axis('off')
    
    # 2. Era diversity
    decade_counts = analyzer.df['decade'].value_counts()
    total_decades = len(analyzer.df)
    decade_entropy = -sum((c/total_decades) * np.log2(c/total_decades) 
                         for c in decade_counts.values if c > 0)  # .values not .values()
    max_decade_entropy = np.log2(len(decade_counts))
    era_diversity = decade_entropy / max_decade_entropy * 100
    
    ax2.text(0.5, 0.6, f"{era_diversity:.1f}%",
            ha='center', fontsize=48, fontweight='bold',
            transform=ax2.transAxes, color=COLORS['accent'])
    ax2.text(0.5, 0.4, "Era Diversity Score",
            ha='center', fontsize=14, transform=ax2.transAxes)
    ax2.text(0.5, 0.25, f"({len(decade_counts)} decades)",
            ha='center', fontsize=10, transform=ax2.transAxes, style='italic')
    ax2.axis('off')
    
    # 3. Quality range
    rating_range = analyzer.df['imdb_rating'].max() - analyzer.df['imdb_rating'].min()
    rating_std = analyzer.df['imdb_rating'].std()
    
    ax3.text(0.5, 0.6, f"{rating_range:.1f}",
            ha='center', fontsize=48, fontweight='bold',
            transform=ax3.transAxes, color=COLORS['warning'])
    ax3.text(0.5, 0.4, "Quality Range",
            ha='center', fontsize=14, transform=ax3.transAxes)
    ax3.text(0.5, 0.25, f"(σ = {rating_std:.2f})",
            ha='center', fontsize=10, transform=ax3.transAxes, style='italic')
    ax3.axis('off')
    
    # 4. Overall diversity summary
    diversity_metrics = {
        'Genre\nDiversity': genre_diversity,
        'Era\nDiversity': era_diversity,
        'Quality\nRange': (rating_range / 10) * 100  # Normalize to percentage
    }
    
    bars = ax4.bar(diversity_metrics.keys(), diversity_metrics.values(),
                  color=[COLORS['success'], COLORS['accent'], COLORS['warning']],
                  alpha=0.8, edgecolor='black')
    
    for bar, val in zip(bars, diversity_metrics.values()):
        ax4.text(bar.get_x() + bar.get_width()/2, val + 3,
                f'{val:.1f}%', ha='center', fontsize=11, fontweight='bold')
    
    ax4.set_ylabel('Diversity Score (%)', fontsize=11, fontweight='bold')
    ax4.set_title('Collection Diversity Metrics', fontsize=12, fontweight='bold', pad=15)
    ax4.set_ylim(0, 110)
    ax4.grid(axis='y', alpha=0.3)
    
    plt.suptitle('Collection Diversity Analysis', fontsize=15, fontweight='bold', y=0.98)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "12_collection_diversity_score.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 12 complete")


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def generate_summary(analyzer: PatternAnalyzer) -> str:
    """Generate comprehensive summary."""
    
    combos = analyzer.get_feature_combinations()
    
    # Pattern stats
    top_patterns = combos.groupby(['primary_genre', 'decade']).size().nlargest(5)
    
    # Similarity stats
    sim_matrix, _ = analyzer.calculate_film_similarity()
    triu_indices = np.triu_indices_from(sim_matrix, k=1)
    similarities = sim_matrix[triu_indices]
    
    summary = f"""
{'='*80}
BATCH 8: PATTERN DISCOVERY & RECOMMENDATIONS - COMPLETE
{'='*80}

📊 PATTERN ANALYSIS

Collection Size: {len(analyzer.df):,} films
Unique Genres: {len(combos['primary_genre'].unique())}
Era Span: {int(combos['decade'].min())}s - {int(combos['decade'].max())}s

Top 5 Patterns (Genre-Decade):
"""
    
    for i, (pattern, count) in enumerate(top_patterns.items(), 1):
        summary += f"  {i}. {pattern[0]} ({int(pattern[1])}s): {count} films\n"
    
    summary += f"""
🔗 SIMILARITY ANALYSIS

Average Film Similarity: {similarities.mean():.3f}
Median Film Similarity: {np.median(similarities):.3f}
Similarity Range: {similarities.min():.3f} - {similarities.max():.3f}

🎯 RECOMMENDATIONS

High-quality underexplored patterns identified
Similar film recommendations generated
Collection diversity calculated

📈 VISUALIZATIONS CREATED:

1. 01_pattern_overview.png - Dominant patterns overview
2. 02_actor_director_patterns.png - Talent collaboration patterns  
3. 03_similarity_clusters.png - Film similarity distribution
4. 04_feature_correlations.png - Feature correlation matrix (FIXED)
5. 05_collection_segments.png - Natural collection groupings
6. 06_similar_films_recommendations.png - Similar films WITH TITLES (FIXED)
7. 07_pattern_strength.png - Pattern consistency analysis
8. 08_interactive_pattern_explorer.html - Interactive dashboard
9. 09_outlier_detection.png - Unusual films identification
10. 10_genre_decade_deep_dive.png - Comprehensive heatmap
11. 11_collection_gaps_analysis.png - Gap analysis for expansion (FIXED)
12. 12_collection_diversity_score.png - Diversity metrics

✅ Batch 8 Complete!
{'='*80}
"""
    
    return summary


def main():
    logger.info("="*80)
    logger.info("BATCH 8: PATTERN DISCOVERY & RECOMMENDATIONS")
    logger.info("="*80)
    
    # Load data
    master_file = DATA_DIR / "watched_movies_master.csv"
    if not master_file.exists():
        logger.error(f"Master file not found: {master_file}")
        logger.error("Please run batch_0_filter_watched.py first")
        return
    
    df = pd.read_csv(master_file, low_memory=False)
    logger.info(f"Loaded {len(df)} films")
    
    # Initialize analyzer
    analyzer = PatternAnalyzer(df)
    
    # Generate visualizations
    logger.info("\n" + "="*80)
    logger.info("GENERATING VISUALIZATIONS")
    logger.info("="*80)
    
    viz_1_pattern_overview(analyzer)
    viz_2_actor_director_patterns(analyzer)
    viz_3_similarity_clusters(analyzer)
    viz_4_feature_correlations(analyzer)
    viz_5_collection_segments(analyzer)
    viz_6_recommendations_similar_films(analyzer)
    viz_7_pattern_strength(analyzer)
    viz_8_interactive_pattern_explorer(analyzer)
    viz_9_outlier_detection(analyzer)
    viz_10_genre_decade_deep_dive(analyzer)
    viz_11_recommendation_engine_output(analyzer)
    viz_12_collection_diversity_score(analyzer)
    
    # Generate and save summary
    summary = generate_summary(analyzer)
    print(summary)
    
    with open(OUTPUT_DIR / "BATCH_8_SUMMARY.txt", 'w') as f:
        f.write(summary)
    
    logger.info(f"\n✅ All visualizations saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()