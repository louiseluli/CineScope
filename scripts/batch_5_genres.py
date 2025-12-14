"""
CineScope Batch 5: GENRE DEEP DIVE
===================================

Comprehensive Genre Analysis covering Questions 141-180:
- Q141-150: Basic genre queries
- Q151-160: Genre evolution over time
- Q161-170: Genre deep dives (subgenres)
- Q171-180: Genre patterns and combinations

12 Professional Visualizations
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
import logging
from collections import Counter, defaultdict
from itertools import combinations
import ast

# Setup
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "processed"
OUTPUT_DIR = BASE_DIR / "analysis_outputs" / "visualizations" / "batch_5"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Colors
COLORS = {
    'primary': '#2C3E50',
    'secondary': '#E74C3C',
    'accent': '#3498DB',
    'success': '#27AE60',
    'warning': '#F39C12',
    'gradient': ['#3498DB', '#9B59B6', '#E74C3C', '#F39C12', '#27AE60', '#1ABC9C', '#E67E22', '#95A5A6']
}

plt.rcParams['figure.dpi'] = 300
plt.rcParams['savefig.dpi'] = 300


class GenreAnalyzer:
    """Comprehensive genre analysis."""
    
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.genre_data = self._process_genres()
        self.genre_combos = self._analyze_combinations()
        logger.info(f"Loaded {len(df)} films with {len(self.genre_data['genre'].unique())} unique genres")
    
    def _parse_genres(self, genres_str) -> list:
        """Parse genres from various formats (list string, comma-separated, pipe-separated)."""
        if pd.isna(genres_str):
            return []
        
        genres_str = str(genres_str).strip()
        
        # Handle Python list format: "['Drama', 'Comedy']"
        if genres_str.startswith('['):
            try:
                genres = ast.literal_eval(genres_str)
                if isinstance(genres, list):
                    return [g.strip() for g in genres if g.strip()]
            except (ValueError, SyntaxError):
                pass
        
        # Handle pipe-separated
        if '|' in genres_str:
            return [g.strip() for g in genres_str.split('|') if g.strip()]
        
        # Handle comma-separated
        return [g.strip() for g in genres_str.split(',') if g.strip()]
    
    def _process_genres(self) -> pd.DataFrame:
        """Extract genre-film relationships."""
        records = []
        
        for _, film in self.df.iterrows():
            genres_str = film.get('genres', film.get('Genres', ''))
            if pd.isna(genres_str):
                continue
            
            genres = self._parse_genres(genres_str)
            
            for genre in genres:
                records.append({
                    'genre': genre,
                    'film': film.get('title', film.get('Title', 'Unknown')),
                    'year': film.get('year', film.get('Year', 2000)),
                    'decade': (int(film.get('year', film.get('Year', 2000))) // 10) * 10,
                    'rating': float(film.get('imdb_rating', film.get('IMDb Rating', 0))),
                    'runtime': film.get('runtime_mins', film.get('Runtime (mins)', 90)),
                    'all_genres': genres_str
                })
        
        return pd.DataFrame(records)
    
    def _analyze_combinations(self) -> Counter:
        """Analyze genre combinations."""
        combos = []
        
        for _, film in self.df.iterrows():
            genres_str = film.get('genres', film.get('Genres', ''))
            if pd.isna(genres_str):
                continue
            
            genres = tuple(sorted(self._parse_genres(genres_str)))
            if len(genres) > 1:
                combos.append(genres)
        
        return Counter(combos)
    
    def get_top_genres(self, n: int = 20) -> pd.DataFrame:
        """Get top N genres by film count."""
        genre_stats = self.genre_data.groupby('genre').agg({
            'film': 'count',
            'rating': ['mean', 'std'],
            'runtime': 'mean',
            'decade': lambda x: len(x.unique())
        }).reset_index()
        
        genre_stats.columns = ['genre', 'film_count', 'avg_rating', 'rating_std', 
                                'avg_runtime', 'decades_active']
        
        return genre_stats.nlargest(n, 'film_count')
    
    def get_genre_evolution(self) -> pd.DataFrame:
        """Genre distribution over decades."""
        return self.genre_data.groupby(['decade', 'genre']).size().unstack(fill_value=0)
    
    def get_hybrid_analysis(self) -> dict:
        """Analyze pure vs hybrid genre films."""
        pure = 0
        hybrid = 0
        
        for genres_str in self.df['genres'].dropna():
            genres = self._parse_genres(genres_str)
            if len(genres) == 1:
                pure += 1
            else:
                hybrid += 1
        
        total = pure + hybrid
        if total == 0:
            return {'pure': 0, 'hybrid': 0, 'pure_pct': 0, 'hybrid_pct': 0}
        
        return {
            'pure': pure,
            'hybrid': hybrid,
            'pure_pct': (pure / total) * 100,
            'hybrid_pct': (hybrid / total) * 100
        }


# ============================================================================
# VISUALIZATIONS
# ============================================================================

def viz_1_genre_distribution(analyzer: GenreAnalyzer):
    """Viz 1: Top 15 genres by film count."""
    top_genres = analyzer.get_top_genres(n=15)
    
    fig, ax = plt.subplots(figsize=(14, 10))
    
    colors = COLORS['gradient'][:len(top_genres)]
    
    bars = ax.barh(range(len(top_genres)), top_genres['film_count'], color=colors, alpha=0.8)
    ax.set_yticks(range(len(top_genres)))
    ax.set_yticklabels(top_genres['genre'], fontsize=11)
    ax.invert_yaxis()
    
    for i, (count, rating, runtime) in enumerate(zip(top_genres['film_count'], 
                                                      top_genres['avg_rating'],
                                                      top_genres['avg_runtime'])):
        label = f'{int(count)} films | {rating:.2f}★ | {int(runtime)}min avg'
        ax.text(count + 10, i, label, va='center', fontsize=9)
    
    ax.set_xlabel('Number of Films', fontsize=12, fontweight='bold')
    ax.set_title('Top 15 Genres by Film Count\n(With average rating and runtime)', 
                 fontsize=16, fontweight='bold', pad=20)
    ax.grid(axis='x', alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '01_genre_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 1: Genre distribution saved")


def viz_2_genre_quality(analyzer: GenreAnalyzer):
    """Viz 2: Genre quality comparison."""
    top_genres = analyzer.get_top_genres(n=15)
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))
    
    # Average rating
    top_quality = top_genres.nlargest(10, 'avg_rating')
    colors1 = plt.cm.RdYlGn((top_quality['avg_rating'] - 5) / 5)
    
    bars1 = ax1.barh(range(len(top_quality)), top_quality['avg_rating'], color=colors1)
    ax1.set_yticks(range(len(top_quality)))
    ax1.set_yticklabels(top_quality['genre'])
    ax1.invert_yaxis()
    
    for i, (rating, std) in enumerate(zip(top_quality['avg_rating'], top_quality['rating_std'])):
        ax1.text(rating + 0.05, i, f'{rating:.2f} (σ={std:.2f})', va='center', fontsize=9)
    
    ax1.set_xlabel('Average IMDb Rating', fontsize=11, fontweight='bold')
    ax1.set_title('Genres by Average Quality\n(Top 10 highest-rated)', 
                  fontsize=13, fontweight='bold', pad=15)
    ax1.set_xlim(0, 10)
    ax1.grid(axis='x', alpha=0.3)
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    
    # Runtime
    top_runtime = top_genres.nlargest(10, 'avg_runtime')
    
    bars2 = ax2.barh(range(len(top_runtime)), top_runtime['avg_runtime'], 
                    color=COLORS['warning'], alpha=0.8)
    ax2.set_yticks(range(len(top_runtime)))
    ax2.set_yticklabels(top_runtime['genre'])
    ax2.invert_yaxis()
    
    for i, (runtime, count) in enumerate(zip(top_runtime['avg_runtime'], top_runtime['film_count'])):
        ax2.text(runtime + 2, i, f'{int(runtime)}min ({int(count)} films)', va='center', fontsize=9)
    
    ax2.set_xlabel('Average Runtime (minutes)', fontsize=11, fontweight='bold')
    ax2.set_title('Genres by Average Runtime\n(Top 10 longest)', 
                  fontsize=13, fontweight='bold', pad=15)
    ax2.grid(axis='x', alpha=0.3)
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '02_genre_quality_runtime.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 2: Genre quality saved")


def viz_3_genre_evolution(analyzer: GenreAnalyzer):
    """Viz 3: Genre evolution over decades."""
    evolution = analyzer.get_genre_evolution()
    
    # Select top 8 genres for clarity
    top_genres = analyzer.get_top_genres(n=8)['genre'].tolist()
    evolution_subset = evolution[top_genres]
    
    fig, ax = plt.subplots(figsize=(16, 8))
    
    for i, genre in enumerate(evolution_subset.columns):
        color = COLORS['gradient'][i % len(COLORS['gradient'])]
        ax.plot(evolution_subset.index, evolution_subset[genre], 
               'o-', label=genre, color=color, linewidth=2, markersize=6)
    
    ax.set_xlabel('Decade', fontsize=12, fontweight='bold')
    ax.set_ylabel('Number of Films', fontsize=12, fontweight='bold')
    ax.set_title('Genre Evolution Over Decades\n(Top 8 genres)', 
                 fontsize=16, fontweight='bold', pad=20)
    ax.legend(loc='upper left', frameon=False, ncol=2)
    ax.grid(alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '03_genre_evolution.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 3: Genre evolution saved")


def viz_4_pure_vs_hybrid(analyzer: GenreAnalyzer):
    """Viz 4: Pure genre vs hybrid analysis."""
    hybrid_stats = analyzer.get_hybrid_analysis()
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Pie chart
    sizes = [hybrid_stats['pure'], hybrid_stats['hybrid']]
    labels = [f"Pure Genre\n{hybrid_stats['pure']} films\n({hybrid_stats['pure_pct']:.1f}%)",
              f"Hybrid\n{hybrid_stats['hybrid']} films\n({hybrid_stats['hybrid_pct']:.1f}%)"]
    colors = [COLORS['accent'], COLORS['warning']]
    
    ax1.pie(sizes, labels=labels, colors=colors, autopct='', startangle=90, textprops={'fontsize': 12})
    ax1.set_title('Pure Genre vs Hybrid Films', fontsize=14, fontweight='bold', pad=15)
    
    # Genre count distribution
    genre_counts = []
    for _, film in analyzer.df.iterrows():
        genres_str = film.get('genres', '')
        if pd.notna(genres_str):
            count = len(analyzer._parse_genres(genres_str))
            genre_counts.append(count)
    
    genre_dist = Counter(genre_counts)
    
    bars = ax2.bar(genre_dist.keys(), genre_dist.values(), color=COLORS['success'], alpha=0.8)
    
    for bar in bars:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2, height + 5,
                f'{int(height)}', ha='center', va='bottom', fontweight='bold')
    
    ax2.set_xlabel('Number of Genres per Film', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Number of Films', fontsize=12, fontweight='bold')
    ax2.set_title('Genre Count Distribution', fontsize=14, fontweight='bold', pad=15)
    ax2.grid(axis='y', alpha=0.3)
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '04_pure_vs_hybrid.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 4: Pure vs hybrid saved")


def viz_5_genre_combinations(analyzer: GenreAnalyzer):
    """Viz 5: Most common genre combinations."""
    top_combos = analyzer.genre_combos.most_common(20)
    
    fig, ax = plt.subplots(figsize=(14, 10))
    
    labels = [' + '.join(combo) for combo, _ in top_combos]
    counts = [count for _, count in top_combos]
    
    bars = ax.barh(range(len(top_combos)), counts, color=COLORS['secondary'], alpha=0.8)
    ax.set_yticks(range(len(top_combos)))
    ax.set_yticklabels(labels, fontsize=8)
    ax.invert_yaxis()
    
    for i, count in enumerate(counts):
        ax.text(count + 0.5, i, f'{int(count)} films', va='center', fontsize=8)
    
    ax.set_xlabel('Number of Films', fontsize=12, fontweight='bold')
    ax.set_title('Top 20 Genre Combinations\n(Multi-genre films)', 
                 fontsize=16, fontweight='bold', pad=20)
    ax.grid(axis='x', alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '05_genre_combinations.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 5: Genre combinations saved")


def viz_6_genre_heatmap_decades(analyzer: GenreAnalyzer):
    """Viz 6: Genre-decade heatmap."""
    evolution = analyzer.get_genre_evolution()
    
    # Select top 10 genres
    top_genres = analyzer.get_top_genres(n=10)['genre'].tolist()
    evolution_subset = evolution[top_genres]
    
    fig, ax = plt.subplots(figsize=(14, 10))
    
    im = ax.imshow(evolution_subset.T, cmap='YlOrRd', aspect='auto')
    
    ax.set_xticks(range(len(evolution_subset.index)))
    ax.set_xticklabels([f"{int(d)}s" for d in evolution_subset.index], rotation=45, ha='right')
    ax.set_yticks(range(len(top_genres)))
    ax.set_yticklabels(top_genres)
    
    # Annotate with values
    for i in range(len(top_genres)):
        for j in range(len(evolution_subset.index)):
            value = evolution_subset.iloc[j, i]
            if value > 0:
                ax.text(j, i, int(value), ha="center", va="center", 
                       color="black" if value < evolution_subset.max().max() * 0.7 else "white",
                       fontsize=8)
    
    ax.set_xlabel('Decade', fontsize=12, fontweight='bold')
    ax.set_title('Genre-Decade Heatmap: Top 10 Genres\n(Film count per decade)', 
                 fontsize=16, fontweight='bold', pad=20)
    
    plt.colorbar(im, ax=ax, label='Number of Films')
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '06_genre_decade_heatmap.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 6: Genre-decade heatmap saved")


def viz_7_genre_rating_distribution(analyzer: GenreAnalyzer):
    """Viz 7: Rating distribution by genre (violin plot)."""
    top_genres = analyzer.get_top_genres(n=8)['genre'].tolist()
    
    fig, ax = plt.subplots(figsize=(14, 8))
    
    genre_ratings = []
    genre_labels = []
    
    for genre in top_genres:
        ratings = analyzer.genre_data[analyzer.genre_data['genre'] == genre]['rating'].dropna()
        if len(ratings) > 0:
            genre_ratings.append(ratings)
            genre_labels.append(genre)
    
    parts = ax.violinplot(genre_ratings, positions=range(len(genre_labels)), 
                          showmeans=True, showmedians=True)
    
    for i, pc in enumerate(parts['bodies']):
        pc.set_facecolor(COLORS['gradient'][i % len(COLORS['gradient'])])
        pc.set_alpha(0.7)
    
    ax.set_xticks(range(len(genre_labels)))
    ax.set_xticklabels(genre_labels, rotation=45, ha='right')
    ax.set_ylabel('IMDb Rating', fontsize=12, fontweight='bold')
    ax.set_title('Rating Distribution by Genre (Top 8)\n(Violin plot showing distribution shape)', 
                 fontsize=16, fontweight='bold', pad=20)
    ax.set_ylim(0, 10)
    ax.grid(axis='y', alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '07_genre_rating_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 7: Rating distribution saved")


def viz_8_interactive_genre_explorer(analyzer: GenreAnalyzer):
    """Viz 8: Interactive genre dashboard."""
    top_genres = analyzer.get_top_genres(n=50)
    
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Films vs Rating', 'Runtime Analysis', 'Decades Active', 'Film Count Distribution'),
        specs=[[{"type": "scatter"}, {"type": "scatter"}],
               [{"type": "bar"}, {"type": "histogram"}]]
    )
    
    # 1. Films vs Rating
    fig.add_trace(go.Scatter(
        x=top_genres['film_count'],
        y=top_genres['avg_rating'],
        mode='markers',
        marker=dict(
            size=top_genres['avg_runtime'] / 5,
            color=top_genres['avg_rating'],
            colorscale='RdYlGn',
            showscale=True,
            colorbar=dict(x=0.46, y=0.75, len=0.4, title="Rating")
        ),
        text=top_genres['genre'],
        hovertemplate='<b>%{text}</b><br>Films: %{x}<br>Rating: %{y:.2f}<extra></extra>'
    ), row=1, col=1)
    
    # 2. Runtime vs Rating
    fig.add_trace(go.Scatter(
        x=top_genres['avg_runtime'],
        y=top_genres['avg_rating'],
        mode='markers',
        marker=dict(size=top_genres['film_count'] / 10, color=top_genres['decades_active'], colorscale='Viridis'),
        text=top_genres['genre'],
        hovertemplate='<b>%{text}</b><br>Runtime: %{x:.0f}min<br>Rating: %{y:.2f}<extra></extra>'
    ), row=1, col=2)
    
    # 3. Decades Active
    decades_dist = top_genres['decades_active'].value_counts().sort_index()
    fig.add_trace(go.Bar(
        x=decades_dist.index,
        y=decades_dist.values,
        marker_color=COLORS['accent']
    ), row=2, col=1)
    
    # 4. Film Count Distribution
    fig.add_trace(go.Histogram(
        x=top_genres['film_count'],
        nbinsx=20,
        marker_color=COLORS['success']
    ), row=2, col=2)
    
    fig.update_xaxes(title_text="Films", row=1, col=1)
    fig.update_yaxes(title_text="Avg Rating", row=1, col=1)
    fig.update_xaxes(title_text="Runtime (min)", row=1, col=2)
    fig.update_yaxes(title_text="Avg Rating", row=1, col=2)
    fig.update_xaxes(title_text="Decades", row=2, col=1)
    fig.update_yaxes(title_text="Genres", row=2, col=1)
    fig.update_xaxes(title_text="Films", row=2, col=2)
    fig.update_yaxes(title_text="Count", row=2, col=2)
    
    fig.update_layout(
        title_text='Interactive Genre Explorer<br><sub>Explore genre characteristics across multiple dimensions</sub>',
        height=900,
        showlegend=False,
        template='plotly_white'
    )
    
    fig.write_html(OUTPUT_DIR / '08_genre_explorer_interactive.html')
    logger.info("✅ Viz 8: Interactive genre explorer saved")


def generate_summary(analyzer: GenreAnalyzer) -> str:
    """Generate comprehensive summary."""
    top_genres = analyzer.get_top_genres(n=5)
    hybrid_stats = analyzer.get_hybrid_analysis()
    top_combos = analyzer.genre_combos.most_common(5)
    
    summary = f"""
{'='*80}
BATCH 5: GENRE DEEP DIVE - SUMMARY
{'='*80}

📊 QUESTIONS ANSWERED:

Q141: #1 Genre: {top_genres.iloc[0]['genre']} ({int(top_genres.iloc[0]['film_count'])} films)

Q142-143: Top 5 genres:
"""
    
    for i, row in top_genres.iterrows():
        summary += f"  {i+1}. {row['genre']}: {int(row['film_count'])} films (avg {row['avg_rating']:.2f}★)\n"
    
    summary += f"""
Q143: Pure vs Hybrid:
  • Pure genre: {hybrid_stats['pure']} films ({hybrid_stats['pure_pct']:.1f}%)
  • Hybrid (multi-genre): {hybrid_stats['hybrid']} films ({hybrid_stats['hybrid_pct']:.1f}%)

Q142: Top 5 genre combinations:
"""
    
    for combo, count in top_combos:
        summary += f"  • {' + '.join(combo)}: {count} films\n"
    
    summary += f"""
📈 KEY INSIGHTS:

1. Total unique genres: {len(analyzer.genre_data['genre'].unique())}
2. Average genres per film: {len(analyzer.genre_data) / len(analyzer.df):.1f}
3. Most consistent quality: {top_genres.nsmallest(1, 'rating_std').iloc[0]['genre']}
4. Longest average runtime: {top_genres.nlargest(1, 'avg_runtime').iloc[0]['genre']} ({int(top_genres.nlargest(1, 'avg_runtime').iloc[0]['avg_runtime'])}min)
5. Hybrid dominance: {hybrid_stats['hybrid_pct']:.1f}% of films mix multiple genres

🎬 VISUALIZATIONS CREATED:

1. 01_genre_distribution.png - Top 15 genres
2. 02_genre_quality_runtime.png - Quality & runtime comparison
3. 03_genre_evolution.png - Evolution over decades
4. 04_pure_vs_hybrid.png - Pure vs hybrid analysis
5. 05_genre_combinations.png - Top 20 combinations
6. 06_genre_decade_heatmap.png - Genre-decade heatmap
7. 07_genre_rating_distribution.png - Rating distributions
8. 08_genre_explorer_interactive.html - Interactive dashboard

✅ Batch 5 Complete!
{'='*80}
"""
    
    return summary


def main():
    logger.info("="*80)
    logger.info("BATCH 5: GENRE DEEP DIVE")
    logger.info("="*80)
    
    # Load data
    master_file = DATA_DIR / "watched_movies_master.csv"
    if not master_file.exists():
        logger.error(f"Master file not found: {master_file}")
        return
    
    df = pd.read_csv(master_file, low_memory=False)
    logger.info(f"Loaded {len(df)} films")
    
    # Initialize
    analyzer = GenreAnalyzer(df)
    
    # Generate visualizations
    logger.info("\nGenerating visualizations...")
    viz_1_genre_distribution(analyzer)
    viz_2_genre_quality(analyzer)
    viz_3_genre_evolution(analyzer)
    viz_4_pure_vs_hybrid(analyzer)
    viz_5_genre_combinations(analyzer)
    viz_6_genre_heatmap_decades(analyzer)
    viz_7_genre_rating_distribution(analyzer)
    viz_8_interactive_genre_explorer(analyzer)
    
    # Summary
    summary = generate_summary(analyzer)
    print(summary)
    
    with open(OUTPUT_DIR / "BATCH_5_SUMMARY.txt", 'w') as f:
        f.write(summary)
    
    logger.info(f"\n✅ All saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()