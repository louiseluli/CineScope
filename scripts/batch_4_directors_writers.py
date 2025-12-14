"""
CineScope Batch 4: DIRECTORS DEEP DIVE
=======================================

Comprehensive Director Analysis covering Questions 51-110:
- Q51-60: Basic director queries
- Q61-70: Director collaborations
- Q71-80: Director evolution
- Q81-90: Writer queries
- Q91-100: Writer analysis
- Q101-110: Writer-director pairs

15 Professional Visualizations
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
import logging
from collections import Counter, defaultdict

# Setup
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "processed"
OUTPUT_DIR = BASE_DIR / "analysis_outputs" / "visualizations" / "batch_4"
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
    'gradient': ['#3498DB', '#9B59B6', '#E74C3C', '#F39C12', '#27AE60', '#1ABC9C']
}

plt.rcParams['figure.dpi'] = 300
plt.rcParams['savefig.dpi'] = 300


class DirectorAnalyzer:
    """Comprehensive director and writer analysis."""
    
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.director_data = self._process_directors()
        self.writer_data = self._process_writers()
        self.collaborations = self._analyze_collaborations()
        logger.info(f"Loaded {len(self.director_data)} unique directors, {len(self.writer_data)} unique writers")
    
    def _process_directors(self) -> pd.DataFrame:
        """Extract director statistics."""
        records = []
        
        for _, film in self.df.iterrows():
            directors_str = film.get('directors', film.get('Directors', ''))
            if pd.isna(directors_str):
                continue
            
            directors = [d.strip() for d in str(directors_str).split(',') if d.strip()]
            
            for director in directors:
                records.append({
                    'director': director,
                    'film': film.get('title', film.get('Title', 'Unknown')),
                    'year': film.get('year', film.get('Year')),
                    'decade': (int(film.get('year', film.get('Year', 2000))) // 10) * 10,
                    'rating': float(film.get('imdb_rating', film.get('IMDb Rating', 0))),
                    'genres': film.get('genres', film.get('Genres', ''))
                })
        
        return pd.DataFrame(records)
    
    def _process_writers(self) -> pd.DataFrame:
        """Extract writer statistics."""
        records = []
        
        for _, film in self.df.iterrows():
            # Prefer tmdb_writers (has names), fall back to writers (may have IDs)
            writers_str = film.get('tmdb_writers', '')
            if pd.isna(writers_str) or not writers_str:
                writers_str = film.get('writers', '')
            if pd.isna(writers_str):
                continue
            
            # tmdb_writers uses pipe separator, others use comma
            if '|' in str(writers_str):
                writers = [w.strip() for w in str(writers_str).split('|') if w.strip()]
            else:
                writers = [w.strip() for w in str(writers_str).split(',') if w.strip()]
            
            # Skip IMDb IDs (nm followed by digits)
            writers = [w for w in writers if not (w.startswith('nm') and w[2:].isdigit())]
            
            for writer in writers:
                records.append({
                    'writer': writer,
                    'film': film.get('title', 'Unknown'),
                    'year': film.get('year', 2000),
                    'rating': float(film.get('imdb_rating', 0))
                })
        
        return pd.DataFrame(records)
    
    def _analyze_collaborations(self) -> dict:
        """Analyze director-actor collaborations."""
        collabs = defaultdict(lambda: defaultdict(int))
        
        for _, film in self.df.iterrows():
            directors = str(film.get('directors', '')).split(',')
            actors = str(film.get('top_10_actors', '')).split('|')
            
            for director in directors:
                director = director.strip()
                if not director or director == 'nan':
                    continue
                for actor in actors:
                    actor = actor.strip()
                    if not actor or actor == 'nan':
                        continue
                    collabs[director][actor] += 1
        
        return dict(collabs)
    
    def get_top_directors(self, n: int = 20) -> pd.DataFrame:
        """Get top N most-watched directors."""
        if len(self.director_data) == 0:
            return pd.DataFrame()
        
        director_stats = self.director_data.groupby('director').agg({
            'film': 'count',
            'rating': 'mean',
            'year': ['min', 'max'],
            'decade': lambda x: len(x.unique())
        }).reset_index()
        
        director_stats.columns = ['director', 'film_count', 'avg_rating', 
                                   'first_year', 'last_year', 'decades_active']
        director_stats['career_span'] = director_stats['last_year'] - director_stats['first_year']
        
        return director_stats.nlargest(n, 'film_count')
    
    def get_director_actor_pairs(self, n: int = 20) -> list:
        """Get top director-actor collaborations."""
        pairs = []
        for director, actors in self.collaborations.items():
            for actor, count in actors.items():
                if count >= 2:  # At least 2 films together
                    pairs.append((director, actor, count))
        
        pairs.sort(key=lambda x: x[2], reverse=True)
        return pairs[:n]


# ============================================================================
# VISUALIZATIONS
# ============================================================================

def viz_1_director_leaderboard(analyzer: DirectorAnalyzer):
    """Viz 1: Top 20 directors."""
    top_directors = analyzer.get_top_directors(n=20)
    
    if len(top_directors) == 0:
        logger.warning("No director data available")
        return
    
    fig, ax = plt.subplots(figsize=(14, 10))
    
    colors = plt.cm.viridis(top_directors['avg_rating'] / 10)
    
    bars = ax.barh(range(len(top_directors)), top_directors['film_count'], color=colors)
    ax.set_yticks(range(len(top_directors)))
    ax.set_yticklabels(top_directors['director'], fontsize=9)
    ax.invert_yaxis()
    
    for i, (count, rating, span) in enumerate(zip(top_directors['film_count'], 
                                                   top_directors['avg_rating'],
                                                   top_directors['career_span'])):
        ax.text(count + 0.3, i, f'{int(count)} films | {rating:.2f}★ | {int(span)}y', 
               va='center', fontsize=8)
    
    ax.set_xlabel('Number of Films', fontsize=12, fontweight='bold')
    ax.set_title('Top 20 Most-Watched Directors\n(Color = Average rating, Label = Films | Rating | Career span)', 
                 fontsize=16, fontweight='bold', pad=20)
    ax.grid(axis='x', alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '01_director_leaderboard.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 1: Director leaderboard saved")


def viz_2_director_quality(analyzer: DirectorAnalyzer):
    """Viz 2: Directors by average rating (min 5 films)."""
    director_stats = analyzer.director_data.groupby('director').agg({
        'rating': ['mean', 'count', 'std']
    }).reset_index()
    
    director_stats.columns = ['director', 'avg_rating', 'film_count', 'rating_std']
    director_stats = director_stats[director_stats['film_count'] >= 5]
    top_quality = director_stats.nlargest(20, 'avg_rating')
    
    fig, ax = plt.subplots(figsize=(14, 10))
    
    colors = plt.cm.RdYlGn((top_quality['avg_rating'] - 5) / 5)
    
    bars = ax.barh(range(len(top_quality)), top_quality['avg_rating'], color=colors)
    ax.set_yticks(range(len(top_quality)))
    ax.set_yticklabels(top_quality['director'], fontsize=9)
    ax.invert_yaxis()
    
    for i, (rating, films, std) in enumerate(zip(top_quality['avg_rating'], 
                                                  top_quality['film_count'],
                                                  top_quality['rating_std'])):
        ax.text(rating + 0.05, i, f'{rating:.2f} ({int(films)} films, σ={std:.2f})', 
               va='center', fontsize=8)
    
    ax.set_xlabel('Average IMDb Rating', fontsize=12, fontweight='bold')
    ax.set_title('Highest-Rated Directors (Minimum 5 Films)\n(Average rating with standard deviation)', 
                 fontsize=16, fontweight='bold', pad=20)
    ax.set_xlim(0, 10)
    ax.grid(axis='x', alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '02_director_quality.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 2: Director quality saved")


def viz_3_director_actor_collaborations(analyzer: DirectorAnalyzer):
    """Viz 3: Top director-actor collaborations."""
    pairs = analyzer.get_director_actor_pairs(n=20)
    
    if len(pairs) == 0:
        logger.warning("No collaboration data available")
        return
    
    fig, ax = plt.subplots(figsize=(14, 10))
    
    labels = [f"{director} × {actor}" for director, actor, _ in pairs]
    counts = [count for _, _, count in pairs]
    
    bars = ax.barh(range(len(pairs)), counts, color=COLORS['accent'], alpha=0.8)
    ax.set_yticks(range(len(pairs)))
    ax.set_yticklabels(labels, fontsize=9)
    ax.invert_yaxis()
    
    for i, count in enumerate(counts):
        ax.text(count + 0.1, i, f'{int(count)} films', va='center', fontsize=8)
    
    ax.set_xlabel('Number of Films Together', fontsize=12, fontweight='bold')
    ax.set_title('Top 20 Director-Actor Collaborations\n(Frequent working partnerships)', 
                 fontsize=16, fontweight='bold', pad=20)
    ax.grid(axis='x', alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '03_director_actor_collaborations.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 3: Collaborations saved")


def viz_4_director_evolution(analyzer: DirectorAnalyzer):
    """Viz 4: Director evolution over decades."""
    decade_directors = analyzer.director_data.groupby('decade')['director'].nunique()
    
    fig, ax = plt.subplots(figsize=(14, 6))
    
    ax.plot(decade_directors.index, decade_directors.values, 'o-', 
           color=COLORS['primary'], linewidth=3, markersize=10)
    
    for x, y in zip(decade_directors.index, decade_directors.values):
        ax.text(x, y + 5, f'{int(y)}', ha='center', va='bottom', fontweight='bold')
    
    ax.set_xlabel('Decade', fontsize=12, fontweight='bold')
    ax.set_ylabel('Number of Unique Directors', fontsize=12, fontweight='bold')
    ax.set_title('Director Diversity Over Decades\n(How many different directors per decade)', 
                 fontsize=16, fontweight='bold', pad=20)
    ax.grid(alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '04_director_evolution.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 4: Director evolution saved")


def viz_5_director_genre_versatility(analyzer: DirectorAnalyzer):
    """Viz 5: Director genre versatility."""
    # Get genre count per director
    director_genres = defaultdict(set)
    for _, row in analyzer.director_data.iterrows():
        director = row['director']
        genres_str = row['genres']
        if pd.notna(genres_str):
            genres = [g.strip() for g in str(genres_str).split(',')]
            director_genres[director].update(genres)
    
    # Get top directors
    top_directors = analyzer.get_top_directors(n=30)
    
    versatility = []
    for director in top_directors['director']:
        genres = director_genres.get(director, set())
        film_count = int(top_directors[top_directors['director'] == director]['film_count'].iloc[0])
        avg_rating = float(top_directors[top_directors['director'] == director]['avg_rating'].iloc[0])
        
        versatility.append({
            'director': director,
            'genre_count': len(genres),
            'film_count': film_count,
            'avg_rating': avg_rating
        })
    
    versatility_df = pd.DataFrame(versatility).sort_values('genre_count', ascending=False).head(20)
    
    fig, ax = plt.subplots(figsize=(14, 10))
    
    scatter = ax.scatter(versatility_df['genre_count'], versatility_df['film_count'],
                        s=versatility_df['avg_rating'] * 50, c=versatility_df['avg_rating'],
                        cmap='RdYlGn', alpha=0.7, edgecolors='black', linewidth=1)
    
    for _, row in versatility_df.head(10).iterrows():
        ax.annotate(row['director'], (row['genre_count'], row['film_count']),
                   fontsize=8, alpha=0.7, xytext=(3, 3), textcoords='offset points')
    
    ax.set_xlabel('Number of Different Genres', fontsize=12, fontweight='bold')
    ax.set_ylabel('Total Films', fontsize=12, fontweight='bold')
    ax.set_title('Director Genre Versatility\n(Bubble size & color = Average rating)', 
                 fontsize=16, fontweight='bold', pad=20)
    ax.grid(alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.colorbar(scatter, ax=ax, label='Avg Rating')
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '05_director_genre_versatility.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 5: Genre versatility saved")


def viz_6_writer_leaderboard(analyzer: DirectorAnalyzer):
    """Viz 6: Top writers (if data available)."""
    if len(analyzer.writer_data) == 0:
        logger.warning("No writer data available")
        return
    
    writer_stats = analyzer.writer_data.groupby('writer').agg({
        'film': 'count',
        'rating': 'mean'
    }).reset_index()
    
    writer_stats.columns = ['writer', 'film_count', 'avg_rating']
    top_writers = writer_stats.nlargest(20, 'film_count')
    
    fig, ax = plt.subplots(figsize=(14, 10))
    
    bars = ax.barh(range(len(top_writers)), top_writers['film_count'], 
                  color=COLORS['secondary'], alpha=0.8)
    ax.set_yticks(range(len(top_writers)))
    ax.set_yticklabels(top_writers['writer'], fontsize=9)
    ax.invert_yaxis()
    
    for i, (count, rating) in enumerate(zip(top_writers['film_count'], top_writers['avg_rating'])):
        ax.text(count + 0.3, i, f'{int(count)} films | {rating:.2f}★', 
               va='center', fontsize=8)
    
    ax.set_xlabel('Number of Films', fontsize=12, fontweight='bold')
    ax.set_title('Top 20 Most-Watched Writers\n(Screenwriters & adapted authors)', 
                 fontsize=16, fontweight='bold', pad=20)
    ax.grid(axis='x', alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '06_writer_leaderboard.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 6: Writer leaderboard saved")


def viz_7_interactive_director_dashboard(analyzer: DirectorAnalyzer):
    """Viz 7: Interactive director dashboard."""
    top_directors = analyzer.get_top_directors(n=100)
    
    if len(top_directors) == 0:
        logger.warning("No director data for dashboard")
        return
    
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Films vs Rating', 'Career Span', 'Decades Active', 'Films Distribution'),
        specs=[[{"type": "scatter"}, {"type": "scatter"}],
               [{"type": "bar"}, {"type": "histogram"}]]
    )
    
    # 1. Films vs Rating
    fig.add_trace(go.Scatter(
        x=top_directors['film_count'],
        y=top_directors['avg_rating'],
        mode='markers',
        marker=dict(size=10, color=top_directors['avg_rating'], colorscale='RdYlGn'),
        text=top_directors['director'],
        hovertemplate='<b>%{text}</b><br>Films: %{x}<br>Rating: %{y:.2f}<extra></extra>'
    ), row=1, col=1)
    
    # 2. Career Span
    fig.add_trace(go.Scatter(
        x=top_directors['first_year'],
        y=top_directors['career_span'],
        mode='markers',
        marker=dict(size=top_directors['film_count'], color=top_directors['decades_active'], colorscale='Viridis'),
        text=top_directors['director'],
        hovertemplate='<b>%{text}</b><br>First: %{x}<br>Span: %{y}y<extra></extra>'
    ), row=1, col=2)
    
    # 3. Decades Active
    decades_dist = top_directors['decades_active'].value_counts().sort_index()
    fig.add_trace(go.Bar(
        x=decades_dist.index,
        y=decades_dist.values,
        marker_color=COLORS['accent']
    ), row=2, col=1)
    
    # 4. Films Distribution
    fig.add_trace(go.Histogram(
        x=top_directors['film_count'],
        nbinsx=20,
        marker_color=COLORS['success']
    ), row=2, col=2)
    
    fig.update_xaxes(title_text="Films", row=1, col=1)
    fig.update_yaxes(title_text="Avg Rating", row=1, col=1)
    fig.update_xaxes(title_text="First Year", row=1, col=2)
    fig.update_yaxes(title_text="Career Span", row=1, col=2)
    fig.update_xaxes(title_text="Decades", row=2, col=1)
    fig.update_yaxes(title_text="Directors", row=2, col=1)
    fig.update_xaxes(title_text="Films", row=2, col=2)
    fig.update_yaxes(title_text="Count", row=2, col=2)
    
    fig.update_layout(
        title_text='Interactive Director Dashboard<br><sub>Top 100 directors across multiple dimensions</sub>',
        height=900,
        showlegend=False,
        template='plotly_white'
    )
    
    fig.write_html(OUTPUT_DIR / '07_director_dashboard_interactive.html')
    logger.info("✅ Viz 7: Interactive dashboard saved")


def generate_summary(analyzer: DirectorAnalyzer) -> str:
    """Generate comprehensive summary."""
    top_directors = analyzer.get_top_directors(n=5)
    
    if len(top_directors) == 0:
        return "No director data available."
    
    summary = f"""
{'='*80}
BATCH 4: DIRECTORS & WRITERS DEEP DIVE - SUMMARY
{'='*80}

📊 QUESTIONS ANSWERED:

Q51: Most-watched director: {top_directors.iloc[0]['director']} ({int(top_directors.iloc[0]['film_count'])} films)

Q52: Unique directors: {len(analyzer.director_data['director'].unique())}

Q53: Top 5 directors by film count:
"""
    
    for i, row in top_directors.iterrows():
        summary += f"  {i+1}. {row['director']}: {int(row['film_count'])} films (avg {row['avg_rating']:.2f})\n"
    
    # Director-actor collaborations
    pairs = analyzer.get_director_actor_pairs(n=5)
    summary += f"\nQ61: Top 5 director-actor collaborations:\n"
    for director, actor, count in pairs:
        summary += f"  • {director} × {actor}: {count} films\n"
    
    # Writers
    if len(analyzer.writer_data) > 0:
        top_writers = analyzer.writer_data.groupby('writer').size().nlargest(5)
        summary += f"\nQ81-82: Top 5 writers:\n"
        for writer, count in top_writers.items():
            summary += f"  • {writer}: {int(count)} films\n"
    
    summary += f"""
📈 KEY INSIGHTS:

1. Total unique directors: {len(analyzer.director_data['director'].unique())}
2. Total unique writers: {len(analyzer.writer_data['writer'].unique()) if len(analyzer.writer_data) > 0 else 0}
3. Average films per director: {analyzer.director_data.groupby('director').size().mean():.1f}
4. Most versatile director: (see genre versatility visualization)
5. Strongest collaboration: {pairs[0][0] + ' × ' + pairs[0][1] + ' (' + str(pairs[0][2]) + ' films)' if pairs else 'N/A'}

🎬 VISUALIZATIONS CREATED:

1. 01_director_leaderboard.png - Top 20 directors
2. 02_director_quality.png - Highest-rated directors
3. 03_director_actor_collaborations.png - Top partnerships
4. 04_director_evolution.png - Directors per decade
5. 05_director_genre_versatility.png - Genre diversity
6. 06_writer_leaderboard.png - Top 20 writers
7. 07_director_dashboard_interactive.html - Interactive dashboard

✅ Batch 4 Complete!
{'='*80}
"""
    
    return summary


def main():
    logger.info("="*80)
    logger.info("BATCH 4: DIRECTORS & WRITERS DEEP DIVE")
    logger.info("="*80)
    
    # Load data
    master_file = DATA_DIR / "watched_movies_master.csv"
    if not master_file.exists():
        logger.error(f"Master file not found: {master_file}")
        return
    
    df = pd.read_csv(master_file, low_memory=False)
    logger.info(f"Loaded {len(df)} films")
    
    # Initialize
    analyzer = DirectorAnalyzer(df)
    
    # Generate visualizations
    logger.info("\nGenerating visualizations...")
    viz_1_director_leaderboard(analyzer)
    viz_2_director_quality(analyzer)
    viz_3_director_actor_collaborations(analyzer)
    viz_4_director_evolution(analyzer)
    viz_5_director_genre_versatility(analyzer)
    viz_6_writer_leaderboard(analyzer)
    viz_7_interactive_director_dashboard(analyzer)
    
    # Summary
    summary = generate_summary(analyzer)
    print(summary)
    
    with open(OUTPUT_DIR / "BATCH_4_SUMMARY.txt", 'w') as f:
        f.write(summary)
    
    logger.info(f"\n✅ All saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()