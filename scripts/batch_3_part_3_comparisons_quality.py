"""
CineScope Batch 3 Part 3: Actor Comparisons & Quality Analysis
Answers Questions 21-30 with gender, era, and quality analysis.
Uses canonical actors table from Batch 2 for consistency.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
import logging
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "scripts"))

from lib.actors_data import load_actor_pairs

OUTPUT_DIR = BASE_DIR / "analysis_outputs" / "visualizations" / "batch_3" / "part_3"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

COLORS = {'Male': '#2196F3', 'Female': '#E91E63', 'Unknown': '#999999'}
ERA_COLORS = {
    'Pre-1960': '#8B4513',
    '1960-1979': '#DAA520',
    '1980-1999': '#FF6347',
    '2000+': '#00CED1'
}
plt.rcParams['figure.dpi'] = 300


def get_actor_era(year):
    """Classify actor by era based on first film year."""
    if pd.isna(year):
        return 'Unknown'
    year = int(year)
    if year < 1960:
        return 'Pre-1960'
    elif year < 1980:
        return '1960-1979'
    elif year < 2000:
        return '1980-1999'
    else:
        return '2000+'


def main():
    logger.info("="*80)
    logger.info("BATCH 3 PART 3: ACTOR COMPARISONS & QUALITY ANALYSIS")
    logger.info("="*80)
    
    actors_df, actors_src = load_actor_pairs(prefer_batch2=True, verbose=True)
    logger.info(f"[INFO] Using canonical actors table from Batch 2")
    
    # Aggregate statistics
    actor_stats = (actors_df.groupby(["actor_id", "actor_name", "gender"], as_index=False)
                            .agg(films=("film_id", "nunique"),
                                 avg_rating=("rating", "mean"),
                                 max_rating=("rating", "max"),
                                 min_rating=("rating", "min"),
                                 first_year=("film_year", "min"),
                                 last_year=("film_year", "max"))
                            .sort_values("avg_rating", ascending=False))
    
    actor_stats['era'] = actor_stats['first_year'].apply(get_actor_era)
    actor_stats['career_span'] = actor_stats['last_year'] - actor_stats['first_year']
    
    logger.info(f"Top actor by quality: {actor_stats.iloc[0]['actor_name']} (avg {actor_stats.iloc[0]['avg_rating']:.2f})")
    
    # Viz 16: Gender preference (4 perspectives)
    fig = make_subplots(rows=2, cols=2, subplot_titles=(
        'Gender Distribution (Films)',
        'Gender Distribution (Actors)',
        'Average Rating by Gender',
        'Career Span by Gender'
    ))
    
    # 16a: Films by gender
    gender_films = actors_df.groupby('gender')['film_id'].nunique()
    fig.add_trace(
        go.Bar(x=gender_films.index, y=gender_films.values, name='Films', marker_color=list(COLORS.values())),
        row=1, col=1
    )
    
    # 16b: Actors by gender
    gender_actors = actors_df.drop_duplicates('actor_id').groupby('gender').size()
    fig.add_trace(
        go.Bar(x=gender_actors.index, y=gender_actors.values, name='Actors', marker_color=list(COLORS.values())),
        row=1, col=2
    )
    
    # 16c: Average rating by gender
    gender_rating = actor_stats.groupby('gender')['avg_rating'].mean()
    fig.add_trace(
        go.Bar(x=gender_rating.index, y=gender_rating.values, name='Avg Rating', marker_color=list(COLORS.values())),
        row=2, col=1
    )
    
    # 16d: Career span by gender
    gender_span = actor_stats.groupby('gender')['career_span'].mean()
    fig.add_trace(
        go.Bar(x=gender_span.index, y=gender_span.values, name='Career Span', marker_color=list(COLORS.values())),
        row=2, col=2
    )
    
    fig.update_layout(height=800, title_text="Viz 16: Gender Preference - 4 Perspectives", showlegend=False)
    fig.write_html(OUTPUT_DIR / 'viz_16_gender_preference.html')
    
    fig2, ax = plt.subplots(figsize=(12, 6))
    gender_films.plot(kind='bar', ax=ax, color=list(COLORS.values()), alpha=0.8)
    ax.set_title('Gender Preference (Films)', fontsize=14, fontweight='bold')
    ax.set_ylabel('Number of Films')
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'viz_16_gender_preference.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 16 saved")
    
    # Viz 17: Classic vs Modern actors
    fig, ax = plt.subplots(figsize=(12, 6))
    era_counts = actor_stats['era'].value_counts()
    colors_era = [ERA_COLORS.get(e, '#999999') for e in era_counts.index]
    era_counts.plot(kind='bar', ax=ax, color=colors_era, alpha=0.8)
    ax.set_title('Actor Era Distribution (Classic vs Modern)', fontsize=14, fontweight='bold')
    ax.set_ylabel('Number of Actors')
    ax.set_xlabel('Era')
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'viz_17_classic_vs_modern.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 17 saved")
    
    # Viz 18: Actors in top films
    top_films = actors_df.nlargest(50, 'rating')['actor_id'].value_counts().head(20)
    fig, ax = plt.subplots(figsize=(12, 8))
    ax.barh(range(len(top_films)), top_films.values, color='#FF6B6B')
    ax.set_yticks(range(len(top_films)))
    ax.set_yticklabels([actor_stats[actor_stats['actor_id']==aid]['actor_name'].values[0] 
                        for aid in top_films.index], fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel('Number of Top-Rated Films')
    ax.set_title('Frequent High-Quality Performers', fontsize=14, fontweight='bold')
    ax.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'viz_18_actors_in_top_films.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 18 saved")
    
    # Viz 19: Rating leaderboard (min 5 films)
    quality_min5 = actor_stats[actor_stats['films'] >= 5].nlargest(25, 'avg_rating')
    fig, ax = plt.subplots(figsize=(14, 10))
    colors_qual = [COLORS.get(g, '#999999') for g in quality_min5['gender']]
    ax.barh(range(len(quality_min5)), quality_min5['avg_rating'], color=colors_qual)
    ax.set_yticks(range(len(quality_min5)))
    ax.set_yticklabels(quality_min5['actor_name'], fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel('Average IMDb Rating')
    ax.set_title('Top 25 Actors by Average Rating (min 5 films)', fontsize=14, fontweight='bold')
    ax.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'viz_19_rating_leaderboard.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 19 saved")
    
    # Viz 20: Genre versatility (analyze genre diversity)
    def count_genres(genres_series):
        try:
            genres_str = ','.join([str(g) for g in genres_series if pd.notna(g)])
            return len(set([g.strip() for g in genres_str.split(',') if g.strip()]))
        except:
            return 1
    
    actor_stats['genres_list'] = actor_stats['actor_id'].map(
        actors_df.groupby('actor_id')['genres'].apply(count_genres)
    )
    versatile = actor_stats[actor_stats['films'] >= 5].nlargest(15, 'genres_list')
    
    fig, ax = plt.subplots(figsize=(12, 8))
    ax.scatter(versatile['films'], versatile['avg_rating'], 
              s=versatile['genres_list']*20, alpha=0.6, c=range(len(versatile)), cmap='viridis')
    for i, row in versatile.iterrows():
        ax.annotate(row['actor_name'], (row['films'], row['avg_rating']), fontsize=8)
    ax.set_xlabel('Number of Films')
    ax.set_ylabel('Average Rating')
    ax.set_title('Genre Versatility: Films vs Quality\n(bubble size = genre count)', fontsize=14, fontweight='bold')
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'viz_20_genre_versatility.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 20 saved")
    
    # Viz 21: Interactive quality dashboard (4-panel)
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Quality Distribution', 'Era Analysis', 'Gender Comparison', 'Rating vs Films'),
        specs=[[{"type": "histogram"}, {"type": "pie"}],
               [{"type": "box"}, {"type": "scatter"}]]
    )
    
    # Quality histogram
    fig.add_trace(go.Histogram(x=actor_stats['avg_rating'], name='Ratings', nbinsx=20),
                 row=1, col=1)
    
    # Era pie chart
    era_dist = actor_stats['era'].value_counts()
    fig.add_trace(go.Pie(labels=era_dist.index, values=era_dist.values, name='Era'),
                 row=1, col=2)
    
    # Gender boxplot
    for gender in ['Male', 'Female', 'Unknown']:
        gender_data = actor_stats[actor_stats['gender']==gender]['avg_rating']
        fig.add_trace(go.Box(y=gender_data, name=gender), row=2, col=1)
    
    # Rating vs Films scatter
    fig.add_trace(
        go.Scatter(x=actor_stats['films'], y=actor_stats['avg_rating'], 
                  mode='markers', marker=dict(size=5), name='Actors'),
        row=2, col=2
    )
    
    fig.update_layout(height=900, title_text="Viz 21: Interactive Quality Dashboard", showlegend=False)
    fig.write_html(OUTPUT_DIR / 'viz_21_interactive_quality_dashboard.html')
    logger.info("✅ Viz 21 saved")
    
    logger.info(f"\n✅ Batch 3 Part 3 Complete! ({OUTPUT_DIR})")


if __name__ == "__main__":
    main()
