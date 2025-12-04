"""
CineScope Batch 3 Parts 4-5: Actor Age, Career & Character Analysis
Answers Questions 31-40 with career, longevity, and character analysis.
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
from collections import Counter

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "scripts"))

from lib.actors_data import load_actor_pairs

OUTPUT_DIR = BASE_DIR / "analysis_outputs" / "visualizations" / "batch_3" / "parts_4_5"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

COLORS = {'Male': '#2196F3', 'Female': '#E91E63', 'Unknown': '#999999'}
plt.rcParams['figure.dpi'] = 300


def get_generation(year):
    """Classify generation by year."""
    if pd.isna(year):
        return 'Unknown'
    year = int(year)
    if year < 1950:
        return 'Silent/Early'
    elif year < 1970:
        return 'Golden Age'
    elif year < 1985:
        return 'Gen X'
    elif year < 2005:
        return 'Millennials'
    else:
        return 'Gen Z'


def main():
    logger.info("="*80)
    logger.info("BATCH 3 PARTS 4-5: ACTOR AGE, CAREER & CHARACTER ANALYSIS")
    logger.info("="*80)
    
    actors_df, actors_src = load_actor_pairs(prefer_batch2=True, verbose=True)
    logger.info(f"[INFO] Using canonical actors table from Batch 2")
    
    # Career span analysis
    career_stats = (actors_df.groupby(["actor_id", "actor_name", "gender"], as_index=False)
                             .agg(films=("film_id", "nunique"),
                                  start_year=("film_year", "min"),
                                  end_year=("film_year", "max"),
                                  avg_rating=("rating", "mean"),
                                  max_rating=("rating", "max"),
                                  min_rating=("rating", "min")))
    career_stats['career_span'] = career_stats['end_year'] - career_stats['start_year']
    career_stats['generation'] = career_stats['start_year'].apply(get_generation)
    
    logger.info(f"Longest career: {career_stats.loc[career_stats['career_span'].idxmax(), 'actor_name']} ({career_stats['career_span'].max()} years)")
    
    # Viz 22: Age distribution (by generation)
    fig, ax = plt.subplots(figsize=(12, 6))
    gen_counts = career_stats['generation'].value_counts()
    gen_order = ['Silent/Early', 'Golden Age', 'Gen X', 'Millennials', 'Gen Z']
    gen_counts = gen_counts.reindex([g for g in gen_order if g in gen_counts.index])
    gen_counts.plot(kind='bar', ax=ax, color='#3498DB', alpha=0.8)
    ax.set_title('Age Distribution: Generational Cohorts', fontsize=14, fontweight='bold')
    ax.set_ylabel('Number of Actors')
    ax.set_xlabel('Generation (First Film Year)')
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'viz_22_age_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 22 saved")
    
    # Viz 23: Career evolution (active years distribution)
    fig, ax = plt.subplots(figsize=(12, 6))
    male_spans = career_stats[career_stats["gender"] == "Male"]["career_span"]
    female_spans = career_stats[career_stats["gender"] == "Female"]["career_span"]
    
    ax.hist([male_spans, female_spans], bins=30, label=["Male", "Female"], 
            color=[COLORS['Male'], COLORS['Female']], alpha=0.7)
    ax.set_xlabel("Career Span (Years)", fontsize=12, fontweight='bold')
    ax.set_ylabel("Number of Actors", fontsize=12, fontweight='bold')
    ax.set_title("Career Evolution: Active Years Distribution by Gender", fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'viz_23_career_evolution.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 23 saved")
    
    # Viz 24: Generation cohorts (detailed breakdown)
    fig = make_subplots(rows=1, cols=2, subplot_titles=('Count by Generation', 'Avg Career Span'))
    
    gen_summary = career_stats.groupby('generation').agg(
        count=('actor_id', 'count'),
        avg_span=('career_span', 'mean')
    ).reindex([g for g in get_generation.__doc__ and ['Silent/Early', 'Golden Age', 'Gen X', 'Millennials', 'Gen Z'] or [] 
              if g in career_stats['generation'].unique()])
    
    fig.add_trace(go.Bar(x=gen_summary.index, y=gen_summary['count'], name='Count'),
                 row=1, col=1)
    fig.add_trace(go.Bar(x=gen_summary.index, y=gen_summary['avg_span'], name='Avg Span'),
                 row=1, col=2)
    
    fig.update_layout(height=500, title_text="Viz 24: Generation Cohorts", showlegend=False)
    fig.write_html(OUTPUT_DIR / 'viz_24_generation_cohorts.html')
    logger.info("✅ Viz 24 saved")
    
    # Viz 25: Career peak decades
    peak_decades = {}
    for _, row in career_stats.iterrows():
        if pd.notna(row['start_year']) and pd.notna(row['end_year']):
            decade_range = range(int(row['start_year'])//10*10, int(row['end_year'])//10*10+10, 10)
            for decade in decade_range:
                peak_decades[decade] = peak_decades.get(decade, 0) + 1
    
    fig, ax = plt.subplots(figsize=(14, 6))
    decades = sorted(peak_decades.keys())
    counts = [peak_decades[d] for d in decades]
    ax.bar([f"{d}s" for d in decades], counts, color='#E74C3C', alpha=0.8)
    ax.set_title('Career Peak Decades: Actor Activity by Decade', fontsize=14, fontweight='bold')
    ax.set_ylabel('Number of Active Actors')
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'viz_25_career_peak_decades.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 25 saved")
    
    # Viz 26: Longevity analysis (top long-career actors)
    top_longevity = career_stats.nlargest(20, 'career_span')
    fig, ax = plt.subplots(figsize=(14, 10))
    colors_long = [COLORS.get(g, '#999999') for g in top_longevity['gender']]
    ax.barh(range(len(top_longevity)), top_longevity['career_span'], color=colors_long)
    ax.set_yticks(range(len(top_longevity)))
    ax.set_yticklabels(top_longevity['actor_name'], fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel('Career Span (Years)', fontsize=12, fontweight='bold')
    ax.set_title('Top 20 Longest Career Spans', fontsize=14, fontweight='bold')
    ax.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'viz_26_longevity_analysis.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 26 saved")
    
    # Viz 27: Actor type classification (prolific vs selective)
    fig, ax = plt.subplots(figsize=(12, 8))
    ax.scatter(career_stats['films'], career_stats['avg_rating'], 
              s=career_stats['career_span']*2, alpha=0.4, c=career_stats['career_span'], cmap='coolwarm')
    ax.set_xlabel('Number of Films', fontsize=12, fontweight='bold')
    ax.set_ylabel('Average Rating', fontsize=12, fontweight='bold')
    ax.set_title('Actor Type Classification\n(X=Prolific, Y=Quality, Size=Longevity)', fontsize=14, fontweight='bold')
    ax.grid(alpha=0.3)
    cbar = plt.colorbar(ax.collections[0], ax=ax)
    cbar.set_label('Career Span (Years)')
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'viz_27_actor_type_classification.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 27 saved")
    
    # Viz 28: Genre crossing (genre diversity analysis)
    def count_genres(genres_series):
        try:
            genres_str = ','.join([str(g) for g in genres_series if pd.notna(g)])
            return len(set([g.strip() for g in genres_str.split(',') if g.strip()]))
        except:
            return 1
    
    genre_diversity = []
    for _, count in actors_df.groupby('actor_id')['genres'].apply(count_genres).items():
        genre_diversity.append(count)
    
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.hist(genre_diversity, bins=20, color='#F39C12', alpha=0.7, edgecolor='black')
    ax.set_xlabel('Number of Genres', fontsize=12, fontweight='bold')
    ax.set_ylabel('Number of Actors', fontsize=12, fontweight='bold')
    ax.set_title('Genre Crossing: How Many Genres per Actor?', fontsize=14, fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'viz_28_genre_crossing.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 28 saved")
    
    # Viz 29: Acting range (rating spread)
    career_stats['rating_range'] = career_stats['max_rating'] - career_stats['min_rating']
    top_range = career_stats[career_stats['films'] >= 5].nlargest(20, 'rating_range')
    
    fig, ax = plt.subplots(figsize=(14, 10))
    colors_range = [COLORS.get(g, '#999999') for g in top_range['gender']]
    ax.barh(range(len(top_range)), top_range['rating_range'], color=colors_range)
    ax.set_yticks(range(len(top_range)))
    ax.set_yticklabels(top_range['actor_name'], fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel('Rating Range (Max - Min)', fontsize=12, fontweight='bold')
    ax.set_title('Top 20 Actors by Acting Range (min 5 films)', fontsize=14, fontweight='bold')
    ax.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'viz_29_acting_range.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 29 saved")
    
    # Viz 30: Ensemble vs solo performers
    ensemble_threshold = actors_df['film_id'].apply(
        lambda x: len(actors_df[actors_df['film_id']==x]['actor_id'].unique())
    )
    
    fig, ax = plt.subplots(figsize=(12, 6))
    male_ensemble = len(actors_df[(actors_df['gender']=='Male') & (ensemble_threshold > 5)])
    male_solo = len(actors_df[(actors_df['gender']=='Male') & (ensemble_threshold <= 5)])
    female_ensemble = len(actors_df[(actors_df['gender']=='Female') & (ensemble_threshold > 5)])
    female_solo = len(actors_df[(actors_df['gender']=='Female') & (ensemble_threshold <= 5)])
    
    x = np.arange(2)
    width = 0.35
    ax.bar(x - width/2, [male_ensemble, female_ensemble], width, label='Ensemble (5+ cast)', color='#3498DB')
    ax.bar(x + width/2, [male_solo, female_solo], width, label='Solo/Limited', color='#E74C3C')
    ax.set_xticks(x)
    ax.set_xticklabels(['Male', 'Female'])
    ax.set_ylabel('Number of Appearances')
    ax.set_title('Ensemble vs Solo Performers', fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'viz_30_ensemble_vs_solo.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 30 saved")
    
    logger.info(f"\n✅ Batch 3 Parts 4-5 Complete! ({OUTPUT_DIR})")


if __name__ == "__main__":
    main()
