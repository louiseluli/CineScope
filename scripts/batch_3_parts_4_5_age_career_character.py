"""
CineScope Batch 3 Parts 4-5: Actor Age, Career & Character Analysis
Uses canonical actors table from Batch 2 for consistency.
"""

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import logging
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "scripts"))

from lib.actors_data import load_actor_pairs

OUTPUT_DIR = BASE_DIR / "analysis_outputs" / "visualizations" / "batch_3" / "parts_4_5"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

COLORS = {'Male': '#2196F3', 'Female': '#E91E63'}
plt.rcParams['figure.dpi'] = 300


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
                                  avg_rating=("rating", "mean")))
    career_stats['career_span'] = career_stats['end_year'] - career_stats['start_year']
    
    logger.info(f"Longest career: {career_stats.loc[career_stats['career_span'].idxmax(), 'actor_name']} ({career_stats['career_span'].max()} years)")
    
    # Viz 1: Career length distribution
    fig, ax = plt.subplots(figsize=(12, 6))
    male_spans = career_stats[career_stats["gender"] == "Male"]["career_span"]
    female_spans = career_stats[career_stats["gender"] == "Female"]["career_span"]
    
    ax.hist([male_spans, female_spans], bins=20, label=["Male", "Female"], 
            color=[COLORS['Male'], COLORS['Female']], alpha=0.7)
    ax.set_xlabel("Career Span (Years)", fontsize=12, fontweight='bold')
    ax.set_ylabel("Count", fontsize=12, fontweight='bold')
    ax.set_title("Career Length Distribution by Gender", fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '19_career_length_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 19 saved")
    
    # Viz 2: Experience vs Quality
    fig, ax = plt.subplots(figsize=(12, 8))
    male_exp = career_stats[career_stats["gender"] == "Male"]
    female_exp = career_stats[career_stats["gender"] == "Female"]
    
    ax.scatter(male_exp['films'], male_exp['avg_rating'], 
              alpha=0.5, s=100, c=COLORS['Male'], label='Male')
    ax.scatter(female_exp['films'], female_exp['avg_rating'], 
              alpha=0.5, s=100, c=COLORS['Female'], label='Female')
    
    ax.set_xlabel("Films", fontsize=12, fontweight='bold')
    ax.set_ylabel("Avg IMDb Rating", fontsize=12, fontweight='bold')
    ax.set_title("Experience vs Quality", fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '20_experience_vs_quality.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 20 saved")
    
    # Viz 3: Top by career length
    top_career = career_stats.nlargest(15, 'career_span')
    fig, ax = plt.subplots(figsize=(14, 10))
    colors = [COLORS.get(g) for g in top_career['gender']]
    ax.barh(range(len(top_career)), top_career['career_span'], color=colors)
    ax.set_yticks(range(len(top_career)))
    ax.set_yticklabels(top_career['actor_name'])
    ax.invert_yaxis()
    ax.set_xlabel('Career Span (Years)', fontsize=12, fontweight='bold')
    ax.set_title('Top 15 Actors by Career Length', fontsize=14, fontweight='bold')
    ax.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '21_top_career_length.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 21 saved")
    
    logger.info(f"\n✅ Batch 3 Parts 4-5 Complete! ({OUTPUT_DIR})")


if __name__ == "__main__":
    main()
