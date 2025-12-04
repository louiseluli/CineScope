"""
CineScope Batch 3 Part 3: Actor Comparisons & Quality Analysis
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

OUTPUT_DIR = BASE_DIR / "analysis_outputs" / "visualizations" / "batch_3" / "part_3"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

COLORS = {'Male': '#2196F3', 'Female': '#E91E63', 'Unknown': '#999999'}
plt.rcParams['figure.dpi'] = 300


def main():
    logger.info("="*80)
    logger.info("BATCH 3 PART 3: ACTOR COMPARISONS & QUALITY ANALYSIS")
    logger.info("="*80)
    
    actors_df, actors_src = load_actor_pairs(prefer_batch2=True, verbose=True)
    logger.info(f"[INFO] Using canonical actors table from Batch 2")
    
    # Aggregate stats
    actor_stats = (actors_df.groupby(["actor_id", "actor_name", "gender"], as_index=False)
                            .agg(films=("film_id", "nunique"),
                                 avg_rating=("rating", "mean"),
                                 max_rating=("rating", "max"),
                                 min_rating=("rating", "min"))
                            .sort_values("avg_rating", ascending=False))
    
    logger.info(f"Top actor by quality: {actor_stats.iloc[0]['actor_name']} (avg {actor_stats.iloc[0]['avg_rating']:.2f})")
    
    # Viz 1: Gender preference (film count)
    male = actors_df[actors_df["gender"] == "Male"]["film_id"].nunique()
    female = actors_df[actors_df["gender"] == "Female"]["film_id"].nunique()
    
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(["Male", "Female"], [male, female], color=[COLORS['Male'], COLORS['Female']], alpha=0.8)
    ax.set_ylabel("Films", fontsize=12, fontweight='bold')
    ax.set_title("Gender Preference (Films)", fontsize=14, fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '16_gender_preference.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 16 saved")
    
    # Viz 2: Quality by gender
    fig, ax = plt.subplots(figsize=(12, 6))
    male_ratings = actors_df[actors_df["gender"] == "Male"]["rating"].dropna()
    female_ratings = actors_df[actors_df["gender"] == "Female"]["rating"].dropna()
    
    ax.boxplot([male_ratings, female_ratings], labels=["Male", "Female"])
    ax.set_ylabel("IMDb Rating", fontsize=12, fontweight='bold')
    ax.set_title("Quality Distribution by Gender", fontsize=14, fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '17_quality_by_gender.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 17 saved")
    
    # Viz 3: Top quality actors
    top_quality = actor_stats.head(20)
    fig, ax = plt.subplots(figsize=(14, 10))
    bar_colors = [COLORS.get(g, '#999999') for g in top_quality['gender']]
    ax.barh(range(len(top_quality)), top_quality['avg_rating'], color=bar_colors)
    ax.set_yticks(range(len(top_quality)))
    ax.set_yticklabels(top_quality['actor_name'])
    ax.invert_yaxis()
    ax.set_xlabel('Avg IMDb Rating', fontsize=12, fontweight='bold')
    ax.set_title('Top 20 Actors by Quality (Avg Rating)', fontsize=14, fontweight='bold')
    ax.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '18_top_quality_actors.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 18 saved")
    
    logger.info(f"\n✅ Batch 3 Part 3 Complete! ({OUTPUT_DIR})")


if __name__ == "__main__":
    main()
