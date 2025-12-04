#!/usr/bin/env python3
"""
CineScope Batch 3 Part 1: Top Performers & Basic Statistics
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

OUTPUT_DIR = BASE_DIR / "analysis_outputs" / "visualizations" / "batch_3" / "part_1"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

COLORS = {'Male': '#2196F3', 'Female': '#E91E63', 'Unknown': '#9E9E9E'}
plt.rcParams['figure.dpi'] = 300


def main():
    logger.info("="*80)
    logger.info("BATCH 3 PART 1: TOP PERFORMERS & BASIC STATISTICS")
    logger.info("="*80)
    
    actors_df, actors_src = load_actor_pairs(prefer_batch2=True, verbose=True)
    logger.info(f"[INFO] Initialized from canonical actors table")
    
    # Viz 1: Top 20 actors
    top_20 = (actors_df.groupby(["actor_id", "actor_name", "gender"], as_index=False)
                       .agg(films=("film_id", "nunique"), avg_rating=("rating", "mean"))
                       .sort_values(["films", "avg_rating"], ascending=[False, False])
                       .head(20))
    
    fig, ax = plt.subplots(figsize=(14, 10))
    colors = [COLORS.get(g, COLORS['Unknown']) for g in top_20['gender']]
    ax.barh(range(len(top_20)), top_20['films'], color=colors)
    ax.set_yticks(range(len(top_20)))
    ax.set_yticklabels(top_20['actor_name'])
    ax.invert_yaxis()
    for i, (cnt, rating) in enumerate(zip(top_20['films'], top_20['avg_rating'])):
        ax.text(cnt + 0.3, i, f'{int(cnt)} • {rating:.2f}', va='center', fontsize=9)
    ax.set_xlabel('Number of Films', fontsize=12, fontweight='bold')
    ax.set_title('Top 20 Most-Watched Actors', fontsize=16, fontweight='bold', pad=20)
    ax.grid(axis='x', alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '01_top_20_actors.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 1 saved")
    
    # Viz 2: Top by gender
    fig, axes = plt.subplots(1, 3, figsize=(20, 8))
    for ax, gender in zip(axes, ['Male', 'Female', 'Unknown']):
        top = (actors_df[actors_df["gender"] == gender]
               .groupby(["actor_id", "actor_name"], as_index=False)
               .agg(films=("film_id", "nunique"))
               .nlargest(15, "films"))
        if len(top) > 0:
            ax.barh(range(len(top)), top['films'], color=COLORS.get(gender), alpha=0.8)
            ax.set_yticks(range(len(top)))
            ax.set_yticklabels(top['actor_name'], fontsize=9)
            ax.invert_yaxis()
        ax.set_title(f'Top 15 {gender}', fontsize=14, fontweight='bold')
        ax.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '02_top_by_gender.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 2 saved")
    
    # Viz 3: Gender distribution
    unique = actors_df.drop_duplicates("actor_id")[["actor_id", "gender"]]
    counts = unique['gender'].value_counts()
    total = len(unique)
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    labels = [f"{k}\n{counts.get(k, 0):,}" for k in ['Male', 'Female', 'Unknown']]
    sizes = [counts.get(k, 0) for k in ['Male', 'Female', 'Unknown']]
    colors = [COLORS[k] for k in ['Male', 'Female', 'Unknown']]
    
    ax1.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%')
    ax1.set_title(f"Gender Distribution ({total:,} actors)", fontsize=14, fontweight='bold')
    
    ax2.bar(['Male', 'Female', 'Unknown'], sizes, color=colors)
    ax2.set_ylabel('Count')
    ax2.set_title('Actor Count by Gender', fontsize=14, fontweight='bold')
    ax2.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '03_gender_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 3 saved")
    
    logger.info(f"\n✅ Batch 3 Part 1 Complete! ({OUTPUT_DIR})")


if __name__ == "__main__":
    main()
