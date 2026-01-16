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

COLORS = {
    'Male': '#27AE60',      # Forest green
    'Female': '#F39C12',    # Warm orange
    'Non-binary': '#9B59B6', # Purple
    'Unknown': '#95A5A6'    # Gray
}
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

    # Convert films to integer
    top_20['films'] = top_20['films'].astype(int)

    fig, ax = plt.subplots(figsize=(14, 10))
    colors = [COLORS.get(g, COLORS['Unknown']) for g in top_20['gender']]
    ax.barh(range(len(top_20)), top_20['films'], color=colors)
    ax.set_yticks(range(len(top_20)))
    ax.set_yticklabels(top_20['actor_name'])
    ax.invert_yaxis()
    for i, (cnt, rating) in enumerate(zip(top_20['films'], top_20['avg_rating'])):
        ax.text(cnt + 0.3, i, f'{cnt} • {rating:.2f}', va='center', fontsize=9)
    ax.set_xlabel('Number of Films', fontsize=12, fontweight='bold')
    ax.set_title('Top 20 Most-Watched Actors', fontsize=16, fontweight='bold', pad=20)
    ax.grid(axis='x', alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    # Force integer ticks on x-axis
    ax.xaxis.set_major_locator(plt.MaxNLocator(integer=True))

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '01_top_20_actors.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 1 saved")
    
    # Viz 2: Top by gender (dynamic based on actual genders present)
    available_genders = actors_df['gender'].unique()
    # Order preference: Male, Female, Non-binary, Unknown
    gender_order = ['Male', 'Female', 'Non-binary', 'Unknown']
    genders_to_show = [g for g in gender_order if g in available_genders]

    # Dynamic sizing: more width for smaller categories to prevent label overlap
    fig_width = sum([5 if g in ['Male', 'Female'] else 7 for g in genders_to_show])
    fig, axes = plt.subplots(1, len(genders_to_show), figsize=(fig_width, 10))
    if len(genders_to_show) == 1:
        axes = [axes]  # Make it iterable

    for ax, gender in zip(axes, genders_to_show):
        top = (actors_df[actors_df["gender"] == gender]
               .groupby(["actor_id", "actor_name"], as_index=False)
               .agg(films=("film_id", "nunique"))
               .nlargest(15, "films"))

        if len(top) > 0:
            # Convert films to integer (currently float from nunique)
            top['films'] = top['films'].astype(int)

            # Adjust font size based on category size
            font_size = 8 if gender in ['Male', 'Female'] else 9

            ax.barh(range(len(top)), top['films'], color=COLORS.get(gender), alpha=0.8)
            ax.set_yticks(range(len(top)))
            ax.set_yticklabels(top['actor_name'], fontsize=font_size)
            ax.invert_yaxis()

            # Add film count labels on bars (as integers)
            for i, films in enumerate(top['films']):
                ax.text(films + 0.1, i, str(int(films)), va='center', fontsize=8, color='black')

        ax.set_title(f'Top 15 {gender}', fontsize=14, fontweight='bold', pad=10)
        ax.set_xlabel('Films', fontsize=10)
        ax.grid(axis='x', alpha=0.3)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        # Force integer ticks on x-axis
        ax.xaxis.set_major_locator(plt.MaxNLocator(integer=True))

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '02_top_by_gender.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 2 saved")
    
    # Viz 3: Gender distribution (dynamic - includes all genders present)
    unique = actors_df.drop_duplicates("actor_id")[["actor_id", "gender"]]
    counts = unique['gender'].value_counts()
    total = len(unique)

    # Get all genders present in order
    genders_present = [g for g in gender_order if counts.get(g, 0) > 0]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    sizes = [counts.get(k, 0) for k in genders_present]
    colors = [COLORS[k] for k in genders_present]

    # Use autopct only for slices > 1%
    def autopct_format(pct):
        return f'{pct:.1f}%' if pct > 1 else ''

    # Create pie WITHOUT labels - we'll add a legend instead
    wedges, texts, autotexts = ax1.pie(
        sizes,
        colors=colors,
        autopct=autopct_format,
        startangle=90,
        pctdistance=0.75,
        textprops={'fontsize': 11, 'fontweight': 'bold'}
    )

    # Make percentage text white for visibility on slices
    for autotext in autotexts:
        autotext.set_color('white')

    # Create legend with counts (avoids overlapping labels entirely)
    legend_labels = [f"{k}: {counts.get(k, 0):,}" for k in genders_present]
    ax1.legend(wedges, legend_labels, title="Gender", loc="center left", 
               bbox_to_anchor=(1, 0, 0.5, 1), fontsize=10)

    ax1.set_title(f"Gender Distribution ({total:,} actors)", fontsize=14, fontweight='bold')

    ax2.bar(genders_present, sizes, color=colors)
    ax2.set_ylabel('Count', fontsize=11)
    ax2.set_title('Actor Count by Gender', fontsize=14, fontweight='bold')
    ax2.grid(axis='y', alpha=0.3)
    # Force integer y-axis for bar chart
    ax2.yaxis.set_major_locator(plt.MaxNLocator(integer=True))

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '03_gender_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 3 saved")
    
    logger.info(f"\n✅ Batch 3 Part 1 Complete! ({OUTPUT_DIR})")


if __name__ == "__main__":
    main()
