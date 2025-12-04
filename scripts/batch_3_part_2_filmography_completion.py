"""
CineScope Batch 3 Part 2: Filmography Completion Analysis
===========================================================

Answers Questions 11-20:
- Q11-12: Actors with highest filmography completion
- Q13-14: Specific actor gaps (Vincent Price, Van Damme, etc.)
- Q15-16: Easiest completions and high completion rates
- Q17-18: Barely explored actors and percentage watched
- Q19-20: Completist status and priority targets

Visualizations:
11. Filmography Completion Leaderboard (top 20)
12. Completion Rate Distribution
13. Near-Complete Actors (75%+) - Low-Hanging Fruit
14. Major Stars with Low Completion (<25%)
15. Interactive Filmography Explorer

This requires external data (actor's complete filmography from TMDB).
We'll use TMDB API to get complete filmography for top actors.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from pathlib import Path
import logging
from typing import Dict, List, Tuple
from collections import Counter, defaultdict
import sys
import json

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.enrichment.tmdb_client import TMDbClient

# Setup paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "processed"
OUTPUT_DIR = BASE_DIR / "analysis_outputs" / "visualizations" / "batch_3" / "part_2"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CACHE_FILE = DATA_DIR / "actor_filmography_cache.json"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Professional color palette
COLORS = {
    'primary': '#2C3E50',
    'secondary': '#E74C3C',
    'accent': '#3498DB',
    'success': '#27AE60',
    'warning': '#F39C12',
    'danger': '#C0392B',
    'gradient': ['#27AE60', '#F39C12', '#E74C3C']
}

plt.rcParams['figure.dpi'] = 300
plt.rcParams['savefig.dpi'] = 300


class FilmographyAnalyzer:
    """Analyzes filmography completion rates."""
    
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.tmdb_client = TMDbClient()
        self.actor_watched = self._get_watched_actors()
        self.filmography_cache = self._load_cache()
        logger.info(f"Initialized with {len(self.actor_watched)} unique actors")
    
    def _get_watched_actors(self) -> Dict[str, Dict]:
        """Extract all watched actors with their TMDb IDs."""
        actor_dict = {}
        
        for _, film in self.df.iterrows():
            # Get cast JSON
            cast_json = film.get('tmdb_cast', [])
            
            if not isinstance(cast_json, list):
                continue
            
            for person in cast_json[:10]:
                if not isinstance(person, dict):
                    continue
                
                actor_name = person.get('name')
                actor_id = person.get('tmdb_person_id')
                
                if not actor_name or not actor_id:
                    continue
                
                if actor_name not in actor_dict:
                    actor_dict[actor_name] = {
                        'tmdb_id': actor_id,
                        'films_watched': [],
                        'gender': person.get('gender', 0)
                    }
                
                film_title = film.get('title', film.get('Title', 'Unknown'))
                actor_dict[actor_name]['films_watched'].append(film_title)
        
        # Count films
        for actor in actor_dict:
            actor_dict[actor]['count'] = len(actor_dict[actor]['films_watched'])
        
        return actor_dict
    
    def _load_cache(self) -> Dict:
        """Load filmography cache."""
        if CACHE_FILE.exists():
            with open(CACHE_FILE, 'r') as f:
                return json.load(f)
        return {}
    
    def _save_cache(self):
        """Save filmography cache."""
        with open(CACHE_FILE, 'w') as f:
            json.dump(self.filmography_cache, f, indent=2)
    
    def get_complete_filmography(self, actor_name: str, tmdb_id: int) -> Dict:
        """Get complete filmography for an actor from TMDb."""
        cache_key = str(tmdb_id)
        
        if cache_key in self.filmography_cache:
            return self.filmography_cache[cache_key]
        
        try:
            # Get person's complete filmography
            credits = self.tmdb_client._make_request(f"/person/{tmdb_id}/movie_credits")
            
            if not credits:
                return {'total': 0, 'titles': []}
            
            # Count cast appearances (not crew)
            cast_films = credits.get('cast', [])
            
            # Filter out unreleased or very obscure entries
            valid_films = [
                f for f in cast_films 
                if f.get('release_date') and f.get('vote_count', 0) > 0
            ]
            
            filmography = {
                'total': len(valid_films),
                'titles': [f.get('title') for f in valid_films if f.get('title')]
            }
            
            self.filmography_cache[cache_key] = filmography
            return filmography
            
        except Exception as e:
            logger.error(f"Error fetching filmography for {actor_name}: {e}")
            return {'total': 0, 'titles': []}
    
    def calculate_completion_rates(self, top_n: int = 100) -> pd.DataFrame:
        """Calculate completion rates for top N most-watched actors."""
        results = []
        
        # Sort actors by films watched
        sorted_actors = sorted(self.actor_watched.items(), 
                              key=lambda x: x[1]['count'], reverse=True)[:top_n]
        
        logger.info(f"Calculating filmography completion for top {top_n} actors...")
        
        for actor_name, data in sorted_actors:
            tmdb_id = data['tmdb_id']
            films_watched = data['count']
            
            # Get complete filmography
            filmography = self.get_complete_filmography(actor_name, tmdb_id)
            total_films = filmography['total']
            
            if total_films == 0:
                continue
            
            completion_rate = (films_watched / total_films) * 100
            films_missing = total_films - films_watched
            
            results.append({
                'actor': actor_name,
                'films_watched': films_watched,
                'total_films': total_films,
                'films_missing': films_missing,
                'completion_rate': completion_rate,
                'gender': data['gender']
            })
        
        self._save_cache()
        
        df = pd.DataFrame(results)
        return df.sort_values('completion_rate', ascending=False)
    
    def get_near_complete_actors(self, min_rate: float = 75.0, min_films: int = 5) -> pd.DataFrame:
        """Find actors you're close to completing."""
        completion_df = self.calculate_completion_rates(top_n=200)
        
        near_complete = completion_df[
            (completion_df['completion_rate'] >= min_rate) &
            (completion_df['films_watched'] >= min_films)
        ]
        
        return near_complete.sort_values('films_missing')
    
    def get_barely_explored(self, max_rate: float = 25.0, min_total: int = 10) -> pd.DataFrame:
        """Find major actors you've barely explored."""
        completion_df = self.calculate_completion_rates(top_n=200)
        
        barely_explored = completion_df[
            (completion_df['completion_rate'] <= max_rate) &
            (completion_df['total_films'] >= min_total)
        ]
        
        return barely_explored.sort_values('total_films', ascending=False)


def viz_11_completion_leaderboard(analyzer: FilmographyAnalyzer):
    """Visualization 11: Filmography completion leaderboard."""
    completion_df = analyzer.calculate_completion_rates(top_n=50)
    top_20 = completion_df.head(20)
    
    fig, ax = plt.subplots(figsize=(14, 10))
    
    # Color by completion rate
    colors = plt.cm.RdYlGn(top_20['completion_rate'] / 100)
    
    bars = ax.barh(range(len(top_20)), top_20['completion_rate'], color=colors)
    ax.set_yticks(range(len(top_20)))
    ax.set_yticklabels(top_20['actor'])
    ax.invert_yaxis()
    
    # Add labels with watched/total
    for i, (rate, watched, total) in enumerate(zip(top_20['completion_rate'], 
                                                    top_20['films_watched'],
                                                    top_20['total_films'])):
        label = f'{rate:.1f}% ({int(watched)}/{int(total)} films)'
        ax.text(rate + 1, i, label, va='center', fontsize=9)
    
    ax.set_xlabel('Filmography Completion Rate (%)', fontsize=12, fontweight='bold')
    ax.set_title('Top 20 Filmography Completion Rates\n(Percentage of actor\'s complete filmography you\'ve watched)', 
                 fontsize=16, fontweight='bold', pad=20)
    ax.set_xlim(0, 105)
    ax.grid(axis='x', alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'viz_11_completion_leaderboard.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 11: Completion leaderboard saved")


def viz_12_completion_distribution(analyzer: FilmographyAnalyzer):
    """Visualization 12: Distribution of completion rates."""
    completion_df = analyzer.calculate_completion_rates(top_n=100)
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Histogram
    ax1.hist(completion_df['completion_rate'], bins=20, color=COLORS['accent'], 
             alpha=0.7, edgecolor='black')
    ax1.axvline(completion_df['completion_rate'].mean(), color=COLORS['danger'], 
                linestyle='--', linewidth=2, label=f'Mean: {completion_df["completion_rate"].mean():.1f}%')
    ax1.axvline(completion_df['completion_rate'].median(), color=COLORS['success'], 
                linestyle='--', linewidth=2, label=f'Median: {completion_df["completion_rate"].median():.1f}%')
    
    ax1.set_xlabel('Completion Rate (%)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Number of Actors', fontsize=12, fontweight='bold')
    ax1.set_title('Filmography Completion Distribution (Top 100 Actors)', 
                  fontsize=14, fontweight='bold', pad=15)
    ax1.legend(frameon=False)
    ax1.grid(alpha=0.3)
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    
    # Box plot by completion tiers
    completion_df['tier'] = pd.cut(completion_df['completion_rate'], 
                                    bins=[0, 25, 50, 75, 100],
                                    labels=['0-25%', '25-50%', '50-75%', '75-100%'])
    
    tier_counts = completion_df['tier'].value_counts().sort_index()
    colors_box = [COLORS['danger'], COLORS['warning'], COLORS['accent'], COLORS['success']]
    
    bars = ax2.bar(range(len(tier_counts)), tier_counts.values, color=colors_box, alpha=0.7)
    ax2.set_xticks(range(len(tier_counts)))
    ax2.set_xticklabels(tier_counts.index)
    
    for bar, count in zip(bars, tier_counts.values):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2, height + 0.5, 
                f'{int(count)}', ha='center', va='bottom', fontweight='bold')
    
    ax2.set_xlabel('Completion Rate Tier', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Number of Actors', fontsize=12, fontweight='bold')
    ax2.set_title('Actors by Completion Tier', fontsize=14, fontweight='bold', pad=15)
    ax2.grid(axis='y', alpha=0.3)
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'viz_12_completion_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 12: Completion distribution saved")


def viz_13_near_complete_actors(analyzer: FilmographyAnalyzer):
    """Visualization 13: Actors you're close to completing (75%+)."""
    near_complete = analyzer.get_near_complete_actors(min_rate=75.0, min_films=5)
    
    if len(near_complete) == 0:
        logger.warning("No near-complete actors found. Skipping viz 13.")
        return
    
    top_30 = near_complete.head(30)
    
    fig, ax = plt.subplots(figsize=(14, 12))
    
    # Color by how many films missing
    colors = plt.cm.RdYlGn_r(top_30['films_missing'] / top_30['films_missing'].max())
    
    bars = ax.barh(range(len(top_30)), top_30['completion_rate'], color=colors)
    ax.set_yticks(range(len(top_30)))
    ax.set_yticklabels(top_30['actor'], fontsize=9)
    ax.invert_yaxis()
    
    # Add labels
    for i, (rate, watched, total, missing) in enumerate(zip(top_30['completion_rate'],
                                                              top_30['films_watched'],
                                                              top_30['total_films'],
                                                              top_30['films_missing'])):
        label = f'{rate:.1f}% - {int(missing)} films to complete'
        ax.text(rate + 1, i, label, va='center', fontsize=8)
    
    ax.set_xlabel('Completion Rate (%)', fontsize=12, fontweight='bold')
    ax.set_title('Near-Complete Filmographies (75%+)\n"Low-Hanging Fruit" - Actors you can complete with fewest films', 
                 fontsize=16, fontweight='bold', pad=20)
    ax.set_xlim(0, 105)
    ax.grid(axis='x', alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'viz_13_near_complete.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 13: Near-complete actors saved")


def viz_14_barely_explored(analyzer: FilmographyAnalyzer):
    """Visualization 14: Major actors with low completion rates."""
    barely_explored = analyzer.get_barely_explored(max_rate=25.0, min_total=15)
    
    if len(barely_explored) == 0:
        logger.warning("No barely-explored actors found. Skipping viz 14.")
        return
    
    top_20 = barely_explored.head(20)
    
    fig, ax = plt.subplots(figsize=(14, 10))
    
    # Create grouped bars
    x = np.arange(len(top_20))
    width = 0.35
    
    bars1 = ax.bar(x - width/2, top_20['films_watched'], width, 
                   label='Watched', color=COLORS['success'], alpha=0.7)
    bars2 = ax.bar(x + width/2, top_20['films_missing'], width, 
                   label='Missing', color=COLORS['danger'], alpha=0.7)
    
    ax.set_xticks(x)
    ax.set_xticklabels(top_20['actor'], rotation=45, ha='right', fontsize=9)
    ax.set_ylabel('Number of Films', fontsize=12, fontweight='bold')
    ax.set_title('Barely Explored Major Actors (<25% completion)\nGreen = Watched | Red = Missing', 
                 fontsize=16, fontweight='bold', pad=20)
    ax.legend(frameon=False)
    ax.grid(axis='y', alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    # Add completion % on top
    for i, rate in enumerate(top_20['completion_rate']):
        total_height = top_20.iloc[i]['total_films']
        ax.text(i, total_height + 1, f'{rate:.1f}%', 
               ha='center', va='bottom', fontsize=8, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'viz_14_barely_explored.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 14: Barely explored saved")


def viz_15_interactive_filmography_explorer(analyzer: FilmographyAnalyzer):
    """Visualization 15: Interactive filmography completion explorer."""
    completion_df = analyzer.calculate_completion_rates(top_n=100)
    
    fig = go.Figure()
    
    # Scatter plot with completion rate vs total films
    fig.add_trace(go.Scatter(
        x=completion_df['total_films'],
        y=completion_df['completion_rate'],
        mode='markers',
        marker=dict(
            size=completion_df['films_watched'],
            color=completion_df['completion_rate'],
            colorscale='RdYlGn',
            showscale=True,
            colorbar=dict(title="Completion %"),
            line=dict(color='white', width=1)
        ),
        text=completion_df['actor'],
        customdata=np.column_stack((
            completion_df['films_watched'],
            completion_df['total_films'],
            completion_df['films_missing'],
            completion_df['completion_rate'].round(1)
        )),
        hovertemplate=(
            '<b>%{text}</b><br>' +
            'Watched: %{customdata[0]} films<br>' +
            'Total Filmography: %{customdata[1]} films<br>' +
            'Missing: %{customdata[2]} films<br>' +
            'Completion: %{customdata[3]}%<br>' +
            '<extra></extra>'
        )
    ))
    
    fig.update_layout(
        title='Interactive Filmography Completion Explorer<br><sub>Bubble size = Films watched | Color = Completion rate | Hover for details</sub>',
        xaxis_title='Total Films in Filmography',
        yaxis_title='Completion Rate (%)',
        hovermode='closest',
        template='plotly_white',
        height=800,
        font=dict(size=12)
    )
    
    fig.write_html(OUTPUT_DIR / 'viz_15_interactive_filmography.html')
    logger.info("✅ Viz 15: Interactive filmography explorer saved")


def generate_summary_stats(analyzer: FilmographyAnalyzer) -> str:
    """Generate summary statistics."""
    completion_df = analyzer.calculate_completion_rates(top_n=100)
    
    # Get top completions
    top_complete = completion_df.head(5)
    
    # Get near-complete
    near_complete = analyzer.get_near_complete_actors(min_rate=75.0, min_films=5)
    
    # Get barely explored
    barely_explored = analyzer.get_barely_explored(max_rate=25.0, min_total=10)
    
    summary = f"""
{'='*80}
BATCH 3 PART 2: FILMOGRAPHY COMPLETION ANALYSIS - SUMMARY
{'='*80}

📊 QUESTIONS ANSWERED:

Q11: Whose complete filmography have you watched most?
Top 5:
"""
    
    for i, row in top_complete.iterrows():
        summary += f"  {int(i+1)}. {row['actor']}: {row['completion_rate']:.1f}% ({int(row['films_watched'])}/{int(row['total_films'])} films)\n"
    
    summary += f"\nQ12-16: Completion insights:\n"
    summary += f"  - Average completion rate (top 100 actors): {completion_df['completion_rate'].mean():.1f}%\n"
    summary += f"  - Median completion rate: {completion_df['completion_rate'].median():.1f}%\n"
    summary += f"  - Actors with 75%+ completion: {len(near_complete)}\n"
    summary += f"  - Actors with <25% completion: {len(barely_explored)}\n"
    
    if len(near_complete) > 0:
        easiest = near_complete.iloc[0]
        summary += f"\nQ15: Easiest to complete: {easiest['actor']} ({int(easiest['films_missing'])} films needed)\n"
    
    if len(barely_explored) > 0:
        summary += f"\nQ17: Major stars barely explored (top 3):\n"
        for i, row in barely_explored.head(3).iterrows():
            summary += f"  - {row['actor']}: {row['completion_rate']:.1f}% ({int(row['films_watched'])}/{int(row['total_films'])} films)\n"
    
    summary += f"""
Q19: Am I a completist with any performer?
  - Yes! {len(completion_df[completion_df['completion_rate'] == 100])} actors at 100% completion
  - {len(completion_df[completion_df['completion_rate'] >= 90])} actors at 90%+ completion

Q20: Priority targets for 75% completion:
"""
    
    if len(near_complete) > 0:
        for i, row in near_complete.head(5).iterrows():
            summary += f"  - {row['actor']}: Need {int(row['films_missing'])} more films (currently {row['completion_rate']:.1f}%)\n"
    
    summary += f"""
📈 KEY INSIGHTS:

1. Total actors analyzed: {len(completion_df)}
2. Completist actors (100%): {len(completion_df[completion_df['completion_rate'] == 100])}
3. Near-complete (75-99%): {len(completion_df[(completion_df['completion_rate'] >= 75) & (completion_df['completion_rate'] < 100)])}
4. Moderate exploration (25-75%): {len(completion_df[(completion_df['completion_rate'] >= 25) & (completion_df['completion_rate'] < 75)])}
5. Barely explored (<25%): {len(completion_df[completion_df['completion_rate'] < 25])}

🎬 VISUALIZATIONS CREATED:

11. viz_11_completion_leaderboard.png - Top 20 completion rates
12. viz_12_completion_distribution.png - Distribution and tiers
13. viz_13_near_complete.png - Low-hanging fruit (75%+)
14. viz_14_barely_explored.png - Major stars to explore (<25%)
15. viz_15_interactive_filmography.html - Interactive explorer

⚠️  NOTE: Completion rates based on TMDb filmography data (films with releases & votes).
May not include all obscure or unreleased works.

{'='*80}
"""
    
    return summary


def main():
    """Main execution."""
    logger.info("="*80)
    logger.info("BATCH 3 PART 2: FILMOGRAPHY COMPLETION ANALYSIS")
    logger.info("="*80)
    
    # Load data
    master_file = DATA_DIR / "watched_movies_master.csv"
    if not master_file.exists():
        logger.error(f"Master file not found: {master_file}")
        return
    
    df = pd.read_csv(master_file, low_memory=False)
    logger.info(f"Loaded {len(df)} films")
    
    # Initialize analyzer
    analyzer = FilmographyAnalyzer(df)
    
    # Generate visualizations
    logger.info("\n" + "="*80)
    logger.info("GENERATING VISUALIZATIONS")
    logger.info("="*80)
    
    viz_11_completion_leaderboard(analyzer)
    viz_12_completion_distribution(analyzer)
    viz_13_near_complete_actors(analyzer)
    viz_14_barely_explored(analyzer)
    viz_15_interactive_filmography_explorer(analyzer)
    
    # Generate summary
    summary = generate_summary_stats(analyzer)
    print(summary)
    
    # Save summary
    summary_file = OUTPUT_DIR / "PART_2_SUMMARY.txt"
    with open(summary_file, 'w') as f:
        f.write(summary)
    
    logger.info(f"\n✅ All visualizations saved to: {OUTPUT_DIR}")
    logger.info(f"✅ Cache saved to: {CACHE_FILE}")


if __name__ == "__main__":
    main()