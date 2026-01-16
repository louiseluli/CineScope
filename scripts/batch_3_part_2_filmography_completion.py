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

Uses IMDb data for filmography completion to ensure international actors
(Asian, European, etc.) are properly included. TMDB has gaps for
non-Western actors.
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
import ast
import time

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# TMDB no longer needed - using IMDb data for filmography completion
# This ensures international actors (Asian, European etc.) are included

# Setup paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "processed"
IMDB_CACHE_DIR = DATA_DIR / "imdb_cache"
OUTPUT_DIR = BASE_DIR / "analysis_outputs" / "visualizations" / "batch_3" / "part_2"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CACHE_FILE = DATA_DIR / "actor_filmography_cache.json"
COMPLETION_CACHE_FILE = DATA_DIR / "filmography_completion.parquet"

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
    """
    Analyzes filmography completion rates using IMDb data.
    
    Uses actors_master.parquet for watched actors and IMDb principals.parquet
    for total filmography counts. This ensures international actors (Asian,
    European, etc.) are included, unlike TMDB which has gaps.
    
    OPTIMIZED: Lazy loading - only loads IMDb data if cache doesn't exist.
    """
    
    def __init__(self, df: pd.DataFrame, skip_imdb_load: bool = False):
        """
        Initialize the analyzer.
        
        Args:
            df: The watched movies dataframe
            skip_imdb_load: If True, skip loading IMDb data (use when only reading from cache)
        """
        start_time = time.time()
        self.df = df.copy()
        self.imdb_principals = None
        self.imdb_names = None
        self.imdb_crew = None
        self.movie_tconsts = None
        self._imdb_loaded = False
        
        # Check if we can skip IMDb loading (if cache exists)
        if not skip_imdb_load and not COMPLETION_CACHE_FILE.exists():
            logger.info("Cache not found - loading IMDb data for initial build...")
            self._load_imdb_data()
        elif skip_imdb_load:
            logger.info("Skipping IMDb data load (using cache only)")
        else:
            logger.info("✅ Cache exists - skipping IMDb data load (fast mode)")
        
        self.actor_watched = self._get_watched_actors()
        self.filmography_cache = self._load_cache()
        
        elapsed = time.time() - start_time
        logger.info(f"Initialized with {len(self.actor_watched)} unique actors in {elapsed:.1f}s")
    
    def _ensure_imdb_loaded(self):
        """Load IMDb data on demand if not already loaded."""
        if not self._imdb_loaded:
            self._load_imdb_data()
    
    def _load_imdb_data(self):
        """Load IMDb cache data for filmography lookup."""
        if self._imdb_loaded:
            return
            
        start_time = time.time()
        logger.info("Loading IMDb data files...")
        
        principals_path = IMDB_CACHE_DIR / 'principals.parquet'
        names_path = IMDB_CACHE_DIR / 'names.parquet'
        basics_path = IMDB_CACHE_DIR / 'basics.parquet'
        crew_path = IMDB_CACHE_DIR / 'crew.parquet'
        
        # First load basics to get movie tconsts (filter out TV episodes, shorts, etc.)
        self.movie_tconsts = None
        if basics_path.exists():
            t0 = time.time()
            basics = pd.read_parquet(basics_path)
            # Include both movies AND TV movies (not TV episodes, shorts, video games, etc.)
            self.movie_tconsts = set(basics[basics['titleType'].isin(['movie', 'tvMovie'])]['tconst'].values)
            logger.info(f"  Loaded {len(self.movie_tconsts):,} movies from basics in {time.time()-t0:.1f}s")
            del basics  # Free memory
        
        if principals_path.exists():
            t0 = time.time()
            self.imdb_principals = pd.read_parquet(principals_path)
            # Filter to only actors/actresses for efficiency
            self.imdb_principals = self.imdb_principals[
                self.imdb_principals['category'].isin(['actor', 'actress', 'self'])
            ]
            # CRITICAL: Filter to only movies + TV movies (not TV episodes, shorts, etc.)
            if self.movie_tconsts is not None:
                self.imdb_principals = self.imdb_principals[
                    self.imdb_principals['tconst'].isin(self.movie_tconsts)
                ]
            logger.info(f"  Loaded {len(self.imdb_principals):,} actor entries in {time.time()-t0:.1f}s")
        else:
            logger.warning("IMDb principals.parquet not found!")
            
        if names_path.exists():
            t0 = time.time()
            self.imdb_names = pd.read_parquet(names_path)
            logger.info(f"  Loaded {len(self.imdb_names):,} names in {time.time()-t0:.1f}s")
        else:
            logger.warning("IMDb names.parquet not found!")
        
        # Load crew for director filmography
        if crew_path.exists():
            t0 = time.time()
            self.imdb_crew = pd.read_parquet(crew_path)
            # Filter to movies + TV movies only
            if self.movie_tconsts is not None:
                self.imdb_crew = self.imdb_crew[self.imdb_crew['tconst'].isin(self.movie_tconsts)]
            logger.info(f"  Loaded {len(self.imdb_crew):,} crew entries in {time.time()-t0:.1f}s")
        
        self._imdb_loaded = True
        logger.info(f"IMDb data loaded in {time.time()-start_time:.1f}s total")
    
    def _get_watched_actors(self) -> Dict[str, Dict]:
        """Extract all watched actors from actors_master.parquet (canonical source)."""
        actor_dict = {}
        
        # Load from actors_master.parquet - the canonical source for watched actors
        actors_master_path = DATA_DIR / 'actors_master.parquet'
        if actors_master_path.exists():
            try:
                actors_df = pd.read_parquet(actors_master_path)
                logger.info(f"Loading actors from actors_master.parquet: {len(actors_df)} entries")
                
                # Deduplicate - each actor should only count once per film
                actors_deduped = actors_df[['actor_id', 'actor_name', 'film_id']].drop_duplicates()
                
                # Group by actor to count films
                for (actor_id, actor_name), group in actors_deduped.groupby(['actor_id', 'actor_name']):
                    if actor_name not in actor_dict:
                        actor_dict[actor_name] = {
                            'imdb_id': actor_id,  # nconst from IMDb
                            'films_watched': group['film_id'].unique().tolist(),
                            'gender': 0  # Not available from IMDb, but not needed for main analysis
                        }
                        actor_dict[actor_name]['count'] = len(actor_dict[actor_name]['films_watched'])
            except Exception as e:
                logger.warning(f"Could not load actors_master.parquet: {e}")
        else:
            logger.error("actors_master.parquet not found! Run batch 2 first.")
        
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
    
    def _load_completion_cache(self) -> pd.DataFrame:
        """Load pre-computed completion rates from parquet cache."""
        if COMPLETION_CACHE_FILE.exists():
            df = pd.read_parquet(COMPLETION_CACHE_FILE)
            logger.info(f"Loaded completion cache: {len(df):,} actors")
            return df
        return None
    
    def _save_completion_cache(self, df: pd.DataFrame):
        """Save completion rates to parquet cache."""
        df.to_parquet(COMPLETION_CACHE_FILE, index=False)
        logger.info(f"Saved completion cache: {len(df):,} actors to {COMPLETION_CACHE_FILE}")
    
    def build_full_completion_cache(self, force_rebuild: bool = False) -> pd.DataFrame:
        """
        Build completion rates for ALL actors and cache as parquet.
        This takes a few minutes the first time, then loads instantly.
        """
        # Check if cache exists and is valid
        if not force_rebuild and COMPLETION_CACHE_FILE.exists():
            cached_df = self._load_completion_cache()
            if cached_df is not None and len(cached_df) > 0:
                return cached_df
        
        # Need to build cache - ensure IMDb data is loaded
        self._ensure_imdb_loaded()
        
        logger.info(f"Building completion cache for ALL {len(self.actor_watched):,} actors...")
        logger.info("This will take a few minutes the first time, then loads instantly on future runs.")
        start_time = time.time()
        
        results = []
        total_actors = len(self.actor_watched)
        
        for i, (actor_name, data) in enumerate(self.actor_watched.items()):
            if (i + 1) % 1000 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed
                remaining = (total_actors - i - 1) / rate
                logger.info(f"  Processing actor {i+1:,}/{total_actors:,}... ({elapsed:.0f}s elapsed, ~{remaining:.0f}s remaining)")
            
            imdb_id = data.get('imdb_id')
            films_watched = data['count']
            
            if imdb_id is None:
                continue
            
            # Get complete filmography from IMDb
            filmography = self.get_complete_filmography(actor_name, imdb_id)
            total_films = filmography['total']
            
            if total_films == 0:
                continue
            
            completion_rate = (films_watched / total_films) * 100
            films_missing = total_films - films_watched
            
            results.append({
                'actor': actor_name,
                'imdb_id': imdb_id,
                'films_watched': films_watched,
                'total_films': total_films,
                'films_missing': films_missing,
                'completion_rate': completion_rate,
                'gender': data.get('gender', 0)
            })
        
        # Save JSON cache for individual lookups
        self._save_cache()
        
        # Create and save parquet
        df = pd.DataFrame(results)
        if len(df) > 0:
            df = df.sort_values('completion_rate', ascending=False)
            self._save_completion_cache(df)
        
        logger.info(f"✅ Built completion data for {len(df):,} actors")
        return df
    
    def get_complete_filmography(self, actor_name: str, imdb_id: str) -> Dict:
        """
        Get complete filmography for an actor from IMDb principals cache.
        
        Args:
            actor_name: Actor's name (for logging)
            imdb_id: IMDb nconst (e.g., 'nm0000001')
            
        Returns:
            Dict with 'total' count and 'titles' list
        """
        if imdb_id is None:
            return {'total': 0, 'titles': []}
            
        cache_key = str(imdb_id)
        
        if cache_key in self.filmography_cache:
            return self.filmography_cache[cache_key]
        
        if self.imdb_principals is None:
            return {'total': 0, 'titles': []}
        
        try:
            # Count all films this actor appears in from IMDb principals
            actor_films = self.imdb_principals[self.imdb_principals['nconst'] == imdb_id]
            total_films = len(actor_films)
            
            # Get unique tconsts (film IDs)
            tconsts = actor_films['tconst'].unique().tolist()
            
            filmography = {
                'total': total_films,
                'titles': tconsts  # We store tconsts instead of titles for efficiency
            }
            
            self.filmography_cache[cache_key] = filmography
            return filmography
            
        except Exception as e:
            logger.error(f"Error fetching filmography for {actor_name}: {e}")
            return {'total': 0, 'titles': []}
    
    def calculate_completion_rates(self, top_n: int = 100, use_cache: bool = True) -> pd.DataFrame:
        """
        Calculate completion rates for top N most-watched actors.
        Uses parquet cache if available for speed.
        """
        # Try to use cached data first
        if use_cache and COMPLETION_CACHE_FILE.exists():
            full_df = self._load_completion_cache()
            if full_df is not None and len(full_df) > 0:
                # Sort by films_watched to get top N by watch count
                sorted_df = full_df.sort_values('films_watched', ascending=False).head(top_n)
                # Return sorted by completion rate
                return sorted_df.sort_values('completion_rate', ascending=False)
        
        # Fall back to computing on the fly
        results = []
        
        # Sort actors by films watched
        sorted_actors = sorted(self.actor_watched.items(), 
                              key=lambda x: x[1]['count'], reverse=True)[:top_n]
        
        logger.info(f"Calculating filmography completion for top {top_n} actors...")
        
        for actor_name, data in sorted_actors:
            imdb_id = data.get('imdb_id')
            films_watched = data['count']
            
            # Skip if no IMDb ID (can't get filmography)
            if imdb_id is None:
                logger.debug(f"Skipping {actor_name} - no IMDb ID")
                continue
            
            # Get complete filmography from IMDb
            filmography = self.get_complete_filmography(actor_name, imdb_id)
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
                'gender': data.get('gender', 0),
                'imdb_id': imdb_id
            })
        
        self._save_cache()
        
        df = pd.DataFrame(results)
        if len(df) > 0:
            return df.sort_values('completion_rate', ascending=False)
        return df
    
    def get_near_complete_actors(self, min_rate: float = 75.0, min_films: int = 5) -> pd.DataFrame:
        """Find actors I'm close to completing."""
        # Use full cache if available
        if COMPLETION_CACHE_FILE.exists():
            full_df = self._load_completion_cache()
            if full_df is not None:
                near_complete = full_df[
                    (full_df['completion_rate'] >= min_rate) &
                    (full_df['films_watched'] >= min_films)
                ]
                return near_complete.sort_values('films_missing')
        
        # Fall back to top 200
        completion_df = self.calculate_completion_rates(top_n=200, use_cache=False)
        
        near_complete = completion_df[
            (completion_df['completion_rate'] >= min_rate) &
            (completion_df['films_watched'] >= min_films)
        ]
        
        return near_complete.sort_values('films_missing')
    
    def get_barely_explored(self, max_rate: float = 25.0, min_total: int = 10) -> pd.DataFrame:
        """Find major actors I've barely explored."""
        # Use full cache if available
        if COMPLETION_CACHE_FILE.exists():
            full_df = self._load_completion_cache()
            if full_df is not None:
                barely_explored = full_df[
                    (full_df['completion_rate'] <= max_rate) &
                    (full_df['total_films'] >= min_total)
                ]
                return barely_explored.sort_values('total_films', ascending=False)
        
        # Fall back to top 200
        completion_df = self.calculate_completion_rates(top_n=200, use_cache=False)
        
        barely_explored = completion_df[
            (completion_df['completion_rate'] <= max_rate) &
            (completion_df['total_films'] >= min_total)
        ]
        
        return barely_explored.sort_values('total_films', ascending=False)
    
    def get_all_completion_data(self) -> pd.DataFrame:
        """Get completion data for ALL actors (from cache or computed)."""
        if COMPLETION_CACHE_FILE.exists():
            return self._load_completion_cache()
        return self.build_full_completion_cache()
    
    # =========================================================================
    # DIRECTOR COMPLETION ANALYSIS
    # =========================================================================
    
    def _get_watched_directors(self) -> Dict[str, Dict]:
        """Extract all watched directors from the watched movies."""
        import numpy as np
        director_dict = {}
        
        # Load watched movies master to get directors
        master_file = DATA_DIR / "watched_movies_master.csv"
        if not master_file.exists():
            logger.warning("watched_movies_master.csv not found!")
            return director_dict
        
        movies_df = pd.read_csv(master_file, low_memory=False)
        
        # Get IMDb names for lookup
        if self.imdb_names is None:
            return director_dict
        
        names_lookup = dict(zip(self.imdb_names['nconst'], self.imdb_names['primaryName']))
        
        # Process directors from imdb_crew cache
        if self.imdb_crew is not None:
            watched_tconsts = set(movies_df['const'].astype(str).values)
            watched_crew = self.imdb_crew[self.imdb_crew['tconst'].isin(watched_tconsts)]
            
            logger.info(f"Processing {len(watched_crew)} crew entries for directors...")
            
            for _, row in watched_crew.iterrows():
                tconst = row['tconst']
                directors_raw = row.get('directors', [])
                
                # Handle numpy arrays (which is how parquet stores lists)
                if isinstance(directors_raw, np.ndarray):
                    directors = directors_raw.tolist()
                elif isinstance(directors_raw, str):
                    try:
                        directors = ast.literal_eval(directors_raw)
                    except:
                        directors = [d.strip() for d in directors_raw.split(',') if d.strip()]
                elif isinstance(directors_raw, list):
                    directors = directors_raw
                else:
                    directors = []
                
                for director_id in directors:
                    director_id = str(director_id).strip("[] '\"")
                    if director_id and director_id in names_lookup:
                        director_name = names_lookup[director_id]
                        if director_name not in director_dict:
                            director_dict[director_name] = {
                                'imdb_id': director_id,
                                'films_watched': [],
                            }
                        if tconst not in director_dict[director_name]['films_watched']:
                            director_dict[director_name]['films_watched'].append(tconst)
            
            # Calculate counts
            for name in director_dict:
                director_dict[name]['count'] = len(director_dict[name]['films_watched'])
        
        logger.info(f"Found {len(director_dict)} directors in watched films")
        return director_dict
    
    def get_director_filmography(self, director_name: str, imdb_id: str) -> Dict:
        """Get complete filmography for a director from IMDb crew cache."""
        if imdb_id is None or self.imdb_crew is None:
            return {'total': 0, 'titles': []}
        
        cache_key = f"director_{imdb_id}"
        if cache_key in self.filmography_cache:
            return self.filmography_cache[cache_key]
        
        try:
            import numpy as np
            
            # Vectorized approach: check if imdb_id is in directors array for each row
            def contains_director(directors_arr, target_id):
                if isinstance(directors_arr, np.ndarray):
                    return target_id in directors_arr
                elif isinstance(directors_arr, list):
                    return target_id in directors_arr
                elif isinstance(directors_arr, str):
                    return target_id in directors_arr
                return False
            
            mask = self.imdb_crew['directors'].apply(lambda x: contains_director(x, imdb_id))
            director_films = self.imdb_crew.loc[mask, 'tconst'].tolist()
            
            filmography = {
                'total': len(director_films),
                'titles': director_films
            }
            
            self.filmography_cache[cache_key] = filmography
            return filmography
            
        except Exception as e:
            logger.error(f"Error fetching director filmography for {director_name}: {e}")
            return {'total': 0, 'titles': []}
    
    def build_director_completion_cache(self) -> pd.DataFrame:
        """Build completion rates for all watched directors."""
        director_cache_file = DATA_DIR / "director_filmography_completion.parquet"
        
        if director_cache_file.exists():
            df = pd.read_parquet(director_cache_file)
            logger.info(f"Loaded director completion cache: {len(df):,} directors")
            return df
        
        # Need to build - ensure IMDb data is loaded
        self._ensure_imdb_loaded()
        
        logger.info("Building director completion cache...")
        start_time = time.time()
        
        director_watched = self._get_watched_directors()
        results = []
        
        total = len(director_watched)
        for i, (director_name, data) in enumerate(director_watched.items()):
            if (i + 1) % 100 == 0:
                elapsed = time.time() - start_time
                logger.info(f"  Processing director {i+1}/{total}... ({elapsed:.0f}s elapsed)")
            
            imdb_id = data.get('imdb_id')
            films_watched = data['count']
            
            if imdb_id is None:
                continue
            
            filmography = self.get_director_filmography(director_name, imdb_id)
            total_films = filmography['total']
            
            if total_films == 0:
                continue
            
            completion_rate = (films_watched / total_films) * 100
            films_missing = total_films - films_watched
            
            results.append({
                'director': director_name,
                'imdb_id': imdb_id,
                'films_watched': films_watched,
                'total_films': total_films,
                'films_missing': films_missing,
                'completion_rate': completion_rate
            })
        
        self._save_cache()
        
        df = pd.DataFrame(results)
        if len(df) > 0:
            df = df.sort_values('completion_rate', ascending=False)
            df.to_parquet(director_cache_file, index=False)
            logger.info(f"Saved director completion cache: {len(df):,} directors in {time.time()-start_time:.1f}s")
        
        return df
    
    def get_all_director_completion_data(self) -> pd.DataFrame:
        """Get completion data for all directors."""
        director_cache_file = DATA_DIR / "director_filmography_completion.parquet"
        if director_cache_file.exists():
            return pd.read_parquet(director_cache_file)
        return self.build_director_completion_cache()


def viz_11_completion_leaderboard(analyzer: FilmographyAnalyzer):
    """Visualization 11: Filmography completion leaderboard - TOP 30 with min 5 films watched."""
    # Use full data from cache
    full_df = analyzer.get_all_completion_data()
    
    # Filter to actors with at least 5 films watched (meaningful completion)
    meaningful_df = full_df[full_df['films_watched'] >= 5].copy()
    top_30 = meaningful_df.sort_values('completion_rate', ascending=False).head(30)
    
    fig, ax = plt.subplots(figsize=(14, 12))
    
    # Color by completion rate
    colors = plt.cm.RdYlGn(top_30['completion_rate'] / 100)
    
    bars = ax.barh(range(len(top_30)), top_30['completion_rate'], color=colors)
    ax.set_yticks(range(len(top_30)))
    ax.set_yticklabels(top_30['actor'])
    ax.invert_yaxis()
    
    # Add labels with watched/total
    for i, (rate, watched, total) in enumerate(zip(top_30['completion_rate'], 
                                                    top_30['films_watched'],
                                                    top_30['total_films'])):
        label = f'{rate:.1f}% ({int(watched)}/{int(total)} films)'
        ax.text(rate + 1, i, label, va='center', fontsize=9)
    
    ax.set_xlabel('Filmography Completion Rate (%)', fontsize=12, fontweight='bold')
    ax.set_title('Top 30 Filmography Completion Rates\n(Actors with 5+ films watched | Percentage of filmography completed)', 
                 fontsize=16, fontweight='bold', pad=20)
    ax.set_xlim(0, 105)
    ax.grid(axis='x', alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'viz_11_completion_leaderboard.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info(f"✅ Viz 11: Completion leaderboard saved (showing {len(top_30)} actors)")


def viz_12_completion_distribution(analyzer: FilmographyAnalyzer):
    """Visualization 12: Distribution of completion rates for ALL actors."""
    # Use full data from cache
    full_df = analyzer.get_all_completion_data()
    
    # Filter to meaningful actors (at least 3 films watched)
    completion_df = full_df[full_df['films_watched'] >= 3].copy()
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Histogram
    ax1.hist(completion_df['completion_rate'], bins=30, color=COLORS['accent'], 
             alpha=0.7, edgecolor='black')
    ax1.axvline(completion_df['completion_rate'].mean(), color=COLORS['danger'], 
                linestyle='--', linewidth=2, label=f'Mean: {completion_df["completion_rate"].mean():.1f}%')
    ax1.axvline(completion_df['completion_rate'].median(), color=COLORS['success'], 
                linestyle='--', linewidth=2, label=f'Median: {completion_df["completion_rate"].median():.1f}%')
    
    ax1.set_xlabel('Completion Rate (%)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Number of Actors', fontsize=12, fontweight='bold')
    ax1.set_title(f'Filmography Completion Distribution\n({len(completion_df):,} actors with 3+ films watched)', 
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
    ax2.set_title(f'Actors by Completion Tier ({len(completion_df):,} total)', fontsize=14, fontweight='bold', pad=15)
    ax2.grid(axis='y', alpha=0.3)
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'viz_12_completion_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 12: Completion distribution saved")


def viz_13_near_complete_actors(analyzer: FilmographyAnalyzer):
    """Visualization 13: Actors I'm close to completing (75%+)."""
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
    ax.set_title('Near-Complete Filmographies (75%+)\n"Low-Hanging Fruit" - Actors I can complete with fewest films', 
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


def viz_16_director_completion_leaderboard(analyzer: FilmographyAnalyzer):
    """Visualization 16: Director filmography completion leaderboard."""
    director_df = analyzer.get_all_director_completion_data()
    
    if len(director_df) == 0:
        logger.warning("No director completion data available. Skipping viz 16.")
        return
    
    # Filter to directors with at least 3 films watched
    meaningful_df = director_df[director_df['films_watched'] >= 3].copy()
    top_30 = meaningful_df.sort_values('completion_rate', ascending=False).head(30)
    
    fig, ax = plt.subplots(figsize=(14, 12))
    
    # Color by completion rate
    colors = plt.cm.RdYlGn(top_30['completion_rate'] / 100)
    
    bars = ax.barh(range(len(top_30)), top_30['completion_rate'], color=colors)
    ax.set_yticks(range(len(top_30)))
    ax.set_yticklabels(top_30['director'])
    ax.invert_yaxis()
    
    # Add labels with watched/total
    for i, (rate, watched, total) in enumerate(zip(top_30['completion_rate'], 
                                                    top_30['films_watched'],
                                                    top_30['total_films'])):
        label = f'{rate:.1f}% ({int(watched)}/{int(total)} films)'
        ax.text(rate + 1, i, label, va='center', fontsize=9)
    
    ax.set_xlabel('Filmography Completion Rate (%)', fontsize=12, fontweight='bold')
    ax.set_title('Top 30 Director Filmography Completion Rates\n(Directors with 3+ films watched | Percentage of filmography completed)', 
                 fontsize=16, fontweight='bold', pad=20)
    ax.set_xlim(0, 105)
    ax.grid(axis='x', alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'viz_16_director_completion_leaderboard.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info(f"✅ Viz 16: Director completion leaderboard saved ({len(top_30)} directors)")


def viz_17_career_span_analysis(analyzer: FilmographyAnalyzer):
    """Visualization 17: Career span analysis - first to last film per actor."""
    full_df = analyzer.get_all_completion_data()
    
    if len(full_df) == 0:
        logger.warning("No completion data available. Skipping viz 17.")
        return
    
    # Get watched films for career span calculation
    watched_df = analyzer.df.copy()
    
    # Determine the actors column name
    actors_col = None
    for col in ['tmdb_actors', 'Actors', 'actors', 'tmdb_cast']:
        if col in watched_df.columns:
            actors_col = col
            break
    
    if actors_col is None:
        logger.warning("No actors column found in data. Skipping viz 17.")
        return
    
    # Determine the year column name
    year_col = None
    for col in ['Year', 'year', 'release_date']:
        if col in watched_df.columns:
            year_col = col
            break
    
    if year_col is None:
        logger.warning("No year column found in data. Skipping viz 17.")
        return
    
    # Calculate career spans from watched films
    career_data = []
    for _, row in full_df.iterrows():
        actor = row['actor']
        # Find films with this actor
        actors_str = watched_df[actors_col].astype(str)
        actor_films = watched_df[actors_str.str.contains(actor, na=False, regex=False)]
        if len(actor_films) > 0:
            years = actor_films[year_col].dropna()
            if len(years) > 0:
                try:
                    first_year = int(years.min())
                    last_year = int(years.max())
                    span = last_year - first_year
                    career_data.append({
                        'actor': actor,
                        'first_year': first_year,
                        'last_year': last_year,
                        'span': span,
                        'films_watched': row['films_watched'],
                        'completion_rate': row['completion_rate']
                    })
                except:
                    continue
    
    if not career_data:
        logger.warning("No career data available. Skipping viz 17.")
        return
    
    career_df = pd.DataFrame(career_data)
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Career span distribution
    ax1 = axes[0, 0]
    ax1.hist(career_df['span'], bins=30, color='#3498db', alpha=0.7, edgecolor='black')
    ax1.axvline(career_df['span'].median(), color='red', linestyle='--', 
                linewidth=2, label=f"Median: {career_df['span'].median():.0f} years")
    ax1.set_xlabel('Career Span (Years)', fontsize=11)
    ax1.set_ylabel('Number of Actors', fontsize=11)
    ax1.set_title('Distribution of Watched Career Spans', fontsize=12, fontweight='bold')
    ax1.legend()
    ax1.grid(axis='y', alpha=0.3)
    
    # 2. Top actors by career span watched
    ax2 = axes[0, 1]
    top_span = career_df.nlargest(15, 'span')
    bars = ax2.barh(range(len(top_span)), top_span['span'], color='#2ecc71', alpha=0.7)
    ax2.set_yticks(range(len(top_span)))
    ax2.set_yticklabels(top_span['actor'], fontsize=9)
    ax2.invert_yaxis()
    ax2.set_xlabel('Career Span (Years)', fontsize=11)
    ax2.set_title('Actors with Longest Watched Career Spans', fontsize=12, fontweight='bold')
    for bar, row in zip(bars, top_span.itertuples()):
        ax2.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2, 
                f"{row.first_year}-{row.last_year}", va='center', fontsize=8)
    
    # 3. Era coverage - which decades are most watched
    ax3 = axes[1, 0]
    decade_counts = {}
    for _, row in watched_df.iterrows():
        year = row.get(year_col)
        if pd.notna(year):
            try:
                decade = (int(year) // 10) * 10
                decade_counts[decade] = decade_counts.get(decade, 0) + 1
            except:
                continue
    
    if decade_counts:
        decades = sorted(decade_counts.keys())
        counts = [decade_counts[d] for d in decades]
        colors = plt.cm.viridis([i/len(decades) for i in range(len(decades))])
        bars = ax3.bar([f"{d}s" for d in decades], counts, color=colors, alpha=0.8)
        ax3.set_xlabel('Decade', fontsize=11)
        ax3.set_ylabel('Films Watched', fontsize=11)
        ax3.set_title('Films Watched by Decade', fontsize=12, fontweight='bold')
        ax3.tick_params(axis='x', rotation=45)
        for bar, count in zip(bars, counts):
            ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(counts)*0.01, 
                    str(count), ha='center', fontsize=9)
    
    # 4. Career span vs completion rate
    ax4 = axes[1, 1]
    scatter = ax4.scatter(career_df['span'], career_df['completion_rate'], 
                         c=career_df['films_watched'], cmap='plasma', alpha=0.5, s=30)
    ax4.set_xlabel('Career Span (Years)', fontsize=11)
    ax4.set_ylabel('Completion Rate (%)', fontsize=11)
    ax4.set_title('Career Span vs Completion Rate', fontsize=12, fontweight='bold')
    ax4.grid(True, alpha=0.3)
    plt.colorbar(scatter, ax=ax4, label='Films Watched')
    
    plt.suptitle('Career Span Analysis\nHow much of each actor\'s career have I explored?', 
                 fontsize=16, fontweight='bold')
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig(OUTPUT_DIR / 'viz_17_career_span_analysis.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info(f"✅ Viz 17: Career span analysis saved")


def viz_18_filmography_size_distribution(analyzer: FilmographyAnalyzer):
    """Visualization 18: Actor filmography size distribution."""
    full_df = analyzer.get_all_completion_data()
    
    if len(full_df) == 0:
        logger.warning("No completion data available. Skipping viz 18.")
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Total filmography size distribution
    ax1 = axes[0, 0]
    ax1.hist(full_df['total_films'], bins=50, color='#9b59b6', alpha=0.7, edgecolor='black')
    ax1.axvline(full_df['total_films'].median(), color='red', linestyle='--', 
                linewidth=2, label=f"Median: {full_df['total_films'].median():.0f} films")
    ax1.set_xlabel('Total Films in Filmography', fontsize=11)
    ax1.set_ylabel('Number of Actors', fontsize=11)
    ax1.set_title('Distribution of Actor Filmography Sizes (IMDb)', fontsize=12, fontweight='bold')
    ax1.legend()
    ax1.grid(axis='y', alpha=0.3)
    
    # 2. Actors with largest filmographies
    ax2 = axes[0, 1]
    top_filmog = full_df.nlargest(15, 'total_films')
    colors = plt.cm.RdYlGn(top_filmog['completion_rate'] / 100)
    bars = ax2.barh(range(len(top_filmog)), top_filmog['total_films'], color=colors, alpha=0.8)
    ax2.set_yticks(range(len(top_filmog)))
    ax2.set_yticklabels(top_filmog['actor'], fontsize=9)
    ax2.invert_yaxis()
    ax2.set_xlabel('Total Films', fontsize=11)
    ax2.set_title('Actors with Largest Filmographies\n(Color = Completion Rate)', fontsize=12, fontweight='bold')
    for bar, row in zip(bars, top_filmog.itertuples()):
        ax2.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2, 
                f"{row.completion_rate:.0f}%", va='center', fontsize=8)
    
    # 3. Filmography size vs completion rate
    ax3 = axes[1, 0]
    ax3.scatter(full_df['total_films'], full_df['completion_rate'], 
               alpha=0.3, c='#3498db', s=20)
    ax3.set_xlabel('Total Films in Filmography', fontsize=11)
    ax3.set_ylabel('Completion Rate (%)', fontsize=11)
    ax3.set_title('Filmography Size vs Completion Rate', fontsize=12, fontweight='bold')
    ax3.grid(True, alpha=0.3)
    
    # Add trend annotation
    small_filmog = full_df[full_df['total_films'] <= 20]['completion_rate'].mean()
    large_filmog = full_df[full_df['total_films'] >= 50]['completion_rate'].mean()
    ax3.annotate(f'Small filmographies (≤20): {small_filmog:.1f}% avg', 
                xy=(0.02, 0.98), xycoords='axes fraction', fontsize=9, va='top')
    ax3.annotate(f'Large filmographies (≥50): {large_filmog:.1f}% avg', 
                xy=(0.02, 0.92), xycoords='axes fraction', fontsize=9, va='top')
    
    # 4. Completion efficiency - who gives me the best "bang for buck"
    ax4 = axes[1, 1]
    # Calculate efficiency = completion gained per film watched
    full_df_copy = full_df.copy()
    full_df_copy['efficiency'] = full_df_copy['completion_rate'] / full_df_copy['films_watched'].clip(lower=1)
    top_efficiency = full_df_copy[full_df_copy['films_watched'] >= 3].nlargest(15, 'efficiency')
    
    bars = ax4.barh(range(len(top_efficiency)), top_efficiency['completion_rate'], color='#f39c12', alpha=0.8)
    ax4.set_yticks(range(len(top_efficiency)))
    ax4.set_yticklabels(top_efficiency['actor'], fontsize=9)
    ax4.invert_yaxis()
    ax4.set_xlabel('Completion Rate (%)', fontsize=11)
    ax4.set_title('Most Efficient Completions\n(High completion with few films watched)', fontsize=12, fontweight='bold')
    for bar, row in zip(bars, top_efficiency.itertuples()):
        ax4.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2, 
                f"{int(row.films_watched)} films", va='center', fontsize=8)
    
    plt.suptitle('Filmography Size Analysis\nUnderstanding the scope of actor filmographies', 
                 fontsize=16, fontweight='bold')
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig(OUTPUT_DIR / 'viz_18_filmography_size_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info(f"✅ Viz 18: Filmography size distribution saved")


def viz_19_actor_biographical_insights(analyzer: FilmographyAnalyzer):
    """Visualization 19: Actor biographical insights using IMDb birth/death data."""
    full_df = analyzer.get_all_completion_data()
    
    if len(full_df) == 0:
        logger.warning("No completion data available. Skipping viz 19.")
        return
    
    # Load IMDb names for biographical data
    names_path = IMDB_CACHE_DIR / 'names.parquet'
    if not names_path.exists():
        logger.warning("IMDb names.parquet not found. Skipping viz 19.")
        return
    
    names_df = pd.read_parquet(names_path)
    
    # Merge with completion data
    df = full_df.merge(names_df[['nconst', 'birthYear', 'deathYear', 'primaryProfession']], 
                       left_on='imdb_id', right_on='nconst', how='left')
    
    # Clean birth/death years
    df['birthYear'] = pd.to_numeric(df['birthYear'], errors='coerce')
    df['deathYear'] = pd.to_numeric(df['deathYear'], errors='coerce')
    
    # Create derived columns
    current_year = 2026
    df['is_deceased'] = df['deathYear'].notna()
    df['birth_decade'] = (df['birthYear'] // 10 * 10).astype('Int64')
    df['approx_age'] = current_year - df['birthYear']
    df['years_since_death'] = current_year - df['deathYear']
    
    # Filter to actors with birth year data for meaningful analysis
    df_with_birth = df[df['birthYear'].notna()].copy()
    
    if len(df_with_birth) < 50:
        logger.warning(f"Only {len(df_with_birth)} actors with birth data. Skipping viz 19.")
        return
    
    fig = plt.figure(figsize=(18, 14))
    gs = fig.add_gridspec(3, 2, hspace=0.35, wspace=0.25)
    
    # 1. Living Legends vs Departed Icons
    ax1 = fig.add_subplot(gs[0, 0])
    living = df_with_birth[~df_with_birth['is_deceased']]
    deceased = df_with_birth[df_with_birth['is_deceased']]
    
    counts = [len(living), len(deceased)]
    labels = [f'Living\n({len(living):,} actors)', f'Deceased\n({len(deceased):,} actors)']
    colors_pie = ['#27AE60', '#7F8C8D']
    explode = (0.02, 0.02)
    
    wedges, texts, autotexts = ax1.pie(counts, labels=labels, autopct='%1.1f%%', 
                                        colors=colors_pie, explode=explode,
                                        textprops={'fontsize': 10})
    for autotext in autotexts:
        autotext.set_fontweight('bold')
    ax1.set_title('Living Legends vs Departed Icons\n(Actors in My Watched Films)', 
                  fontsize=12, fontweight='bold')
    
    # 2. Completion Rate: Living vs Deceased
    ax2 = fig.add_subplot(gs[0, 1])
    living_completion = living['completion_rate'].mean() if len(living) > 0 else 0
    deceased_completion = deceased['completion_rate'].mean() if len(deceased) > 0 else 0
    
    bars = ax2.bar(['Living Actors', 'Deceased Actors'], 
                   [living_completion, deceased_completion],
                   color=['#27AE60', '#7F8C8D'], alpha=0.8, edgecolor='black')
    ax2.set_ylabel('Average Completion Rate (%)', fontsize=11)
    ax2.set_title('Filmography Completion: Living vs Deceased', fontsize=12, fontweight='bold')
    ax2.set_ylim(0, max(living_completion, deceased_completion) * 1.3)
    for bar, val in zip(bars, [living_completion, deceased_completion]):
        ax2.text(bar.get_x() + bar.get_width()/2, val + 1, f'{val:.1f}%', 
                ha='center', fontweight='bold', fontsize=11)
    ax2.grid(axis='y', alpha=0.3)
    
    # 3. Birth Decade Distribution
    ax3 = fig.add_subplot(gs[1, 0])
    decade_counts = df_with_birth['birth_decade'].value_counts().sort_index()
    decade_counts = decade_counts[decade_counts.index >= 1900]  # Filter to 1900+
    
    colors_decade = plt.cm.viridis(np.linspace(0.2, 0.9, len(decade_counts)))
    bars = ax3.bar([f"{int(d)}s" for d in decade_counts.index], decade_counts.values, 
                   color=colors_decade, alpha=0.8, edgecolor='black')
    ax3.set_xlabel('Birth Decade', fontsize=11)
    ax3.set_ylabel('Number of Actors', fontsize=11)
    ax3.set_title('Actors by Birth Decade\n(When were my favorite actors born?)', 
                  fontsize=12, fontweight='bold')
    ax3.tick_params(axis='x', rotation=45)
    ax3.grid(axis='y', alpha=0.3)
    
    # Add count labels
    for bar, count in zip(bars, decade_counts.values):
        if count > 50:
            ax3.text(bar.get_x() + bar.get_width()/2, count + max(decade_counts.values)*0.02, 
                    f'{count:,}', ha='center', fontsize=8, fontweight='bold')
    
    # 4. Completion Rate by Birth Decade
    ax4 = fig.add_subplot(gs[1, 1])
    decade_completion = df_with_birth.groupby('birth_decade')['completion_rate'].mean()
    decade_completion = decade_completion[decade_completion.index >= 1900]
    
    ax4.plot([f"{int(d)}s" for d in decade_completion.index], decade_completion.values, 
             'o-', color=COLORS['accent'], markersize=8, linewidth=2, markeredgecolor='black')
    ax4.set_xlabel('Birth Decade', fontsize=11)
    ax4.set_ylabel('Average Completion Rate (%)', fontsize=11)
    ax4.set_title('Completion Rate by Birth Decade\n(Older actors = more complete filmographies?)', 
                  fontsize=12, fontweight='bold')
    ax4.tick_params(axis='x', rotation=45)
    ax4.grid(alpha=0.3)
    ax4.set_ylim(0, max(decade_completion.values) * 1.2)
    
    # 5. Top Watched Living Legends
    ax5 = fig.add_subplot(gs[2, 0])
    top_living = living.nlargest(12, 'films_watched')
    if len(top_living) > 0:
        y_pos = range(len(top_living))
        colors_bar = plt.cm.Greens(np.linspace(0.4, 0.9, len(top_living)))[::-1]
        bars = ax5.barh(y_pos, top_living['films_watched'], color=colors_bar, alpha=0.8)
        ax5.set_yticks(y_pos)
        labels = []
        for _, row in top_living.iterrows():
            age = int(row['approx_age']) if pd.notna(row['approx_age']) else '?'
            labels.append(f"{row['actor']} (age ~{age})")
        ax5.set_yticklabels(labels, fontsize=9)
        ax5.invert_yaxis()
        ax5.set_xlabel('Films Watched', fontsize=11)
        ax5.set_title('Top 12 Living Legends by Films Watched', fontsize=12, fontweight='bold')
        ax5.grid(axis='x', alpha=0.3)
        
        for bar, rate in zip(bars, top_living['completion_rate']):
            ax5.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2, 
                    f'{rate:.0f}%', va='center', fontsize=8, color='#666')
    
    # 6. Top Watched Departed Icons
    ax6 = fig.add_subplot(gs[2, 1])
    top_deceased = deceased.nlargest(12, 'films_watched')
    if len(top_deceased) > 0:
        y_pos = range(len(top_deceased))
        colors_bar = plt.cm.Greys(np.linspace(0.4, 0.8, len(top_deceased)))[::-1]
        bars = ax6.barh(y_pos, top_deceased['films_watched'], color=colors_bar, alpha=0.8)
        ax6.set_yticks(y_pos)
        labels = []
        for _, row in top_deceased.iterrows():
            birth = int(row['birthYear']) if pd.notna(row['birthYear']) else '?'
            death = int(row['deathYear']) if pd.notna(row['deathYear']) else '?'
            labels.append(f"{row['actor']} ({birth}-{death})")
        ax6.set_yticklabels(labels, fontsize=9)
        ax6.invert_yaxis()
        ax6.set_xlabel('Films Watched', fontsize=11)
        ax6.set_title('Top 12 Departed Icons by Films Watched', fontsize=12, fontweight='bold')
        ax6.grid(axis='x', alpha=0.3)
        
        for bar, rate in zip(bars, top_deceased['completion_rate']):
            ax6.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2, 
                    f'{rate:.0f}%', va='center', fontsize=8, color='#666')
    
    plt.suptitle('Actor Biographical Insights\n(Birth/Death data from IMDb)', 
                 fontsize=16, fontweight='bold')
    plt.savefig(OUTPUT_DIR / 'viz_19_actor_biographical_insights.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info(f"✅ Viz 19: Actor biographical insights saved ({len(df_with_birth):,} actors with birth data)")


def viz_20_completion_opportunities(analyzer: FilmographyAnalyzer):
    """Visualization 20: Quick completion opportunities - actors close to milestones."""
    full_df = analyzer.get_all_completion_data()
    
    if len(full_df) == 0:
        logger.warning("No completion data available. Skipping viz 20.")
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Actors 1 film away from next tier
    ax1 = axes[0, 0]
    
    # Calculate films needed for each tier
    tiers = [(100, '100%'), (90, '90%'), (75, '75%'), (50, '50%')]
    opportunities = []
    
    for tier_pct, tier_name in tiers:
        for _, row in full_df.iterrows():
            if row['completion_rate'] < tier_pct:
                films_needed = np.ceil((tier_pct / 100 * row['total_films']) - row['films_watched'])
                if 0 < films_needed <= 3:  # Within 3 films of tier
                    opportunities.append({
                        'actor': row['actor'],
                        'current_rate': row['completion_rate'],
                        'target_tier': tier_name,
                        'films_needed': int(films_needed),
                        'films_watched': row['films_watched']
                    })
    
    if opportunities:
        opps_df = pd.DataFrame(opportunities).drop_duplicates(subset=['actor']).head(15)
        colors = plt.cm.YlOrRd(1 - opps_df['films_needed'] / 4)
        bars = ax1.barh(range(len(opps_df)), opps_df['current_rate'], color=colors, alpha=0.8)
        ax1.set_yticks(range(len(opps_df)))
        ax1.set_yticklabels([f"{row['actor']} → {row['target_tier']}" for _, row in opps_df.iterrows()], fontsize=9)
        ax1.invert_yaxis()
        ax1.set_xlabel('Current Completion Rate (%)', fontsize=11)
        ax1.set_title('Close to Milestone! (1-3 films needed)', fontsize=12, fontweight='bold')
        for bar, row in zip(bars, opps_df.itertuples()):
            ax1.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2, 
                    f"+{row.films_needed} films", va='center', fontsize=8, fontweight='bold')
    
    # 2. Best "return on investment" - high completion for few more films
    ax2 = axes[0, 1]
    
    roi_df = full_df.copy()
    roi_df['films_to_100'] = roi_df['total_films'] - roi_df['films_watched']
    roi_df['completion_gain_per_film'] = (100 - roi_df['completion_rate']) / roi_df['films_to_100'].clip(lower=1)
    
    # Filter to actors with 50%+ completion and some missing
    good_roi = roi_df[(roi_df['completion_rate'] >= 50) & (roi_df['films_to_100'] > 0) & (roi_df['films_to_100'] <= 20)]
    good_roi = good_roi.nlargest(15, 'completion_gain_per_film')
    
    if len(good_roi) > 0:
        colors = plt.cm.Greens(good_roi['completion_rate'] / 100)
        bars = ax2.barh(range(len(good_roi)), good_roi['films_to_100'], color=colors, alpha=0.8)
        ax2.set_yticks(range(len(good_roi)))
        ax2.set_yticklabels(good_roi['actor'], fontsize=9)
        ax2.invert_yaxis()
        ax2.set_xlabel('Films Needed for 100%', fontsize=11)
        ax2.set_title('Best ROI: Few Films to Complete\n(Already at 50%+)', fontsize=12, fontweight='bold')
        for bar, row in zip(bars, good_roi.itertuples()):
            ax2.text(bar.get_width() + 0.2, bar.get_y() + bar.get_height()/2, 
                    f"Currently {row.completion_rate:.0f}%", va='center', fontsize=8)
    
    # 3. Completion rate tiers
    ax3 = axes[1, 0]
    tier_counts = {
        '100%': len(full_df[full_df['completion_rate'] >= 99.9]),
        '90-99%': len(full_df[(full_df['completion_rate'] >= 90) & (full_df['completion_rate'] < 100)]),
        '75-89%': len(full_df[(full_df['completion_rate'] >= 75) & (full_df['completion_rate'] < 90)]),
        '50-74%': len(full_df[(full_df['completion_rate'] >= 50) & (full_df['completion_rate'] < 75)]),
        '25-49%': len(full_df[(full_df['completion_rate'] >= 25) & (full_df['completion_rate'] < 50)]),
        '<25%': len(full_df[full_df['completion_rate'] < 25])
    }
    
    colors = ['#27ae60', '#2ecc71', '#82e0aa', '#f39c12', '#e67e22', '#e74c3c']
    bars = ax3.bar(tier_counts.keys(), tier_counts.values(), color=colors, alpha=0.8)
    ax3.set_xlabel('Completion Tier', fontsize=11)
    ax3.set_ylabel('Number of Actors', fontsize=11)
    ax3.set_title('Actor Completion Tiers', fontsize=12, fontweight='bold')
    for bar in bars:
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2, height + max(tier_counts.values())*0.01, 
                f'{int(height):,}', ha='center', fontsize=10, fontweight='bold')
    ax3.tick_params(axis='x', rotation=45)
    ax3.grid(axis='y', alpha=0.3)
    
    # 4. Summary text
    ax4 = axes[1, 1]
    ax4.axis('off')
    
    summary_text = f"""
COMPLETION OPPORTUNITIES SUMMARY
═══════════════════════════════════════════

Quick Wins (1-3 films from milestone):
• {len([o for o in opportunities if o['films_needed'] == 1])} actors 1 film away
• {len([o for o in opportunities if o['films_needed'] == 2])} actors 2 films away
• {len([o for o in opportunities if o['films_needed'] == 3])} actors 3 films away

Completion Tiers:
• Completists (100%): {tier_counts['100%']:,} actors
• Near-complete (90-99%): {tier_counts['90-99%']:,} actors
• Strong progress (75-89%): {tier_counts['75-89%']:,} actors
• Halfway there (50-74%): {tier_counts['50-74%']:,} actors
• Exploring (25-49%): {tier_counts['25-49%']:,} actors
• Just started (<25%): {tier_counts['<25%']:,} actors

Total Actors Tracked: {len(full_df):,}

💡 TIP: Focus on actors in the "1-3 films away" 
   category for quick milestone achievements!
"""
    
    ax4.text(0.05, 0.95, summary_text, transform=ax4.transAxes,
            fontsize=11, verticalalignment='top', fontfamily='monospace',
            bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.5))
    
    plt.suptitle('Completion Opportunities\nStrategic viewing for maximum progress', 
                 fontsize=16, fontweight='bold')
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig(OUTPUT_DIR / 'viz_20_completion_opportunities.png', dpi=300, bbox_inches='tight')
    plt.close()
    logger.info(f"✅ Viz 20: Completion opportunities saved")


def generate_summary_stats(analyzer: FilmographyAnalyzer) -> str:
    """Generate summary statistics using ALL actors from cache."""
    # Use full data from cache
    full_df = analyzer.get_all_completion_data()
    
    # Filter to meaningful actors (at least 3 films watched)
    completion_df = full_df[full_df['films_watched'] >= 3].copy()
    
    # Get top completions (min 5 films watched)
    meaningful_df = full_df[full_df['films_watched'] >= 5]
    top_complete = meaningful_df.sort_values('completion_rate', ascending=False).head(5)
    
    # Get near-complete
    near_complete = analyzer.get_near_complete_actors(min_rate=75.0, min_films=5)
    
    # Get barely explored
    barely_explored = analyzer.get_barely_explored(max_rate=25.0, min_total=10)
    
    summary = f"""
{'='*80}
BATCH 3 PART 2: FILMOGRAPHY COMPLETION ANALYSIS - SUMMARY
{'='*80}

📊 QUESTIONS ANSWERED:

Q11: Whose complete filmography have I watched most? (min 5 films watched)
Top 5:
"""
    
    for i, (_, row) in enumerate(top_complete.iterrows()):
        summary += f"  {i+1}. {row['actor']}: {row['completion_rate']:.1f}% ({int(row['films_watched'])}/{int(row['total_films'])} films)\n"
    
    summary += f"\nQ12-16: Completion insights (ALL {len(full_df):,} actors):\n"
    summary += f"  - Average completion rate: {full_df['completion_rate'].mean():.1f}%\n"
    summary += f"  - Median completion rate: {full_df['completion_rate'].median():.1f}%\n"
    summary += f"  - Actors with 75%+ completion: {len(near_complete)}\n"
    summary += f"  - Actors with <25% completion (min 10 films total): {len(barely_explored)}\n"
    
    if len(near_complete) > 0:
        easiest = near_complete.iloc[0]
        summary += f"\nQ15: Easiest to complete: {easiest['actor']} ({int(easiest['films_missing'])} films needed)\n"
    
    if len(barely_explored) > 0:
        summary += f"\nQ17: Major stars barely explored (top 5):\n"
        for _, row in barely_explored.head(5).iterrows():
            summary += f"  - {row['actor']}: {row['completion_rate']:.1f}% ({int(row['films_watched'])}/{int(row['total_films'])} films)\n"
    
    summary += f"""
Q19: Am I a completist with any performer?
  - Yes! {len(full_df[full_df['completion_rate'] == 100])} actors at 100% completion
  - {len(full_df[full_df['completion_rate'] >= 90])} actors at 90%+ completion

Q20: Priority targets for 75% completion:
"""
    
    if len(near_complete) > 0:
        for _, row in near_complete.head(5).iterrows():
            summary += f"  - {row['actor']}: Need {int(row['films_missing'])} more films (currently {row['completion_rate']:.1f}%)\n"
    
    summary += f"""
📈 KEY INSIGHTS:

1. Total actors analyzed: {len(full_df):,}
2. Completist actors (100%): {len(full_df[full_df['completion_rate'] == 100])}
3. Near-complete (75-99%): {len(full_df[(full_df['completion_rate'] >= 75) & (full_df['completion_rate'] < 100)])}
4. Moderate exploration (25-75%): {len(full_df[(full_df['completion_rate'] >= 25) & (full_df['completion_rate'] < 75)])}
5. Barely explored (<25%): {len(full_df[full_df['completion_rate'] < 25])}

🎬 VISUALIZATIONS CREATED:

11. viz_11_completion_leaderboard.png - Top 30 actor completion rates (min 5 films)
12. viz_12_completion_distribution.png - Distribution and tiers (all actors)
13. viz_13_near_complete.png - Low-hanging fruit (75%+)
14. viz_14_barely_explored.png - Major stars to explore (<25%)
15. viz_15_interactive_filmography.html - Interactive explorer (all actors)
16. viz_16_director_completion_leaderboard.png - Top 30 director completion rates
17. viz_17_career_span_analysis.png - Career span analysis (first to last film)
18. viz_18_filmography_size_distribution.png - Filmography size distribution
19. viz_19_actor_biographical_insights.png - Living vs deceased, birth decade analysis (IMDb)
20. viz_20_completion_opportunities.png - Quick wins & milestone opportunities

⚠️  NOTE: Completion rates based on IMDb filmography data (movies + TV movies).
Total actors with completion data: {len(full_df):,}

{'='*80}
"""
    
    return summary


def main():
    """Main execution."""
    total_start = time.time()
    
    logger.info("="*80)
    logger.info("BATCH 3 PART 2: FILMOGRAPHY COMPLETION ANALYSIS")
    logger.info("="*80)
    
    # Load data
    master_file = DATA_DIR / "watched_movies_master.csv"
    if not master_file.exists():
        logger.error(f"Master file not found: {master_file}")
        return
    
    t0 = time.time()
    df = pd.read_csv(master_file, low_memory=False)
    logger.info(f"Loaded {len(df)} films in {time.time()-t0:.1f}s")
    
    # Initialize analyzer (skips IMDb load if cache exists)
    t0 = time.time()
    analyzer = FilmographyAnalyzer(df)
    logger.info(f"Analyzer initialized in {time.time()-t0:.1f}s")
    
    # Build or load full completion cache (this is the key step!)
    logger.info("\n" + "="*80)
    logger.info("LOADING FILMOGRAPHY COMPLETION CACHE")
    logger.info("="*80)
    
    t0 = time.time()
    # Check if cache exists
    if COMPLETION_CACHE_FILE.exists():
        logger.info(f"✅ Loading cached actor completion data from {COMPLETION_CACHE_FILE}")
        full_completion_df = analyzer.get_all_completion_data()
    else:
        logger.info("Building full completion cache for ALL actors (first run only)...")
        full_completion_df = analyzer.build_full_completion_cache()
    
    logger.info(f"Total actors with completion data: {len(full_completion_df):,} (loaded in {time.time()-t0:.1f}s)")
    
    # Build director completion cache
    logger.info("\n" + "="*80)
    logger.info("LOADING DIRECTOR COMPLETION CACHE")
    logger.info("="*80)
    
    t0 = time.time()
    director_completion_df = analyzer.get_all_director_completion_data()
    logger.info(f"Total directors with completion data: {len(director_completion_df):,} (loaded in {time.time()-t0:.1f}s)")
    
    # Generate visualizations
    logger.info("\n" + "="*80)
    logger.info("GENERATING VISUALIZATIONS")
    logger.info("="*80)
    
    t0 = time.time()
    viz_11_completion_leaderboard(analyzer)
    viz_12_completion_distribution(analyzer)
    viz_13_near_complete_actors(analyzer)
    viz_14_barely_explored(analyzer)
    viz_15_interactive_filmography_explorer(analyzer)
    viz_16_director_completion_leaderboard(analyzer)
    viz_17_career_span_analysis(analyzer)
    viz_18_filmography_size_distribution(analyzer)
    viz_19_actor_biographical_insights(analyzer)
    viz_20_completion_opportunities(analyzer)
    logger.info(f"All visualizations generated in {time.time()-t0:.1f}s")
    
    # Generate summary
    summary = generate_summary_stats(analyzer)
    print(summary)
    
    # Save summary
    summary_file = OUTPUT_DIR / "PART_2_SUMMARY.txt"
    with open(summary_file, 'w') as f:
        f.write(summary)
    
    total_time = time.time() - total_start
    logger.info(f"\n✅ All visualizations saved to: {OUTPUT_DIR}")
    logger.info(f"✅ Actor completion cache: {COMPLETION_CACHE_FILE}")
    logger.info(f"\n⏱️  TOTAL RUNTIME: {total_time:.1f}s ({total_time/60:.1f} minutes)")
    logger.info(f"✅ Director completion cache: {DATA_DIR / 'director_filmography_completion.parquet'}")


if __name__ == "__main__":
    main()