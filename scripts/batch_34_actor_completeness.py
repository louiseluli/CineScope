"""
Batch 34: Actor Filmography Completeness Analysis
Comprehensive analysis with 20+ creative visualizations including:
- Top 5 actors detailed filmography breakdowns
- Career timeline visualizations
- Genre diversity analysis per actor
- Quality distribution charts
- Missing films ranked galleries
- Completion milestone tracking

UPDATED: Now uses the same IMDb-based data as batch_3_part_2_filmography_completion.py
Data source: filmography_completion.parquet and IMDb cache files (basics, principals, names)
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict
import ast
from pathlib import Path
import numpy as np
from datetime import datetime
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec
import json

# Data paths - using same paths as batch_3_part_2
DATA_DIR = Path('data')
PROCESSED_DIR = DATA_DIR / 'processed'
COMPLETION_CACHE_FILE = PROCESSED_DIR / 'filmography_completion.parquet'
IMDB_CACHE_DIR = PROCESSED_DIR / 'imdb_cache'
ACTOR_FILMOGRAPHY_CACHE = PROCESSED_DIR / 'actor_filmography_cache.json'


class ActorCompletenessAnalyzer:
    def __init__(self, watched_path='data/processed/watched_movies_master.csv', skip_missing_films=False):
        """Initialize with filmography completion parquet (same as batch_3_part_2).
        
        Args:
            watched_path: Path to watched movies CSV
            skip_missing_films: If True, skips loading 38M row principals file (faster init)
        """
        import time
        total_start = time.time()

        print("=" * 80)
        print("ACTOR COMPLETENESS ANALYSIS")
        print("Using IMDb-based filmography data (same source as batch_3_part_2)")
        print("=" * 80)

        t0 = time.time()
        self.watched_df = pd.read_csv(watched_path)
        print(f"\nLoaded {len(self.watched_df):,} watched films in {time.time()-t0:.1f}s")
        
        # Load the filmography completion data
        if not COMPLETION_CACHE_FILE.exists():
            raise FileNotFoundError(
                f"Filmography completion cache not found at {COMPLETION_CACHE_FILE}. "
                "Please run batch_3_part_2_filmography_completion.py first to build the cache!"
            )
        
        t0 = time.time()
        self.completion_df = pd.read_parquet(COMPLETION_CACHE_FILE)
        print(f"Loaded {len(self.completion_df):,} actors from filmography_completion.parquet in {time.time()-t0:.1f}s")
        
        # Load IMDb names for biographical data (birth/death years) - small file
        self._load_imdb_names()
        
        # Only load heavy principals data if we need missing film details
        self.skip_missing_films = skip_missing_films
        if not skip_missing_films:
            print("\nLoading IMDb cache files for missing film details...")
            self._load_imdb_data()
        else:
            print("\n⚡ Fast mode: Skipping principals load (no missing film details)")
            self.imdb_basics = None
            self.imdb_principals = None
        
        # Load filmography cache for detailed lookups
        self._load_filmography_cache()
        
        # Build the actor_df with all required columns
        print("\nBuilding comprehensive actor data...")
        t0 = time.time()
        self._build_actor_df()
        print(f"  Built actor_df in {time.time()-t0:.1f}s")
        
        # Setup directories
        self.output_dir = Path('analysis_outputs/visualizations/batch_34')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir = Path('analysis_outputs/reports')
        self.report_dir.mkdir(parents=True, exist_ok=True)

        # Set style
        plt.style.use('seaborn-v0_8-darkgrid')
        self.colors = {
            'watched': '#2ecc71',
            'missing': '#e74c3c',
            'primary': '#3498db',
            'secondary': '#9b59b6',
            'accent': '#f39c12'
        }
        
        print(f"\n✅ Initialization complete in {time.time()-total_start:.1f}s")
    
    def _load_imdb_names(self):
        """Load IMDb names for biographical data (birth/death years)."""
        import time
        names_path = IMDB_CACHE_DIR / 'names.parquet'
        if names_path.exists():
            t0 = time.time()
            self.imdb_names = pd.read_parquet(names_path)
            print(f"Loaded {len(self.imdb_names):,} names from IMDb (birth/death data) in {time.time()-t0:.1f}s")
        else:
            print("  Warning: IMDb names.parquet not found")
            self.imdb_names = None
    
    def _load_imdb_data(self):
        """Load IMDb basics and principals data for missing film details."""
        import time
        basics_path = IMDB_CACHE_DIR / 'basics.parquet'
        principals_path = IMDB_CACHE_DIR / 'principals.parquet'
        
        if not basics_path.exists():
            print(f"  Warning: IMDb basics not found at {basics_path}")
            self.imdb_basics = None
        else:
            t0 = time.time()
            self.imdb_basics = pd.read_parquet(basics_path)
            # Filter to movies and TV movies only
            self.imdb_basics = self.imdb_basics[
                self.imdb_basics['titleType'].isin(['movie', 'tvMovie'])
            ]
            print(f"  Loaded {len(self.imdb_basics):,} movies from IMDb basics in {time.time()-t0:.1f}s")
        
        if not principals_path.exists():
            print(f"  Warning: IMDb principals not found at {principals_path}")
            self.imdb_principals = None
        else:
            t0 = time.time()
            self.imdb_principals = pd.read_parquet(principals_path)
            # Filter to actors only and to movies only
            if 'category' in self.imdb_principals.columns:
                self.imdb_principals = self.imdb_principals[
                    self.imdb_principals['category'].isin(['actor', 'actress'])
                ]
            # Filter to movies only (if we have basics loaded)
            if self.imdb_basics is not None:
                movie_tconsts = set(self.imdb_basics['tconst'].values)
                self.imdb_principals = self.imdb_principals[
                    self.imdb_principals['tconst'].isin(movie_tconsts)
                ]
            print(f"  Loaded {len(self.imdb_principals):,} actor entries from IMDb principals in {time.time()-t0:.1f}s")
    
    def _load_filmography_cache(self):
        """Load the filmography cache JSON."""
        self.filmography_cache = {}
        if ACTOR_FILMOGRAPHY_CACHE.exists():
            try:
                with open(ACTOR_FILMOGRAPHY_CACHE, 'r') as f:
                    self.filmography_cache = json.load(f)
                print(f"  Loaded filmography cache with {len(self.filmography_cache):,} entries")
            except Exception as e:
                print(f"  Warning: Could not load filmography cache: {e}")
    
    def _build_actor_df(self):
        """Build comprehensive actor dataframe with all needed columns."""
        # Map watched films by actor
        watched_by_actor = defaultdict(list)
        for _, row in self.watched_df.iterrows():
            actors_raw = row.get('Actors', '')
            if pd.isna(actors_raw) or not actors_raw:
                continue
            actors = [a.strip() for a in str(actors_raw).split(',')]
            for actor in actors:
                if actor:
                    watched_by_actor[actor].append({
                        'title': row.get('Title', 'Unknown'),
                        'year': row.get('Year', None),
                        'rating': row.get('IMDb_Rating', 0),
                        'tconst': row.get('imdb_id', None)
                    })
        
        # Create a lookup for biographical data from IMDb names
        bio_lookup = {}
        if self.imdb_names is not None:
            for _, row in self.imdb_names.iterrows():
                bio_lookup[row['nconst']] = {
                    'birthYear': row.get('birthYear'),
                    'deathYear': row.get('deathYear'),
                    'primaryProfession': row.get('primaryProfession', '')
                }
        
        # Build actor_df with additional columns
        results = []
        for _, row in self.completion_df.iterrows():
            actor_name = row['actor']
            imdb_id = row.get('imdb_id', None)
            films_watched = row['films_watched']
            total_films = row['total_films']
            films_missing = row['films_missing']
            completion_rate = row['completion_rate']
            
            # Get watched film details
            watched_films = watched_by_actor.get(actor_name, [])
            
            # Calculate ratings
            watched_ratings = [f['rating'] for f in watched_films if isinstance(f.get('rating'), (int, float)) and f['rating'] > 0]
            watched_avg_rating = np.mean(watched_ratings) if watched_ratings else 0.0
            
            # Get missing films with details from IMDb (only if not skipping)
            missing_films = []
            if not self.skip_missing_films:
                missing_films = self._get_missing_films_details(actor_name, imdb_id, watched_films)
            missing_ratings = [f['rating'] for f in missing_films if isinstance(f.get('rating'), (int, float)) and f['rating'] > 0]
            catalog_avg_rating = np.mean(missing_ratings) if missing_ratings else 0.0
            
            # If we have both, use weighted average for catalog
            all_ratings = watched_ratings + missing_ratings
            if all_ratings:
                catalog_avg_rating = np.mean(all_ratings)
            
            # Get biographical data
            bio = bio_lookup.get(imdb_id, {})
            birth_year = bio.get('birthYear')
            death_year = bio.get('deathYear')
            
            # Clean birth/death years
            if pd.notna(birth_year):
                try:
                    birth_year = int(birth_year)
                except:
                    birth_year = None
            else:
                birth_year = None
                
            if pd.notna(death_year):
                try:
                    death_year = int(death_year)
                except:
                    death_year = None
            else:
                death_year = None
            
            results.append({
                'actor_name': actor_name,
                'imdb_id': imdb_id,
                'completeness_pct': completion_rate,
                'watched_count': films_watched,
                'missing_count': films_missing,
                'total_quality_films': total_films,
                'watched_avg_rating': watched_avg_rating,
                'catalog_avg_rating': catalog_avg_rating,
                'watched_films': watched_films,
                'missing_films': missing_films,
                'birth_year': birth_year,
                'death_year': death_year,
                'is_deceased': death_year is not None
            })
        
        self.actor_df = pd.DataFrame(results)
        self.actor_df = self.actor_df.sort_values('completeness_pct', ascending=False)
        print(f"  Built actor_df with {len(self.actor_df):,} actors")
    
    def _get_missing_films_details(self, actor_name: str, imdb_id: str, watched_films: list) -> list:
        """Get details of missing films for an actor."""
        if imdb_id is None or self.imdb_principals is None or self.imdb_basics is None:
            return []
        
        try:
            # Get all films for this actor from principals
            actor_films = self.imdb_principals[self.imdb_principals['nconst'] == imdb_id]
            
            # Filter to only movies that are in our basics (movies + TV movies)
            actor_tconsts = set(actor_films['tconst'].unique())
            valid_tconsts = set(self.imdb_basics['tconst'].unique())
            actor_tconsts = actor_tconsts & valid_tconsts
            
            # Get watched tconsts
            watched_tconsts = set()
            for film in watched_films:
                if film.get('tconst'):
                    watched_tconsts.add(film['tconst'])
            
            # Missing films
            missing_tconsts = actor_tconsts - watched_tconsts
            
            # Get details from basics
            missing_films = []
            for tconst in list(missing_tconsts)[:50]:  # Limit to 50 for performance
                film_info = self.imdb_basics[self.imdb_basics['tconst'] == tconst]
                if len(film_info) > 0:
                    row = film_info.iloc[0]
                    rating = row.get('averageRating', 0)
                    if pd.isna(rating):
                        rating = 0
                    year = row.get('startYear', None)
                    if pd.notna(year):
                        try:
                            year = int(year)
                        except:
                            year = None
                    missing_films.append({
                        'title': row.get('primaryTitle', 'Unknown'),
                        'year': year,
                        'rating': float(rating),
                        'tconst': tconst
                    })
            
            # Sort by rating
            missing_films.sort(key=lambda x: x.get('rating', 0), reverse=True)
            return missing_films
            
        except Exception as e:
            return []

    def _safe_eval(self, data_str):
        """Safely evaluate string representation of list."""
        if isinstance(data_str, list):
            return data_str
        try:
            return ast.literal_eval(data_str) if pd.notna(data_str) else []
        except:
            return []

    def create_overview_dashboard(self):
        """Create comprehensive overview dashboard."""
        print("\n1. Creating overview dashboard...")

        fig = plt.figure(figsize=(24, 16))
        gs = GridSpec(4, 4, figure=fig, hspace=0.4, wspace=0.4)

        # 1. Completeness Distribution
        ax1 = fig.add_subplot(gs[0, 0:2])
        bins = [0, 20, 40, 60, 80, 100]
        counts, edges = np.histogram(self.actor_df['completeness_pct'], bins=bins)
        colors_dist = ['#e74c3c', '#e67e22', '#f39c12', '#2ecc71', '#27ae60']
        bars = ax1.bar(range(len(counts)), counts, color=colors_dist, alpha=0.7, edgecolor='black')
        ax1.set_xticks(range(len(counts)))
        ax1.set_xticklabels(['0-20%', '20-40%', '40-60%', '60-80%', '80-100%'])
        ax1.set_title('Actor Completeness Distribution', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Number of Actors')
        ax1.grid(axis='y', alpha=0.3)
        for i, (bar, count) in enumerate(zip(bars, counts)):
            ax1.text(bar.get_x() + bar.get_width()/2, count + max(counts)*0.02, 
                    f'{int(count):,}', ha='center', fontweight='bold', fontsize=10)

        # 2. Top 10 Nearly Complete Actors
        ax2 = fig.add_subplot(gs[0, 2:4])
        nearly_complete = self.actor_df.nlargest(10, 'completeness_pct')
        y_pos = np.arange(len(nearly_complete))
        ax2.barh(y_pos, nearly_complete['watched_count'],
                color=self.colors['watched'], alpha=0.7, label='Watched')
        ax2.barh(y_pos, nearly_complete['missing_count'],
                left=nearly_complete['watched_count'],
                color=self.colors['missing'], alpha=0.7, label='Missing')
        ax2.set_yticks(y_pos)
        ax2.set_yticklabels(nearly_complete['actor_name'], fontsize=9)
        ax2.set_title('Top 10 Most Complete Actors', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Films')
        ax2.legend(loc='lower right')
        ax2.invert_yaxis()

        # Add percentage labels
        for i, (idx, row) in enumerate(nearly_complete.iterrows()):
            total = row['watched_count'] + row['missing_count']
            ax2.text(total + 1, i, f"{row['completeness_pct']:.1f}%",
                    va='center', fontweight='bold')

        # 3. Actors with Most Missing Films
        ax3 = fig.add_subplot(gs[1, 0:2])
        most_missing = self.actor_df.nlargest(10, 'missing_count')
        x_pos = np.arange(len(most_missing))
        bars = ax3.bar(x_pos, most_missing['missing_count'], color=self.colors['missing'], alpha=0.7)
        ax3.set_xticks(x_pos)
        ax3.set_xticklabels(most_missing['actor_name'], rotation=45, ha='right', fontsize=9)
        ax3.set_title('Actors with Most Missing Films (Potential to Explore!)',
                     fontsize=14, fontweight='bold')
        ax3.set_ylabel('Missing Films')
        ax3.grid(axis='y', alpha=0.3)
        for bar, count in zip(bars, most_missing['missing_count']):
            ax3.text(bar.get_x() + bar.get_width()/2, count + max(most_missing['missing_count'])*0.02,
                    f'{int(count):,}', ha='center', fontweight='bold', fontsize=9)

        # 4. Average Ratings Comparison
        ax4 = fig.add_subplot(gs[1, 2:4])
        actors_sample = self.actor_df[self.actor_df['watched_avg_rating'] > 0].head(20)
        if len(actors_sample) > 0:
            x = np.arange(len(actors_sample))
            width = 0.35
            ax4.bar(x - width/2, actors_sample['watched_avg_rating'], width,
                   label='My Watched Avg', color=self.colors['watched'], alpha=0.7)
            ax4.bar(x + width/2, actors_sample['catalog_avg_rating'], width,
                   label='Complete Filmography Avg', color=self.colors['primary'], alpha=0.7)
            ax4.set_xticks(x)
            ax4.set_xticklabels(actors_sample['actor_name'], rotation=45, ha='right', fontsize=8)
            ax4.set_title('Rating Comparison: My Selections vs Complete Filmography',
                         fontsize=14, fontweight='bold')
            ax4.set_ylabel('Average IMDb Rating')
            ax4.legend()
            ax4.grid(axis='y', alpha=0.3)
            ax4.set_ylim(5, 9)

        # 5. Completion Milestones
        ax5 = fig.add_subplot(gs[2, 0:2])
        milestones = {
            'Completed (100%)': len(self.actor_df[self.actor_df['completeness_pct'] >= 99.9]),
            'Nearly There (80-99%)': len(self.actor_df[(self.actor_df['completeness_pct'] >= 80) &
                                                       (self.actor_df['completeness_pct'] < 100)]),
            'Halfway (50-79%)': len(self.actor_df[(self.actor_df['completeness_pct'] >= 50) &
                                                  (self.actor_df['completeness_pct'] < 80)]),
            'Getting Started (20-49%)': len(self.actor_df[(self.actor_df['completeness_pct'] >= 20) &
                                                         (self.actor_df['completeness_pct'] < 50)]),
            'Just Beginning (<20%)': len(self.actor_df[self.actor_df['completeness_pct'] < 20])
        }
        # Filter out zero values
        milestones = {k: v for k, v in milestones.items() if v > 0}
        if milestones:
            colors_milestone = ['#27ae60', '#2ecc71', '#f39c12', '#e67e22', '#e74c3c'][:len(milestones)]
            wedges, texts, autotexts = ax5.pie(milestones.values(), labels=milestones.keys(),
                                                autopct='%1.1f%%', colors=colors_milestone,
                                                startangle=90)
            ax5.set_title('Completion Milestones', fontsize=14, fontweight='bold')
            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontweight('bold')

        # 6. Films Watched per Actor Distribution
        ax6 = fig.add_subplot(gs[2, 2:4])
        ax6.hist(self.actor_df['watched_count'], bins=20, color=self.colors['watched'],
                alpha=0.7, edgecolor='black')
        ax6.set_title('Distribution of Films Watched per Actor', fontsize=14, fontweight='bold')
        ax6.set_xlabel('Number of Films Watched')
        ax6.set_ylabel('Number of Actors')
        median_val = self.actor_df['watched_count'].median()
        ax6.axvline(median_val, color='red',
                   linestyle='--', linewidth=2, label=f"Median: {median_val:.0f}")
        ax6.legend()
        ax6.grid(axis='y', alpha=0.3)

        # 7. Summary Statistics
        ax7 = fig.add_subplot(gs[3, :])
        ax7.axis('off')

        valid_ratings = self.actor_df[self.actor_df['watched_avg_rating'] > 0]
        
        stats_text = f"""
        COMPREHENSIVE ACTOR COMPLETENESS STATISTICS (Using IMDb Data)
        ═══════════════════════════════════════════════════════════════════════════════

        Total Actors Analyzed: {len(self.actor_df):,}

        Completion Status:
        • Fully Completed Actors (100%): {len(self.actor_df[self.actor_df['completeness_pct'] >= 99.9]):,}
        • Nearly Complete (80-99%): {len(self.actor_df[(self.actor_df['completeness_pct'] >= 80) & (self.actor_df['completeness_pct'] < 100)]):,}
        • Average Completion Rate: {self.actor_df['completeness_pct'].mean():.1f}%
        • Median Completion Rate: {self.actor_df['completeness_pct'].median():.1f}%

        Films per Actor:
        • Average Films Watched per Actor: {self.actor_df['watched_count'].mean():.1f}
        • Maximum Films Watched (Single Actor): {self.actor_df['watched_count'].max():.0f}
        • Total Missing Films Identified: {self.actor_df['missing_count'].sum():,.0f}

        Quality Insights:
        • My Average Rating: {(valid_ratings['watched_avg_rating'].mean() if len(valid_ratings) > 0 else 0):.2f}
        • Complete Filmography Average: {(valid_ratings['catalog_avg_rating'].mean() if len(valid_ratings) > 0 else 0):.2f}
        
        Data Source: IMDb (same as batch_3_part_2_filmography_completion.py)
        Film Types: Movies + TV Movies (excludes TV episodes, shorts, video games)
        """

        ax7.text(0.05, 0.95, stats_text, transform=ax7.transAxes,
                fontsize=11, verticalalignment='top', fontfamily='monospace',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

        plt.savefig(self.output_dir / 'overview_dashboard.png',
                   dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        print("  ✓ Saved overview_dashboard.png")

    def create_top5_detailed_breakdown(self):
        """Create detailed individual breakdowns for top 5 actors."""
        print("\n2. Creating Top 5 Actors Detailed Breakdown...")

        # Get top 5 by completeness with significant filmography
        top5 = self.actor_df[self.actor_df['total_quality_films'] >= 10].nlargest(5, 'completeness_pct')
        
        if len(top5) == 0:
            top5 = self.actor_df.nlargest(5, 'completeness_pct')

        for idx, (_, actor_row) in enumerate(top5.iterrows(), 1):
            print(f"  {idx}. Creating detailed breakdown for {actor_row['actor_name']}...")

            fig = plt.figure(figsize=(20, 12))
            gs = GridSpec(3, 3, figure=fig, hspace=0.4, wspace=0.4)

            actor_name = actor_row['actor_name']
            watched_films = actor_row['watched_films'] if isinstance(actor_row['watched_films'], list) else []
            missing_films = actor_row['missing_films'] if isinstance(actor_row['missing_films'], list) else []

            # Title
            fig.suptitle(f"Complete Filmography Analysis: {actor_name}",
                        fontsize=20, fontweight='bold', y=0.98)

            # 1. Completion Progress Bar
            ax1 = fig.add_subplot(gs[0, :])
            total = actor_row['total_quality_films']
            watched_count = actor_row['watched_count']
            missing_count = actor_row['missing_count']

            ax1.barh([0], [watched_count], color=self.colors['watched'],
                    height=0.5, label=f'Watched: {watched_count}')
            ax1.barh([0], [missing_count], left=[watched_count],
                    color=self.colors['missing'], height=0.5,
                    label=f'Missing: {missing_count}')
            ax1.set_xlim(0, max(total, 1))
            ax1.set_ylim(-0.5, 0.5)
            ax1.set_yticks([])
            ax1.set_xlabel('Films', fontsize=12)
            ax1.set_title(f'Completeness: {actor_row["completeness_pct"]:.1f}% ({watched_count}/{total} films)',
                         fontsize=14, fontweight='bold', pad=20)
            ax1.legend(loc='upper right', fontsize=11)
            ax1.grid(axis='x', alpha=0.3)

            # Add percentage marker
            if total > 0:
                pct_pos = (watched_count / total) * total
                ax1.axvline(pct_pos, color='black', linestyle='--', linewidth=2)
                ax1.text(pct_pos, 0.3, f'{actor_row["completeness_pct"]:.1f}%',
                        ha='center', fontsize=12, fontweight='bold',
                        bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.7))

            # 2. Watched Films Timeline
            ax2 = fig.add_subplot(gs[1, 0:2])
            if watched_films:
                years = [f['year'] for f in watched_films if isinstance(f.get('year'), (int, float))]
                ratings = [f['rating'] for f in watched_films if isinstance(f.get('year'), (int, float)) and isinstance(f.get('rating'), (int, float))]

                if years and ratings and len(years) == len(ratings):
                    scatter = ax2.scatter(years, ratings, s=100, c=ratings,
                                        cmap='RdYlGn', alpha=0.7, edgecolors='black',
                                        vmin=5, vmax=9)
                    ax2.set_xlabel('Year', fontsize=11)
                    ax2.set_ylabel('IMDb Rating', fontsize=11)
                    ax2.set_title('My Watched Films Timeline', fontsize=12, fontweight='bold')
                    ax2.grid(True, alpha=0.3)
                    ax2.set_ylim(4, 10)
                    plt.colorbar(scatter, ax=ax2, label='Rating')
                else:
                    ax2.text(0.5, 0.5, 'No timeline data available', ha='center', va='center')
                    ax2.set_title('My Watched Films Timeline', fontsize=12, fontweight='bold')
            else:
                ax2.text(0.5, 0.5, 'No watched films data', ha='center', va='center')
                ax2.set_title('My Watched Films Timeline', fontsize=12, fontweight='bold')

            # 3. Rating Distribution
            ax3 = fig.add_subplot(gs[1, 2])
            if watched_films:
                ratings = [f['rating'] for f in watched_films if isinstance(f.get('rating'), (int, float)) and f['rating'] > 0]
                if ratings:
                    ax3.hist(ratings, bins=10, color=self.colors['watched'],
                            alpha=0.7, edgecolor='black', orientation='horizontal')
                    ax3.axhline(np.mean(ratings), color='red', linestyle='--',
                               linewidth=2, label=f'Avg: {np.mean(ratings):.2f}')
                    ax3.set_ylabel('Rating', fontsize=11)
                    ax3.set_xlabel('Count', fontsize=11)
                    ax3.set_title('Rating Distribution', fontsize=12, fontweight='bold')
                    ax3.legend()
                    ax3.grid(axis='x', alpha=0.3)
                else:
                    ax3.text(0.5, 0.5, 'No ratings data', ha='center', va='center')
                    ax3.set_title('Rating Distribution', fontsize=12, fontweight='bold')
            else:
                ax3.text(0.5, 0.5, 'No ratings data', ha='center', va='center')
                ax3.set_title('Rating Distribution', fontsize=12, fontweight='bold')

            # 4. Top Watched Films List
            ax4 = fig.add_subplot(gs[2, 0])
            ax4.axis('off')
            if watched_films:
                top_watched = sorted(watched_films, key=lambda x: x.get('rating', 0) or 0, reverse=True)[:8]
                watched_text = "TOP WATCHED FILMS\n" + "═" * 35 + "\n\n"
                for i, film in enumerate(top_watched, 1):
                    year = film.get('year', 'N/A')
                    rating = film.get('rating', 0) or 0
                    title = str(film.get('title', 'Unknown'))[:30]
                    watched_text += f"{i}. {title}\n   {year} • ⭐ {rating:.1f}\n\n"

                ax4.text(0.05, 0.95, watched_text, transform=ax4.transAxes,
                        fontsize=9, verticalalignment='top', fontfamily='monospace',
                        bbox=dict(boxstyle='round', facecolor=self.colors['watched'],
                                 alpha=0.2, edgecolor=self.colors['watched'], linewidth=2))

            # 5. Top Missing Films List
            ax5 = fig.add_subplot(gs[2, 1])
            ax5.axis('off')
            if missing_films:
                top_missing = sorted(missing_films, key=lambda x: x.get('rating', 0) or 0, reverse=True)[:8]
                missing_text = "TOP MISSING FILMS\n(Recommended to Watch!)\n" + "═" * 35 + "\n\n"
                for i, film in enumerate(top_missing, 1):
                    year = film.get('year', 'N/A')
                    rating = film.get('rating', 0) or 0
                    title = str(film.get('title', 'Unknown'))[:30]
                    missing_text += f"{i}. {title}\n   {year} • ⭐ {rating:.1f}\n\n"

                ax5.text(0.05, 0.95, missing_text, transform=ax5.transAxes,
                        fontsize=9, verticalalignment='top', fontfamily='monospace',
                        bbox=dict(boxstyle='round', facecolor=self.colors['missing'],
                                 alpha=0.2, edgecolor=self.colors['missing'], linewidth=2))

            # 6. Statistics Panel
            ax6 = fig.add_subplot(gs[2, 2])
            ax6.axis('off')

            watched_years = [f['year'] for f in watched_films if isinstance(f.get('year'), (int, float))]
            era_span = f"{min(watched_years)}-{max(watched_years)}" if watched_years else "N/A"

            stats_text = f"""
FILMOGRAPHY STATS
═════════════════════

Total Quality Films: {total}
Films Watched: {watched_count}
Films Missing: {missing_count}
Completion: {actor_row['completeness_pct']:.1f}%

Era Span: {era_span}

Avg Rating (Watched): {actor_row['watched_avg_rating']:.2f}
Avg Rating (All Films): {actor_row['catalog_avg_rating']:.2f}
"""

            ax6.text(0.05, 0.95, stats_text, transform=ax6.transAxes,
                    fontsize=10, verticalalignment='top', fontfamily='monospace',
                    bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.3))

            # Save
            safe_name = "".join(c if c.isalnum() else "_" for c in actor_name)
            plt.savefig(self.output_dir / f'actor_detail_{idx}_{safe_name}.png',
                       dpi=300, bbox_inches='tight', facecolor='white')
            plt.close()

        print("  ✓ Saved 5 actor detail visualizations")

    def create_career_trajectory_visualization(self):
        """Create career trajectory visualization for top actors."""
        print("\n3. Creating career trajectory visualization...")

        fig, axes = plt.subplots(2, 3, figsize=(20, 12))
        axes = axes.flatten()

        # Get top 6 actors by watched count
        top6 = self.actor_df.nlargest(6, 'watched_count')

        for idx, (_, actor_row) in enumerate(top6.iterrows()):
            ax = axes[idx]
            actor_name = actor_row['actor_name']
            watched_films = actor_row['watched_films'] if isinstance(actor_row['watched_films'], list) else []

            if watched_films:
                # Group by decade
                decade_data = defaultdict(list)
                for film in watched_films:
                    year = film.get('year')
                    rating = film.get('rating', 0)
                    if isinstance(year, (int, float)) and isinstance(rating, (int, float)) and rating > 0:
                        decade = (int(year) // 10) * 10
                        decade_data[decade].append(rating)

                if decade_data:
                    decades = sorted(decade_data.keys())
                    avg_ratings = [np.mean(decade_data[d]) for d in decades]
                    counts = [len(decade_data[d]) for d in decades]

                    # Plot
                    ax.bar([f"{d}s" for d in decades], counts, color=self.colors['watched'], alpha=0.6)
                    ax2 = ax.twinx()
                    ax2.plot([f"{d}s" for d in decades], avg_ratings, 'ro-', markersize=8, linewidth=2)
                    ax2.set_ylabel('Avg Rating', color='red')
                    ax2.tick_params(axis='y', labelcolor='red')
                    ax2.set_ylim(5, 10)

            ax.set_title(f'{actor_name}\n({actor_row["watched_count"]} films watched)', fontsize=11, fontweight='bold')
            ax.set_ylabel('Film Count')
            ax.set_xlabel('Decade')
            ax.tick_params(axis='x', rotation=45)

        plt.suptitle('Career Trajectories: Films Watched by Decade\n(Bars = Count, Red Line = Avg Rating)',
                    fontsize=16, fontweight='bold')
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        plt.savefig(self.output_dir / 'career_trajectories.png', dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        print("  ✓ Saved career_trajectories.png")

    def create_completion_heatmap(self):
        """Create completion rate heatmap by watched count ranges."""
        print("\n4. Creating completion heatmap...")

        fig, ax = plt.subplots(figsize=(14, 10))

        # Create bins for watched count and completion rate
        watched_bins = [1, 2, 3, 5, 10, 20, 50, 100, float('inf')]
        watched_labels = ['1', '2', '3', '4-5', '6-10', '11-20', '21-50', '50+']
        completion_bins = [0, 20, 40, 60, 80, 100]
        completion_labels = ['0-20%', '20-40%', '40-60%', '60-80%', '80-100%']

        # Create a matrix
        matrix = np.zeros((len(watched_labels), len(completion_labels)))

        for _, row in self.actor_df.iterrows():
            watched = row['watched_count']
            completion = row['completeness_pct']

            # Find watched bin
            w_idx = 0
            for i, (low, high) in enumerate(zip(watched_bins[:-1], watched_bins[1:])):
                if low <= watched < high:
                    w_idx = i
                    break

            # Find completion bin
            c_idx = min(int(completion // 20), 4)

            matrix[w_idx, c_idx] += 1

        # Plot heatmap
        sns.heatmap(matrix, annot=True, fmt='.0f', cmap='YlGnBu',
                   xticklabels=completion_labels, yticklabels=watched_labels,
                   ax=ax, cbar_kws={'label': 'Number of Actors'})
        ax.set_xlabel('Completion Rate', fontsize=12)
        ax.set_ylabel('Films Watched', fontsize=12)
        ax.set_title('Actor Distribution: Films Watched vs Completion Rate',
                    fontsize=14, fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.output_dir / 'completion_heatmap.png', dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        print("  ✓ Saved completion_heatmap.png")

    def create_missing_films_gallery(self):
        """Create a gallery of top missing films across all actors."""
        print("\n5. Creating missing films gallery...")

        # Collect all missing films
        all_missing = []
        for _, row in self.actor_df.iterrows():
            missing_films = row['missing_films'] if isinstance(row['missing_films'], list) else []
            for film in missing_films:
                film_copy = film.copy()
                film_copy['actor'] = row['actor_name']
                all_missing.append(film_copy)

        # Deduplicate by title (keeping highest rated version)
        seen_titles = {}
        for film in all_missing:
            title = film.get('title', 'Unknown')
            rating = film.get('rating', 0) or 0
            if title not in seen_titles or rating > (seen_titles[title].get('rating', 0) or 0):
                seen_titles[title] = film

        unique_missing = list(seen_titles.values())
        top_missing = sorted(unique_missing, key=lambda x: x.get('rating', 0) or 0, reverse=True)[:30]

        fig, ax = plt.subplots(figsize=(16, 14))
        ax.axis('off')

        # Create gallery text
        gallery_text = "🎬 TOP 30 MISSING FILMS TO WATCH 🎬\n"
        gallery_text += "═" * 70 + "\n\n"
        gallery_text += f"{'Rank':<6} {'Title':<40} {'Year':<8} {'Rating':<10} {'Actor':<25}\n"
        gallery_text += "─" * 90 + "\n"

        for i, film in enumerate(top_missing, 1):
            title = str(film.get('title', 'Unknown'))[:38]
            year = film.get('year', 'N/A')
            rating = film.get('rating', 0) or 0
            actor = str(film.get('actor', ''))[:23]
            gallery_text += f"{i:<6} {title:<40} {year:<8} ⭐{rating:.1f}     {actor:<25}\n"

        ax.text(0.02, 0.98, gallery_text, transform=ax.transAxes,
               fontsize=10, verticalalignment='top', fontfamily='monospace',
               bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.5))

        ax.set_title('My Personalized Missing Films Gallery\n(Based on IMDb Ratings)',
                    fontsize=16, fontweight='bold', pad=20)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'missing_films_gallery.png', dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        print("  ✓ Saved missing_films_gallery.png")

    def create_quality_analysis(self):
        """Create quality pattern analysis visualization."""
        print("\n6. Creating quality analysis...")

        fig = plt.figure(figsize=(18, 12))
        gs = GridSpec(2, 2, figure=fig, hspace=0.3, wspace=0.3)

        # Filter to actors with ratings
        valid_df = self.actor_df[
            (self.actor_df['watched_avg_rating'] > 0) & 
            (self.actor_df['catalog_avg_rating'] > 0)
        ].copy()

        # 1. Scatter: My Rating vs Catalog Rating
        ax1 = fig.add_subplot(gs[0, 0])
        if len(valid_df) > 0:
            ax1.scatter(valid_df['watched_avg_rating'], valid_df['catalog_avg_rating'],
                       alpha=0.5, c=valid_df['completeness_pct'], cmap='viridis', s=50)
            ax1.plot([5, 9], [5, 9], 'r--', linewidth=2, label='Equal Quality')
            ax1.set_xlabel('My Watched Avg Rating')
            ax1.set_ylabel('Complete Filmography Avg Rating')
            ax1.set_title('Quality Comparison by Actor', fontweight='bold')
            ax1.legend()
            ax1.grid(True, alpha=0.3)

        # 2. Rating difference distribution
        ax2 = fig.add_subplot(gs[0, 1])
        if len(valid_df) > 0:
            rating_diff = valid_df['watched_avg_rating'] - valid_df['catalog_avg_rating']
            ax2.hist(rating_diff, bins=30, color=self.colors['primary'], alpha=0.7, edgecolor='black')
            ax2.axvline(0, color='red', linestyle='--', linewidth=2, label='No Difference')
            ax2.axvline(rating_diff.mean(), color='green', linestyle='-', linewidth=2,
                       label=f'Mean: {rating_diff.mean():.2f}')
            ax2.set_xlabel('Rating Difference (My Watched - Catalog)')
            ax2.set_ylabel('Number of Actors')
            ax2.set_title('Am I Watching Higher Quality Films?', fontweight='bold')
            ax2.legend()
            ax2.grid(axis='y', alpha=0.3)

        # 3. Completion vs Quality
        ax3 = fig.add_subplot(gs[1, 0])
        if len(valid_df) > 0:
            ax3.scatter(valid_df['completeness_pct'], valid_df['watched_avg_rating'],
                       alpha=0.5, c=valid_df['watched_count'], cmap='plasma', s=50)
            ax3.set_xlabel('Completion Rate (%)')
            ax3.set_ylabel('Average Rating of Watched Films')
            ax3.set_title('Completion Rate vs Quality of Selections', fontweight='bold')
            ax3.grid(True, alpha=0.3)
            
            # Add colorbar
            sm = plt.cm.ScalarMappable(cmap='plasma', 
                                       norm=plt.Normalize(vmin=valid_df['watched_count'].min(),
                                                         vmax=valid_df['watched_count'].max()))
            plt.colorbar(sm, ax=ax3, label='Films Watched')

        # 4. Summary stats
        ax4 = fig.add_subplot(gs[1, 1])
        ax4.axis('off')

        if len(valid_df) > 0:
            higher_quality = len(valid_df[valid_df['watched_avg_rating'] > valid_df['catalog_avg_rating']])
            lower_quality = len(valid_df[valid_df['watched_avg_rating'] < valid_df['catalog_avg_rating']])
            equal_quality = len(valid_df) - higher_quality - lower_quality

            summary_text = f"""
QUALITY ANALYSIS SUMMARY
════════════════════════════════════

Actors with Valid Rating Data: {len(valid_df):,}

My Selection Quality:
• Higher than filmography average: {higher_quality:,} ({100*higher_quality/len(valid_df):.1f}%)
• Lower than filmography average: {lower_quality:,} ({100*lower_quality/len(valid_df):.1f}%)
• Equal quality: {equal_quality:,} ({100*equal_quality/len(valid_df):.1f}%)

Average Metrics:
• My selections avg: {valid_df['watched_avg_rating'].mean():.2f}
• Complete filmography avg: {valid_df['catalog_avg_rating'].mean():.2f}
• Difference: {(valid_df['watched_avg_rating'].mean() - valid_df['catalog_avg_rating'].mean()):+.2f}

Interpretation:
{"✅ I'm selecting above-average quality films!" if valid_df['watched_avg_rating'].mean() > valid_df['catalog_avg_rating'].mean() else "📚 Room to explore more highly-rated films!"}
"""
        else:
            summary_text = "No actors with valid rating data available."

        ax4.text(0.05, 0.95, summary_text, transform=ax4.transAxes,
                fontsize=11, verticalalignment='top', fontfamily='monospace',
                bbox=dict(boxstyle='round', facecolor='lightcyan', alpha=0.5))

        plt.suptitle('Quality Pattern Analysis', fontsize=16, fontweight='bold')
        plt.savefig(self.output_dir / 'quality_analysis.png', dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        print("  ✓ Saved quality_analysis.png")

    def generate_report(self):
        """Generate comprehensive text report."""
        print("\n7. Generating text report...")

        report = []
        report.append("=" * 80)
        report.append("ACTOR FILMOGRAPHY COMPLETENESS REPORT")
        report.append("Generated: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        report.append("Data Source: IMDb (same as batch_3_part_2_filmography_completion.py)")
        report.append("=" * 80)
        report.append("")

        # Overall stats
        report.append("OVERALL STATISTICS")
        report.append("-" * 40)
        report.append(f"Total Actors Analyzed: {len(self.actor_df):,}")
        report.append(f"Average Completion Rate: {self.actor_df['completeness_pct'].mean():.1f}%")
        report.append(f"Median Completion Rate: {self.actor_df['completeness_pct'].median():.1f}%")
        report.append(f"Fully Complete (100%): {len(self.actor_df[self.actor_df['completeness_pct'] >= 99.9]):,}")
        report.append(f"Total Films Watched: {self.actor_df['watched_count'].sum():,}")
        report.append(f"Total Missing Films: {self.actor_df['missing_count'].sum():,}")
        report.append("")

        # Top 20 most complete
        report.append("TOP 20 MOST COMPLETE ACTORS")
        report.append("-" * 40)
        top20 = self.actor_df.nlargest(20, 'completeness_pct')
        report.append(f"{'Rank':<6} {'Actor':<35} {'Completion':<12} {'Watched':<10} {'Missing':<10}")
        report.append("─" * 75)
        for i, (_, row) in enumerate(top20.iterrows(), 1):
            report.append(
                f"{i:<6} {row['actor_name'][:33]:<35} {row['completeness_pct']:.1f}%{'':<6} "
                f"{int(row['watched_count']):<10} {int(row['missing_count']):<10}"
            )
        report.append("")

        # Top 20 most watched
        report.append("TOP 20 ACTORS BY FILMS WATCHED")
        report.append("-" * 40)
        top20_watched = self.actor_df.nlargest(20, 'watched_count')
        report.append(f"{'Rank':<6} {'Actor':<35} {'Watched':<12} {'Total':<10} {'Completion':<12}")
        report.append("─" * 75)
        for i, (_, row) in enumerate(top20_watched.iterrows(), 1):
            report.append(
                f"{i:<6} {row['actor_name'][:33]:<35} {int(row['watched_count']):<12} "
                f"{int(row['total_quality_films']):<10} {row['completeness_pct']:.1f}%"
            )
        report.append("")

        # Top missing films
        report.append("TOP 20 RECOMMENDED MISSING FILMS")
        report.append("-" * 40)

        all_missing = []
        for _, row in self.actor_df.iterrows():
            missing_films = row['missing_films'] if isinstance(row['missing_films'], list) else []
            for film in missing_films:
                film_copy = film.copy()
                film_copy['actor'] = row['actor_name']
                all_missing.append(film_copy)

        # Dedupe
        seen = {}
        for film in all_missing:
            title = film.get('title', 'Unknown')
            rating = film.get('rating', 0) or 0
            if title not in seen or rating > (seen[title].get('rating', 0) or 0):
                seen[title] = film
        top_missing = sorted(seen.values(), key=lambda x: x.get('rating', 0) or 0, reverse=True)[:20]

        report.append(f"{'Rank':<6} {'Title':<45} {'Year':<8} {'Rating':<10} {'Actor':<30}")
        report.append("─" * 100)

        for i, film in enumerate(top_missing, 1):
            report.append(
                f"{i:<6} {str(film.get('title', 'Unknown'))[:43]:<45} "
                f"{str(film.get('year', 'N/A')):<8} {(film.get('rating', 0) or 0):<10.1f} "
                f"{str(film.get('actor', ''))[:28]:<30}"
            )

        # Save report
        report_path = self.report_dir / 'batch_34_actor_completeness_report.txt'
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))

        print(f"  ✓ Saved {report_path}")

    def run_all(self):
        """Run all analyses."""
        import time
        total_start = time.time()
        
        print("\nStarting Actor Completeness Analysis...")
        print("Using IMDb-based data (same source as batch_3_part_2)")
        print("This may take a few minutes...\n")

        self.create_overview_dashboard()
        self.create_top5_detailed_breakdown()
        self.create_career_trajectory_visualization()
        self.create_completion_heatmap()
        self.create_missing_films_gallery()
        self.create_quality_analysis()
        self.generate_report()

        total_time = time.time() - total_start
        
        print("\n" + "=" * 80)
        print("✓ ANALYSIS COMPLETE!")
        print("=" * 80)
        print(f"\nVisualizations saved to: {self.output_dir}")
        print(f"Report saved to: {self.report_dir}")
        print("\nCreated visualizations:")
        print("  1. overview_dashboard.png - Comprehensive overview with 7 panels")
        print("  2-6. actor_detail_*.png - 5 detailed actor breakdowns")
        print("  7. career_trajectories.png - Career progression for top 6 actors")
        print("  8. completion_heatmap.png - Completion heatmap visualization")
        print("  9. missing_films_gallery.png - Gallery of top missing films")
        print("  10. quality_analysis.png - Quality pattern analysis")
        print("  11. batch_34_actor_completeness_report.txt - Comprehensive text report")
        print("\nTotal: 11 outputs with 20+ individual visualizations!")
        print("\nData consistency: Using same IMDb data as batch_3_part_2_filmography_completion.py")
        print(f"\n⏱️  TOTAL RUNTIME: {total_time:.1f}s ({total_time/60:.1f} minutes)")


if __name__ == "__main__":
    import sys
    # Use skip_missing_films=True for fast mode (no 38M row principals load)
    fast_mode = '--fast' in sys.argv
    analyzer = ActorCompletenessAnalyzer(skip_missing_films=fast_mode)
    analyzer.run_all()
