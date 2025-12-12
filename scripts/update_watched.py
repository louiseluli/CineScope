#!/usr/bin/env python3
"""
CineScope - Incremental Watched Movies Update

This script handles adding NEW watched movies to your enriched database.
It detects new movies from your IMDB export and enriches only those.

WORKFLOW:
1. Export your ratings from IMDB (https://www.imdb.com/list/ratings)
2. Save as data/raw/Watched-NEW.csv (or any name)
3. Run: python scripts/update_watched.py data/raw/Watched-NEW.csv

The script will:
- Detect movies not already in watched_movies_master.csv
- Run all enrichments (TMDB, OMDB, DDD, Wikidata) for new movies only
- Merge new enriched movies into the master file
- Update people_cache.json with any new actors/directors

Usage:
    python scripts/update_watched.py <new_export.csv>
    python scripts/update_watched.py data/raw/Watched-Dec.csv --dry-run
    python scripts/update_watched.py data/raw/Watched-Dec.csv --skip-ddd
"""
import sys
import argparse
import pandas as pd
import json
import time
import logging
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.core.config import settings
from src.enrichment.tmdb_client import TMDbClient
from src.enrichment.omdb_client import OMDbClient
from src.enrichment.ddd_client import DDDClient
from src.enrichment.wikidata_client import WikidataClient

settings.ensure_directories()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(settings.LOG_FILE)
    ]
)
logger = logging.getLogger(__name__)


class IncrementalUpdater:
    """Handles incremental updates to the watched movies database."""
    
    def __init__(self):
        self.master_file = settings.PROCESSED_DATA_DIR / "watched_movies_master.csv"
        self.people_cache_file = settings.PROCESSED_DATA_DIR / "people_cache.json"
        
        # Initialize API clients
        self.tmdb = TMDbClient()
        self.omdb = OMDbClient()
        self.ddd = DDDClient()
        self.wikidata = WikidataClient()
        
        # Load existing data
        self.master_df = self._load_master()
        self.people_cache = self._load_people_cache()
        
        # Stats
        self.stats = {
            'new_movies': 0,
            'tmdb_enriched': 0,
            'omdb_enriched': 0,
            'ddd_enriched': 0,
            'wikidata_enriched': 0,
            'new_people': 0,
            'errors': []
        }
    
    def _load_master(self) -> pd.DataFrame:
        """Load existing master file."""
        if self.master_file.exists():
            df = pd.read_csv(self.master_file, low_memory=False)
            logger.info(f"Loaded {len(df):,} existing movies from master")
            return df
        logger.warning("No master file found - will create new one")
        return pd.DataFrame()
    
    def _load_people_cache(self) -> dict:
        """Load people cache."""
        if self.people_cache_file.exists():
            with open(self.people_cache_file, 'r') as f:
                return json.load(f)
        return {}
    
    def _save_people_cache(self):
        """Save people cache."""
        with open(self.people_cache_file, 'w') as f:
            json.dump(self.people_cache, f, indent=2)
    
    def find_new_movies(self, export_file: Path) -> pd.DataFrame:
        """Find movies in export that aren't in master."""
        # Read new export
        new_df = pd.read_csv(export_file)
        
        # Normalize column names
        column_map = {
            'Const': 'const', 'const': 'const', 'IMDb ID': 'const',
            'Title': 'title', 'Year': 'year', 'Your Rating': 'your_rating',
            'Date Rated': 'date_rated', 'Title Type': 'title_type',
            'IMDb Rating': 'imdb_rating', 'Runtime (mins)': 'runtime_mins',
            'Genres': 'genres', 'Num Votes': 'num_votes', 'Directors': 'directors',
            'URL': 'url', 'Release Date': 'release_date'
        }
        new_df = new_df.rename(columns=column_map)
        
        if 'const' not in new_df.columns:
            logger.error(f"No 'Const' or 'const' column in {export_file}")
            return pd.DataFrame()
        
        # Filter to movies only (skip TV episodes, etc.)
        if 'title_type' in new_df.columns:
            new_df = new_df[new_df['title_type'].isin(['Movie', 'movie', 'tvMovie', 'short'])]
        
        logger.info(f"Found {len(new_df):,} movies in export")
        
        # Find movies not in master
        if not self.master_df.empty:
            existing_ids = set(self.master_df['const'].astype(str).unique())
            new_movies = new_df[~new_df['const'].astype(str).isin(existing_ids)]
        else:
            new_movies = new_df
        
        logger.info(f"🆕 {len(new_movies):,} NEW movies to add")
        self.stats['new_movies'] = len(new_movies)
        
        return new_movies
    
    def enrich_movie(self, row: pd.Series, skip_ddd: bool = False) -> dict:
        """Enrich a single movie with all sources."""
        imdb_id = row['const']
        enriched = row.to_dict()
        
        # 1. TMDB Enrichment
        try:
            tmdb_data = self.tmdb.search_by_imdb_id(imdb_id)
            if tmdb_data:
                for key, value in tmdb_data.items():
                    enriched[f'tmdb_{key}'] = value
                self.stats['tmdb_enriched'] += 1
                
                # Cache people from cast/crew
                self._cache_people_from_tmdb(tmdb_data)
            time.sleep(1 / settings.TMDB_RATE_LIMIT)
        except Exception as e:
            logger.debug(f"TMDB error for {imdb_id}: {e}")
        
        # 2. OMDB Enrichment
        try:
            omdb_data = self.omdb.get_movie_details(imdb_id)
            if omdb_data:
                for key, value in omdb_data.items():
                    enriched[f'omdb_{key}'] = value
                self.stats['omdb_enriched'] += 1
            time.sleep(0.5)
        except Exception as e:
            logger.debug(f"OMDB error for {imdb_id}: {e}")
        
        # 3. DDD Enrichment (optional - slow)
        if not skip_ddd:
            try:
                ddd_data = self.ddd.get_content_warnings(imdb_id)
                if ddd_data:
                    for key, value in ddd_data.items():
                        enriched[f'ddd_{key}'] = value
                    self.stats['ddd_enriched'] += 1
                time.sleep(1.0)
            except Exception as e:
                logger.debug(f"DDD error for {imdb_id}: {e}")
        
        # 4. Wikidata Enrichment
        try:
            wd_data = self.wikidata.get_movie_details(imdb_id)
            if wd_data:
                for key, value in wd_data.items():
                    enriched[key] = value  # Already prefixed with wd_
                self.stats['wikidata_enriched'] += 1
            time.sleep(1.0)
        except Exception as e:
            logger.debug(f"Wikidata error for {imdb_id}: {e}")
        
        return enriched
    
    def _cache_people_from_tmdb(self, tmdb_data: dict):
        """Add new people to cache from TMDB response."""
        # Cast
        cast = tmdb_data.get('cast', [])
        if isinstance(cast, list):
            for person in cast[:10]:  # Top 10 cast
                if isinstance(person, dict):
                    tmdb_id = str(person.get('tmdb_person_id') or person.get('id', ''))
                    if tmdb_id and tmdb_id not in self.people_cache:
                        self.people_cache[tmdb_id] = {
                            'name': person.get('name'),
                            'tmdb_id': int(tmdb_id) if tmdb_id.isdigit() else tmdb_id,
                            'gender': person.get('gender', 0),
                            'known_for_department': 'Acting'
                        }
                        self.stats['new_people'] += 1
        
        # Directors
        directors = tmdb_data.get('directors', [])
        if isinstance(directors, list):
            for person in directors:
                if isinstance(person, dict):
                    tmdb_id = str(person.get('tmdb_id') or person.get('id', ''))
                    if tmdb_id and tmdb_id not in self.people_cache:
                        self.people_cache[tmdb_id] = {
                            'name': person.get('name'),
                            'tmdb_id': int(tmdb_id) if tmdb_id.isdigit() else tmdb_id,
                            'gender': person.get('gender', 0),
                            'known_for_department': 'Directing'
                        }
                        self.stats['new_people'] += 1
    
    def run_update(self, export_file: Path, dry_run: bool = False, skip_ddd: bool = False):
        """Run the incremental update process."""
        logger.info("="*70)
        logger.info("CINESCOPE INCREMENTAL UPDATE")
        logger.info("="*70)
        
        # Find new movies
        new_movies = self.find_new_movies(export_file)
        
        if new_movies.empty:
            logger.info("✅ No new movies to add - master is up to date!")
            return
        
        if dry_run:
            logger.info("\n🔍 DRY RUN - Would add these movies:")
            for _, row in new_movies.head(20).iterrows():
                logger.info(f"  - {row.get('title', 'Unknown')} ({row.get('year', '?')}) [{row['const']}]")
            if len(new_movies) > 20:
                logger.info(f"  ... and {len(new_movies) - 20} more")
            return
        
        # Enrich each new movie
        logger.info(f"\n📥 Enriching {len(new_movies)} new movies...")
        enriched_movies = []
        
        for _, row in tqdm(new_movies.iterrows(), total=len(new_movies), desc="Enriching"):
            try:
                enriched = self.enrich_movie(row, skip_ddd=skip_ddd)
                enriched_movies.append(enriched)
            except Exception as e:
                logger.error(f"Error enriching {row['const']}: {e}")
                self.stats['errors'].append(row['const'])
                enriched_movies.append(row.to_dict())  # Add unenriched
        
        # Create DataFrame from enriched movies
        new_df = pd.DataFrame(enriched_movies)
        
        # Merge with master
        logger.info("\n📊 Merging with master file...")
        if not self.master_df.empty:
            # Align columns
            all_cols = list(set(self.master_df.columns) | set(new_df.columns))
            for col in all_cols:
                if col not in self.master_df.columns:
                    self.master_df[col] = None
                if col not in new_df.columns:
                    new_df[col] = None
            
            updated_master = pd.concat([self.master_df, new_df], ignore_index=True)
        else:
            updated_master = new_df
        
        # Remove duplicates (keep latest)
        updated_master = updated_master.drop_duplicates(subset='const', keep='last')
        
        # Save updated master
        backup_file = self.master_file.with_suffix(f'.csv.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
        if self.master_file.exists():
            import shutil
            shutil.copy(self.master_file, backup_file)
            logger.info(f"Backed up to: {backup_file.name}")
        
        updated_master.to_csv(self.master_file, index=False)
        logger.info(f"Saved {len(updated_master):,} movies to {self.master_file.name}")
        
        # Save people cache
        self._save_people_cache()
        
        # Report
        self._print_report()
    
    def _print_report(self):
        """Print summary report."""
        logger.info("\n" + "="*70)
        logger.info("UPDATE COMPLETE!")
        logger.info("="*70)
        logger.info(f"New movies added:     {self.stats['new_movies']:,}")
        logger.info(f"TMDB enriched:        {self.stats['tmdb_enriched']:,}")
        logger.info(f"OMDB enriched:        {self.stats['omdb_enriched']:,}")
        logger.info(f"DDD enriched:         {self.stats['ddd_enriched']:,}")
        logger.info(f"Wikidata enriched:    {self.stats['wikidata_enriched']:,}")
        logger.info(f"New people cached:    {self.stats['new_people']:,}")
        if self.stats['errors']:
            logger.warning(f"Errors:               {len(self.stats['errors'])}")
        logger.info("="*70)


def main():
    parser = argparse.ArgumentParser(
        description="Incrementally update watched movies with new IMDB export",
        epilog="""
Examples:
  python scripts/update_watched.py data/raw/Watched-NEW.csv
  python scripts/update_watched.py data/raw/Watched-Dec.csv --dry-run
  python scripts/update_watched.py data/raw/Watched-Dec.csv --skip-ddd
        """
    )
    parser.add_argument('export_file', type=Path, 
                       help="Path to new IMDB export CSV (e.g., data/raw/Watched-NEW.csv)")
    parser.add_argument('--dry-run', action='store_true',
                       help="Show what would be added without making changes")
    parser.add_argument('--skip-ddd', action='store_true',
                       help="Skip DDD enrichment (faster, but no content warnings)")
    
    args = parser.parse_args()
    
    if not args.export_file.exists():
        logger.error(f"File not found: {args.export_file}")
        sys.exit(1)
    
    try:
        updater = IncrementalUpdater()
        updater.run_update(args.export_file, dry_run=args.dry_run, skip_ddd=args.skip_ddd)
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
