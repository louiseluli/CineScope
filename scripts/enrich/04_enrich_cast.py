"""
CineScope Cast & People Enrichment Script

This script enriches your movies with full cast and crew information including:
- Actors with character names
- Directors, writers, producers
- Person details: photos, bios, birth/death dates
- Filmographies

Data sources: IMDb database (principals table) + TMDb People API

Usage:
    python scripts/enrich/04_enrich_cast.py
    python scripts/enrich/04_enrich_cast.py --force
    python scripts/enrich/04_enrich_cast.py --limit 50
"""
import sys
import pandas as pd
from pathlib import Path
import logging
from tqdm import tqdm
import argparse
import time
from typing import Dict, List
import json

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.core.config import settings
from src.core.db_connector import DBConnector
from src.enrichment.tmdb_client import TMDbClient

settings.ensure_directories()

logging.basicConfig(
    level=settings.LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(settings.LOG_FILE)
    ]
)
logger = logging.getLogger(__name__)


class CastEnricher:
    """Enriches movies with full cast and crew information."""
    
    def __init__(self):
        self.tmdb_client = TMDbClient()
        self.input_file = settings.PROCESSED_DATA_DIR / "03_ddd_enriched_media.csv"
        self.output_file = settings.PROCESSED_DATA_DIR / "04_cast_enriched_media.csv"
        self.people_cache_file = settings.PROCESSED_DATA_DIR / "people_cache.json"
        self.people_cache = self._load_people_cache()
        
    def _load_people_cache(self) -> Dict:
        """Load cached people data to avoid redundant API calls."""
        if self.people_cache_file.exists():
            with open(self.people_cache_file, 'r') as f:
                return json.load(f)
        return {}
    
    def _save_people_cache(self):
        """Save people cache to file."""
        with open(self.people_cache_file, 'w') as f:
            json.dump(self.people_cache, f, indent=2)
    
    def run(self, force: bool = False, limit: int = None):
        """Execute the full cast enrichment workflow."""
        source_df = self._load_source_data()
        dest_df = self._load_or_initialize_dest_df(force)
        items_to_process = self._get_items_to_process(source_df, dest_df)
        
        if limit:
            items_to_process = items_to_process.head(limit)
        
        if items_to_process.empty:
            logger.info("✅ All media already enriched with cast data.")
            return
        
        logger.info(f"Found {len(items_to_process)} items to enrich with cast data.")
        
        # Load IMDb principals (cast/crew) data
        imdb_principals = self._load_imdb_principals()
        logger.info(f"Loaded {len(imdb_principals)} cast/crew records from IMDb")
        
        enriched_data = []
        with tqdm(total=len(items_to_process), desc="Enriching with Cast") as pbar:
            for _, row in items_to_process.iterrows():
                try:
                    enriched_row = self._process_item(row, imdb_principals)
                    enriched_data.append(enriched_row)
                    
                    # Save checkpoint every 10 items
                    if len(enriched_data) % 10 == 0:
                        self._save_checkpoint(dest_df, enriched_data)
                        self._save_people_cache()
                        
                except Exception as e:
                    logger.error(f"Error processing {row['const']}: {e}")
                finally:
                    pbar.update(1)
                    time.sleep(1 / settings.TMDB_RATE_LIMIT)
        
        # Final save
        self._save_checkpoint(dest_df, enriched_data)
        self._save_people_cache()
        
        logger.info("="*60)
        logger.info("✅ Cast Enrichment Complete!")
        logger.info(f"Processed {len(enriched_data)} items.")
        logger.info(f"Cached {len(self.people_cache)} unique people.")
        logger.info(f"Output: {self.output_file}")
        logger.info("="*60)
    
    def _load_source_data(self) -> pd.DataFrame:
        """Load the DDD-enriched data."""
        if not self.input_file.exists():
            logger.error(f"Input file not found: {self.input_file}")
            logger.error("Please run 03_enrich_ddd.py first.")
            sys.exit(1)
        return pd.read_csv(self.input_file, low_memory=False)
    
    def _load_or_initialize_dest_df(self, force: bool) -> pd.DataFrame:
        """Load existing cast-enriched data or start fresh."""
        if self.output_file.exists() and not force:
            logger.info(f"Resuming from: {self.output_file}")
            return pd.read_csv(self.output_file, low_memory=False)
        logger.info("Starting new cast enrichment.")
        return pd.DataFrame()
    
    def _get_items_to_process(self, source_df: pd.DataFrame, dest_df: pd.DataFrame) -> pd.DataFrame:
        """Determine which items need cast enrichment."""
        if dest_df.empty:
            return source_df
        enriched_ids = set(dest_df['const'].astype(str).unique())
        return source_df[~source_df['const'].astype(str).isin(enriched_ids)]
    
    def _load_imdb_principals(self) -> pd.DataFrame:
        """Load cast/crew data from IMDb principals table."""
        imdb_db = settings.RAW_DATA_DIR / 'imdb.db'
        if not imdb_db.exists():
            logger.warning("IMDb database not found. Cast data will be limited.")
            return pd.DataFrame()
        
        connector = DBConnector(imdb_db)
        tables = connector.get_table_names()
        
        if 'title_principals' not in tables:
            logger.warning("title_principals table not found. Run setup_database.py with all tables.")
            return pd.DataFrame()
        
        principals_df = connector.get_table_as_df('title_principals')
        return principals_df
    
    def _process_item(self, item_row: pd.Series, imdb_principals: pd.DataFrame) -> Dict:
        """Process cast/crew for a single movie."""
        imdb_id = item_row['const']
        tmdb_id = item_row.get('tmdb_id')
        result = item_row.to_dict()
        
        # Get cast from IMDb principals table
        if not imdb_principals.empty:
            movie_principals = imdb_principals[imdb_principals['tconst'] == imdb_id]
            
            if not movie_principals.empty:
                # Separate by category
                actors = movie_principals[movie_principals['category'].isin(['actor', 'actress'])]
                directors = movie_principals[movie_principals['category'] == 'director']
                writers = movie_principals[movie_principals['category'] == 'writer']
                producers = movie_principals[movie_principals['category'] == 'producer']
                
                # Store top cast (ordered by billing)
                result['imdb_cast_ids'] = actors.head(20)['nconst'].tolist()
                result['imdb_cast_characters'] = actors.head(20)['characters'].tolist()
                result['imdb_director_ids'] = directors['nconst'].tolist()
                result['imdb_writer_ids'] = writers['nconst'].tolist()
                result['imdb_producer_ids'] = producers.head(10)['nconst'].tolist()
        
        # Enrich with TMDb cast data if available
        if pd.notna(tmdb_id):
            tmdb_cast = self._get_tmdb_cast(int(tmdb_id), item_row.get('tmdb_media_type', 'movie'))
            if tmdb_cast:
                result.update(tmdb_cast)
        
        return result
    
    def _get_tmdb_cast(self, tmdb_id: int, media_type: str) -> Dict:
        """Get cast and crew from TMDb with person details."""
        result = {}
        
        try:
            if media_type == 'movie':
                credits = self.tmdb_client._make_request(f"/movie/{tmdb_id}/credits")
            elif media_type == 'tv':
                credits = self.tmdb_client._make_request(f"/tv/{tmdb_id}/credits")
            else:
                return result
            
            if not credits:
                return result
            
            # Process cast (actors)
            cast = credits.get('cast', [])[:20]  # Top 20 billed
            cast_data = []
            
            for person in cast:
                person_id = person.get('id')
                person_info = {
                    'tmdb_person_id': person_id,
                    'name': person.get('name'),
                    'character': person.get('character'),
                    'order': person.get('order'),
                    'profile_path': person.get('profile_path')
                }
                
                # Get detailed person info (with caching)
                if person_id:
                    detailed_info = self._get_person_details(person_id)
                    if detailed_info:
                        person_info.update(detailed_info)
                
                cast_data.append(person_info)
            
            result['tmdb_cast'] = cast_data
            
            # Process crew
            crew = credits.get('crew', [])
            
            # Extract key crew members
            result['tmdb_directors'] = [
                {'name': c['name'], 'tmdb_id': c['id']} 
                for c in crew if c.get('job') == 'Director'
            ]
            result['tmdb_writers'] = [
                {'name': c['name'], 'tmdb_id': c['id']} 
                for c in crew if c.get('department') == 'Writing'
            ][:10]
            result['tmdb_producers'] = [
                {'name': c['name'], 'tmdb_id': c['id']} 
                for c in crew if c.get('job') in ['Producer', 'Executive Producer']
            ][:10]
            result['tmdb_cinematographers'] = [
                {'name': c['name'], 'tmdb_id': c['id']} 
                for c in crew if c.get('job') == 'Director of Photography'
            ]
            result['tmdb_composers'] = [
                {'name': c['name'], 'tmdb_id': c['id']} 
                for c in crew if c.get('department') == 'Sound' and 'Composer' in c.get('job', '')
            ]
            
        except Exception as e:
            logger.debug(f"Error getting TMDb cast for {tmdb_id}: {e}")
        
        return result
    
    def _get_person_details(self, person_id: int) -> Dict:
        """Get detailed person information from TMDb (with caching)."""
        cache_key = str(person_id)
        
        # Check cache first
        if cache_key in self.people_cache:
            return self.people_cache[cache_key]
        
        try:
            person = self.tmdb_client._make_request(f"/person/{person_id}")
            if not person:
                return {}
            
            details = {
                'biography': person.get('biography'),
                'birthday': person.get('birthday'),
                'deathday': person.get('deathday'),
                'place_of_birth': person.get('place_of_birth'),
                'known_for_department': person.get('known_for_department'),
                'gender': person.get('gender'),  # 1=female, 2=male
                'popularity': person.get('popularity')
            }
            
            # Cache the result
            self.people_cache[cache_key] = details
            return details
            
        except Exception as e:
            logger.debug(f"Error getting person {person_id}: {e}")
            return {}
    
    def _save_checkpoint(self, dest_df: pd.DataFrame, new_data: List[Dict]):
        """Save enrichment checkpoint."""
        if new_data:
            new_df = pd.DataFrame(new_data)
            final_df = pd.concat([dest_df, new_df], ignore_index=True)
            final_df = final_df.drop_duplicates(subset='const', keep='last')
            final_df.to_csv(self.output_file, index=False)


def main():
    parser = argparse.ArgumentParser(description="Enrich media with full cast and crew data.")
    parser.add_argument('--force', action='store_true', help="Re-enrich all media.")
    parser.add_argument('--limit', type=int, help="Limit number of items to process.")
    args = parser.parse_args()
    
    try:
        enricher = CastEnricher()
        enricher.run(force=args.force, limit=args.limit)
    except KeyboardInterrupt:
        logger.info("\nInterrupted. Progress saved. Run again to resume.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Critical error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()