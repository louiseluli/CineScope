"""
CineScope TMDb Enrichment Script

This script takes the clean watchlist, finds each movie OR TV series on TMDb,
fetches detailed information, and saves the result to a new file.

The script is designed to be resumable. If interrupted, it will load the
partially enriched file and continue where it left off.

Usage:
    python scripts/enrich/01_enrich_tmdb.py
    python scripts/enrich/01_enrich_tmdb.py --force  (to re-enrich all)
    python scripts/enrich/01_enrich_tmdb.py --limit 50 (to process only 50)
"""
import sys
import pandas as pd
from pathlib import Path
import logging
from tqdm import tqdm
import argparse
import time
from typing import Dict

# Add 'src' to the Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.core.config import settings
from src.core.data_loader import DataLoader
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

class TMDbEnricher:
    """Orchestrates the TMDb enrichment process for movies and TV shows."""

    def __init__(self):
        self.client = TMDbClient()
        self.output_file = settings.PROCESSED_DATA_DIR / "01_tmdb_enriched_media.csv"

    def run(self, force: bool = False, limit: int = None):
        """Executes the full enrichment workflow."""
        source_df = self._load_source_data()
        dest_df = self._load_or_initialize_dest_df(force)
        items_to_process = self._get_items_to_process(source_df, dest_df)
        
        if limit:
            items_to_process = items_to_process.head(limit)

        if items_to_process.empty:
            logger.info("✅ All media are already enriched with TMDb data.")
            logger.info(f"Find the data at: {self.output_file}")
            return

        logger.info(f"Found {len(items_to_process)} items (movies/TV) to enrich with TMDb data.")
        
        enriched_data = []
        with tqdm(total=len(items_to_process), desc="Enriching with TMDb") as pbar:
            for _, row in items_to_process.iterrows():
                try:
                    enriched_row = self._process_item(row)
                    enriched_data.append(enriched_row)
                    if len(enriched_data) % 10 == 0:
                        self._save_checkpoint(dest_df, enriched_data)
                except Exception as e:
                    logger.error(f"An unexpected error occurred for IMDb ID {row['const']}: {e}", exc_info=False)
                finally:
                    pbar.update(1)
                    time.sleep(1 / settings.TMDB_RATE_LIMIT)
        
        # Final save
        self._save_checkpoint(dest_df, enriched_data)

        logger.info("="*60)
        logger.info("✅ TMDb Enrichment Complete!")
        logger.info(f"Processed {len(enriched_data)} new items.")
        logger.info(f"Enriched data saved to: {self.output_file}")
        logger.info("="*60)

    def _load_source_data(self) -> pd.DataFrame:
        loader = DataLoader()
        return loader.load_watchlist()

    def _load_or_initialize_dest_df(self, force: bool) -> pd.DataFrame:
        if self.output_file.exists() and not force:
            logger.info(f"Resuming from existing file: {self.output_file}")
            return pd.read_csv(self.output_file, low_memory=False)
        logger.info("Starting a new enrichment process.")
        return pd.DataFrame()

    def _get_items_to_process(self, source_df: pd.DataFrame, dest_df: pd.DataFrame) -> pd.DataFrame:
        if dest_df.empty:
            return source_df
        enriched_ids = set(dest_df['const'].astype(str).unique())
        return source_df[~source_df['const'].astype(str).isin(enriched_ids)]

    def _process_item(self, item_row: pd.Series) -> Dict:
        """ Fetches and processes data for a single movie or TV show. """
        imdb_id = item_row['const']
        result = item_row.to_dict()
        
        find_result = self.client.find_by_imdb_id(imdb_id)
        if not find_result:
            logger.warning(f"Could not find TMDb entry for IMDb ID: {imdb_id} ('{item_row['title']}')")
            return result
        
        media_type, basic_info = find_result
        tmdb_id = basic_info.get('id')

        if not tmdb_id:
            logger.warning(f"Found TMDb entry but no ID for IMDb ID: {imdb_id}")
            return result

        result['tmdb_id'] = tmdb_id
        result['tmdb_media_type'] = media_type

        if media_type == "movie":
            details = self.client.get_movie_details(tmdb_id)
            if details:
                result = self._parse_movie_details(result, details)
        elif media_type == "tv":
            details = self.client.get_tv_details(tmdb_id)
            if details:
                result = self._parse_tv_details(result, details)
        
        return result
    
    def _parse_movie_details(self, result: Dict, details: Dict) -> Dict:
        """Parses the detailed API response for a movie."""
        result['tmdb_title'] = details.get('title')
        result['tmdb_original_title'] = details.get('original_title')
        result['tmdb_original_language'] = details.get('original_language')
        result['tmdb_tagline'] = details.get('tagline')
        result['tmdb_overview'] = details.get('overview')
        result['tmdb_popularity'] = details.get('popularity')
        result['tmdb_poster_path'] = details.get('poster_path')
        result['tmdb_backdrop_path'] = details.get('backdrop_path')
        result['tmdb_budget'] = details.get('budget')
        result['tmdb_revenue'] = details.get('revenue')
        result['tmdb_status'] = details.get('status')
        result['tmdb_vote_average'] = details.get('vote_average')
        result['tmdb_vote_count'] = details.get('vote_count')
        result['tmdb_release_date'] = details.get('release_date')
        
        result['tmdb_genres'] = [g['name'] for g in details.get('genres', [])]
        result['tmdb_production_companies'] = [c['name'] for c in details.get('production_companies', [])]
        result['tmdb_production_countries'] = [c['name'] for c in details.get('production_countries', [])]
        result['tmdb_spoken_languages'] = [l['english_name'] for l in details.get('spoken_languages', [])]
        result['tmdb_keywords'] = [k['name'] for k in details.get('keywords', {}).get('keywords', [])]
        return result

    def _parse_tv_details(self, result: Dict, details: Dict) -> Dict:
        """Parses the detailed API response for a TV series."""
        result['tmdb_title'] = details.get('name')
        result['tmdb_original_title'] = details.get('original_name')
        result['tmdb_original_language'] = details.get('original_language')
        result['tmdb_tagline'] = details.get('tagline')
        result['tmdb_overview'] = details.get('overview')
        result['tmdb_popularity'] = details.get('popularity')
        result['tmdb_poster_path'] = details.get('poster_path')
        result['tmdb_backdrop_path'] = details.get('backdrop_path')
        result['tmdb_status'] = details.get('status')
        result['tmdb_vote_average'] = details.get('vote_average')
        result['tmdb_vote_count'] = details.get('vote_count')
        result['tmdb_first_air_date'] = details.get('first_air_date')
        result['tmdb_last_air_date'] = details.get('last_air_date')
        result['tmdb_number_of_seasons'] = details.get('number_of_seasons')
        result['tmdb_number_of_episodes'] = details.get('number_of_episodes')

        result['tmdb_genres'] = [g['name'] for g in details.get('genres', [])]
        result['tmdb_production_companies'] = [c['name'] for c in details.get('production_companies', [])]
        # --- THIS IS THE LINE THAT IS NOW FIXED ---
        result['tmdb_production_countries'] = details.get('origin_country', [])
        result['tmdb_spoken_languages'] = [l['english_name'] for l in details.get('spoken_languages', [])]
        result['tmdb_keywords'] = [k['name'] for k in details.get('keywords', {}).get('results', [])]
        result['tmdb_networks'] = [n['name'] for n in details.get('networks', [])]
        return result

    def _save_checkpoint(self, dest_df, new_data):
        """Saves a checkpoint of the currently enriched data."""
        if new_data:
            # Combine the already existing data with the newly fetched chunk
            temp_df = pd.concat([dest_df, pd.DataFrame(new_data).set_index(dest_df.columns[0], drop=False)], ignore_index=True)
            # Remove duplicates just in case, keeping the last entry
            temp_df = temp_df.drop_duplicates(subset=['const'], keep='last')
            temp_df.to_csv(self.output_file, index=False)

def main():
    parser = argparse.ArgumentParser(description="Enrich media from your watchlist with TMDb data.")
    parser.add_argument('--force', action='store_true', help="Re-enrich all media.")
    parser.add_argument('--limit', type=int, help="Limit the number of media items to process.")
    args = parser.parse_args()

    try:
        enricher = TMDbEnricher()
        enricher.run(force=args.force, limit=args.limit)
    except KeyboardInterrupt:
        logger.info("\nInterrupted. Progress saved. Run again to resume.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"A critical error occurred: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()