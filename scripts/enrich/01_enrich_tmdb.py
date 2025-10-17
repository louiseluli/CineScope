"""
CineScope TMDb Enrichment Script

This is the first enrichment script to run. It takes the clean watchlist,
finds each movie on TMDb using its IMDb ID, fetches detailed information,
and saves the result to a new file.

The script is designed to be resumable. If interrupted, it will load the
partially enriched file and continue where it left off.

Usage:
    python scripts/enrich/01_enrich_tmdb.py
    python scripts/enrich/01_enrich_tmdb.py --force  (to re-enrich all movies)
    python scripts/enrich/01_enrich_tmdb.py --limit 50 (to process only 50 movies)
"""
import sys
import pandas as pd
from pathlib import Path
import logging
from tqdm import tqdm
import argparse
import time

# Add 'src' to the Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.core.config import settings
from src.core.data_loader import DataLoader
from src.enrichment.tmdb_client import TMDbClient

# --- Logging Configuration ---
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
    """Orchestrates the TMDb enrichment process."""

    def __init__(self):
        self.client = TMDbClient()
        self.output_file = settings.PROCESSED_DATA_DIR / "01_tmdb_enriched_movies.csv"

    def run(self, force: bool = False, limit: Optional[int] = None):
        """
        Executes the full enrichment workflow.

        Args:
            force (bool): If True, re-processes all movies.
            limit (int, optional): The maximum number of movies to process.
        """
        settings.ensure_directories()
        
        # Load source data
        source_df = self._load_source_data()

        # Load or initialize the destination DataFrame
        dest_df = self._load_or_initialize_dest_df(source_df, force)

        # Identify movies to process
        movies_to_process = self._get_movies_to_process(source_df, dest_df)
        
        if limit:
            movies_to_process = movies_to_process.head(limit)

        if movies_to_process.empty:
            logger.info("✅ All movies are already enriched with TMDb data.")
            logger.info(f"Find the data at: {self.output_file}")
            return

        logger.info(f"Found {len(movies_to_process)} movies to enrich with TMDb data.")

        enriched_data = []
        with tqdm(total=len(movies_to_process), desc="Enriching with TMDb") as pbar:
            for _, row in movies_to_process.iterrows():
                try:
                    enriched_row = self._process_movie(row)
                    enriched_data.append(enriched_row)
                    self._save_checkpoint(dest_df, enriched_data)
                except Exception as e:
                    logger.error(f"An unexpected error occurred for IMDb ID {row['const']}: {e}")
                finally:
                    pbar.update(1)
                    # Respect API rate limits
                    time.sleep(1 / settings.TMDB_RATE_LIMIT)
        
        # Final save
        final_df = pd.concat([dest_df, pd.DataFrame(enriched_data)], ignore_index=True)
        final_df.to_csv(self.output_file, index=False)

        logger.info("="*60)
        logger.info("✅ TMDb Enrichment Complete!")
        logger.info(f"Processed {len(enriched_data)} new movies.")
        logger.info(f"Enriched data saved to: {self.output_file}")
        logger.info("="*60)


    def _load_source_data(self) -> pd.DataFrame:
        """Loads the initial watchlist."""
        loader = DataLoader()
        return loader.load_watchlist()

    def _load_or_initialize_dest_df(self, source_df: pd.DataFrame, force: bool) -> pd.DataFrame:
        """Loads the existing enriched file or creates a new DataFrame."""
        if self.output_file.exists() and not force:
            logger.info(f"Resuming from existing file: {self.output_file}")
            return pd.read_csv(self.output_file)
        logger.info("Starting a new enrichment process.")
        return pd.DataFrame()

    def _get_movies_to_process(self, source_df: pd.DataFrame, dest_df: pd.DataFrame) -> pd.DataFrame:
        """Determines which movies still need to be enriched."""
        if dest_df.empty:
            return source_df
        
        enriched_ids = set(dest_df['const'].unique())
        return source_df[~source_df['const'].isin(enriched_ids)]

    def _process_movie(self, movie_row: pd.Series) -> Dict:
        """Fetches and processes data for a single movie."""
        imdb_id = movie_row['const']
        
        # Start with the original data
        result = movie_row.to_dict()

        # Find movie on TMDb
        basic_info = self.client.find_movie_by_imdb_id(imdb_id)
        if not basic_info:
            logger.warning(f"Could not find TMDb entry for IMDb ID: {imdb_id} ('{movie_row['title']}')")
            return result
        
        tmdb_id = basic_info.get('id')
        if not tmdb_id:
            logger.warning(f"Found TMDb entry but no ID for IMDb ID: {imdb_id}")
            return result

        # Get detailed movie info
        details = self.client.get_movie_details(tmdb_id)
        if not details:
            logger.warning(f"Could not fetch details for TMDb ID: {tmdb_id}")
            return result
        
        # --- Add new, enriched fields ---
        # We prefix with 'tmdb_' to avoid column name conflicts
        result['tmdb_id'] = tmdb_id
        result['tmdb_title'] = details.get('title')
        result['tmdb_original_title'] = details.get('original_title')
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
        
        # Extract nested data safely
        result['tmdb_genres'] = [g['name'] for g in details.get('genres', [])]
        result['tmdb_production_companies'] = [c['name'] for c in details.get('production_companies', [])]
        result['tmdb_production_countries'] = [c['name'] for c in details.get('production_countries', [])]
        result['tmdb_spoken_languages'] = [l['english_name'] for l in details.get('spoken_languages', [])]
        result['tmdb_keywords'] = [k['name'] for k in details.get('keywords', {}).get('keywords', [])]

        return result

    def _save_checkpoint(self, dest_df, new_data):
        """Saves a checkpoint of the currently enriched data."""
        if new_data:
            temp_df = pd.concat([dest_df, pd.DataFrame(new_data)], ignore_index=True)
            temp_df.to_csv(self.output_file, index=False)

def main():
    """Main execution function with command-line argument parsing."""
    parser = argparse.ArgumentParser(description="Enrich movies from your watchlist with TMDb data.")
    parser.add_argument(
        '--force',
        action='store_true',
        help="Re-enrich all movies, even if an enriched file already exists."
    )
    parser.add_argument(
        '--limit',
        type=int,
        help="Limit the number of movies to process in this run."
    )
    args = parser.parse_args()

    try:
        enricher = TMDbEnricher()
        enricher.run(force=args.force, limit=args.limit)
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user. Progress has been saved. Run again to resume.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"A critical error occurred: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()