"""
CineScope OMDb Enrichment Script

This is the second enrichment script. It reads the TMDb-enriched data,
fetches supplementary information from OMDb (like Metascore, Rotten Tomatoes
ratings, awards), and saves it to a new file.

This script is designed to respect OMDb's 1,000 daily API call limit. It
tracks daily usage and will pause itself, ready to be resumed the next day.

Usage:
    python scripts/enrich/02_enrich_omdb.py
    python scripts/enrich/02_enrich_omdb.py --force  (to re-enrich all items)
    python scripts/enrich/02_enrich_omdb.py --limit 50 (to process only 50 items)
"""
import sys
import pandas as pd
from pathlib import Path
import logging
from tqdm import tqdm
import argparse
import time
import json
from datetime import datetime

# Add 'src' to the Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.core.config import settings
from src.enrichment.omdb_client import OMDbClient

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

class OMDbEnricher:
    """Orchestrates the OMDb enrichment process with daily limit handling."""
    
    # Stop a bit short of the actual limit to be safe
    DAILY_LIMIT = 980 

    def __init__(self):
        self.client = OMDbClient()
        self.input_file = settings.PROCESSED_DATA_DIR / "01_tmdb_enriched_media.csv"
        self.output_file = settings.PROCESSED_DATA_DIR / "02_omdb_enriched_media.csv"
        self.status_file = settings.PROCESSED_DATA_DIR / "omdb_enrichment_status.json"
        self.status = self._load_status()

    def _load_status(self) -> dict:
        """Loads enrichment status, resetting daily count if a new day has started."""
        today = datetime.now().strftime("%Y-%m-%d")
        if self.status_file.exists():
            with open(self.status_file, 'r') as f:
                status = json.load(f)
            # Reset daily call count if it's a new day
            if status.get("last_run_date") != today:
                status["calls_today"] = 0
                status["last_run_date"] = today
            return status
        
        return {"calls_today": 0, "last_run_date": today, "enriched_ids": []}

    def _save_status(self):
        """Saves the current enrichment status to a file."""
        with open(self.status_file, 'w') as f:
            json.dump(self.status, f, indent=4)

    def run(self, force: bool = False, limit: int = None):
        """Executes the full enrichment workflow."""
        source_df = self._load_source_data()
        dest_df = self._load_or_initialize_dest_df(force)
        items_to_process = self._get_items_to_process(source_df, dest_df)
        
        if limit:
            items_to_process = items_to_process.head(limit)

        if items_to_process.empty:
            logger.info("✅ All media are already enriched with OMDb data.")
            return

        logger.info(f"Found {len(items_to_process)} items to enrich with OMDb data.")

        enriched_data = []
        with tqdm(total=len(items_to_process), desc="Enriching with OMDb") as pbar:
            for _, row in items_to_process.iterrows():
                if self.status["calls_today"] >= self.DAILY_LIMIT:
                    logger.warning("Reached OMDb daily API limit.")
                    logger.info("Run the script again tomorrow to continue.")
                    break
                
                try:
                    enriched_row = self._process_item(row)
                    enriched_data.append(enriched_row)
                    self.status["calls_today"] += 1
                except Exception as e:
                    logger.error(f"Unexpected error for IMDb ID {row['const']}: {e}")
                finally:
                    pbar.update(1)
        
        # Save results
        if enriched_data:
            new_data_df = pd.DataFrame(enriched_data)
            final_df = pd.concat([dest_df, new_data_df], ignore_index=True)
            final_df.to_csv(self.output_file, index=False)
            # Update the list of enriched IDs in the status
            self.status["enriched_ids"].extend(new_data_df['const'].tolist())

        self._save_status()
        
        logger.info("="*60)
        logger.info("✅ OMDb Enrichment Run Complete!")
        logger.info(f"Processed {len(enriched_data)} new items in this run.")
        logger.info(f"OMDb API calls today: {self.status['calls_today']}/{self.DAILY_LIMIT}")
        logger.info(f"Enriched data saved to: {self.output_file}")
        logger.info("="*60)


    def _load_source_data(self) -> pd.DataFrame:
        if not self.input_file.exists():
            logger.error(f"Input file not found: {self.input_file}")
            logger.error("Please run the TMDb enrichment script first.")
            sys.exit(1)
        return pd.read_csv(self.input_file, low_memory=False)

    def _load_or_initialize_dest_df(self, force: bool) -> pd.DataFrame:
        if self.output_file.exists() and not force:
            logger.info(f"Resuming from existing file: {self.output_file}")
            return pd.read_csv(self.output_file, low_memory=False)
        
        logger.info("Starting new OMDb enrichment. Using TMDb file as base.")
        # If starting fresh, the destination is the same as the source initially
        return self._load_source_data()

    def _get_items_to_process(self, source_df: pd.DataFrame, dest_df: pd.DataFrame) -> pd.DataFrame:
        # A movie needs processing if it doesn't have the 'omdb_title' column
        # or if the value in that column is null.
        if 'omdb_title' not in dest_df.columns:
            return source_df
        
        processed_ids = set(dest_df.dropna(subset=['omdb_title'])['const'].unique())
        return source_df[~source_df['const'].isin(processed_ids)]

    def _process_item(self, item_row: pd.Series) -> Dict:
        """Fetches and parses OMDb data for a single item."""
        imdb_id = item_row['const']
        details = self.client.get_details_by_imdb_id(imdb_id)
        
        # Start with the original data from the input row
        result = item_row.to_dict()

        if not details:
            return result # Return original data if not found

        # --- Add new, OMDb-specific fields ---
        result['omdb_title'] = details.get('Title')
        result['omdb_rated'] = details.get('Rated')
        result['omdb_released'] = details.get('Released')
        result['omdb_plot'] = details.get('Plot')
        result['omdb_language'] = details.get('Language')
        result['omdb_country'] = details.get('Country')
        result['omdb_awards'] = details.get('Awards')
        result['omdb_metascore'] = pd.to_numeric(details.get('Metascore'), errors='coerce')
        result['omdb_imdb_rating'] = pd.to_numeric(details.get('imdbRating'), errors='coerce')
        result['omdb_imdb_votes'] = pd.to_numeric(
            details.get('imdbVotes', '').replace(',', ''), errors='coerce'
        )
        result['omdb_box_office'] = details.get('BoxOffice')
        result['omdb_dvd_release'] = details.get('DVD')
        result['omdb_production_co'] = details.get('Production')

        # Safely parse the 'Ratings' list
        ratings = details.get('Ratings', [])
        for rating in ratings:
            source = rating.get('Source', '').replace(' ', '_')
            if source == 'Internet_Movie_Database':
                result['omdb_rating_imdb'] = rating.get('Value')
            elif source == 'Rotten_Tomatoes':
                result['omdb_rating_rotten_tomatoes'] = rating.get('Value')
            elif source == 'Metacritic':
                result['omdb_rating_metacritic'] = rating.get('Value')
        
        return result

def main():
    parser = argparse.ArgumentParser(description="Enrich media with OMDb data.")
    parser.add_argument(
        '--force', action='store_true', help="Re-enrich all media, ignoring previous OMDb data."
    )

    parser.add_argument(
        '--limit', type=int, help="Limit the number of items to process in this run."
    )
    args = parser.parse_args()

    try:
        enricher = OMDbEnricher()
        enricher.run(force=args.force, limit=args.limit)
    except KeyboardInterrupt:
        logger.info("\nInterrupted. Progress saved. Run again to resume.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"A critical error occurred: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()