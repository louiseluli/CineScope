"""
CineScope Does the Dog Die? (DDD) Enrichment Script

This is the third enrichment script. It reads the OMDb-enriched data,
fetches content warnings from the DDD API, and saves the result to a new file.

The script transforms the list of topics from the API into individual columns
for each warning, making the data easy to analyze.

Usage:
    python scripts/enrich/03_enrich_ddd.py
    python scripts/enrich/03_enrich_ddd.py --force
    python scripts/enrich/03_enrich_ddd.py --limit 50
"""
import sys
import pandas as pd
from pathlib import Path
import logging
from tqdm import tqdm
import argparse
import time
from typing import Dict, List

# Add 'src' to the Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.core.config import settings
from src.enrichment.ddd_client import DDDClient

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

class DDDEnricher:
    """Orchestrates the Does the Dog Die? enrichment process."""

    def __init__(self):
        self.client = DDDClient()
        self.input_file = settings.PROCESSED_DATA_DIR / "02_omdb_enriched_media.csv"
        self.output_file = settings.PROCESSED_DATA_DIR / "03_ddd_enriched_media.csv"

    def run(self, force: bool = False, limit: int = None):
        """Executes the full enrichment workflow."""
        source_df = self._load_source_data()
        
        # Load existing file to resume, or start fresh
        if self.output_file.exists() and not force:
            logger.info(f"Resuming from existing file: {self.output_file}")
            dest_df = pd.read_csv(self.output_file, low_memory=False)
            
            # Ensure alignment if source has changed
            if len(dest_df) != len(source_df):
                logger.warning("Length mismatch between source and dest. Using source as base and merging existing data.")
                # This is a simple recovery: use source as base, merge known DDD cols
                temp_dest = dest_df.set_index('const')
                dest_df = source_df.copy()
                # Find DDD columns
                ddd_cols = [c for c in temp_dest.columns if c.startswith('ddd_')]
                if ddd_cols:
                    dest_df = dest_df.merge(temp_dest[ddd_cols], on='const', how='left')
        else:
            logger.info("Starting new DDD enrichment or running with --force.")
            dest_df = source_df.copy()

        items_to_process = self._get_items_to_process(dest_df)
        
        if limit:
            items_to_process = items_to_process.head(limit)

        if items_to_process.empty:
            logger.info("✅ All media are already enriched with DDD data.")
            return

        logger.info(f"Found {len(items_to_process)} items to enrich with DDD data.")
        
        processed_indices = []
        enriched_rows_data = []

        # We will flush the buffer every N items to keep memory usage low
        # and ensure regular saves.
        BATCH_SIZE = 20

        with tqdm(total=len(items_to_process), desc="Enriching with DDD") as pbar:
            for index, row in items_to_process.iterrows():
                try:
                    enriched_data = self._process_item(row)
                    processed_indices.append(index)
                    enriched_rows_data.append(enriched_data)

                    # Checkpoint saving
                    if len(processed_indices) >= BATCH_SIZE:
                        self._save_batch(dest_df, enriched_rows_data, processed_indices)
                        # Clear buffers
                        processed_indices = []
                        enriched_rows_data = []

                except Exception as e:
                    logger.error(f"Unexpected error for IMDb ID {row['const']}: {e}")
                finally:
                    pbar.update(1)
                    # DDD doesn't have a strict rate limit, but it's polite to be gentle
                    time.sleep(0.1) 
        
        # Final save of remaining items
        if enriched_rows_data:
            self._save_batch(dest_df, enriched_rows_data, processed_indices)

        logger.info("="*60)
        logger.info("✅ DDD Enrichment Run Complete!")
        logger.info(f"Enriched data saved to: {self.output_file}")
        logger.info("="*60)

    def _save_batch(self, dest_df: pd.DataFrame, data: List[Dict], indices: List[int]):
        """Helper to safely update the dataframe with new columns and save."""
        if not data:
            return

        temp_update_df = pd.DataFrame(data, index=indices)
        
        # --- FIX: Ensure new columns exist in dest_df before updating ---
        # Pandas .update() ignores columns that don't exist in the target df.
        # We must create them first.
        new_cols = temp_update_df.columns.difference(dest_df.columns)
        if not new_cols.empty:
            # Efficiently add new columns initialized to None/NaN
            dest_df[list(new_cols)] = pd.NA
        
        # Now update existing rows with new data
        dest_df.update(temp_update_df)
        
        # Save to disk
        dest_df.to_csv(self.output_file, index=False)
        logger.debug(f"Saved batch of {len(data)} items.")

    def _load_source_data(self) -> pd.DataFrame:
        if not self.input_file.exists():
            # Fallback: if 02_omdb doesn't exist, try 01_tmdb
            # This allows skipping OMDb if it's failing
            alt_input = settings.PROCESSED_DATA_DIR / "01_tmdb_enriched_media.csv"
            if alt_input.exists():
                logger.warning(f"OMDb file not found. Falling back to: {alt_input.name}")
                return pd.read_csv(alt_input, low_memory=False)
            
            logger.error(f"Input file not found: {self.input_file}")
            logger.error("Please run the TMDb (01) or OMDb (02) enrichment script first.")
            sys.exit(1)
        return pd.read_csv(self.input_file, low_memory=False)

    def _get_items_to_process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Finds rows that haven't been successfully enriched yet."""
        # An item needs processing if the 'ddd_id' column (our marker) is null or doesn't exist.
        if 'ddd_id' not in df.columns:
            return df
        return df[df['ddd_id'].isnull()]

    def _process_item(self, item_row: pd.Series) -> Dict:
        """Fetches and parses DDD data for a single item."""
        imdb_id = item_row['const']
        ddd_data = self.client.get_ddd_info_by_imdb_id(imdb_id)
        
        result = {} # We will only return the new columns

        if not ddd_data or 'item' not in ddd_data:
            result['ddd_id'] = "NOT_FOUND" # Mark as processed but not found
            return result

        result['ddd_id'] = ddd_data['item'].get('id')
        
        # --- Data Transformation ---
        # Pivot the topic stats into individual columns
        triggers = ddd_data.get('topicItemStats', [])
        
        # Check if triggers is actually a list (API sometimes returns None)
        if triggers and isinstance(triggers, list):
            for trigger in triggers:
                topic = trigger.get('topic')
                if not topic:
                    continue

                # Create a clean column name like 'ddd_a_dog_dies'
                col_name = f"ddd_{topic.get('name', 'unknown').lower().replace(' ', '_')}"
                
                # Determine the vote status
                yes_votes = trigger.get('yesSum', 0)
                no_votes = trigger.get('noSum', 0)
                
                # Use a clear categorical result
                if yes_votes > no_votes:
                    result[col_name] = "Yes"
                elif no_votes > yes_votes:
                    result[col_name] = "No"
                elif yes_votes > 0 and yes_votes == no_votes:
                    result[col_name] = "Controversial"
                else: # (yes_votes == 0 and no_votes == 0)
                    result[col_name] = "No Votes"
        
        return result

def main():
    parser = argparse.ArgumentParser(description="Enrich media with Does the Dog Die? content warnings.")
    parser.add_argument('--force', action='store_true', help="Re-enrich all media with DDD data.")
    parser.add_argument('--limit', type=int, help="Limit the number of items to process in this run.")
    args = parser.parse_args()

    try:
        enricher = DDDEnricher()
        enricher.run(force=args.force, limit=args.limit)
    except KeyboardInterrupt:
        logger.info("\nInterrupted. Progress has been saved. Run again to resume.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"A critical error occurred: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()