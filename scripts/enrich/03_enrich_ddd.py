"""
CineScope Does the Dog Die? (DDD) Enrichment Script

This is the third enrichment script. It reads the OMDb-enriched data,
fetches content warnings from the DDD API, and saves the result to a new file.

The script transforms the list of topics from the API into individual columns
for each warning, making the data easy to analyze.

Usage:
    python scripts/enrich/03_enrich_ddd.py
    python scripts/enrich/03_enrich_ddd.py --force
    python scripts/enrich/03_enrich_drum.py --limit 50
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
        
        if self.output_file.exists() and not force:
            dest_df = pd.read_csv(self.output_file, low_memory=False)
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

        with tqdm(total=len(items_to_process), desc="Enriching with DDD") as pbar:
            for index, row in items_to_process.iterrows():
                try:
                    enriched_data = self._process_item(row)
                    processed_indices.append(index)
                    enriched_rows_data.append(enriched_data)

                    # Checkpoint saving
                    if len(processed_indices) % 20 == 0:
                        temp_update_df = pd.DataFrame(enriched_rows_data, index=processed_indices)
                        dest_df.update(temp_update_df)
                        dest_df.to_csv(self.output_file, index=False)

                except Exception as e:
                    logger.error(f"Unexpected error for IMDb ID {row['const']}: {e}")
                finally:
                    pbar.update(1)
                    # DDD doesn't have a strict rate limit, but it's polite to be gentle
                    time.sleep(0.1) 
        
        # Final save of all new data from this run
        if enriched_rows_data:
            update_df = pd.DataFrame(enriched_rows_data, index=processed_indices)
            dest_df.update(update_df)
            dest_df.to_csv(self.output_file, index=False)

        logger.info("="*60)
        logger.info("✅ DDD Enrichment Run Complete!")
        logger.info(f"Processed {len(enriched_rows_data)} new items in this run.")
        logger.info(f"Enriched data saved to: {self.output_file}")
        logger.info("="*60)

    def _load_source_data(self) -> pd.DataFrame:
        if not self.input_file.exists():
            logger.error(f"Input file not found: {self.input_file}")
            logger.error("Please run the OMDb enrichment script first.")
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

        if not ddd_data or 'item' not in ddd_data or 'topicItemStats' not in ddd_data:
            result['ddd_id'] = "NOT_FOUND" # Mark as processed but not found
            return result

        result['ddd_id'] = ddd_data['item'].get('id')
        
        # --- Data Transformation ---
        # Pivot the topic stats into individual columns
        triggers = ddd_data.get('topicItemStats', [])
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