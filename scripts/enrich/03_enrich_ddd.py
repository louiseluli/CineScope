"""
CineScope Does the Dog Die? (DDD) Enrichment Script (Step 3) - FIXED FOR WATCHED FILMS ONLY

CRITICAL: This script ONLY processes films from Watched-Dec.csv (ground truth)
Never processes more than ~2,289 watched films.
INDEPENDENT: Can run before or after cast enrichment.

Fetches ALL content warnings from Does the Dog Die? API for watched movies.

COMPREHENSIVE DDD EXTRACTION:
- ddd_id: Internal DDD ID
- ~70 individual topic columns (e.g., ddd_a_dog_dies, ddd_violence, ddd_gore)
- Each topic has value: 'Yes', 'No', 'Controversial', or 'No Votes'

Topics include: animal deaths, violence, gore, profanity, nudity, sexual content,
substance abuse, self-harm, discrimination, mental health, jump scares, and many more.

Reads from: Watched-Dec.csv (ALWAYS)
Outputs: 03_ddd_enriched_media.csv

Usage:
    python scripts/enrich/03_enrich_ddd.py
    python scripts/enrich/03_enrich_ddd.py --force
    python scripts/enrich/03_enrich_ddd.py --limit 10
"""
import sys
import pandas as pd
from pathlib import Path
import logging
from tqdm import tqdm
import argparse
import time
from typing import Dict, Optional

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
    """Comprehensive Does the Dog Die? enrichment - ONLY for watched films."""

    def __init__(self):
        self.client = DDDClient()
        # CRITICAL: ALWAYS use Watched-Dec.csv as source (ground truth)
        self.input_file = settings.WATCHED_CSV
        self.output_file = settings.PROCESSED_DATA_DIR / "03_ddd_enriched_media.csv"
        
        if not self.input_file.exists():
            raise FileNotFoundError(f"Watched films file not found: {self.input_file}")

    def run(self, force: bool = False, limit: Optional[int] = None):
        """Execute comprehensive DDD enrichment."""
        source_df = self._load_source_data()
        dest_df = self._load_or_initialize_dest_df(force)
        items_to_process = self._get_items_to_process(source_df, dest_df)
        
        if limit:
            items_to_process = items_to_process.head(limit)

        if items_to_process.empty:
            logger.info("✅ All watched movies enriched with DDD warnings.")
            logger.info(f"Total: {len(dest_df)} items")
            return

        logger.info(f"📊 Source: {len(source_df)} watched films from {self.input_file.name}")
        logger.info(f"🔄 Enriching {len(items_to_process)} items with comprehensive DDD content warnings...")
        logger.info("ℹ️  Extracting ALL individual warning topics (~70 columns)")
        
        enriched_data = []
        with tqdm(total=len(items_to_process), desc="DDD Enrichment") as pbar:
            for _, row in items_to_process.iterrows():
                try:
                    enriched_row = self._process_item(row)
                    enriched_data.append(enriched_row)
                    
                    # Checkpoint every 10 items
                    if len(enriched_data) % 10 == 0:
                        self._save_checkpoint(dest_df, enriched_data)
                except Exception as e:
                    logger.error(f"Error {row['const']}: {e}", exc_info=False)
                finally:
                    pbar.update(1)
                    time.sleep(0.5)  # Be respectful with DDD API
        
        # Final save
        self._save_checkpoint(dest_df, enriched_data)

        logger.info("=" * 80)
        logger.info("✅ DDD Comprehensive Enrichment Complete!")
        logger.info(f"Processed: {len(enriched_data)} items")
        logger.info(f"Total: {len(dest_df) + len(enriched_data)} watched films")
        logger.info(f"Saved to: {self.output_file}")
        logger.info("=" * 80)

    def _load_source_data(self) -> pd.DataFrame:
        """Load WATCHED films from Watched-Dec.csv."""
        df = pd.read_csv(self.input_file, low_memory=False)
        
        # Standardize column names
        if 'Const' in df.columns:
            df.rename(columns={'Const': 'const'}, inplace=True)
        
        logger.info(f"Loaded {len(df)} watched films from {self.input_file.name}")
        return df

    def _load_or_initialize_dest_df(self, force: bool = False) -> pd.DataFrame:
        """Load existing DDD data or start fresh."""
        if self.output_file.exists() and not force:
            df = pd.read_csv(self.output_file, low_memory=False)
            logger.info(f"Resuming from: {self.output_file} ({len(df)} items)")
            return df
        logger.info("Starting new DDD enrichment.")
        return pd.DataFrame(columns=['const'])

    def _get_items_to_process(self, source_df: pd.DataFrame, dest_df: pd.DataFrame) -> pd.DataFrame:
        """Find items needing DDD enrichment."""
        if dest_df.empty or 'const' not in dest_df.columns:
            return source_df
        
        # Items already processed have ddd_id
        processed_consts = set(dest_df[dest_df['ddd_id'].notna()]['const'].unique()) if 'ddd_id' in dest_df.columns else set()
        items_to_process = source_df[~source_df['const'].isin(processed_consts)].copy()
        return items_to_process

    def _process_item(self, item_row: pd.Series) -> Dict:
        """Fetch and parse comprehensive DDD data - ALL warning topics."""
        imdb_id = item_row['const']
        ddd_data = self.client.get_ddd_info_by_imdb_id(imdb_id)
        
        result = {'const': imdb_id}
        
        if not ddd_data or 'item' not in ddd_data:
            logger.debug(f"No DDD data for {imdb_id}")
            result['ddd_id'] = 'NOT_FOUND'
            return result

        # Extract DDD ID
        result['ddd_id'] = ddd_data['item'].get('id')
        
        # Extract ALL topic warnings (~70 individual columns)
        triggers = ddd_data.get('topicItemStats', [])
        
        if triggers and isinstance(triggers, list):
            for trigger in triggers:
                topic = trigger.get('topic')
                if not topic:
                    continue

                # Create clean column name: ddd_a_dog_dies, ddd_violence, etc.
                topic_name = topic.get('name', 'unknown')
                col_name = f"ddd_{topic_name.lower().replace(' ', '_').replace('?', '').replace('/', '_').replace('-', '_')}"
                
                # Determine consensus based on votes
                yes_sum = trigger.get('yesSum', 0)
                no_sum = trigger.get('noSum', 0)
                
                if yes_sum > no_sum:
                    result[col_name] = 'Yes'
                elif no_sum > yes_sum:
                    result[col_name] = 'No'
                elif yes_sum > 0 and yes_sum == no_sum:
                    result[col_name] = 'Controversial'
                else:  # Both are 0
                    result[col_name] = 'No Votes'
        
        return result

    def _save_checkpoint(self, dest_df: pd.DataFrame, new_data: list):
        """Save enrichment checkpoint."""
        if not new_data:
            return
        
        new_data_df = pd.DataFrame(new_data)
        
        # Combine with existing data
        final_df = pd.concat([dest_df, new_data_df], ignore_index=True)
        
        # Remove duplicates, keeping most recent
        final_df = final_df.drop_duplicates(subset=['const'], keep='last')
        
        # Save
        final_df.to_csv(self.output_file, index=False)
        logger.debug(f"Checkpoint saved: {len(final_df)} items")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Comprehensive DDD enrichment - WATCHED FILMS ONLY."
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help="Re-enrich all items with fresh DDD data."
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help="Limit number of items to process."
    )
    args = parser.parse_args()

    try:
        enricher = DDDEnricher()
        enricher.run(force=args.force, limit=args.limit)
    except KeyboardInterrupt:
        logger.info("\nInterrupted. Progress saved. Run again to resume.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Critical error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()