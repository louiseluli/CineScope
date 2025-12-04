"""
CineScope Wikidata Enrichment Script (Step 5)

Enriches my movies and TV shows with comprehensive data from Wikidata,
the free and collaborative knowledge base. Provides additional metadata like
awards, box office data, filming locations, and detailed crew information.

This enrichment adds ONLY Wikidata-specific columns:
- wd_qid (Wikidata ID)
- wd_label, wd_description
- wd_imdb_id
- wd_awards (formatted list of awards)
- wd_box_office, wd_budget
- wd_filming_locations
- wd_production_countries
- wd_distributors
- wd_music_composer, wd_cinematographer
- wd_genres_from_wikidata
- wd_first_aired (for TV shows)

Wikidata is queried using SPARQL and matches are made primarily via IMDb ID.

Reads from: 04_cast_enriched_media.csv
Outputs: 05_wikidata_enriched_media.csv (Wikidata columns ONLY, not accumulated)

Usage:
    python scripts/enrich/05_enrich_wikidata.py
    python scripts/enrich/05_enrich_wikidata.py --force
    python scripts/enrich/05_enrich_wikidata.py --limit 50
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
from src.enrichment.wikidata_client import WikidataClient

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


class WikidataEnricher:
    """Orchestrates my Wikidata enrichment process."""
    
    def __init__(self):
        self.client = WikidataClient()
        self.input_file = settings.PROCESSED_DATA_DIR / "04_cast_enriched_media.csv"
        self.output_file = settings.PROCESSED_DATA_DIR / "05_wikidata_enriched_media.csv"
    
    def run(self, force: bool = False, limit: int = None):
        """Execute my full Wikidata enrichment workflow."""
        source_df = self._load_source_data()
        dest_df = self._load_or_initialize_dest_df(force)
        items_to_process = self._get_items_to_process(source_df, dest_df)
        
        if limit:
            items_to_process = items_to_process.head(limit)
        
        if items_to_process.empty:
            logger.info("✅ All my media already enriched with Wikidata.")
            return
        
        logger.info(f"Found {len(items_to_process)} items to enrich with Wikidata.")
        logger.info("ℹ️  Wikidata is a free knowledge base - being respectful with rate limiting.")
        
        enriched_data = []
        with tqdm(total=len(items_to_process), desc="Enriching with Wikidata") as pbar:
            for _, row in items_to_process.iterrows():
                try:
                    enriched_row = self._process_item(row)
                    enriched_data.append(enriched_row)
                    
                    # Save checkpoint every 20 items
                    if len(enriched_data) % 20 == 0:
                        self._save_checkpoint(dest_df, enriched_data)
                        
                except Exception as e:
                    logger.error(f"Error processing {row['const']}: {e}")
                finally:
                    pbar.update(1)
                    # Respectful rate limiting (1 second between queries)
                    time.sleep(1.0)
        
        # Final save
        self._save_checkpoint(dest_df, enriched_data)
        
        logger.info("="*60)
        logger.info("✅ Wikidata Enrichment Complete!")
        logger.info(f"Processed {len(enriched_data)} items in this run.")
        logger.info(f"My enriched Wikidata saved to: {self.output_file}")
        logger.info("="*60)
        logger.info("ℹ️  Wikidata is maintained by volunteers.")
        logger.info("Consider supporting: https://donate.wikimedia.org/")
    
    def _load_source_data(self) -> pd.DataFrame:
        """Load the cast-enriched data."""
        if not self.input_file.exists():
            logger.error(f"Input file not found: {self.input_file}")
            logger.error("Please run 04_enrich_cast.py first.")
            sys.exit(1)
        return pd.read_csv(self.input_file, low_memory=False)
    
    def _load_or_initialize_dest_df(self, force: bool) -> pd.DataFrame:
        """Load existing Wikidata-enriched data or start fresh."""
        if self.output_file.exists() and not force:
            logger.info(f"Resuming from my Wikidata enrichment: {self.output_file}")
            return pd.read_csv(self.output_file, low_memory=False)
        logger.info("Starting new Wikidata enrichment. Initializing with 'const' column only.")
        return pd.DataFrame(columns=['const'])
    
    def _get_items_to_process(self, source_df: pd.DataFrame, dest_df: pd.DataFrame) -> pd.DataFrame:
        """Determine which items need Wikidata enrichment."""
        if dest_df.empty:
            return source_df
        enriched_ids = set(dest_df['const'].astype(str).unique())
        return source_df[~source_df['const'].astype(str).isin(enriched_ids)]
    
    def _process_item(self, item_row: pd.Series) -> Dict:
        """Process Wikidata enrichment for a single movie/show. Returns ONLY Wikidata columns + const."""
        imdb_id = item_row['const']
        
        # Start with just the const ID
        result = {'const': imdb_id}
        
        # Get comprehensive Wikidata info using IMDb ID
        wd_data = self.client.get_movie_details(imdb_id)
        
        if wd_data:
            # Add only Wikidata-specific columns (not accumulated)
            result.update(wd_data)
            logger.debug(f"✓ Wikidata: {wd_data.get('wd_label', 'Unknown')}")
        else:
            logger.debug(f"No Wikidata match for {imdb_id}")
        
        return result
    
    def _save_checkpoint(self, dest_df: pd.DataFrame, new_data: list):
        """Save enrichment checkpoint."""
        if new_data:
            new_df = pd.DataFrame(new_data)
            final_df = pd.concat([dest_df, new_df], ignore_index=True)
            final_df = final_df.drop_duplicates(subset='const', keep='last')
            final_df.to_csv(self.output_file, index=False)


def main():
    parser = argparse.ArgumentParser(description="Enrich media with Wikidata knowledge base.")
    parser.add_argument('--force', action='store_true', help="Re-enrich all media.")
    parser.add_argument('--limit', type=int, help="Limit number of items to process.")
    args = parser.parse_args()
    
    try:
        enricher = WikidataEnricher()
        enricher.run(force=args.force, limit=args.limit)
    except KeyboardInterrupt:
        logger.info("\nInterrupted. Progress saved. Run again to resume.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Critical error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()