"""
CineScope TMDb Enrichment Script (Step 1) - FIXED FOR WATCHED FILMS ONLY

CRITICAL: This script ONLY processes films from Watched-Dec.csv (ground truth)
Never processes more than ~2,289 watched films.

Fetches comprehensive metadata from TMDb for watched movies and TV shows.

COMPREHENSIVE TMDB FIELDS:
- Basic: title, year, runtime, ratings, tagline, overview, poster, backdrop
- Production: budget, revenue, companies, countries, languages, status
- Content: genres, keywords (pipe-separated), collections  
- People: cast (top 15), directors, writers, producers, cinematographers, composers, editors
- Media: trailers, video count
- Social: Facebook, Instagram, Twitter IDs
- Recommendations: similar/recommended movies

Reads from: Watched-Dec.csv (ALWAYS)
Outputs: 01_tmdb_enriched_media.csv

Usage:
    python scripts/enrich/01_enrich_tmdb.py
    python scripts/enrich/01_enrich_tmdb.py --force
    python scripts/enrich/01_enrich_tmdb.py --limit 10
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
    """Comprehensive TMDb enrichment - ONLY for watched films."""

    def __init__(self):
        self.client = TMDbClient()
        # CRITICAL: ALWAYS use Watched-Dec.csv as source (ground truth)
        self.input_file = settings.WATCHED_CSV
        self.output_file = settings.PROCESSED_DATA_DIR / "01_tmdb_enriched_media.csv"
        
        if not self.input_file.exists():
            raise FileNotFoundError(f"Watched films file not found: {self.input_file}")

    def run(self, force: bool = False, limit: Optional[int] = None):
        """Execute comprehensive TMDb enrichment."""
        source_df = self._load_source_data()
        dest_df = self._load_or_initialize_dest_df(force)
        items_to_process = self._get_items_to_process(source_df, dest_df)
        
        if limit:
            items_to_process = items_to_process.head(limit)

        if items_to_process.empty:
            logger.info("✅ All watched movies enriched with TMDb data.")
            logger.info(f"Total: {len(dest_df)} items")
            return

        logger.info(f"📊 Source: {len(source_df)} watched films from {self.input_file.name}")
        logger.info(f"🔄 Enriching {len(items_to_process)} items with comprehensive TMDb data...")
        
        enriched_data = []
        with tqdm(total=len(items_to_process), desc="TMDb Enrichment") as pbar:
            for _, row in items_to_process.iterrows():
                try:
                    enriched_row = self._process_item(row)
                    enriched_data.append(enriched_row)
                    if len(enriched_data) % 10 == 0:
                        self._save_checkpoint(dest_df, enriched_data)
                except Exception as e:
                    logger.error(f"Error {row['const']}: {e}", exc_info=False)
                finally:
                    pbar.update(1)
                    time.sleep(1 / settings.TMDB_RATE_LIMIT)
        
        # Final save
        self._save_checkpoint(dest_df, enriched_data)

        logger.info("=" * 80)
        logger.info("✅ TMDb Comprehensive Enrichment Complete!")
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

    def _load_or_initialize_dest_df(self, force: bool) -> pd.DataFrame:
        """Load existing enrichment or start fresh."""
        if self.output_file.exists() and not force:
            logger.info(f"Resuming from: {self.output_file}")
            return pd.read_csv(self.output_file, low_memory=False)
        logger.info("Starting new TMDb enrichment.")
        return pd.DataFrame(columns=['const'])

    def _get_items_to_process(self, source_df: pd.DataFrame, dest_df: pd.DataFrame) -> pd.DataFrame:
        """Find items needing TMDb enrichment."""
        if dest_df.empty or 'const' not in dest_df.columns:
            return source_df
        enriched_ids = set(dest_df['const'].astype(str).unique())
        return source_df[~source_df['const'].astype(str).isin(enriched_ids)]

    def _process_item(self, item_row: pd.Series) -> Dict:
        """Fetch and process comprehensive TMDb data."""
        imdb_id = item_row['const']
        result = {'const': imdb_id}
        
        # Find movie/TV show on TMDb
        find_result = self.client.find_by_imdb_id(imdb_id)
        if not find_result:
            logger.debug(f"No TMDb entry for {imdb_id}")
            return result
        
        media_type, basic_info = find_result
        tmdb_id = basic_info.get('id')

        if not tmdb_id:
            logger.warning(f"TMDb entry found but no ID for {imdb_id}")
            return result

        result['tmdb_id'] = tmdb_id
        result['tmdb_media_type'] = media_type

        # Get comprehensive details with append_to_response
        if media_type == "movie":
            details = self.client._make_request(
                f"/movie/{tmdb_id}",
                params={'append_to_response': 'credits,keywords,videos,similar,recommendations,external_ids'}
            )
            if details:
                result = self._parse_movie_details(result, details)
        elif media_type == "tv":
            details = self.client._make_request(
                f"/tv/{tmdb_id}",
                params={'append_to_response': 'credits,keywords,videos,similar,recommendations,external_ids'}
            )
            if details:
                result = self._parse_tv_details(result, details)
        
        return result
    
    def _parse_movie_details(self, result: Dict, details: Dict) -> Dict:
        """Parse comprehensive movie details from TMDb API."""
        # Basic metadata
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
        result['tmdb_runtime'] = details.get('runtime')
        result['tmdb_adult'] = details.get('adult')
        result['tmdb_homepage'] = details.get('homepage')
        
        # Production details (pipe-separated)
        result['tmdb_genres'] = '|'.join([g['name'] for g in details.get('genres', [])])
        result['tmdb_production_companies'] = '|'.join([c['name'] for c in details.get('production_companies', [])])
        result['tmdb_production_countries'] = '|'.join([c['name'] for c in details.get('production_countries', [])])
        result['tmdb_spoken_languages'] = '|'.join([l.get('english_name', l.get('name', '')) for l in details.get('spoken_languages', [])])
        
        # Keywords (pipe-separated) - FIXED FORMAT
        keywords = [k['name'] for k in details.get('keywords', {}).get('keywords', [])]
        result['tmdb_keywords'] = '|'.join(keywords) if keywords else ''
        
        # Cast (top 15 actors) - pipe-separated
        cast = details.get('credits', {}).get('cast', [])[:15]
        result['tmdb_actors'] = '|'.join([c['name'] for c in cast]) if cast else ''
        result['tmdb_actor_ids'] = '|'.join([str(c['id']) for c in cast]) if cast else ''
        result['tmdb_characters'] = '|'.join([c.get('character', '') for c in cast[:10] if c.get('character')]) if cast else ''
        
        # Crew - pipe-separated
        crew = details.get('credits', {}).get('crew', [])
        result['tmdb_directors'] = '|'.join([c['name'] for c in crew if c.get('job') == 'Director'])
        result['tmdb_writers'] = '|'.join([c['name'] for c in crew if c.get('department') == 'Writing'][:5])
        result['tmdb_producers'] = '|'.join([c['name'] for c in crew if c.get('job') == 'Producer'][:3])
        result['tmdb_cinematographers'] = '|'.join([c['name'] for c in crew if c.get('job') == 'Director of Photography'][:2])
        result['tmdb_composers'] = '|'.join([c['name'] for c in crew if c.get('department') == 'Sound' and 'Music' in c.get('job', '')][:2])
        result['tmdb_editors'] = '|'.join([c['name'] for c in crew if c.get('job') == 'Editor'][:2])
        
        # Videos (trailers) - pipe-separated YouTube keys
        videos = details.get('videos', {}).get('results', [])
        trailers = [v['key'] for v in videos if v.get('type') == 'Trailer'][:3]
        result['tmdb_trailers'] = '|'.join(trailers) if trailers else ''
        result['tmdb_video_count'] = len(videos)
        
        # External IDs (social media)
        ext_ids = details.get('external_ids', {})
        result['tmdb_facebook_id'] = ext_ids.get('facebook_id')
        result['tmdb_instagram_id'] = ext_ids.get('instagram_id')
        result['tmdb_twitter_id'] = ext_ids.get('twitter_id')
        
        # Similar and recommended movies - pipe-separated
        similar = details.get('similar', {}).get('results', [])[:5]
        recommended = details.get('recommendations', {}).get('results', [])[:5]
        result['tmdb_similar_movies'] = '|'.join([m.get('title', '') for m in similar if m.get('title')])
        result['tmdb_recommended_movies'] = '|'.join([m.get('title', '') for m in recommended if m.get('title')])
        
        # Collections
        collection = details.get('belongs_to_collection')
        result['tmdb_belongs_to_collection'] = collection.get('name') if collection else None
        result['tmdb_collection_id'] = collection.get('id') if collection else None
        
        return result

    def _parse_tv_details(self, result: Dict, details: Dict) -> Dict:
        """Parse comprehensive TV series details from TMDb API."""
        # Basic metadata
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
        result['tmdb_homepage'] = details.get('homepage')

        # Production details (pipe-separated)
        result['tmdb_genres'] = '|'.join([g['name'] for g in details.get('genres', [])])
        result['tmdb_production_companies'] = '|'.join([c['name'] for c in details.get('production_companies', [])])
        result['tmdb_production_countries'] = '|'.join(details.get('origin_country', []))
        result['tmdb_spoken_languages'] = '|'.join([l.get('english_name', l.get('name', '')) for l in details.get('spoken_languages', [])])
        result['tmdb_networks'] = '|'.join([n['name'] for n in details.get('networks', [])])
        
        # Keywords (pipe-separated) - FIXED FORMAT
        keywords = [k['name'] for k in details.get('keywords', {}).get('results', [])]
        result['tmdb_keywords'] = '|'.join(keywords) if keywords else ''
        
        # Cast (top 15) - pipe-separated
        cast = details.get('credits', {}).get('cast', [])[:15]
        result['tmdb_actors'] = '|'.join([c['name'] for c in cast]) if cast else ''
        result['tmdb_actor_ids'] = '|'.join([str(c['id']) for c in cast]) if cast else ''
        result['tmdb_characters'] = '|'.join([c.get('character', '') for c in cast[:10] if c.get('character')]) if cast else ''
        
        # Crew - pipe-separated
        crew = details.get('credits', {}).get('crew', [])
        result['tmdb_directors'] = '|'.join([c['name'] for c in crew if c.get('job') == 'Director'][:5])
        result['tmdb_writers'] = '|'.join([c['name'] for c in crew if c.get('department') == 'Writing'][:5])
        result['tmdb_producers'] = '|'.join([c['name'] for c in crew if c.get('job') in ['Producer', 'Executive Producer']][:5])
        
        # Videos (trailers) - pipe-separated
        videos = details.get('videos', {}).get('results', [])
        trailers = [v['key'] for v in videos if v.get('type') == 'Trailer'][:3]
        result['tmdb_trailers'] = '|'.join(trailers) if trailers else ''
        result['tmdb_video_count'] = len(videos)
        
        # External IDs
        ext_ids = details.get('external_ids', {})
        result['tmdb_facebook_id'] = ext_ids.get('facebook_id')
        result['tmdb_instagram_id'] = ext_ids.get('instagram_id')
        result['tmdb_twitter_id'] = ext_ids.get('twitter_id')
        
        # Similar and recommended - pipe-separated
        similar = details.get('similar', {}).get('results', [])[:5]
        recommended = details.get('recommendations', {}).get('results', [])[:5]
        result['tmdb_similar_shows'] = '|'.join([m.get('name', '') for m in similar if m.get('name')])
        result['tmdb_recommended_shows'] = '|'.join([m.get('name', '') for m in recommended if m.get('name')])
        
        return result

    def _save_checkpoint(self, dest_df, new_data):
        """Save enrichment checkpoint."""
        if new_data:
            temp_df = pd.concat([dest_df, pd.DataFrame(new_data)], ignore_index=True)
            temp_df = temp_df.drop_duplicates(subset=['const'], keep='last')
            temp_df.to_csv(self.output_file, index=False)


def main():
    parser = argparse.ArgumentParser(description="Comprehensive TMDb enrichment - WATCHED FILMS ONLY.")
    parser.add_argument('--force', action='store_true', help="Re-enrich all items.")
    parser.add_argument('--limit', type=int, help="Limit number of items to process.")
    args = parser.parse_args()

    try:
        enricher = TMDbEnricher()
        enricher.run(force=args.force, limit=args.limit)
    except KeyboardInterrupt:
        logger.info("\nInterrupted. Progress saved. Run again to resume.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Critical error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()