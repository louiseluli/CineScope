"""
CineScope Keywords Enrichment Script (Step 8)

MOVIE KEYWORDS & THEMES
=======================
This script enriches movies with keywords from TMDB:

- Keywords per movie
- Theme categorization
- Content tags
- Mood/tone descriptors
- Setting information

Usage:
    python scripts/enrich/08_enrich_keywords.py                  # Enrich all
    python scripts/enrich/08_enrich_keywords.py --limit 100      # Limited batch
    python scripts/enrich/08_enrich_keywords.py --force          # Re-enrich all
"""
import sys
import json
import csv
import time
import argparse
import logging
from pathlib import Path
from typing import Dict, Optional, List, Set
from datetime import datetime
from collections import Counter
from tqdm import tqdm
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.core.config import settings

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


# Keyword categories for grouping
KEYWORD_CATEGORIES = {
    'themes': [
        'love', 'revenge', 'redemption', 'survival', 'betrayal', 'friendship',
        'family', 'war', 'death', 'loss', 'identity', 'power', 'freedom',
        'justice', 'corruption', 'obsession', 'isolation', 'sacrifice',
        'coming of age', 'good vs evil', 'morality', 'ambition', 'greed'
    ],
    'settings': [
        'new york', 'los angeles', 'london', 'paris', 'small town', 'suburb',
        'prison', 'hospital', 'school', 'college', 'desert', 'island',
        'space', 'underwater', 'jungle', 'forest', 'mountain', 'beach',
        'countryside', 'apartment', 'mansion', 'castle', 'ship', 'airplane'
    ],
    'character_types': [
        'detective', 'serial killer', 'superhero', 'villain', 'antihero',
        'orphan', 'outcast', 'rebel', 'soldier', 'spy', 'scientist',
        'journalist', 'lawyer', 'doctor', 'teacher', 'artist', 'musician',
        'criminal', 'hitman', 'gangster', 'cop', 'politician'
    ],
    'narrative': [
        'plot twist', 'flashback', 'unreliable narrator', 'multiple storylines',
        'nonlinear', 'based on true story', 'remake', 'sequel', 'prequel',
        'adaptation', 'origin story', 'time travel', 'dream sequence',
        'prophecy', 'quest', 'heist', 'chase', 'investigation', 'trial'
    ],
    'emotional': [
        'dark', 'disturbing', 'heartwarming', 'inspiring', 'suspenseful',
        'terrifying', 'romantic', 'funny', 'tragic', 'bittersweet',
        'emotional', 'tense', 'violent', 'gory', 'sexy', 'provocative'
    ],
    'content': [
        'nudity', 'sex scene', 'violence', 'gore', 'drug use', 'alcohol',
        'smoking', 'profanity', 'adult content', 'mature themes'
    ]
}


class KeywordsEnricher:
    """
    Enriches movies with keywords from TMDB API.
    """
    
    TMDB_BASE = "https://api.themoviedb.org/3"
    RATE_LIMIT_DELAY = 0.25  # TMDB allows 40 requests/10 seconds
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or settings.TMDB_API_KEY
        if not self.api_key:
            raise ValueError("TMDB API key required. Set TMDB_API_KEY environment variable.")
        
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {self.api_key}'
        })
        self._last_request_time = 0
        
        # Paths - Use watched_movies_master.csv (your ~2,300 watched films)
        # NOT master_cinema_data.csv (which includes unwatched catalog)
        self.master_csv = settings.PROCESSED_DATA_DIR / "watched_movies_master.csv"
        self.keywords_cache_file = settings.PROCESSED_DATA_DIR / "keywords_cache.json"
        self.keywords_output_csv = settings.PROCESSED_DATA_DIR / "06_keywords_enriched_media.csv"
        
        # Load data
        self.movies = self._load_movies()
        self.keywords_cache = self._load_cache()
        
        # Stats
        self.stats = {
            'total': 0,
            'enriched': 0,
            'with_keywords': 0,
            'no_tmdb_id': 0,
            'api_errors': 0
        }
        
        # All keywords for analysis
        self.all_keywords: Counter = Counter()
    
    def _load_movies(self) -> List[Dict]:
        """Load movies from master CSV."""
        if not self.master_csv.exists():
            logger.error(f"Master CSV not found: {self.master_csv}")
            return []
        
        movies = []
        with open(self.master_csv, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                movies.append(row)
        
        logger.info(f"Loaded {len(movies):,} movies from master CSV")
        return movies
    
    def _load_cache(self) -> Dict:
        """Load keywords cache."""
        if self.keywords_cache_file.exists():
            with open(self.keywords_cache_file, 'r') as f:
                return json.load(f)
        return {}
    
    def _save_cache(self):
        """Save keywords cache."""
        with open(self.keywords_cache_file, 'w') as f:
            json.dump(self.keywords_cache, f, indent=2)
    
    def _rate_limit(self):
        """Rate limit requests to TMDB."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.RATE_LIMIT_DELAY:
            time.sleep(self.RATE_LIMIT_DELAY - elapsed)
        self._last_request_time = time.time()
    
    def fetch_keywords(self, tmdb_id: str) -> Optional[Dict]:
        """
        Fetch keywords for a movie from TMDB.
        
        Args:
            tmdb_id: TMDB movie ID
            
        Returns:
            Dictionary with keywords or None
        """
        if not tmdb_id:
            return None
        
        # Check cache
        cache_key = str(tmdb_id)
        if cache_key in self.keywords_cache:
            return self.keywords_cache[cache_key]
        
        self._rate_limit()
        
        try:
            url = f"{self.TMDB_BASE}/movie/{tmdb_id}/keywords"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            keywords = data.get('keywords', [])
            
            # Process keywords
            keyword_names = [kw['name'] for kw in keywords]
            keyword_ids = [kw['id'] for kw in keywords]
            
            # Categorize keywords
            categories = self._categorize_keywords(keyword_names)
            
            result = {
                'keywords': keyword_names,
                'keyword_ids': keyword_ids,
                'keyword_count': len(keyword_names),
                'categories': categories,
                'fetched_date': datetime.now().isoformat()
            }
            
            # Cache result
            self.keywords_cache[cache_key] = result
            return result
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # Movie not found - cache empty result
                self.keywords_cache[cache_key] = {
                    'keywords': [],
                    'keyword_ids': [],
                    'keyword_count': 0,
                    'categories': {},
                    'fetched_date': datetime.now().isoformat(),
                    'not_found': True
                }
                return self.keywords_cache[cache_key]
            logger.debug(f"HTTP error for {tmdb_id}: {e}")
            return None
        except Exception as e:
            logger.debug(f"Error fetching keywords for {tmdb_id}: {e}")
            return None
    
    def _categorize_keywords(self, keywords: List[str]) -> Dict[str, List[str]]:
        """
        Categorize keywords into predefined categories.
        
        Args:
            keywords: List of keyword strings
            
        Returns:
            Dictionary mapping category names to matching keywords
        """
        result = {}
        keywords_lower = [kw.lower() for kw in keywords]
        
        for category, category_keywords in KEYWORD_CATEGORIES.items():
            matching = []
            for kw in keywords:
                kw_lower = kw.lower()
                # Check for exact match or partial match
                for cat_kw in category_keywords:
                    if cat_kw in kw_lower or kw_lower in cat_kw:
                        matching.append(kw)
                        break
            if matching:
                result[category] = matching
        
        return result
    
    def enrich_all(self, limit: int = None, force: bool = False):
        """
        Enrich all movies with keywords.
        
        Args:
            limit: Maximum number of movies to process
            force: Re-enrich even if already in cache
        """
        logger.info("=" * 80)
        logger.info("KEYWORDS ENRICHMENT")
        logger.info("=" * 80)
        
        # Filter movies
        to_enrich = []
        for movie in self.movies:
            tmdb_id = movie.get('tmdb_id') or movie.get('TMDB_ID')
            if not tmdb_id:
                self.stats['no_tmdb_id'] += 1
                continue
            if not force and str(tmdb_id) in self.keywords_cache:
                continue
            to_enrich.append(movie)
        
        if limit:
            to_enrich = to_enrich[:limit]
        
        logger.info(f"Movies to enrich: {len(to_enrich):,}")
        logger.info(f"Movies without TMDB ID: {self.stats['no_tmdb_id']:,}")
        
        if not to_enrich:
            logger.info("No movies need keyword enrichment!")
            self._export_results()
            return
        
        self.stats['total'] = len(to_enrich)
        
        for movie in tqdm(to_enrich, desc="Fetching keywords"):
            tmdb_id = movie.get('tmdb_id') or movie.get('TMDB_ID')
            
            try:
                keywords_data = self.fetch_keywords(tmdb_id)
                
                if keywords_data and keywords_data.get('keywords'):
                    self.stats['with_keywords'] += 1
                    for kw in keywords_data['keywords']:
                        self.all_keywords[kw] += 1
                
                self.stats['enriched'] += 1
                
                # Checkpoint every 100
                if self.stats['enriched'] % 100 == 0:
                    self._save_cache()
                    logger.info(f"Progress: {self.stats['enriched']}/{self.stats['total']}")
                    
            except Exception as e:
                logger.debug(f"Error enriching keywords for {tmdb_id}: {e}")
                self.stats['api_errors'] += 1
        
        self._save_cache()
        self._export_results()
        self._print_summary()
    
    def _export_results(self):
        """Export enriched data to CSV."""
        logger.info("Exporting enriched data...")
        
        # Add keywords to movies and export
        enriched_movies = []
        for movie in self.movies:
            tmdb_id = str(movie.get('tmdb_id') or movie.get('TMDB_ID') or '')
            
            if tmdb_id and tmdb_id in self.keywords_cache:
                kw_data = self.keywords_cache[tmdb_id]
                movie['keywords'] = '|'.join(kw_data.get('keywords', []))
                movie['keyword_count'] = kw_data.get('keyword_count', 0)
                movie['themes'] = '|'.join(kw_data.get('categories', {}).get('themes', []))
                movie['settings'] = '|'.join(kw_data.get('categories', {}).get('settings', []))
                movie['character_types'] = '|'.join(kw_data.get('categories', {}).get('character_types', []))
                movie['emotional_keywords'] = '|'.join(kw_data.get('categories', {}).get('emotional', []))
                movie['content_keywords'] = '|'.join(kw_data.get('categories', {}).get('content', []))
            else:
                movie['keywords'] = ''
                movie['keyword_count'] = 0
                movie['themes'] = ''
                movie['settings'] = ''
                movie['character_types'] = ''
                movie['emotional_keywords'] = ''
                movie['content_keywords'] = ''
            
            enriched_movies.append(movie)
        
        # Write CSV
        if enriched_movies:
            fieldnames = list(enriched_movies[0].keys())
            with open(self.keywords_output_csv, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(enriched_movies)
            
            logger.info(f"Exported to {self.keywords_output_csv}")
        
        # Export top keywords
        top_keywords_file = settings.PROCESSED_DATA_DIR / "top_keywords.json"
        with open(top_keywords_file, 'w') as f:
            json.dump({
                'top_100': self.all_keywords.most_common(100),
                'total_unique': len(self.all_keywords),
                'total_occurrences': sum(self.all_keywords.values())
            }, f, indent=2)
        logger.info(f"Exported top keywords to {top_keywords_file}")
    
    def _print_summary(self):
        """Print enrichment summary."""
        logger.info("\n" + "=" * 80)
        logger.info("KEYWORDS ENRICHMENT SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total processed:       {self.stats['total']:,}")
        logger.info(f"Successfully enriched: {self.stats['enriched']:,}")
        logger.info(f"With keywords:         {self.stats['with_keywords']:,}")
        logger.info(f"API errors:            {self.stats['api_errors']:,}")
        logger.info(f"No TMDB ID:            {self.stats['no_tmdb_id']:,}")
        logger.info(f"Unique keywords:       {len(self.all_keywords):,}")
        
        # Top 20 keywords
        logger.info("\nTop 20 Keywords:")
        for kw, count in self.all_keywords.most_common(20):
            logger.info(f"  {kw}: {count}")
    
    def analyze_keywords(self):
        """Analyze keyword distribution without fetching new data."""
        logger.info("Analyzing existing keyword data...")
        
        total = 0
        with_keywords = 0
        
        for tmdb_id, data in self.keywords_cache.items():
            total += 1
            keywords = data.get('keywords', [])
            if keywords:
                with_keywords += 1
                for kw in keywords:
                    self.all_keywords[kw] += 1
        
        logger.info(f"Total cached: {total:,}")
        logger.info(f"With keywords: {with_keywords:,}")
        logger.info(f"Unique keywords: {len(self.all_keywords):,}")
        
        # Category breakdown
        logger.info("\nKeyword Category Breakdown:")
        for category, kw_list in KEYWORD_CATEGORIES.items():
            matches = sum(self.all_keywords[kw] for kw in kw_list if kw in self.all_keywords)
            logger.info(f"  {category}: {matches:,} occurrences")


def main():
    parser = argparse.ArgumentParser(description='Movie keywords enrichment')
    parser.add_argument('--limit', type=int, help='Max movies to process')
    parser.add_argument('--force', action='store_true', help='Re-enrich all')
    parser.add_argument('--analyze', action='store_true', help='Analyze existing data only')
    args = parser.parse_args()
    
    enricher = KeywordsEnricher()
    
    if args.analyze:
        enricher.analyze_keywords()
    else:
        enricher.enrich_all(limit=args.limit, force=args.force)


if __name__ == '__main__':
    main()
