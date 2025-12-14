"""
CineScope Keyword Enrichment Client

Fetches keywords and plot themes from TMDB API for thematic analysis.
Keywords help identify patterns in viewing preferences beyond genres.

Usage:
    from src.enrichment.keywords_client import KeywordsClient
    
    client = KeywordsClient()
    keywords = client.get_movie_keywords("tt0111161")  # Shawshank
"""
import requests
import time
import logging
from typing import Optional, Dict, List, Any
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.core.config import settings

logger = logging.getLogger(__name__)


# Keyword categories for classification
KEYWORD_CATEGORIES = {
    'themes': {
        'redemption', 'revenge', 'betrayal', 'sacrifice', 'corruption',
        'survival', 'identity', 'justice', 'freedom', 'destiny',
        'love', 'friendship', 'family', 'loyalty', 'honor',
        'greed', 'power', 'ambition', 'obsession', 'madness',
        'hope', 'despair', 'loss', 'grief', 'healing',
        'transformation', 'coming of age', 'self-discovery', 'redemption arc'
    },
    'settings': {
        'prison', 'hospital', 'school', 'military', 'courtroom',
        'space', 'underwater', 'desert', 'jungle', 'island',
        'new york city', 'los angeles', 'london', 'paris', 'tokyo',
        'small town', 'suburbia', 'countryside', 'mountains', 'beach',
        'dystopia', 'utopia', 'post-apocalyptic', 'medieval', 'victorian',
        'future', 'past', 'alternate reality', 'virtual reality'
    },
    'characters': {
        'anti-hero', 'villain', 'hero', 'femme fatale', 'mentor',
        'underdog', 'outcast', 'rebel', 'detective', 'spy',
        'serial killer', 'psychopath', 'con artist', 'thief', 'assassin',
        'soldier', 'scientist', 'artist', 'teacher', 'doctor',
        'child', 'teenager', 'elderly', 'ghost', 'monster'
    },
    'narrative': {
        'twist ending', 'non-linear', 'flashback', 'dream sequence',
        'unreliable narrator', 'multiple perspectives', 'found footage',
        'mockumentary', 'anthology', 'frame story', 'voiceover',
        'time travel', 'parallel universe', 'simulation', 'loop'
    },
    'emotional': {
        'dark', 'uplifting', 'heartwarming', 'heartbreaking', 'disturbing',
        'suspenseful', 'thrilling', 'terrifying', 'hilarious', 'touching',
        'melancholic', 'nostalgic', 'surreal', 'intense', 'atmospheric'
    },
    'content': {
        'based on true story', 'based on novel', 'remake', 'sequel', 'prequel',
        'biopic', 'documentary', 'musical', 'animated', 'anthology',
        'independent film', 'blockbuster', 'cult film', 'arthouse'
    }
}

# Flatten for quick lookup
ALL_KNOWN_KEYWORDS = set()
for category, keywords in KEYWORD_CATEGORIES.items():
    ALL_KNOWN_KEYWORDS.update(keywords)


class KeywordsClient:
    """Client for fetching and categorizing movie keywords."""
    
    BASE_URL = "https://api.themoviedb.org/3"
    
    def __init__(self, api_key: str = None):
        """Initialize the keywords client."""
        self.api_key = api_key or settings.TMDB_API_KEY
        self.session = requests.Session()
        self.session.headers.update({
            "accept": "application/json",
            "Authorization": f"Bearer {settings.TMDB_READ_TOKEN}"
        })
        self._last_request_time = 0
        self._rate_limit = 1 / settings.TMDB_RATE_LIMIT  # seconds between requests
    
    def _rate_limit_wait(self):
        """Ensure we don't exceed rate limits."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._rate_limit:
            time.sleep(self._rate_limit - elapsed)
        self._last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Make a rate-limited request to TMDB API."""
        self._rate_limit_wait()
        
        url = f"{self.BASE_URL}{endpoint}"
        try:
            response = self.session.get(url, params=params or {})
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                logger.warning("Rate limit exceeded, waiting 10s...")
                time.sleep(10)
                return self._make_request(endpoint, params)
            logger.error(f"HTTP error for {url}: {e}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
        return None
    
    def get_movie_keywords(self, tmdb_id: int) -> Optional[Dict]:
        """
        Get keywords for a movie from TMDB.
        
        Args:
            tmdb_id: TMDB movie ID
            
        Returns:
            Dictionary with keywords list and categorized keywords
        """
        data = self._make_request(f"/movie/{tmdb_id}/keywords")
        
        if not data or 'keywords' not in data:
            return None
        
        raw_keywords = data['keywords']
        
        # Extract and normalize keywords
        keywords = []
        for kw in raw_keywords:
            name = kw.get('name', '').lower().strip()
            if name:
                keywords.append({
                    'id': kw.get('id'),
                    'name': name,
                    'original_name': kw.get('name'),
                    'category': self._categorize_keyword(name)
                })
        
        return {
            'tmdb_id': tmdb_id,
            'keyword_count': len(keywords),
            'keywords': keywords,
            'keyword_names': [k['original_name'] for k in keywords],
            'categorized': self._group_by_category(keywords)
        }
    
    def _categorize_keyword(self, keyword: str) -> str:
        """Categorize a keyword into our defined categories."""
        keyword_lower = keyword.lower()
        
        for category, keywords_set in KEYWORD_CATEGORIES.items():
            if keyword_lower in keywords_set:
                return category
            # Partial match
            for known in keywords_set:
                if known in keyword_lower or keyword_lower in known:
                    return category
        
        return 'other'
    
    def _group_by_category(self, keywords: List[Dict]) -> Dict[str, List[str]]:
        """Group keywords by their categories."""
        grouped = {cat: [] for cat in KEYWORD_CATEGORIES}
        grouped['other'] = []
        
        for kw in keywords:
            category = kw['category']
            grouped[category].append(kw['original_name'])
        
        # Remove empty categories
        return {k: v for k, v in grouped.items() if v}
    
    def find_tmdb_id_by_imdb(self, imdb_id: str) -> Optional[int]:
        """
        Find TMDB ID from IMDB ID.
        
        Args:
            imdb_id: IMDB ID (e.g., 'tt0111161')
            
        Returns:
            TMDB ID or None
        """
        # Ensure tt prefix
        if not imdb_id.startswith('tt'):
            imdb_id = f'tt{imdb_id}'
        
        data = self._make_request(f"/find/{imdb_id}", {'external_source': 'imdb_id'})
        
        if data and data.get('movie_results'):
            return data['movie_results'][0].get('id')
        if data and data.get('tv_results'):
            return data['tv_results'][0].get('id')
        
        return None
    
    def get_keywords_by_imdb(self, imdb_id: str) -> Optional[Dict]:
        """
        Get keywords using IMDB ID (looks up TMDB ID first).
        
        Args:
            imdb_id: IMDB ID
            
        Returns:
            Keywords dictionary or None
        """
        tmdb_id = self.find_tmdb_id_by_imdb(imdb_id)
        if not tmdb_id:
            logger.debug(f"Could not find TMDB ID for {imdb_id}")
            return None
        
        return self.get_movie_keywords(tmdb_id)
    
    def analyze_keywords_batch(self, keywords_data: List[Dict]) -> Dict:
        """
        Analyze patterns across a batch of movie keywords.
        
        Args:
            keywords_data: List of keyword results from get_movie_keywords
            
        Returns:
            Analysis summary
        """
        from collections import Counter
        
        all_keywords = Counter()
        category_counts = Counter()
        movies_per_keyword = {}
        
        for movie in keywords_data:
            if not movie:
                continue
                
            for kw in movie.get('keywords', []):
                name = kw['original_name']
                all_keywords[name] += 1
                category_counts[kw['category']] += 1
                
                if name not in movies_per_keyword:
                    movies_per_keyword[name] = []
                movies_per_keyword[name].append(movie['tmdb_id'])
        
        return {
            'total_unique_keywords': len(all_keywords),
            'top_keywords': dict(all_keywords.most_common(50)),
            'category_distribution': dict(category_counts),
            'keywords_by_frequency': {
                'very_common': [k for k, v in all_keywords.items() if v >= 10],
                'common': [k for k, v in all_keywords.items() if 5 <= v < 10],
                'rare': [k for k, v in all_keywords.items() if v < 5],
            },
            'movies_per_keyword': {k: len(v) for k, v in movies_per_keyword.items()}
        }


class ExtendedWikidataPersonClient:
    """
    Extended Wikidata client for fetching additional person data:
    - Cause of death
    - Height
    - Spouses
    - Children
    - Education
    - Burial location
    """
    
    SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
    USER_AGENT = "CineScope/1.0 (Movie Analytics; Educational Project)"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.USER_AGENT,
            'Accept': 'application/sparql-results+json'
        })
        self._last_query_time = 0
    
    def _rate_limit(self):
        """Rate limit: 1 request per second for Wikidata."""
        elapsed = time.time() - self._last_query_time
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
        self._last_query_time = time.time()
    
    def _query(self, sparql: str) -> Optional[List[Dict]]:
        """Execute SPARQL query."""
        self._rate_limit()
        
        try:
            response = self.session.post(
                self.SPARQL_ENDPOINT,
                data={'query': sparql},
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            return data.get('results', {}).get('bindings', [])
        except Exception as e:
            logger.error(f"Wikidata query failed: {e}")
            return None
    
    def get_person_extended_data(self, imdb_id: str) -> Optional[Dict]:
        """
        Fetch extended biographical data from Wikidata.
        
        Args:
            imdb_id: IMDB person ID (e.g., 'nm0000138')
            
        Returns:
            Dictionary with extended data
        """
        if not imdb_id:
            return None
        
        # Ensure nm prefix
        if not imdb_id.startswith('nm'):
            imdb_id = f'nm{imdb_id}'
        
        sparql = f"""
        SELECT DISTINCT
            ?person ?personLabel
            ?birthDate ?deathDate
            ?causeOfDeath ?causeOfDeathLabel
            ?height
            ?burialPlace ?burialPlaceLabel
            ?educatedAt ?educatedAtLabel
            (GROUP_CONCAT(DISTINCT ?spouseLabel; SEPARATOR="|") AS ?spouses)
            ?numberOfChildren
        WHERE {{
            ?person wdt:P345 "{imdb_id}" .
            
            OPTIONAL {{ ?person wdt:P569 ?birthDate . }}
            OPTIONAL {{ ?person wdt:P570 ?deathDate . }}
            OPTIONAL {{ 
                ?person wdt:P509 ?causeOfDeath . 
            }}
            OPTIONAL {{ ?person wdt:P2048 ?height . }}
            OPTIONAL {{ 
                ?person wdt:P119 ?burialPlace . 
            }}
            OPTIONAL {{ 
                ?person wdt:P69 ?educatedAt . 
            }}
            OPTIONAL {{ 
                ?person wdt:P26 ?spouse .
                ?spouse rdfs:label ?spouseLabel .
                FILTER(LANG(?spouseLabel) = "en")
            }}
            OPTIONAL {{ ?person wdt:P1971 ?numberOfChildren . }}
            
            SERVICE wikibase:label {{ 
                bd:serviceParam wikibase:language "en" .
            }}
        }}
        GROUP BY ?person ?personLabel ?birthDate ?deathDate 
                 ?causeOfDeath ?causeOfDeathLabel ?height 
                 ?burialPlace ?burialPlaceLabel
                 ?educatedAt ?educatedAtLabel ?numberOfChildren
        LIMIT 1
        """
        
        results = self._query(sparql)
        
        if not results:
            return None
        
        r = results[0]
        
        def get_val(key: str) -> Optional[str]:
            return r.get(key, {}).get('value')
        
        # Parse height (comes in meters)
        height_val = get_val('height')
        height_cm = None
        height_imperial = None
        if height_val:
            try:
                height_m = float(height_val)
                height_cm = int(height_m * 100)
                feet = int(height_cm / 30.48)
                inches = int((height_cm / 2.54) % 12)
                height_imperial = f"{feet}'{inches}\""
            except (ValueError, TypeError):
                pass
        
        return {
            'wd_birth_date_full': get_val('birthDate'),
            'wd_death_date_full': get_val('deathDate'),
            'wd_cause_of_death': get_val('causeOfDeathLabel'),
            'wd_height_cm': height_cm,
            'wd_height_imperial': height_imperial,
            'wd_burial_place': get_val('burialPlaceLabel'),
            'wd_education': get_val('educatedAtLabel'),
            'wd_spouses': get_val('spouses'),
            'wd_number_of_children': get_val('numberOfChildren'),
        }


# Test
if __name__ == '__main__':
    # Test keywords
    print("Testing Keywords Client...")
    client = KeywordsClient()
    
    # Find TMDB ID for Shawshank
    tmdb_id = client.find_tmdb_id_by_imdb('tt0111161')
    print(f"Shawshank Redemption TMDB ID: {tmdb_id}")
    
    if tmdb_id:
        keywords = client.get_movie_keywords(tmdb_id)
        if keywords:
            print(f"Found {keywords['keyword_count']} keywords:")
            for cat, kws in keywords['categorized'].items():
                print(f"  {cat}: {', '.join(kws[:5])}")
    
    print("\n" + "="*50)
    print("Testing Extended Wikidata Client...")
    
    wd_client = ExtendedWikidataPersonClient()
    
    # Test with a deceased actor
    data = wd_client.get_person_extended_data('nm0000008')  # Marlon Brando
    if data:
        print("Marlon Brando extended data:")
        for key, val in data.items():
            if val:
                print(f"  {key}: {val}")
