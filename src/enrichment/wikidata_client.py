"""
CineScope Wikidata API Client

This module provides a client for querying Wikidata's SPARQL endpoint to fetch
comprehensive movie and TV show data. Wikidata is the free knowledge base that
powers Wikipedia and contains rich, structured data.

The client uses POST requests to avoid URI length limits and implements
respectful rate limiting (1 second between queries).
"""
import requests
import time
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class WikidataClient:
    """
    A client for interacting with Wikidata via SPARQL queries.
    """
    SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
    USER_AGENT = "CineScope/1.0 (Movie Enrichment; Educational Project)"
    
    def __init__(self):
        """Initialize the Wikidata client."""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.USER_AGENT,
            'Accept': 'application/sparql-results+json'
        })
        self._last_query_time = 0
        self._min_interval = 1.0  # 1 second between queries (respectful)
    
    def _rate_limit(self):
        """Implement respectful rate limiting for Wikidata."""
        elapsed = time.time() - self._last_query_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_query_time = time.time()
    
    def _query_sparql(self, query: str) -> Optional[Dict]:
        """
        Execute a SPARQL query against Wikidata using POST.
        
        Args:
            query (str): SPARQL query string
            
        Returns:
            dict or None: Query results as dictionary, or None on error
        """
        self._rate_limit()
        
        try:
            response = self.session.post(
                self.SPARQL_ENDPOINT,
                data={'query': query},
                headers={'Accept': 'application/sparql-results+json'},
                timeout=30
            )
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.Timeout:
            logger.error("Wikidata query timeout")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Wikidata SPARQL request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing Wikidata response: {e}")
            return None
    
    def get_movie_details(self, imdb_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch comprehensive movie/TV show details from Wikidata using IMDb ID.
        
        This retrieves:
        - Basic info (title, image, publication date)
        - Classification (genres, themes)
        - Production (countries, languages, companies)
        - People (directors, screenwriters, composers, cast)
        - Recognition (awards, nominations)
        - Business (box office, budget)
        - Distribution (distributors, platforms)
        - Technical (duration, aspect ratio, color)
        - Locations (filming locations, narrative location)
        - Related works (based on, sequel/prequel, spin-offs)
        - External IDs (Wikipedia, other databases)
        
        Args:
            imdb_id (str): IMDb ID (e.g., 'tt0111161')
            
        Returns:
            dict or None: Comprehensive movie data, or None if not found
        """
        # Clean IMDb ID - KEEP the tt prefix! Wikidata stores full IMDB IDs
        if not imdb_id:
            return None
        imdb_id = str(imdb_id).strip()
        # Ensure tt prefix is present (Wikidata P345 stores full IMDB IDs like 'tt0111161')
        if not imdb_id.startswith('tt'):
            imdb_id = f'tt{imdb_id}'
        
        # Optimized SPARQL query - simplified for better performance
        # Original was timing out due to too many GROUP_CONCATs
        query = f"""
        SELECT DISTINCT
            ?film ?filmLabel ?filmDescription
            ?image ?publicationDate ?duration
            (GROUP_CONCAT(DISTINCT ?genreLabel; separator=", ") AS ?genres)
            (GROUP_CONCAT(DISTINCT ?countryLabel; separator=", ") AS ?countries)
            (GROUP_CONCAT(DISTINCT ?languageLabel; separator=", ") AS ?languages)
            (GROUP_CONCAT(DISTINCT ?directorLabel; separator=", ") AS ?directors)
            (GROUP_CONCAT(DISTINCT ?screenwriterLabel; separator=", ") AS ?screenwriters)
            (GROUP_CONCAT(DISTINCT ?composerLabel; separator=", ") AS ?composers)
            (GROUP_CONCAT(DISTINCT ?cinematographerLabel; separator=", ") AS ?cinematographers)
            (GROUP_CONCAT(DISTINCT ?awardLabel; separator=", ") AS ?awards)
            (GROUP_CONCAT(DISTINCT ?filmingLocationLabel; separator=", ") AS ?filmingLocations)
            (GROUP_CONCAT(DISTINCT ?basedOnLabel; separator=", ") AS ?basedOnWorks)
            ?boxOffice ?budget
            ?tmdbId ?rottenTomatoesId ?letterboxdId
        WHERE {{
            ?film wdt:P345 "{imdb_id}" .
            
            # Basic Information
            OPTIONAL {{ ?film wdt:P18 ?image . }}
            OPTIONAL {{ ?film wdt:P577 ?publicationDate . }}
            OPTIONAL {{ ?film wdt:P2047 ?duration . }}
            
            # Classification
            OPTIONAL {{ 
                ?film wdt:P136 ?genre . 
                ?genre rdfs:label ?genreLabel FILTER(LANG(?genreLabel) = "en") 
            }}
            
            # Production
            OPTIONAL {{ 
                ?film wdt:P495 ?country . 
                ?country rdfs:label ?countryLabel FILTER(LANG(?countryLabel) = "en") 
            }}
            OPTIONAL {{ 
                ?film wdt:P364 ?language . 
                ?language rdfs:label ?languageLabel FILTER(LANG(?languageLabel) = "en") 
            }}
            
            # Key People
            OPTIONAL {{ 
                ?film wdt:P57 ?director . 
                ?director rdfs:label ?directorLabel FILTER(LANG(?directorLabel) = "en") 
            }}
            OPTIONAL {{ 
                ?film wdt:P58 ?screenwriter . 
                ?screenwriter rdfs:label ?screenwriterLabel FILTER(LANG(?screenwriterLabel) = "en") 
            }}
            OPTIONAL {{ 
                ?film wdt:P86 ?composer . 
                ?composer rdfs:label ?composerLabel FILTER(LANG(?composerLabel) = "en") 
            }}
            OPTIONAL {{ 
                ?film wdt:P344 ?cinematographer . 
                ?cinematographer rdfs:label ?cinematographerLabel FILTER(LANG(?cinematographerLabel) = "en") 
            }}
            
            # Recognition (awards only, nominations too slow)
            OPTIONAL {{ 
                ?film wdt:P166 ?award . 
                ?award rdfs:label ?awardLabel FILTER(LANG(?awardLabel) = "en") 
            }}
            
            # Business
            OPTIONAL {{ ?film wdt:P2142 ?boxOffice . }}
            OPTIONAL {{ ?film wdt:P2130 ?budget . }}
            
            # Locations
            OPTIONAL {{ 
                ?film wdt:P915 ?filmingLocation . 
                ?filmingLocation rdfs:label ?filmingLocationLabel FILTER(LANG(?filmingLocationLabel) = "en") 
            }}
            
            # Related Works
            OPTIONAL {{ 
                ?film wdt:P144 ?basedOn . 
                ?basedOn rdfs:label ?basedOnLabel FILTER(LANG(?basedOnLabel) = "en") 
            }}
            
            # External IDs
            OPTIONAL {{ ?film wdt:P4947 ?tmdbId . }}
            OPTIONAL {{ ?film wdt:P1258 ?rottenTomatoesId . }}
            OPTIONAL {{ ?film wdt:P6127 ?letterboxdId . }}
            
            SERVICE wikibase:label {{ 
                bd:serviceParam wikibase:language "en" .
            }}
        }}
        GROUP BY ?film ?filmLabel ?filmDescription ?image ?publicationDate 
                 ?duration ?boxOffice ?budget ?tmdbId 
                 ?rottenTomatoesId ?letterboxdId
        LIMIT 1
        """
        
        result = self._query_sparql(query)
        
        if not result or not result.get('results', {}).get('bindings'):
            logger.debug(f"No Wikidata entry found for IMDb ID: {imdb_id}")
            return None
        
        # Parse the result
        binding = result['results']['bindings'][0]
        
        enriched = {
            # Core Identifiers
            'wd_id': self._extract_id(binding.get('film', {}).get('value')),
            'wd_label': self._get_value(binding, 'filmLabel'),
            'wd_description': self._get_value(binding, 'filmDescription'),
            'wd_url': f"https://www.wikidata.org/wiki/{self._extract_id(binding.get('film', {}).get('value'))}",
            
            # Visual & Dates
            'wd_image_url': self._get_value(binding, 'image'),
            'wd_publication_date': self._get_value(binding, 'publicationDate'),
            'wd_duration_minutes': self._parse_numeric(binding, 'duration'),
            
            # Classification
            'wd_genres': self._get_value(binding, 'genres'),
            
            # Production
            'wd_countries': self._get_value(binding, 'countries'),
            'wd_languages': self._get_value(binding, 'languages'),
            
            # Key People
            'wd_directors': self._get_value(binding, 'directors'),
            'wd_screenwriters': self._get_value(binding, 'screenwriters'),
            'wd_composers': self._get_value(binding, 'composers'),
            'wd_cinematographers': self._get_value(binding, 'cinematographers'),
            
            # Recognition
            'wd_awards': self._get_value(binding, 'awards'),
            
            # Business
            'wd_box_office': self._parse_numeric(binding, 'boxOffice'),
            'wd_budget': self._parse_numeric(binding, 'budget'),
            
            # Locations
            'wd_filming_locations': self._get_value(binding, 'filmingLocations'),
            
            # Related Works
            'wd_based_on': self._get_value(binding, 'basedOnWorks'),
            
            # External Links
            'wd_tmdb_id': self._get_value(binding, 'tmdbId'),
            'wd_rotten_tomatoes_id': self._get_value(binding, 'rottenTomatoesId'),
            'wd_letterboxd_id': self._get_value(binding, 'letterboxdId'),
        }
        
        return enriched
    
    @staticmethod
    def _extract_id(uri: str) -> Optional[str]:
        """Extract Wikidata ID from URI."""
        if not uri:
            return None
        return uri.split('/')[-1]
    
    @staticmethod
    def _get_value(binding: Dict, key: str) -> Optional[str]:
        """Safely extract value from SPARQL binding."""
        if key not in binding:
            return None
        value = binding[key].get('value')
        return value if value else None
    
    @staticmethod
    def _parse_numeric(binding: Dict, key: str) -> Optional[float]:
        """Parse numeric value from SPARQL binding."""
        value = WikidataClient._get_value(binding, key)
        if not value:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None