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
        # Clean IMDb ID
        if not imdb_id:
            return None
        imdb_id = imdb_id.strip().replace('tt', '')
        
        query = f"""
        PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX schema: <http://schema.org/>
        
        SELECT DISTINCT
            ?film ?filmLabel ?filmDescription
            ?image ?publicationDate
            (GROUP_CONCAT(DISTINCT ?genreLabel; separator=", ") AS ?genres)
            (GROUP_CONCAT(DISTINCT ?countryLabel; separator=", ") AS ?countries)
            (GROUP_CONCAT(DISTINCT ?languageLabel; separator=", ") AS ?languages)
            (GROUP_CONCAT(DISTINCT ?directorLabel; separator=", ") AS ?directors)
            (GROUP_CONCAT(DISTINCT ?screenwriterLabel; separator=", ") AS ?screenwriters)
            (GROUP_CONCAT(DISTINCT ?composerLabel; separator=", ") AS ?composers)
            (GROUP_CONCAT(DISTINCT ?producerLabel; separator=", ") AS ?producers)
            (GROUP_CONCAT(DISTINCT ?cinematographerLabel; separator=", ") AS ?cinematographers)
            (GROUP_CONCAT(DISTINCT ?editorLabel; separator=", ") AS ?editors)
            (GROUP_CONCAT(DISTINCT ?productionCompanyLabel; separator=", ") AS ?productionCompanies)
            (GROUP_CONCAT(DISTINCT ?distributorLabel; separator=", ") AS ?distributors)
            (GROUP_CONCAT(DISTINCT ?castMemberLabel; separator="|") AS ?castMembers)
            (GROUP_CONCAT(DISTINCT ?awardLabel; separator=", ") AS ?awards)
            (GROUP_CONCAT(DISTINCT ?nominationLabel; separator=", ") AS ?nominations)
            (GROUP_CONCAT(DISTINCT ?narrativeLocationLabel; separator=", ") AS ?narrativeLocations)
            (GROUP_CONCAT(DISTINCT ?filmingLocationLabel; separator=", ") AS ?filmingLocations)
            (GROUP_CONCAT(DISTINCT ?basedOnLabel; separator=", ") AS ?basedOnWorks)
            ?duration ?boxOffice ?budget ?costOfProduction
            ?aspectRatio ?colorLabel
            ?followsLabel ?followedByLabel
            ?tmdbId ?rottenTomatoesId ?letterboxdId
            ?wikipediaUrl ?officialWebsite
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
            OPTIONAL {{ 
                ?film wdt:P272 ?productionCompany . 
                ?productionCompany rdfs:label ?productionCompanyLabel FILTER(LANG(?productionCompanyLabel) = "en") 
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
                ?film wdt:P162 ?producer . 
                ?producer rdfs:label ?producerLabel FILTER(LANG(?producerLabel) = "en") 
            }}
            OPTIONAL {{ 
                ?film wdt:P344 ?cinematographer . 
                ?cinematographer rdfs:label ?cinematographerLabel FILTER(LANG(?cinematographerLabel) = "en") 
            }}
            OPTIONAL {{ 
                ?film wdt:P1040 ?editor . 
                ?editor rdfs:label ?editorLabel FILTER(LANG(?editorLabel) = "en") 
            }}
            
            # Cast (limited to avoid query timeout)
            OPTIONAL {{ 
                ?film wdt:P161 ?castMember . 
                ?castMember rdfs:label ?castMemberLabel FILTER(LANG(?castMemberLabel) = "en") 
            }}
            
            # Distribution
            OPTIONAL {{ 
                ?film wdt:P750 ?distributor . 
                ?distributor rdfs:label ?distributorLabel FILTER(LANG(?distributorLabel) = "en") 
            }}
            
            # Recognition
            OPTIONAL {{ 
                ?film wdt:P166 ?award . 
                ?award rdfs:label ?awardLabel FILTER(LANG(?awardLabel) = "en") 
            }}
            OPTIONAL {{ 
                ?film wdt:P1411 ?nomination . 
                ?nomination rdfs:label ?nominationLabel FILTER(LANG(?nominationLabel) = "en") 
            }}
            
            # Business
            OPTIONAL {{ ?film wdt:P2142 ?boxOffice . }}
            OPTIONAL {{ ?film wdt:P2130 ?budget . }}
            OPTIONAL {{ ?film wdt:P5371 ?costOfProduction . }}
            
            # Technical
            OPTIONAL {{ ?film wdt:P2061 ?aspectRatio . }}
            OPTIONAL {{ 
                ?film wdt:P462 ?color . 
                ?color rdfs:label ?colorLabel FILTER(LANG(?colorLabel) = "en") 
            }}
            
            # Locations
            OPTIONAL {{ 
                ?film wdt:P840 ?narrativeLocation . 
                ?narrativeLocation rdfs:label ?narrativeLocationLabel FILTER(LANG(?narrativeLocationLabel) = "en") 
            }}
            OPTIONAL {{ 
                ?film wdt:P915 ?filmingLocation . 
                ?filmingLocation rdfs:label ?filmingLocationLabel FILTER(LANG(?filmingLocationLabel) = "en") 
            }}
            
            # Related Works
            OPTIONAL {{ 
                ?film wdt:P144 ?basedOn . 
                ?basedOn rdfs:label ?basedOnLabel FILTER(LANG(?basedOnLabel) = "en") 
            }}
            OPTIONAL {{ 
                ?film wdt:P155 ?follows . 
                ?follows rdfs:label ?followsLabel FILTER(LANG(?followsLabel) = "en") 
            }}
            OPTIONAL {{ 
                ?film wdt:P156 ?followedBy . 
                ?followedBy rdfs:label ?followedByLabel FILTER(LANG(?followedByLabel) = "en") 
            }}
            
            # External IDs
            OPTIONAL {{ ?film wdt:P4947 ?tmdbId . }}
            OPTIONAL {{ ?film wdt:P1258 ?rottenTomatoesId . }}
            OPTIONAL {{ ?film wdt:P6127 ?letterboxdId . }}
            OPTIONAL {{ ?film wdt:P856 ?officialWebsite . }}
            OPTIONAL {{
                ?wikipediaUrl schema:about ?film ;
                              schema:isPartOf <https://en.wikipedia.org/> .
            }}
            
            SERVICE wikibase:label {{ 
                bd:serviceParam wikibase:language "en" .
                ?film rdfs:label ?filmLabel .
                ?film schema:description ?filmDescription .
            }}
        }}
        GROUP BY ?film ?filmLabel ?filmDescription ?image ?publicationDate 
                 ?duration ?boxOffice ?budget ?costOfProduction ?aspectRatio 
                 ?colorLabel ?followsLabel ?followedByLabel ?tmdbId 
                 ?rottenTomatoesId ?letterboxdId ?wikipediaUrl ?officialWebsite
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
            'wd_production_companies': self._get_value(binding, 'productionCompanies'),
            
            # Key People
            'wd_directors': self._get_value(binding, 'directors'),
            'wd_screenwriters': self._get_value(binding, 'screenwriters'),
            'wd_composers': self._get_value(binding, 'composers'),
            'wd_producers': self._get_value(binding, 'producers'),
            'wd_cinematographers': self._get_value(binding, 'cinematographers'),
            'wd_editors': self._get_value(binding, 'editors'),
            'wd_cast_members': self._get_value(binding, 'castMembers'),
            
            # Distribution
            'wd_distributors': self._get_value(binding, 'distributors'),
            
            # Recognition
            'wd_awards': self._get_value(binding, 'awards'),
            'wd_nominations': self._get_value(binding, 'nominations'),
            
            # Business
            'wd_box_office': self._parse_numeric(binding, 'boxOffice'),
            'wd_budget': self._parse_numeric(binding, 'budget'),
            'wd_cost_of_production': self._parse_numeric(binding, 'costOfProduction'),
            
            # Technical Details
            'wd_aspect_ratio': self._get_value(binding, 'aspectRatio'),
            'wd_color': self._get_value(binding, 'colorLabel'),
            
            # Locations
            'wd_narrative_locations': self._get_value(binding, 'narrativeLocations'),
            'wd_filming_locations': self._get_value(binding, 'filmingLocations'),
            
            # Related Works
            'wd_based_on': self._get_value(binding, 'basedOnWorks'),
            'wd_follows': self._get_value(binding, 'followsLabel'),
            'wd_followed_by': self._get_value(binding, 'followedByLabel'),
            
            # External Links
            'wd_tmdb_id': self._get_value(binding, 'tmdbId'),
            'wd_rotten_tomatoes_id': self._get_value(binding, 'rottenTomatoesId'),
            'wd_letterboxd_id': self._get_value(binding, 'letterboxdId'),
            'wd_wikipedia_url': self._get_value(binding, 'wikipediaUrl'),
            'wd_official_website': self._get_value(binding, 'officialWebsite'),
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