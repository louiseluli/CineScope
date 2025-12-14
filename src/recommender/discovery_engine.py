"""
CineScope Discovery Recommendation Engine

A recommendation system that discovers NEW movies from TMDB based on:
- User preferences (genres, decades, actors, directors)
- Similar movies to ones you've loved
- Trending/popular films matching your taste
- Deep cuts and hidden gems

This engine recommends movies you HAVEN'T seen yet, using TMDB's vast database.

Usage:
    from src.recommender.discovery_engine import DiscoveryEngine
    
    engine = DiscoveryEngine()
    engine.set_preferences(
        favorite_genres=['Drama', 'Thriller'],
        favorite_decades=[1990, 2000],
        favorite_directors=['Christopher Nolan', 'Denis Villeneuve']
    )
    recommendations = engine.discover(limit=20)
"""
import requests
import time
import logging
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
from datetime import datetime
import json

logger = logging.getLogger(__name__)


@dataclass
class UserPreferences:
    """User's movie preferences for discovery."""
    favorite_genres: List[str] = field(default_factory=list)
    favorite_decades: List[int] = field(default_factory=list)
    favorite_directors: List[str] = field(default_factory=list)
    favorite_actors: List[str] = field(default_factory=list)
    min_rating: float = 6.0
    max_runtime: int = 240
    include_adult: bool = False
    preferred_languages: List[str] = field(default_factory=lambda: ['en'])
    avoid_genres: List[str] = field(default_factory=list)
    mood: str = 'any'  # 'feel_good', 'intense', 'thought_provoking', 'fun', 'any'


@dataclass
class DiscoveredMovie:
    """A discovered movie recommendation."""
    tmdb_id: int
    imdb_id: Optional[str]
    title: str
    original_title: str
    year: int
    genres: List[str]
    overview: str
    rating: float
    vote_count: int
    popularity: float
    poster_path: Optional[str]
    backdrop_path: Optional[str]
    runtime: Optional[int]
    directors: List[str]
    cast: List[str]
    keywords: List[str]
    original_language: str
    production_countries: List[str]
    
    # Recommendation metadata
    match_score: float = 0.0
    match_reasons: List[str] = field(default_factory=list)
    discovery_source: str = ''  # 'similar', 'discover', 'trending', 'director', etc.
    
    def to_dict(self) -> Dict:
        return {
            'tmdb_id': self.tmdb_id,
            'imdb_id': self.imdb_id,
            'title': self.title,
            'original_title': self.original_title,
            'display_title': f"{self.title}" if self.title == self.original_title else f"{self.title} ({self.original_title})",
            'year': self.year,
            'genres': self.genres,
            'overview': self.overview,
            'rating': self.rating,
            'vote_count': self.vote_count,
            'popularity': self.popularity,
            'poster_url': f"https://image.tmdb.org/t/p/w500{self.poster_path}" if self.poster_path else None,
            'backdrop_url': f"https://image.tmdb.org/t/p/original{self.backdrop_path}" if self.backdrop_path else None,
            'runtime': self.runtime,
            'directors': self.directors,
            'cast': self.cast[:10],  # Top 10 cast
            'keywords': self.keywords[:15],
            'original_language': self.original_language,
            'countries': self.production_countries,
            'match_score': round(self.match_score, 1),
            'match_reasons': self.match_reasons,
            'discovery_source': self.discovery_source
        }


# TMDB Genre ID mapping
TMDB_GENRES = {
    28: 'Action', 12: 'Adventure', 16: 'Animation', 35: 'Comedy',
    80: 'Crime', 99: 'Documentary', 18: 'Drama', 10751: 'Family',
    14: 'Fantasy', 36: 'History', 27: 'Horror', 10402: 'Music',
    9648: 'Mystery', 10749: 'Romance', 878: 'Science Fiction',
    10770: 'TV Movie', 53: 'Thriller', 10752: 'War', 37: 'Western'
}

GENRE_TO_ID = {v: k for k, v in TMDB_GENRES.items()}


class DiscoveryEngine:
    """
    Discovers new movies from TMDB based on user preferences.
    
    This engine searches TMDB's database to find movies the user
    hasn't seen yet that match their preferences.
    """
    
    BASE_URL = "https://api.themoviedb.org/3"
    RATE_LIMIT_DELAY = 0.25  # TMDB allows 40 req/10s
    
    def __init__(self, api_key: str = None, read_token: str = None):
        """
        Initialize the discovery engine.
        
        Args:
            api_key: TMDB API key
            read_token: TMDB Read Access Token (v4)
        """
        # Try to get from config
        try:
            from src.core.config import settings
            api_key = api_key or settings.TMDB_API_KEY
            read_token = read_token or settings.TMDB_READ_TOKEN
        except ImportError:
            pass
        
        if not api_key and not read_token:
            raise ValueError("TMDB API key or read token required")
        
        self.api_key = api_key
        self.session = requests.Session()
        if read_token:
            self.session.headers.update({
                'Authorization': f'Bearer {read_token}',
                'Accept': 'application/json'
            })
        
        self._last_request = 0
        self.preferences = UserPreferences()
        self.watched_ids: Set[int] = set()  # TMDB IDs of watched movies
        
    def _rate_limit(self):
        """Respect TMDB rate limits."""
        elapsed = time.time() - self._last_request
        if elapsed < self.RATE_LIMIT_DELAY:
            time.sleep(self.RATE_LIMIT_DELAY - elapsed)
        self._last_request = time.time()
    
    def _request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Make API request with rate limiting."""
        self._rate_limit()
        
        url = f"{self.BASE_URL}{endpoint}"
        if params is None:
            params = {}
        if self.api_key and 'api_key' not in params:
            params['api_key'] = self.api_key
            
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                logger.warning("Rate limited, waiting...")
                time.sleep(2)
                return self._request(endpoint, params)
            logger.error(f"HTTP error: {e}")
        except Exception as e:
            logger.error(f"Request error: {e}")
        return None
    
    def set_watched_movies(self, tmdb_ids: List[int]):
        """Set list of watched movie TMDB IDs to exclude from recommendations."""
        self.watched_ids = set(tmdb_ids)
        logger.info(f"Loaded {len(self.watched_ids)} watched movies to exclude")
    
    def set_preferences(self,
                       favorite_genres: List[str] = None,
                       favorite_decades: List[int] = None,
                       favorite_directors: List[str] = None,
                       favorite_actors: List[str] = None,
                       min_rating: float = 6.0,
                       avoid_genres: List[str] = None,
                       mood: str = 'any'):
        """
        Set user preferences for discovery.
        
        Args:
            favorite_genres: List of preferred genres
            favorite_decades: List of preferred decades (e.g., [1990, 2000])
            favorite_directors: List of favorite director names
            favorite_actors: List of favorite actor names
            min_rating: Minimum TMDB rating
            avoid_genres: Genres to exclude
            mood: 'feel_good', 'intense', 'thought_provoking', 'fun', 'any'
        """
        if favorite_genres:
            self.preferences.favorite_genres = favorite_genres
        if favorite_decades:
            self.preferences.favorite_decades = favorite_decades
        if favorite_directors:
            self.preferences.favorite_directors = favorite_directors
        if favorite_actors:
            self.preferences.favorite_actors = favorite_actors
        if min_rating:
            self.preferences.min_rating = min_rating
        if avoid_genres:
            self.preferences.avoid_genres = avoid_genres
        if mood:
            self.preferences.mood = mood
    
    def _parse_movie(self, data: Dict, source: str = '') -> Optional[DiscoveredMovie]:
        """Parse TMDB movie data into DiscoveredMovie."""
        if not data:
            return None
        
        tmdb_id = data.get('id')
        if tmdb_id in self.watched_ids:
            return None
        
        # Get external IDs if not present
        imdb_id = data.get('imdb_id')
        if not imdb_id and tmdb_id:
            ext = self._request(f'/movie/{tmdb_id}/external_ids')
            if ext:
                imdb_id = ext.get('imdb_id')
        
        # Parse genres
        genres = []
        for g in data.get('genres', []):
            if isinstance(g, dict):
                genres.append(g.get('name', ''))
            elif isinstance(g, int):
                genres.append(TMDB_GENRES.get(g, ''))
        
        # For basic movie data from discover/search (genre_ids instead of genres)
        if not genres:
            for gid in data.get('genre_ids', []):
                genres.append(TMDB_GENRES.get(gid, ''))
        
        # Parse credits
        directors = []
        cast = []
        credits = data.get('credits', {})
        for crew in credits.get('crew', []):
            if crew.get('job') == 'Director':
                directors.append(crew.get('name', ''))
        for person in credits.get('cast', [])[:15]:
            cast.append(person.get('name', ''))
        
        # Parse keywords
        keywords = []
        for kw in data.get('keywords', {}).get('keywords', []):
            keywords.append(kw.get('name', ''))
        
        # Parse countries
        countries = []
        for c in data.get('production_countries', []):
            countries.append(c.get('name', ''))
        
        # Parse year
        release_date = data.get('release_date', '')
        year = int(release_date[:4]) if release_date and len(release_date) >= 4 else 0
        
        return DiscoveredMovie(
            tmdb_id=tmdb_id,
            imdb_id=imdb_id,
            title=data.get('title', ''),
            original_title=data.get('original_title', ''),
            year=year,
            genres=genres,
            overview=data.get('overview', ''),
            rating=data.get('vote_average', 0),
            vote_count=data.get('vote_count', 0),
            popularity=data.get('popularity', 0),
            poster_path=data.get('poster_path'),
            backdrop_path=data.get('backdrop_path'),
            runtime=data.get('runtime'),
            directors=directors,
            cast=cast,
            keywords=keywords,
            original_language=data.get('original_language', ''),
            production_countries=countries,
            discovery_source=source
        )
    
    def _get_full_movie_details(self, tmdb_id: int) -> Optional[Dict]:
        """Get full movie details including credits and keywords."""
        return self._request(f'/movie/{tmdb_id}', {
            'append_to_response': 'credits,keywords,external_ids'
        })
    
    def _calculate_match_score(self, movie: DiscoveredMovie) -> float:
        """Calculate how well a movie matches user preferences."""
        score = 50.0  # Base score
        reasons = []
        
        # Genre matching (up to +30)
        genre_matches = set(movie.genres) & set(self.preferences.favorite_genres)
        if genre_matches:
            score += len(genre_matches) * 10
            reasons.append(f"Matches your favorite genres: {', '.join(genre_matches)}")
        
        # Avoid genres (penalty)
        genre_avoid = set(movie.genres) & set(self.preferences.avoid_genres)
        if genre_avoid:
            score -= len(genre_avoid) * 15
            reasons.append(f"Contains genres you avoid: {', '.join(genre_avoid)}")
        
        # Decade matching (+15)
        if movie.year:
            decade = (movie.year // 10) * 10
            if decade in self.preferences.favorite_decades:
                score += 15
                reasons.append(f"From your preferred era: {decade}s")
        
        # Director matching (+25)
        for director in movie.directors:
            if director in self.preferences.favorite_directors:
                score += 25
                reasons.append(f"Directed by {director}")
                break
        
        # Actor matching (+15)
        for actor in movie.cast[:5]:
            if actor in self.preferences.favorite_actors:
                score += 15
                reasons.append(f"Starring {actor}")
                break
        
        # Rating quality (+10 for highly rated)
        if movie.rating >= 8.0:
            score += 10
            reasons.append(f"Critically acclaimed ({movie.rating})")
        elif movie.rating >= 7.5:
            score += 5
            reasons.append(f"Well-reviewed ({movie.rating})")
        
        # Vote count credibility
        if movie.vote_count >= 1000:
            score += 5
            reasons.append("Wide audience consensus")
        
        # Mood matching
        mood_genres = {
            'feel_good': ['Comedy', 'Family', 'Animation', 'Romance'],
            'intense': ['Thriller', 'Horror', 'War', 'Crime'],
            'thought_provoking': ['Drama', 'Documentary', 'History'],
            'fun': ['Action', 'Adventure', 'Comedy', 'Science Fiction']
        }
        if self.preferences.mood in mood_genres:
            if set(movie.genres) & set(mood_genres[self.preferences.mood]):
                score += 10
                reasons.append(f"Matches your {self.preferences.mood.replace('_', ' ')} mood")
        
        movie.match_score = max(0, min(100, score))
        movie.match_reasons = reasons
        return movie.match_score
    
    def discover_by_genre(self, genres: List[str] = None, limit: int = 20) -> List[DiscoveredMovie]:
        """Discover movies by genre preferences."""
        genres = genres or self.preferences.favorite_genres
        if not genres:
            return []
        
        # Convert genre names to IDs
        genre_ids = [GENRE_TO_ID.get(g) for g in genres if g in GENRE_TO_ID]
        if not genre_ids:
            return []
        
        movies = []
        params = {
            'sort_by': 'vote_average.desc',
            'vote_count.gte': 500,
            'vote_average.gte': self.preferences.min_rating,
            'with_genres': ','.join(map(str, genre_ids)),
            'include_adult': str(self.preferences.include_adult).lower(),
            'page': 1
        }
        
        data = self._request('/discover/movie', params)
        if data and 'results' in data:
            for item in data['results']:
                movie = self._parse_movie(item, 'genre_discover')
                if movie:
                    self._calculate_match_score(movie)
                    movies.append(movie)
                    if len(movies) >= limit:
                        break
        
        return sorted(movies, key=lambda m: m.match_score, reverse=True)[:limit]
    
    def discover_by_decade(self, decade: int = None, limit: int = 20) -> List[DiscoveredMovie]:
        """Discover top movies from a specific decade."""
        if decade is None:
            decade = self.preferences.favorite_decades[0] if self.preferences.favorite_decades else 2000
        
        movies = []
        params = {
            'sort_by': 'vote_average.desc',
            'vote_count.gte': 1000,
            'vote_average.gte': self.preferences.min_rating,
            'primary_release_date.gte': f'{decade}-01-01',
            'primary_release_date.lte': f'{decade + 9}-12-31',
            'page': 1
        }
        
        data = self._request('/discover/movie', params)
        if data and 'results' in data:
            for item in data['results']:
                movie = self._parse_movie(item, 'decade_discover')
                if movie:
                    self._calculate_match_score(movie)
                    movies.append(movie)
                    if len(movies) >= limit:
                        break
        
        return sorted(movies, key=lambda m: m.match_score, reverse=True)[:limit]
    
    def find_similar_to(self, tmdb_id: int, limit: int = 10) -> List[DiscoveredMovie]:
        """Find movies similar to a specific movie."""
        movies = []
        
        data = self._request(f'/movie/{tmdb_id}/similar')
        if data and 'results' in data:
            for item in data['results']:
                movie = self._parse_movie(item, 'similar')
                if movie:
                    self._calculate_match_score(movie)
                    movies.append(movie)
                    if len(movies) >= limit:
                        break
        
        return sorted(movies, key=lambda m: m.match_score, reverse=True)[:limit]
    
    def discover_by_director(self, director_name: str, limit: int = 20) -> List[DiscoveredMovie]:
        """Discover movies by a specific director."""
        # First, search for the director
        data = self._request('/search/person', {'query': director_name})
        if not data or not data.get('results'):
            return []
        
        # Get director's ID
        director_id = data['results'][0]['id']
        
        # Get their movie credits
        movies = []
        credits = self._request(f'/person/{director_id}/movie_credits')
        if credits and 'crew' in credits:
            for item in credits['crew']:
                if item.get('job') == 'Director':
                    # Get full details
                    full = self._get_full_movie_details(item['id'])
                    if full:
                        movie = self._parse_movie(full, f'director:{director_name}')
                        if movie:
                            self._calculate_match_score(movie)
                            movies.append(movie)
                            if len(movies) >= limit:
                                break
        
        return sorted(movies, key=lambda m: m.match_score, reverse=True)[:limit]
    
    def discover_by_actor(self, actor_name: str, limit: int = 20) -> List[DiscoveredMovie]:
        """Discover movies by a specific actor."""
        data = self._request('/search/person', {'query': actor_name})
        if not data or not data.get('results'):
            return []
        
        actor_id = data['results'][0]['id']
        
        movies = []
        credits = self._request(f'/person/{actor_id}/movie_credits')
        if credits and 'cast' in credits:
            # Sort by popularity
            sorted_credits = sorted(credits['cast'], 
                                   key=lambda x: x.get('popularity', 0), 
                                   reverse=True)
            for item in sorted_credits[:limit * 2]:
                movie = self._parse_movie(item, f'actor:{actor_name}')
                if movie and movie.vote_count >= 100:
                    self._calculate_match_score(movie)
                    movies.append(movie)
                    if len(movies) >= limit:
                        break
        
        return sorted(movies, key=lambda m: m.match_score, reverse=True)[:limit]
    
    def get_trending(self, time_window: str = 'week', limit: int = 20) -> List[DiscoveredMovie]:
        """Get trending movies matching user preferences."""
        movies = []
        
        data = self._request(f'/trending/movie/{time_window}')
        if data and 'results' in data:
            for item in data['results']:
                movie = self._parse_movie(item, 'trending')
                if movie:
                    self._calculate_match_score(movie)
                    # Only include if reasonably matches preferences
                    if movie.match_score >= 40:
                        movies.append(movie)
                    if len(movies) >= limit:
                        break
        
        return sorted(movies, key=lambda m: m.match_score, reverse=True)[:limit]
    
    def discover_hidden_gems(self, limit: int = 20) -> List[DiscoveredMovie]:
        """Find highly-rated but less popular movies matching preferences."""
        genre_ids = [GENRE_TO_ID.get(g) for g in self.preferences.favorite_genres if g in GENRE_TO_ID]
        
        movies = []
        params = {
            'sort_by': 'vote_average.desc',
            'vote_count.gte': 100,
            'vote_count.lte': 1000,  # Not too popular
            'vote_average.gte': 7.5,
            'page': 1
        }
        if genre_ids:
            params['with_genres'] = '|'.join(map(str, genre_ids))  # OR logic
        
        data = self._request('/discover/movie', params)
        if data and 'results' in data:
            for item in data['results']:
                movie = self._parse_movie(item, 'hidden_gem')
                if movie:
                    self._calculate_match_score(movie)
                    movie.match_reasons.insert(0, "Hidden gem: Highly rated but under the radar")
                    movies.append(movie)
                    if len(movies) >= limit:
                        break
        
        return sorted(movies, key=lambda m: m.match_score, reverse=True)[:limit]
    
    def search(self, query: str, limit: int = 20) -> List[DiscoveredMovie]:
        """Search for movies by title."""
        movies = []
        
        data = self._request('/search/movie', {'query': query})
        if data and 'results' in data:
            for item in data['results']:
                # Get full details for better data
                full = self._get_full_movie_details(item['id'])
                movie = self._parse_movie(full or item, 'search')
                if movie:
                    self._calculate_match_score(movie)
                    movies.append(movie)
                    if len(movies) >= limit:
                        break
        
        return movies[:limit]
    
    def discover(self, limit: int = 20) -> Dict[str, List[DiscoveredMovie]]:
        """
        Run full discovery based on all preferences.
        
        Returns categorized recommendations.
        """
        results = {}
        per_category = max(5, limit // 4)
        
        # Trending that matches preferences
        results['trending_for_you'] = self.get_trending(limit=per_category)
        
        # Genre-based discovery
        if self.preferences.favorite_genres:
            results['based_on_genres'] = self.discover_by_genre(limit=per_category)
        
        # Decade exploration
        if self.preferences.favorite_decades:
            results['from_your_favorite_era'] = self.discover_by_decade(
                self.preferences.favorite_decades[0], limit=per_category
            )
        
        # Director exploration
        if self.preferences.favorite_directors:
            director = self.preferences.favorite_directors[0]
            results[f'more_from_{director.replace(" ", "_")}'] = self.discover_by_director(
                director, limit=per_category
            )
        
        # Hidden gems
        results['hidden_gems'] = self.discover_hidden_gems(limit=per_category)
        
        return results
    
    def get_recommendations_json(self, limit: int = 20) -> Dict:
        """Get recommendations in JSON-serializable format."""
        discoveries = self.discover(limit)
        return {
            category: [movie.to_dict() for movie in movies]
            for category, movies in discoveries.items()
        }
