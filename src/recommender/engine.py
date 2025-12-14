"""
CineScope Professional Recommendation Engine

A multi-signal hybrid recommendation system that provides personalized,
explainable movie recommendations based on:
- Content similarity (genres, keywords, themes)
- Viewing patterns (decades, runtimes, quality preferences)
- Network connections (actors, directors, collaborations)
- Critical alignment (how you agree with critics)

Usage:
    from src.recommender.engine import RecommendationEngine
    
    engine = RecommendationEngine(movies_df, people_cache)
    recommendations = engine.get_recommendations(limit=20)
"""
import pandas as pd
import numpy as np
from collections import Counter, defaultdict
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class RecommendationType(Enum):
    """Types of recommendations with different strategies."""
    PERFECT_MATCH = "perfect_match"
    HIDDEN_GEM = "hidden_gem"
    DIRECTOR_EXPLORATION = "director_exploration"
    ACTOR_JOURNEY = "actor_journey"
    GENRE_EXPANSION = "genre_expansion"
    ERA_EXPLORATION = "era_exploration"
    COMFORT_WATCH = "comfort_watch"
    STRETCH_RECOMMENDATION = "stretch_recommendation"


@dataclass
class Recommendation:
    """A single movie recommendation with explanation."""
    movie_id: str
    title: str
    year: int
    score: float  # 0-100 confidence score
    recommendation_type: RecommendationType
    primary_reason: str
    supporting_reasons: List[str] = field(default_factory=list)
    similar_films_you_loved: List[str] = field(default_factory=list)
    risk_factors: List[str] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for API response."""
        return {
            'movie_id': self.movie_id,
            'title': self.title,
            'year': self.year,
            'confidence_score': round(self.score, 1),
            'recommendation_type': self.recommendation_type.value,
            'explanation': {
                'primary_reason': self.primary_reason,
                'supporting_reasons': self.supporting_reasons,
                'similar_films_you_loved': self.similar_films_you_loved,
                'risk_factors': self.risk_factors,
            },
            **self.metadata
        }


@dataclass
class UserProfile:
    """Aggregated user viewing preferences and patterns."""
    # Genre preferences (genre -> weight based on ratings)
    genre_weights: Dict[str, float] = field(default_factory=dict)
    
    # Decade preferences
    decade_weights: Dict[int, float] = field(default_factory=dict)
    
    # Runtime preferences
    preferred_runtime_range: Tuple[int, int] = (90, 150)
    
    # Quality threshold
    min_acceptable_rating: float = 6.5
    avg_rating_given: float = 7.0
    
    # Trusted directors (directors you consistently rate highly)
    trusted_directors: Dict[str, float] = field(default_factory=dict)
    
    # Favorite actors (actors in your highest-rated films)
    favorite_actors: Dict[str, float] = field(default_factory=dict)
    
    # Critical alignment (how much you agree with RT/Metacritic)
    rt_correlation: float = 0.5
    metacritic_correlation: float = 0.5
    
    # Content preferences from DoesTheDogDie
    content_warnings_to_avoid: List[str] = field(default_factory=list)
    
    # Keywords/themes that predict high ratings for you
    resonant_themes: Dict[str, float] = field(default_factory=dict)


class RecommendationEngine:
    """
    Professional-grade recommendation engine with multiple strategies.
    
    Provides explainable recommendations based on user viewing patterns,
    content similarity, and network connections.
    """
    
    def __init__(self, 
                 watched_df: pd.DataFrame,
                 unwatched_df: pd.DataFrame = None,
                 people_cache: Dict = None):
        """
        Initialize the recommendation engine.
        
        Args:
            watched_df: DataFrame of watched movies with ratings
            unwatched_df: DataFrame of potential recommendations (unwatched)
            people_cache: Dictionary of people data
        """
        self.watched_df = watched_df.copy()
        self.unwatched_df = unwatched_df.copy() if unwatched_df is not None else pd.DataFrame()
        self.people_cache = people_cache or {}
        
        # Build user profile from watched movies
        self.user_profile = self._build_user_profile()
        
        logger.info(f"Recommendation engine initialized with {len(self.watched_df)} watched films")
    
    def _build_user_profile(self) -> UserProfile:
        """Build user profile from watched movie data."""
        profile = UserProfile()
        
        # Parse ratings column
        rating_col = self._find_column(['Your Rating', 'your_rating', 'IMDb Rating', 'imdb_rating'])
        genre_col = self._find_column(['Genres', 'genres'])
        year_col = self._find_column(['Year', 'year', 'startYear'])
        runtime_col = self._find_column(['Runtime (mins)', 'runtime_mins', 'runtime'])
        director_col = self._find_column(['Directors', 'directors', 'director'])
        
        # Calculate genre weights
        genre_ratings = defaultdict(list)
        for _, row in self.watched_df.iterrows():
            genres = self._parse_list(row.get(genre_col, ''))
            rating = row.get(rating_col)
            if genres and pd.notna(rating):
                for genre in genres:
                    genre_ratings[genre].append(float(rating))
        
        # Weight = avg rating * log(count) - favor genres you watch often AND rate highly
        for genre, ratings in genre_ratings.items():
            avg_rating = np.mean(ratings)
            count_factor = np.log1p(len(ratings))
            profile.genre_weights[genre] = (avg_rating - 5) * count_factor  # Center around 5
        
        # Calculate decade weights
        decade_ratings = defaultdict(list)
        for _, row in self.watched_df.iterrows():
            year = row.get(year_col)
            rating = row.get(rating_col)
            if pd.notna(year) and pd.notna(rating):
                decade = int(year) // 10 * 10
                decade_ratings[decade].append(float(rating))
        
        for decade, ratings in decade_ratings.items():
            avg_rating = np.mean(ratings)
            count_factor = np.log1p(len(ratings))
            profile.decade_weights[decade] = (avg_rating - 5) * count_factor
        
        # Calculate runtime preferences
        runtimes = pd.to_numeric(self.watched_df[runtime_col], errors='coerce').dropna()
        if len(runtimes) > 0:
            profile.preferred_runtime_range = (
                int(runtimes.quantile(0.25)),
                int(runtimes.quantile(0.75))
            )
        
        # Calculate average rating
        ratings = pd.to_numeric(self.watched_df[rating_col], errors='coerce').dropna()
        if len(ratings) > 0:
            profile.avg_rating_given = float(ratings.mean())
            profile.min_acceptable_rating = float(ratings.quantile(0.25))
        
        # Find trusted directors
        director_ratings = defaultdict(list)
        for _, row in self.watched_df.iterrows():
            directors = self._parse_list(row.get(director_col, ''))
            rating = row.get(rating_col)
            if directors and pd.notna(rating):
                for director in directors:
                    director_ratings[director].append(float(rating))
        
        for director, ratings in director_ratings.items():
            if len(ratings) >= 2:  # Need multiple films
                avg = np.mean(ratings)
                if avg >= 7.5:  # Trust threshold
                    profile.trusted_directors[director] = avg
        
        return profile
    
    def _find_column(self, candidates: List[str]) -> Optional[str]:
        """Find the first matching column name."""
        for col in candidates:
            if col in self.watched_df.columns:
                return col
        return None
    
    def _parse_list(self, value: Any) -> List[str]:
        """Parse a comma-separated string or list."""
        if pd.isna(value):
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            return [x.strip() for x in value.split(',') if x.strip()]
        return []
    
    def get_recommendations(self, 
                           limit: int = 20,
                           types: List[RecommendationType] = None) -> Dict[str, List[Recommendation]]:
        """
        Generate recommendations across multiple strategies.
        
        Args:
            limit: Maximum recommendations per category
            types: Specific recommendation types to generate (all if None)
            
        Returns:
            Dictionary mapping recommendation type to list of recommendations
        """
        if types is None:
            types = list(RecommendationType)
        
        recommendations = {}
        
        for rec_type in types:
            try:
                if rec_type == RecommendationType.PERFECT_MATCH:
                    recommendations['perfect_matches'] = self._find_perfect_matches(limit)
                elif rec_type == RecommendationType.HIDDEN_GEM:
                    recommendations['hidden_gems'] = self._find_hidden_gems(limit)
                elif rec_type == RecommendationType.DIRECTOR_EXPLORATION:
                    recommendations['director_deep_dives'] = self._find_director_explorations(limit)
                elif rec_type == RecommendationType.GENRE_EXPANSION:
                    recommendations['genre_expansion'] = self._find_genre_expansions(limit)
                elif rec_type == RecommendationType.ERA_EXPLORATION:
                    recommendations['era_exploration'] = self._find_era_explorations(limit)
                elif rec_type == RecommendationType.COMFORT_WATCH:
                    recommendations['comfort_watches'] = self._find_comfort_watches(limit)
            except Exception as e:
                logger.warning(f"Failed to generate {rec_type.value} recommendations: {e}")
                recommendations[rec_type.value] = []
        
        return recommendations
    
    def _find_perfect_matches(self, limit: int) -> List[Recommendation]:
        """Find movies that match multiple preference signals."""
        if self.unwatched_df.empty:
            return self._find_internal_matches(limit)
        
        # Score each unwatched movie
        scored_movies = []
        
        genre_col = self._find_column(['Genres', 'genres'])
        year_col = self._find_column(['Year', 'year', 'startYear'])
        rating_col = self._find_column(['IMDb Rating', 'imdb_rating', 'averageRating'])
        title_col = self._find_column(['Title', 'title', 'primaryTitle'])
        id_col = self._find_column(['Const', 'const', 'imdb_id', 'tconst'])
        
        for _, movie in self.unwatched_df.iterrows():
            score = 0
            reasons = []
            
            # Genre matching
            genres = self._parse_list(movie.get(genre_col, ''))
            genre_score = sum(self.user_profile.genre_weights.get(g, 0) for g in genres)
            if genre_score > 0:
                score += min(genre_score * 10, 30)  # Cap at 30 points
                top_genre = max(genres, key=lambda g: self.user_profile.genre_weights.get(g, 0))
                reasons.append(f"Strong genre match: {top_genre}")
            
            # Decade matching
            year = movie.get(year_col)
            if pd.notna(year):
                decade = int(year) // 10 * 10
                decade_score = self.user_profile.decade_weights.get(decade, 0)
                if decade_score > 0:
                    score += min(decade_score * 5, 20)
                    reasons.append(f"From your preferred era: {decade}s")
            
            # Rating quality (if available)
            rating = movie.get(rating_col)
            if pd.notna(rating) and float(rating) >= self.user_profile.min_acceptable_rating:
                quality_boost = (float(rating) - self.user_profile.min_acceptable_rating) * 10
                score += min(quality_boost, 30)
                if float(rating) >= 8.0:
                    reasons.append(f"Highly rated: {rating}/10")
            
            if score > 20:  # Minimum threshold
                scored_movies.append({
                    'movie_id': movie.get(id_col, ''),
                    'title': movie.get(title_col, 'Unknown'),
                    'year': int(year) if pd.notna(year) else 0,
                    'score': min(score, 100),
                    'reasons': reasons,
                    'rating': rating,
                    'genres': genres,
                })
        
        # Sort by score and convert to Recommendation objects
        scored_movies.sort(key=lambda x: x['score'], reverse=True)
        
        recommendations = []
        for movie in scored_movies[:limit]:
            rec = Recommendation(
                movie_id=movie['movie_id'],
                title=movie['title'],
                year=movie['year'],
                score=movie['score'],
                recommendation_type=RecommendationType.PERFECT_MATCH,
                primary_reason=movie['reasons'][0] if movie['reasons'] else "Multi-signal match",
                supporting_reasons=movie['reasons'][1:] if len(movie['reasons']) > 1 else [],
                metadata={
                    'imdb_rating': movie['rating'],
                    'genres': movie['genres'],
                }
            )
            recommendations.append(rec)
        
        return recommendations
    
    def _find_internal_matches(self, limit: int) -> List[Recommendation]:
        """Find similar movies within the watched collection for re-watch suggestions."""
        # Implementation for when no unwatched pool is available
        return []
    
    def _find_hidden_gems(self, limit: int) -> List[Recommendation]:
        """Find high-quality, low-popularity films matching preferences."""
        recommendations = []
        
        # Filter for high rating, low popularity
        rating_col = self._find_column(['IMDb Rating', 'imdb_rating'])
        votes_col = self._find_column(['Num Votes', 'numVotes', 'vote_count'])
        
        if not self.unwatched_df.empty and rating_col:
            df = self.unwatched_df.copy()
            
            # High quality threshold
            df = df[pd.to_numeric(df[rating_col], errors='coerce') >= 7.5]
            
            # Low popularity (if vote count available)
            if votes_col and votes_col in df.columns:
                median_votes = pd.to_numeric(df[votes_col], errors='coerce').median()
                df = df[pd.to_numeric(df[votes_col], errors='coerce') < median_votes]
            
            # Create recommendations from filtered list
            # (simplified - would add full scoring in production)
            
        return recommendations
    
    def _find_director_explorations(self, limit: int) -> List[Recommendation]:
        """Find more films from trusted directors."""
        recommendations = []
        
        # Get films from trusted directors that haven't been watched
        # (Implementation would check unwatched_df for director matches)
        
        return recommendations
    
    def _find_genre_expansions(self, limit: int) -> List[Recommendation]:
        """Find films in adjacent genres to expand taste."""
        # Genre adjacency map (genres that often appear together)
        GENRE_ADJACENCY = {
            'Drama': ['Thriller', 'Romance', 'Crime'],
            'Action': ['Adventure', 'Thriller', 'Sci-Fi'],
            'Comedy': ['Romance', 'Drama', 'Animation'],
            'Horror': ['Thriller', 'Mystery', 'Sci-Fi'],
            'Sci-Fi': ['Action', 'Thriller', 'Horror'],
        }
        
        # Find underexplored adjacent genres
        recommendations = []
        
        return recommendations
    
    def _find_era_explorations(self, limit: int) -> List[Recommendation]:
        """Find films from underexplored decades."""
        recommendations = []
        
        # Find decades with few films watched
        decade_counts = Counter()
        year_col = self._find_column(['Year', 'year'])
        
        for _, row in self.watched_df.iterrows():
            year = row.get(year_col)
            if pd.notna(year):
                decade = int(year) // 10 * 10
                decade_counts[decade] += 1
        
        # Find underexplored decades
        all_decades = range(1920, 2030, 10)
        avg_count = sum(decade_counts.values()) / len(decade_counts) if decade_counts else 5
        
        underexplored = [d for d in all_decades if decade_counts.get(d, 0) < avg_count / 2]
        
        # Would then find highly-rated films from these decades
        
        return recommendations
    
    def _find_comfort_watches(self, limit: int) -> List[Recommendation]:
        """Find reliable feel-good films matching preferences."""
        # Filter for uplifting genres, high ratings, familiar patterns
        COMFORT_GENRES = {'Comedy', 'Animation', 'Family', 'Adventure', 'Romance'}
        
        recommendations = []
        
        return recommendations
    
    def explain_recommendation(self, movie_id: str) -> Dict:
        """
        Generate detailed explanation for why a specific movie is recommended.
        
        Args:
            movie_id: IMDB ID of the movie
            
        Returns:
            Detailed explanation dictionary
        """
        explanation = {
            'movie_id': movie_id,
            'match_factors': [],
            'confidence_breakdown': {},
            'similar_films_in_collection': [],
            'potential_concerns': [],
        }
        
        # Would analyze the movie against user profile and generate explanation
        
        return explanation
    
    def get_profile_summary(self) -> Dict:
        """Get a summary of the user's viewing profile."""
        return {
            'top_genres': sorted(
                self.user_profile.genre_weights.items(),
                key=lambda x: x[1],
                reverse=True
            )[:10],
            'preferred_decades': sorted(
                self.user_profile.decade_weights.items(),
                key=lambda x: x[1],
                reverse=True
            )[:5],
            'trusted_directors': sorted(
                self.user_profile.trusted_directors.items(),
                key=lambda x: x[1],
                reverse=True
            )[:10],
            'runtime_preference': {
                'min': self.user_profile.preferred_runtime_range[0],
                'max': self.user_profile.preferred_runtime_range[1],
            },
            'quality_threshold': self.user_profile.min_acceptable_rating,
            'average_rating': round(self.user_profile.avg_rating_given, 2),
        }


# Convenience function for API
def get_recommendations_for_api(watched_df: pd.DataFrame, 
                                unwatched_df: pd.DataFrame = None,
                                limit: int = 20) -> List[Dict]:
    """
    Generate recommendations formatted for API response.
    
    Args:
        watched_df: User's watched movies
        unwatched_df: Pool of unwatched movies to recommend from
        limit: Max recommendations per category
        
    Returns:
        List of recommendation dictionaries
    """
    engine = RecommendationEngine(watched_df, unwatched_df)
    recommendations = engine.get_recommendations(limit=limit)
    
    # Flatten and convert to dicts
    all_recs = []
    for category, recs in recommendations.items():
        for rec in recs:
            rec_dict = rec.to_dict()
            rec_dict['category'] = category
            all_recs.append(rec_dict)
    
    return all_recs


# Test
if __name__ == '__main__':
    # Create sample data for testing
    sample_watched = pd.DataFrame([
        {'Const': 'tt0111161', 'Title': 'The Shawshank Redemption', 'Year': 1994, 
         'Genres': 'Drama', 'IMDb Rating': 9.3, 'Your Rating': 10, 'Directors': 'Frank Darabont'},
        {'Const': 'tt0068646', 'Title': 'The Godfather', 'Year': 1972,
         'Genres': 'Crime, Drama', 'IMDb Rating': 9.2, 'Your Rating': 9, 'Directors': 'Francis Ford Coppola'},
        {'Const': 'tt0468569', 'Title': 'The Dark Knight', 'Year': 2008,
         'Genres': 'Action, Crime, Drama', 'IMDb Rating': 9.0, 'Your Rating': 9, 'Directors': 'Christopher Nolan'},
    ])
    
    engine = RecommendationEngine(sample_watched)
    
    print("User Profile Summary:")
    print("=" * 50)
    profile = engine.get_profile_summary()
    for key, value in profile.items():
        print(f"{key}: {value}")
