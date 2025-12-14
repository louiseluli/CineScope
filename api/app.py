"""
CineScope API - Flask Backend for the React UI
Serves movie data from the enriched CSV files
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Data paths
DATA_DIR = Path(__file__).parent.parent / 'data'
PROCESSED_DIR = DATA_DIR / 'processed'

# Global data storage
movies_df = None
people_cache = None

def load_data():
    """Load movie data from CSV files"""
    global movies_df, people_cache
    
    # Use watched_movies_master.csv - the actual watched collection
    watched_file = PROCESSED_DIR / 'watched_movies_master.csv'
    master_file = PROCESSED_DIR / 'master_cinema_data.csv'
    
    if watched_file.exists():
        logger.info(f"Loading from {watched_file}")
        movies_df = pd.read_csv(watched_file, low_memory=False)
    elif master_file.exists():
        logger.info(f"Loading from {master_file}")
        movies_df = pd.read_csv(master_file, low_memory=False)
    else:
        logger.error("No movie data file found!")
        movies_df = pd.DataFrame()
    
    # Load people cache if exists
    people_file = PROCESSED_DIR / 'people_cache.json'
    if people_file.exists():
        with open(people_file, 'r') as f:
            people_cache = json.load(f)
        logger.info(f"Loaded {len(people_cache)} people from cache")
    else:
        people_cache = {}
    
    if not movies_df.empty:
        logger.info(f"Loaded {len(movies_df)} movies with {len(movies_df.columns)} columns")

def safe_value(val):
    """Convert numpy/pandas types to JSON-serializable Python types"""
    if pd.isna(val) or val is None:
        return None
    if isinstance(val, (np.integer, np.int64)):
        return int(val)
    if isinstance(val, (np.floating, np.float64)):
        return float(val) if not np.isnan(val) else None
    if isinstance(val, np.bool_):
        return bool(val)
    return val

def clean_value(val):
    """Clean a value - return None for NaN/empty, otherwise return the value as string/original type"""
    if pd.isna(val) or val is None:
        return None
    if isinstance(val, float) and np.isnan(val):
        return None
    if isinstance(val, str) and val.strip() == '':
        return None
    return val

def movie_to_dict(row):
    """Convert a DataFrame row to a movie dictionary"""
    # Get the IMDB ID (handle different column names)
    imdb_id = clean_value(row.get('Const') or row.get('const') or row.get('imdb_id') or row.get('tconst'))
    
    # Get poster URL from various sources
    poster_url = clean_value(
        row.get('tmdb_poster_path') or 
        row.get('poster_url') or 
        row.get('omdb_poster')
    )
    if poster_url and isinstance(poster_url, str) and poster_url.startswith('/'):
        poster_url = f"https://image.tmdb.org/t/p/w500{poster_url}"
    
    # Get genres (handle string or list)
    genres = clean_value(row.get('Genres') or row.get('genres') or '')
    if isinstance(genres, str):
        genres = [g.strip() for g in genres.split(',') if g.strip()]
    elif not isinstance(genres, list):
        genres = []
    
    # Get directors
    directors = clean_value(row.get('Directors') or row.get('directors') or row.get('director') or '')
    if isinstance(directors, str):
        directors = [d.strip() for d in directors.split(',') if d.strip()]
    elif not isinstance(directors, list):
        directors = []
    
    # Get backdrop URL
    backdrop_url = clean_value(row.get('tmdb_backdrop_path'))
    if backdrop_url and isinstance(backdrop_url, str) and not backdrop_url.startswith('http'):
        # It's a TMDB path
        pass  # Frontend will add the base URL
    
    return {
        'const': safe_value(imdb_id),
        'title': safe_value(row.get('Title') or row.get('title')),
        'year': safe_value(row.get('Year') or row.get('year') or row.get('startYear')),
        'imdbRating': safe_value(row.get('IMDb Rating') or row.get('imdb_rating') or row.get('averageRating')),
        'yourRating': safe_value(row.get('Your Rating') or row.get('your_rating')),
        'runtime': safe_value(row.get('Runtime (mins)') or row.get('runtime') or row.get('runtimeMinutes')),
        'genres': genres[:5],  # Limit genres for display
        'directors': directors[:3],  # Limit directors
        'overview': safe_value(row.get('tmdb_overview') or row.get('omdb_plot') or row.get('plot')),
        'posterUrl': poster_url,
        'backdropUrl': backdrop_url,
        
        # TMDB data
        'tmdb_id': safe_value(row.get('tmdb_id')),
        'tmdb_vote_average': safe_value(row.get('tmdb_vote_average')),
        'tmdb_vote_count': safe_value(row.get('tmdb_vote_count')),
        'tmdb_popularity': safe_value(row.get('tmdb_popularity')),
        'tmdb_tagline': safe_value(row.get('tmdb_tagline')),
        'tmdb_budget': safe_value(row.get('tmdb_budget')),
        'tmdb_revenue': safe_value(row.get('tmdb_revenue')),
        
        # OMDB data
        'omdb_awards': safe_value(row.get('omdb_awards')),
        'omdb_metascore': safe_value(row.get('omdb_metascore')),
        'omdb_rotten_tomatoes': safe_value(row.get('omdb_rotten_tomatoes')),
        'omdb_rated': safe_value(row.get('omdb_rated')),
        'omdb_language': safe_value(row.get('omdb_language')),
        'omdb_country': safe_value(row.get('omdb_country')),
        'countries': safe_value(row.get('omdb_country') or row.get('production_countries')),
        'awards': safe_value(row.get('omdb_awards')),
        
        # DDD (Does the Dog Die) data
        'ddd_dog_dies': safe_value(row.get('ddd_dog_dies')),
        'ddd_dog_survives': safe_value(row.get('ddd_dog_survives')),
        
        # Wikidata
        'wd_box_office': safe_value(row.get('wd_box_office')),
        'wd_filming_location': safe_value(row.get('wd_filming_location')),
        
        # Cast
        'cast': safe_value(row.get('cast')),
        'numVotes': safe_value(row.get('Num Votes') or row.get('numVotes')),
        'dateRated': safe_value(row.get('Date Rated') or row.get('date_rated')),
    }

@app.route('/')
def index():
    """API root - health check"""
    return jsonify({
        'status': 'ok',
        'name': 'CineScope API',
        'version': '1.0.0',
        'movies_loaded': len(movies_df) if movies_df is not None else 0,
        'people_loaded': len(people_cache) if people_cache else 0,
    })

@app.route('/api/stats')
def get_stats():
    """Get collection statistics"""
    if movies_df is None or movies_df.empty:
        return jsonify({
            'totalMovies': 0,
            'avgRating': 0,
            'totalHours': 0,
            'topYear': None,
        })
    
    # Calculate stats
    rating_col = 'IMDb Rating' if 'IMDb Rating' in movies_df.columns else 'imdb_rating'
    year_col = 'Year' if 'Year' in movies_df.columns else 'year'
    
    # Try multiple runtime column names
    runtime_col = None
    for col in ['Runtime (mins)', 'runtime_mins', 'runtime', 'tmdb_runtime']:
        if col in movies_df.columns:
            runtime_col = col
            break
    
    ratings = pd.to_numeric(movies_df[rating_col], errors='coerce')
    runtimes = pd.to_numeric(movies_df[runtime_col], errors='coerce') if runtime_col else pd.Series()
    years = movies_df.get(year_col, pd.Series())
    
    # Find top year
    top_year = None
    if not years.empty:
        year_counts = years.value_counts()
        if not year_counts.empty:
            top_year = int(year_counts.index[0])
    
    return jsonify({
        'totalMovies': len(movies_df),
        'avgRating': round(ratings.mean(), 2) if not ratings.empty else 0,
        'totalHours': round(runtimes.sum() / 60, 1) if not runtimes.empty else 0,
        'topYear': top_year,
        'totalGenres': len(get_unique_genres()),
    })

def get_unique_genres():
    """Extract unique genres from the dataframe"""
    genre_col = 'Genres' if 'Genres' in movies_df.columns else 'genres'
    if genre_col not in movies_df.columns:
        return []
    
    all_genres = set()
    for genres in movies_df[genre_col].dropna():
        if isinstance(genres, str):
            for g in genres.split(','):
                all_genres.add(g.strip())
    return sorted(all_genres)

@app.route('/api/stats/genres')
def get_genre_stats():
    """Get genre statistics"""
    if movies_df is None or movies_df.empty:
        return jsonify([])
    
    genre_col = 'Genres' if 'Genres' in movies_df.columns else 'genres'
    rating_col = 'IMDb Rating' if 'IMDb Rating' in movies_df.columns else 'imdb_rating'
    
    genre_stats = {}
    for idx, row in movies_df.iterrows():
        genres = row.get(genre_col, '')
        rating = row.get(rating_col)
        
        if isinstance(genres, str):
            for g in genres.split(','):
                g = g.strip()
                if g:
                    if g not in genre_stats:
                        genre_stats[g] = {'count': 0, 'ratings': []}
                    genre_stats[g]['count'] += 1
                    if pd.notna(rating):
                        genre_stats[g]['ratings'].append(float(rating))
    
    result = []
    for genre, stats in genre_stats.items():
        avg_rating = np.mean(stats['ratings']) if stats['ratings'] else 0
        result.append({
            'genre': genre,
            'count': stats['count'],
            'avgRating': round(avg_rating, 2)
        })
    
    return jsonify(sorted(result, key=lambda x: x['count'], reverse=True))

@app.route('/api/stats/decades')
def get_decade_stats():
    """Get decade statistics"""
    if movies_df is None or movies_df.empty:
        return jsonify([])
    
    year_col = 'Year' if 'Year' in movies_df.columns else 'year'
    rating_col = 'IMDb Rating' if 'IMDb Rating' in movies_df.columns else 'imdb_rating'
    
    decade_stats = {}
    for idx, row in movies_df.iterrows():
        year = row.get(year_col)
        rating = row.get(rating_col)
        
        if pd.notna(year):
            try:
                decade = int(year) // 10 * 10
                if decade not in decade_stats:
                    decade_stats[decade] = {'count': 0, 'ratings': []}
                decade_stats[decade]['count'] += 1
                if pd.notna(rating):
                    decade_stats[decade]['ratings'].append(float(rating))
            except (ValueError, TypeError):
                pass
    
    result = []
    for decade, stats in sorted(decade_stats.items()):
        avg_rating = np.mean(stats['ratings']) if stats['ratings'] else 0
        result.append({
            'decade': str(decade),
            'count': stats['count'],
            'avgRating': round(avg_rating, 2)
        })
    
    return jsonify(result)

@app.route('/api/stats/directors')
def get_director_stats():
    """Get director statistics"""
    if movies_df is None or movies_df.empty:
        return jsonify([])
    
    director_col = 'Directors' if 'Directors' in movies_df.columns else 'directors'
    rating_col = 'IMDb Rating' if 'IMDb Rating' in movies_df.columns else 'imdb_rating'
    
    director_stats = {}
    for idx, row in movies_df.iterrows():
        directors = row.get(director_col, '')
        rating = row.get(rating_col)
        
        if isinstance(directors, str):
            for d in directors.split(','):
                d = d.strip()
                if d:
                    if d not in director_stats:
                        director_stats[d] = {'movieCount': 0, 'ratings': []}
                    director_stats[d]['movieCount'] += 1
                    if pd.notna(rating):
                        director_stats[d]['ratings'].append(float(rating))
    
    result = []
    for name, stats in director_stats.items():
        avg_rating = np.mean(stats['ratings']) if stats['ratings'] else 0
        result.append({
            'name': name,
            'movieCount': stats['movieCount'],
            'avgRating': round(avg_rating, 2)
        })
    
    return jsonify(sorted(result, key=lambda x: x['movieCount'], reverse=True)[:50])

@app.route('/api/movies')
def get_movies():
    """Get paginated movie list with filtering"""
    if movies_df is None or movies_df.empty:
        return jsonify({
            'results': [],
            'total': 0,
            'page': 1,
            'pageSize': 24,
            'totalPages': 0,
        })
    
    # Get query parameters
    page = request.args.get('page', 1, type=int)
    page_size = request.args.get('pageSize', 24, type=int)
    search = request.args.get('search', '')
    genre = request.args.get('genre', '')
    min_year = request.args.get('minYear', type=int)
    max_year = request.args.get('maxYear', type=int)
    min_rating = request.args.get('minRating', type=float)
    sort_by = request.args.get('sortBy', 'rating')
    sort_order = request.args.get('sortOrder', 'desc')
    
    # Column names
    title_col = 'Title' if 'Title' in movies_df.columns else 'title'
    genre_col = 'Genres' if 'Genres' in movies_df.columns else 'genres'
    year_col = 'Year' if 'Year' in movies_df.columns else 'year'
    rating_col = 'IMDb Rating' if 'IMDb Rating' in movies_df.columns else 'imdb_rating'
    
    # Start with all movies
    filtered_df = movies_df.copy()
    
    # Apply filters
    if search:
        search_lower = search.lower()
        mask = filtered_df[title_col].fillna('').str.lower().str.contains(search_lower, na=False)
        filtered_df = filtered_df[mask]
    
    if genre:
        mask = filtered_df[genre_col].fillna('').str.contains(genre, case=False, na=False)
        filtered_df = filtered_df[mask]
    
    if min_year:
        filtered_df = filtered_df[pd.to_numeric(filtered_df[year_col], errors='coerce') >= min_year]
    
    if max_year:
        filtered_df = filtered_df[pd.to_numeric(filtered_df[year_col], errors='coerce') <= max_year]
    
    if min_rating:
        filtered_df = filtered_df[pd.to_numeric(filtered_df[rating_col], errors='coerce') >= min_rating]
    
    # Sorting
    sort_col_map = {
        'rating': rating_col,
        'year': year_col,
        'title': title_col,
    }
    sort_col = sort_col_map.get(sort_by, rating_col)
    ascending = sort_order == 'asc'
    
    try:
        filtered_df = filtered_df.sort_values(
            by=sort_col, 
            ascending=ascending, 
            na_position='last'
        )
    except Exception as e:
        logger.warning(f"Sort error: {e}")
    
    # Pagination
    total = len(filtered_df)
    total_pages = (total + page_size - 1) // page_size
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    
    page_df = filtered_df.iloc[start_idx:end_idx]
    
    # Convert to list of dicts
    results = [movie_to_dict(row) for _, row in page_df.iterrows()]
    
    return jsonify({
        'results': results,
        'total': total,
        'page': page,
        'pageSize': page_size,
        'totalPages': total_pages,
    })

@app.route('/api/movies/<movie_id>')
def get_movie(movie_id):
    """Get single movie by ID"""
    if movies_df is None or movies_df.empty:
        return jsonify({'error': 'Movie not found'}), 404
    
    id_col = 'Const' if 'Const' in movies_df.columns else 'const'
    if id_col not in movies_df.columns:
        id_col = 'imdb_id' if 'imdb_id' in movies_df.columns else 'tconst'
    
    movie = movies_df[movies_df[id_col] == movie_id]
    
    if movie.empty:
        return jsonify({'error': 'Movie not found'}), 404
    
    return jsonify(movie_to_dict(movie.iloc[0]))

@app.route('/api/movies/<movie_id>/similar')
def get_similar_movies(movie_id):
    """Get similar movies based on genres and rating"""
    if movies_df is None or movies_df.empty:
        return jsonify([])
    
    id_col = 'Const' if 'Const' in movies_df.columns else 'const'
    genre_col = 'Genres' if 'Genres' in movies_df.columns else 'genres'
    rating_col = 'IMDb Rating' if 'IMDb Rating' in movies_df.columns else 'imdb_rating'
    
    movie = movies_df[movies_df[id_col] == movie_id]
    if movie.empty:
        return jsonify([])
    
    movie = movie.iloc[0]
    movie_genres = set()
    if isinstance(movie.get(genre_col), str):
        movie_genres = set(g.strip() for g in movie[genre_col].split(','))
    
    # Find movies with similar genres
    similar_scores = []
    for idx, row in movies_df.iterrows():
        if row.get(id_col) == movie_id:
            continue
        
        row_genres = set()
        if isinstance(row.get(genre_col), str):
            row_genres = set(g.strip() for g in row[genre_col].split(','))
        
        # Calculate similarity score (Jaccard similarity)
        if movie_genres and row_genres:
            intersection = len(movie_genres & row_genres)
            union = len(movie_genres | row_genres)
            score = intersection / union if union > 0 else 0
        else:
            score = 0
        
        if score > 0:
            similar_scores.append((idx, score))
    
    # Sort by score and get top 10
    similar_scores.sort(key=lambda x: x[1], reverse=True)
    top_similar = similar_scores[:10]
    
    results = [movie_to_dict(movies_df.iloc[idx]) for idx, _ in top_similar]
    return jsonify(results)

@app.route('/api/movies/search')
def search_movies():
    """Search movies by title"""
    query = request.args.get('q', '')
    limit = request.args.get('limit', 20, type=int)
    
    if not query or movies_df is None or movies_df.empty:
        return jsonify([])
    
    title_col = 'Title' if 'Title' in movies_df.columns else 'title'
    
    query_lower = query.lower()
    matches = movies_df[movies_df[title_col].fillna('').str.lower().str.contains(query_lower, na=False)]
    
    results = [movie_to_dict(row) for _, row in matches.head(limit).iterrows()]
    return jsonify(results)

@app.route('/api/people')
def get_people():
    """Get paginated people list"""
    if not people_cache:
        return jsonify({
            'results': [],
            'total': 0,
            'page': 1,
            'totalPages': 0,
        })
    
    page = request.args.get('page', 1, type=int)
    page_size = request.args.get('pageSize', 100, type=int)
    search = request.args.get('q', '').lower()
    role = request.args.get('role', 'all')  # all, director, actor, writer
    
    # Transform people cache to list with proper structure
    people_list = []
    for tmdb_id, person in people_cache.items():
        name = person.get('imdb_name') or person.get('primaryName', '')
        known_for = person.get('known_for_department') or person.get('imdb_profession', '')
        
        # Filter by search
        if search and search not in name.lower():
            continue
        
        # Filter by role
        if role != 'all':
            role_lower = role.lower()
            known_for_lower = known_for.lower() if known_for else ''
            if role_lower == 'director' and 'direct' not in known_for_lower:
                continue
            elif role_lower == 'actor' and 'act' not in known_for_lower:
                continue
            elif role_lower == 'writer' and 'writ' not in known_for_lower:
                continue
        
        people_list.append({
            'id': tmdb_id,
            'name': name,
            'profilePath': person.get('profile_path'),
            'knownFor': known_for,
            'birthYear': person.get('imdb_birth_year') or (person.get('birthday', '')[:4] if person.get('birthday') else None),
            'biography': person.get('biography', '')[:200] + '...' if person.get('biography') and len(person.get('biography', '')) > 200 else person.get('biography'),
            'birthplace': person.get('wd_birthplace') or person.get('place_of_birth'),
            'imdbId': person.get('imdb_id'),
        })
    
    # Pagination
    total = len(people_list)
    total_pages = max(1, (total + page_size - 1) // page_size)
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    
    page_people = people_list[start_idx:end_idx]
    
    return jsonify({
        'results': page_people,
        'total': total,
        'page': page,
        'totalPages': total_pages,
    })

@app.route('/api/people/<person_id>')
def get_person(person_id):
    """Get person by ID"""
    if not people_cache:
        return jsonify({'error': 'Person not found'}), 404
    
    person = people_cache.get(person_id)
    if not person:
        return jsonify({'error': 'Person not found'}), 404
    
    # Transform to API format
    name = person.get('imdb_name') or person.get('primaryName', '')
    known_for = person.get('known_for_department') or person.get('imdb_profession', '')
    
    return jsonify({
        'id': person_id,
        'name': name,
        'profilePath': person.get('profile_path'),
        'knownFor': known_for,
        'birthYear': person.get('imdb_birth_year') or (person.get('birthday', '')[:4] if person.get('birthday') else None),
        'biography': person.get('biography'),
        'birthplace': person.get('wd_birthplace') or person.get('place_of_birth'),
        'imdbId': person.get('imdb_id'),
        'deathYear': person.get('imdb_death_year'),
        'nationality': person.get('wd_nationality'),
        'awardsCount': person.get('wd_awards_count'),
        'wikipediaUrl': person.get('wd_wikipedia_url'),
    })

@app.route('/api/people/<person_id>/filmography')
def get_person_filmography(person_id):
    """Get filmography for a person - movies in the user's collection featuring this person"""
    if movies_df is None or movies_df.empty:
        return jsonify([])
    
    # Get person name to search
    person = people_cache.get(person_id, {})
    name = person.get('imdb_name') or person.get('primaryName', '')
    
    if not name:
        return jsonify([])
    
    # Search for movies with this person in cast or directors
    results = []
    for _, row in movies_df.iterrows():
        cast = str(row.get('cast', '') or row.get('Cast', '') or '')
        directors = str(row.get('directors', '') or row.get('Directors', '') or row.get('Director', '') or '')
        
        if name.lower() in cast.lower() or name.lower() in directors.lower():
            results.append(movie_to_dict(row))
    
    return jsonify(results)

@app.route('/api/people/search')
def search_people():
    """Search people by name"""
    query = request.args.get('q', '')
    limit = request.args.get('limit', 20, type=int)
    
    if not query or not people_cache:
        return jsonify([])
    
    query_lower = query.lower()
    matches = [
        p for p in people_cache.values()
        if query_lower in (p.get('primaryName', '') or '').lower()
    ][:limit]
    
    return jsonify(matches)

@app.route('/api/recommendations')
def get_recommendations():
    """Get personalized recommendations"""
    if movies_df is None or movies_df.empty:
        return jsonify([])
    
    rating_col = 'IMDb Rating' if 'IMDb Rating' in movies_df.columns else 'imdb_rating'
    your_rating_col = 'Your Rating' if 'Your Rating' in movies_df.columns else 'your_rating'
    
    # Get top rated movies as recommendations
    top_rated = movies_df.nlargest(20, rating_col)
    
    recommendations = [{
        'title': 'Top Rated in Your Collection',
        'description': 'Movies with the highest ratings',
        'recommendations': [
            {
                'movie': movie_to_dict(row),
                'reason': f'Rated {row.get(rating_col)}/10'
            }
            for _, row in top_rated.iterrows()
        ]
    }]
    
    # Add genre-based recommendations if we can
    genre_col = 'Genres' if 'Genres' in movies_df.columns else 'genres'
    genre_counts = {}
    for genres in movies_df[genre_col].dropna():
        if isinstance(genres, str):
            for g in genres.split(','):
                g = g.strip()
                genre_counts[g] = genre_counts.get(g, 0) + 1
    
    if genre_counts:
        top_genre = max(genre_counts, key=genre_counts.get)
        genre_movies = movies_df[movies_df[genre_col].fillna('').str.contains(top_genre, na=False)]
        genre_movies = genre_movies.nlargest(10, rating_col)
        
        recommendations.append({
            'title': f'Best {top_genre} Movies',
            'description': f'Your favorite genre with {genre_counts[top_genre]} movies',
            'recommendations': [
                {
                    'movie': movie_to_dict(row),
                    'reason': f'{top_genre} • {row.get(rating_col)}/10'
                }
                for _, row in genre_movies.iterrows()
            ]
        })
    
    return jsonify(recommendations)

@app.route('/api/recommendations/gaps')
def get_gaps():
    """Get collection gap recommendations"""
    if movies_df is None or movies_df.empty:
        return jsonify([])
    
    genre_col = 'Genres' if 'Genres' in movies_df.columns else 'genres'
    
    # Find genres with few movies
    genre_counts = {}
    for genres in movies_df[genre_col].dropna():
        if isinstance(genres, str):
            for g in genres.split(','):
                g = g.strip()
                genre_counts[g] = genre_counts.get(g, 0) + 1
    
    # Get underrepresented genres (less than 10% of collection)
    threshold = len(movies_df) * 0.05
    gaps = []
    
    for genre, count in sorted(genre_counts.items(), key=lambda x: x[1]):
        if count < threshold and count > 0:
            genre_movies = movies_df[movies_df[genre_col].fillna('').str.contains(genre, na=False)]
            genre_movies = genre_movies.head(5)
            
            if not genre_movies.empty:
                gaps.append({
                    'title': f'{genre} Cinema',
                    'description': f'You have only {count} {genre} movies',
                    'recommendations': [
                        {
                            'movie': movie_to_dict(row),
                            'reason': f'One of your {count} {genre} films'
                        }
                        for _, row in genre_movies.iterrows()
                    ]
                })
        
        if len(gaps) >= 4:
            break
    
    return jsonify(gaps)

@app.route('/api/filters/genres')
def get_genre_options():
    """Get list of all genres for filtering"""
    return jsonify(get_unique_genres())

@app.route('/api/filters/years')
def get_year_range():
    """Get min and max years for filtering"""
    if movies_df is None or movies_df.empty:
        return jsonify({'min': 1900, 'max': 2025})
    
    year_col = 'Year' if 'Year' in movies_df.columns else 'year'
    years = pd.to_numeric(movies_df[year_col], errors='coerce').dropna()
    
    return jsonify({
        'min': int(years.min()) if not years.empty else 1900,
        'max': int(years.max()) if not years.empty else 2025,
    })

@app.route('/api/genres')
def get_genres():
    """Alias for genre options (compatibility)"""
    return jsonify({'genres': get_unique_genres()})

@app.route('/api/insights')
def get_insights():
    """Get comprehensive insights from the data analysis"""
    if movies_df is None or movies_df.empty:
        return jsonify({'error': 'No data available'}), 404
    
    # Column name mapping
    rating_col = 'IMDb Rating' if 'IMDb Rating' in movies_df.columns else 'imdb_rating'
    your_rating_col = 'Your Rating' if 'Your Rating' in movies_df.columns else 'your_rating'
    year_col = 'Year' if 'Year' in movies_df.columns else 'year'
    genre_col = 'Genres' if 'Genres' in movies_df.columns else 'genres'
    runtime_col = next((c for c in ['Runtime (mins)', 'runtime', 'runtime_mins', 'runtimeMinutes'] if c in movies_df.columns), None)
    director_col = next((c for c in ['Directors', 'directors', 'Director'] if c in movies_df.columns), None)
    cast_col = next((c for c in ['Cast', 'cast', 'Actors'] if c in movies_df.columns), None)
    
    insights = {
        'summary': {},
        'genres': [],
        'decades': [],
        'directors': [],
        'actors': [],
        'ratings': {},
        'patterns': [],
    }
    
    # Summary stats
    total_movies = len(movies_df)
    insights['summary'] = {
        'totalMovies': total_movies,
        'totalHours': int(movies_df[runtime_col].sum() / 60) if runtime_col else 0,
        'avgRating': round(movies_df[rating_col].mean(), 2) if rating_col else 0,
        'yearSpan': f"{int(movies_df[year_col].min())}-{int(movies_df[year_col].max())}" if year_col else '',
    }
    
    # Genre insights
    genre_stats = {}
    for genres in movies_df[genre_col].dropna():
        if isinstance(genres, str):
            for g in genres.split(','):
                g = g.strip()
                if g not in genre_stats:
                    genre_stats[g] = {'count': 0, 'ratings': []}
                genre_stats[g]['count'] += 1
    
    # Get avg rating per genre
    for g in genre_stats:
        genre_movies = movies_df[movies_df[genre_col].fillna('').str.contains(g, na=False)]
        genre_stats[g]['avgRating'] = round(genre_movies[rating_col].mean(), 2)
    
    insights['genres'] = sorted([
        {'name': g, 'count': s['count'], 'avgRating': s['avgRating'], 'percentage': round(s['count']/total_movies*100, 1)}
        for g, s in genre_stats.items()
    ], key=lambda x: x['count'], reverse=True)[:15]
    
    # Decade insights
    movies_df['decade'] = (movies_df[year_col] // 10 * 10).astype(int)
    decade_stats = movies_df.groupby('decade').agg({
        rating_col: 'mean',
        year_col: 'count'
    }).reset_index()
    
    insights['decades'] = [
        {'decade': f"{int(row['decade'])}s", 'count': int(row[year_col]), 'avgRating': round(row[rating_col], 2)}
        for _, row in decade_stats.iterrows()
    ]
    
    # Director insights
    if director_col:
        director_counts = {}
        for directors in movies_df[director_col].dropna():
            for d in str(directors).split(','):
                d = d.strip()
                if d and d != 'nan':
                    director_counts[d] = director_counts.get(d, 0) + 1
        
        # Get avg rating per director
        director_insights = []
        for d, count in sorted(director_counts.items(), key=lambda x: x[1], reverse=True)[:20]:
            director_movies = movies_df[movies_df[director_col].fillna('').str.contains(d, na=False, regex=False)]
            avg_rating = round(director_movies[rating_col].mean(), 2)
            director_insights.append({
                'name': d,
                'count': count,
                'avgRating': avg_rating
            })
        insights['directors'] = director_insights
    
    # Actor insights
    if cast_col:
        actor_counts = {}
        for cast in movies_df[cast_col].dropna():
            for a in str(cast).split(',')[:5]:  # Only first 5 actors
                a = a.strip()
                if a and a != 'nan':
                    actor_counts[a] = actor_counts.get(a, 0) + 1
        
        # Get avg rating per actor
        actor_insights = []
        for a, count in sorted(actor_counts.items(), key=lambda x: x[1], reverse=True)[:20]:
            actor_movies = movies_df[movies_df[cast_col].fillna('').str.contains(a, na=False, regex=False)]
            avg_rating = round(actor_movies[rating_col].mean(), 2)
            actor_insights.append({
                'name': a,
                'count': count,
                'avgRating': avg_rating
            })
        insights['actors'] = actor_insights
    
    # Rating distribution
    rating_dist = movies_df[rating_col].value_counts().sort_index()
    insights['ratings'] = {
        'distribution': [{'rating': float(r), 'count': int(c)} for r, c in rating_dist.items()],
        'average': round(movies_df[rating_col].mean(), 2),
        'median': round(movies_df[rating_col].median(), 2),
    }
    
    # Interesting patterns
    patterns = []
    
    # Most prolific decade
    top_decade = decade_stats.loc[decade_stats[year_col].idxmax()]
    patterns.append({
        'type': 'decade',
        'title': 'Most Active Era',
        'value': f"{int(top_decade['decade'])}s",
        'detail': f"{int(top_decade[year_col])} movies watched from this decade"
    })
    
    # Favorite genre
    top_genre = insights['genres'][0]
    patterns.append({
        'type': 'genre',
        'title': 'Favorite Genre',
        'value': top_genre['name'],
        'detail': f"{top_genre['count']} movies ({top_genre['percentage']}% of collection)"
    })
    
    # Highest rated genre
    highest_rated_genre = max(insights['genres'], key=lambda x: x['avgRating'])
    patterns.append({
        'type': 'quality',
        'title': 'Highest Rated Genre',
        'value': highest_rated_genre['name'],
        'detail': f"Average rating: {highest_rated_genre['avgRating']}/10"
    })
    
    # Top director
    if insights['directors']:
        top_director = insights['directors'][0]
        patterns.append({
            'type': 'director',
            'title': 'Most Watched Director',
            'value': top_director['name'],
            'detail': f"{top_director['count']} films (avg rating: {top_director['avgRating']})"
        })
    
    # Top actor
    if insights['actors']:
        top_actor = insights['actors'][0]
        patterns.append({
            'type': 'actor',
            'title': 'Most Watched Actor',
            'value': top_actor['name'],
            'detail': f"Appears in {top_actor['count']} films"
        })
    
    # Runtime preference
    if runtime_col:
        avg_runtime = movies_df[runtime_col].mean()
        patterns.append({
            'type': 'runtime',
            'title': 'Average Runtime Preference',
            'value': f"{int(avg_runtime)} minutes",
            'detail': f"~{int(avg_runtime/60)}h {int(avg_runtime%60)}m average movie length"
        })
    
    insights['patterns'] = patterns
    
    return jsonify(insights)


# ============================================================
# ACTOR ANALYTICS ENDPOINTS
# ============================================================

@app.route('/api/people/<person_id>/analytics')
def get_person_analytics(person_id):
    """Get detailed analytics for a specific person (actor/director)"""
    if not people_cache or movies_df is None or movies_df.empty:
        return jsonify({'error': 'Data not available'}), 404
    
    person = people_cache.get(person_id)
    if not person:
        return jsonify({'error': 'Person not found'}), 404
    
    name = person.get('imdb_name') or person.get('primaryName', '')
    if not name:
        return jsonify({'error': 'Person name not found'}), 404
    
    # Get column names
    rating_col = 'IMDb Rating' if 'IMDb Rating' in movies_df.columns else 'imdb_rating'
    year_col = 'Year' if 'Year' in movies_df.columns else 'year'
    genre_col = 'Genres' if 'Genres' in movies_df.columns else 'genres'
    director_col = 'Directors' if 'Directors' in movies_df.columns else 'directors'
    runtime_col = None
    for col in ['Runtime (mins)', 'runtime_mins', 'runtime', 'tmdb_runtime']:
        if col in movies_df.columns:
            runtime_col = col
            break
    
    # Find all movies with this person
    person_movies = []
    for _, row in movies_df.iterrows():
        # Parse tmdb_cast for actor check
        tmdb_cast = row.get('tmdb_cast', '')
        cast_names = []
        if tmdb_cast and isinstance(tmdb_cast, str) and tmdb_cast.strip():
            try:
                import ast
                cast_list = ast.literal_eval(tmdb_cast)
                cast_names = [c.get('name', '').lower() for c in cast_list if isinstance(c, dict)]
            except (ValueError, SyntaxError):
                pass
        
        directors = str(row.get(director_col, '') or '')
        writers = str(row.get('tmdb_writers', '') or row.get('writers', '') or '')
        
        is_actor = name.lower() in cast_names
        is_director = name.lower() in directors.lower()
        is_writer = name.lower() in writers.lower()
        
        if is_actor or is_director or is_writer:
            movie_data = {
                'title': row.get('Title') or row.get('title'),
                'year': safe_value(row.get(year_col)),
                'rating': safe_value(row.get(rating_col)),
                'genres': row.get(genre_col, ''),
                'runtime': safe_value(row.get(runtime_col)) if runtime_col else None,
                'role': 'director' if is_director else ('writer' if is_writer and not is_actor else 'actor'),
                'const': row.get('Const') or row.get('const') or row.get('imdb_id'),
                'poster': row.get('tmdb_poster_path'),
            }
            person_movies.append(movie_data)
    
    if not person_movies:
        return jsonify({
            'person': {
                'id': person_id,
                'name': name,
                'knownFor': person.get('known_for_department') or person.get('imdb_profession', ''),
            },
            'filmCount': 0,
            'analytics': {}
        })
    
    # Calculate analytics
    ratings = [m['rating'] for m in person_movies if m['rating'] is not None]
    years = [m['year'] for m in person_movies if m['year'] is not None]
    runtimes = [m['runtime'] for m in person_movies if m['runtime'] is not None]
    
    # Genre distribution
    genre_counts = {}
    for m in person_movies:
        genres_str = m.get('genres', '')
        if isinstance(genres_str, str):
            for g in genres_str.split(','):
                g = g.strip()
                if g:
                    genre_counts[g] = genre_counts.get(g, 0) + 1
    
    genre_distribution = sorted(
        [{'genre': g, 'count': c, 'percentage': round(c / len(person_movies) * 100, 1)} 
         for g, c in genre_counts.items()],
        key=lambda x: x['count'],
        reverse=True
    )[:10]
    
    # Decade distribution
    decade_counts = {}
    decade_ratings = {}
    for m in person_movies:
        if m['year']:
            try:
                decade = int(m['year']) // 10 * 10
                decade_counts[decade] = decade_counts.get(decade, 0) + 1
                if m['rating']:
                    if decade not in decade_ratings:
                        decade_ratings[decade] = []
                    decade_ratings[decade].append(m['rating'])
            except (ValueError, TypeError):
                pass
    
    decade_distribution = sorted([
        {
            'decade': str(d) + 's',
            'count': c,
            'avgRating': round(np.mean(decade_ratings.get(d, [0])), 1) if decade_ratings.get(d) else None
        }
        for d, c in decade_counts.items()
    ], key=lambda x: x['decade'])
    
    # Career timeline (movies per year with ratings)
    year_data = {}
    for m in person_movies:
        if m['year']:
            try:
                y = int(m['year'])
                if y not in year_data:
                    year_data[y] = {'count': 0, 'ratings': [], 'movies': []}
                year_data[y]['count'] += 1
                if m['rating']:
                    year_data[y]['ratings'].append(m['rating'])
                year_data[y]['movies'].append({
                    'title': m['title'],
                    'rating': m['rating'],
                    'const': m['const']
                })
            except (ValueError, TypeError):
                pass
    
    career_timeline = sorted([
        {
            'year': y,
            'count': data['count'],
            'avgRating': round(np.mean(data['ratings']), 1) if data['ratings'] else None,
            'movies': data['movies']
        }
        for y, data in year_data.items()
    ], key=lambda x: x['year'])
    
    # Rating distribution
    rating_distribution = {}
    for r in ratings:
        bucket = int(r)
        rating_distribution[bucket] = rating_distribution.get(bucket, 0) + 1
    
    rating_dist_list = [
        {'rating': r, 'count': rating_distribution.get(r, 0)}
        for r in range(1, 11)
    ]
    
    # Collaborations (directors worked with most if actor, actors worked with most if director)
    collaborations = {}
    known_for = (person.get('known_for_department') or person.get('imdb_profession', '')).lower()
    is_primarily_director = 'direct' in known_for
    
    for _, row in movies_df.iterrows():
        # Parse tmdb_cast
        tmdb_cast = row.get('tmdb_cast', '')
        cast_names = []
        if tmdb_cast and isinstance(tmdb_cast, str) and tmdb_cast.strip():
            try:
                import ast
                cast_list = ast.literal_eval(tmdb_cast)
                cast_names = [c.get('name', '') for c in cast_list if isinstance(c, dict)]
            except (ValueError, SyntaxError):
                pass
        
        directors = str(row.get(director_col, '') or '')
        
        # Check if this person is in the movie
        in_cast = name.lower() in [c.lower() for c in cast_names]
        in_directors = name.lower() in directors.lower()
        if not in_cast and not in_directors:
            continue
        
        # Find collaborators
        if is_primarily_director:
            # Find actors they've directed
            for actor_name in cast_names:
                if actor_name and actor_name.lower() != name.lower():
                    collaborations[actor_name] = collaborations.get(actor_name, 0) + 1
        else:
            # Find directors they've worked with
            for director in directors.split(','):
                director = director.strip()
                if director and director.lower() != name.lower():
                    collaborations[director] = collaborations.get(director, 0) + 1
    
    top_collaborations = sorted(
        [{'name': n, 'count': c} for n, c in collaborations.items()],
        key=lambda x: x['count'],
        reverse=True
    )[:10]
    
    # Best and worst rated films
    sorted_by_rating = sorted(
        [m for m in person_movies if m['rating']],
        key=lambda x: x['rating'],
        reverse=True
    )
    
    best_films = sorted_by_rating[:5]
    worst_films = sorted_by_rating[-5:][::-1] if len(sorted_by_rating) >= 5 else []
    
    # Career span
    career_span = None
    if years:
        career_span = {
            'start': min(years),
            'end': max(years),
            'years': max(years) - min(years) + 1
        }
    
    return jsonify({
        'person': {
            'id': person_id,
            'name': name,
            'knownFor': person.get('known_for_department') or person.get('imdb_profession', ''),
            'profilePath': person.get('profile_path'),
            'biography': person.get('biography'),
            'birthYear': person.get('imdb_birth_year') or (person.get('birthday', '')[:4] if person.get('birthday') else None),
            'birthplace': person.get('wd_birthplace') or person.get('place_of_birth'),
            'awardsCount': person.get('wd_awards_count'),
            'wikipediaUrl': person.get('wd_wikipedia_url'),
            'imdbId': person.get('imdb_id'),
        },
        'filmCount': len(person_movies),
        'analytics': {
            'avgRating': round(np.mean(ratings), 2) if ratings else None,
            'highestRating': max(ratings) if ratings else None,
            'lowestRating': min(ratings) if ratings else None,
            'totalRuntime': sum(runtimes) if runtimes else 0,
            'avgRuntime': round(np.mean(runtimes), 0) if runtimes else None,
            'careerSpan': career_span,
            'genreDistribution': genre_distribution,
            'decadeDistribution': decade_distribution,
            'careerTimeline': career_timeline,
            'ratingDistribution': rating_dist_list,
            'topCollaborations': top_collaborations,
            'bestFilms': best_films,
            'worstFilms': worst_films,
            'roleBreakdown': {
                'actor': len([m for m in person_movies if m['role'] == 'actor']),
                'director': len([m for m in person_movies if m['role'] == 'director']),
                'writer': len([m for m in person_movies if m['role'] == 'writer']),
            }
        },
        'films': sorted(person_movies, key=lambda x: x['year'] or 0, reverse=True)
    })


@app.route('/api/analytics/actors')
def get_actors_analytics():
    """Get analytics for top actors in the collection"""
    if movies_df is None or movies_df.empty:
        return jsonify({'actors': [], 'summary': {}})
    
    # Get column names
    rating_col = 'IMDb Rating' if 'IMDb Rating' in movies_df.columns else 'imdb_rating'
    
    # Build actor statistics from tmdb_cast data (which contains structured cast info)
    actor_stats = {}
    
    for _, row in movies_df.iterrows():
        # Parse tmdb_cast which is a JSON-like string
        tmdb_cast = row.get('tmdb_cast', '')
        rating = row.get(rating_col)
        year = row.get('Year') or row.get('year')
        genres = row.get('Genres') or row.get('genres') or ''
        
        # Parse the cast data
        cast_list = []
        if tmdb_cast and isinstance(tmdb_cast, str) and tmdb_cast.strip():
            try:
                import ast
                cast_list = ast.literal_eval(tmdb_cast)
            except (ValueError, SyntaxError):
                pass
        
        for cast_member in cast_list:
            if not isinstance(cast_member, dict):
                continue
            actor_name = cast_member.get('name', '').strip()
            if not actor_name:
                continue
            
            if actor_name not in actor_stats:
                actor_stats[actor_name] = {
                    'name': actor_name,
                    'filmCount': 0,
                    'ratings': [],
                    'years': [],
                    'genres': [],
                    'tmdb_person_id': cast_member.get('tmdb_person_id'),
                    'profile_path': cast_member.get('profile_path'),
                }
            
            actor_stats[actor_name]['filmCount'] += 1
            if pd.notna(rating):
                actor_stats[actor_name]['ratings'].append(float(rating))
            if pd.notna(year):
                try:
                    actor_stats[actor_name]['years'].append(int(year))
                except (ValueError, TypeError):
                    pass
            if isinstance(genres, str):
                actor_stats[actor_name]['genres'].extend([g.strip() for g in genres.split(',') if g.strip()])
    
    # Process and sort actors
    processed_actors = []
    for name, stats in actor_stats.items():
        if stats['filmCount'] < 2:  # Only include actors with 2+ films
            continue
        
        # Use TMDB ID from cast data if available
        tmdb_id = str(stats.get('tmdb_person_id', '')) if stats.get('tmdb_person_id') else None
        profile_path = stats.get('profile_path')
        
        # If not found in cast, try people_cache
        if not tmdb_id:
            for pid, pdata in people_cache.items():
                pname = pdata.get('imdb_name') or pdata.get('primaryName', '')
                if pname.lower() == name.lower():
                    tmdb_id = pid
                    profile_path = profile_path or pdata.get('profile_path')
                    break
        
        # Get top genre
        genre_counts = {}
        for g in stats['genres']:
            genre_counts[g] = genre_counts.get(g, 0) + 1
        top_genre = max(genre_counts, key=genre_counts.get) if genre_counts else None
        
        processed_actors.append({
            'id': tmdb_id,
            'name': name,
            'profilePath': profile_path,
            'filmCount': stats['filmCount'],
            'avgRating': round(np.mean(stats['ratings']), 2) if stats['ratings'] else None,
            'highestRating': max(stats['ratings']) if stats['ratings'] else None,
            'careerSpan': f"{min(stats['years'])}-{max(stats['years'])}" if stats['years'] else None,
            'topGenre': top_genre,
        })
    
    # Sort by film count
    processed_actors.sort(key=lambda x: (x['filmCount'], x['avgRating'] or 0), reverse=True)
    
    # Get top 50
    top_actors = processed_actors[:50]
    
    # Calculate summary statistics
    all_film_counts = [a['filmCount'] for a in processed_actors]
    all_avg_ratings = [a['avgRating'] for a in processed_actors if a['avgRating']]
    
    summary = {
        'totalActors': len(processed_actors),
        'actorsWithMultipleFilms': len([a for a in processed_actors if a['filmCount'] >= 3]),
        'avgFilmsPerActor': round(np.mean(all_film_counts), 1) if all_film_counts else 0,
        'avgRatingAcrossActors': round(np.mean(all_avg_ratings), 2) if all_avg_ratings else 0,
        'topActorFilmCount': max(all_film_counts) if all_film_counts else 0,
    }
    
    return jsonify({
        'actors': top_actors,
        'summary': summary
    })


@app.route('/api/analytics/actors/compare')
def compare_actors():
    """Compare two actors side by side"""
    actor1_id = request.args.get('actor1')
    actor2_id = request.args.get('actor2')
    
    if not actor1_id or not actor2_id:
        return jsonify({'error': 'Both actor1 and actor2 IDs required'}), 400
    
    # Get analytics for both actors
    from flask import url_for
    
    # We'll call the analytics function directly
    # First, get actor 1 data
    actor1_data = None
    actor2_data = None
    
    if people_cache and movies_df is not None:
        # Get both actors' analytics
        person1 = people_cache.get(actor1_id)
        person2 = people_cache.get(actor2_id)
        
        if not person1 or not person2:
            return jsonify({'error': 'One or both actors not found'}), 404
        
        # Build comparison data
        comparison = {
            'actor1': {
                'id': actor1_id,
                'name': person1.get('imdb_name') or person1.get('primaryName', ''),
                'profilePath': person1.get('profile_path'),
            },
            'actor2': {
                'id': actor2_id,
                'name': person2.get('imdb_name') or person2.get('primaryName', ''),
                'profilePath': person2.get('profile_path'),
            },
            'note': 'Use /api/people/{id}/analytics for detailed comparison data'
        }
        
        return jsonify(comparison)
    
    return jsonify({'error': 'Data not available'}), 500


# Error handlers
@app.errorhandler(404)
def not_found(e):
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(500)
def server_error(e):
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    print("\n" + "="*60)
    print("🎬 CINESCOPE API SERVER")
    print("="*60)
    
    # Load data
    load_data()
    
    if movies_df is not None and not movies_df.empty:
        print(f"✅ Loaded {len(movies_df)} movies")
        print(f"✅ Loaded {len(people_cache)} people")
    else:
        print("⚠️  No movie data found - check data paths")
    
    print("\n🌐 Starting API server...")
    print("📍 Access at: http://localhost:5001")
    print("📍 API endpoints at: http://localhost:5001/api/*")
    print("="*60 + "\n")
    
    app.run(debug=True, host='0.0.0.0', port=5001)
