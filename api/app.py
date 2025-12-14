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


# =============================================================================
# ZODIAC & ASTROLOGICAL ANALYSIS ENDPOINTS
# =============================================================================

@app.route('/api/zodiac/distribution')
def get_zodiac_distribution():
    """Get zodiac sign distribution among actors/directors"""
    if not people_cache:
        return jsonify({'error': 'People data not loaded'}), 500
    
    # Western zodiac
    western_zodiac = {}
    chinese_zodiac = {}
    elements = {'Fire': 0, 'Earth': 0, 'Air': 0, 'Water': 0}
    
    element_map = {
        'Aries': 'Fire', 'Leo': 'Fire', 'Sagittarius': 'Fire',
        'Taurus': 'Earth', 'Virgo': 'Earth', 'Capricorn': 'Earth',
        'Gemini': 'Air', 'Libra': 'Air', 'Aquarius': 'Air',
        'Cancer': 'Water', 'Scorpio': 'Water', 'Pisces': 'Water'
    }
    
    for person_id, person in people_cache.items():
        sign = person.get('zodiac_western')
        chinese = person.get('zodiac_chinese')
        
        if sign:
            western_zodiac[sign] = western_zodiac.get(sign, 0) + 1
            if sign in element_map:
                elements[element_map[sign]] += 1
        
        if chinese:
            chinese_zodiac[chinese] = chinese_zodiac.get(chinese, 0) + 1
    
    return jsonify({
        'western': western_zodiac,
        'chinese': chinese_zodiac,
        'elements': elements,
        'total_with_zodiac': sum(western_zodiac.values())
    })


@app.route('/api/zodiac/people/<sign>')
def get_people_by_zodiac(sign):
    """Get people by zodiac sign"""
    if not people_cache:
        return jsonify({'error': 'People data not loaded'}), 500
    
    sign = sign.title()
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    
    people = []
    for person_id, person in people_cache.items():
        if person.get('zodiac_western') == sign or person.get('zodiac_chinese') == sign:
            people.append({
                'id': person_id,
                'name': person.get('name'),
                'profile_path': person.get('profile_path'),
                'known_for': person.get('known_for_department'),
                'zodiac_western': person.get('zodiac_western'),
                'zodiac_chinese': person.get('zodiac_chinese'),
                'birthday': person.get('birthday') or person.get('ext_birth_date'),
                'popularity': person.get('popularity', 0)
            })
    
    # Sort by popularity
    people.sort(key=lambda x: x.get('popularity', 0) or 0, reverse=True)
    
    # Paginate
    start = (page - 1) * per_page
    end = start + per_page
    
    return jsonify({
        'sign': sign,
        'total': len(people),
        'page': page,
        'per_page': per_page,
        'people': people[start:end]
    })


@app.route('/api/zodiac/stats')
def get_zodiac_stats():
    """Get zodiac-related statistics"""
    if not people_cache:
        return jsonify({'error': 'People data not loaded'}), 500
    
    stats = {
        'by_department': {},
        'mortality': {
            'causes': {},
            'avg_age_at_death': None,
            'deceased_count': 0
        }
    }
    
    ages_at_death = []
    
    for person_id, person in people_cache.items():
        sign = person.get('zodiac_western')
        dept = person.get('known_for_department', 'Unknown')
        
        if sign:
            if dept not in stats['by_department']:
                stats['by_department'][dept] = {}
            stats['by_department'][dept][sign] = stats['by_department'][dept].get(sign, 0) + 1
        
        # Mortality
        cause = person.get('ext_cause_of_death')
        if cause:
            stats['mortality']['causes'][cause] = stats['mortality']['causes'].get(cause, 0) + 1
            stats['mortality']['deceased_count'] += 1
        
        age_at_death = person.get('ext_age_at_death')
        if age_at_death:
            ages_at_death.append(age_at_death)
    
    if ages_at_death:
        stats['mortality']['avg_age_at_death'] = sum(ages_at_death) / len(ages_at_death)
        stats['mortality']['median_age_at_death'] = sorted(ages_at_death)[len(ages_at_death) // 2]
    
    # Sort causes
    stats['mortality']['top_causes'] = sorted(
        stats['mortality']['causes'].items(),
        key=lambda x: x[1],
        reverse=True
    )[:20]
    
    return jsonify(stats)


# =============================================================================
# KEYWORDS & THEMES ENDPOINTS
# =============================================================================

# Load keywords cache
keywords_cache = None

def load_keywords_cache():
    """Load keywords cache"""
    global keywords_cache
    keywords_file = PROCESSED_DIR / 'keywords_cache.json'
    if keywords_file.exists():
        with open(keywords_file, 'r') as f:
            keywords_cache = json.load(f)
        logger.info(f"Loaded keywords for {len(keywords_cache)} movies")
    else:
        keywords_cache = {}


@app.route('/api/keywords/movie/<movie_id>')
def get_movie_keywords(movie_id):
    """Get keywords for a specific movie"""
    if keywords_cache is None:
        load_keywords_cache()
    
    # Try to find by TMDB ID
    if str(movie_id) in keywords_cache:
        return jsonify(keywords_cache[str(movie_id)])
    
    # Try to find by IMDB ID - need to map
    if movies_df is not None:
        match = movies_df[
            (movies_df.get('Const', pd.Series()) == movie_id) |
            (movies_df.get('const', pd.Series()) == movie_id) |
            (movies_df.get('imdb_id', pd.Series()) == movie_id)
        ]
        if not match.empty:
            tmdb_id = match.iloc[0].get('tmdb_id')
            if tmdb_id and str(tmdb_id) in keywords_cache:
                return jsonify(keywords_cache[str(tmdb_id)])
    
    return jsonify({'error': 'Keywords not found', 'keywords': []}), 404


@app.route('/api/keywords/top')
def get_top_keywords():
    """Get top keywords across all movies"""
    if keywords_cache is None:
        load_keywords_cache()
    
    limit = request.args.get('limit', 50, type=int)
    
    # Count all keywords
    keyword_counts = {}
    for movie_id, data in keywords_cache.items():
        for kw in data.get('keywords', []):
            keyword_counts[kw] = keyword_counts.get(kw, 0) + 1
    
    # Sort and return top
    top_keywords = sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True)[:limit]
    
    return jsonify({
        'total_unique': len(keyword_counts),
        'keywords': [{'keyword': k, 'count': c} for k, c in top_keywords]
    })


@app.route('/api/keywords/search')
def search_by_keyword():
    """Search movies by keyword"""
    if keywords_cache is None:
        load_keywords_cache()
    
    keyword = request.args.get('q', '').lower()
    if not keyword:
        return jsonify({'error': 'Keyword query required'}), 400
    
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    
    # Find movies with this keyword
    matching_movies = []
    for movie_id, data in keywords_cache.items():
        keywords_lower = [kw.lower() for kw in data.get('keywords', [])]
        if keyword in keywords_lower:
            matching_movies.append(movie_id)
    
    # Get movie details
    movies = []
    if movies_df is not None:
        for tmdb_id in matching_movies:
            match = movies_df[movies_df.get('tmdb_id', pd.Series()).astype(str) == str(tmdb_id)]
            if not match.empty:
                movies.append(movie_to_dict(match.iloc[0]))
    
    # Paginate
    start = (page - 1) * per_page
    end = start + per_page
    
    return jsonify({
        'keyword': keyword,
        'total': len(movies),
        'page': page,
        'per_page': per_page,
        'movies': movies[start:end]
    })


@app.route('/api/keywords/categories')
def get_keyword_categories():
    """Get keywords grouped by category"""
    if keywords_cache is None:
        load_keywords_cache()
    
    categories = {
        'themes': {},
        'settings': {},
        'character_types': {},
        'narrative': {},
        'emotional': {},
        'content': {}
    }
    
    for movie_id, data in keywords_cache.items():
        for category, kws in data.get('categories', {}).items():
            if category in categories:
                for kw in kws:
                    categories[category][kw] = categories[category].get(kw, 0) + 1
    
    # Sort each category
    result = {}
    for category, kws in categories.items():
        sorted_kws = sorted(kws.items(), key=lambda x: x[1], reverse=True)[:30]
        result[category] = [{'keyword': k, 'count': c} for k, c in sorted_kws]
    
    return jsonify(result)


# =============================================================================
# ENHANCED RECOMMENDATIONS ENDPOINTS
# =============================================================================

@app.route('/api/recommendations/personalized')
def get_personalized_recommendations():
    """Get personalized recommendations based on viewing history"""
    if movies_df is None or movies_df.empty:
        return jsonify({'error': 'Movie data not loaded'}), 500
    
    recommendation_type = request.args.get('type', 'all')  # all, hidden_gems, new_directors, etc.
    limit = request.args.get('limit', 10, type=int)
    
    # Get user's highly rated movies (8+)
    rating_col = 'Your Rating' if 'Your Rating' in movies_df.columns else 'your_rating'
    high_rated = movies_df[pd.to_numeric(movies_df[rating_col], errors='coerce') >= 8]
    
    # Analyze preferences
    user_genres = {}
    user_directors = {}
    user_decades = {}
    
    for _, row in high_rated.iterrows():
        # Genres
        genres = str(row.get('Genres') or row.get('genres') or '').split(',')
        for genre in genres:
            genre = genre.strip()
            if genre:
                user_genres[genre] = user_genres.get(genre, 0) + 1
        
        # Directors
        directors = str(row.get('Directors') or row.get('directors') or '').split(',')
        for director in directors:
            director = director.strip()
            if director:
                user_directors[director] = user_directors.get(director, 0) + 1
        
        # Decades
        year = row.get('Year') or row.get('year')
        if year:
            try:
                decade = (int(year) // 10) * 10
                user_decades[decade] = user_decades.get(decade, 0) + 1
            except:
                pass
    
    # Get top preferences
    top_genres = sorted(user_genres.items(), key=lambda x: x[1], reverse=True)[:5]
    top_directors = sorted(user_directors.items(), key=lambda x: x[1], reverse=True)[:10]
    top_decades = sorted(user_decades.items(), key=lambda x: x[1], reverse=True)[:3]
    
    return jsonify({
        'profile': {
            'favorite_genres': [{'genre': g, 'count': c} for g, c in top_genres],
            'favorite_directors': [{'name': d, 'count': c} for d, c in top_directors],
            'favorite_decades': [{'decade': d, 'count': c} for d, c in top_decades],
            'total_highly_rated': len(high_rated)
        },
        'recommendations': {
            'note': 'Use /api/recommendations/gaps for viewing gaps, /api/movies for filtered browsing'
        }
    })


@app.route('/api/recommendations/director/<director_name>')
def get_director_recommendations(director_name):
    """Get unwatched films by a specific director"""
    if movies_df is None:
        return jsonify({'error': 'Data not loaded'}), 500
    
    # This would need a database of all films by director
    # For now, show watched films by this director
    director_col = 'Directors' if 'Directors' in movies_df.columns else 'directors'
    
    director_films = movies_df[
        movies_df[director_col].str.contains(director_name, case=False, na=False)
    ]
    
    films = [movie_to_dict(row) for _, row in director_films.iterrows()]
    films.sort(key=lambda x: x.get('year') or 0)
    
    return jsonify({
        'director': director_name,
        'watched_count': len(films),
        'films': films,
        'note': 'These are watched films. Full filmography requires external API.'
    })


@app.route('/api/recommendations/similar/<movie_id>')
def get_similar_recommendations(movie_id):
    """Get movies similar to a specific movie (enhanced version)"""
    if movies_df is None:
        return jsonify({'error': 'Data not loaded'}), 500
    
    if keywords_cache is None:
        load_keywords_cache()
    
    # Find the source movie
    match = movies_df[
        (movies_df.get('Const', pd.Series()).astype(str) == str(movie_id)) |
        (movies_df.get('const', pd.Series()).astype(str) == str(movie_id)) |
        (movies_df.get('tmdb_id', pd.Series()).astype(str) == str(movie_id))
    ]
    
    if match.empty:
        return jsonify({'error': 'Movie not found'}), 404
    
    source = match.iloc[0]
    source_genres = set(str(source.get('Genres') or source.get('genres') or '').lower().split(','))
    source_directors = set(str(source.get('Directors') or source.get('directors') or '').lower().split(','))
    
    # Get source keywords
    source_tmdb_id = str(source.get('tmdb_id', ''))
    source_keywords = set()
    if source_tmdb_id in keywords_cache:
        source_keywords = set(kw.lower() for kw in keywords_cache[source_tmdb_id].get('keywords', []))
    
    # Score all other movies
    similarities = []
    
    for _, row in movies_df.iterrows():
        if row.get('Const') == movie_id or row.get('tmdb_id') == source.get('tmdb_id'):
            continue
        
        score = 0
        
        # Genre overlap
        genres = set(str(row.get('Genres') or row.get('genres') or '').lower().split(','))
        genre_overlap = len(source_genres & genres)
        score += genre_overlap * 3
        
        # Director overlap
        directors = set(str(row.get('Directors') or row.get('directors') or '').lower().split(','))
        if source_directors & directors:
            score += 5
        
        # Keyword overlap
        row_tmdb_id = str(row.get('tmdb_id', ''))
        if row_tmdb_id in keywords_cache:
            row_keywords = set(kw.lower() for kw in keywords_cache[row_tmdb_id].get('keywords', []))
            keyword_overlap = len(source_keywords & row_keywords)
            score += keyword_overlap * 2
        
        # Year proximity (within 10 years)
        try:
            source_year = int(source.get('Year') or source.get('year') or 0)
            row_year = int(row.get('Year') or row.get('year') or 0)
            if abs(source_year - row_year) <= 10:
                score += 1
        except:
            pass
        
        if score > 0:
            similarities.append((movie_to_dict(row), score))
    
    # Sort by score
    similarities.sort(key=lambda x: x[1], reverse=True)
    
    limit = request.args.get('limit', 10, type=int)
    
    return jsonify({
        'source_movie': movie_to_dict(source),
        'similar_movies': [
            {**movie, 'similarity_score': score}
            for movie, score in similarities[:limit]
        ]
    })


# =============================================================================
# DISCOVERY ENGINE - Find ANY movie from TMDB
# =============================================================================

discovery_engine = None

def get_discovery_engine():
    """Initialize discovery engine lazily."""
    global discovery_engine
    if discovery_engine is None:
        try:
            import sys
            sys.path.insert(0, str(Path(__file__).parent.parent))
            from src.recommender.discovery_engine import DiscoveryEngine
            discovery_engine = DiscoveryEngine()
            
            # Load watched movie IDs to exclude
            if movies_df is not None:
                tmdb_ids = movies_df['tmdb_id'].dropna().astype(int).tolist()
                discovery_engine.set_watched_movies(tmdb_ids)
                logger.info(f"Discovery engine initialized with {len(tmdb_ids)} watched movies")
        except Exception as e:
            logger.error(f"Failed to initialize discovery engine: {e}")
            return None
    return discovery_engine


@app.route('/api/discover')
def discover_movies():
    """
    Discover NEW movies from TMDB based on preferences.
    
    Query params:
        genres: Comma-separated list of genres
        decades: Comma-separated list of decades (e.g., "1990,2000")
        directors: Comma-separated list of director names
        actors: Comma-separated list of actor names
        min_rating: Minimum TMDB rating (default: 6.0)
        mood: feel_good, intense, thought_provoking, fun, any
        limit: Max results per category (default: 10)
    """
    engine = get_discovery_engine()
    if not engine:
        return jsonify({'error': 'Discovery engine not available. Check TMDB API key.'}), 500
    
    # Parse preferences from query params
    genres = request.args.get('genres', '')
    decades = request.args.get('decades', '')
    directors = request.args.get('directors', '')
    actors = request.args.get('actors', '')
    min_rating = request.args.get('min_rating', 6.0, type=float)
    mood = request.args.get('mood', 'any')
    limit = request.args.get('limit', 10, type=int)
    
    # Set preferences
    engine.set_preferences(
        favorite_genres=[g.strip() for g in genres.split(',') if g.strip()] if genres else [],
        favorite_decades=[int(d.strip()) for d in decades.split(',') if d.strip()] if decades else [],
        favorite_directors=[d.strip() for d in directors.split(',') if d.strip()] if directors else [],
        favorite_actors=[a.strip() for a in actors.split(',') if a.strip()] if actors else [],
        min_rating=min_rating,
        mood=mood
    )
    
    # Run discovery
    results = engine.get_recommendations_json(limit)
    
    return jsonify({
        'preferences': {
            'genres': engine.preferences.favorite_genres,
            'decades': engine.preferences.favorite_decades,
            'directors': engine.preferences.favorite_directors,
            'actors': engine.preferences.favorite_actors,
            'min_rating': engine.preferences.min_rating,
            'mood': engine.preferences.mood
        },
        'discoveries': results,
        'note': 'These are movies you have NOT watched yet, discovered from TMDB'
    })


@app.route('/api/discover/search')
def discover_search():
    """Search for any movie in TMDB database."""
    engine = get_discovery_engine()
    if not engine:
        return jsonify({'error': 'Discovery engine not available'}), 500
    
    query = request.args.get('q', '')
    if not query:
        return jsonify({'error': 'Search query required'}), 400
    
    limit = request.args.get('limit', 20, type=int)
    results = engine.search(query, limit)
    
    return jsonify({
        'query': query,
        'results': [m.to_dict() for m in results],
        'total': len(results)
    })


@app.route('/api/discover/similar/<int:tmdb_id>')
def discover_similar(tmdb_id):
    """Find movies similar to a specific TMDB movie."""
    engine = get_discovery_engine()
    if not engine:
        return jsonify({'error': 'Discovery engine not available'}), 500
    
    limit = request.args.get('limit', 10, type=int)
    results = engine.find_similar_to(tmdb_id, limit)
    
    return jsonify({
        'source_tmdb_id': tmdb_id,
        'similar_movies': [m.to_dict() for m in results]
    })


@app.route('/api/discover/director/<director_name>')
def discover_by_director(director_name):
    """Discover all movies by a specific director."""
    engine = get_discovery_engine()
    if not engine:
        return jsonify({'error': 'Discovery engine not available'}), 500
    
    limit = request.args.get('limit', 30, type=int)
    results = engine.discover_by_director(director_name, limit)
    
    return jsonify({
        'director': director_name,
        'movies': [m.to_dict() for m in results],
        'total': len(results)
    })


@app.route('/api/discover/actor/<actor_name>')
def discover_by_actor(actor_name):
    """Discover movies featuring a specific actor."""
    engine = get_discovery_engine()
    if not engine:
        return jsonify({'error': 'Discovery engine not available'}), 500
    
    limit = request.args.get('limit', 30, type=int)
    results = engine.discover_by_actor(actor_name, limit)
    
    return jsonify({
        'actor': actor_name,
        'movies': [m.to_dict() for m in results],
        'total': len(results)
    })


@app.route('/api/discover/trending')
def discover_trending():
    """Get trending movies that match your preferences."""
    engine = get_discovery_engine()
    if not engine:
        return jsonify({'error': 'Discovery engine not available'}), 500
    
    time_window = request.args.get('window', 'week')  # 'day' or 'week'
    limit = request.args.get('limit', 20, type=int)
    results = engine.get_trending(time_window, limit)
    
    return jsonify({
        'time_window': time_window,
        'movies': [m.to_dict() for m in results]
    })


@app.route('/api/discover/hidden-gems')
def discover_hidden_gems():
    """Find highly-rated but under-the-radar movies."""
    engine = get_discovery_engine()
    if not engine:
        return jsonify({'error': 'Discovery engine not available'}), 500
    
    # Optionally set genre preferences
    genres = request.args.get('genres', '')
    if genres:
        engine.set_preferences(
            favorite_genres=[g.strip() for g in genres.split(',') if g.strip()]
        )
    
    limit = request.args.get('limit', 20, type=int)
    results = engine.discover_hidden_gems(limit)
    
    return jsonify({
        'movies': [m.to_dict() for m in results]
    })


@app.route('/api/discover/genre/<genre>')
def discover_by_genre(genre):
    """Discover top movies in a specific genre."""
    engine = get_discovery_engine()
    if not engine:
        return jsonify({'error': 'Discovery engine not available'}), 500
    
    limit = request.args.get('limit', 20, type=int)
    min_rating = request.args.get('min_rating', 6.5, type=float)
    
    engine.set_preferences(favorite_genres=[genre], min_rating=min_rating)
    results = engine.discover_by_genre([genre], limit)
    
    return jsonify({
        'genre': genre,
        'movies': [m.to_dict() for m in results]
    })


@app.route('/api/discover/decade/<int:decade>')
def discover_by_decade(decade):
    """Discover top movies from a specific decade."""
    engine = get_discovery_engine()
    if not engine:
        return jsonify({'error': 'Discovery engine not available'}), 500
    
    limit = request.args.get('limit', 20, type=int)
    results = engine.discover_by_decade(decade, limit)
    
    return jsonify({
        'decade': f"{decade}s",
        'movies': [m.to_dict() for m in results]
    })


@app.route('/api/preferences')
def get_user_preferences():
    """
    Analyze user's preferences based on watched movies.
    Returns suggested preferences for discovery.
    """
    if movies_df is None or movies_df.empty:
        return jsonify({'error': 'No movie data'}), 500
    
    # Analyze genres
    genre_col = 'Genres' if 'Genres' in movies_df.columns else 'genres'
    rating_col = 'Your Rating' if 'Your Rating' in movies_df.columns else 'your_rating'
    
    genre_ratings = {}
    genre_counts = {}
    
    for _, row in movies_df.iterrows():
        genres = str(row.get(genre_col, '')).split(',')
        rating = row.get(rating_col)
        
        for g in genres:
            g = g.strip()
            if g:
                genre_counts[g] = genre_counts.get(g, 0) + 1
                if pd.notna(rating):
                    if g not in genre_ratings:
                        genre_ratings[g] = []
                    genre_ratings[g].append(float(rating))
    
    # Calculate weighted scores
    genre_scores = {}
    for genre, ratings in genre_ratings.items():
        avg = sum(ratings) / len(ratings)
        count = genre_counts.get(genre, 0)
        # Score = avg rating * log(count) 
        import math
        genre_scores[genre] = avg * math.log1p(count)
    
    top_genres = sorted(genre_scores.items(), key=lambda x: x[1], reverse=True)[:5]
    
    # Analyze decades
    year_col = 'Year' if 'Year' in movies_df.columns else 'year'
    decade_counts = {}
    for _, row in movies_df.iterrows():
        year = row.get(year_col)
        if pd.notna(year):
            decade = (int(year) // 10) * 10
            decade_counts[decade] = decade_counts.get(decade, 0) + 1
    
    top_decades = sorted(decade_counts.items(), key=lambda x: x[1], reverse=True)[:3]
    
    # Find top directors
    director_col = 'Directors' if 'Directors' in movies_df.columns else 'directors'
    director_ratings = {}
    
    for _, row in movies_df.iterrows():
        directors = str(row.get(director_col, '')).split(',')
        rating = row.get(rating_col)
        
        for d in directors:
            d = d.strip()
            if d and pd.notna(rating):
                if d not in director_ratings:
                    director_ratings[d] = []
                director_ratings[d].append(float(rating))
    
    # Directors with multiple high-rated films
    trusted_directors = [
        (d, sum(r)/len(r), len(r)) 
        for d, r in director_ratings.items() 
        if len(r) >= 2 and sum(r)/len(r) >= 7.5
    ]
    trusted_directors.sort(key=lambda x: x[1] * math.log1p(x[2]), reverse=True)
    
    return jsonify({
        'suggested_preferences': {
            'genres': [g for g, _ in top_genres],
            'decades': [d for d, _ in top_decades],
            'directors': [d for d, _, _ in trusted_directors[:5]],
            'min_rating': 6.5
        },
        'analysis': {
            'favorite_genres': [
                {'genre': g, 'score': round(s, 2), 'count': genre_counts.get(g, 0)}
                for g, s in top_genres
            ],
            'favorite_decades': [
                {'decade': f"{d}s", 'count': c}
                for d, c in top_decades
            ],
            'trusted_directors': [
                {'name': d, 'avg_rating': round(r, 1), 'films': c}
                for d, r, c in trusted_directors[:10]
            ],
            'total_movies_watched': len(movies_df)
        },
        'api_usage': {
            'discover_url': '/api/discover?genres=Drama,Thriller&decades=1990,2000&min_rating=7',
            'search_url': '/api/discover/search?q=movie+title',
            'director_url': '/api/discover/director/Christopher%20Nolan',
            'genre_url': '/api/discover/genre/Thriller',
            'trending_url': '/api/discover/trending'
        }
    })


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
