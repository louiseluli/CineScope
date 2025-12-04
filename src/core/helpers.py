"""
CineScope Data Analysis - Helper Functions & Utilities
======================================================
This module contains reusable utility functions for data processing,
genre parsing, statistical analysis, and visualization helpers.
"""

import pandas as pd
import numpy as np
from collections import Counter
from datetime import datetime, timedelta
import re
import warnings
warnings.filterwarnings('ignore')

# Import configuration
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.core.config import GENRE_SEPARATOR, MISSING_VALUES, log_message


# --- GENRE NORMALIZATION HELPERS ---------------------------------------------

def _coerce_to_list(x):
    """
    Robustly coerce a 'genres' cell to a Python list of raw tokens.
    Accepts:
      - Python repr strings: "['Comedy', 'Romance']"
      - JSON arrays: '["Comedy","Romance"]'
      - Comma/semicolon-separated strings: "Comedy, Romance; Drama"
      - Already-a-list objects
    Returns a list of strings (possibly dirty), or [] if empty.
    """
    import ast
    import json

    if x is None or (isinstance(x, float) and pd.isna(x)):  # noqa: F821 (pd imported in module)
        return []
    if isinstance(x, list):
        return [str(t) for t in x]

    s = str(x).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return []

    # Try literal_eval (Python list repr)
    if (s.startswith("[") and s.endswith("]")) or (s.startswith("(") and s.endswith(")")):
        try:
            val = ast.literal_eval(s)
            if isinstance(val, (list, tuple)):
                return [str(t) for t in val]
        except Exception:
            pass

    # Try JSON
    if s.startswith("[") and s.endswith("]"):
        try:
            val = json.loads(s)
            if isinstance(val, list):
                return [str(t) for t in val]
        except Exception:
            pass

    # Fallback: split on commas / semicolons / pipes
    parts = []
    for sep in ["|", ";", ","]:
        if sep in s:
            parts = [p for p in s.replace("|", ",").replace(";", ",").split(",")]
            break
    if not parts:
        parts = [s]

    # Strip wrapping quotes and brackets that slipped through
    cleaned = []
    for p in parts:
        p = p.strip().strip('"').strip("'")
        p = p.replace("['", "").replace("']", "").replace('["', '').replace('"]', '')
        cleaned.append(p)
    return cleaned


def normalize_genre_name(name: str) -> str:
    """
    Normalize a single genre label:
      - strip spaces/quotes/brackets
      - casefold → title case (with a few acronyms fixed)
      - unify common aliases ("Sci Fi" → "Sci-Fi", "Film-Noir" → "Noir", etc.)
    """
    if not name:
        return ""
    g = str(name).strip().strip('"').strip("'")

    # Canonicalize separators/hyphens
    g = g.replace("‐", "-").replace("–", "-").replace("—", "-")

    # Lower for alias matching
    low = g.lower()

    # Alias map (expand as needed)
    aliases = {
        "sci fi": "Sci-Fi",
        "sci-fi": "Sci-Fi",
        "scifi": "Sci-Fi",
        "science fiction": "Sci-Fi",
        "film-noir": "Noir",
        "noir": "Noir",
        "tv movie": "TV Movie",
        "tv-movie": "TV Movie",
        "biography": "Biography",
        "rom-com": "Romance",
        "romcom": "Romance",
        "kids": "Family",
        "children": "Family",
    }
    if low in aliases:
        return aliases[low]

    # Title case default
    g = g.title()

    # Preserve acronyms and common caps
    fixes = {
        "Sci-Fi": "Sci-Fi",     # use non-breaking hyphen variant for consistency
        "Imdb": "IMDb",
        "Tv Movie": "TV Movie",
        "Usa": "USA",
    }
    return fixes.get(g, g)


def parse_genres(x):
    """
    Robustly parse a 'genres' field into a clean, de-duplicated list of genre names.

    Accepts:
      - list/tuple of strings,
      - comma/pipe/semicolon/slash-separated strings,
      - list-like strings (e.g., "['Comedy', 'Romance']" or '["Drama","Crime"]').

    Normalizes:
      - strips brackets/quotes and odd punctuation,
      - splits on , | ; /,
      - trims whitespace, collapses inner spaces,
      - Title-cases consistently,
      - de-duplicates while preserving original order,
      - maps common synonyms (e.g., "Science Fiction" → "Sci-Fi", "TV Movie" casing).

    Returns: list[str]
    """
    import re

    if x is None or (isinstance(x, float) and str(x) == "nan"):
        return []

    # If already a list/tuple, coerce to flat string list first
    if isinstance(x, (list, tuple)):
        parts = [str(v) for v in x if v is not None and str(v).strip()]
    else:
        s = str(x)

        # Remove outer list-like wrappers: ["..."], ['...']
        # and any stray quotes around the whole string
        s = s.strip()
        if s.startswith("[") and s.endswith("]"):
            s = s[1:-1]

        # Replace common joiners in list-like strings: "', '", '", "'
        s = s.replace("', '", ", ").replace('", "', ", ")

        # Strip leading/trailing quotes that sometimes remain
        s = re.sub(r"^[\"']+|[\"']+$", "", s)

        # Split on commas, pipes, semicolons, or slashes
        parts = re.split(r"[,\|;/]", s)

    # Clean each token
    cleaned = []
    seen = set()
    for p in parts:
        t = str(p).strip()
        if not t:
            continue

        # Remove any leftover quotes/brackets
        t = t.strip(" '\"\t\n\r[]()")

        # Normalize inner whitespace
        t = re.sub(r"\s+", " ", t)

        # Title-case with some exceptions
        t_norm = t.title()

        # Common synonym / casing fixes
        replacements = {
            "Science Fiction": "Sci-Fi",
            "Sci Fi": "Sci-Fi",
            "Tv Movie": "TV Movie",
            "Film Noir": "Film-Noir",   # IMDb style
            "Bio-Graphy": "Biography",  # occasional weirdness
        }
        t_norm = replacements.get(t_norm, t_norm)

        # Final guard: drop any empty result
        if not t_norm or t_norm.lower() in {"na", "n/a", "none", "null"}:
            continue

        # De-duplicate preserving order
        key = t_norm.lower()
        if key not in seen:
            seen.add(key)
            cleaned.append(t_norm)

    return cleaned



def explode_genres(
    df,
    genres_col: str = "genres",
    id_col: str = "const",
    keep_cols: list[str] | None = None,
):
    """
    Explode a dataframe so each (film, genre) becomes one row.

    - Uses the robust parse_genres() above (fixes ['Comedy'] / stray quotes).
    - De-duplicates genres *per film*.
    - Preserves common analysis columns if present.

    Parameters
    ----------
    df : pd.DataFrame
    genres_col : str
        Column containing genres (string, list-like, or messy list-in-string).
    id_col : str
        Identifier column for the title (defaults to 'const').
    keep_cols : list[str] | None
        Additional columns to keep in the exploded frame.
        If None, we keep a sensible default set when available:
        ['const', 'imdb_rating', 'runtime_mins', 'year', 'decade'].

    Returns
    -------
    pd.DataFrame with columns:
        - id_col (e.g., 'const')
        - 'genre'
        - plus any available keep columns
    """
    import pandas as pd

    if keep_cols is None:
        default_candidates = ["imdb_rating", "runtime_mins", "year", "decade"]
        keep_cols = [c for c in default_candidates if c in df.columns]

    base_cols = []
    if id_col in df.columns:
        base_cols.append(id_col)
    if genres_col in df.columns and genres_col not in base_cols:
        base_cols.append(genres_col)

    # Also carry the keep_cols if they exist
    cols_to_use = base_cols + [c for c in keep_cols if c in df.columns]

    if not set(base_cols).issubset(df.columns):
        # Fall back gracefully if the genres column doesn't exist
        # Return empty frame with expected columns
        out_cols = ([id_col] if id_col in df.columns else []) + ["genre"] + keep_cols
        return pd.DataFrame(columns=out_cols)

    rows = []
    for _, row in df[cols_to_use].iterrows():
        genres = parse_genres(row.get(genres_col))
        if not genres:
            continue

        # ensure per-film unique genres
        seen = set()
        for g in genres:
            key = g.lower()
            if key in seen:
                continue
            seen.add(key)

            new_row = {id_col: row[id_col], "genre": g}
            for c in keep_cols:
                if c in row.index:
                    new_row[c] = row[c]
            rows.append(new_row)

    exploded = pd.DataFrame(rows)

    # Optional: keep a stable sort (genre alpha then id)
    if not exploded.empty:
        sort_cols = ["genre"]
        if id_col in exploded.columns:
            sort_cols.append(id_col)
        exploded = exploded.sort_values(sort_cols).reset_index(drop=True)

    return exploded


def _normalize_list_str(s: str) -> str:
    """
    Utility to quickly strip brackets/quotes from a list-like string.
    Not used outside helpers; safe to keep internal.
    """
    import re
    if s is None:
        return ""
    s = str(s).strip()
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1]
    s = s.replace("', '", ", ").replace('", "', ", ")
    s = re.sub(r"^[\"']+|[\"']+$", "", s)
    return s


def parse_directors(director_string):
    """
    Parse director string into a clean list.
    
    Args:
        director_string: String with comma-separated directors
        
    Returns:
        List of director names
    """
    if pd.isna(director_string) or director_string in MISSING_VALUES:
        return []
    
    directors = [d.strip() for d in str(director_string).split(',')]
    directors = [d for d in directors if d and d not in MISSING_VALUES]
    
    return directors


def parse_actors(actor_string):
    """
    Parse actor string into a clean list.
    
    Args:
        actor_string: String with comma or pipe-separated actors
        
    Returns:
        List of actor names
    """
    if pd.isna(actor_string) or actor_string in MISSING_VALUES:
        return []
    
    # Try different separators
    if '|' in str(actor_string):
        actors = [a.strip() for a in str(actor_string).split('|')]
    else:
        actors = [a.strip() for a in str(actor_string).split(',')]
    
    actors = [a for a in actors if a and a not in MISSING_VALUES]
    
    return actors


def parse_keywords(keyword_string):
    """
    Parse keyword/theme string into a clean list.
    
    Args:
        keyword_string: String with comma-separated keywords
        
    Returns:
        List of keywords
    """
    if pd.isna(keyword_string) or keyword_string in MISSING_VALUES:
        return []
    
    keywords = [k.strip() for k in str(keyword_string).split(',')]
    keywords = [k.lower() for k in keywords if k and k not in MISSING_VALUES]
    
    return keywords


def get_primary_genre(genre_string):
    """
    Get the first/primary genre from a genre string.
    
    Args:
        genre_string: Comma-separated genre string
        
    Returns:
        Primary genre or 'Unknown'
    """
    genres = parse_genres(genre_string)
    return genres[0] if genres else 'Unknown'


# ============================================================================
# TEMPORAL FUNCTIONS
# ============================================================================

def get_decade(year):
    """
    Get the decade for a given year.
    
    Args:
        year: Year as integer
        
    Returns:
        Decade as integer (e.g., 1990)
    """
    if pd.isna(year):
        return None
    return int(year // 10 * 10)


def get_era(year):
    """
    Get cinema era for a given year.
    
    Args:
        year: Year as integer
        
    Returns:
        Era name as string
    """
    if pd.isna(year):
        return 'Unknown'
    
    year = int(year)
    
    if year < 1927:
        return 'Silent Era (pre-1927)'
    elif year < 1935:
        return 'Pre-Code (1927-1934)'
    elif year < 1960:
        return 'Golden Age (1935-1959)'
    elif year < 1980:
        return 'New Hollywood (1960-1979)'
    elif year < 2000:
        return 'Blockbuster Era (1980-1999)'
    elif year < 2010:
        return 'Digital Age (2000-2009)'
    elif year < 2020:
        return 'Modern (2010-2019)'
    else:
        return 'Current (2020+)'


def calculate_time_lag(release_year, watch_date):
    """
    Calculate years between release and watching.
    
    Args:
        release_year: Year movie was released
        watch_date: Date movie was watched (datetime or year)
        
    Returns:
        Number of years between release and watch
    """
    if pd.isna(release_year) or pd.isna(watch_date):
        return None
    
    # If watch_date is datetime, extract year
    if isinstance(watch_date, (pd.Timestamp, datetime)):
        watch_year = watch_date.year
    else:
        watch_year = int(watch_date)
    
    return watch_year - int(release_year)


def get_season(date):
    """
    Get season for a given date.
    
    Args:
        date: datetime object
        
    Returns:
        Season name
    """
    if pd.isna(date):
        return None
    
    month = date.month
    
    if month in [12, 1, 2]:
        return 'Winter'
    elif month in [3, 4, 5]:
        return 'Spring'
    elif month in [6, 7, 8]:
        return 'Summer'
    else:  # 9, 10, 11
        return 'Fall'


# ============================================================================
# RATING & QUALITY ANALYSIS
# ============================================================================

def categorize_rating(rating):
    """
    Categorize a rating into quality tiers.
    
    Args:
        rating: Rating value (1-10 scale)
        
    Returns:
        Category string
    """
    if pd.isna(rating):
        return 'Unrated'
    
    rating = float(rating)
    
    if rating >= 9:
        return 'Masterpiece (9-10)'
    elif rating >= 8:
        return 'Excellent (8-8.9)'
    elif rating >= 7:
        return 'Good (7-7.9)'
    elif rating >= 6:
        return 'Average (6-6.9)'
    elif rating >= 5:
        return 'Below Average (5-5.9)'
    elif rating >= 4:
        return 'Poor (4-4.9)'
    else:
        return 'Bad (1-3.9)'


def calculate_rating_deviation(your_rating, imdb_rating):
    """
    Calculate deviation between your rating and IMDB.
    
    Args:
        your_rating: Your rating
        imdb_rating: IMDB rating
        
    Returns:
        Deviation (positive means you rated higher)
    """
    if pd.isna(your_rating) or pd.isna(imdb_rating):
        return None
    
    return float(your_rating) - float(imdb_rating)


def identify_contrarian_picks(df, deviation_threshold=2.0):
    """
    Identify films where your rating differs significantly from consensus.
    
    Args:
        df: DataFrame with your_rating and imdb_rating
        deviation_threshold: Minimum deviation to be considered contrarian
        
    Returns:
        DataFrame with contrarian picks
    """
    df_copy = df.copy()
    df_copy['rating_deviation'] = df_copy.apply(
        lambda x: calculate_rating_deviation(x['your_rating'], x['imdb_rating']),
        axis=1
    )
    
    # Loved by you, not by others
    loved = df_copy[df_copy['rating_deviation'] >= deviation_threshold].copy()
    loved['type'] = 'Hidden Gem (You loved it)'
    
    # Hated by you, loved by others
    hated = df_copy[df_copy['rating_deviation'] <= -deviation_threshold].copy()
    hated['type'] = 'Overrated (You disliked it)'
    
    contrarian = pd.concat([loved, hated])
    contrarian = contrarian.sort_values('rating_deviation', ascending=False)
    
    return contrarian


def calculate_rating_correlation(df):
    """
    Calculate correlations between different rating systems.
    
    Args:
        df: DataFrame with rating columns
        
    Returns:
        Correlation matrix
    """
    rating_cols = []
    
    if 'your_rating' in df.columns:
        rating_cols.append('your_rating')
    if 'imdb_rating' in df.columns:
        rating_cols.append('imdb_rating')
    if 'tmdb_rating' in df.columns:
        rating_cols.append('tmdb_rating')
    if 'tmdb_vote_average' in df.columns:
        rating_cols.append('tmdb_vote_average')
    if 'omdb_metascore' in df.columns:
        # Normalize metascore to 10-point scale
        df['metascore_normalized'] = df['omdb_metascore'] / 10
        rating_cols.append('metascore_normalized')
    
    if len(rating_cols) < 2:
        return None
    
    return df[rating_cols].corr()


# ============================================================================
# RUNTIME ANALYSIS
# ============================================================================

def categorize_runtime(runtime_mins):
    """
    Categorize runtime into groups.
    
    Args:
        runtime_mins: Runtime in minutes
        
    Returns:
        Runtime category
    """
    if pd.isna(runtime_mins):
        return 'Unknown'
    
    runtime = float(runtime_mins)
    
    if runtime < 90:
        return 'Short (<90 min)'
    elif runtime < 120:
        return 'Standard (90-120 min)'
    elif runtime < 150:
        return 'Long (120-150 min)'
    else:
        return 'Epic (150+ min)'


def calculate_total_watch_time(df, runtime_column='runtime_mins'):
    """
    Calculate total watch time in various units.
    
    Args:
        df: DataFrame with runtime column
        runtime_column: Name of runtime column
        
    Returns:
        Dictionary with total time in different units
    """
    total_mins = df[runtime_column].sum()
    
    return {
        'minutes': total_mins,
        'hours': total_mins / 60,
        'days': total_mins / (60 * 24),
        'weeks': total_mins / (60 * 24 * 7)
    }


# ============================================================================
# ACTOR/DIRECTOR ANALYSIS
# ============================================================================

def get_top_performers(df, column='directors', top_n=50):
    """
    Get top performers (actors/directors) by count.
    
    Args:
        df: DataFrame
        column: Column containing performer names (comma-separated)
        top_n: Number of top performers to return
        
    Returns:
        DataFrame with performer counts
    """
    # Parse and count
    if column == 'directors':
        parse_func = parse_directors
    else:
        parse_func = parse_actors
    
    all_performers = []
    for performers_string in df[column].dropna():
        all_performers.extend(parse_func(performers_string))
    
    # Count occurrences
    counter = Counter(all_performers)
    
    # Create DataFrame
    result = pd.DataFrame(counter.most_common(top_n), 
                         columns=['name', 'count'])
    
    return result


def calculate_performer_stats(df, performer_name, performer_column='directors'):
    """
    Calculate statistics for a specific performer.
    
    Args:
        df: DataFrame
        performer_name: Name of performer
        performer_column: Column to search in
        
    Returns:
        Dictionary with statistics
    """
    # Filter films with this performer
    if performer_column == 'directors':
        parse_func = parse_directors
    else:
        parse_func = parse_actors
    
    performer_films = df[
        df[performer_column].apply(
            lambda x: performer_name in parse_func(x) if pd.notna(x) else False
        )
    ].copy()
    
    if len(performer_films) == 0:
        return None
    
    stats = {
        'name': performer_name,
        'film_count': len(performer_films),
        'avg_your_rating': performer_films['your_rating'].mean() if 'your_rating' in performer_films.columns else None,
        'avg_imdb_rating': performer_films['imdb_rating'].mean() if 'imdb_rating' in performer_films.columns else None,
        'earliest_year': performer_films['year'].min() if 'year' in performer_films.columns else None,
        'latest_year': performer_films['year'].max() if 'year' in performer_films.columns else None,
        'genres': get_performer_genres(performer_films),
        'total_runtime': performer_films['runtime_mins'].sum() if 'runtime_mins' in performer_films.columns else None
    }
    
    return stats


def get_performer_genres(df):
    """
    Get all genres for a set of films.
    
    Args:
        df: DataFrame with genre column
        
    Returns:
        List of unique genres
    """
    all_genres = []
    for genre_string in df['genres'].dropna():
        all_genres.extend(parse_genres(genre_string))
    
    return list(set(all_genres))


# ============================================================================
# STATISTICAL HELPERS
# ============================================================================

def calculate_percentile_rank(value, series):
    """
    Calculate percentile rank of a value within a series.
    
    Args:
        value: Value to rank
        series: Pandas Series
        
    Returns:
        Percentile (0-100)
    """
    if pd.isna(value):
        return None
    
    return (series <= value).sum() / len(series) * 100


def detect_outliers_iqr(series, multiplier=1.5):
    """
    Detect outliers using IQR method.
    
    Args:
        series: Pandas Series
        multiplier: IQR multiplier (default 1.5)
        
    Returns:
        Boolean series indicating outliers
    """
    Q1 = series.quantile(0.25)
    Q3 = series.quantile(0.75)
    IQR = Q3 - Q1
    
    lower_bound = Q1 - multiplier * IQR
    upper_bound = Q3 + multiplier * IQR
    
    return (series < lower_bound) | (series > upper_bound)


def calculate_moving_average(series, window=10):
    """
    Calculate moving average for time series data.
    
    Args:
        series: Pandas Series
        window: Window size for moving average
        
    Returns:
        Series with moving average
    """
    return series.rolling(window=window, min_periods=1).mean()


# ============================================================================
# DATA CLEANING
# ============================================================================

def clean_text(text):
    """
    Clean text field by removing extra whitespace and special characters.
    
    Args:
        text: Text string
        
    Returns:
        Cleaned text
    """
    if pd.isna(text):
        return None
    
    text = str(text)
    
    # Remove extra whitespace
    text = ' '.join(text.split())
    
    # Remove common artifacts
    text = text.replace('\\N', '')
    text = text.replace('\\n', ' ')
    
    return text.strip()


def normalize_country_name(country):
    """
    Normalize country names to standard format.
    
    Args:
        country: Country name
        
    Returns:
        Normalized country name
    """
    if pd.isna(country):
        return None
    
    country = str(country).strip()
    
    # Common mappings
    country_map = {
        'USA': 'United States',
        'US': 'United States',
        'U.S.A.': 'United States',
        'UK': 'United Kingdom',
        'U.K.': 'United Kingdom',
        'Soviet Union': 'USSR',
        'Korea': 'South Korea'
    }
    
    return country_map.get(country, country)


def parse_budget_revenue(value_string):
    """
    Parse budget/revenue string to numeric value.
    
    Args:
        value_string: String like "$1,234,567" or "1234567"
        
    Returns:
        Numeric value
    """
    if pd.isna(value_string):
        return None
    
    # Remove currency symbols and commas
    value_string = str(value_string).replace('$', '').replace(',', '').strip()
    
    try:
        return float(value_string)
    except:
        return None


# ============================================================================
# VISUALIZATION HELPERS
# ============================================================================

def truncate_label(text, max_length=30):
    """
    Truncate long labels for visualization.
    
    Args:
        text: Text to truncate
        max_length: Maximum length
        
    Returns:
        Truncated text
    """
    if pd.isna(text):
        return ''
    
    text = str(text)
    if len(text) <= max_length:
        return text
    
    return text[:max_length-3] + '...'


def format_large_number(number):
    """
    Format large numbers for display (e.g., 1,234,567).
    
    Args:
        number: Number to format
        
    Returns:
        Formatted string
    """
    if pd.isna(number):
        return 'N/A'
    
    try:
        return f"{int(number):,}"
    except:
        return str(number)


def format_currency(amount):
    """
    Format currency amount.
    
    Args:
        amount: Numeric amount
        
    Returns:
        Formatted currency string
    """
    if pd.isna(amount):
        return 'N/A'
    
    amount = float(amount)
    
    if amount >= 1e9:
        return f"${amount/1e9:.1f}B"
    elif amount >= 1e6:
        return f"${amount/1e6:.1f}M"
    elif amount >= 1e3:
        return f"${amount/1e3:.1f}K"
    else:
        return f"${amount:.0f}"


def create_age_bins(ages, bin_size=10):
    """
    Create age bins for categorical analysis.
    
    Args:
        ages: Series of ages
        bin_size: Size of each bin
        
    Returns:
        Series with age categories
    """
    min_age = ages.min()
    max_age = ages.max()
    
    bins = list(range(int(min_age), int(max_age) + bin_size, bin_size))
    labels = [f"{b}-{b+bin_size-1}" for b in bins[:-1]]
    
    return pd.cut(ages, bins=bins, labels=labels, include_lowest=True)


# ============================================================================
# FRANCHISE & SERIES DETECTION
# ============================================================================

def detect_franchise(title):
    """
    Detect if a title is part of a franchise.
    
    Args:
        title: Movie title
        
    Returns:
        Franchise name or None
    """
    if pd.isna(title):
        return None
    
    title = str(title).lower()
    
    # Common franchise patterns
    franchise_patterns = {
        'Marvel Cinematic Universe': ['iron man', 'captain america', 'thor', 'avengers', 'guardians of the galaxy'],
        'Star Wars': ['star wars'],
        'Lord of the Rings': ['lord of the rings', 'hobbit'],
        'James Bond': ['james bond', '007'],
        'Harry Potter': ['harry potter', 'fantastic beasts'],
        'Fast & Furious': ['fast & furious', 'fast and furious'],
        'Mission Impossible': ['mission: impossible', 'mission impossible'],
        'Indiana Jones': ['indiana jones'],
        'Jurassic Park': ['jurassic park', 'jurassic world'],
        'Batman': ['batman', 'dark knight'],
        'Spider-Man': ['spider-man', 'spiderman']
    }
    
    for franchise, patterns in franchise_patterns.items():
        for pattern in patterns:
            if pattern in title:
                return franchise
    
    # Check for numbered sequels
    sequel_pattern = r'\b(2|3|II|III|IV|V|VI|VII|VIII|IX|X)\b'
    if re.search(sequel_pattern, title):
        # Extract base title
        base_title = re.sub(sequel_pattern, '', title).strip()
        base_title = re.sub(r':\s*$', '', base_title).strip()
        return f"{base_title} Series"
    
    return None


def extract_sequel_number(title):
    """
    Extract sequel number from title.
    
    Args:
        title: Movie title
        
    Returns:
        Sequel number or None
    """
    if pd.isna(title):
        return None
    
    title = str(title)
    
    # Roman numerals
    roman_map = {'II': 2, 'III': 3, 'IV': 4, 'V': 5, 'VI': 6, 'VII': 7, 'VIII': 8, 'IX': 9, 'X': 10}
    for roman, num in roman_map.items():
        if roman in title:
            return num
    
    # Arabic numerals
    match = re.search(r'\b([2-9]|10)\b', title)
    if match:
        return int(match.group(1))
    
    return 1  # Original if no number found


# ============================================================================
# EXPORT HELPERS
# ============================================================================

def export_to_csv(df, filename, output_dir):
    """
    Export DataFrame to CSV with proper formatting.
    
    Args:
        df: DataFrame to export
        filename: Output filename
        output_dir: Output directory path
        
    Returns:
        Path to exported file
    """
    output_path = Path(output_dir) / filename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(output_path, index=False)
    log_message(f"✅ Exported: {output_path}")
    
    return output_path


def create_summary_dict(df, title="Dataset Summary"):
    """
    Create a summary dictionary from a DataFrame.
    
    Args:
        df: DataFrame
        title: Summary title
        
    Returns:
        Dictionary with summary statistics
    """
    summary = {
        'title': title,
        'total_records': len(df),
        'total_columns': len(df.columns),
        'date_generated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # Add numeric summaries
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        summary[f'{col}_mean'] = df[col].mean()
        summary[f'{col}_median'] = df[col].median()
        summary[f'{col}_min'] = df[col].min()
        summary[f'{col}_max'] = df[col].max()
    
    return summary


# ============================================================================
# MODULE VALIDATION
# ============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("CineScope Helper Functions - Module Test")
    print("=" * 80 + "\n")
    
    # Test genre parsing
    test_genres = "Action, Drama, Thriller"
    parsed = parse_genres(test_genres)
    print(f"✓ Genre parsing: {test_genres} → {parsed}")
    
    # Test decade calculation
    test_year = 1994
    decade = get_decade(test_year)
    era = get_era(test_year)
    print(f"✓ Year {test_year} → Decade: {decade}, Era: {era}")
    
    # Test rating categorization
    test_rating = 8.5
    category = categorize_rating(test_rating)
    print(f"✓ Rating {test_rating} → Category: {category}")
    
    # Test runtime categorization
    test_runtime = 142
    runtime_cat = categorize_runtime(test_runtime)
    print(f"✓ Runtime {test_runtime} min → Category: {runtime_cat}")
    
    print("\n✅ All helper functions loaded successfully!\n")