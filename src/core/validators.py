"""
CineScope Data Validation Utilities

Centralized validation functions for data integrity checks.
"""
import re
import logging
from typing import Optional, Any, Dict, List, Union
from datetime import datetime

logger = logging.getLogger(__name__)


# =============================================================================
# IMDB ID VALIDATION
# =============================================================================

def is_valid_imdb_title_id(imdb_id: Any) -> bool:
    """
    Validate IMDB title ID format (tt followed by digits).
    
    Args:
        imdb_id: Value to validate
        
    Returns:
        True if valid IMDB title ID format
        
    Examples:
        >>> is_valid_imdb_title_id("tt0111161")
        True
        >>> is_valid_imdb_title_id("nm0000001")
        False
    """
    if imdb_id is None or not isinstance(imdb_id, str):
        return False
    return bool(re.match(r'^tt\d{5,}$', imdb_id))


def is_valid_imdb_person_id(imdb_id: Any) -> bool:
    """
    Validate IMDB person ID format (nm followed by digits).
    
    Args:
        imdb_id: Value to validate
        
    Returns:
        True if valid IMDB person ID format
    """
    if imdb_id is None or not isinstance(imdb_id, str):
        return False
    return bool(re.match(r'^nm\d{5,}$', imdb_id))


def normalize_imdb_id(imdb_id: Any) -> Optional[str]:
    """
    Normalize IMDB ID to standard format.
    
    Args:
        imdb_id: Raw IMDB ID value
        
    Returns:
        Normalized ID or None if invalid
    """
    if imdb_id is None:
        return None
    
    imdb_str = str(imdb_id).strip()
    
    # Already valid format
    if re.match(r'^(tt|nm)\d+$', imdb_str):
        return imdb_str
    
    # Just digits - assume title
    if re.match(r'^\d+$', imdb_str):
        return f"tt{imdb_str.zfill(7)}"
    
    return None


# =============================================================================
# DATA TYPE VALIDATION
# =============================================================================

def validate_year(value: Any, min_year: int = 1800, max_year: int = None) -> Optional[int]:
    """
    Validate and convert year value.
    
    Args:
        value: Year value to validate
        min_year: Minimum valid year (default 1800)
        max_year: Maximum valid year (default current year + 5)
        
    Returns:
        Valid year as int, or None if invalid
    """
    if max_year is None:
        max_year = datetime.now().year + 5
    
    if value is None or value == '\\N' or value == '':
        return None
    
    try:
        year = int(float(value))
        if min_year <= year <= max_year:
            return year
    except (ValueError, TypeError):
        pass
    
    return None


def validate_rating(value: Any, min_val: float = 0.0, max_val: float = 10.0) -> Optional[float]:
    """
    Validate rating value within range.
    
    Args:
        value: Rating value to validate
        min_val: Minimum valid rating
        max_val: Maximum valid rating
        
    Returns:
        Valid rating as float, or None if invalid
    """
    if value is None or value == '\\N' or value == '':
        return None
    
    try:
        rating = float(value)
        if min_val <= rating <= max_val:
            return round(rating, 2)
    except (ValueError, TypeError):
        pass
    
    return None


def validate_runtime(value: Any, max_runtime: int = 1000) -> Optional[int]:
    """
    Validate runtime in minutes.
    
    Args:
        value: Runtime value
        max_runtime: Maximum plausible runtime in minutes
        
    Returns:
        Valid runtime as int, or None if invalid
    """
    if value is None or value == '\\N' or value == '':
        return None
    
    try:
        runtime = int(float(value))
        if 0 < runtime <= max_runtime:
            return runtime
    except (ValueError, TypeError):
        pass
    
    return None


def validate_gender(value: Any) -> int:
    """
    Validate and normalize gender value.
    
    TMDB gender codes:
        0 = Unknown
        1 = Female
        2 = Male
        3 = Non-binary
        
    Args:
        value: Gender value to validate
        
    Returns:
        Normalized gender code (0 if invalid)
    """
    if value is None:
        return 0
    
    try:
        gender = int(value)
        if gender in (0, 1, 2, 3):
            return gender
    except (ValueError, TypeError):
        pass
    
    # Try string matching
    if isinstance(value, str):
        value_lower = value.lower().strip()
        if value_lower in ('female', 'f', 'woman'):
            return 1
        elif value_lower in ('male', 'm', 'man'):
            return 2
        elif value_lower in ('non-binary', 'nonbinary', 'nb'):
            return 3
    
    return 0


# =============================================================================
# STRING VALIDATION & CLEANING
# =============================================================================

def clean_string(value: Any, max_length: int = None) -> Optional[str]:
    """
    Clean and validate string value.
    
    Args:
        value: String value to clean
        max_length: Maximum allowed length
        
    Returns:
        Cleaned string or None if empty/invalid
    """
    if value is None or value == '\\N':
        return None
    
    if not isinstance(value, str):
        value = str(value)
    
    cleaned = value.strip()
    
    if not cleaned:
        return None
    
    if max_length and len(cleaned) > max_length:
        cleaned = cleaned[:max_length-3] + '...'
    
    return cleaned


def validate_name(name: Any) -> Optional[str]:
    """
    Validate person or title name.
    
    Args:
        name: Name to validate
        
    Returns:
        Valid name or None
    """
    cleaned = clean_string(name, max_length=500)
    
    if not cleaned:
        return None
    
    # Reject obviously invalid names
    if cleaned.lower() in ('unknown', 'n/a', 'none', 'null', '\\n'):
        return None
    
    # Should have at least one letter
    if not re.search(r'[a-zA-Z]', cleaned):
        return None
    
    return cleaned


# =============================================================================
# LIST/ARRAY VALIDATION
# =============================================================================

def validate_pipe_separated(value: Any, max_items: int = 100) -> List[str]:
    """
    Validate and parse pipe-separated string.
    
    Args:
        value: Pipe-separated string or list
        max_items: Maximum items to return
        
    Returns:
        List of cleaned strings
    """
    if value is None or value == '\\N':
        return []
    
    if isinstance(value, list):
        items = value
    elif isinstance(value, str):
        items = value.split('|')
    else:
        return []
    
    result = []
    for item in items[:max_items]:
        cleaned = clean_string(item)
        if cleaned:
            result.append(cleaned)
    
    return result


def validate_genres(genres: Any) -> List[str]:
    """
    Validate genre list/string.
    
    Args:
        genres: Genres as string, list, or pipe-separated
        
    Returns:
        List of valid genre strings
    """
    items = validate_pipe_separated(genres)
    
    # Common genre normalization
    normalized = []
    for genre in items:
        # Capitalize each word
        genre = ' '.join(word.capitalize() for word in genre.split())
        # Skip very short or very long "genres"
        if 2 <= len(genre) <= 50:
            normalized.append(genre)
    
    return normalized


# =============================================================================
# DATA RECORD VALIDATION
# =============================================================================

def validate_movie_record(record: Dict) -> Dict[str, List[str]]:
    """
    Validate a complete movie record and return issues found.
    
    Args:
        record: Dictionary with movie data
        
    Returns:
        Dictionary with field names and list of issues
    """
    issues = {}
    
    # Required fields
    if not record.get('const'):
        issues.setdefault('const', []).append('Missing IMDB ID')
    elif not is_valid_imdb_title_id(record['const']):
        issues.setdefault('const', []).append(f"Invalid format: {record['const']}")
    
    # Optional but validated fields
    if 'imdb_rating' in record:
        if validate_rating(record['imdb_rating']) is None and record['imdb_rating'] not in (None, '', '\\N'):
            issues.setdefault('imdb_rating', []).append(f"Invalid rating: {record['imdb_rating']}")
    
    if 'year' in record:
        if validate_year(record['year']) is None and record['year'] not in (None, '', '\\N'):
            issues.setdefault('year', []).append(f"Invalid year: {record['year']}")
    
    if 'runtime' in record:
        if validate_runtime(record['runtime']) is None and record['runtime'] not in (None, '', '\\N'):
            issues.setdefault('runtime', []).append(f"Invalid runtime: {record['runtime']}")
    
    return issues


def validate_person_record(record: Dict) -> Dict[str, List[str]]:
    """
    Validate a complete person record and return issues found.
    
    Args:
        record: Dictionary with person data
        
    Returns:
        Dictionary with field names and list of issues
    """
    issues = {}
    
    # Check for valid IMDB ID if present
    if record.get('imdb_id'):
        if not is_valid_imdb_person_id(record['imdb_id']):
            issues.setdefault('imdb_id', []).append(f"Invalid format: {record['imdb_id']}")
    
    # Validate gender
    gender = record.get('gender')
    if gender is not None and validate_gender(gender) == 0 and gender not in (0, '0'):
        issues.setdefault('gender', []).append(f"Invalid gender value: {gender}")
    
    # Validate birth year if present
    if record.get('birthday'):
        try:
            year = int(str(record['birthday'])[:4])
            if not (1800 <= year <= datetime.now().year):
                issues.setdefault('birthday', []).append(f"Invalid birth year: {year}")
        except (ValueError, TypeError):
            pass
    
    return issues


# =============================================================================
# BATCH VALIDATION
# =============================================================================

def validate_dataframe(df, record_type: str = 'movie') -> Dict:
    """
    Validate entire dataframe and return summary statistics.
    
    Args:
        df: Pandas DataFrame to validate
        record_type: 'movie' or 'person'
        
    Returns:
        Dictionary with validation statistics
    """
    stats = {
        'total_records': len(df),
        'valid_records': 0,
        'records_with_issues': 0,
        'issues_by_field': {},
        'sample_issues': []
    }
    
    validator = validate_movie_record if record_type == 'movie' else validate_person_record
    
    for idx, row in df.iterrows():
        record = row.to_dict()
        issues = validator(record)
        
        if issues:
            stats['records_with_issues'] += 1
            for field, field_issues in issues.items():
                stats['issues_by_field'].setdefault(field, 0)
                stats['issues_by_field'][field] += 1
            
            if len(stats['sample_issues']) < 10:
                stats['sample_issues'].append({
                    'index': idx,
                    'issues': issues
                })
        else:
            stats['valid_records'] += 1
    
    return stats


if __name__ == "__main__":
    # Quick self-test
    print("Running validation self-tests...")
    
    assert is_valid_imdb_title_id("tt0111161") == True
    assert is_valid_imdb_title_id("nm0000001") == False
    assert is_valid_imdb_person_id("nm0000001") == True
    assert validate_year(1994) == 1994
    assert validate_year("\\N") == None
    assert validate_rating(8.5) == 8.5
    assert validate_gender(1) == 1
    assert validate_gender("female") == 1
    
    print("✅ All self-tests passed!")
