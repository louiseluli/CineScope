"""
CineScope Custom Exceptions

Centralized exception definitions for better error handling and debugging.
"""


class CineScopeError(Exception):
    """Base exception for all CineScope errors."""
    
    def __init__(self, message: str, details: dict = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}
    
    def __str__(self):
        if self.details:
            return f"{self.message} | Details: {self.details}"
        return self.message


# =============================================================================
# API ERRORS
# =============================================================================

class APIError(CineScopeError):
    """Base class for API-related errors."""
    pass


class RateLimitError(APIError):
    """Raised when API rate limit is exceeded."""
    
    def __init__(self, service: str, retry_after: int = None):
        message = f"{service} rate limit exceeded"
        details = {'service': service}
        if retry_after:
            details['retry_after_seconds'] = retry_after
            message += f" - retry after {retry_after}s"
        super().__init__(message, details)


class APIKeyMissingError(APIError):
    """Raised when required API key is not configured."""
    
    def __init__(self, service: str):
        super().__init__(
            f"API key not configured for {service}. "
            f"Please set {service.upper()}_API_KEY in your .env file.",
            {'service': service}
        )


class APIConnectionError(APIError):
    """Raised when API connection fails."""
    
    def __init__(self, service: str, url: str = None, original_error: Exception = None):
        message = f"Failed to connect to {service}"
        details = {'service': service}
        if url:
            details['url'] = url
        if original_error:
            details['original_error'] = str(original_error)
        super().__init__(message, details)


class APIResponseError(APIError):
    """Raised when API returns unexpected response."""
    
    def __init__(self, service: str, status_code: int = None, response_body: str = None):
        message = f"Unexpected response from {service}"
        details = {'service': service}
        if status_code:
            details['status_code'] = status_code
            message += f" (HTTP {status_code})"
        if response_body:
            details['response'] = response_body[:500]  # Truncate long responses
        super().__init__(message, details)


# =============================================================================
# DATA ERRORS
# =============================================================================

class DataError(CineScopeError):
    """Base class for data-related errors."""
    pass


class DataNotFoundError(DataError):
    """Raised when required data file or record is not found."""
    
    def __init__(self, resource_type: str, identifier: str = None, path: str = None):
        message = f"{resource_type} not found"
        details = {'resource_type': resource_type}
        if identifier:
            details['identifier'] = identifier
            message += f": {identifier}"
        if path:
            details['path'] = path
        super().__init__(message, details)


class DataValidationError(DataError):
    """Raised when data fails validation."""
    
    def __init__(self, field: str, value: any, expected: str = None):
        message = f"Invalid value for {field}: {value!r}"
        details = {'field': field, 'value': str(value)[:100]}
        if expected:
            details['expected'] = expected
            message += f" (expected: {expected})"
        super().__init__(message, details)


class DataParsingError(DataError):
    """Raised when data cannot be parsed."""
    
    def __init__(self, source: str, line_number: int = None, original_error: Exception = None):
        message = f"Failed to parse data from {source}"
        details = {'source': source}
        if line_number:
            details['line_number'] = line_number
            message += f" at line {line_number}"
        if original_error:
            details['original_error'] = str(original_error)
        super().__init__(message, details)


class DataDuplicateError(DataError):
    """Raised when duplicate data is detected."""
    
    def __init__(self, identifier: str, existing_record: dict = None):
        message = f"Duplicate record found: {identifier}"
        details = {'identifier': identifier}
        if existing_record:
            details['existing_record'] = str(existing_record)[:200]
        super().__init__(message, details)


# =============================================================================
# ENRICHMENT ERRORS
# =============================================================================

class EnrichmentError(CineScopeError):
    """Base class for enrichment-related errors."""
    pass


class IMDBLookupError(EnrichmentError):
    """Raised when IMDB lookup fails."""
    
    def __init__(self, identifier: str, lookup_type: str = "person"):
        super().__init__(
            f"IMDB {lookup_type} lookup failed for: {identifier}",
            {'identifier': identifier, 'lookup_type': lookup_type}
        )


class TMDBLookupError(EnrichmentError):
    """Raised when TMDB lookup fails."""
    
    def __init__(self, tmdb_id: int, resource_type: str = "person"):
        super().__init__(
            f"TMDB {resource_type} lookup failed for ID: {tmdb_id}",
            {'tmdb_id': tmdb_id, 'resource_type': resource_type}
        )


class WikidataLookupError(EnrichmentError):
    """Raised when Wikidata SPARQL query fails."""
    
    def __init__(self, query_type: str, identifier: str = None, original_error: Exception = None):
        message = f"Wikidata {query_type} query failed"
        details = {'query_type': query_type}
        if identifier:
            details['identifier'] = identifier
        if original_error:
            details['original_error'] = str(original_error)
        super().__init__(message, details)


class GenderResolutionError(EnrichmentError):
    """Raised when gender resolution fails."""
    
    def __init__(self, person_name: str, sources_tried: list = None):
        message = f"Could not resolve gender for: {person_name}"
        details = {'person_name': person_name}
        if sources_tried:
            details['sources_tried'] = sources_tried
        super().__init__(message, details)


# =============================================================================
# FILE ERRORS
# =============================================================================

class FileError(CineScopeError):
    """Base class for file-related errors."""
    pass


class CheckpointError(FileError):
    """Raised when checkpoint save/load fails."""
    
    def __init__(self, operation: str, filepath: str, original_error: Exception = None):
        message = f"Checkpoint {operation} failed: {filepath}"
        details = {'operation': operation, 'filepath': filepath}
        if original_error:
            details['original_error'] = str(original_error)
        super().__init__(message, details)


class CacheCorruptionError(FileError):
    """Raised when cache file is corrupted."""
    
    def __init__(self, filepath: str, reason: str = None):
        message = f"Cache file corrupted: {filepath}"
        details = {'filepath': filepath}
        if reason:
            details['reason'] = reason
        super().__init__(message, details)


# =============================================================================
# VISUALIZATION ERRORS
# =============================================================================

class VisualizationError(CineScopeError):
    """Raised when visualization generation fails."""
    
    def __init__(self, chart_type: str, reason: str = None, original_error: Exception = None):
        message = f"Failed to generate {chart_type} visualization"
        details = {'chart_type': chart_type}
        if reason:
            details['reason'] = reason
        if original_error:
            details['original_error'] = str(original_error)
        super().__init__(message, details)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def handle_api_exception(e: Exception, service: str, url: str = None) -> APIError:
    """
    Convert a generic exception to an appropriate API error.
    
    Args:
        e: Original exception
        service: Name of the API service
        url: Optional URL that was being accessed
        
    Returns:
        Appropriate APIError subclass
    """
    error_str = str(e).lower()
    
    if 'rate' in error_str or '429' in error_str:
        return RateLimitError(service)
    elif 'timeout' in error_str or 'timed out' in error_str:
        return APIConnectionError(service, url, e)
    elif 'connection' in error_str or 'network' in error_str:
        return APIConnectionError(service, url, e)
    else:
        return APIError(f"{service} error: {e}", {'original_error': str(e)})
