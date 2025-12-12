"""
CineScope Utilities - Common helper functions and decorators

This module provides reusable utilities for:
- Retry logic with exponential backoff
- Rate limiting
- Safe file operations
- Logging helpers
"""
import time
import functools
import logging
import json
import shutil
from pathlib import Path
from typing import Optional, Callable, Any, TypeVar, Dict
from datetime import datetime
import hashlib

logger = logging.getLogger(__name__)

T = TypeVar('T')


# =============================================================================
# RETRY DECORATOR WITH EXPONENTIAL BACKOFF
# =============================================================================

def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    exceptions: tuple = (Exception,),
    on_retry: Optional[Callable] = None
):
    """
    Decorator for retrying functions with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay in seconds
        max_delay: Maximum delay cap in seconds
        exponential_base: Base for exponential calculation
        exceptions: Tuple of exceptions to catch and retry
        on_retry: Optional callback function(attempt, exception, delay)
    
    Example:
        @retry_with_backoff(max_retries=3, exceptions=(requests.RequestException,))
        def fetch_data(url):
            return requests.get(url)
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        logger.error(f"All {max_retries} retries failed for {func.__name__}: {e}")
                        raise
                    
                    delay = min(base_delay * (exponential_base ** attempt), max_delay)
                    
                    if on_retry:
                        on_retry(attempt + 1, e, delay)
                    else:
                        logger.warning(
                            f"Retry {attempt + 1}/{max_retries} for {func.__name__} "
                            f"after {delay:.1f}s due to: {e}"
                        )
                    
                    time.sleep(delay)
            
            raise last_exception
        return wrapper
    return decorator


# =============================================================================
# RATE LIMITER
# =============================================================================

class RateLimiter:
    """
    Simple rate limiter to prevent API throttling.
    
    Example:
        limiter = RateLimiter(calls_per_second=10)
        for item in items:
            limiter.wait()
            api_call(item)
    """
    
    def __init__(self, calls_per_second: float = 10.0, burst_allowance: int = 0):
        """
        Initialize rate limiter.
        
        Args:
            calls_per_second: Maximum calls per second
            burst_allowance: Allow this many extra calls before limiting
        """
        self.min_interval = 1.0 / calls_per_second
        self.last_call = 0.0
        self.burst_allowance = burst_allowance
        self.burst_count = 0
    
    def wait(self):
        """Wait if necessary to maintain rate limit."""
        now = time.time()
        elapsed = now - self.last_call
        
        if elapsed < self.min_interval:
            if self.burst_count < self.burst_allowance:
                self.burst_count += 1
            else:
                sleep_time = self.min_interval - elapsed
                time.sleep(sleep_time)
                self.burst_count = 0
        else:
            self.burst_count = 0
        
        self.last_call = time.time()
    
    def __enter__(self):
        self.wait()
        return self
    
    def __exit__(self, *args):
        pass


# =============================================================================
# SAFE FILE OPERATIONS
# =============================================================================

def safe_write_json(filepath: Path, data: Any, backup: bool = True) -> bool:
    """
    Safely write JSON data with optional backup and atomic write.
    
    Args:
        filepath: Target file path
        data: Data to serialize as JSON
        backup: Whether to create backup of existing file
        
    Returns:
        True if successful, False otherwise
    """
    filepath = Path(filepath)
    temp_file = filepath.with_suffix('.json.tmp')
    backup_file = filepath.with_suffix('.json.bak')
    
    try:
        # Write to temp file first
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Create backup of existing file
        if backup and filepath.exists():
            shutil.copy2(filepath, backup_file)
        
        # Atomic rename
        temp_file.rename(filepath)
        
        logger.debug(f"Successfully wrote {filepath}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to write {filepath}: {e}")
        # Clean up temp file if it exists
        if temp_file.exists():
            temp_file.unlink()
        return False


def safe_read_json(filepath: Path, default: Any = None) -> Any:
    """
    Safely read JSON file with fallback to default.
    
    Args:
        filepath: File path to read
        default: Default value if file doesn't exist or is invalid
        
    Returns:
        Parsed JSON data or default value
    """
    filepath = Path(filepath)
    
    if not filepath.exists():
        logger.debug(f"File not found, using default: {filepath}")
        return default if default is not None else {}
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {filepath}: {e}")
        # Try backup file
        backup_file = filepath.with_suffix('.json.bak')
        if backup_file.exists():
            logger.info(f"Attempting to load backup: {backup_file}")
            try:
                with open(backup_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                pass
        return default if default is not None else {}
    except Exception as e:
        logger.error(f"Error reading {filepath}: {e}")
        return default if default is not None else {}


def ensure_directory(path: Path) -> Path:
    """
    Ensure directory exists, creating if necessary.
    
    Args:
        path: Directory path
        
    Returns:
        Path object (for chaining)
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


# =============================================================================
# CHECKPOINTING
# =============================================================================

class Checkpoint:
    """
    Checkpoint manager for long-running operations.
    
    Example:
        checkpoint = Checkpoint("enrichment_progress.json")
        checkpoint.load()
        
        for item in items:
            if checkpoint.is_done(item['id']):
                continue
            
            process(item)
            checkpoint.mark_done(item['id'])
            checkpoint.save_if_needed()
    """
    
    def __init__(self, filepath: Path, save_interval: int = 10):
        """
        Initialize checkpoint manager.
        
        Args:
            filepath: Path to checkpoint file
            save_interval: Save after this many operations
        """
        self.filepath = Path(filepath)
        self.save_interval = save_interval
        self.data: Dict = {
            'completed': set(),
            'failed': {},
            'metadata': {}
        }
        self.operations_since_save = 0
    
    def load(self) -> 'Checkpoint':
        """Load checkpoint from file."""
        raw_data = safe_read_json(self.filepath, {})
        self.data['completed'] = set(raw_data.get('completed', []))
        self.data['failed'] = raw_data.get('failed', {})
        self.data['metadata'] = raw_data.get('metadata', {})
        logger.info(f"Loaded checkpoint: {len(self.data['completed'])} completed items")
        return self
    
    def save(self) -> bool:
        """Save checkpoint to file."""
        save_data = {
            'completed': list(self.data['completed']),
            'failed': self.data['failed'],
            'metadata': self.data['metadata'],
            'last_saved': datetime.now().isoformat()
        }
        success = safe_write_json(self.filepath, save_data)
        if success:
            self.operations_since_save = 0
        return success
    
    def save_if_needed(self) -> bool:
        """Save if enough operations have occurred."""
        self.operations_since_save += 1
        if self.operations_since_save >= self.save_interval:
            return self.save()
        return True
    
    def is_done(self, item_id: str) -> bool:
        """Check if item is already completed."""
        return str(item_id) in self.data['completed']
    
    def mark_done(self, item_id: str):
        """Mark item as completed."""
        self.data['completed'].add(str(item_id))
    
    def mark_failed(self, item_id: str, error: str):
        """Mark item as failed with error message."""
        self.data['failed'][str(item_id)] = {
            'error': str(error),
            'timestamp': datetime.now().isoformat()
        }
    
    def get_progress(self) -> Dict:
        """Get progress statistics."""
        return {
            'completed': len(self.data['completed']),
            'failed': len(self.data['failed']),
            'last_saved': self.data.get('metadata', {}).get('last_saved')
        }


# =============================================================================
# HASHING & DEDUPLICATION
# =============================================================================

def generate_hash(data: Any) -> str:
    """
    Generate a consistent hash for data.
    
    Args:
        data: Any JSON-serializable data
        
    Returns:
        SHA256 hash string
    """
    if isinstance(data, dict):
        # Sort keys for consistent hashing
        data_str = json.dumps(data, sort_keys=True)
    else:
        data_str = str(data)
    
    return hashlib.sha256(data_str.encode()).hexdigest()[:16]


# =============================================================================
# LOGGING HELPERS
# =============================================================================

def setup_logging(
    name: str,
    level: int = logging.INFO,
    log_file: Optional[Path] = None,
    format_string: str = '%(asctime)s - %(levelname)s - %(message)s'
) -> logging.Logger:
    """
    Set up logging with consistent format.
    
    Args:
        name: Logger name
        level: Logging level
        log_file: Optional file to write logs to
        format_string: Log format string
        
    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    formatter = logging.Formatter(format_string)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


class ProgressLogger:
    """
    Log progress at regular intervals.
    
    Example:
        progress = ProgressLogger(total=1000, log_interval=100)
        for item in items:
            process(item)
            progress.update()
    """
    
    def __init__(self, total: int, log_interval: int = 100, name: str = "Progress"):
        self.total = total
        self.log_interval = log_interval
        self.name = name
        self.current = 0
        self.start_time = time.time()
    
    def update(self, count: int = 1):
        """Update progress counter."""
        self.current += count
        
        if self.current % self.log_interval == 0 or self.current == self.total:
            elapsed = time.time() - self.start_time
            rate = self.current / elapsed if elapsed > 0 else 0
            remaining = (self.total - self.current) / rate if rate > 0 else 0
            
            logger.info(
                f"{self.name}: {self.current}/{self.total} "
                f"({self.current/self.total*100:.1f}%) - "
                f"{rate:.1f}/s - ETA: {remaining/60:.1f}m"
            )


if __name__ == "__main__":
    # Quick self-test
    print("Running utility self-tests...")
    
    # Test rate limiter
    limiter = RateLimiter(calls_per_second=100)
    start = time.time()
    for _ in range(10):
        limiter.wait()
    elapsed = time.time() - start
    assert elapsed >= 0.09, f"Rate limiter too fast: {elapsed}"
    
    # Test retry decorator
    call_count = 0
    
    @retry_with_backoff(max_retries=2, base_delay=0.1)
    def flaky_function():
        global call_count
        call_count += 1
        if call_count < 3:
            raise ValueError("Simulated failure")
        return "success"
    
    result = flaky_function()
    assert result == "success"
    assert call_count == 3
    
    print("✅ All utility self-tests passed!")
