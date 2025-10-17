"""
CineScope Configuration Module

This module reads the .env file and exposes the configuration variables
as a singleton `settings` object. It uses Pydantic for type validation
and Pathlib for robust path management.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

class Settings:
    """
    A singleton class to hold all project settings.
    """
    def __init__(self):
        # Define Project Root
        # This makes all paths relative to the project's base directory
        self.PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

        # Load environment variables from .env file
        load_dotenv(self.PROJECT_ROOT / '.env')

        # API Keys
        self.TMDB_API_KEY = os.getenv("TMDB_API_KEY")
        self.TMDB_READ_TOKEN = os.getenv("TMDB_READ_TOKEN")
        self.OMDB_API_KEY = os.getenv("OMDB_API_KEY")
        self.DDD_API_KEY = os.getenv("DDD_API_KEY")

        # Database Configuration
        self.DATABASE_URL = os.getenv("DATABASE_URL")

        # Core Paths
        self.DATA_DIR = self.PROJECT_ROOT / 'data'
        self.RAW_DATA_DIR = self.DATA_DIR / 'raw'
        self.PROCESSED_DATA_DIR = self.DATA_DIR / 'processed'
        self.REPORTS_DIR = self.DATA_DIR / 'reports'
        self.LOGS_DIR = self.DATA_DIR / 'logs'

        # File Paths
        self.WATCHLIST_FILE = self.RAW_DATA_DIR / 'Watchlist_IMDB.csv'
        self.IMDB_DATABASE = self.RAW_DATA_DIR / 'imdb.db'

        # Logging
        self.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
        self.LOG_FILE = self.LOGS_DIR / 'cinescope.log'

        # Recommendation Engine Settings
        self.MIN_RATING_THRESHOLD = float(os.getenv("MIN_RATING_THRESHOLD", 6.0))
        self.TOP_N_RECOMMENDATIONS = int(os.getenv("TOP_N_RECOMMENDATIONS", 20))
        self.SIMILARITY_THRESHOLD = float(os.getenv("SIMILARITY_THRESHOLD", 0.3))

        # External API Rate Limiting
        self.TMDB_RATE_LIMIT = int(os.getenv("TMDB_RATE_LIMIT", 40))
        self.OMDB_RATE_LIMIT = int(os.getenv("OMDB_RATE_LIMIT", 1000))

    def ensure_directories(self):
        """
        Creates all necessary directories if they don't exist.
        This is useful to run at the start of scripts.
        """
        dirs_to_create = [
            self.DATA_DIR,
            self.RAW_DATA_DIR,
            self.PROCESSED_DATA_DIR,
            self.REPORTS_DIR,
            self.LOGS_DIR,
        ]
        for directory in dirs_to_create:
            directory.mkdir(parents=True, exist_ok=True)
        print("✅ Core directories ensured.")


# Create a single, importable instance of the settings
settings = Settings()

# Example of how to use this module in other files:
# from src.core.config import settings
# print(settings.TMDB_API_KEY)
# settings.ensure_directories()