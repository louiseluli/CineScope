"""
CineScope Data Loader Module

This module is responsible for loading and performing initial cleaning on the
IMDB Watchlist CSV. It ensures data types are correct and provides a
standardized entry point for accessing the raw user data.
"""
import pandas as pd
from pathlib import Path
import logging
import sys

# Add 'src' to the Python path to find the config module
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from core.config import settings

# Configure logging
logging.basicConfig(level=settings.LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataLoader:
    """
    Handles loading the user's movie watchlist data.
    """
    def __init__(self, filepath: Path = settings.WATCHLIST_FILE):
        """
        Initializes the DataLoader.

        Args:
            filepath (Path): The path to the watchlist CSV file.
        """
        self.filepath = filepath
        if not self.filepath.exists():
            logger.error(f"Watchlist file not found at: {self.filepath}")
            raise FileNotFoundError(f"Watchlist file not found at: {self.filepath}")

    def load_watchlist(self) -> pd.DataFrame:
        """
        Loads the watchlist CSV into a pandas DataFrame and performs
        initial cleaning and type casting.

        Returns:
            pd.DataFrame: A DataFrame containing the cleaned watchlist data.
        """
        logger.info(f"Loading watchlist from: {self.filepath}")

        try:
            df = pd.read_csv(self.filepath)

            # --- Data Cleaning and Preprocessing ---

            # Rename columns for consistency
            df.columns = [
                'position', 'const', 'created', 'modified', 'description', 'title',
                'original_title', 'url', 'title_type', 'imdb_rating',
                'runtime_mins', 'year', 'genres', 'num_votes', 'release_date',
                'directors', 'your_rating', 'date_rated'
            ]

            # Convert date columns to datetime objects
            date_cols = ['created', 'modified', 'release_date', 'date_rated']
            for col in date_cols:
                df[col] = pd.to_datetime(df[col], errors='coerce')

            # Convert numeric columns
            numeric_cols = ['imdb_rating', 'runtime_mins', 'year', 'num_votes', 'your_rating']
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # Ensure 'const' (IMDb ID) is always a string
            df['const'] = df['const'].astype(str)

            # Split genres into a list of strings, handling potential nulls
            df['genres'] = df['genres'].str.split(', ').fillna("").apply(list)
            
            # Drop the 'Description' column if it's empty
            if 'description' in df.columns and df['description'].isnull().all():
                df = df.drop(columns=['description'])


            logger.info(f"✅ Watchlist loaded successfully. Found {len(df)} items.")
            return df

        except Exception as e:
            logger.error(f"Failed to load or process watchlist file: {e}")
            raise