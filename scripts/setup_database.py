"""
CineScope IMDb Database Setup

This script handles the download of IMDb's non-commercial datasets and
the creation of a local SQLite database from them. It's designed to be
run once to initialize the project's core data source.

Usage: python scripts/setup_database.py
"""
import sqlite3
import pandas as pd
import requests
import gzip
import sys
from pathlib import Path
import logging
from tqdm import tqdm

# Add the 'src' directory to the Python path to allow for absolute imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.core.config import settings

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def download_imdb_datasets():
    """
    Downloads the required IMDb gzipped TSV files if they don't already exist.
    """
    base_url = "https://datasets.imdbws.com/"
    files_to_download = [
        "title.basics.tsv.gz",
        "title.ratings.tsv.gz",
        "title.crew.tsv.gz",
        "name.basics.tsv.gz",
    ]

    settings.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("--- Checking IMDb Dataset Files ---")

    for filename in files_to_download:
        file_path = settings.RAW_DATA_DIR / filename
        if file_path.exists():
            logger.info(f"✅ '{filename}' already exists. Skipping download.")
            continue

        logger.info(f"Downloading '{filename}'...")
        try:
            response = requests.get(base_url + filename, stream=True)
            response.raise_for_status()
            total_size = int(response.headers.get('content-length', 0))

            with open(file_path, 'wb') as f, tqdm(
                desc=filename,
                total=total_size,
                unit='iB',
                unit_scale=True,
                unit_divisor=1024,
            ) as bar:
                for chunk in response.iter_content(chunk_size=8192):
                    size = f.write(chunk)
                    bar.update(size)
            logger.info(f"✅ Download complete for '{filename}'.")
        except requests.RequestException as e:
            logger.error(f"❌ Failed to download {filename}: {e}")
            if file_path.exists():
                file_path.unlink() # Clean up partial download
            sys.exit(1) # Exit if a crucial file fails to download

class IMDbDatabaseSetup:
    """Manages the creation and population of the IMDb SQLite database."""

    def __init__(self, db_path: Path, data_dir: Path):
        self.db_path = db_path
        self.data_dir = data_dir
        self.conn = None

    def __enter__(self):
        """Connect to the database upon entering the context."""
        self.conn = sqlite3.connect(self.db_path)
        logger.info(f"Opened database connection to: {self.db_path}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the database connection upon exiting the context."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed.")

    def create_tables(self):
        """Creates all necessary tables based on the IMDb dataset schemas."""
        logger.info("--- Creating Database Tables ---")
        cursor = self.conn.cursor()

        # title.basics
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS title_basics (
            tconst TEXT PRIMARY KEY,
            titleType TEXT,
            primaryTitle TEXT,
            originalTitle TEXT,
            isAdult INTEGER,
            startYear INTEGER,
            endYear INTEGER,
            runtimeMinutes INTEGER,
            genres TEXT
        );
        """)

        # title.ratings
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS title_ratings (
            tconst TEXT PRIMARY KEY,
            averageRating REAL,
            numVotes INTEGER,
            FOREIGN KEY (tconst) REFERENCES title_basics (tconst)
        );
        """)

        # title.crew
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS title_crew (
            tconst TEXT PRIMARY KEY,
            directors TEXT,
            writers TEXT,
            FOREIGN KEY (tconst) REFERENCES title_basics (tconst)
        );
        """)
        
        # name.basics
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS name_basics (
            nconst TEXT PRIMARY KEY,
            primaryName TEXT,
            birthYear INTEGER,
            deathYear INTEGER,
            primaryProfession TEXT,
            knownForTitles TEXT
        );
        """)
        self.conn.commit()
        logger.info("✅ All tables created successfully.")

    def import_tsv_to_table(self, file_name: str, table_name: str, columns: list, chunksize=50000):
        """
        Imports data from a gzipped TSV file into a specified database table.

        Args:
            file_name (str): The name of the .tsv.gz file.
            table_name (str): The name of the table to insert data into.
            columns (list): A list of column names for the data.
            chunksize (int): The number of rows to process at a time.
        """
        file_path = self.data_dir / file_name
        if not file_path.exists():
            logger.error(f"❌ Data file not found: {file_path}. Cannot import.")
            return

        logger.info(f"--- Importing data from '{file_name}' to table '{table_name}' ---")

        # Get total lines for tqdm progress bar
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            total_lines = sum(1 for line in f)

        # Process the file in chunks
        with gzip.open(file_path, 'rt', encoding='utf-8') as f_in:
            reader = pd.read_csv(
                f_in,
                sep='\\t',
                header=0,
                names=columns,
                na_values='\\\\N', # Correctly handle IMDb's null value representation
                chunksize=chunksize,
                quotechar='"',
                low_memory=False
            )

            with tqdm(total=total_lines, desc=f"Processing {file_name}") as pbar:
                for chunk in reader:
                    # Specific cleaning for title.basics to only include movies
                    if table_name == 'title_basics':
                        chunk = chunk[chunk['titleType'].isin(['movie', 'tvMovie', 'video'])]
                        chunk['startYear'] = pd.to_numeric(chunk['startYear'], errors='coerce')
                        chunk['runtimeMinutes'] = pd.to_numeric(chunk['runtimeMinutes'], errors='coerce')

                    chunk.to_sql(table_name, self.conn, if_exists='append', index=False)
                    pbar.update(len(chunk))
        
        logger.info(f"✅ Finished importing data into '{table_name}'.")

    def create_indexes(self):
        """Creates indexes on tables to speed up queries."""
        logger.info("--- Creating database indexes for faster queries ---")
        cursor = self.conn.cursor()
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_title_basics_primaryTitle ON title_basics(primaryTitle);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_name_basics_primaryName ON name_basics(primaryName);")
        self.conn.commit()
        logger.info("✅ Indexes created successfully.")

def main():
    """Main function to orchestrate the database setup."""
    logger.info("========== CineScope IMDb Database Setup ==========")
    settings.ensure_directories() # Make sure all needed folders exist

    # 1. Download Datasets
    download_imdb_datasets()

    # 2. Setup Database and Import Data
    with IMDbDatabaseSetup(settings.IMDB_DATABASE, settings.RAW_DATA_DIR) as db_setup:
        db_setup.create_tables()
        db_setup.import_tsv_to_table(
            "title.basics.tsv.gz", "title_basics",
            ['tconst', 'titleType', 'primaryTitle', 'originalTitle', 'isAdult', 'startYear', 'endYear', 'runtimeMinutes', 'genres']
        )
        db_setup.import_tsv_to_table(
            "title.ratings.tsv.gz", "title_ratings",
            ['tconst', 'averageRating', 'numVotes']
        )
        db_setup.import_tsv_to_table(
            "title.crew.tsv.gz", "title_crew",
            ['tconst', 'directors', 'writers']
        )
        db_setup.import_tsv_to_table(
            "name.basics.tsv.gz", "name_basics",
            ['nconst', 'primaryName', 'birthYear', 'deathYear', 'primaryProfession', 'knownForTitles']
        )
        db_setup.create_indexes()

    logger.info("========== ✅ Database setup complete! ==========")
    logger.info("You can now proceed with the data enrichment scripts.")


if __name__ == "__main__":
    main()