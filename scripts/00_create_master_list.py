"""
CineScope - Step 00: Create Master Media List

This script creates the master list by:
1. Loading your watched and collection movies
2. Enriching them with data from the IMDb database
3. Deduplicating and saving the complete master list

Usage:
    python scripts/00_create_master_list.py
"""
import sys
import pandas as pd
from pathlib import Path
import logging

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.core.config import settings
from src.core.db_connector import DBConnector

settings.ensure_directories()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(settings.LOG_FILE)
    ]
)
logger = logging.getLogger(__name__)

def load_from_database(db_path: Path, expected_table: str, source_name: str) -> pd.DataFrame:
    """Loads and standardizes data from a SQLite database."""
    if not db_path.exists():
        logger.warning(f"Database file not found: {db_path}. Skipping.")
        return pd.DataFrame()
        
    connector = DBConnector(db_path)
    tables = connector.get_table_names()
    
    if expected_table not in tables:
        logger.error(f"Table '{expected_table}' not found in {db_path}.")
        logger.info(f"Available tables: {tables}")
        return pd.DataFrame()
        
    df = connector.get_table_as_df(expected_table)
    
    # Standardize column names (assuming 'imdb_id' or 'const' exists)
    if 'imdb_id' in df.columns:
        df = df.rename(columns={'imdb_id': 'const'})
    
    if 'const' not in df.columns and 'tconst' in df.columns:
        df = df.rename(columns={'tconst': 'const'})
    
    if 'const' not in df.columns:
        logger.error(f"No IMDb ID column found in table '{expected_table}' from {db_path}.")
        return pd.DataFrame()
    
    # Ensure const is string type
    df['const'] = df['const'].astype(str)
    
    # Add source tracking
    df['data_source'] = source_name

    return df

def load_imdb_data(imdb_db_path: Path) -> pd.DataFrame:
    """Loads and combines all relevant IMDb data tables."""
    logger.info("--- Loading IMDb database for enrichment ---")
    
    # Load title basics
    basics_df = load_from_database(imdb_db_path, 'title_basics', 'imdb')
    if basics_df.empty:
        logger.warning("Could not load title_basics from IMDb database.")
        return pd.DataFrame()
    
    logger.info(f"Loaded {len(basics_df)} titles from IMDb title_basics")
    
    # Load ratings
    ratings_df = load_from_database(imdb_db_path, 'title_ratings', 'imdb')
    if not ratings_df.empty:
        logger.info(f"Loaded {len(ratings_df)} ratings from IMDb title_ratings")
        # Merge ratings with basics
        basics_df = basics_df.merge(
            ratings_df[['const', 'averageRating', 'numVotes']], 
            on='const', 
            how='left'
        )
    
    # Load crew
    crew_df = load_from_database(imdb_db_path, 'title_crew', 'imdb')
    if not crew_df.empty:
        logger.info(f"Loaded {len(crew_df)} crew records from IMDb title_crew")
        # Merge crew with basics
        basics_df = basics_df.merge(
            crew_df[['const', 'directors', 'writers']], 
            on='const', 
            how='left'
        )
    
    # Standardize column names to match your schema
    column_mapping = {
        'primaryTitle': 'title',
        'originalTitle': 'original_title',
        'startYear': 'year',
        'runtimeMinutes': 'runtime_mins',
        'averageRating': 'imdb_rating',
        'numVotes': 'num_votes',
        'titleType': 'title_type'
    }
    
    basics_df = basics_df.rename(columns=column_mapping)
    
    return basics_df

def main():
    """Main function to generate the master list."""
    logger.info("========== Step 00: Creating Master Media List ==========")
    
    # 1. Load your personal movie data
    logger.info("--- Loading your personal movie databases ---")
    
    watched_df = load_from_database(
        settings.RAW_DATA_DIR / 'watched_movies.db', 
        'movies',
        'watched'
    )
    logger.info(f"Loaded {len(watched_df)} items from watched_movies.db")

    collection_df = load_from_database(
        settings.RAW_DATA_DIR / 'collection_movies.db', 
        'movies',
        'collection'
    )
    logger.info(f"Loaded {len(collection_df)} items from collection_movies.db")

    if watched_df.empty and collection_df.empty:
        logger.error("No data loaded from personal databases. Cannot create master list.")
        sys.exit(1)

    # 2. Combine your personal data
    logger.info("--- Combining personal movie data ---")
    personal_df = pd.concat([watched_df, collection_df], ignore_index=True)
    
    # Track which sources each movie came from
    source_tracking = personal_df.groupby('const')['data_source'].apply(
        lambda x: ','.join(sorted(set(x)))
    ).to_dict()
    
    # Deduplicate personal data (watched takes priority over collection)
    personal_df = personal_df.drop_duplicates(subset='const', keep='first').copy()
    personal_df['personal_sources'] = personal_df['const'].map(source_tracking)
    personal_df = personal_df.drop(columns=['data_source'])
    
    logger.info(f"You have {len(personal_df)} unique movies in your personal databases")
    
    # 3. Load IMDb database for enrichment
    imdb_df = load_imdb_data(settings.IMDB_DATABASE)
    
    if imdb_df.empty:
        logger.warning("IMDb database not available. Using only personal data.")
        master_df = personal_df.copy()
    else:
        # 4. Enrich personal data with IMDb information
        logger.info("--- Enriching with IMDb data ---")
        
        # Get list of your movie IDs
        your_movie_ids = set(personal_df['const'].unique())
        
        # Filter IMDb data to only your movies
        imdb_enrichment = imdb_df[imdb_df['const'].isin(your_movie_ids)].copy()
        logger.info(f"Found {len(imdb_enrichment)} of your movies in IMDb database")
        
        # Merge personal data with IMDb enrichment
        # Personal data takes priority (suffixes indicate source)
        master_df = personal_df.merge(
            imdb_enrichment,
            on='const',
            how='left',
            suffixes=('', '_imdb')
        )
        
        # For columns that exist in both, use personal data if available, otherwise IMDb
        merge_columns = ['title', 'original_title', 'year', 'runtime_mins', 
                        'imdb_rating', 'num_votes', 'genres', 'directors', 'title_type']
        
        for col in merge_columns:
            personal_col = col
            imdb_col = f"{col}_imdb"
            
            if personal_col in master_df.columns and imdb_col in master_df.columns:
                # Fill missing personal data with IMDb data
                master_df[personal_col] = master_df[personal_col].fillna(master_df[imdb_col])
                # Drop the IMDb duplicate column
                master_df = master_df.drop(columns=[imdb_col])
        
        # Keep track of data sources
        master_df['data_source'] = 'imdb'
        master_df = master_df.drop(columns=['data_source'])
    
    # 5. Final data cleaning and type casting
    logger.info("--- Performing final data cleaning ---")
    
    # Ensure year is an integer
    if 'year' in master_df.columns:
        master_df.loc[:, 'year'] = pd.to_numeric(master_df['year'], errors='coerce').astype('Int64')
        
    # Ensure rating columns are floats
    for col in ['imdb_rating', 'your_rating']:
        if col in master_df.columns:
            master_df.loc[:, col] = pd.to_numeric(master_df[col], errors='coerce').round(1)
    
    # Ensure num_votes is integer
    if 'num_votes' in master_df.columns:
        master_df.loc[:, 'num_votes'] = pd.to_numeric(master_df['num_votes'], errors='coerce').astype('Int64')
    
    # Handle genres - convert to list if it's a string
    if 'genres' in master_df.columns:
        if master_df['genres'].dtype == 'object':
            master_df.loc[:, 'genres'] = master_df['genres'].apply(
                lambda x: x.split(',') if isinstance(x, str) and x else []
            )
            
    # Select core columns for the master list
    core_columns = [
        'const', 'title', 'original_title', 'year', 'runtime_mins', 
        'imdb_rating', 'your_rating', 'num_votes', 'genres', 'directors', 
        'writers', 'title_type', 'personal_sources', 'is_watched', 'is_in_collection'
    ]
    
    # Filter for columns that actually exist
    final_columns = [col for col in core_columns if col in master_df.columns]
    master_df = master_df[final_columns]
    
    # Ensure watched/collection flags are integers (0 or 1)
    for col in ['is_watched', 'is_in_collection']:
        if col in master_df.columns:
            master_df[col] = pd.to_numeric(master_df[col], errors='coerce').fillna(0).astype('int')

    # 6. Save the master list
    output_path = settings.PROCESSED_DATA_DIR / "master_media_list.csv"
    master_df.to_csv(output_path, index=False)
    
    # Calculate statistics
    if 'personal_sources' in master_df.columns:
        watched_only = len(master_df[master_df['personal_sources'] == 'watched'])
        collection_only = len(master_df[master_df['personal_sources'] == 'collection'])
        both = len(master_df[master_df['personal_sources'] == 'collection,watched'])
    else:
        watched_only = collection_only = both = 0
    
    logger.info("="*60)
    logger.info("✅ Master Media List created successfully!")
    logger.info(f"Saved {len(master_df)} unique items to: {output_path}")
    logger.info(f"")
    logger.info(f"  📊 Breakdown:")
    logger.info(f"  • Watched movies (not in collection): {watched_only}")
    logger.info(f"  • Collection movies (not watched): {collection_only}")
    logger.info(f"  • Movies you own AND watched: {both}")
    logger.info(f"")
    logger.info("You can now run the enrichment scripts (01, 02, 03).")
    logger.info("="*60)


if __name__ == "__main__":
    main()