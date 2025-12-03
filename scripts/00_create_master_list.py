"""
CineScope - Step 00: Create Master Media List

This script creates the master list by:
1. Loading your WATCHED movies from 'Watched-Dec.csv'
2. Loading your COLLECTION movies from 'ground_truth_validated.csv' (Ground Truth)
3. Merging them based on IMDb ID (const) to find overlaps
4. Enriching with basic IMDb data and saving the master list

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

# Expanded mapping to handle various CSV headers (standard + verification formats)
COLUMN_MAPPING = {
    # Standard
    'Const': 'const',
    'const': 'const',
    'imdb_id': 'const',
    'IMDb ID': 'const',
    'tconst': 'const',
    
    # Verification / Ground Truth Files
    'Current_IMDb_ID': 'const',
    'New_IMDb_ID': 'const',
    'Correct_IMDb_ID': 'const',
    
    # Metadata
    'Title': 'title',
    'Current_Title': 'title',
    'Original Title': 'original_title',
    'Year': 'year',
    'Current_Year': 'year',
    'Runtime (mins)': 'runtime_mins',
    'IMDb Rating': 'imdb_rating',
    'Your Rating': 'your_rating',
    'Num Votes': 'num_votes',
    'Genres': 'genres',
    'Current_Genres': 'genres',
    'Directors': 'directors',
    'Title Type': 'title_type',
    'URL': 'url',
    'Date Rated': 'date_rated'
}

def clean_and_normalize(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """Standardizes column names and types for any dataframe."""
    # 1. Rename columns using the mapping
    df = df.rename(columns=COLUMN_MAPPING)
    
    # 2. Remove duplicate columns (e.g., if 'Const' and 'tconst' both existed)
    df = df.loc[:, ~df.columns.duplicated()]
    
    # 3. Check for 'const' column
    if 'const' not in df.columns:
        logger.warning(f"⚠️  No IMDb ID column found in {source_name} data. Columns: {list(df.columns)}")
        return pd.DataFrame() 
    
    # 4. Filter out empty IDs
    df = df.dropna(subset=['const'])
    df = df[df['const'].astype(str).str.startswith('tt')]
    
    # 5. Ensure const is string
    df['const'] = df['const'].astype(str).str.strip()
    
    # 6. Clean up genres
    if 'genres' in df.columns and df['genres'].dtype == 'object':
         df['genres'] = df['genres'].fillna('')

    # 7. Add source tracking
    df['data_source'] = source_name
    return df

def load_from_csv(file_path: Path, source_name: str) -> pd.DataFrame:
    """Loads and standardizes data from a CSV file."""
    if not file_path.exists():
        logger.error(f"❌ File not found: {file_path}")
        return pd.DataFrame()
    
    logger.info(f"Loading {source_name} from CSV: {file_path.name}")
    try:
        # Read CSV (handle potential encoding issues if needed)
        df = pd.read_csv(file_path, low_memory=False)
        return clean_and_normalize(df, source_name)
    except Exception as e:
        logger.error(f"Error reading CSV {file_path}: {e}")
        return pd.DataFrame()

def load_from_database(db_path: Path, expected_table: str, source_name: str) -> pd.DataFrame:
    """Loads and standardizes data from a SQLite database (Used for IMDb cache)."""
    if not db_path.exists():
        logger.warning(f"Database file not found: {db_path}. Skipping.")
        return pd.DataFrame()
        
    connector = DBConnector(db_path)
    tables = connector.get_table_names()
    
    if expected_table not in tables:
        # Silent return if table missing, as we might not use this for personal data anymore
        return pd.DataFrame()
        
    df = connector.get_table_as_df(expected_table)
    return clean_and_normalize(df, source_name)

def load_imdb_data(imdb_db_path: Path) -> pd.DataFrame:
    """Loads and combines all relevant IMDb data tables."""
    logger.info("--- Loading IMDb database for enrichment ---")
    
    # Load title basics
    basics_df = load_from_database(imdb_db_path, 'title_basics', 'imdb')
    if basics_df.empty:
        logger.warning("Could not load title_basics from IMDb database.")
        return pd.DataFrame()
    
    # Load ratings
    ratings_df = load_from_database(imdb_db_path, 'title_ratings', 'imdb')
    if not ratings_df.empty:
        # Standardize rating columns manually as they differ in raw IMDb tables
        ratings_df = ratings_df.rename(columns={'averageRating': 'imdb_rating', 'numVotes': 'num_votes'})
        ratings_df = ratings_df.loc[:, ~ratings_df.columns.duplicated()]
        
        # Merge ratings with basics
        basics_df = basics_df.merge(
            ratings_df[['const', 'imdb_rating', 'num_votes']], 
            on='const', 
            how='left'
        )
    
    # Load crew
    crew_df = load_from_database(imdb_db_path, 'title_crew', 'imdb')
    if not crew_df.empty:
        # Merge crew with basics
        basics_df = basics_df.merge(
            crew_df[['const', 'directors', 'writers']], 
            on='const', 
            how='left'
        )
    
    # Ensure correct internal names
    imdb_mapping = {
        'primaryTitle': 'title',
        'originalTitle': 'original_title',
        'startYear': 'year',
        'runtimeMinutes': 'runtime_mins',
        'titleType': 'title_type'
    }
    
    basics_df = basics_df.rename(columns=imdb_mapping)
    basics_df = basics_df.loc[:, ~basics_df.columns.duplicated()]
    
    return basics_df

def main():
    logger.info("========== Step 00: Creating Master Media List ==========")
    
    # 1. Load data from specified sources
    logger.info("--- Loading Personal Data ---")
    
    # SOURCE 1: WATCHED (CSV)
    watched_path = settings.RAW_DATA_DIR / 'Watched-Dec.csv'
    watched_df = load_from_csv(watched_path, 'watched')
    
    # SOURCE 2: COLLECTION (Ground Truth CSV)
    collection_path = settings.RAW_DATA_DIR / 'ground_truth_validated.csv'
    collection_df = load_from_csv(collection_path, 'collection')

    logger.info(f"Loaded {len(watched_df)} items from Watched CSV")
    logger.info(f"Loaded {len(collection_df)} items from Collection CSV")

    if watched_df.empty and collection_df.empty:
        logger.error("No data loaded from personal sources. Cannot create master list.")
        sys.exit(1)

    # 2. Combine personal data
    logger.info("--- Combining and calculating overlaps ---")
    
    # Drop existing flags to force recalculation
    cols_to_drop = ['is_watched', 'is_in_collection', 'personal_sources']
    if not watched_df.empty:
        watched_df = watched_df.drop(columns=[c for c in cols_to_drop if c in watched_df.columns])
    if not collection_df.empty:
        collection_df = collection_df.drop(columns=[c for c in cols_to_drop if c in collection_df.columns])

    # Concatenate
    personal_df = pd.concat([watched_df, collection_df], ignore_index=True)
    
    # Group by ID to find sources
    # This detects if an ID appears in 'watched', 'collection', or both
    source_tracking = personal_df.groupby('const')['data_source'].apply(
        lambda x: ','.join(sorted(set(x)))
    ).to_dict()
    
    # Deduplicate (keep first occurrence)
    personal_df = personal_df.drop_duplicates(subset='const', keep='first').copy()
    
    # Apply source flags
    personal_df['personal_sources'] = personal_df['const'].map(source_tracking)
    personal_df = personal_df.drop(columns=['data_source'])
    
    logger.info(f"Total unique movies across all sources: {len(personal_df)}")
    
    # 3. Load IMDb database for enrichment
    imdb_df = load_imdb_data(settings.IMDB_DATABASE)
    
    if imdb_df.empty:
        logger.warning("IMDb database not available. Using only personal data.")
        master_df = personal_df.copy()
    else:
        # 4. Enrich personal data with IMDb information
        logger.info("--- Enriching with IMDb data ---")
        
        your_movie_ids = set(personal_df['const'].unique())
        imdb_enrichment = imdb_df[imdb_df['const'].isin(your_movie_ids)].copy()
        
        # Merge (Left Join to keep all personal movies)
        master_df = personal_df.merge(
            imdb_enrichment,
            on='const',
            how='left',
            suffixes=('', '_imdb')
        )
        
        # Fill missing data from IMDb
        merge_columns = ['title', 'original_title', 'year', 'runtime_mins', 
                        'imdb_rating', 'num_votes', 'genres', 'directors', 'title_type']
        
        for col in merge_columns:
            personal_col = col
            imdb_col = f"{col}_imdb"
            
            if personal_col in master_df.columns and imdb_col in master_df.columns:
                master_df[personal_col] = master_df[personal_col].fillna(master_df[imdb_col])
                master_df = master_df.drop(columns=[imdb_col])
    
    # 5. Final data cleaning
    logger.info("--- Performing final data cleaning ---")
    
    # Type casting
    numeric_cols = {'year': 'Int64', 'num_votes': 'Int64', 'imdb_rating': 'float', 'your_rating': 'float'}
    for col, dtype in numeric_cols.items():
        if col in master_df.columns:
            if dtype == 'float':
                master_df[col] = pd.to_numeric(master_df[col], errors='coerce').round(1)
            else:
                master_df[col] = pd.to_numeric(master_df[col], errors='coerce').astype(dtype)
    
    # Clean genres
    if 'genres' in master_df.columns:
        master_df.loc[:, 'genres'] = master_df['genres'].apply(
            lambda x: x.split(',') if isinstance(x, str) and x else (x if isinstance(x, list) else [])
        )
            
    # Set Flags based on source tracking
    master_df['is_watched'] = master_df['personal_sources'].astype(str).str.contains('watched').astype(int)
    master_df['is_in_collection'] = master_df['personal_sources'].astype(str).str.contains('collection').astype(int)
    
    # Select columns
    core_columns = [
        'const', 'title', 'original_title', 'year', 'runtime_mins', 
        'imdb_rating', 'your_rating', 'date_rated', 'num_votes', 'genres', 'directors', 
        'writers', 'title_type', 'personal_sources', 'is_watched', 'is_in_collection'
    ]
    final_columns = [col for col in core_columns if col in master_df.columns]
    master_df = master_df[final_columns]

    # 6. Save
    output_path = settings.PROCESSED_DATA_DIR / "master_media_list.csv"
    master_df.to_csv(output_path, index=False)
    
    # Validation stats
    watched_only = len(master_df[(master_df['is_watched'] == 1) & (master_df['is_in_collection'] == 0)])
    collection_only = len(master_df[(master_df['is_watched'] == 0) & (master_df['is_in_collection'] == 1)])
    both = len(master_df[(master_df['is_watched'] == 1) & (master_df['is_in_collection'] == 1)])
    
    logger.info("="*60)
    logger.info("✅ Master Media List created successfully!")
    logger.info(f"Saved {len(master_df)} items to: {output_path}")
    logger.info(f"")
    logger.info(f"  📊 Verification:")
    logger.info(f"  • Watched ONLY (not in collection): {watched_only}")
    logger.info(f"  • Collection ONLY (owned but NOT watched): {collection_only}")
    logger.info(f"  • Collection AND Watched (owned and seen): {both}")
    logger.info(f"")
    logger.info("You can now run the enrichment scripts.")
    logger.info("="*60)

if __name__ == "__main__":
    main()