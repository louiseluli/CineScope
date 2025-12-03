"""
CineScope Master List Verification

This script verifies the master list and shows statistics about data sources.

Usage:
    python verify_master_list.py
"""
import pandas as pd
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.core.config import settings
from src.core.data_loader import DataLoader
from src.core.db_connector import DBConnector

def load_from_database(db_path: Path, expected_table: str) -> pd.DataFrame:
    """Loads and standardizes data from a SQLite database."""
    if not db_path.exists():
        print(f"⚠️  Database file not found: {db_path}")
        return pd.DataFrame()
        
    connector = DBConnector(db_path)
    tables = connector.get_table_names()
    
    if expected_table not in tables:
        print(f"⚠️  Table '{expected_table}' not found in {db_path}")
        print(f"   Available tables: {tables}")
        return pd.DataFrame()
        
    df = connector.get_table_as_df(expected_table)
    
    # Standardize column names
    if 'imdb_id' in df.columns:
        df = df.rename(columns={'imdb_id': 'const'})
    
    if 'const' not in df.columns:
        print(f"⚠️  'const' or 'imdb_id' column not found in {expected_table}")
        return pd.DataFrame()

    return df

def main():
    print("="*60)
    print("CineScope Master List Verification")
    print("="*60)
    
    # Load all sources
    print("\n📊 Loading data from all sources...")
    
    loader = DataLoader()
    watchlist_df = loader.load_watchlist()
    print(f"✅ Watchlist: {len(watchlist_df)} items")
    
    collection_df = load_from_database(
        settings.RAW_DATA_DIR / 'collection_movies.db', 
        'movies'
    )
    print(f"✅ Collection: {len(collection_df)} items")
    
    watched_df = load_from_database(
        settings.RAW_DATA_DIR / 'watched_movies.db', 
        'movies'
    )
    print(f"✅ Watched: {len(watched_df)} items")
    
    # Check master list
    master_file = settings.PROCESSED_DATA_DIR / "master_media_list.csv"
    if master_file.exists():
        master_df = pd.read_csv(master_file)
        print(f"\n📋 Current Master List: {len(master_df)} items")
    else:
        print(f"\n⚠️  Master list not found at: {master_file}")
        master_df = pd.DataFrame()
    
    # Calculate expected total
    combined_df = pd.concat([watchlist_df, collection_df, watched_df], ignore_index=True)
    combined_df['const'] = combined_df['const'].astype(str)
    expected_unique = combined_df['const'].nunique()
    
    print(f"\n📈 Statistics:")
    print(f"   Total items across all sources: {len(combined_df)}")
    
    # Check for duplicate sources
    watchlist_ids = set(watchlist_df['const'].astype(str))
    collection_ids = set(collection_df['const'].astype(str)) if not collection_df.empty else set()
    watched_ids = set(watched_df['const'].astype(str)) if not watched_df.empty else set()
    
    # Check if watchlist and watched are identical
    if watchlist_ids == watched_ids and len(watchlist_ids) > 0:
        print(f"   ℹ️  Watchlist CSV and Watched DB are identical")
        print(f"   Expected unique items: {len(watchlist_ids | collection_ids)}")
        expected_unique = len(watchlist_ids | collection_ids)
    else:
        print(f"   Expected unique items: {expected_unique}")
    
    if not master_df.empty:
        print(f"   Current master list: {len(master_df)}")
        if len(master_df) == expected_unique:
            print("   ✅ Master list is complete!")
        elif len(master_df) > expected_unique:
            print(f"   ℹ️  Master list has {len(master_df) - expected_unique} extra items (might include duplicates)")
        else:
            print(f"   ⚠️  Missing {expected_unique - len(master_df)} items")
            print("   Run: python scripts/00_create_master_list.py")
    else:
        print("   ⚠️  Master list needs to be created")
        print("   Run: python scripts/00_create_master_list.py")
    
    # Show overlap statistics
    print(f"\n🔄 Source Analysis:")
    print(f"   Watchlist CSV: {len(watchlist_ids)} items")
    print(f"   Collection DB: {len(collection_ids)} items")
    print(f"   Watched DB: {len(watched_ids)} items")
    print(f"\n   Overlap breakdown:")
    print(f"   • Watchlist only: {len(watchlist_ids - collection_ids - watched_ids)}")
    print(f"   • Collection only: {len(collection_ids - watchlist_ids - watched_ids)}")
    print(f"   • Watched only: {len(watched_ids - watchlist_ids - collection_ids)}")
    print(f"   • In Watchlist + Collection: {len(watchlist_ids & collection_ids - watched_ids)}")
    print(f"   • In all three sources: {len(watchlist_ids & collection_ids & watched_ids)}")
    
    print("\n" + "="*60)

if __name__ == "__main__":
    main()