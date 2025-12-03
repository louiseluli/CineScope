"""
CineScope Detailed Source Analysis

Analyzes the actual IMDb IDs across all sources to understand overlap.

Usage:
    python analyze_sources.py
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
        return pd.DataFrame()
        
    df = connector.get_table_as_df(expected_table)
    
    # Standardize column names
    if 'imdb_id' in df.columns:
        df = df.rename(columns={'imdb_id': 'const'})
    
    return df

def main():
    print("="*70)
    print("CineScope Detailed Source Analysis (by IMDb ID)")
    print("="*70)
    
    # Load all sources
    print("\n📊 Loading data from all sources...")
    
    loader = DataLoader()
    watchlist_df = loader.load_watchlist()
    print(f"✅ Watchlist CSV: {len(watchlist_df)} items")
    
    collection_df = load_from_database(
        settings.RAW_DATA_DIR / 'collection_movies.db', 
        'movies'
    )
    print(f"✅ Collection DB: {len(collection_df)} items")
    
    watched_df = load_from_database(
        settings.RAW_DATA_DIR / 'watched_movies.db', 
        'movies'
    )
    print(f"✅ Watched DB: {len(watched_df)} items")
    
    # Get IMDb ID sets
    print("\n🔍 Analyzing IMDb IDs...")
    watchlist_ids = set(watchlist_df['const'].astype(str))
    collection_ids = set(collection_df['const'].astype(str)) if not collection_df.empty else set()
    watched_ids = set(watched_df['const'].astype(str)) if not watched_df.empty else set()
    
    print(f"   Unique IMDb IDs in Watchlist CSV: {len(watchlist_ids)}")
    print(f"   Unique IMDb IDs in Collection DB: {len(collection_ids)}")
    print(f"   Unique IMDb IDs in Watched DB: {len(watched_ids)}")
    
    # Check if watchlist and watched are identical
    print("\n🔄 Source Comparison:")
    if watchlist_ids == watched_ids:
        print("   ✅ Watchlist CSV and Watched DB have IDENTICAL IMDb IDs")
        print("   → They are the same dataset (use Watched DB as it's the SQL source)")
    else:
        diff_w_to_wd = watchlist_ids - watched_ids
        diff_wd_to_w = watched_ids - watchlist_ids
        print(f"   ⚠️  Watchlist CSV and Watched DB are DIFFERENT")
        print(f"   → In Watchlist but not Watched: {len(diff_w_to_wd)}")
        print(f"   → In Watched but not Watchlist: {len(diff_wd_to_w)}")
    
    # Analyze overlaps
    print("\n📈 Overlap Analysis:")
    
    # Only in one source
    only_watchlist = watchlist_ids - collection_ids - watched_ids
    only_collection = collection_ids - watchlist_ids - watched_ids
    only_watched = watched_ids - watchlist_ids - collection_ids
    
    # In two sources
    watchlist_collection = (watchlist_ids & collection_ids) - watched_ids
    watchlist_watched = (watchlist_ids & watched_ids) - collection_ids
    collection_watched = (collection_ids & watched_ids) - watchlist_ids
    
    # In all three
    all_three = watchlist_ids & collection_ids & watched_ids
    
    print("\n   Single source:")
    print(f"   • Only in Watchlist CSV: {len(only_watchlist)}")
    print(f"   • Only in Collection DB: {len(only_collection)}")
    print(f"   • Only in Watched DB: {len(only_watched)}")
    
    print("\n   Two sources:")
    print(f"   • Watchlist + Collection (not Watched): {len(watchlist_collection)}")
    print(f"   • Watchlist + Watched (not Collection): {len(watchlist_watched)}")
    print(f"   • Collection + Watched (not Watchlist): {len(collection_watched)}")
    
    print("\n   All three sources:")
    print(f"   • In all three: {len(all_three)}")
    
    # Total unique
    total_unique = len(watchlist_ids | collection_ids | watched_ids)
    print(f"\n📊 Total unique IMDb IDs across all sources: {total_unique}")
    
    # Recommended approach
    print("\n" + "="*70)
    print("💡 RECOMMENDATION:")
    print("="*70)
    if watchlist_ids == watched_ids:
        print("Since Watchlist CSV and Watched DB are identical:")
        print("  1. Use WATCHED DB as the primary source (it's the SQL database)")
        print("  2. Combine with COLLECTION DB")
        print(f"  3. Expected unique items: {len(watched_ids | collection_ids)}")
        print(f"\n  Breakdown:")
        print(f"  • From Watched DB only: {len(watched_ids - collection_ids)}")
        print(f"  • From Collection DB only: {len(collection_ids - watched_ids)}")
        print(f"  • In both (watched movies you own): {len(watched_ids & collection_ids)}")
    else:
        print("Using all three sources:")
        print(f"  Expected unique items: {total_unique}")
    print("="*70)

if __name__ == "__main__":
    main()