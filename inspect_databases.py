"""
Quick script to inspect database structure and sample data

Usage:
    python inspect_databases.py
"""
import pandas as pd
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.core.config import settings
from src.core.db_connector import DBConnector

def inspect_db(db_path: Path, db_name: str):
    """Inspect a database and show its structure."""
    print(f"\n{'='*70}")
    print(f"📁 {db_name}: {db_path.name}")
    print('='*70)
    
    if not db_path.exists():
        print(f"❌ File not found!")
        return
    
    connector = DBConnector(db_path)
    tables = connector.get_table_names()
    
    print(f"📋 Tables: {tables}")
    
    for table in tables:
        print(f"\n  Table: '{table}'")
        df = connector.get_table_as_df(table)
        
        print(f"  • Rows: {len(df)}")
        print(f"  • Columns: {list(df.columns)}")
        
        # Show first few rows
        print(f"\n  Sample data (first 3 rows):")
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        print(df.head(3).to_string(index=False))
        
        # Check for IMDb ID column
        if 'imdb_id' in df.columns:
            print(f"\n  ✅ Has 'imdb_id' column")
            print(f"  • Unique IMDb IDs: {df['imdb_id'].nunique()}")
        elif 'const' in df.columns:
            print(f"\n  ✅ Has 'const' column")
            print(f"  • Unique IMDb IDs: {df['const'].nunique()}")
        else:
            print(f"\n  ⚠️  No IMDb ID column found")

def main():
    print("="*70)
    print("CineScope Database Inspector")
    print("="*70)
    
    # Inspect each database
    inspect_db(
        settings.RAW_DATA_DIR / 'watched_movies.db',
        'Watched Movies Database'
    )
    
    inspect_db(
        settings.RAW_DATA_DIR / 'collection_movies.db',
        'Collection Movies Database'
    )
    
    inspect_db(
        settings.RAW_DATA_DIR / 'master_movies.db',
        'Master Movies Database'
    )
    
    print("\n" + "="*70)

if __name__ == "__main__":
    main()