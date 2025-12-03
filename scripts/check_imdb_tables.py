"""
Check which tables exist in the IMDb database and their row counts

Usage:
    python scripts/check_imdb_tables.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.core.config import settings
from src.core.db_connector import DBConnector

def main():
    print("="*70)
    print("IMDb Database Table Check")
    print("="*70)
    
    imdb_db = settings.RAW_DATA_DIR / 'imdb.db'
    
    if not imdb_db.exists():
        print(f"\n❌ IMDb database not found at: {imdb_db}")
        print("Run: python scripts/setup_database.py")
        return
    
    connector = DBConnector(imdb_db)
    tables = connector.get_table_names()
    
    print(f"\n📊 Found {len(tables)} tables:\n")
    
    expected_tables = {
        'title_akas': 'Alternative/localized titles',
        'title_basics': 'Core movie/TV information',
        'title_ratings': 'IMDb ratings',
        'title_crew': 'Directors and writers',
        'title_episode': 'TV episode information',
        'title_principals': 'Cast and crew (REQUIRED for cast enrichment)',
        'name_basics': 'People information'
    }
    
    for table_name, description in expected_tables.items():
        if table_name in tables:
            df = connector.get_table_as_df(table_name)
            print(f"✅ {table_name:20} | {len(df):>12,} rows | {description}")
        else:
            print(f"❌ {table_name:20} | {'MISSING':>12} | {description}")
    
    # Check for unexpected tables
    unexpected = set(tables) - set(expected_tables.keys())
    if unexpected:
        print(f"\n📋 Additional tables: {', '.join(unexpected)}")
    
    # Recommendations
    print("\n" + "="*70)
    if 'title_principals' not in tables:
        print("⚠️  CRITICAL: title_principals table is MISSING!")
        print("\nThis table is required for cast enrichment.")
        print("You need to re-run setup_database.py to import it.")
        print("\nThe updated setup_database.py should import:")
        print("  - title.principals.tsv.gz → title_principals table")
    else:
        print("✅ All required tables present!")
        print("You can run cast enrichment:")
        print("  python scripts/enrich/04_enrich_cast.py")
    print("="*70)

if __name__ == "__main__":
    main()