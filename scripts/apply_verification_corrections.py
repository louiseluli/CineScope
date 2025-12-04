"""
Apply corrections from master_verification file to the collection database

Usage:
    python scripts/apply_verification_corrections.py
"""
import pandas as pd
import sqlite3
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.core.config import settings

def main():
    print("="*70)
    print("Applying Verification Corrections to Collection Database")
    print("="*70)
    
    # Load verification file
    verification_file = settings.RAW_DATA_DIR / 'master_verification_20251013.csv'
    if not verification_file.exists():
        print(f"❌ Verification file not found: {verification_file}")
        sys.exit(1)
    
    verif_df = pd.read_csv(verification_file)
    print(f"\n📋 Loaded {len(verif_df)} verification records")
    
    # Show quality issues breakdown
    print("\n📊 Quality Issues Summary:")
    issue_counts = verif_df['Issues'].value_counts().head(10)
    for issue, count in issue_counts.items():
        print(f"  {issue}: {count}")
    
    # Filter for records that need correction (have New_IMDb_ID)
    corrections = verif_df[verif_df['New_IMDb_ID'].notna()].copy()
    print(f"\n📝 Found {len(corrections)} records with IMDb ID corrections to apply")
    
    # Also identify movies that won't be in TMDb/OMDb
    no_tmdb = verif_df[verif_df['Has_TMDb'] == 'NO']
    print(f"⚠️  {len(no_tmdb)} movies NOT in TMDb (will show warnings during enrichment)")
    
    if corrections.empty:
        print("\n✅ No IMDb ID corrections needed!")
        print("\nMy collection has verified IDs, but some movies may not exist")
        print("in external databases (TMDb, OMDb). This is normal for rare/old films.")
        return
    
    # Show sample of corrections
    print("\n📊 Sample corrections:")
    print(corrections[['Current_IMDb_ID', 'Current_Title', 'New_IMDb_ID']].head(10))
    
    # Connect to collection database
    db_path = settings.RAW_DATA_DIR / 'collection_movies.db'
    conn = sqlite3.connect(db_path)
    
    # Load current collection data
    collection_df = pd.read_sql_query("SELECT * FROM movies", conn)
    print(f"\n📦 Collection has {len(collection_df)} movies")
    
    # Apply corrections
    updates_made = 0
    not_found = []
    
    for _, corr in corrections.iterrows():
        old_id = corr['Current_IMDb_ID']
        new_id = corr['New_IMDb_ID']
        
        # Find movie with old ID
        mask = collection_df['imdb_id'] == old_id
        
        if mask.any():
            # Update the IMDb ID
            collection_df.loc[mask, 'imdb_id'] = new_id
            collection_df.loc[mask, 'Const'] = new_id
            
            # Also update Correct_IMDb_ID if it exists
            if 'Correct_IMDb_ID' in collection_df.columns:
                collection_df.loc[mask, 'Correct_IMDb_ID'] = new_id
            
            updates_made += 1
            print(f"✓ Updated {old_id} → {new_id}")
        else:
            not_found.append(old_id)
    
    print(f"\n✅ Applied {updates_made} corrections")
    
    if not_found:
        print(f"\n⚠️  {len(not_found)} IDs not found in collection:")
        for idx in not_found[:10]:
            print(f"  - {idx}")
    
    # Save updated collection back to database
    print("\n💾 Saving updated collection to database...")
    collection_df.to_sql('movies', conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()
    
    print("\n✅ Collection database updated successfully!")
    print(f"Updated database: {db_path}")
    print("\nNext steps:")
    print("  1. Run: python scripts/00_create_master_list.py")
    print("  2. Run: python scripts/enrich/01_enrich_tmdb.py")
    print("="*70)

if __name__ == "__main__":
    main()