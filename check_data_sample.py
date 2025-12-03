"""
Quick script to check sample data from databases
"""
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from src.core.config import settings
from src.core.db_connector import DBConnector

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 50)

print("="*100)
print("WATCHED MOVIES DATABASE")
print("="*100)
connector = DBConnector(settings.RAW_DATA_DIR / 'watched_movies.db')
watched = connector.get_table_as_df('movies')
print(f"\nTotal rows: {len(watched)}")
print(f"\nColumns: {list(watched.columns)}")
print(f"\nFirst 10 rows:")
print(watched.head(10).to_string(index=False))

print("\n" + "="*100)
print("COLLECTION MOVIES DATABASE")
print("="*100)
connector = DBConnector(settings.RAW_DATA_DIR / 'collection_movies.db')
collection = connector.get_table_as_df('movies')
print(f"\nTotal rows: {len(collection)}")
print(f"\nColumns: {list(collection.columns)}")
print(f"\nFirst 10 rows:")
print(collection.head(10).to_string(index=False))

print("\n" + "="*100)
print("MASTER MEDIA LIST (after merging)")
print("="*100)
master = pd.read_csv(settings.PROCESSED_DATA_DIR / 'master_media_list.csv')
print(f"\nTotal rows: {len(master)}")
print(f"\nColumns: {list(master.columns)}")
print(f"\nFirst 10 rows:")
print(master.head(10).to_string(index=False))

print("\n" + "="*100)
print("CHECKING SPECIFIC PROBLEMATIC TITLES")
print("="*100)

problem_ids = ['tt0298481', 'tt1634107', 'tt0246076', 'tt0462003', 'tt4649946']

for imdb_id in problem_ids:
    print(f"\n--- {imdb_id} ---")
    
    # Check in watched (use actual column names)
    if 'imdb_id' in watched.columns:
        match_w = watched[watched['imdb_id'] == imdb_id]
    elif 'Const' in watched.columns:
        match_w = watched[watched['Const'] == imdb_id]
    else:
        match_w = pd.DataFrame()
    
    if not match_w.empty:
        title_col = 'Title' if 'Title' in watched.columns else 'title'
        year_col = 'Year' if 'Year' in watched.columns else 'year'
        print(f"WATCHED: {imdb_id} | {match_w.iloc[0][title_col]} | Year: {match_w.iloc[0][year_col]}")
    
    # Check in collection
    if 'imdb_id' in collection.columns:
        match_c = collection[collection['imdb_id'] == imdb_id]
    elif 'Const' in collection.columns:
        match_c = collection[collection['Const'] == imdb_id]
    else:
        match_c = pd.DataFrame()
    
    if not match_c.empty:
        title_col = 'Title' if 'Title' in collection.columns else 'title'
        year_col = 'Year' if 'Year' in collection.columns else 'year'
        print(f"COLLECTION: {imdb_id} | {match_c.iloc[0][title_col]} | Year: {match_c.iloc[0][year_col]}")
    
    # Check in master
    match_m = master[master['const'] == imdb_id]
    if not match_m.empty:
        print(f"MASTER: {imdb_id} | {match_m.iloc[0]['title']} | Year: {match_m.iloc[0]['year']}")
    
    if match_w.empty and match_c.empty and match_m.empty:
        print("❌ NOT FOUND in any database")

print("\n" + "="*100)
print("SEARCH FOR 'NORMA' IN ALL DATABASES")
print("="*100)

print("\n--- In WATCHED ---")
title_col = 'Title' if 'Title' in watched.columns else 'title'
norma_w = watched[watched[title_col].str.contains('Norma', case=False, na=False)]
if not norma_w.empty:
    id_col = 'imdb_id' if 'imdb_id' in watched.columns else 'Const'
    year_col = 'Year' if 'Year' in watched.columns else 'year'
    print(norma_w[[id_col, title_col, year_col]].head(10).to_string(index=False))
else:
    print("No matches")

print("\n--- In COLLECTION ---")
title_col = 'Title' if 'Title' in collection.columns else 'title'
norma_c = collection[collection[title_col].str.contains('Norma', case=False, na=False)]
if not norma_c.empty:
    id_col = 'imdb_id' if 'imdb_id' in collection.columns else 'Const'
    year_col = 'Year' if 'Year' in collection.columns else 'year'
    print(norma_c[[id_col, title_col, year_col]].head(10).to_string(index=False))
else:
    print("No matches")

print("\n--- In MASTER ---")
norma_m = master[master['title'].str.contains('Norma', case=False, na=False)]
if not norma_m.empty:
    print(norma_m[['const', 'title', 'year']].head(10).to_string(index=False))
else:
    print("No matches")

print("\n--- In COLLECTION ---")
title_col = 'Title' if 'Title' in collection.columns else 'title'
norma_c = collection[collection[title_col].str.contains('Norma', case=False, na=False)]
if not norma_c.empty:
    id_col = 'imdb_id' if 'imdb_id' in collection.columns else 'Const'
    year_col = 'Year' if 'Year' in collection.columns else 'year'
    print(norma_c[[id_col, title_col, year_col]].head(10).to_string(index=False))
else:
    print("No matches")

print("\n--- In MASTER ---")
norma_m = master[master['title'].str.contains('Norma', case=False, na=False)]
if not norma_m.empty:
    print(norma_m[['const', 'title', 'year']].head(10).to_string(index=False))
else:
    print("No matches")

print("\n" + "="*100)