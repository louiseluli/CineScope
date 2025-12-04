"""
CineScope - Data Cleaning & Deduplication Script
================================================
Fixes issues found in Batch 0:
1. Remove 2,275 duplicates
2. Fix genre parsing (remove brackets and quotes)
3. Fix director names (convert IMDB IDs to names)
4. Handle missing data

Run this AFTER batch_0_data_engineering.py completes.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import MASTER_DATA, PROCESSED_DATA_DIR, log_message
from src.core.helpers import parse_genres


class DataCleaner:
    """Clean and deduplicate the master dataset."""
    
    def __init__(self):
        """Initialize the cleaner."""
        self.df = None
        self.original_count = 0
        self.cleaned_count = 0
        self.duplicates_removed = 0
        
    def load_data(self):
        """Load the master dataset."""
        log_message("=" * 80)
        log_message("Loading master dataset...")
        log_message("=" * 80)
        
        self.df = pd.read_csv(MASTER_DATA)
        self.original_count = len(self.df)
        
        log_message(f"Loaded {self.original_count:,} records")
        log_message(f"Columns: {len(self.df.columns)}")
        
        return self
    
    def remove_duplicates(self):
        """Remove duplicate films based on IMDB ID (const)."""
        log_message("\n" + "=" * 80)
        log_message("Removing Duplicates...")
        log_message("=" * 80)
        
        # Count duplicates
        duplicates = self.df['const'].duplicated()
        duplicate_count = duplicates.sum()
        
        log_message(f"Found {duplicate_count:,} duplicate records")
        
        # Keep first occurrence of each unique const
        self.df = self.df.drop_duplicates(subset='const', keep='first')
        
        self.duplicates_removed = duplicate_count
        log_message(f"✅ Removed {duplicate_count:,} duplicates")
        log_message(f"Remaining records: {len(self.df):,}")
        
        return self
    
    def fix_genre_parsing(self):
        """Fix genre column to remove brackets and quotes."""
        log_message("\n" + "=" * 80)
        log_message("Fixing Genre Parsing...")
        log_message("=" * 80)
        
        if 'genres' not in self.df.columns:
            log_message("⚠️ No genres column found", level="WARNING")
            return self
        
        def clean_genre_string(genre_str):
            """Clean a genre string."""
            if pd.isna(genre_str):
                return genre_str
            
            # Convert to string
            genre_str = str(genre_str)
            
            # Remove brackets and extra quotes
            genre_str = genre_str.replace("['", "").replace("']", "")
            genre_str = genre_str.replace('["', '').replace('"]', '')
            genre_str = genre_str.replace("', '", ", ").replace('", "', ', ')
            
            return genre_str
        
        # Clean genres
        before_sample = self.df['genres'].head(10).tolist()
        self.df['genres'] = self.df['genres'].apply(clean_genre_string)
        after_sample = self.df['genres'].head(10).tolist()
        
        log_message("Sample before:")
        for i, g in enumerate(before_sample[:3], 1):
            log_message(f"  {i}. {g}")
        
        log_message("\nSample after:")
        for i, g in enumerate(after_sample[:3], 1):
            log_message(f"  {i}. {g}")
        
        log_message("\n✅ Genre strings cleaned")
        
        # Recalculate primary genre
        self.df['primary_genre'] = self.df['genres'].apply(
            lambda x: parse_genres(x)[0] if parse_genres(x) else 'Unknown'
        )
        
        return self
    
    def fix_director_names(self):
        """
        Normalize person-name fields so plots show clean names (no IMDb IDs).
        - Strips patterns like " | nm0000033"
        - Splits on commas/pipes and removes any lone nm######## tokens
        - De-dups and rejoins with ", "
        Applies to 'directors' and also prepares 'directors_clean' (keeps original intact if already clean).
        """
        log_message("\n" + "=" * 80)
        log_message("Normalizing person names (removing IMDb IDs)...")
        log_message("=" * 80)

        if 'directors' not in self.df.columns:
            log_message("⚠️ No directors column found", level="WARNING")
            return self

        import re
        id_token = re.compile(r'\bnm\d{7,}\b')
        pipe_id_suffix = re.compile(r'\s*\|\s*nm\d{7,}\b')   # e.g., "Billy Wilder | nm0000697"

        def clean_people_field(val: str) -> str:
            if pd.isna(val):
                return val
            s = str(val)

            # 1) Remove trailing " | nm########" fragments
            s = pipe_id_suffix.sub("", s)

            # 2) Split on common delimiters (commas or pipes)
            parts = re.split(r'[|,]', s)
            cleaned = []
            for p in parts:
                token = p.strip()
                if not token:
                    continue
                # drop tokens that are only ids
                if id_token.fullmatch(token):
                    continue
                # also remove any embedded ids inside the token
                token = id_token.sub("", token).strip()
                if token:
                    cleaned.append(token)

            # 3) De-duplicate while preserving order
            seen = set()
            uniq = []
            for c in cleaned:
                if c not in seen:
                    seen.add(c)
                    uniq.append(c)

            return ", ".join(uniq) if uniq else None

        before_samples = self.df['directors'].head(5).tolist()
        self.df['directors_clean'] = self.df['directors'].apply(clean_people_field)

        # If 'directors' is messy (contains nm ids or pipes), overwrite with clean
        def looks_dirty(x: str) -> bool:
            if pd.isna(x):
                return False
            s = str(x)
            return bool(id_token.search(s) or '|' in s)

        dirty_mask = self.df['directors'].astype(str).apply(looks_dirty)
        self.df.loc[dirty_mask, 'directors'] = self.df.loc[dirty_mask, 'directors_clean']

        after_samples = self.df['directors'].head(5).tolist()

        log_message("Sample directors BEFORE:")
        for i, s in enumerate(before_samples, 1):
            log_message(f"  {i}. {s}")
        log_message("\nSample directors AFTER:")
        for i, s in enumerate(after_samples, 1):
            log_message(f"  {i}. {s}")

        log_message("✅ Person-name normalization complete")
        return self

    def ensure_cast_columns(self):
        """
        Ensure standard cast columns exist so Batch 2 never says 'No cast detected'.
        Creates:
        - cast_list: list[str] of actor names
        - cast_display: comma-joined names for quick plotting
        Sources checked in priority order:
        ['cast_list', 'actors', 'cast', 'tmdb_cast_names', 'cast_names', 'principal_cast']
        Accepts either CSV-like strings or JSON-like lists.
        """
        log_message("\n" + "=" * 80)
        log_message("Ensuring cast columns are present...")
        log_message("=" * 80)

        import ast

        candidate_cols = [
            'cast_list',
            'actors',
            'cast',
            'tmdb_cast_names',
            'cast_names',
            'principal_cast'
        ]

        # Pick the first existing column
        src = None
        for c in candidate_cols:
            if c in self.df.columns and self.df[c].notna().any():
                src = c
                break

        if src is None:
            log_message("⚠️ No cast-like columns found; creating empty placeholders", level="WARNING")
            self.df['cast_list'] = [[] for _ in range(len(self.df))]
            self.df['cast_display'] = ""
            return self

        def parse_names(val):
            if pd.isna(val):
                return []
            s = str(val).strip()
            # Already a python/list-like string?
            if (s.startswith('[') and s.endswith(']')) or (s.startswith('(') and s.endswith(')')):
                try:
                    parsed = ast.literal_eval(s)
                    # flatten entries to strings
                    out = []
                    for item in parsed if isinstance(parsed, (list, tuple)) else [parsed]:
                        if pd.isna(item):
                            continue
                        out.append(str(item).strip())
                    return [x for x in out if x]
                except Exception:
                    pass
            # Fallback: split on commas or pipes
            parts = [p.strip() for p in re.split(r'[|,]', s)]
            return [p for p in parts if p]

        import re
        names_series = self.df[src].apply(parse_names)

        # Remove any nm######## tokens that slipped in
        id_token = re.compile(r'\bnm\d{7,}\b')
        def drop_ids(name_list):
            cleaned = []
            for n in name_list:
                if not n:
                    continue
                if id_token.fullmatch(n):
                    continue
                n2 = id_token.sub("", n).strip()
                if n2:
                    cleaned.append(n2)
            # de-dupe preserving order
            seen = set()
            uniq = []
            for c in cleaned:
                if c not in seen:
                    seen.add(c)
                    uniq.append(c)
            return uniq

        names_series = names_series.apply(drop_ids)

        self.df['cast_list'] = names_series
        self.df['cast_display'] = self.df['cast_list'].apply(lambda xs: ", ".join(xs[:10]))

        log_message(f"✅ Cast columns created from '{src}'")
        log_message("Sample cast rows:")
        for i in range(min(3, len(self.df))):
            log_message(f"  {i+1}. {self.df.iloc[i].get('cast_display', '')}")

        return self

    
    def handle_missing_data(self):
        """Handle missing titles and years."""
        log_message("\n" + "=" * 80)
        log_message("Handling Missing Data...")
        log_message("=" * 80)
        
        # Check for missing titles
        missing_titles = self.df['title'].isna().sum()
        if missing_titles > 0:
            log_message(f"⚠️ {missing_titles} films with missing titles")
            
            # Try to use original_title if title is missing
            self.df['title'] = self.df['title'].fillna(self.df['original_title'])
            
            # If still missing, use "Unknown Title"
            still_missing = self.df['title'].isna().sum()
            if still_missing > 0:
                self.df.loc[self.df['title'].isna(), 'title'] = 'Unknown Title'
                log_message(f"  Set {still_missing} to 'Unknown Title'")
        else:
            log_message("✅ No missing titles")
        
        # Check for missing years
        missing_years = self.df['year'].isna().sum()
        if missing_years > 0:
            log_message(f"⚠️ {missing_years} films with missing years")
            log_message(f"  These will be excluded from temporal analysis")
        else:
            log_message("✅ No missing years")
        
        # Regenerate display_title after fixing titles
        def create_display_title(row):
            original = str(row.get('original_title', '')).strip()
            english = str(row.get('title', '')).strip()
            
            if pd.isna(original) or original == '' or original == 'nan':
                original = english
            if pd.isna(english) or english == '' or english == 'nan':
                english = original
            
            if original.lower() == english.lower():
                return original
            
            if not original or original == 'nan':
                return english
            
            if not english or english == 'nan':
                return original
            
            return f"{original} ({english})"
        
        log_message("\nRegenerating display titles...")
        self.df['display_title'] = self.df.apply(create_display_title, axis=1)
        log_message("✅ Display titles regenerated")
        
        return self
    
    def validate_cleaned_data(self):
        """Validate the cleaned dataset (post-fixes)."""
        log_message("\n" + "=" * 80)
        log_message("Validating Cleaned Data...")
        log_message("=" * 80)
        
        # Check for duplicates
        duplicates = self.df['const'].duplicated().sum()
        log_message(f"Duplicates: {duplicates}")
        
        # Check data completeness
        log_message(f"\nData Completeness:")
        log_message(f"  Total films: {len(self.df):,}")
        log_message(f"  Films with titles: {self.df['title'].notna().sum():,}")
        log_message(f"  Films with years: {self.df['year'].notna().sum():,}")
        log_message(f"  Films with genres: {self.df['genres'].notna().sum():,}")
        if 'directors' in self.df.columns:
            log_message(f"  Films with directors: {self.df['directors'].notna().sum():,}")
        if 'cast_list' in self.df.columns:
            has_cast = self.df['cast_list'].apply(lambda x: isinstance(x, list) and len(x) > 0).sum()
            log_message(f"  Films with cast_list: {has_cast:,}")

        # Show genre distribution
        from src.core.helpers import explode_genres
        genre_exploded = explode_genres(self.df)
        top_genres = genre_exploded['genre'].value_counts().head(10)
        
        log_message(f"\nTop 10 Genres (cleaned):")
        for i, (genre, count) in enumerate(top_genres.items(), 1):
            log_message(f"  {i:2d}. {genre:20s} - {count:4d} films")

        # Quick peek at names hygiene
        if 'directors' in self.df.columns:
            sample_directors = self.df['directors'].dropna().astype(str).head(5).tolist()
            log_message("\nSample cleaned directors:")
            for i, d in enumerate(sample_directors, 1):
                log_message(f"  {i}. {d}")

        return self

    
    def save_cleaned_data(self):
        """Save the cleaned dataset."""
        log_message("\n" + "=" * 80)
        log_message("Saving Cleaned Data...")
        log_message("=" * 80)
        
        # Backup original
        backup_path = MASTER_DATA.parent / "master_cinema_data_BACKUP.csv"
        if not backup_path.exists():
            log_message(f"Creating backup: {backup_path.name}")
            import shutil
            shutil.copy(MASTER_DATA, backup_path)
        
        # Save cleaned version
        self.df.to_csv(MASTER_DATA, index=False)
        log_message(f"✅ Cleaned data saved: {MASTER_DATA}")
        
        self.cleaned_count = len(self.df)
        
        return self
    
    def generate_summary(self):
        """Generate cleaning summary."""
        log_message("\n" + "=" * 80)
        log_message("CLEANING SUMMARY")
        log_message("=" * 80)
        
        log_message(f"\nOriginal records: {self.original_count:,}")
        log_message(f"Duplicates removed: {self.duplicates_removed:,}")
        log_message(f"Final records: {self.cleaned_count:,}")
        log_message(f"Reduction: {((self.original_count - self.cleaned_count) / self.original_count * 100):.1f}%")
        
        log_message("\n✅ Data cleaning complete!")
        
        return self


def main():
    """Main execution."""
    print("\n" + "🧹" * 40)
    print("\n" + " " * 20 + "CINESCOPE DATA CLEANING")
    print(" " * 22 + "Fixing Batch 0 Issues")
    print("\n" + "🧹" * 40 + "\n")
    
    try:
        cleaner = DataCleaner()
        cleaner.load_data()
        cleaner.remove_duplicates()
        cleaner.fix_genre_parsing()
        cleaner.fix_director_names()
        cleaner.ensure_cast_columns() 
        cleaner.handle_missing_data()
        cleaner.validate_cleaned_data()
        cleaner.save_cleaned_data()
        cleaner.generate_summary()
        
        print("\n" + "✨" * 40)
        print("\n" + " " * 15 + "DATA CLEANING SUCCESSFUL!")
        print(" " * 10 + "Ready to proceed with analysis batches")
        print("\n" + "✨" * 40 + "\n")
        
        return 0
        
    except Exception as e:
        print("\n" + "❌" * 40)
        print(f"\nERROR: {str(e)}")
        print("\n" + "❌" * 40 + "\n")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)