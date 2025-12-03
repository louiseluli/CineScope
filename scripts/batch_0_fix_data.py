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
        """Attempt to fix director names showing as IMDB IDs."""
        log_message("\n" + "=" * 80)
        log_message("Checking Director Names...")
        log_message("=" * 80)
        
        if 'directors' not in self.df.columns:
            log_message("⚠️ No directors column found", level="WARNING")
            return self
        
        # Count directors that are IMDB IDs (format: nm#######)
        imdb_id_pattern = r'^nm\d{7,}$'
        
        # Check for IMDB IDs in director field
        has_imdb_ids = self.df['directors'].astype(str).str.match(imdb_id_pattern)
        imdb_id_count = has_imdb_ids.sum()
        
        if imdb_id_count > 0:
            log_message(f"⚠️ Found {imdb_id_count} films with IMDB IDs instead of names")
            log_message("Note: These need to be looked up from IMDB name database")
            
            # Try to load IMDB names cache
            names_cache = PROCESSED_DATA_DIR / "imdb_cache" / "names.parquet"
            
            if names_cache.exists():
                log_message(f"Loading IMDB names database...")
                try:
                    import pyarrow.parquet as pq
                    names_df = pd.read_parquet(names_cache)
                    
                    # Create a mapping
                    name_mapping = dict(zip(names_df['nconst'], names_df['primaryName']))
                    
                    # Replace IMDB IDs with names where possible
                    def replace_id_with_name(director_str):
                        if pd.isna(director_str):
                            return director_str
                        
                        director_str = str(director_str)
                        
                        # Check if it's an IMDB ID
                        if director_str.startswith('nm') and director_str[2:].isdigit():
                            return name_mapping.get(director_str, director_str)
                        
                        return director_str
                    
                    self.df['directors'] = self.df['directors'].apply(replace_id_with_name)
                    
                    # Count remaining IMDB IDs
                    still_has_ids = self.df['directors'].astype(str).str.match(imdb_id_pattern)
                    remaining = still_has_ids.sum()
                    
                    log_message(f"✅ Replaced {imdb_id_count - remaining} IMDB IDs with names")
                    if remaining > 0:
                        log_message(f"⚠️ {remaining} IDs could not be resolved", level="WARNING")
                    
                except Exception as e:
                    log_message(f"⚠️ Could not load names database: {e}", level="WARNING")
            else:
                log_message("⚠️ IMDB names cache not found - cannot resolve IDs", level="WARNING")
        else:
            log_message("✅ All director names look good (no IMDB IDs)")
        
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
        """Validate the cleaned dataset."""
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
        log_message(f"  Films with directors: {self.df['directors'].notna().sum():,}")
        
        # Show genre distribution
        from src.core.helpers import explode_genres
        genre_exploded = explode_genres(self.df)
        top_genres = genre_exploded['genre'].value_counts().head(10)
        
        log_message(f"\nTop 10 Genres (cleaned):")
        for i, (genre, count) in enumerate(top_genres.items(), 1):
            log_message(f"  {i:2d}. {genre:20s} - {count:4d} films")
        
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