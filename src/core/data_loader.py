"""
CineScope Data Analysis - Data Loading & Merging Module
=======================================================
This module handles loading and merging all enriched data sources into a unified master dataset.
Preserves ORIGINAL titles first, then English titles.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Import configuration
import sys
sys.path.append(str(Path(__file__).parent.parent))
from core.config import (
    WATCHED_CSV, TMDB_ENRICHED, OMDB_ENRICHED, DDD_ENRICHED,
    CAST_ENRICHED, WIKIDATA_ENRICHED, MASTER_DATA, VALIDATION_REPORT,
    MISSING_VALUES, DATE_FORMAT, log_message, PROCESSED_DATA_DIR
)


class DataLoader:
    """Handles loading and merging all cinema data sources."""
    
    def __init__(self):
        """Initialize the data loader."""
        self.watched_df = None
        self.tmdb_df = None
        self.omdb_df = None
        self.ddd_df = None
        self.cast_df = None
        self.wikidata_df = None
        self.master_df = None
        self.validation_results = {}
        
    def load_all_sources(self):
        """Load all CSV data sources."""
        log_message("=" * 80)
        log_message("Loading all data sources...")
        log_message("=" * 80)
        
        # Load WatchedDec.csv (base watchlist)
        if WATCHED_CSV.exists():
            log_message(f"Loading: {WATCHED_CSV.name}")
            self.watched_df = pd.read_csv(WATCHED_CSV)
            log_message(f"  ✓ Loaded {len(self.watched_df)} records")
        else:
            log_message(f"⚠️  File not found: {WATCHED_CSV}", level="WARNING")
        
        # Load TMDB enriched data
        if TMDB_ENRICHED.exists():
            log_message(f"Loading: {TMDB_ENRICHED.name}")
            self.tmdb_df = pd.read_csv(TMDB_ENRICHED)
            log_message(f"  ✓ Loaded {len(self.tmdb_df)} records")
        else:
            log_message(f"⚠️  File not found: {TMDB_ENRICHED}", level="WARNING")
        
        # Load OMDB enriched data
        if OMDB_ENRICHED.exists():
            log_message(f"Loading: {OMDB_ENRICHED.name}")
            self.omdb_df = pd.read_csv(OMDB_ENRICHED)
            log_message(f"  ✓ Loaded {len(self.omdb_df)} records")
        else:
            log_message(f"⚠️  File not found: {OMDB_ENRICHED}", level="WARNING")
        
        # Load DDD enriched data
        if DDD_ENRICHED.exists():
            log_message(f"Loading: {DDD_ENRICHED.name}")
            self.ddd_df = pd.read_csv(DDD_ENRICHED)
            log_message(f"  ✓ Loaded {len(self.ddd_df)} records")
        else:
            log_message(f"⚠️  File not found: {DDD_ENRICHED}", level="WARNING")
        
        # Load Cast enriched data (optional - may not exist yet)
        if CAST_ENRICHED.exists():
            log_message(f"Loading: {CAST_ENRICHED.name}")
            self.cast_df = pd.read_csv(CAST_ENRICHED)
            log_message(f"  ✓ Loaded {len(self.cast_df)} records")
        else:
            log_message(f"ℹ️  Cast enriched file not found (optional)", level="INFO")
        
        # Load Wikidata enriched data (optional)
        if WIKIDATA_ENRICHED.exists():
            log_message(f"Loading: {WIKIDATA_ENRICHED.name}")
            self.wikidata_df = pd.read_csv(WIKIDATA_ENRICHED)
            log_message(f"  ✓ Loaded {len(self.wikidata_df)} records")
        else:
            log_message(f"ℹ️  Wikidata enriched file not found (optional)", level="INFO")
        
        log_message("\n✅ All available sources loaded successfully!\n")
        return self
    
    def normalize_imdb_ids(self, df, id_column='const'):
        """Normalize IMDB IDs to ensure consistent format (tt + 7 digits)."""
        if id_column not in df.columns:
            # Try alternative column names
            for alt_col in ['Const', 'tconst', 'imdb_id']:
                if alt_col in df.columns:
                    id_column = alt_col
                    break
        
        if id_column in df.columns:
            # Ensure ID starts with 'tt' and has 7+ digits
            df[id_column] = df[id_column].astype(str).str.strip()
            df[id_column] = df[id_column].apply(lambda x: x if x.startswith('tt') else f'tt{x}')
            
            # Validate format
            valid_pattern = df[id_column].str.match(r'^tt\d{7,}$')
            invalid_count = (~valid_pattern).sum()
            
            if invalid_count > 0:
                log_message(f"  ⚠️  {invalid_count} invalid IMDB IDs found", level="WARNING")
            
        return df
    
    def create_display_title(self, row):
        """
        Create display title: Original Title first, then English title if different.
        Format: "Original Title" or "Original Title (English Title)"
        """
        original = str(row.get('original_title', '')).strip()
        english = str(row.get('title', '')).strip()
        
        # Handle missing values
        if pd.isna(original) or original == '' or original == 'nan':
            original = english
        if pd.isna(english) or english == '' or english == 'nan':
            english = original
        
        # If titles are the same, show only once
        if original.lower() == english.lower():
            return original
        
        # If original is empty, use English
        if not original or original == 'nan':
            return english
        
        # If English is empty, use original
        if not english or english == 'nan':
            return original
        
        # Show both: "Original Title (English Title)"
        return f"{original} ({english})"
    
    def merge_data_sources(self):
        """Merge all data sources into a unified master dataset."""
        log_message("=" * 80)
        log_message("Merging data sources...")
        log_message("=" * 80)
        
        # Start with the most complete dataset (usually DDD or OMDB enriched)
        if self.ddd_df is not None:
            base_df = self.ddd_df.copy()
            log_message("Using DDD enriched data as base")
        elif self.omdb_df is not None:
            base_df = self.omdb_df.copy()
            log_message("Using OMDB enriched data as base")
        elif self.tmdb_df is not None:
            base_df = self.tmdb_df.copy()
            log_message("Using TMDB enriched data as base")
        elif self.watched_df is not None:
            base_df = self.watched_df.copy()
            log_message("Using Watched data as base")
        else:
            raise ValueError("No data sources available to merge!")
        
        # Normalize IMDB IDs
        base_df = self.normalize_imdb_ids(base_df)
        
        # Merge with other sources if not already included
        merge_key = 'const'
        
        # The enriched files should already contain all the data
        # But we'll standardize column names and add display title
        
        log_message(f"\nStarting with {len(base_df)} records")
        
        # Standardize column names
        column_mapping = {
            'Const': 'const',
            'Title': 'title',
            'Original Title': 'original_title',
            'Year': 'year',
            'IMDb Rating': 'imdb_rating',
            'Runtime (mins)': 'runtime_mins',
            'Genres': 'genres',
            'Directors': 'directors',
            'Your Rating': 'your_rating',
            'Date Rated': 'date_rated'
        }
        
        base_df = base_df.rename(columns=column_mapping)
        
        # Create display title (ORIGINAL first, then English)
        log_message("\n📝 Creating display titles (Original first)...")
        base_df['display_title'] = base_df.apply(self.create_display_title, axis=1)
        
        # Parse dates
        date_columns = ['date_rated', 'created', 'modified', 'release_date']
        for col in date_columns:
            if col in base_df.columns:
                base_df[col] = pd.to_datetime(base_df[col], errors='coerce')
        
        # Convert numeric columns
        numeric_columns = ['year', 'runtime_mins', 'imdb_rating', 'your_rating', 
                          'num_votes', 'tmdb_rating', 'tmdb_vote_average', 
                          'omdb_metascore', 'tmdb_popularity']
        
        for col in numeric_columns:
            if col in base_df.columns:
                base_df[col] = pd.to_numeric(base_df[col], errors='coerce')
        
        self.master_df = base_df
        
        log_message(f"\n✅ Master dataset created: {len(self.master_df)} records")
        log_message(f"   Columns: {len(self.master_df.columns)}")
        
        return self
    
    def validate_data(self):
        """Validate the merged dataset."""
        log_message("\n" + "=" * 80)
        log_message("Validating master dataset...")
        log_message("=" * 80)
        
        validation = {}
        
        # 1. Check for required columns
        required_columns = ['const', 'title', 'original_title', 'display_title', 
                           'year', 'genres', 'directors']
        missing_cols = [col for col in required_columns if col not in self.master_df.columns]
        
        validation['missing_columns'] = missing_cols
        if missing_cols:
            log_message(f"⚠️  Missing required columns: {missing_cols}", level="WARNING")
        else:
            log_message("✓ All required columns present")
        
        # 2. Check IMDB ID format
        if 'const' in self.master_df.columns:
            valid_ids = self.master_df['const'].str.match(r'^tt\d{7,}$', na=False)
            invalid_count = (~valid_ids).sum()
            validation['invalid_imdb_ids'] = invalid_count
            log_message(f"✓ IMDB IDs: {valid_ids.sum()} valid, {invalid_count} invalid")
        
        # 3. Check year range
        if 'year' in self.master_df.columns:
            year_min = self.master_df['year'].min()
            year_max = self.master_df['year'].max()
            validation['year_range'] = (year_min, year_max)
            log_message(f"✓ Year range: {int(year_min) if not pd.isna(year_min) else 'N/A'} to {int(year_max) if not pd.isna(year_max) else 'N/A'}")
            
            # Check for unrealistic years
            unrealistic_years = self.master_df[
                (self.master_df['year'] < 1888) | (self.master_df['year'] > 2030)
            ]
            if len(unrealistic_years) > 0:
                log_message(f"⚠️  {len(unrealistic_years)} films with unrealistic years", level="WARNING")
        
        # 4. Check rating ranges
        if 'imdb_rating' in self.master_df.columns:
            out_of_range = self.master_df[
                (self.master_df['imdb_rating'] < 0) | (self.master_df['imdb_rating'] > 10)
            ]
            validation['invalid_imdb_ratings'] = len(out_of_range)
            log_message(f"✓ IMDB ratings: {len(out_of_range)} out of range")
        
        if 'your_rating' in self.master_df.columns:
            out_of_range = self.master_df[
                (self.master_df['your_rating'] < 0) | (self.master_df['your_rating'] > 10)
            ]
            validation['invalid_your_ratings'] = len(out_of_range)
            log_message(f"✓ Your ratings: {len(out_of_range)} out of range")
            
            # Count rated vs unrated
            rated_count = self.master_df['your_rating'].notna().sum()
            validation['rated_count'] = rated_count
            log_message(f"✓ Rated films: {rated_count} / {len(self.master_df)}")
        
        # 5. Check for duplicates
        if 'const' in self.master_df.columns:
            duplicates = self.master_df['const'].duplicated().sum()
            validation['duplicates'] = duplicates
            if duplicates > 0:
                log_message(f"⚠️  {duplicates} duplicate IMDB IDs found", level="WARNING")
            else:
                log_message("✓ No duplicate IMDB IDs")
        
        # 6. Check for missing critical data
        critical_columns = ['title', 'year', 'genres']
        for col in critical_columns:
            if col in self.master_df.columns:
                missing = self.master_df[col].isna().sum()
                validation[f'missing_{col}'] = missing
                if missing > 0:
                    log_message(f"⚠️  {col}: {missing} missing values", level="WARNING")
                else:
                    log_message(f"✓ {col}: No missing values")
        
        # 7. Check runtime values
        if 'runtime_mins' in self.master_df.columns:
            invalid_runtime = self.master_df[
                (self.master_df['runtime_mins'] < 1) | (self.master_df['runtime_mins'] > 600)
            ]
            validation['invalid_runtimes'] = len(invalid_runtime)
            if len(invalid_runtime) > 0:
                log_message(f"⚠️  {len(invalid_runtime)} films with unusual runtimes", level="WARNING")
        
        # 8. Summary statistics
        log_message("\n📊 Summary Statistics:")
        log_message(f"   Total films: {len(self.master_df)}")
        
        if 'year' in self.master_df.columns:
            year_counts = self.master_df.groupby(self.master_df['year'] // 10 * 10).size()
            log_message(f"   Decades covered: {len(year_counts)}")
        
        if 'genres' in self.master_df.columns:
            # Count unique genres
            all_genres = self.master_df['genres'].dropna().str.split(',').explode().str.strip()
            unique_genres = all_genres.nunique()
            log_message(f"   Unique genres: {unique_genres}")
        
        if 'directors' in self.master_df.columns:
            unique_directors = self.master_df['directors'].nunique()
            log_message(f"   Unique directors: {unique_directors}")
        
        self.validation_results = validation
        log_message("\n✅ Validation complete!\n")
        
        return self
    
    def save_master_dataset(self):
        """Save the master dataset to CSV."""
        log_message("=" * 80)
        log_message("Saving master dataset...")
        log_message("=" * 80)
        
        # Ensure output directory exists
        MASTER_DATA.parent.mkdir(parents=True, exist_ok=True)
        
        # Save master dataset
        self.master_df.to_csv(MASTER_DATA, index=False)
        log_message(f"✅ Master dataset saved: {MASTER_DATA}")
        log_message(f"   Size: {len(self.master_df)} rows × {len(self.master_df.columns)} columns")
        
        # Save validation report
        self._save_validation_report()
        
        return self
    
    def _save_validation_report(self):
        """Save validation results to a text report."""
        VALIDATION_REPORT.parent.mkdir(parents=True, exist_ok=True)
        
        with open(VALIDATION_REPORT, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("CineScope Data Validation Report\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")
            
            f.write(f"Total Records: {len(self.master_df)}\n")
            f.write(f"Total Columns: {len(self.master_df.columns)}\n\n")
            
            f.write("Validation Results:\n")
            f.write("-" * 40 + "\n")
            for key, value in self.validation_results.items():
                f.write(f"{key}: {value}\n")
            
            f.write("\n" + "=" * 80 + "\n")
            f.write("Column Summary:\n")
            f.write("-" * 40 + "\n")
            
            # Write column info
            for col in sorted(self.master_df.columns):
                dtype = self.master_df[col].dtype
                missing = self.master_df[col].isna().sum()
                missing_pct = (missing / len(self.master_df)) * 100
                f.write(f"{col:30} | {str(dtype):15} | Missing: {missing:5} ({missing_pct:5.1f}%)\n")
        
        log_message(f"✅ Validation report saved: {VALIDATION_REPORT}")
    
    def get_master_data(self):
        """Return the master dataset."""
        if self.master_df is None:
            raise ValueError("Master dataset not yet created. Run load_all_sources() and merge_data_sources() first.")
        return self.master_df


def load_master_data():
    """Convenience function to load the master dataset."""
    if MASTER_DATA.exists():
        log_message(f"Loading master dataset from: {MASTER_DATA}")
        df = pd.read_csv(MASTER_DATA)
        
        # Parse dates
        date_columns = ['date_rated', 'created', 'modified', 'release_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        log_message(f"✅ Loaded {len(df)} records")
        return df
    else:
        raise FileNotFoundError(f"Master dataset not found at {MASTER_DATA}. Run data engineering first.")


def get_genre_list(genre_string):
    """Parse genre string into list."""
    if pd.isna(genre_string):
        return []
    return [g.strip() for g in str(genre_string).split(',') if g.strip()]


def get_director_list(director_string):
    """Parse director string into list."""
    if pd.isna(director_string):
        return []
    return [d.strip() for d in str(director_string).split(',') if d.strip()]


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("CineScope Data Engineering - Data Loading & Merging")
    print("=" * 80 + "\n")
    
    # Create data loader
    loader = DataLoader()
    
    # Execute pipeline
    try:
        loader.load_all_sources()
        loader.merge_data_sources()
        loader.validate_data()
        loader.save_master_dataset()
        
        print("\n" + "=" * 80)
        print("✨ SUCCESS! Master dataset created successfully!")
        print("=" * 80)
        print(f"\nMaster dataset location: {MASTER_DATA}")
        print(f"Validation report: {VALIDATION_REPORT}")
        print(f"\nTotal films: {len(loader.master_df)}")
        print(f"Total columns: {len(loader.master_df.columns)}")
        
        # Show sample display titles
        print("\n📝 Sample Display Titles (Original First):")
        print("-" * 80)
        sample = loader.master_df[['display_title', 'original_title', 'title']].head(10)
        for idx, row in sample.iterrows():
            print(f"  {row['display_title']}")
        
        print("\n🎬 Ready for analysis!")
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()