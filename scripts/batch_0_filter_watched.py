"""
CineScope - Filter to Watched Movies Only
=========================================
Creates a dataset containing ONLY watched movies based on Watched-Dec.csv
This will be used for all analysis batches.

The master_cinema_data.csv includes your entire collection (watched + unwatched).
This script filters to ONLY the films you've actually watched.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import MASTER_DATA, RAW_DATA_DIR, PROCESSED_DATA_DIR, log_message


class WatchedFilter:
    """Filter master dataset to watched movies only."""
    
    def __init__(self):
        """Initialize the filter."""
        self.master_df = None
        self.watched_df = None
        self.filtered_df = None
        
    def load_data(self):
        """Load both datasets."""
        log_message("=" * 80)
        log_message("Loading Datasets...")
        log_message("=" * 80)
        
        # Load master (cleaned) dataset
        log_message("Loading master dataset...")
        self.master_df = pd.read_csv(MASTER_DATA)
        log_message(f"  Master: {len(self.master_df):,} films")
        
        # Load Watched-Dec.csv (ground truth)
        watched_path = RAW_DATA_DIR / "Watched-Dec.csv"
        log_message(f"\nLoading watched list: {watched_path.name}")
        self.watched_df = pd.read_csv(watched_path)
        log_message(f"  Watched: {len(self.watched_df):,} films")
        
        return self
    
    def filter_to_watched(self):
        """Filter master dataset to only watched movies."""
        log_message("\n" + "=" * 80)
        log_message("Filtering to Watched Movies Only...")
        log_message("=" * 80)
        
        # Normalize IMDB IDs in watched list
        if 'Const' in self.watched_df.columns:
            watched_ids = self.watched_df['Const'].str.strip().unique()
        elif 'const' in self.watched_df.columns:
            watched_ids = self.watched_df['const'].str.strip().unique()
        else:
            raise ValueError("Cannot find IMDB ID column in Watched-Dec.csv")
        
        log_message(f"Unique watched film IDs: {len(watched_ids):,}")
        
        # Filter master to only watched IDs
        self.filtered_df = self.master_df[
            self.master_df['const'].isin(watched_ids)
        ].copy()
        
        log_message(f"Matched films in master: {len(self.filtered_df):,}")
        
        # Check for films in watched list but not in master
        master_ids = set(self.master_df['const'].unique())
        watched_ids_set = set(watched_ids)
        missing_in_master = watched_ids_set - master_ids
        
        if len(missing_in_master) > 0:
            log_message(f"\n⚠️ Warning: {len(missing_in_master)} watched films not found in master", level="WARNING")
            log_message(f"  (These may have been filtered out during cleaning)")
        
        return self
    
    def add_watched_metadata(self):
        """Add metadata from watched list (like date rated, position)."""
        log_message("\n" + "=" * 80)
        log_message("Adding Watched Metadata...")
        log_message("=" * 80)
        
        # Prepare watched dataframe
        watched_meta = self.watched_df.copy()
        
        # Normalize column names
        if 'Const' in watched_meta.columns:
            watched_meta = watched_meta.rename(columns={'Const': 'const'})
        
        # Select relevant columns from watched list
        watched_cols = ['const', 'Position', 'Created', 'Modified', 'Date Rated', 'Your Rating']
        
        # Rename to match master format
        rename_map = {
            'Position': 'watched_position',
            'Created': 'watched_created',
            'Modified': 'watched_modified',
            'Date Rated': 'date_rated',
            'Your Rating': 'your_rating'
        }
        
        # Only keep columns that exist
        existing_cols = [c for c in watched_cols if c in watched_meta.columns]
        watched_meta = watched_meta[existing_cols].copy()
        
        # Rename columns
        for old, new in rename_map.items():
            if old in watched_meta.columns:
                watched_meta = watched_meta.rename(columns={old: new})
        
        # Merge with filtered dataset
        # Drop existing date_rated/your_rating from master if they exist
        cols_to_drop = [c for c in ['date_rated', 'your_rating', 'watched_position', 
                                     'watched_created', 'watched_modified'] 
                       if c in self.filtered_df.columns]
        if cols_to_drop:
            self.filtered_df = self.filtered_df.drop(columns=cols_to_drop)
        
        # Merge
        self.filtered_df = self.filtered_df.merge(
            watched_meta,
            on='const',
            how='left'
        )
        
        log_message(f"✅ Added watched metadata")
        
        # Convert dates
        date_cols = ['date_rated', 'watched_created', 'watched_modified']
        for col in date_cols:
            if col in self.filtered_df.columns:
                self.filtered_df[col] = pd.to_datetime(self.filtered_df[col], errors='coerce')
        
        return self
    
    def recalculate_stats(self):
        """Recalculate statistics for watched-only dataset."""
        log_message("\n" + "=" * 80)
        log_message("Recalculating Statistics...")
        log_message("=" * 80)
        
        df = self.filtered_df
        
        # Recalculate decade
        df['decade'] = (df['year'] // 10 * 10).astype('Int64')
        
        # Recalculate era
        from src.core.helpers import get_era
        df['era'] = df['year'].apply(get_era)
        
        # Recalculate time lag
        if 'date_rated' in df.columns and 'year' in df.columns:
            df['watch_year'] = df['date_rated'].dt.year
            df['time_lag'] = df['watch_year'] - df['year']
        
        # Recalculate rating deviation
        if 'your_rating' in df.columns and 'imdb_rating' in df.columns:
            df['rating_deviation'] = df['your_rating'] - df['imdb_rating']
            df['is_contrarian'] = abs(df['rating_deviation']) >= 2.0
        
        # Mark all as watched
        df['is_watched'] = True
        
        self.filtered_df = df
        
        log_message("✅ Statistics recalculated")
        
        return self
    
    def generate_summary(self):
        """Generate summary statistics."""
        log_message("\n" + "=" * 80)
        log_message("WATCHED COLLECTION SUMMARY")
        log_message("=" * 80)
        
        df = self.filtered_df
        
        log_message(f"\nTotal Watched Films: {len(df):,}")
        
        if 'year' in df.columns:
            year_min = df['year'].min()
            year_max = df['year'].max()
            log_message(f"Year Range: {int(year_min) if not pd.isna(year_min) else 'N/A'} - {int(year_max) if not pd.isna(year_max) else 'N/A'}")
        
        if 'runtime_mins' in df.columns:
            total_mins = df['runtime_mins'].sum()
            log_message(f"Total Watch Time: {total_mins/60:.0f} hours ({total_mins/(60*24):.1f} days)")
        
        if 'imdb_rating' in df.columns:
            log_message(f"Avg IMDB Rating: {df['imdb_rating'].mean():.2f}")
        
        if 'your_rating' in df.columns:
            rated_count = df['your_rating'].notna().sum()
            log_message(f"Films You Rated: {rated_count} ({rated_count/len(df)*100:.1f}%)")
            if rated_count > 0:
                log_message(f"Your Avg Rating: {df['your_rating'].mean():.2f}")
        
        # Genre breakdown
        from src.core.helpers import explode_genres
        genre_exploded = explode_genres(df)
        top_genres = genre_exploded['genre'].value_counts().head(10)
        
        log_message(f"\nTop 10 Genres Watched:")
        for i, (genre, count) in enumerate(top_genres.items(), 1):
            log_message(f"  {i:2d}. {genre:20s} - {count:4d} films")
        
        # Decade breakdown
        if 'decade' in df.columns:
            decade_counts = df['decade'].value_counts().sort_index()
            log_message(f"\nFilms by Decade:")
            for decade, count in decade_counts.items():
                if not pd.isna(decade):
                    log_message(f"  {int(decade)}s: {count:4d} films")
        
        return self
    
    def save_watched_dataset(self):
        """Save the watched-only dataset."""
        log_message("\n" + "=" * 80)
        log_message("Saving Watched-Only Dataset...")
        log_message("=" * 80)
        
        output_path = PROCESSED_DATA_DIR / "watched_movies_master.csv"
        self.filtered_df.to_csv(output_path, index=False)
        
        log_message(f"✅ Saved: {output_path}")
        log_message(f"   {len(self.filtered_df):,} watched films")
        log_message(f"   {len(self.filtered_df.columns)} columns")
        
        return self


def main():
    """Main execution."""
    print("\n" + "🎬" * 40)
    print("\n" + " " * 20 + "FILTER TO WATCHED MOVIES")
    print(" " * 15 + "Creating Watched-Only Analysis Dataset")
    print("\n" + "🎬" * 40 + "\n")
    
    try:
        filter = WatchedFilter()
        filter.load_data()
        filter.filter_to_watched()
        filter.add_watched_metadata()
        filter.recalculate_stats()
        filter.generate_summary()
        filter.save_watched_dataset()
        
        print("\n" + "✨" * 40)
        print("\n" + " " * 10 + "WATCHED-ONLY DATASET CREATED!")
        print(" " * 5 + "Ready for analysis on YOUR watched films")
        print("\n" + "✨" * 40 + "\n")
        
        print("📊 Next: Run Batch 1 to create visualizations!")
        print("    All analysis will be on YOUR watched films only.\n")
        
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