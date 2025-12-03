"""
CineScope Data Analysis - Main Data Engineering Script
======================================================
This script orchestrates the complete data engineering pipeline:
1. Load all enriched data sources
2. Merge into unified master dataset
3. Validate data quality
4. Create helper indices and caches
5. Generate data quality report

Run this script FIRST before any analysis batches.

Usage:
    python batch_0_data_engineering.py
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import modules
from src.core.config import (
    BASE_DIR, PROCESSED_DATA_DIR, OUTPUT_DIR, REPORTS_DIR,
    MASTER_DATA, VALIDATION_REPORT, log_message,
    validate_config, EXPORTS_DIR
)
from src.core.data_loader import DataLoader, load_master_data
from src.core.helpers import (
    parse_genres, parse_directors, get_decade, get_era,
    explode_genres, get_top_performers, calculate_total_watch_time
)


class DataEngineeringPipeline:
    """Complete data engineering pipeline for CineScope analysis."""
    
    def __init__(self):
        """Initialize the pipeline."""
        self.loader = None
        self.master_df = None
        self.genre_stats = None
        self.director_stats = None
        self.decade_stats = None
        self.quality_metrics = {}
        
    def run_pipeline(self):
        """Execute the complete data engineering pipeline."""
        
        print("\n" + "🎬" * 40)
        print("\n" + " " * 20 + "CINESCOPE DATA ENGINEERING")
        print(" " * 20 + "Batch 0: Foundation Setup")
        print("\n" + "🎬" * 40 + "\n")
        
        # Step 1: Validate configuration
        self.step_1_validate_config()
        
        # Step 2: Load and merge data
        self.step_2_load_and_merge()
        
        # Step 3: Validate data quality
        self.step_3_validate_quality()
        
        # Step 4: Create enhanced columns
        self.step_4_create_enhanced_columns()
        
        # Step 5: Generate statistics
        self.step_5_generate_statistics()
        
        # Step 6: Save outputs
        self.step_6_save_outputs()
        
        # Step 7: Generate report
        self.step_7_generate_report()
        
        print("\n" + "✨" * 40)
        print("\n" + " " * 15 + "DATA ENGINEERING COMPLETE!")
        print(" " * 10 + "Ready to proceed with analysis batches")
        print("\n" + "✨" * 40 + "\n")
    
    def step_1_validate_config(self):
        """Step 1: Validate configuration."""
        print("\n" + "=" * 80)
        print("STEP 1: Validating Configuration")
        print("=" * 80 + "\n")
        
        is_valid = validate_config()
        
        if not is_valid:
            raise ValueError("Configuration validation failed. Please check paths.")
        
        print("✅ Configuration validated\n")
    
    def step_2_load_and_merge(self):
        """Step 2: Load all sources and merge."""
        print("\n" + "=" * 80)
        print("STEP 2: Loading and Merging Data Sources")
        print("=" * 80 + "\n")
        
        self.loader = DataLoader()
        self.loader.load_all_sources()
        self.loader.merge_data_sources()
        
        self.master_df = self.loader.get_master_data()
        
        print(f"\n✅ Master dataset created: {len(self.master_df)} films\n")
    
    def step_3_validate_quality(self):
        """Step 3: Validate data quality."""
        print("\n" + "=" * 80)
        print("STEP 3: Validating Data Quality")
        print("=" * 80 + "\n")
        
        self.loader.validate_data()
        
        # Additional quality checks
        self._check_rating_consistency()
        self._check_temporal_consistency()
        self._check_genre_consistency()
        
        print("\n✅ Data quality validation complete\n")
    
    def step_4_create_enhanced_columns(self):
        """Step 4: Create enhanced columns for analysis."""
        print("\n" + "=" * 80)
        print("STEP 4: Creating Enhanced Columns")
        print("=" * 80 + "\n")
        
        df = self.master_df
        
        # 1. Decade and Era
        log_message("Creating decade and era columns...")
        df['decade'] = df['year'].apply(get_decade)
        df['era'] = df['year'].apply(get_era)
        
        # 2. Primary genre
        log_message("Extracting primary genres...")
        df['primary_genre'] = df['genres'].apply(
            lambda x: parse_genres(x)[0] if parse_genres(x) else 'Unknown'
        )
        
        # 3. Genre count
        df['genre_count'] = df['genres'].apply(
            lambda x: len(parse_genres(x))
        )
        
        # 4. Director count
        df['director_count'] = df['directors'].apply(
            lambda x: len(parse_directors(x)) if pd.notna(x) else 0
        )
        
        # 5. Rating category
        from src.core.helpers import categorize_rating
        if 'your_rating' in df.columns:
            df['rating_category'] = df['your_rating'].apply(categorize_rating)
        if 'imdb_rating' in df.columns:
            df['imdb_rating_category'] = df['imdb_rating'].apply(categorize_rating)
        
        # 6. Rating deviation
        if 'your_rating' in df.columns and 'imdb_rating' in df.columns:
            df['rating_deviation'] = df['your_rating'] - df['imdb_rating']
            df['is_contrarian'] = abs(df['rating_deviation']) >= 2.0
        
        # 7. Runtime category
        from src.core.helpers import categorize_runtime
        if 'runtime_mins' in df.columns:
            df['runtime_category'] = df['runtime_mins'].apply(categorize_runtime)
        
        # 8. Is rated (by user)
        if 'your_rating' in df.columns:
            df['is_rated'] = df['your_rating'].notna()
        
        # 9. Time lag (if date_rated exists)
        if 'date_rated' in df.columns and 'year' in df.columns:
            df['watch_year'] = pd.to_datetime(df['date_rated'], errors='coerce').dt.year
            df['time_lag'] = df['watch_year'] - df['year']
        
        # 10. Season watched (if date_rated exists)
        if 'date_rated' in df.columns:
            from src.core.helpers import get_season
            df['watch_season'] = pd.to_datetime(df['date_rated'], errors='coerce').apply(get_season)
        
        self.master_df = df
        
        log_message(f"✅ Created {10} enhanced columns")
        print()
    
    def step_5_generate_statistics(self):
        """Step 5: Generate summary statistics."""
        print("\n" + "=" * 80)
        print("STEP 5: Generating Statistics")
        print("=" * 80 + "\n")
        
        df = self.master_df
        
        # Genre statistics
        log_message("Calculating genre statistics...")
        genre_exploded = explode_genres(df)
        self.genre_stats = genre_exploded.groupby('genre').agg({
            'const': 'count',
            'your_rating': 'mean',
            'imdb_rating': 'mean',
            'runtime_mins': 'mean'
        }).rename(columns={'const': 'count'}).sort_values('count', ascending=False)
        
        log_message(f"  Found {len(self.genre_stats)} unique genres")
        
        # Director statistics
        log_message("Calculating director statistics...")
        self.director_stats = get_top_performers(df, column='directors', top_n=100)
        log_message(f"  Analyzed top {len(self.director_stats)} directors")
        
        # Decade statistics
        log_message("Calculating decade statistics...")
        self.decade_stats = df.groupby('decade').agg({
            'const': 'count',
            'your_rating': 'mean',
            'imdb_rating': 'mean',
            'runtime_mins': 'mean'
        }).rename(columns={'const': 'count'})
        log_message(f"  Covered {len(self.decade_stats)} decades")
        
        # Quality metrics
        log_message("Calculating quality metrics...")
        self.quality_metrics = {
            'total_films': len(df),
            'rated_films': df['your_rating'].notna().sum() if 'your_rating' in df.columns else 0,
            'avg_your_rating': df['your_rating'].mean() if 'your_rating' in df.columns else None,
            'avg_imdb_rating': df['imdb_rating'].mean() if 'imdb_rating' in df.columns else None,
            'total_runtime_hours': df['runtime_mins'].sum() / 60 if 'runtime_mins' in df.columns else 0,
            'total_runtime_days': df['runtime_mins'].sum() / (60 * 24) if 'runtime_mins' in df.columns else 0,
            'unique_directors': df['directors'].nunique(),
            'unique_genres': len(self.genre_stats),
            'year_span': f"{int(df['year'].min())}-{int(df['year'].max())}" if 'year' in df.columns else 'Unknown',
            'earliest_film': df.nsmallest(1, 'year')['display_title'].iloc[0] if 'year' in df.columns else 'Unknown',
            'latest_film': df.nlargest(1, 'year')['display_title'].iloc[0] if 'year' in df.columns else 'Unknown'
        }
        
        print("\n📊 Key Statistics:")
        print(f"   Total Films: {self.quality_metrics['total_films']:,}")
        print(f"   Rated Films: {self.quality_metrics['rated_films']:,}")
        print(f"   Total Watch Time: {self.quality_metrics['total_runtime_days']:.1f} days ({self.quality_metrics['total_runtime_hours']:.0f} hours)")
        print(f"   Year Span: {self.quality_metrics['year_span']}")
        print(f"   Unique Directors: {self.quality_metrics['unique_directors']:,}")
        print(f"   Unique Genres: {self.quality_metrics['unique_genres']}")
        
        if self.quality_metrics['avg_your_rating']:
            print(f"   Your Avg Rating: {self.quality_metrics['avg_your_rating']:.2f}")
        if self.quality_metrics['avg_imdb_rating']:
            print(f"   IMDB Avg Rating: {self.quality_metrics['avg_imdb_rating']:.2f}")
        
        print()
    
    def step_6_save_outputs(self):
        """Step 6: Save all outputs."""
        print("\n" + "=" * 80)
        print("STEP 6: Saving Outputs")
        print("=" * 80 + "\n")
        
        # Save master dataset
        log_message("Saving master dataset...")
        self.master_df.to_csv(MASTER_DATA, index=False)
        log_message(f"✅ Saved: {MASTER_DATA}")
        
        # Save genre statistics
        genre_output = EXPORTS_DIR / 'genre_statistics.csv'
        self.genre_stats.to_csv(genre_output)
        log_message(f"✅ Saved: {genre_output}")
        
        # Save director statistics
        director_output = EXPORTS_DIR / 'director_statistics.csv'
        self.director_stats.to_csv(director_output, index=False)
        log_message(f"✅ Saved: {director_output}")
        
        # Save decade statistics
        decade_output = EXPORTS_DIR / 'decade_statistics.csv'
        self.decade_stats.to_csv(decade_output)
        log_message(f"✅ Saved: {decade_output}")
        
        # Save quality metrics as JSON
        metrics_output = EXPORTS_DIR / 'quality_metrics.json'
        with open(metrics_output, 'w') as f:
            # Convert numpy types to native Python types
            metrics_clean = {}
            for k, v in self.quality_metrics.items():
                if isinstance(v, (np.integer, np.floating)):
                    metrics_clean[k] = float(v)
                else:
                    metrics_clean[k] = v
            json.dump(metrics_clean, f, indent=2)
        log_message(f"✅ Saved: {metrics_output}")
        
        print()
    
    def step_7_generate_report(self):
        """Step 7: Generate comprehensive report."""
        print("\n" + "=" * 80)
        print("STEP 7: Generating Report")
        print("=" * 80 + "\n")
        
        report_path = REPORTS_DIR / 'batch_0_data_engineering_report.txt'
        
        with open(report_path, 'w', encoding='utf-8') as f:
            self._write_report_header(f)
            self._write_data_summary(f)
            self._write_quality_checks(f)
            self._write_statistics_summary(f)
            self._write_top_lists(f)
            self._write_recommendations(f)
        
        log_message(f"✅ Generated report: {report_path}")
        print()
    
    def _check_rating_consistency(self):
        """Check rating consistency."""
        df = self.master_df
        
        if 'your_rating' in df.columns and 'imdb_rating' in df.columns:
            # Check for extreme deviations
            extreme_dev = abs(df['your_rating'] - df['imdb_rating']) > 3
            if extreme_dev.sum() > 0:
                log_message(f"  ℹ️  {extreme_dev.sum()} films with extreme rating deviations (>3 points)", level="INFO")
    
    def _check_temporal_consistency(self):
        """Check temporal consistency."""
        df = self.master_df
        
        if 'year' in df.columns and 'date_rated' in df.columns:
            # Check for films watched before release
            df['watch_year_temp'] = pd.to_datetime(df['date_rated'], errors='coerce').dt.year
            impossible = df['watch_year_temp'] < df['year']
            if impossible.sum() > 0:
                log_message(f"  ⚠️  {impossible.sum()} films watched before release year", level="WARNING")
    
    def _check_genre_consistency(self):
        """Check genre consistency."""
        df = self.master_df
        
        if 'genres' in df.columns:
            missing_genres = df['genres'].isna().sum()
            if missing_genres > 0:
                log_message(f"  ℹ️  {missing_genres} films missing genre information", level="INFO")
    
    def _write_report_header(self, f):
        """Write report header."""
        f.write("=" * 100 + "\n")
        f.write(" " * 30 + "CINESCOPE DATA ENGINEERING REPORT\n")
        f.write(" " * 35 + "Batch 0: Foundation Setup\n")
        f.write("=" * 100 + "\n\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Base Directory: {BASE_DIR}\n")
        f.write(f"Master Dataset: {MASTER_DATA}\n\n")
    
    def _write_data_summary(self, f):
        """Write data summary section."""
        f.write("=" * 100 + "\n")
        f.write("DATA SUMMARY\n")
        f.write("=" * 100 + "\n\n")
        
        f.write(f"Total Films: {self.quality_metrics['total_films']:,}\n")
        f.write(f"Rated Films: {self.quality_metrics['rated_films']:,}\n")
        f.write(f"Year Span: {self.quality_metrics['year_span']}\n")
        f.write(f"Earliest Film: {self.quality_metrics['earliest_film']}\n")
        f.write(f"Latest Film: {self.quality_metrics['latest_film']}\n\n")
        
        f.write(f"Total Watch Time:\n")
        f.write(f"  - {self.quality_metrics['total_runtime_hours']:.0f} hours\n")
        f.write(f"  - {self.quality_metrics['total_runtime_days']:.1f} days\n")
        f.write(f"  - {self.quality_metrics['total_runtime_days']/7:.1f} weeks\n\n")
        
        f.write(f"Unique Directors: {self.quality_metrics['unique_directors']:,}\n")
        f.write(f"Unique Genres: {self.quality_metrics['unique_genres']}\n\n")
    
    def _write_quality_checks(self, f):
        """Write quality checks section."""
        f.write("=" * 100 + "\n")
        f.write("QUALITY CHECKS\n")
        f.write("=" * 100 + "\n\n")
        
        validation = self.loader.validation_results
        
        for key, value in validation.items():
            f.write(f"{key}: {value}\n")
        
        f.write("\n")
    
    def _write_statistics_summary(self, f):
        """Write statistics summary."""
        f.write("=" * 100 + "\n")
        f.write("STATISTICS SUMMARY\n")
        f.write("=" * 100 + "\n\n")
        
        # Ratings
        if self.quality_metrics['avg_your_rating']:
            f.write(f"Average Your Rating: {self.quality_metrics['avg_your_rating']:.2f}\n")
        if self.quality_metrics['avg_imdb_rating']:
            f.write(f"Average IMDB Rating: {self.quality_metrics['avg_imdb_rating']:.2f}\n")
        
        f.write("\n")
        
        # Top genres
        f.write("Top 10 Genres by Count:\n")
        for idx, (genre, row) in enumerate(self.genre_stats.head(10).iterrows(), 1):
            f.write(f"  {idx:2d}. {genre:20s} - {int(row['count']):4d} films\n")
        
        f.write("\n")
    
    def _write_top_lists(self, f):
        """Write top lists section."""
        f.write("=" * 100 + "\n")
        f.write("TOP DIRECTORS\n")
        f.write("=" * 100 + "\n\n")
        
        for idx, row in self.director_stats.head(20).iterrows():
            f.write(f"  {idx+1:2d}. {row['name']:40s} - {int(row['count']):3d} films\n")
        
        f.write("\n")
    
    def _write_recommendations(self, f):
        """Write recommendations section."""
        f.write("=" * 100 + "\n")
        f.write("RECOMMENDATIONS FOR ANALYSIS\n")
        f.write("=" * 100 + "\n\n")
        
        f.write("Based on your data, we recommend focusing on:\n\n")
        
        # Top genre
        top_genre = self.genre_stats.index[0]
        f.write(f"1. {top_genre} deep dive - your most-watched genre\n")
        
        # Top director
        top_director = self.director_stats.iloc[0]['name']
        f.write(f"2. {top_director} filmography analysis - your most-watched director\n")
        
        # Decade with most films
        top_decade = self.decade_stats['count'].idxmax()
        f.write(f"3. {int(top_decade)}s cinema - your dominant decade\n")
        
        f.write("\n4. Rating deviation analysis - identify your contrarian picks\n")
        f.write("5. Temporal viewing patterns - understand your watching habits\n")
        f.write("6. Actor collaboration networks - discover your favorite ensembles\n\n")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function."""
    try:
        pipeline = DataEngineeringPipeline()
        pipeline.run_pipeline()
        
        print("\n" + "🎉" * 40)
        print("\nSUCCESS! Data engineering complete.")
        print("\nNext Steps:")
        print("  1. Review the data engineering report")
        print("  2. Check master_cinema_data.csv")
        print("  3. Proceed to Batch 1: Quantified Self Analysis")
        print("\n" + "🎉" * 40 + "\n")
        
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