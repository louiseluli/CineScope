"""
CineScope Data Analysis - Configuration File
=============================================
This file contains all configuration settings for the cinema analysis project.
"""

import os
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns

# ============================================================================
# PROJECT PATHS
# ============================================================================

# Base directory (update this to your actual path)
BASE_DIR = Path("/Users/louisesfer/Documents/Programming/CineScope")

# Data directories
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
CACHE_DIR = PROCESSED_DATA_DIR / "imdb_cache"

# Output directories
OUTPUT_DIR = BASE_DIR / "analysis_outputs"
VISUALIZATIONS_DIR = OUTPUT_DIR / "visualizations"
REPORTS_DIR = OUTPUT_DIR / "reports"
EXPORTS_DIR = OUTPUT_DIR / "exports"

# Create output directories if they don't exist
for directory in [OUTPUT_DIR, VISUALIZATIONS_DIR, REPORTS_DIR, EXPORTS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Source file paths
WATCHED_CSV = PROCESSED_DATA_DIR / "WatchedDec.csv"
TMDB_ENRICHED = PROCESSED_DATA_DIR / "01_tmdb_enriched_media.csv"
OMDB_ENRICHED = PROCESSED_DATA_DIR / "02_omdb_enriched_media.csv"
DDD_ENRICHED = PROCESSED_DATA_DIR / "03_ddd_enriched_media.csv"
CAST_ENRICHED = PROCESSED_DATA_DIR / "04_cast_enriched_media.csv"
WIKIDATA_ENRICHED = PROCESSED_DATA_DIR / "05_wikidata_enriched_media.csv"
GROUND_TRUTH = RAW_DATA_DIR / "ground_truth_validated.csv"

# IMDB cache files
IMDB_BASICS = CACHE_DIR / "basics.parquet"
IMDB_NAMES = CACHE_DIR / "names.parquet"
IMDB_PRINCIPALS = CACHE_DIR / "principals.parquet"
IMDB_CREW = CACHE_DIR / "crew.parquet"
IMDB_RATINGS = CACHE_DIR / "ratings.parquet"

# Output file paths
MASTER_DATA = PROCESSED_DATA_DIR / "master_cinema_data.csv"
VALIDATION_REPORT = REPORTS_DIR / "data_validation_report.txt"

# ============================================================================
# COLOR SCHEMES
# ============================================================================

# Rating colors (for different rating systems)
RATING_COLORS = {
    'your_rating': '#FF6B6B',      # Coral red - Your personal ratings
    'imdb': '#4ECDC4',             # Turquoise - IMDB ratings
    'tmdb': '#95E1D3',             # Mint - TMDB ratings
    'critics': '#F38181',          # Salmon - Critics (Metascore)
    'rotten_tomatoes': '#FFE66D'   # Yellow - Rotten Tomatoes
}

# Genre colors (consistent palette for all genre visualizations)
GENRE_COLORS = {
    'Action': '#E63946',           # Bold red
    'Adventure': '#F77F00',        # Orange
    'Animation': '#FCBF49',        # Golden yellow
    'Comedy': '#06FFA5',           # Bright green
    'Crime': '#2B2D42',            # Dark navy
    'Documentary': '#8D99AE',      # Gray blue
    'Drama': '#457B9D',            # Steel blue
    'Family': '#A8DADC',           # Light blue
    'Fantasy': '#9D4EDD',          # Purple
    'Film-Noir': '#1A1A1D',        # Almost black
    'History': '#C3B299',          # Beige
    'Horror': '#6A0572',           # Deep purple
    'Music': '#FF006E',            # Hot pink
    'Musical': '#FB5607',          # Bright orange
    'Mystery': '#3A0CA3',          # Royal blue
    'Romance': '#FF6B9D',          # Pink
    'Sci-Fi': '#4CC9F0',           # Cyan
    'Sport': '#06D6A0',            # Teal
    'Thriller': '#2D3142',         # Charcoal
    'War': '#8B4513',              # Saddle brown
    'Western': '#D4A574'           # Tan
}

# Era/Decade colors (for temporal analysis)
ERA_COLORS = {
    'Silent Era (pre-1927)': '#1A1A1D',
    'Pre-Code (1927-1934)': '#4A4A4A',
    'Golden Age (1935-1959)': '#C3B299',
    'New Hollywood (1960-1979)': '#E8871E',
    'Blockbuster Era (1980-1999)': '#DA4167',
    'Digital Age (2000-2009)': '#4A90E2',
    'Modern (2010-2019)': '#4CC9F0',
    'Current (2020+)': '#B4E7CE'
}

# Gender colors
GENDER_COLORS = {
    'Male': '#4A90E2',            # Blue
    'Female': '#FF6B9D',          # Pink
    'Non-binary': '#9D4EDD',      # Purple
    'Unknown': '#8D99AE'          # Gray
}

# Quality tier colors (for rating-based groupings)
QUALITY_COLORS = {
    'Masterpiece (9-10)': '#2EC4B6',
    'Excellent (8-8.9)': '#4CC9F0',
    'Good (7-7.9)': '#95E1D3',
    'Average (6-6.9)': '#FFD60A',
    'Below Average (5-5.9)': '#FCA311',
    'Poor (4-4.9)': '#E76F51',
    'Bad (1-3.9)': '#C1121F'
}

# Diverging palette (for deviation analysis)
DIVERGING_PALETTE = ['#C1121F', '#E76F51', '#FCA311', '#FFD60A', 
                     '#95E1D3', '#4CC9F0', '#2EC4B6']

# Sequential palettes
SEQUENTIAL_BLUE = ['#E3F2FD', '#90CAF9', '#42A5F5', '#1E88E5', '#1565C0', '#0D47A1']
SEQUENTIAL_RED = ['#FFEBEE', '#EF9A9A', '#E57373', '#EF5350', '#E53935', '#C62828']
SEQUENTIAL_GREEN = ['#E8F5E9', '#A5D6A7', '#66BB6A', '#43A047', '#2E7D32', '#1B5E20']

# ============================================================================
# VISUALIZATION SETTINGS
# ============================================================================

# Default figure settings
DEFAULT_FIGSIZE = (16, 10)
DEFAULT_DPI = 300
DEFAULT_STYLE = 'seaborn-v0_8-darkgrid'  # Using stable seaborn style

# Font settings
FONT_SETTINGS = {
    'family': 'DejaVu Sans',
    'title_size': 20,
    'label_size': 14,
    'tick_size': 12,
    'legend_size': 11
}

# Apply global matplotlib settings
plt.rcParams['figure.figsize'] = DEFAULT_FIGSIZE
plt.rcParams['figure.dpi'] = DEFAULT_DPI
plt.rcParams['font.family'] = FONT_SETTINGS['family']
plt.rcParams['font.size'] = FONT_SETTINGS['tick_size']
plt.rcParams['axes.titlesize'] = FONT_SETTINGS['title_size']
plt.rcParams['axes.labelsize'] = FONT_SETTINGS['label_size']
plt.rcParams['xtick.labelsize'] = FONT_SETTINGS['tick_size']
plt.rcParams['ytick.labelsize'] = FONT_SETTINGS['tick_size']
plt.rcParams['legend.fontsize'] = FONT_SETTINGS['legend_size']
plt.rcParams['figure.titlesize'] = FONT_SETTINGS['title_size'] + 2

# Grid settings
plt.rcParams['grid.alpha'] = 0.3
plt.rcParams['grid.linestyle'] = '--'

# Seaborn settings
sns.set_style('darkgrid')
sns.set_context('notebook', font_scale=1.2)

# ============================================================================
# DATA PROCESSING SETTINGS
# ============================================================================

# Date formats
DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

# Missing value indicators
MISSING_VALUES = ['', 'N/A', 'NA', 'NaN', 'None', 'null', '\\N']

# Rating scale
RATING_SCALE = {
    'min': 1,
    'max': 10,
    'default': 5
}

# Genre parsing settings
GENRE_SEPARATOR = ','
GENRE_CLEAN_PATTERNS = [
    r'\s+',  # Extra whitespace
    r'[^\w\s-]'  # Special characters except hyphens
]

# ============================================================================
# ANALYSIS THRESHOLDS
# ============================================================================

# Minimum counts for inclusion in analyses
MIN_ACTOR_FILMS = 3          # Minimum films for actor analysis
MIN_DIRECTOR_FILMS = 2       # Minimum films for director analysis
MIN_GENRE_COUNT = 5          # Minimum films for genre analysis
MIN_YEAR_COUNT = 1           # Minimum films for year analysis

# Statistical significance
SIGNIFICANCE_LEVEL = 0.05
CONFIDENCE_INTERVAL = 0.95

# Outlier detection (IQR method)
IQR_MULTIPLIER = 1.5

# ============================================================================
# BATCH-SPECIFIC SETTINGS
# ============================================================================

# Batch 1: Quantified Self
BATCH1_HEATMAP_CMAP = 'YlOrRd'
BATCH1_BINS = {
    'runtime': [0, 90, 120, 150, 180, 999],
    'rating': list(range(1, 12))
}

# Batch 2: Content Genome
BATCH2_WORDCLOUD_MAX_WORDS = 100
BATCH2_NETWORK_MIN_CONNECTIONS = 2
BATCH2_TREEMAP_MIN_SIZE = 5

# Batch 3: Actor Analysis
BATCH3_TOP_N_ACTORS = 50
BATCH3_COMPLETION_THRESHOLD = 75  # Percentage for "close to complete"

# Batch 4: Director/Writer Analysis
BATCH4_TOP_N_DIRECTORS = 30
BATCH4_TOP_N_WRITERS = 25
BATCH4_MIN_COLLABORATIONS = 2

# Batch 5: Geography & Time
BATCH5_MAP_PROJECTION = 'natural earth'
BATCH5_DECADES = list(range(1910, 2030, 10))

# Batch 6: Industry Analysis
BATCH6_TOP_N_STUDIOS = 30
BATCH6_BUDGET_BINS = [0, 1e6, 5e6, 20e6, 50e6, 100e6, float('inf')]

# Batch 7: Critical Alignment
BATCH7_DEVIATION_THRESHOLD = 2.0  # Rating points
BATCH7_TOP_N_OUTLIERS = 20

# Batch 8: Advanced Analytics
BATCH8_PREDICTION_FEATURES = ['genre', 'decade', 'runtime', 'director', 'actors']
BATCH8_TOP_N_RECOMMENDATIONS = 50

# Batch 9: Special Categories
BATCH9_MIN_FRANCHISE_SIZE = 2
BATCH9_OSCAR_YEARS = list(range(1928, 2026))

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_batch_output_dir(batch_number):
    """Create and return batch-specific output directory."""
    batch_dir = VISUALIZATIONS_DIR / f"batch_{batch_number}"
    batch_dir.mkdir(parents=True, exist_ok=True)
    return batch_dir

def get_color_palette(palette_name, n_colors=None):
    """Get a color palette by name."""
    palettes = {
        'rating': list(RATING_COLORS.values()),
        'genre': list(GENRE_COLORS.values()),
        'era': list(ERA_COLORS.values()),
        'gender': list(GENDER_COLORS.values()),
        'quality': list(QUALITY_COLORS.values()),
        'diverging': DIVERGING_PALETTE,
        'sequential_blue': SEQUENTIAL_BLUE,
        'sequential_red': SEQUENTIAL_RED,
        'sequential_green': SEQUENTIAL_GREEN
    }
    
    palette = palettes.get(palette_name.lower())
    if palette and n_colors:
        # Interpolate if more colors needed
        if len(palette) < n_colors:
            from matplotlib.colors import LinearSegmentedColormap
            cmap = LinearSegmentedColormap.from_list('custom', palette)
            return [cmap(i / n_colors) for i in range(n_colors)]
        else:
            return palette[:n_colors]
    return palette

def save_figure(fig, filename, batch_number, dpi=None):
    """Save figure to batch-specific directory."""
    output_dir = get_batch_output_dir(batch_number)
    filepath = output_dir / filename
    
    if dpi is None:
        dpi = DEFAULT_DPI
    
    fig.savefig(filepath, dpi=dpi, bbox_inches='tight', facecolor='white')
    print(f"✅ Saved: {filepath}")
    return filepath

def log_message(message, level='INFO'):
    """Simple logging function."""
    print(f"[{level}] {message}")

# ============================================================================
# VALIDATION
# ============================================================================

def validate_config():
    """Validate configuration settings."""
    issues = []
    
    # Check if data directories exist
    if not PROCESSED_DATA_DIR.exists():
        issues.append(f"Processed data directory not found: {PROCESSED_DATA_DIR}")
    
    # Check if key data files exist
    key_files = [WATCHED_CSV, TMDB_ENRICHED, OMDB_ENRICHED]
    for filepath in key_files:
        if not filepath.exists():
            issues.append(f"Required file not found: {filepath}")
    
    if issues:
        print("⚠️ Configuration Issues Found:")
        for issue in issues:
            print(f"  - {issue}")
        return False
    
    print("✅ Configuration validated successfully!")
    return True

# ============================================================================
# MODULE INITIALIZATION
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("CineScope Configuration")
    print("=" * 80)
    print(f"\nBase Directory: {BASE_DIR}")
    print(f"Output Directory: {OUTPUT_DIR}")
    print(f"\nValidating configuration...")
    validate_config()
    print("\nConfiguration loaded successfully! ✨")