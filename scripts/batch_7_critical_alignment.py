"""
CineScope Batch 7: CRITICAL ALIGNMENT
======================================

Analysis of how different critical sources rate my collection
covering Questions 221-250:

CRITICAL CONSENSUS (Q221-230):
- Q221-226: IMDb vs TMDB vs Rotten Tomatoes vs Metacritic alignment
- Q227-228: Genre and decade quality patterns
- Q229-230: Quality distribution and rating variance

QUALITY PATTERNS (Q231-240):
- Q231-235: Quality tier analysis (masterpieces, acclaimed, solid, mixed, poor)
- Q236-240: Source agreement and divergence patterns

COLLECTION PROFILE (Q241-250):
- Q241-250: What my collection reveals about quality preferences

10 Professional Visualizations
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
import logging
from scipy import stats
import sys
import warnings
warnings.filterwarnings('ignore')

# Setup
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "processed"
OUTPUT_DIR = BASE_DIR / "analysis_outputs" / "visualizations" / "batch_7"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Add to path for helpers
sys.path.insert(0, str(BASE_DIR))
from src.core.helpers import parse_genres, explode_genres

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Professional color palette
COLORS = {
    'primary': '#2C3E50',
    'secondary': '#E74C3C',
    'accent': '#3498DB',
    'success': '#27AE60',
    'warning': '#F39C12',
    'info': '#9B59B6',
    'teal': '#1ABC9C',
    'gradient': ['#3498DB', '#9B59B6', '#E74C3C', '#F39C12', '#27AE60', '#1ABC9C']
}

plt.rcParams['figure.dpi'] = 300
plt.rcParams['savefig.dpi'] = 300
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica']


class CriticalAlignmentAnalyzer:
    """Analyze critical consensus across multiple rating sources."""
    
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self._prepare_data()
        logger.info(f"Analyzing {len(self.df)} films")
        logger.info(f"Rating sources available: {', '.join(self.available_sources)}")
        
    def _prepare_data(self):
        """Prepare and normalize rating data from all sources."""
        
        # Parse Rotten Tomatoes percentage to 0-10 scale
        if 'omdb_rating_rotten_tomatoes' in self.df.columns:
            def parse_rt(val):
                if pd.isna(val):
                    return np.nan
                s = str(val).strip().replace('%', '')
                try:
                    return float(s) / 10
                except:
                    return np.nan
            self.df['rt_score'] = self.df['omdb_rating_rotten_tomatoes'].apply(parse_rt)
        
        # Parse Metacritic score to 0-10 scale
        if 'omdb_rating_metacritic' in self.df.columns:
            def parse_meta(val):
                if pd.isna(val):
                    return np.nan
                s = str(val).strip().replace('/100', '')
                try:
                    return float(s) / 10
                except:
                    return np.nan
            self.df['meta_score'] = self.df['omdb_rating_metacritic'].apply(parse_meta)
        
        # Ensure numeric
        for col in ['imdb_rating', 'tmdb_vote_average']:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
        
        # Track which sources are available
        self.available_sources = []
        if 'imdb_rating' in self.df.columns and self.df['imdb_rating'].notna().any():
            self.available_sources.append('IMDb')
        if 'tmdb_vote_average' in self.df.columns and self.df['tmdb_vote_average'].notna().any():
            self.available_sources.append('TMDB')
        if 'rt_score' in self.df.columns and self.df['rt_score'].notna().any():
            self.available_sources.append('Rotten Tomatoes')
        if 'meta_score' in self.df.columns and self.df['meta_score'].notna().any():
            self.available_sources.append('Metacritic')
        
        # Add decade
        if 'year' in self.df.columns:
            self.df['decade'] = (self.df['year'] // 10) * 10
        
        # Quality tiers based on IMDb
        self.df['quality_tier'] = pd.cut(
            self.df['imdb_rating'],
            bins=[0, 5.0, 6.0, 7.0, 8.0, 10],
            labels=['Poor (<5)', 'Mixed (5-6)', 'Solid (6-7)', 'Acclaimed (7-8)', 'Masterpiece (8+)']
        )
        
    def get_source_stats(self, source_name: str, column: str):
        """Get statistics for a rating source."""
        valid = self.df[column].dropna()
        if len(valid) == 0:
            return None
        return {
            'name': source_name,
            'count': len(valid),
            'coverage': len(valid) / len(self.df) * 100,
            'mean': valid.mean(),
            'median': valid.median(),
            'std': valid.std(),
            'min': valid.min(),
            'max': valid.max()
        }


# =============================================================================
# VISUALIZATIONS
# =============================================================================

def viz_1_sources_overview(analyzer: CriticalAlignmentAnalyzer):
    """Compare all available rating sources."""
    logger.info("Creating Viz 1: Rating Sources Overview...")
    
    # Collect stats for each source
    sources = []
    if 'imdb_rating' in analyzer.df.columns:
        stats = analyzer.get_source_stats('IMDb', 'imdb_rating')
        if stats:
            sources.append(stats)
    
    if 'tmdb_vote_average' in analyzer.df.columns:
        stats = analyzer.get_source_stats('TMDB', 'tmdb_vote_average')
        if stats:
            sources.append(stats)
    
    if 'rt_score' in analyzer.df.columns:
        stats = analyzer.get_source_stats('Rotten Tomatoes', 'rt_score')
        if stats:
            sources.append(stats)
    
    if 'meta_score' in analyzer.df.columns:
        stats = analyzer.get_source_stats('Metacritic', 'meta_score')
        if stats:
            sources.append(stats)
    
    if not sources:
        logger.warning("No rating sources available")
        return
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Average ratings comparison
    names = [s['name'] for s in sources]
    means = [s['mean'] for s in sources]
    colors_src = COLORS['gradient'][:len(sources)]
    
    bars = ax1.bar(names, means, color=colors_src, alpha=0.8, edgecolor='black', linewidth=1.5)
    for bar, val in zip(bars, means):
        ax1.text(bar.get_x() + bar.get_width()/2, val + 0.15,
                f'{val:.2f}', ha='center', fontsize=11, fontweight='bold')
    
    ax1.set_ylabel('Average Rating (0-10)', fontsize=11, fontweight='bold')
    ax1.set_title('Average Ratings by Source', fontsize=12, fontweight='bold', pad=15)
    ax1.set_ylim(0, 10)
    ax1.axhline(y=7.0, color='red', linestyle='--', alpha=0.5, linewidth=1)
    ax1.grid(axis='y', alpha=0.3)
    
    # 2. Coverage comparison
    coverage = [s['coverage'] for s in sources]
    ax2.bar(names, coverage, color=colors_src, alpha=0.8, edgecolor='black', linewidth=1.5)
    for i, (name, cov) in enumerate(zip(names, coverage)):
        ax2.text(i, cov + 2, f'{cov:.1f}%', ha='center', fontsize=10, fontweight='bold')
    
    ax2.set_ylabel('Coverage (%)', fontsize=11, fontweight='bold')
    ax2.set_title('Data Coverage by Source', fontsize=12, fontweight='bold', pad=15)
    ax2.set_ylim(0, 110)
    ax2.grid(axis='y', alpha=0.3)
    
    # 3. Distribution violin plots
    plot_data = []
    plot_labels = []
    for s in sources:
        col_map = {
            'IMDb': 'imdb_rating',
            'TMDB': 'tmdb_vote_average',
            'Rotten Tomatoes': 'rt_score',
            'Metacritic': 'meta_score'
        }
        col = col_map.get(s['name'])
        if col and col in analyzer.df.columns:
            data = analyzer.df[col].dropna()
            if len(data) > 0:
                plot_data.append(data)
                plot_labels.append(s['name'])
    
    if plot_data:
        parts = ax3.violinplot(plot_data, positions=range(len(plot_data)),
                               showmeans=True, showmedians=True)
        for pc, color in zip(parts['bodies'], colors_src[:len(plot_data)]):
            pc.set_facecolor(color)
            pc.set_alpha(0.7)
        
        ax3.set_xticks(range(len(plot_labels)))
        ax3.set_xticklabels(plot_labels, rotation=20)
        ax3.set_ylabel('Rating', fontsize=11, fontweight='bold')
        ax3.set_title('Rating Distribution by Source', fontsize=12, fontweight='bold', pad=15)
        ax3.set_ylim(0, 10)
        ax3.axhline(y=7.0, color='red', linestyle='--', alpha=0.5)
        ax3.grid(axis='y', alpha=0.3)
    
    # 4. Standard deviation (consistency)
    stds = [s['std'] for s in sources]
    ax4.bar(names, stds, color=colors_src, alpha=0.8, edgecolor='black', linewidth=1.5)
    for i, (name, std) in enumerate(zip(names, stds)):
        ax4.text(i, std + 0.05, f'{std:.2f}', ha='center', fontsize=10, fontweight='bold')
    
    ax4.set_ylabel('Standard Deviation', fontsize=11, fontweight='bold')
    ax4.set_title('Rating Consistency by Source\n(Lower = More Agreement)', 
                 fontsize=12, fontweight='bold', pad=15)
    ax4.grid(axis='y', alpha=0.3)
    
    plt.suptitle('Critical Sources Comparison', fontsize=15, fontweight='bold', y=0.98)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "01_sources_overview.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 1 complete")


def viz_2_source_correlations(analyzer: CriticalAlignmentAnalyzer):
    """Correlation matrix and scatter plots between sources."""
    logger.info("Creating Viz 2: Source Correlations...")
    
    # Build correlation data
    corr_data = {}
    if 'imdb_rating' in analyzer.df.columns:
        corr_data['IMDb'] = analyzer.df['imdb_rating']
    if 'tmdb_vote_average' in analyzer.df.columns:
        corr_data['TMDB'] = analyzer.df['tmdb_vote_average']
    if 'rt_score' in analyzer.df.columns:
        corr_data['Rotten Tomatoes'] = analyzer.df['rt_score']
    if 'meta_score' in analyzer.df.columns:
        corr_data['Metacritic'] = analyzer.df['meta_score']
    
    if len(corr_data) < 2:
        logger.warning("Need at least 2 sources for correlation")
        return
    
    corr_df = pd.DataFrame(corr_data).corr()
    
    # Create figure with correlation matrix + scatter plots
    n_sources = len(corr_data)
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    axes = axes.flatten()
    
    # Plot 1: Correlation heatmap
    im = axes[0].imshow(corr_df, cmap='RdYlGn', vmin=0.5, vmax=1.0, aspect='auto')
    axes[0].set_xticks(range(len(corr_df.columns)))
    axes[0].set_yticks(range(len(corr_df.columns)))
    axes[0].set_xticklabels(corr_df.columns, rotation=45, ha='right')
    axes[0].set_yticklabels(corr_df.columns)
    
    for i in range(len(corr_df)):
        for j in range(len(corr_df.columns)):
            text = axes[0].text(j, i, f'{corr_df.iloc[i, j]:.3f}',
                              ha="center", va="center", color="black", 
                              fontsize=11, fontweight='bold')
    
    axes[0].set_title('Source Correlation Matrix', fontsize=12, fontweight='bold', pad=15)
    plt.colorbar(im, ax=axes[0], label='Correlation')
    
    # Plots 2-4: Scatter plots between major sources
    plot_idx = 1
    source_pairs = [
        ('IMDb', 'TMDB', 'imdb_rating', 'tmdb_vote_average'),
        ('IMDb', 'Rotten Tomatoes', 'imdb_rating', 'rt_score'),
        ('IMDb', 'Metacritic', 'imdb_rating', 'meta_score')
    ]
    
    for name1, name2, col1, col2 in source_pairs:
        if plot_idx >= 4:
            break
        if col1 in analyzer.df.columns and col2 in analyzer.df.columns:
            valid = ~(analyzer.df[col1].isna() | analyzer.df[col2].isna())
            if valid.sum() > 0:
                x = analyzer.df.loc[valid, col1]
                y = analyzer.df.loc[valid, col2]
                
                axes[plot_idx].scatter(x, y, alpha=0.4, s=20, 
                                      edgecolors='black', linewidth=0.3,
                                      c=COLORS['accent'])
                axes[plot_idx].plot([0, 10], [0, 10], 'k--', alpha=0.5, linewidth=1.5)
                
                # Correlation coefficient
                corr = np.corrcoef(x, y)[0, 1]
                axes[plot_idx].text(0.05, 0.95, f'r = {corr:.3f}',
                                   transform=axes[plot_idx].transAxes,
                                   fontsize=11, fontweight='bold',
                                   bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
                
                axes[plot_idx].set_xlabel(name1, fontsize=11, fontweight='bold')
                axes[plot_idx].set_ylabel(name2, fontsize=11, fontweight='bold')
                axes[plot_idx].set_title(f'{name1} vs {name2}', fontsize=12, fontweight='bold', pad=15)
                axes[plot_idx].grid(alpha=0.3)
                axes[plot_idx].set_xlim(0, 10)
                axes[plot_idx].set_ylim(0, 10)
                
                plot_idx += 1
    
    # Hide unused subplots
    for i in range(plot_idx, 4):
        axes[i].axis('off')
    
    plt.suptitle('Critical Source Correlations', fontsize=15, fontweight='bold', y=0.98)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "02_source_correlations.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 2 complete")


def viz_3_genre_quality(analyzer: CriticalAlignmentAnalyzer):
    """Quality by genre using proper genre parsing."""
    logger.info("Creating Viz 3: Genre Quality Analysis...")
    
    if 'genres' not in analyzer.df.columns:
        logger.warning("No genre data available")
        return
    
    # Use proper genre parsing
    genre_df = explode_genres(analyzer.df, genres_col='genres', id_col='const',
                              keep_cols=['imdb_rating', 'tmdb_vote_average', 'rt_score', 'meta_score'])
    
    if genre_df.empty:
        logger.warning("No genres could be parsed")
        return
    
    # Calculate genre statistics
    genre_stats = genre_df.groupby('genre').agg({
        'const': 'count',
        'imdb_rating': 'mean'
    }).reset_index()
    genre_stats.columns = ['genre', 'count', 'avg_rating']
    genre_stats = genre_stats[genre_stats['count'] >= 5].sort_values('avg_rating', ascending=False)
    
    top_15 = genre_stats.head(15)
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    # 1. Average rating by genre
    bars = ax1.barh(top_15['genre'], top_15['avg_rating'],
                   color=COLORS['accent'], alpha=0.8, edgecolor='black')
    
    for i, (genre, rating, count) in enumerate(zip(top_15['genre'], 
                                                    top_15['avg_rating'],
                                                    top_15['count'])):
        ax1.text(rating + 0.05, i, f'{rating:.2f} ({int(count)})', 
                va='center', fontsize=9)
    
    ax1.set_xlabel('Average IMDb Rating', fontsize=11, fontweight='bold')
    ax1.set_title('Top 15 Genres by Quality', fontsize=12, fontweight='bold', pad=15)
    ax1.set_xlim(0, 10)
    ax1.axvline(x=7.0, color='red', linestyle='--', alpha=0.5, linewidth=1)
    ax1.grid(axis='x', alpha=0.3)
    
    # 2. Distribution for top 5 genres
    top_5_genres = genre_stats.head(5)['genre'].tolist()
    box_data = []
    labels = []
    
    for genre in top_5_genres:
        data = genre_df[genre_df['genre'] == genre]['imdb_rating'].dropna()
        if len(data) >= 3:
            box_data.append(data)
            labels.append(genre)
    
    if box_data:
        bp = ax2.boxplot(box_data, labels=labels, patch_artist=True)
        for patch, color in zip(bp['boxes'], COLORS['gradient'][:len(box_data)]):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
        
        ax2.set_ylabel('IMDb Rating', fontsize=11, fontweight='bold')
        ax2.set_title('Rating Distribution: Top 5 Genres', fontsize=12, fontweight='bold', pad=15)
        ax2.tick_params(axis='x', rotation=20)
        ax2.grid(axis='y', alpha=0.3)
        ax2.axhline(y=7.0, color='red', linestyle='--', alpha=0.5)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "03_genre_quality.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 3 complete")


def viz_4_quality_tiers(analyzer: CriticalAlignmentAnalyzer):
    """Quality tier distribution and analysis."""
    logger.info("Creating Viz 4: Quality Tiers...")
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Quality tier distribution
    tier_counts = analyzer.df['quality_tier'].value_counts().sort_index()
    colors_tier = ['#E74C3C', '#F39C12', '#3498DB', '#27AE60', '#9B59B6']
    
    bars = ax1.barh(tier_counts.index.astype(str), tier_counts.values,
                   color=colors_tier, alpha=0.8, edgecolor='black')
    
    for i, (tier, count) in enumerate(zip(tier_counts.index, tier_counts.values)):
        pct = count / len(analyzer.df) * 100
        ax1.text(count + 5, i, f'{count} ({pct:.1f}%)', va='center', fontsize=10, fontweight='bold')
    
    ax1.set_xlabel('Number of Films', fontsize=11, fontweight='bold')
    ax1.set_title('Quality Tier Distribution', fontsize=12, fontweight='bold', pad=15)
    ax1.grid(axis='x', alpha=0.3)
    
    # 2. IMDb rating histogram with tier overlays
    ax2.hist(analyzer.df['imdb_rating'].dropna(), bins=40, color=COLORS['accent'],
            alpha=0.7, edgecolor='black')
    
    tier_boundaries = [5.0, 6.0, 7.0, 8.0]
    for boundary, color in zip(tier_boundaries, colors_tier[1:]):
        ax2.axvline(boundary, color=color, linestyle='--', linewidth=2, alpha=0.7)
    
    mean_rating = analyzer.df['imdb_rating'].mean()
    ax2.axvline(mean_rating, color='red', linestyle='-', linewidth=2.5,
               label=f'Mean: {mean_rating:.2f}')
    
    ax2.set_xlabel('IMDb Rating', fontsize=11, fontweight='bold')
    ax2.set_ylabel('Count', fontsize=11, fontweight='bold')
    ax2.set_title('Rating Distribution with Tier Boundaries', fontsize=12, fontweight='bold', pad=15)
    ax2.legend()
    ax2.grid(alpha=0.3)
    
    # 3. Cumulative distribution
    sorted_ratings = np.sort(analyzer.df['imdb_rating'].dropna())
    cumulative = np.arange(1, len(sorted_ratings) + 1) / len(sorted_ratings) * 100
    
    ax3.plot(sorted_ratings, cumulative, linewidth=2.5, color=COLORS['success'])
    ax3.fill_between(sorted_ratings, cumulative, alpha=0.3, color=COLORS['success'])
    
    for boundary in tier_boundaries:
        pct = (sorted_ratings <= boundary).sum() / len(sorted_ratings) * 100
        ax3.axvline(boundary, color='red', linestyle='--', alpha=0.5)
        ax3.text(boundary, pct - 5, f'{pct:.0f}%', fontsize=9, rotation=90)
    
    ax3.set_xlabel('IMDb Rating', fontsize=11, fontweight='bold')
    ax3.set_ylabel('Cumulative Percentage', fontsize=11, fontweight='bold')
    ax3.set_title('Cumulative Quality Distribution', fontsize=12, fontweight='bold', pad=15)
    ax3.grid(alpha=0.3)
    
    # 4. Key statistics
    high_quality = (analyzer.df['imdb_rating'] >= 7.0).sum()
    masterpieces = (analyzer.df['imdb_rating'] >= 8.0).sum()
    acclaimed = ((analyzer.df['imdb_rating'] >= 7.0) & (analyzer.df['imdb_rating'] < 8.0)).sum()
    
    stats_text = f"""
    QUALITY STATISTICS
    
    Total Films: {len(analyzer.df):,}
    
    High Quality (≥7.0): {high_quality:,} ({high_quality/len(analyzer.df)*100:.1f}%)
    
    Masterpieces (≥8.0): {masterpieces:,} ({masterpieces/len(analyzer.df)*100:.1f}%)
    
    Acclaimed (7-8): {acclaimed:,} ({acclaimed/len(analyzer.df)*100:.1f}%)
    
    Average Rating: {mean_rating:.2f}
    Median Rating: {analyzer.df['imdb_rating'].median():.2f}
    
    Top Tier: {tier_counts.iloc[-1]} films
    """
    
    ax4.text(0.1, 0.5, stats_text, fontsize=11, verticalalignment='center',
            fontfamily='monospace',
            bbox=dict(boxstyle='round', facecolor=COLORS['teal'], alpha=0.2))
    ax4.axis('off')
    
    plt.suptitle('Quality Tier Analysis', fontsize=15, fontweight='bold', y=0.98)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "04_quality_tiers.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 4 complete")


def viz_5_decade_quality(analyzer: CriticalAlignmentAnalyzer):
    """Quality trends across decades."""
    logger.info("Creating Viz 5: Decade Quality Trends...")
    
    if 'decade' not in analyzer.df.columns:
        logger.warning("No decade information available")
        return
    
    decade_stats = analyzer.df.groupby('decade').agg({
        'imdb_rating': ['mean', 'median', 'std', 'count']
    }).reset_index()
    decade_stats.columns = ['decade', 'mean', 'median', 'std', 'count']
    decade_stats = decade_stats[decade_stats['count'] >= 3]
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Mean & median by decade
    ax1.plot(decade_stats['decade'], decade_stats['mean'],
            marker='o', linewidth=2, markersize=8, color=COLORS['accent'],
            label='Mean')
    ax1.plot(decade_stats['decade'], decade_stats['median'],
            marker='s', linewidth=2, markersize=8, color=COLORS['success'],
            label='Median')
    ax1.fill_between(decade_stats['decade'], decade_stats['mean'],
                    alpha=0.2, color=COLORS['accent'])
    ax1.axhline(analyzer.df['imdb_rating'].mean(), color='red',
               linestyle='--', alpha=0.5, linewidth=1)
    
    ax1.set_xlabel('Decade', fontsize=11, fontweight='bold')
    ax1.set_ylabel('IMDb Rating', fontsize=11, fontweight='bold')
    ax1.set_title('Average Quality by Decade', fontsize=12, fontweight='bold', pad=15)
    ax1.legend()
    ax1.grid(alpha=0.3)
    
    # 2. Collection size by decade
    ax2.bar(decade_stats['decade'], decade_stats['count'],
           color=COLORS['success'], alpha=0.8, edgecolor='black', width=5)
    ax2.set_xlabel('Decade', fontsize=11, fontweight='bold')
    ax2.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
    ax2.set_title('Collection Size by Decade', fontsize=12, fontweight='bold', pad=15)
    ax2.grid(axis='y', alpha=0.3)
    
    # 3. Quality consistency (std dev)
    ax3.bar(decade_stats['decade'], decade_stats['std'],
           color=COLORS['warning'], alpha=0.8, edgecolor='black', width=5)
    ax3.set_xlabel('Decade', fontsize=11, fontweight='bold')
    ax3.set_ylabel('Standard Deviation', fontsize=11, fontweight='bold')
    ax3.set_title('Quality Variance by Decade\n(Lower = More Consistent)',
                 fontsize=12, fontweight='bold', pad=15)
    ax3.grid(axis='y', alpha=0.3)
    
    # 4. Quality vs quantity
    scatter = ax4.scatter(decade_stats['count'], decade_stats['mean'],
                         s=decade_stats['count']*3, alpha=0.6,
                         c=decade_stats['decade'], cmap='viridis',
                         edgecolors='black', linewidth=1.5)
    
    for _, row in decade_stats.iterrows():
        ax4.annotate(f"{int(row['decade'])}s",
                    (row['count'], row['mean']),
                    xytext=(5, 5), textcoords='offset points', fontsize=9)
    
    ax4.set_xlabel('Films Watched', fontsize=11, fontweight='bold')
    ax4.set_ylabel('Average Rating', fontsize=11, fontweight='bold')
    ax4.set_title('Quality vs Quantity by Decade', fontsize=12, fontweight='bold', pad=15)
    ax4.grid(alpha=0.3)
    
    plt.suptitle('Decade Quality Evolution', fontsize=15, fontweight='bold', y=0.98)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "05_decade_quality.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 5 complete")


def viz_6_source_divergence(analyzer: CriticalAlignmentAnalyzer):
    """Films where sources disagree most."""
    logger.info("Creating Viz 6: Source Divergence Analysis...")
    
    # Need at least 2 sources
    rating_cols = []
    if 'imdb_rating' in analyzer.df.columns:
        rating_cols.append('imdb_rating')
    if 'tmdb_vote_average' in analyzer.df.columns:
        rating_cols.append('tmdb_vote_average')
    if 'rt_score' in analyzer.df.columns:
        rating_cols.append('rt_score')
    if 'meta_score' in analyzer.df.columns:
        rating_cols.append('meta_score')
    
    if len(rating_cols) < 2:
        logger.warning("Need at least 2 sources for divergence analysis")
        return
    
    # Calculate variance across sources for each film
    df = analyzer.df[rating_cols + ['title']].copy()
    df['variance'] = df[rating_cols].var(axis=1, skipna=True)
    df['range'] = df[rating_cols].max(axis=1, skipna=True) - df[rating_cols].min(axis=1, skipna=True)
    
    # Get films with highest divergence
    divergent = df.nlargest(20, 'range').dropna(subset=['range'])
    
    if divergent.empty:
        logger.warning("No divergence data available")
        return
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    # 1. Top divergent films
    y_pos = range(len(divergent))
    ax1.barh(y_pos, divergent['range'], color=COLORS['secondary'], alpha=0.8, edgecolor='black')
    
    labels = [f"{row['title'][:40]}..." if len(str(row['title'])) > 40
             else row['title'] for _, row in divergent.iterrows()]
    ax1.set_yticks(y_pos)
    ax1.set_yticklabels(labels, fontsize=8)
    ax1.set_xlabel('Rating Range (Max - Min)', fontsize=11, fontweight='bold')
    ax1.set_title('Top 20 Films with Biggest Source Disagreement',
                 fontsize=12, fontweight='bold', pad=15)
    ax1.grid(axis='x', alpha=0.3)
    
    # 2. Variance distribution
    all_variance = df['variance'].dropna()
    ax2.hist(all_variance, bins=30, color=COLORS['accent'], alpha=0.7, edgecolor='black')
    ax2.axvline(all_variance.mean(), color='red', linestyle='--', linewidth=2,
               label=f'Mean: {all_variance.mean():.2f}')
    ax2.axvline(all_variance.median(), color='orange', linestyle='--', linewidth=2,
               label=f'Median: {all_variance.median():.2f}')
    
    ax2.set_xlabel('Rating Variance', fontsize=11, fontweight='bold')
    ax2.set_ylabel('Count', fontsize=11, fontweight='bold')
    ax2.set_title('Distribution of Source Agreement\n(Lower = More Agreement)',
                 fontsize=12, fontweight='bold', pad=15)
    ax2.legend()
    ax2.grid(alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "06_source_divergence.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 6 complete")


def viz_7_interactive_dashboard(analyzer: CriticalAlignmentAnalyzer):
    """Interactive rating dashboard."""
    logger.info("Creating Viz 7: Interactive Dashboard...")
    
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Rating Distribution', 'Source Comparison',
                       'Quality Tiers', 'Decade Evolution'),
        specs=[[{'type': 'histogram'}, {'type': 'bar'}],
               [{'type': 'bar'}, {'type': 'scatter'}]]
    )
    
    # 1. IMDb rating histogram
    fig.add_trace(
        go.Histogram(x=analyzer.df['imdb_rating'], nbinsx=40,
                    marker_color=COLORS['accent'], name='IMDb Rating'),
        row=1, col=1
    )
    
    # 2. Source averages
    sources = []
    avgs = []
    if 'imdb_rating' in analyzer.df.columns:
        sources.append('IMDb')
        avgs.append(analyzer.df['imdb_rating'].mean())
    if 'tmdb_vote_average' in analyzer.df.columns:
        sources.append('TMDB')
        avgs.append(analyzer.df['tmdb_vote_average'].mean())
    if 'rt_score' in analyzer.df.columns:
        sources.append('RT')
        avgs.append(analyzer.df['rt_score'].mean())
    if 'meta_score' in analyzer.df.columns:
        sources.append('Meta')
        avgs.append(analyzer.df['meta_score'].mean())
    
    fig.add_trace(
        go.Bar(x=sources, y=avgs, marker_color=COLORS['gradient'][:len(sources)],
              name='Avg Rating'),
        row=1, col=2
    )
    
    # 3. Quality tiers
    tier_counts = analyzer.df['quality_tier'].value_counts().sort_index()
    fig.add_trace(
        go.Bar(x=tier_counts.index.astype(str), y=tier_counts.values,
              marker_color=['#E74C3C', '#F39C12', '#3498DB', '#27AE60', '#9B59B6'],
              name='Tier Count'),
        row=2, col=1
    )
    
    # 4. Decade evolution
    if 'decade' in analyzer.df.columns:
        decade_avg = analyzer.df.groupby('decade')['imdb_rating'].mean().reset_index()
        fig.add_trace(
            go.Scatter(x=decade_avg['decade'], y=decade_avg['imdb_rating'],
                      mode='lines+markers', name='Decade Avg',
                      line=dict(color=COLORS['success'], width=3),
                      marker=dict(size=10)),
            row=2, col=2
        )
    
    fig.update_layout(
        height=900,
        title_text="Critical Alignment Dashboard",
        title_font_size=18,
        showlegend=False,
        template='plotly_white'
    )
    
    fig.write_html(OUTPUT_DIR / "07_interactive_dashboard.html")
    logger.info("✅ Viz 7 complete")


def viz_8_quality_heatmap(analyzer: CriticalAlignmentAnalyzer):
    """Quality heatmap: decade × rating tier."""
    logger.info("Creating Viz 8: Quality Heatmap...")
    
    if 'decade' not in analyzer.df.columns:
        logger.warning("No decade information")
        return
    
    # Create pivot
    pivot = pd.crosstab(analyzer.df['decade'], analyzer.df['quality_tier'])
    
    fig, ax = plt.subplots(figsize=(14, 10))
    
    im = ax.imshow(pivot.values, cmap='YlGnBu', aspect='auto')
    
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_yticks(range(len(pivot.index)))
    ax.set_xticklabels(pivot.columns, rotation=45, ha='right')
    ax.set_yticklabels([f"{int(d)}s" for d in pivot.index])
    
    # Annotations
    for i in range(len(pivot.index)):
        for j in range(len(pivot.columns)):
            text = ax.text(j, i, int(pivot.iloc[i, j]),
                         ha="center", va="center", color="black", fontsize=10)
    
    ax.set_xlabel('Quality Tier', fontsize=12, fontweight='bold')
    ax.set_ylabel('Decade', fontsize=12, fontweight='bold')
    ax.set_title('Quality Heatmap: Decade × Tier',
                fontsize=14, fontweight='bold', pad=20)
    
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label('Number of Films', fontsize=11)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "08_quality_heatmap.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 8 complete")


def viz_9_rotten_tomatoes_analysis(analyzer: CriticalAlignmentAnalyzer):
    """Specific Rotten Tomatoes analysis."""
    logger.info("Creating Viz 9: Rotten Tomatoes Analysis...")
    
    if 'rt_score' not in analyzer.df.columns or analyzer.df['rt_score'].isna().all():
        logger.warning("No Rotten Tomatoes data available")
        return
    
    rt_data = analyzer.df[analyzer.df['rt_score'].notna()].copy()
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. RT distribution
    ax1.hist(rt_data['rt_score'], bins=30, color='#FA320A', alpha=0.7, edgecolor='black')
    mean_rt = rt_data['rt_score'].mean()
    ax1.axvline(mean_rt, color='black', linestyle='--', linewidth=2,
               label=f'Mean: {mean_rt:.2f}')
    ax1.set_xlabel('Rotten Tomatoes Score (0-10)', fontsize=11, fontweight='bold')
    ax1.set_ylabel('Count', fontsize=11, fontweight='bold')
    ax1.set_title('Rotten Tomatoes Distribution', fontsize=12, fontweight='bold', pad=15)
    ax1.legend()
    ax1.grid(alpha=0.3)
    
    # 2. RT vs IMDb
    ax2.scatter(rt_data['imdb_rating'], rt_data['rt_score'],
               alpha=0.5, s=30, color='#FA320A', edgecolors='black', linewidth=0.3)
    ax2.plot([0, 10], [0, 10], 'k--', alpha=0.5, linewidth=1.5)
    
    corr = np.corrcoef(rt_data['imdb_rating'].dropna(), 
                       rt_data['rt_score'].dropna())[0, 1]
    ax2.text(0.05, 0.95, f'r = {corr:.3f}',
            transform=ax2.transAxes, fontsize=11, fontweight='bold',
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    ax2.set_xlabel('IMDb Rating', fontsize=11, fontweight='bold')
    ax2.set_ylabel('Rotten Tomatoes Score', fontsize=11, fontweight='bold')
    ax2.set_title('RT vs IMDb Correlation', fontsize=12, fontweight='bold', pad=15)
    ax2.grid(alpha=0.3)
    
    # 3. Fresh vs Rotten (60% threshold)
    fresh = (rt_data['rt_score'] >= 6.0).sum()
    rotten = (rt_data['rt_score'] < 6.0).sum()
    
    ax3.pie([fresh, rotten], labels=['Fresh (≥60%)', 'Rotten (<60%)'],
           autopct='%1.1f%%', colors=['#FA320A', '#6c757d'],
           startangle=90)
    ax3.set_title('Fresh vs Rotten Classification', fontsize=12, fontweight='bold', pad=15)
    
    # 4. RT coverage
    has_rt = analyzer.df['rt_score'].notna().sum()
    no_rt = analyzer.df['rt_score'].isna().sum()
    
    ax4.bar(['Has RT Score', 'No RT Score'], [has_rt, no_rt],
           color=[COLORS['success'], COLORS['secondary']], alpha=0.8, edgecolor='black')
    
    for i, val in enumerate([has_rt, no_rt]):
        pct = val / len(analyzer.df) * 100
        ax4.text(i, val + 10, f'{val}\n({pct:.1f}%)',
                ha='center', fontsize=10, fontweight='bold')
    
    ax4.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
    ax4.set_title('Rotten Tomatoes Coverage', fontsize=12, fontweight='bold', pad=15)
    ax4.grid(axis='y', alpha=0.3)
    
    plt.suptitle('Rotten Tomatoes Analysis', fontsize=15, fontweight='bold', y=0.98)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "09_rotten_tomatoes_analysis.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 9 complete")


def viz_10_metacritic_analysis(analyzer: CriticalAlignmentAnalyzer):
    """Specific Metacritic analysis."""
    logger.info("Creating Viz 10: Metacritic Analysis...")
    
    if 'meta_score' not in analyzer.df.columns or analyzer.df['meta_score'].isna().all():
        logger.warning("No Metacritic data available")
        return
    
    meta_data = analyzer.df[analyzer.df['meta_score'].notna()].copy()
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Metacritic distribution
    ax1.hist(meta_data['meta_score'], bins=30, color='#66CC33', alpha=0.7, edgecolor='black')
    mean_meta = meta_data['meta_score'].mean()
    ax1.axvline(mean_meta, color='black', linestyle='--', linewidth=2,
               label=f'Mean: {mean_meta:.2f}')
    ax1.set_xlabel('Metacritic Score (0-10)', fontsize=11, fontweight='bold')
    ax1.set_ylabel('Count', fontsize=11, fontweight='bold')
    ax1.set_title('Metacritic Distribution', fontsize=12, fontweight='bold', pad=15)
    ax1.legend()
    ax1.grid(alpha=0.3)
    
    # 2. Metacritic vs IMDb
    ax2.scatter(meta_data['imdb_rating'], meta_data['meta_score'],
               alpha=0.5, s=30, color='#66CC33', edgecolors='black', linewidth=0.3)
    ax2.plot([0, 10], [0, 10], 'k--', alpha=0.5, linewidth=1.5)
    
    corr = np.corrcoef(meta_data['imdb_rating'].dropna(),
                       meta_data['meta_score'].dropna())[0, 1]
    ax2.text(0.05, 0.95, f'r = {corr:.3f}',
            transform=ax2.transAxes, fontsize=11, fontweight='bold',
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    ax2.set_xlabel('IMDb Rating', fontsize=11, fontweight='bold')
    ax2.set_ylabel('Metacritic Score', fontsize=11, fontweight='bold')
    ax2.set_title('Metacritic vs IMDb Correlation', fontsize=12, fontweight='bold', pad=15)
    ax2.grid(alpha=0.3)
    
    # 3. Metacritic categories
    universal_acclaim = (meta_data['meta_score'] >= 8.1).sum()
    generally_favorable = ((meta_data['meta_score'] >= 6.1) & (meta_data['meta_score'] < 8.1)).sum()
    mixed = ((meta_data['meta_score'] >= 4.0) & (meta_data['meta_score'] < 6.1)).sum()
    unfavorable = (meta_data['meta_score'] < 4.0).sum()
    
    categories = ['Universal\nAcclaim\n(81+)', 'Generally\nFavorable\n(61-80)',
                 'Mixed\n(40-60)', 'Unfavorable\n(<40)']
    counts = [universal_acclaim, generally_favorable, mixed, unfavorable]
    colors = ['#66CC33', '#FFCC33', '#FF9933', '#FF0000']
    
    bars = ax3.bar(categories, counts, color=colors, alpha=0.8, edgecolor='black')
    for bar, count in zip(bars, counts):
        pct = count / len(meta_data) * 100
        ax3.text(bar.get_x() + bar.get_width()/2, count + 2,
                f'{count}\n({pct:.1f}%)', ha='center', fontsize=9, fontweight='bold')
    
    ax3.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
    ax3.set_title('Metacritic Categories', fontsize=12, fontweight='bold', pad=15)
    ax3.grid(axis='y', alpha=0.3)
    
    # 4. Metacritic coverage
    has_meta = analyzer.df['meta_score'].notna().sum()
    no_meta = analyzer.df['meta_score'].isna().sum()
    
    ax4.bar(['Has Meta Score', 'No Meta Score'], [has_meta, no_meta],
           color=[COLORS['success'], COLORS['secondary']], alpha=0.8, edgecolor='black')
    
    for i, val in enumerate([has_meta, no_meta]):
        pct = val / len(analyzer.df) * 100
        ax4.text(i, val + 10, f'{val}\n({pct:.1f}%)',
                ha='center', fontsize=10, fontweight='bold')
    
    ax4.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
    ax4.set_title('Metacritic Coverage', fontsize=12, fontweight='bold', pad=15)
    ax4.grid(axis='y', alpha=0.3)
    
    plt.suptitle('Metacritic Analysis', fontsize=15, fontweight='bold', y=0.98)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "10_metacritic_analysis.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 10 complete")


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def generate_summary(analyzer: CriticalAlignmentAnalyzer) -> str:
    """Generate comprehensive summary."""
    
    # Get source stats
    source_stats = []
    for name, col in [('IMDb', 'imdb_rating'), ('TMDB', 'tmdb_vote_average'),
                      ('Rotten Tomatoes', 'rt_score'), ('Metacritic', 'meta_score')]:
        stats = analyzer.get_source_stats(name, col)
        if stats:
            source_stats.append(stats)
    
    summary = f"""
{'='*80}
BATCH 7: CRITICAL ALIGNMENT - COMPLETE
{'='*80}

📊 RATING SOURCES AVAILABLE: {len(source_stats)}

"""
    
    for stats in source_stats:
        summary += f"""
{stats['name']}:
  Coverage: {stats['count']:,} films ({stats['coverage']:.1f}%)
  Average: {stats['mean']:.2f}
  Range: {stats['min']:.1f} - {stats['max']:.1f}
"""
    
    # Quality distribution
    tier_counts = analyzer.df['quality_tier'].value_counts().sort_index()
    high_quality = (analyzer.df['imdb_rating'] >= 7.0).sum()
    masterpieces = (analyzer.df['imdb_rating'] >= 8.0).sum()
    
    summary += f"""
📈 QUALITY DISTRIBUTION

Total Films: {len(analyzer.df):,}

High Quality (≥7.0): {high_quality:,} ({high_quality/len(analyzer.df)*100:.1f}%)
Masterpieces (≥8.0): {masterpieces:,} ({masterpieces/len(analyzer.df)*100:.1f}%)

Quality Tiers:
"""
    
    for tier, count in tier_counts.items():
        pct = count / len(analyzer.df) * 100
        summary += f"  {tier}: {count} ({pct:.1f}%)\n"
    
    summary += f"""
🎬 VISUALIZATIONS CREATED:

1. 01_sources_overview.png - Rating sources comparison
2. 02_source_correlations.png - Source correlation matrix
3. 03_genre_quality.png - Quality by genre (properly parsed)
4. 04_quality_tiers.png - Tier distribution analysis
5. 05_decade_quality.png - Quality evolution across decades
6. 06_source_divergence.png - Films where critics disagree
7. 07_interactive_dashboard.html - Interactive explorer
8. 08_quality_heatmap.png - Decade × quality matrix
9. 09_rotten_tomatoes_analysis.png - RT-specific analysis
10. 10_metacritic_analysis.png - Metacritic-specific analysis

✅ Batch 7 Complete!
{'='*80}
"""
    
    return summary


def main():
    logger.info("="*80)
    logger.info("BATCH 7: CRITICAL ALIGNMENT")
    logger.info("="*80)
    
    # Load data
    master_file = DATA_DIR / "watched_movies_master.csv"
    if not master_file.exists():
        logger.error(f"Master file not found: {master_file}")
        logger.error("Please run batch_0_filter_watched.py first")
        return
    
    df = pd.read_csv(master_file, low_memory=False)
    logger.info(f"Loaded {len(df)} films")
    
    # Initialize analyzer
    analyzer = CriticalAlignmentAnalyzer(df)
    
    # Generate visualizations
    logger.info("\n" + "="*80)
    logger.info("GENERATING VISUALIZATIONS")
    logger.info("="*80)
    
    viz_1_sources_overview(analyzer)
    viz_2_source_correlations(analyzer)
    viz_3_genre_quality(analyzer)
    viz_4_quality_tiers(analyzer)
    viz_5_decade_quality(analyzer)
    viz_6_source_divergence(analyzer)
    viz_7_interactive_dashboard(analyzer)
    viz_8_quality_heatmap(analyzer)
    viz_9_rotten_tomatoes_analysis(analyzer)
    viz_10_metacritic_analysis(analyzer)
    
    # Generate and save summary
    summary = generate_summary(analyzer)
    print(summary)
    
    with open(OUTPUT_DIR / "BATCH_7_SUMMARY.txt", 'w') as f:
        f.write(summary)
    
    logger.info(f"\n✅ All visualizations saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()