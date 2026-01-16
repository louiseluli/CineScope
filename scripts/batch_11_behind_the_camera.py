"""
CineScope Batch 11: BEHIND THE CAMERA
======================================

Comprehensive Cinematographer & Composer Analysis

This batch analyzes the UNDERUTILIZED tmdb_cinematographers and tmdb_composers
columns in my dataset. These contain rich data that has never been visualized!

Coverage:
- Cinematographers: 95.5% of films (2,186 films)
- Composers: 93.3% of films (2,135 films)

10 Professional Visualizations:
1. Top Cinematographers Leaderboard
2. Top Composers Leaderboard
3. Director-Cinematographer Collaborations (Heatmap)
4. Director-Composer Collaborations (Heatmap)
5. Cinematographer × Genre Preferences
6. Composer × Genre Preferences
7. Behind-Camera Gender Representation (if detectable)
8. Award-Winning Crew in Collection
9. Cinematography Style by Era
10. Crew Collaboration Network

Author: CineScope Analysis Pipeline
Date: December 2025
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
import logging
from collections import Counter, defaultdict
from datetime import datetime

# ============================================================================
# SETUP
# ============================================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "processed"
OUTPUT_DIR = BASE_DIR / "analysis_outputs" / "visualizations" / "batch_11"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPORT_FILE = BASE_DIR / "analysis_outputs" / "reports" / "batch_11_behind_camera_report.txt"
REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# CineScope Color Palette
COLORS = {
    'primary': '#2C3E50',
    'secondary': '#E74C3C',
    'accent': '#3498DB',
    'success': '#27AE60',
    'warning': '#F39C12',
    'info': '#1ABC9C',
    'gradient': ['#3498DB', '#9B59B6', '#E74C3C', '#F39C12', '#27AE60', '#1ABC9C'],
    'cinema_gold': '#FFD700',
    'cinema_silver': '#C0C0C0',
    'cinema_bronze': '#CD7F32'
}

# Matplotlib settings
plt.rcParams['figure.dpi'] = 300
plt.rcParams['savefig.dpi'] = 300
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica', 'DejaVu Sans']

# ============================================================================
# DATA LOADING & PROCESSING
# ============================================================================

class BehindTheCameraAnalyzer:
    """Analyze cinematographers and composers in my film collection."""

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.cinematographer_data = None
        self.composer_data = None
        self.stats = {}

        logger.info(f"Loaded {len(self.df)} films for behind-the-camera analysis")
        self._process_data()

    def _process_data(self):
        """Extract and process cinematographer and composer data."""

        # Process cinematographers
        cinematographer_records = []
        for _, film in self.df.iterrows():
            cinematographers_str = film.get('tmdb_cinematographers', '')
            if pd.isna(cinematographers_str) or not cinematographers_str:
                continue

            # Split by pipe (TMDB uses | separator)
            cinematographers = [c.strip() for c in str(cinematographers_str).split('|') if c.strip()]

            for cinematographer in cinematographers:
                # Safe year extraction
                year_val = film.get('year', film.get('Year', 2000))
                try:
                    year = int(year_val) if pd.notna(year_val) and str(year_val).strip() != '' else 2000
                except Exception:
                    year = 2000
                cinematographer_records.append({
                    'name': cinematographer,
                    'film': film.get('title', film.get('Title', 'Unknown')),
                    'year': year,
                    'decade': (year // 10) * 10,
                    'rating': float(film.get('imdb_rating', film.get('IMDb Rating', 0))),
                    'genres': film.get('genres', film.get('Genres', '')),
                    'director': film.get('directors', film.get('Directors', '')),
                    'budget': float(film.get('tmdb_budget', 0)) if pd.notna(film.get('tmdb_budget')) else 0,
                    'revenue': float(film.get('tmdb_revenue', 0)) if pd.notna(film.get('tmdb_revenue')) else 0
                })

        self.cinematographer_data = pd.DataFrame(cinematographer_records)

        # Process composers
        composer_records = []
        for _, film in self.df.iterrows():
            composers_str = film.get('tmdb_composers', '')
            if pd.isna(composers_str) or not composers_str:
                continue

            # Split by pipe
            composers = [c.strip() for c in str(composers_str).split('|') if c.strip()]

            for composer in composers:
                year_val = film.get('year', film.get('Year', 2000))
                try:
                    year = int(year_val) if pd.notna(year_val) and str(year_val).strip() != '' else 2000
                except Exception:
                    year = 2000
                composer_records.append({
                    'name': composer,
                    'film': film.get('title', film.get('Title', 'Unknown')),
                    'year': year,
                    'decade': (year // 10) * 10,
                    'rating': float(film.get('imdb_rating', film.get('IMDb Rating', 0))),
                    'genres': film.get('genres', film.get('Genres', '')),
                    'director': film.get('directors', film.get('Directors', '')),
                    'budget': float(film.get('tmdb_budget', 0)) if pd.notna(film.get('tmdb_budget')) else 0,
                    'revenue': float(film.get('tmdb_revenue', 0)) if pd.notna(film.get('tmdb_revenue')) else 0
                })

        self.composer_data = pd.DataFrame(composer_records)

        # Calculate stats
        self.stats = {
            'total_films': len(self.df),
            'films_with_cinematographers': self.df['tmdb_cinematographers'].notna().sum(),
            'films_with_composers': self.df['tmdb_composers'].notna().sum(),
            'unique_cinematographers': len(self.cinematographer_data['name'].unique()) if len(self.cinematographer_data) > 0 else 0,
            'unique_composers': len(self.composer_data['name'].unique()) if len(self.composer_data) > 0 else 0,
            'cinematographer_coverage': (self.df['tmdb_cinematographers'].notna().sum() / len(self.df) * 100),
            'composer_coverage': (self.df['tmdb_composers'].notna().sum() / len(self.df) * 100)
        }

        logger.info(f"Processed {self.stats['unique_cinematographers']} unique cinematographers")
        logger.info(f"Processed {self.stats['unique_composers']} unique composers")
        logger.info(f"Cinematographer coverage: {self.stats['cinematographer_coverage']:.1f}%")
        logger.info(f"Composer coverage: {self.stats['composer_coverage']:.1f}%")

    def viz_01_top_cinematographers(self):
        """Visualization 1: Top Cinematographers Leaderboard."""

        logger.info("Creating Visualization 1: Top Cinematographers Leaderboard")

        # Aggregate by cinematographer
        cinematographer_stats = self.cinematographer_data.groupby('name').agg({
            'film': 'count',
            'rating': 'mean',
            'year': ['min', 'max']
        }).reset_index()

        cinematographer_stats.columns = ['name', 'film_count', 'avg_rating', 'first_year', 'last_year']
        cinematographer_stats = cinematographer_stats.sort_values('film_count', ascending=False).head(25)

        # Create figure
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 10))
        fig.suptitle('Top 25 Cinematographers in My Collection',
                     fontsize=20, fontweight='bold', y=0.98)

        # Left: Film count
        bars1 = ax1.barh(range(len(cinematographer_stats)),
                         cinematographer_stats['film_count'],
                         color=COLORS['gradient'][:len(cinematographer_stats)])
        ax1.set_yticks(range(len(cinematographer_stats)))
        ax1.set_yticklabels(cinematographer_stats['name'], fontsize=9)
        ax1.invert_yaxis()
        ax1.set_xlabel('Number of Films', fontsize=12, fontweight='bold')
        ax1.set_title('By Frequency', fontsize=14, fontweight='bold')
        ax1.grid(axis='x', alpha=0.3, linestyle='--')

        # Add value labels
        for i, (bar, value) in enumerate(zip(bars1, cinematographer_stats['film_count'])):
            ax1.text(value + 0.2, bar.get_y() + bar.get_height()/2,
                    f'{int(value)}',
                    va='center', fontsize=8, fontweight='bold')

        # Right: Average rating
        bars2 = ax2.barh(range(len(cinematographer_stats)),
                         cinematographer_stats['avg_rating'],
                         color=COLORS['gradient'][:len(cinematographer_stats)])
        ax2.set_yticks(range(len(cinematographer_stats)))
        ax2.set_yticklabels(cinematographer_stats['name'], fontsize=9)
        ax2.invert_yaxis()
        ax2.set_xlabel('Average IMDb Rating', fontsize=12, fontweight='bold')
        ax2.set_title('By Quality', fontsize=14, fontweight='bold')
        ax2.set_xlim(0, 10)
        ax2.grid(axis='x', alpha=0.3, linestyle='--')

        # Add value labels
        for i, (bar, value) in enumerate(zip(bars2, cinematographer_stats['avg_rating'])):
            ax2.text(value + 0.1, bar.get_y() + bar.get_height()/2,
                    f'{value:.1f}',
                    va='center', fontsize=8, fontweight='bold')

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / '01_top_cinematographers.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 01_top_cinematographers.png")

        return cinematographer_stats.head(10)

    def viz_02_top_composers(self):
        """Visualization 2: Top Composers Leaderboard."""

        logger.info("Creating Visualization 2: Top Composers Leaderboard")

        # Aggregate by composer
        composer_stats = self.composer_data.groupby('name').agg({
            'film': 'count',
            'rating': 'mean',
            'year': ['min', 'max']
        }).reset_index()

        composer_stats.columns = ['name', 'film_count', 'avg_rating', 'first_year', 'last_year']
        composer_stats = composer_stats.sort_values('film_count', ascending=False).head(25)

        # Create figure
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 10))
        fig.suptitle('Top 25 Composers in My Collection',
                     fontsize=20, fontweight='bold', y=0.98)

        # Left: Film count
        bars1 = ax1.barh(range(len(composer_stats)),
                         composer_stats['film_count'],
                         color=COLORS['gradient'][:len(composer_stats)])
        ax1.set_yticks(range(len(composer_stats)))
        ax1.set_yticklabels(composer_stats['name'], fontsize=9)
        ax1.invert_yaxis()
        ax1.set_xlabel('Number of Films', fontsize=12, fontweight='bold')
        ax1.set_title('By Frequency', fontsize=14, fontweight='bold')
        ax1.grid(axis='x', alpha=0.3, linestyle='--')

        # Add value labels
        for i, (bar, value) in enumerate(zip(bars1, composer_stats['film_count'])):
            ax1.text(value + 0.2, bar.get_y() + bar.get_height()/2,
                    f'{int(value)}',
                    va='center', fontsize=8, fontweight='bold')

        # Right: Average rating
        bars2 = ax2.barh(range(len(composer_stats)),
                         composer_stats['avg_rating'],
                         color=COLORS['gradient'][:len(composer_stats)])
        ax2.set_yticks(range(len(composer_stats)))
        ax2.set_yticklabels(composer_stats['name'], fontsize=9)
        ax2.invert_yaxis()
        ax2.set_xlabel('Average IMDb Rating', fontsize=12, fontweight='bold')
        ax2.set_title('By Quality', fontsize=14, fontweight='bold')
        ax2.set_xlim(0, 10)
        ax2.grid(axis='x', alpha=0.3, linestyle='--')

        # Add value labels
        for i, (bar, value) in enumerate(zip(bars2, composer_stats['avg_rating'])):
            ax2.text(value + 0.1, bar.get_y() + bar.get_height()/2,
                    f'{value:.1f}',
                    va='center', fontsize=8, fontweight='bold')

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / '02_top_composers.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 02_top_composers.png")

        return composer_stats.head(10)

    def viz_03_director_cinematographer_collaborations(self):
        """Visualization 3: Director-Cinematographer Collaboration Heatmap."""

        logger.info("Creating Visualization 3: Director-Cinematographer Collaborations")

        # Get top directors and cinematographers
        top_cinematographers = self.cinematographer_data['name'].value_counts().head(15).index.tolist()
        top_directors = self.cinematographer_data['director'].value_counts().head(15).index.tolist()

        # Create collaboration matrix
        collab_matrix = np.zeros((len(top_directors), len(top_cinematographers)))

        for i, director in enumerate(top_directors):
            for j, cinematographer in enumerate(top_cinematographers):
                count = len(self.cinematographer_data[
                    (self.cinematographer_data['director'] == director) &
                    (self.cinematographer_data['name'] == cinematographer)
                ])
                collab_matrix[i, j] = count

        # Create heatmap
        fig, ax = plt.subplots(figsize=(16, 12))

        im = ax.imshow(collab_matrix, cmap='YlOrRd', aspect='auto')

        # Set ticks
        ax.set_xticks(range(len(top_cinematographers)))
        ax.set_yticks(range(len(top_directors)))
        ax.set_xticklabels(top_cinematographers, rotation=45, ha='right', fontsize=9)
        ax.set_yticklabels(top_directors, fontsize=9)

        # Add colorbar
        cbar = plt.colorbar(im, ax=ax)
        cbar.set_label('Number of Collaborations', rotation=270, labelpad=20, fontsize=12)

        # Add text annotations
        for i in range(len(top_directors)):
            for j in range(len(top_cinematographers)):
                if collab_matrix[i, j] > 0:
                    text = ax.text(j, i, int(collab_matrix[i, j]),
                                 ha="center", va="center", color="white" if collab_matrix[i, j] > collab_matrix.max()/2 else "black",
                                 fontweight='bold', fontsize=10)

        ax.set_title('Director × Cinematographer Collaborations\n(Top 15 of Each)',
                    fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Cinematographer', fontsize=12, fontweight='bold')
        ax.set_ylabel('Director', fontsize=12, fontweight='bold')

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / '03_director_cinematographer_heatmap.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 03_director_cinematographer_heatmap.png")

        # Find top collaborations
        top_collabs = []
        for i, director in enumerate(top_directors):
            for j, cinematographer in enumerate(top_cinematographers):
                if collab_matrix[i, j] > 1:
                    top_collabs.append({
                        'director': director,
                        'cinematographer': cinematographer,
                        'collaborations': int(collab_matrix[i, j])
                    })

        return pd.DataFrame(top_collabs).sort_values('collaborations', ascending=False).head(10)

    def viz_04_director_composer_collaborations(self):
        """Visualization 4: Director-Composer Collaboration Heatmap."""

        logger.info("Creating Visualization 4: Director-Composer Collaborations")

        # Get top directors and composers
        top_composers = self.composer_data['name'].value_counts().head(15).index.tolist()
        top_directors = self.composer_data['director'].value_counts().head(15).index.tolist()

        # Create collaboration matrix
        collab_matrix = np.zeros((len(top_directors), len(top_composers)))

        for i, director in enumerate(top_directors):
            for j, composer in enumerate(top_composers):
                count = len(self.composer_data[
                    (self.composer_data['director'] == director) &
                    (self.composer_data['name'] == composer)
                ])
                collab_matrix[i, j] = count

        # Create heatmap
        fig, ax = plt.subplots(figsize=(16, 12))

        im = ax.imshow(collab_matrix, cmap='PuBuGn', aspect='auto')

        # Set ticks
        ax.set_xticks(range(len(top_composers)))
        ax.set_yticks(range(len(top_directors)))
        ax.set_xticklabels(top_composers, rotation=45, ha='right', fontsize=9)
        ax.set_yticklabels(top_directors, fontsize=9)

        # Add colorbar
        cbar = plt.colorbar(im, ax=ax)
        cbar.set_label('Number of Collaborations', rotation=270, labelpad=20, fontsize=12)

        # Add text annotations
        for i in range(len(top_directors)):
            for j in range(len(top_composers)):
                if collab_matrix[i, j] > 0:
                    text = ax.text(j, i, int(collab_matrix[i, j]),
                                 ha="center", va="center", color="white" if collab_matrix[i, j] > collab_matrix.max()/2 else "black",
                                 fontweight='bold', fontsize=10)

        ax.set_title('Director × Composer Collaborations\n(Top 15 of Each)',
                    fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Composer', fontsize=12, fontweight='bold')
        ax.set_ylabel('Director', fontsize=12, fontweight='bold')

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / '04_director_composer_heatmap.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 04_director_composer_heatmap.png")

        # Find top collaborations
        top_collabs = []
        for i, director in enumerate(top_directors):
            for j, composer in enumerate(top_composers):
                if collab_matrix[i, j] > 1:
                    top_collabs.append({
                        'director': director,
                        'composer': composer,
                        'collaborations': int(collab_matrix[i, j])
                    })

        return pd.DataFrame(top_collabs).sort_values('collaborations', ascending=False).head(10)

    def viz_05_cinematographer_genre_preferences(self):
        """Visualization 5: Cinematographer × Genre Preferences."""

        logger.info("Creating Visualization 5: Cinematographer × Genre Preferences")

        # Get top 15 cinematographers
        top_cinematographers = self.cinematographer_data['name'].value_counts().head(15).index.tolist()

        # Extract genres and create matrix
        genre_counts = defaultdict(lambda: defaultdict(int))

        for _, row in self.cinematographer_data[self.cinematographer_data['name'].isin(top_cinematographers)].iterrows():
            cinematographer = row['name']
            genres_str = row['genres']
            if pd.notna(genres_str):
                # Handle both list-style strings and comma-separated
                if genres_str.startswith('['):
                    import ast
                    genres = ast.literal_eval(genres_str)
                else:
                    genres = [g.strip() for g in str(genres_str).split(',') if g.strip()]

                for genre in genres:
                    genre_counts[cinematographer][genre] += 1

        # Get most common genres overall
        all_genres = Counter()
        for cinematographer_genres in genre_counts.values():
            all_genres.update(cinematographer_genres)
        top_genres = [g for g, _ in all_genres.most_common(10)]

        # Create matrix
        matrix = np.zeros((len(top_cinematographers), len(top_genres)))
        for i, cinematographer in enumerate(top_cinematographers):
            for j, genre in enumerate(top_genres):
                matrix[i, j] = genre_counts[cinematographer][genre]

        # Normalize by row (percentage of each cinematographer's films)
        row_sums = matrix.sum(axis=1, keepdims=True)
        matrix_pct = np.divide(matrix, row_sums, where=row_sums!=0) * 100

        # Create heatmap
        fig, ax = plt.subplots(figsize=(14, 12))

        im = ax.imshow(matrix_pct, cmap='viridis', aspect='auto')

        ax.set_xticks(range(len(top_genres)))
        ax.set_yticks(range(len(top_cinematographers)))
        ax.set_xticklabels(top_genres, rotation=45, ha='right', fontsize=10)
        ax.set_yticklabels(top_cinematographers, fontsize=10)

        cbar = plt.colorbar(im, ax=ax)
        cbar.set_label('% of Films', rotation=270, labelpad=20, fontsize=12)

        # Add text annotations
        for i in range(len(top_cinematographers)):
            for j in range(len(top_genres)):
                if matrix_pct[i, j] > 0:
                    text = ax.text(j, i, f'{matrix_pct[i, j]:.0f}%',
                                 ha="center", va="center",
                                 color="white" if matrix_pct[i, j] > 25 else "black",
                                 fontsize=8)

        ax.set_title('Cinematographer × Genre Preferences\n(Top 15 Cinematographers, Top 10 Genres)',
                    fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Genre', fontsize=12, fontweight='bold')
        ax.set_ylabel('Cinematographer', fontsize=12, fontweight='bold')

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / '05_cinematographer_genre_heatmap.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 05_cinematographer_genre_heatmap.png")

    def viz_06_composer_genre_preferences(self):
        """Visualization 6: Composer × Genre Preferences."""

        logger.info("Creating Visualization 6: Composer × Genre Preferences")

        # Get top 15 composers
        top_composers = self.composer_data['name'].value_counts().head(15).index.tolist()

        # Extract genres and create matrix
        genre_counts = defaultdict(lambda: defaultdict(int))

        for _, row in self.composer_data[self.composer_data['name'].isin(top_composers)].iterrows():
            composer = row['name']
            genres_str = row['genres']
            if pd.notna(genres_str):
                # Handle both list-style strings and comma-separated
                if genres_str.startswith('['):
                    import ast
                    genres = ast.literal_eval(genres_str)
                else:
                    genres = [g.strip() for g in str(genres_str).split(',') if g.strip()]

                for genre in genres:
                    genre_counts[composer][genre] += 1

        # Get most common genres overall
        all_genres = Counter()
        for composer_genres in genre_counts.values():
            all_genres.update(composer_genres)
        top_genres = [g for g, _ in all_genres.most_common(10)]

        # Create matrix
        matrix = np.zeros((len(top_composers), len(top_genres)))
        for i, composer in enumerate(top_composers):
            for j, genre in enumerate(top_genres):
                matrix[i, j] = genre_counts[composer][genre]

        # Normalize by row
        row_sums = matrix.sum(axis=1, keepdims=True)
        matrix_pct = np.divide(matrix, row_sums, where=row_sums!=0) * 100

        # Create heatmap
        fig, ax = plt.subplots(figsize=(14, 12))

        im = ax.imshow(matrix_pct, cmap='plasma', aspect='auto')

        ax.set_xticks(range(len(top_genres)))
        ax.set_yticks(range(len(top_composers)))
        ax.set_xticklabels(top_genres, rotation=45, ha='right', fontsize=10)
        ax.set_yticklabels(top_composers, fontsize=10)

        cbar = plt.colorbar(im, ax=ax)
        cbar.set_label('% of Films', rotation=270, labelpad=20, fontsize=12)

        # Add text annotations
        for i in range(len(top_composers)):
            for j in range(len(top_genres)):
                if matrix_pct[i, j] > 0:
                    text = ax.text(j, i, f'{matrix_pct[i, j]:.0f}%',
                                 ha="center", va="center",
                                 color="white" if matrix_pct[i, j] > 25 else "black",
                                 fontsize=8)

        ax.set_title('Composer × Genre Preferences\n(Top 15 Composers, Top 10 Genres)',
                    fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Genre', fontsize=12, fontweight='bold')
        ax.set_ylabel('Composer', fontsize=12, fontweight='bold')

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / '06_composer_genre_heatmap.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 06_composer_genre_heatmap.png")

    def viz_07_crew_by_decade(self):
        """Visualization 7: Cinematography & Music by Era."""

        logger.info("Creating Visualization 7: Crew Evolution by Decade")

        # Group by decade
        cinematographer_decades = self.cinematographer_data.groupby('decade').agg({
            'name': 'nunique',
            'rating': 'mean'
        }).reset_index()

        composer_decades = self.composer_data.groupby('decade').agg({
            'name': 'nunique',
            'rating': 'mean'
        }).reset_index()

        # Create figure
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
        fig.suptitle('Cinematography & Music Evolution Across Decades',
                     fontsize=18, fontweight='bold')

        # Cinematographers by decade
        ax1.plot(cinematographer_decades['decade'],
                cinematographer_decades['name'],
                marker='o', linewidth=2, markersize=8,
                color=COLORS['accent'], label='Unique Cinematographers')
        ax1.set_xlabel('Decade', fontsize=12)
        ax1.set_ylabel('Number of Unique Cinematographers', fontsize=12, color=COLORS['accent'])
        ax1.tick_params(axis='y', labelcolor=COLORS['accent'])
        ax1.grid(alpha=0.3)
        ax1.set_title('Cinematographers in My Collection', fontsize=14, fontweight='bold')

        ax1_twin = ax1.twinx()
        ax1_twin.plot(cinematographer_decades['decade'],
                     cinematographer_decades['rating'],
                     marker='s', linewidth=2, markersize=8,
                     color=COLORS['secondary'], linestyle='--',
                     label='Average Rating')
        ax1_twin.set_ylabel('Average IMDb Rating', fontsize=12, color=COLORS['secondary'])
        ax1_twin.tick_params(axis='y', labelcolor=COLORS['secondary'])
        ax1_twin.set_ylim(0, 10)

        # Composers by decade
        ax2.plot(composer_decades['decade'],
                composer_decades['name'],
                marker='o', linewidth=2, markersize=8,
                color=COLORS['success'], label='Unique Composers')
        ax2.set_xlabel('Decade', fontsize=12)
        ax2.set_ylabel('Number of Unique Composers', fontsize=12, color=COLORS['success'])
        ax2.tick_params(axis='y', labelcolor=COLORS['success'])
        ax2.grid(alpha=0.3)
        ax2.set_title('Composers in My Collection', fontsize=14, fontweight='bold')

        ax2_twin = ax2.twinx()
        ax2_twin.plot(composer_decades['decade'],
                     composer_decades['rating'],
                     marker='s', linewidth=2, markersize=8,
                     color=COLORS['warning'], linestyle='--',
                     label='Average Rating')
        ax2_twin.set_ylabel('Average IMDb Rating', fontsize=12, color=COLORS['warning'])
        ax2_twin.tick_params(axis='y', labelcolor=COLORS['warning'])
        ax2_twin.set_ylim(0, 10)

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / '07_crew_evolution_by_decade.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 07_crew_evolution_by_decade.png")

    def viz_08_famous_collaborations(self):
        """Visualization 8: Famous Director-Cinematographer-Composer Trios."""

        logger.info("Creating Visualization 8: Famous Collaborations")

        # Merge cinematographer and composer data on film
        merged = self.cinematographer_data.merge(
            self.composer_data[['film', 'name']],
            on='film',
            how='inner',
            suffixes=('_cinematographer', '_composer')
        )

        # Find recurring trios
        trio_counts = merged.groupby(['director', 'name_cinematographer', 'name_composer']).size().reset_index(name='count')
        trio_counts = trio_counts[trio_counts['count'] > 1].sort_values('count', ascending=False).head(20)

        # Create visualization
        fig, ax = plt.subplots(figsize=(16, 10))

        y_pos = range(len(trio_counts))
        bars = ax.barh(y_pos, trio_counts['count'], color=COLORS['gradient'][:len(trio_counts)])

        # Create labels
        labels = []
        for _, row in trio_counts.iterrows():
            label = f"{row['director'][:20]}\n{row['name_cinematographer'][:20]} (DP)\n{row['name_composer'][:20]} (Music)"
            labels.append(label)

        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels, fontsize=8)
        ax.invert_yaxis()
        ax.set_xlabel('Number of Collaborations', fontsize=12, fontweight='bold')
        ax.set_title('Famous Director-Cinematographer-Composer Trios\n(Recurring Collaborations)',
                    fontsize=16, fontweight='bold', pad=20)
        ax.grid(axis='x', alpha=0.3, linestyle='--')

        # Add value labels
        for bar, value in zip(bars, trio_counts['count']):
            ax.text(value + 0.1, bar.get_y() + bar.get_height()/2,
                   f'{int(value)}', va='center', fontsize=9, fontweight='bold')

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / '08_famous_collaborations.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 08_famous_collaborations.png")

        return trio_counts.head(10)

    def viz_09_crew_quality_impact(self):
        """Visualization 9: Crew Quality Impact Analysis."""

        logger.info("Creating Visualization 9: Crew Quality Impact")

        # Get top tier cinematographers and composers (min 3 films)
        top_cinematographers = self.cinematographer_data.groupby('name').filter(lambda x: len(x) >= 3).groupby('name')['rating'].mean().sort_values(ascending=False).head(15)
        top_composers = self.composer_data.groupby('name').filter(lambda x: len(x) >= 3).groupby('name')['rating'].mean().sort_values(ascending=False).head(15)

        # Create figure
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 10))
        fig.suptitle('Crew Quality Impact: Average Film Ratings\n(Minimum 3 Films)',
                     fontsize=18, fontweight='bold', y=0.98)

        # Cinematographers
        ax1.barh(range(len(top_cinematographers)), top_cinematographers.values,
                color=COLORS['gradient'][:len(top_cinematographers)])
        ax1.set_yticks(range(len(top_cinematographers)))
        ax1.set_yticklabels(top_cinematographers.index, fontsize=9)
        ax1.invert_yaxis()
        ax1.set_xlabel('Average IMDb Rating', fontsize=12, fontweight='bold')
        ax1.set_title('Top 15 Cinematographers by Quality', fontsize=14, fontweight='bold')
        ax1.set_xlim(0, 10)
        ax1.grid(axis='x', alpha=0.3, linestyle='--')
        ax1.axvline(x=self.cinematographer_data['rating'].mean(),
                   color='red', linestyle='--', linewidth=2, label='Collection Average')
        ax1.legend()

        # Add value labels
        for i, value in enumerate(top_cinematographers.values):
            ax1.text(value + 0.1, i, f'{value:.2f}',
                    va='center', fontsize=8, fontweight='bold')

        # Composers
        ax2.barh(range(len(top_composers)), top_composers.values,
                color=COLORS['gradient'][:len(top_composers)])
        ax2.set_yticks(range(len(top_composers)))
        ax2.set_yticklabels(top_composers.index, fontsize=9)
        ax2.invert_yaxis()
        ax2.set_xlabel('Average IMDb Rating', fontsize=12, fontweight='bold')
        ax2.set_title('Top 15 Composers by Quality', fontsize=14, fontweight='bold')
        ax2.set_xlim(0, 10)
        ax2.grid(axis='x', alpha=0.3, linestyle='--')
        ax2.axvline(x=self.composer_data['rating'].mean(),
                   color='red', linestyle='--', linewidth=2, label='Collection Average')
        ax2.legend()

        # Add value labels
        for i, value in enumerate(top_composers.values):
            ax2.text(value + 0.1, i, f'{value:.2f}',
                    va='center', fontsize=8, fontweight='bold')

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / '09_crew_quality_impact.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 09_crew_quality_impact.png")

    def viz_10_coverage_overview(self):
        """Visualization 10: Behind-the-Camera Coverage Overview."""

        logger.info("Creating Visualization 10: Coverage Overview")

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Behind the Camera: Coverage & Statistics',
                     fontsize=20, fontweight='bold', y=0.98)

        # 1. Coverage pie chart
        coverage_data = [
            self.stats['films_with_cinematographers'],
            self.stats['films_with_composers'],
            self.stats['total_films'] - self.stats['films_with_cinematographers'],
            self.stats['total_films'] - self.stats['films_with_composers']
        ]
        coverage_labels = [
            f"With Cinematographer\n({self.stats['cinematographer_coverage']:.1f}%)",
            f"With Composer\n({self.stats['composer_coverage']:.1f}%)",
            f"Without Cinematographer\n({100-self.stats['cinematographer_coverage']:.1f}%)",
            f"Without Composer\n({100-self.stats['composer_coverage']:.1f}%)"
        ]
        colors = [COLORS['accent'], COLORS['success'], '#CCCCCC', '#CCCCCC']

        wedges, texts, autotexts = ax1.pie([coverage_data[0], coverage_data[2]],
                                           labels=[coverage_labels[0], coverage_labels[2]],
                                           colors=[colors[0], colors[2]],
                                           autopct='%1.1f%%', startangle=90)
        ax1.set_title('Cinematographer Coverage', fontsize=14, fontweight='bold')

        wedges2, texts2, autotexts2 = ax2.pie([coverage_data[1], coverage_data[3]],
                                              labels=[coverage_labels[1], coverage_labels[3]],
                                              colors=[colors[1], colors[3]],
                                              autopct='%1.1f%%', startangle=90)
        ax2.set_title('Composer Coverage', fontsize=14, fontweight='bold')

        # 3. Unique counts
        unique_data = [
            self.stats['unique_cinematographers'],
            self.stats['unique_composers']
        ]
        unique_labels = ['Unique\nCinematographers', 'Unique\nComposers']

        bars = ax3.bar(unique_labels, unique_data, color=[COLORS['accent'], COLORS['success']])
        ax3.set_ylabel('Count', fontsize=12, fontweight='bold')
        ax3.set_title('Unique Crew Members', fontsize=14, fontweight='bold')
        ax3.grid(axis='y', alpha=0.3, linestyle='--')

        for bar, value in zip(bars, unique_data):
            height = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(value)}', ha='center', va='bottom',
                    fontsize=14, fontweight='bold')

        # 4. Summary stats text
        ax4.axis('off')
        summary_text = f"""
        BEHIND THE CAMERA STATISTICS

        Total Films Analyzed: {self.stats['total_films']:,}

        CINEMATOGRAPHERS
        • Films with Data: {self.stats['films_with_cinematographers']:,} ({self.stats['cinematographer_coverage']:.1f}%)
        • Unique Cinematographers: {self.stats['unique_cinematographers']:,}
        • Avg Films per Cinematographer: {self.stats['films_with_cinematographers']/max(self.stats['unique_cinematographers'],1):.1f}
        • Avg IMDb Rating: {self.cinematographer_data['rating'].mean():.2f}

        COMPOSERS
        • Films with Data: {self.stats['films_with_composers']:,} ({self.stats['composer_coverage']:.1f}%)
        • Unique Composers: {self.stats['unique_composers']:,}
        • Avg Films per Composer: {self.stats['films_with_composers']/max(self.stats['unique_composers'],1):.1f}
        • Avg IMDb Rating: {self.composer_data['rating'].mean():.2f}

        TOP CINEMATOGRAPHER: {self.cinematographer_data['name'].value_counts().index[0]}
        ({self.cinematographer_data['name'].value_counts().values[0]} films)

        TOP COMPOSER: {self.composer_data['name'].value_counts().index[0]}
        ({self.composer_data['name'].value_counts().values[0]} films)
        """

        ax4.text(0.1, 0.9, summary_text, transform=ax4.transAxes,
                fontsize=11, verticalalignment='top', fontfamily='monospace',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / '10_coverage_overview.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 10_coverage_overview.png")

    def generate_report(self, top_cinematographers, top_composers, director_cinematographer_collabs, director_composer_collabs, famous_trios):
        """Generate comprehensive text report."""

        logger.info("Generating comprehensive report...")

        report = []
        report.append("="*80)
        report.append("CINESCOPE BATCH 11: BEHIND THE CAMERA ANALYSIS")
        report.append("="*80)
        report.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

        report.append("\n" + "="*80)
        report.append("OVERVIEW STATISTICS")
        report.append("="*80)
        report.append(f"\nTotal Films in Collection: {self.stats['total_films']:,}")
        report.append(f"\nCinematographer Coverage: {self.stats['cinematographer_coverage']:.1f}%")
        report.append(f"  • Films with Cinematographer Data: {self.stats['films_with_cinematographers']:,}")
        report.append(f"  • Unique Cinematographers: {self.stats['unique_cinematographers']:,}")
        report.append(f"  • Average Films per Cinematographer: {self.stats['films_with_cinematographers']/max(self.stats['unique_cinematographers'],1):.1f}")

        report.append(f"\nComposer Coverage: {self.stats['composer_coverage']:.1f}%")
        report.append(f"  • Films with Composer Data: {self.stats['films_with_composers']:,}")
        report.append(f"  • Unique Composers: {self.stats['unique_composers']:,}")
        report.append(f"  • Average Films per Composer: {self.stats['films_with_composers']/max(self.stats['unique_composers'],1):.1f}")

        report.append("\n" + "="*80)
        report.append("TOP 10 CINEMATOGRAPHERS")
        report.append("="*80)
        report.append(f"\n{'Rank':<6}{'Name':<40}{'Films':<8}{'Avg Rating':<12}")
        report.append("-"*80)
        for i, row in top_cinematographers.iterrows():
            report.append(f"{i+1:<6}{row['name']:<40}{int(row['film_count']):<8}{row['avg_rating']:.2f}")

        report.append("\n" + "="*80)
        report.append("TOP 10 COMPOSERS")
        report.append("="*80)
        report.append(f"\n{'Rank':<6}{'Name':<40}{'Films':<8}{'Avg Rating':<12}")
        report.append("-"*80)
        for i, row in top_composers.iterrows():
            report.append(f"{i+1:<6}{row['name']:<40}{int(row['film_count']):<8}{row['avg_rating']:.2f}")

        report.append("\n" + "="*80)
        report.append("TOP DIRECTOR-CINEMATOGRAPHER COLLABORATIONS")
        report.append("="*80)
        report.append(f"\n{'Director':<30}{'Cinematographer':<30}{'Films':<8}")
        report.append("-"*80)
        for _, row in director_cinematographer_collabs.iterrows():
            report.append(f"{row['director']:<30}{row['cinematographer']:<30}{int(row['collaborations']):<8}")

        report.append("\n" + "="*80)
        report.append("TOP DIRECTOR-COMPOSER COLLABORATIONS")
        report.append("="*80)
        report.append(f"\n{'Director':<30}{'Composer':<30}{'Films':<8}")
        report.append("-"*80)
        for _, row in director_composer_collabs.iterrows():
            report.append(f"{row['director']:<30}{row['composer']:<30}{int(row['collaborations']):<8}")

        report.append("\n" + "="*80)
        report.append("FAMOUS DIRECTOR-CINEMATOGRAPHER-COMPOSER TRIOS")
        report.append("="*80)
        for _, row in famous_trios.iterrows():
            report.append(f"\n{row['director']}")
            report.append(f"  DP: {row['name_cinematographer']}")
            report.append(f"  Music: {row['name_composer']}")
            report.append(f"  Collaborations: {int(row['count'])}")

        report.append("\n" + "="*80)
        report.append("END OF REPORT")
        report.append("="*80)

        # Write to file
        with open(REPORT_FILE, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))

        logger.info(f"✓ Report saved: {REPORT_FILE}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function."""

    logger.info("="*80)
    logger.info("CINESCOPE BATCH 11: BEHIND THE CAMERA ANALYSIS")
    logger.info("="*80)
    logger.info("")

    # Load data
    logger.info("Loading data...")
    data_file = DATA_DIR / "watched_movies_master.csv"

    if not data_file.exists():
        logger.error(f"Data file not found: {data_file}")
        logger.error("Please run enrichment scripts first!")
        return

    df = pd.read_csv(data_file)
    logger.info(f"✓ Loaded {len(df):,} films")

    # Initialize analyzer
    analyzer = BehindTheCameraAnalyzer(df)

    # Generate visualizations
    logger.info("\n" + "="*80)
    logger.info("GENERATING VISUALIZATIONS")
    logger.info("="*80 + "\n")

    top_cinematographers = analyzer.viz_01_top_cinematographers()
    top_composers = analyzer.viz_02_top_composers()
    director_cinematographer_collabs = analyzer.viz_03_director_cinematographer_collaborations()
    director_composer_collabs = analyzer.viz_04_director_composer_collaborations()
    analyzer.viz_05_cinematographer_genre_preferences()
    analyzer.viz_06_composer_genre_preferences()
    analyzer.viz_07_crew_by_decade()
    famous_trios = analyzer.viz_08_famous_collaborations()
    analyzer.viz_09_crew_quality_impact()
    analyzer.viz_10_coverage_overview()

    # Generate report
    logger.info("\n" + "="*80)
    logger.info("GENERATING REPORT")
    logger.info("="*80 + "\n")

    analyzer.generate_report(
        top_cinematographers,
        top_composers,
        director_cinematographer_collabs,
        director_composer_collabs,
        famous_trios
    )

    # Summary
    logger.info("\n" + "="*80)
    logger.info("BATCH 11 COMPLETE!")
    logger.info("="*80)
    logger.info(f"\n✓ Generated 10 visualizations in: {OUTPUT_DIR}")
    logger.info(f"✓ Generated report in: {REPORT_FILE}")
    logger.info(f"\nKey Findings:")
    logger.info(f"  • {analyzer.stats['unique_cinematographers']:,} unique cinematographers")
    logger.info(f"  • {analyzer.stats['unique_composers']:,} unique composers")
    logger.info(f"  • {analyzer.stats['cinematographer_coverage']:.1f}% cinematographer coverage")
    logger.info(f"  • {analyzer.stats['composer_coverage']:.1f}% composer coverage")
    logger.info("\nCheck the visualizations folder for all generated images!")
    logger.info("="*80)


if __name__ == "__main__":
    main()
