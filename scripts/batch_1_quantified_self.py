"""
CineScope - Batch 1: Quantified Self Analysis
=============================================
Comprehensive analysis of my watched movies with 8 professional visualizations.

Focus: Rating patterns, genre preferences, director favorites, discovery profile
All based on IMDB/TMDB ratings (public consensus), not personal ratings

Generates:
- 8 PNG files at 300 DPI
- 1 interactive HTML file (Rating vs Runtime with hover details)

Visualizations:
1. Rating Distribution (IMDB vs TMDB comparison)
2. Decade × Genre Heatmap (where my taste sits in film-history space)
3. Director Leaderboard (top 20 directors by count + quality)
4. Genre Distribution (breakdown of preferences)
5. Runtime Sweet Spot (preferred film lengths)
6. Interactive Rating vs Runtime (HTML with hover details)
7. Decade Distribution (which decades I watch most)
8. Popularity vs Rating (hidden gems vs popular hits)
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import sys
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import (
    WATCHED_ONLY_DATA, VISUALIZATIONS_DIR, RATING_COLORS, QUALITY_COLORS,
    DEFAULT_FIGSIZE, DEFAULT_DPI, save_figure, get_batch_output_dir,
    log_message, BATCH1_BINS
)
from src.core.helpers import (
    categorize_rating, categorize_runtime, get_decade, get_season,
    explode_genres
)


class QuantifiedSelfAnalysis:
    """Batch 1: Analyze personal viewing behavior and rating patterns."""
    
    def __init__(self):
        """Initialize the analysis."""
        self.df = None
        self.batch_dir = get_batch_output_dir(1)
        
    def load_data(self):
        """Load watched movies dataset."""
        log_message("=" * 80)
        log_message("BATCH 1: Quantified Self Analysis")
        log_message("Loading my watched movies...")
        log_message("=" * 80)
        
        self.df = pd.read_csv(WATCHED_ONLY_DATA)
        
        # Parse dates
        date_cols = ['date_rated', 'watched_created', 'watched_modified']
        for col in date_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
        
        log_message(f"\nLoaded: {len(self.df):,} watched films")
        log_message(f"Year range: {int(self.df['year'].min())} - {int(self.df['year'].max())}")
        log_message(f"IMDB avg rating: {self.df['imdb_rating'].mean():.2f}")
        
        return self
    
    def viz_1_rating_distribution(self):
        """Viz 1: Rating Distribution - IMDB vs TMDB comparison."""
        log_message("\n📊 Creating Visualization 1: Rating Distribution")
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))
        
        # Left: IMDB Rating Distribution
        imdb_ratings = self.df['imdb_rating'].dropna()
        
        ax1.hist(imdb_ratings, bins=20, alpha=0.7, color=RATING_COLORS['imdb'],
                edgecolor='black', linewidth=1.5)
        
        # Add mean line
        mean_imdb = imdb_ratings.mean()
        ax1.axvline(mean_imdb, color='red', linestyle='--', linewidth=2, 
                   label=f'Mean: {mean_imdb:.2f}')
        
        # Add median line
        median_imdb = imdb_ratings.median()
        ax1.axvline(median_imdb, color='orange', linestyle='--', linewidth=2,
                   label=f'Median: {median_imdb:.2f}')
        
        ax1.set_xlabel('IMDB Rating', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Number of Films', fontsize=14, fontweight='bold')
        ax1.set_title('IMDB Rating Distribution\nMy Watched Films', 
                     fontsize=16, fontweight='bold', pad=20)
        ax1.legend(fontsize=12)
        ax1.grid(True, alpha=0.3)
        
        # Right: TMDB Rating Distribution (if available)
        if 'tmdb_vote_average' in self.df.columns:
            tmdb_ratings = self.df['tmdb_vote_average'].dropna()
            
            if len(tmdb_ratings) > 0:
                ax2.hist(tmdb_ratings, bins=20, alpha=0.7, color=RATING_COLORS['tmdb'],
                        edgecolor='black', linewidth=1.5)
                
                mean_tmdb = tmdb_ratings.mean()
                ax2.axvline(mean_tmdb, color='red', linestyle='--', linewidth=2,
                           label=f'Mean: {mean_tmdb:.2f}')
                
                median_tmdb = tmdb_ratings.median()
                ax2.axvline(median_tmdb, color='orange', linestyle='--', linewidth=2,
                           label=f'Median: {median_tmdb:.2f}')
                
                ax2.set_xlabel('TMDB Rating', fontsize=14, fontweight='bold')
                ax2.set_ylabel('Number of Films', fontsize=14, fontweight='bold')
                ax2.set_title('TMDB Rating Distribution\nMy Watched Films',
                             fontsize=16, fontweight='bold', pad=20)
                ax2.legend(fontsize=12)
                ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        filepath = save_figure(fig, '01_rating_distribution.png', batch_number=1)
        plt.close()
        
        return self
    
    def viz_2_decade_genre_heatmap(self):
        """Viz 2: Decade × Genre Heatmap - where my taste sits in film-history space."""
        log_message("📊 Creating Visualization 2: Decade × Genre Heatmap")
        
        # Get top 10 genres
        genre_exploded = explode_genres(self.df)
        top_genres = genre_exploded['genre'].value_counts().head(10).index.tolist()
        
        # Filter to films with decade and top genres
        df_filtered = genre_exploded[
            (genre_exploded['decade'].notna()) & 
            (genre_exploded['genre'].isin(top_genres))
        ].copy()
        
        # Create pivot table
        heatmap_data = df_filtered.groupby(['decade', 'genre']).size().unstack(fill_value=0)
        
        # Sort by decade
        heatmap_data = heatmap_data.sort_index()
        
        # Reorder columns by total count
        col_order = heatmap_data.sum().sort_values(ascending=False).index
        heatmap_data = heatmap_data[col_order]
        
        fig, ax = plt.subplots(figsize=(16, 10))
        
        sns.heatmap(heatmap_data, annot=True, fmt='d', cmap='YlOrRd',
                   cbar_kws={'label': 'Number of Films'},
                   linewidths=1, linecolor='white',
                   ax=ax)
        
        # Format decade labels
        yticklabels = [f"{int(d)}s" if not pd.isna(d) else '' for d in heatmap_data.index]
        ax.set_yticklabels(yticklabels, rotation=0)
        
        ax.set_title('Decade × Genre Heatmap\nWhere My Taste Sits in Film-History Space',
                    fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Genre', fontsize=14, fontweight='bold')
        ax.set_ylabel('Decade', fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        filepath = save_figure(fig, '02_decade_genre_heatmap.png', batch_number=1)
        plt.close()
        
        return self
    
    def viz_3_director_leaderboard(self):
        log_message("📊 Creating Visualization 3: Director Leaderboard")

        # Use normalized parsing → list[str] of director names
        from src.core.helpers import parse_directors
        df = self.df.copy()

        # Build directors list and explode
        df["directors_list"] = df["directors"].apply(lambda x: parse_directors(x) if pd.notna(x) else [])
        if df["directors_list"].map(len).sum() == 0:
            # Graceful fallback if we truly have no director data
            fig, ax = plt.subplots(figsize=(16, 10))
            ax.text(0.5, 0.5, "No director names available.", ha="center", va="center", fontsize=16)
            ax.axis("off")
            ax.set_title("Most Watched Directors")
            save_figure(fig, '03_director_leaderboard.png', batch_number=1)
            plt.close()
            return self

        expl = df[["const", "imdb_rating", "directors_list"]].explode("directors_list").dropna(subset=["directors_list"])
        expl["director"] = (expl["directors_list"]
                            .astype(str)
                            .str.replace(r"\s*\|\s*nm\d{7,}\b", "", regex=True)  # strip " | nm########"
                            .str.replace(r"\bnm\d{7,}\b", "", regex=True)       # strip stray IDs
                            .str.replace(r"\s+", " ", regex=True)
                            .str.strip())

        # Aggregate by UNIQUE films per director
        agg = (expl.groupby("director")
                    .agg(Films=("const", "nunique"),
                        Avg_Rating=("imdb_rating", "mean"))
                    .reset_index())

        # Remove empties and duplicates after cleaning
        agg = agg[agg["director"].astype(str).str.len() > 0]

        # Sort and take top 20 by film count (break ties by rating)
        top = agg.sort_values(["Films", "Avg_Rating"], ascending=[False, False]).head(20)

        # Plot — names only, no IDs
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(22, 10))
        ax1.barh(top["director"][::-1], top["Films"][::-1], color="#8cd3c3", edgecolor="black")
        for y, v in enumerate(top["Films"][::-1]):
            ax1.text(v + 0.3, y, f"{int(v)}", va="center", fontsize=10)
        ax1.set_title("Most Watched Directors\nMy Top 20 by Film Count", fontweight="bold")
        ax1.set_xlabel("Number of Films")

        # Quality panel: keep same directors, order by rating descending for readability
        top_q = top.sort_values("Avg_Rating", ascending=False)
        ax2.barh(top_q["director"][::-1], top_q["Avg_Rating"][::-1], color="#f6a5a0", edgecolor="black")
        for y, r in enumerate(top_q["Avg_Rating"][::-1]):
            ax2.text(r + 0.05, y, f"{r:.2f}", va="center", fontsize=10)
        ax2.set_title("Director Quality\nAverage Rating (Min 3 Films)", fontweight="bold")
        ax2.set_xlabel("Average IMDB Rating")

        plt.tight_layout()
        save_figure(fig, '03_director_leaderboard.png', batch_number=1)
        plt.close()
        return self

    
    def viz_4_genre_distribution(self):
        """Viz 4: Genre Distribution - breakdown of my genre preferences."""
        log_message("📊 Creating Visualization 4: Genre Distribution")
        
        # Explode genres
        genre_exploded = explode_genres(self.df)
        genre_counts = genre_exploded['genre'].value_counts().head(15)
        
        # Calculate percentages
        total_genre_instances = genre_counts.sum()
        genre_pct = (genre_counts / total_genre_instances * 100).round(1)
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 10))
        
        # Left: Bar chart with counts
        bars = ax1.barh(range(len(genre_counts)), genre_counts.values,
                       edgecolor='black', linewidth=1.5, alpha=0.8)
        
        # Color bars by genre using genre colors from config
        from src.core.config import GENRE_COLORS
        for i, (bar, genre) in enumerate(zip(bars, genre_counts.index)):
            color = GENRE_COLORS.get(genre, '#95a5a6')
            bar.set_color(color)
        
        ax1.set_yticks(range(len(genre_counts)))
        ax1.set_yticklabels(genre_counts.index)
        ax1.invert_yaxis()
        ax1.set_xlabel('Number of Films', fontsize=14, fontweight='bold')
        ax1.set_title('Top 15 Genres by Film Count\nMy Genre Preferences',
                     fontsize=16, fontweight='bold', pad=20)
        ax1.grid(True, alpha=0.3, axis='x')
        
        # Add count and percentage labels
        for i, (count, pct) in enumerate(zip(genre_counts.values, genre_pct.values)):
            ax1.text(count, i, f'  {int(count)} ({pct}%)', 
                    va='center', fontsize=10, fontweight='bold')
        
        # Right: Pie chart for top 8
        top_8 = genre_counts.head(8)
        other = genre_counts[8:].sum()
        
        pie_data = list(top_8.values) + [other]
        pie_labels = list(top_8.index) + ['Other']
        
        colors_pie = [GENRE_COLORS.get(g, '#95a5a6') for g in top_8.index] + ['#bdc3c7']
        
        wedges, texts, autotexts = ax2.pie(pie_data, labels=pie_labels, autopct='%1.1f%%',
                                            colors=colors_pie, startangle=90,
                                            textprops={'fontsize': 11, 'fontweight': 'bold'})
        
        # Make percentage text white
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontsize(10)
        
        ax2.set_title('Genre Distribution (Top 8)\nProportional View',
                     fontsize=16, fontweight='bold', pad=20)
        
        plt.tight_layout()
        filepath = save_figure(fig, '04_genre_distribution.png', batch_number=1)
        plt.close()
        
        return self
    
    def viz_5_runtime_sweet_spot(self):
        """Viz 5: Runtime Sweet Spot - preferred film lengths."""
        log_message("📊 Creating Visualization 5: Runtime Sweet Spot")
        
        df_runtime = self.df[self.df['runtime_mins'].notna()].copy()
        
        # Create runtime bins
        bins = [0, 90, 120, 150, 180, 999]
        labels = ['<90 min', '90-120 min', '120-150 min', '150-180 min', '>180 min']
        df_runtime['runtime_bin'] = pd.cut(df_runtime['runtime_mins'], bins=bins, labels=labels)
        
        # Count and average rating by bin
        runtime_stats = df_runtime.groupby('runtime_bin').agg({
            'const': 'count',
            'imdb_rating': 'mean'
        }).reset_index()
        runtime_stats.columns = ['Runtime', 'Count', 'Avg_Rating']
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))
        
        # Left: Count by runtime
        ax1.bar(runtime_stats['Runtime'], runtime_stats['Count'],
               color=RATING_COLORS['imdb'], edgecolor='black', linewidth=1.5, alpha=0.7)
        ax1.set_xlabel('Runtime', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Number of Films', fontsize=14, fontweight='bold')
        ax1.set_title('Films by Runtime Length\nMy Viewing Distribution',
                     fontsize=16, fontweight='bold', pad=20)
        ax1.grid(True, alpha=0.3, axis='y')
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # Right: Average rating by runtime
        ax2.plot(runtime_stats['Runtime'], runtime_stats['Avg_Rating'],
                marker='o', markersize=12, linewidth=3, color=RATING_COLORS['critics'])
        ax2.set_xlabel('Runtime', fontsize=14, fontweight='bold')
        ax2.set_ylabel('Average IMDB Rating', fontsize=14, fontweight='bold')
        ax2.set_title('Rating by Runtime Length\nDoes Length Affect Quality?',
                     fontsize=16, fontweight='bold', pad=20)
        ax2.grid(True, alpha=0.3)
        ax2.set_ylim([5, 8])
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        plt.tight_layout()
        filepath = save_figure(fig, '05_runtime_sweet_spot.png', batch_number=1)
        plt.close()
        
        return self
    
    def viz_6_interactive_rating_vs_runtime(self):
        """Viz 6: Interactive Rating vs Runtime - hover to see movie details (HTML)."""
        log_message("📊 Creating Visualization 6: Interactive Rating vs Runtime (HTML)")
        
        df_scatter = self.df[
            (self.df['runtime_mins'].notna()) & 
            (self.df['imdb_rating'].notna())
        ].copy()
        
        # Create static version first
        fig, ax = plt.subplots(figsize=(14, 10))
        
        scatter = ax.scatter(df_scatter['runtime_mins'], 
                           df_scatter['imdb_rating'],
                           c=df_scatter['decade'], 
                           cmap='viridis',
                           alpha=0.6, 
                           s=50,
                           edgecolors='black',
                           linewidth=0.5)
        
        cbar = plt.colorbar(scatter, ax=ax)
        cbar.set_label('Decade', fontsize=12, fontweight='bold')
        
        # Add trend line
        z = np.polyfit(df_scatter['runtime_mins'], df_scatter['imdb_rating'], 1)
        p = np.poly1d(z)
        ax.plot(df_scatter['runtime_mins'], p(df_scatter['runtime_mins']),
               "r--", linewidth=2, label=f'Trend: y={z[0]:.4f}x+{z[1]:.2f}')
        
        ax.set_xlabel('Runtime (minutes)', fontsize=14, fontweight='bold')
        ax.set_ylabel('IMDB Rating', fontsize=14, fontweight='bold')
        ax.set_title('Runtime vs Rating\n(See HTML version for interactive hover)',
                    fontsize=16, fontweight='bold', pad=20)
        ax.legend(fontsize=12)
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        filepath = save_figure(fig, '06_rating_vs_runtime_scatter.png', batch_number=1)
        plt.close()
        
        # Create interactive HTML version
        log_message("  Creating interactive HTML version...")
        
        html_content = """
<!DOCTYPE html>
<html>
<head>
    <title>Interactive Rating vs Runtime</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {
            font-family: 'DejaVu Sans', Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }
        h1 {
            text-align: center;
            color: #2c3e50;
        }
        #plotDiv {
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            padding: 20px;
        }
    </style>
</head>
<body>
    <h1>🎬 Interactive Rating vs Runtime Analysis</h1>
    <p style="text-align: center; color: #666;">Hover over points to see movie details</p>
    <div id="plotDiv"></div>
    <script>
"""
        
        # Prepare data for plotly
        hover_texts = []
        for idx, row in df_scatter.iterrows():
            text = f"<b>{row['display_title']}</b><br>"
            text += f"Year: {int(row['year']) if not pd.isna(row['year']) else 'N/A'}<br>"
            text += f"Runtime: {int(row['runtime_mins'])} min<br>"
            text += f"IMDB Rating: {row['imdb_rating']:.1f}<br>"
            if pd.notna(row.get('directors')):
                text += f"Director: {str(row['directors'])[:50]}<br>"
            if pd.notna(row.get('genres')):
                text += f"Genres: {str(row['genres'])[:60]}"
            hover_texts.append(text)
        
        # Create JavaScript data
        html_content += f"""
        var data = [{{
            x: {df_scatter['runtime_mins'].tolist()},
            y: {df_scatter['imdb_rating'].tolist()},
            mode: 'markers',
            type: 'scatter',
            text: {hover_texts},
            hovertemplate: '%{{text}}<extra></extra>',
            marker: {{
                size: 8,
                color: {df_scatter['decade'].fillna(2000).tolist()},
                colorscale: 'Viridis',
                showscale: true,
                colorbar: {{
                    title: 'Decade'
                }},
                line: {{
                    color: 'black',
                    width: 0.5
                }}
            }}
        }}];
        
        var layout = {{
            title: {{
                text: 'Rating vs Runtime: Hover to Discover Films',
                font: {{ size: 20, family: 'DejaVu Sans, Arial' }}
            }},
            xaxis: {{
                title: 'Runtime (minutes)',
                gridcolor: '#e0e0e0'
            }},
            yaxis: {{
                title: 'IMDB Rating',
                gridcolor: '#e0e0e0'
            }},
            hovermode: 'closest',
            plot_bgcolor: '#fafafa',
            paper_bgcolor: 'white'
        }};
        
        var config = {{
            responsive: true,
            displayModeBar: true,
            displaylogo: false
        }};
        
        Plotly.newPlot('plotDiv', data, layout, config);
    </script>
</body>
</html>
"""
        
        # Save HTML file
        html_path = self.batch_dir / '06_rating_vs_runtime_interactive.html'
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        log_message(f"  ✅ Interactive HTML saved: {html_path.name}")
        
        return self
    
    def viz_7_decade_distribution(self):
        """Viz 7: Decade Distribution - which decades I watch most."""
        log_message("📊 Creating Visualization 7: Decade Distribution")
        
        df_decades = self.df[self.df['decade'].notna()].copy()
        
        decade_stats = df_decades.groupby('decade').agg({
            'const': 'count',
            'imdb_rating': 'mean'
        }).reset_index()
        decade_stats.columns = ['Decade', 'Count', 'Avg_Rating']
        decade_stats['Decade'] = decade_stats['Decade'].astype(int)
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12))
        
        # Top: Count by decade
        bars = ax1.bar(decade_stats['Decade'], decade_stats['Count'],
                      width=8, edgecolor='black', linewidth=1.5, alpha=0.7)
        
        # Color bars by count (gradient)
        colors = plt.cm.viridis(decade_stats['Count'] / decade_stats['Count'].max())
        for bar, color in zip(bars, colors):
            bar.set_color(color)
        
        ax1.set_xlabel('Decade', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Number of Films', fontsize=14, fontweight='bold')
        ax1.set_title('Films Watched by Decade\nMy Cinema Timeline',
                     fontsize=16, fontweight='bold', pad=20)
        ax1.grid(True, alpha=0.3, axis='y')
        
        # Add value labels on bars
        for decade, count in zip(decade_stats['Decade'], decade_stats['Count']):
            ax1.text(decade, count, str(count), ha='center', va='bottom', fontsize=10, fontweight='bold')
        
        # Bottom: Average rating by decade
        ax2.plot(decade_stats['Decade'], decade_stats['Avg_Rating'],
                marker='o', markersize=10, linewidth=3, color=RATING_COLORS['critics'])
        ax2.fill_between(decade_stats['Decade'], decade_stats['Avg_Rating'],
                        alpha=0.3, color=RATING_COLORS['critics'])
        
        ax2.set_xlabel('Decade', fontsize=14, fontweight='bold')
        ax2.set_ylabel('Average IMDB Rating', fontsize=14, fontweight='bold')
        ax2.set_title('Average Rating by Decade\nQuality Across Time',
                     fontsize=16, fontweight='bold', pad=20)
        ax2.grid(True, alpha=0.3)
        ax2.set_ylim([5, 8])
        
        plt.tight_layout()
        filepath = save_figure(fig, '07_decade_distribution.png', batch_number=1)
        plt.close()
        
        return self
    
    def viz_8_popularity_vs_rating(self):
        """Viz 8: Popularity vs Rating - am I drawn to hits or hidden gems?"""
        log_message("📊 Creating Visualization 8: Popularity vs Rating (Hidden Gems)")
        
        # Use num_votes as popularity metric
        df_pop = self.df[
            (self.df['num_votes'].notna()) & 
            (self.df['imdb_rating'].notna())
        ].copy()
        
        # Define quadrants
        median_votes = df_pop['num_votes'].median()
        median_rating = df_pop['imdb_rating'].median()
        
        # Categorize films
        def categorize_film(row):
            if row['num_votes'] > median_votes and row['imdb_rating'] > median_rating:
                return 'Popular Hits'
            elif row['num_votes'] <= median_votes and row['imdb_rating'] > median_rating:
                return 'Hidden Gems'
            elif row['num_votes'] > median_votes and row['imdb_rating'] <= median_rating:
                return 'Popular but Mediocre'
            else:
                return 'Obscure & Low-Rated'
        
        df_pop['category'] = df_pop.apply(categorize_film, axis=1)
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 10))
        
        # Left: Scatter plot
        colors = {
            'Popular Hits': '#27ae60',
            'Hidden Gems': '#f39c12',
            'Popular but Mediocre': '#e74c3c',
            'Obscure & Low-Rated': '#95a5a6'
        }
        
        for category in colors.keys():
            df_cat = df_pop[df_pop['category'] == category]
            ax1.scatter(df_cat['num_votes'], df_cat['imdb_rating'],
                       label=category, alpha=0.6, s=50, c=colors[category],
                       edgecolors='black', linewidth=0.5)
        
        # Add median lines
        ax1.axvline(median_votes, color='red', linestyle='--', linewidth=2, alpha=0.5)
        ax1.axhline(median_rating, color='red', linestyle='--', linewidth=2, alpha=0.5)
        
        ax1.set_xscale('log')
        ax1.set_xlabel('Number of Votes (Popularity)', fontsize=14, fontweight='bold')
        ax1.set_ylabel('IMDB Rating', fontsize=14, fontweight='bold')
        ax1.set_title('Popularity vs Rating\nAm I Drawn to Hits or Hidden Gems?',
                     fontsize=16, fontweight='bold', pad=20)
        ax1.legend(fontsize=11, loc='best')
        ax1.grid(True, alpha=0.3)
        
        # Right: Category breakdown
        category_counts = df_pop['category'].value_counts()
        category_pcts = (category_counts / len(df_pop) * 100).round(1)
        
        bars = ax2.barh(range(len(category_counts)), category_counts.values,
                       color=[colors[cat] for cat in category_counts.index],
                       edgecolor='black', linewidth=1.5, alpha=0.7)
        
        ax2.set_yticks(range(len(category_counts)))
        ax2.set_yticklabels(category_counts.index)
        ax2.invert_yaxis()
        ax2.set_xlabel('Number of Films', fontsize=14, fontweight='bold')
        ax2.set_title('My Film Discovery Profile\nWhat Type of Films Do I Watch?',
                     fontsize=16, fontweight='bold', pad=20)
        ax2.grid(True, alpha=0.3, axis='x')
        
        # Add labels
        for i, (count, pct) in enumerate(zip(category_counts.values, category_pcts.values)):
            ax2.text(count, i, f'  {int(count)} ({pct}%)', 
                    va='center', fontsize=11, fontweight='bold')
        
        plt.tight_layout()
        filepath = save_figure(fig, '08_popularity_vs_rating.png', batch_number=1)
        plt.close()
        
        return self
    
    def generate_summary_report(self):
        """Generate text summary of findings."""
        log_message("\n" + "=" * 80)
        log_message("BATCH 1 ANALYSIS SUMMARY")
        log_message("=" * 80)
        
        log_message(f"\nTotal Films Analyzed: {len(self.df):,}")
        log_message(f"Year Range: {int(self.df['year'].min())} - {int(self.df['year'].max())}")
        log_message(f"Total Watch Time: {self.df['runtime_mins'].sum()/60:.0f} hours")
        
        log_message(f"\nRating Statistics:")
        log_message(f"  IMDB Avg: {self.df['imdb_rating'].mean():.2f}")
        log_message(f"  IMDB Median: {self.df['imdb_rating'].median():.2f}")
        
        if 'tmdb_vote_average' in self.df.columns:
            tmdb_avg = self.df['tmdb_vote_average'].mean()
            if not pd.isna(tmdb_avg):
                log_message(f"  TMDB Avg: {tmdb_avg:.2f}")
        
        log_message(f"\nTop 5 Decades:")
        top_decades = self.df['decade'].value_counts().head(5)
        for decade, count in top_decades.items():
            if not pd.isna(decade):
                log_message(f"  {int(decade)}s: {count} films")
        
        log_message(f"\nTop 5 Genres:")
        genre_exploded = explode_genres(self.df)
        top_genres = genre_exploded['genre'].value_counts().head(5)
        for genre, count in top_genres.items():
            log_message(f"  {genre}: {count} films")
        
        log_message("\n✅ Batch 1 Analysis Complete!")
        
        return self


def main():
    """Main execution."""
    print("\n" + "🎨" * 40)
    print("\n" + " " * 15 + "BATCH 1: QUANTIFIED SELF ANALYSIS")
    print(" " * 10 + "Analyzing my Watched Movies with 8 Visualizations")
    print("\n" + "🎨" * 40 + "\n")
    
    try:
        analysis = QuantifiedSelfAnalysis()
        analysis.load_data()
        
        # Generate all 8 visualizations
        analysis.viz_1_rating_distribution()
        analysis.viz_2_decade_genre_heatmap()
        analysis.viz_3_director_leaderboard()
        analysis.viz_4_genre_distribution()
        analysis.viz_5_runtime_sweet_spot()
        analysis.viz_6_interactive_rating_vs_runtime()
        analysis.viz_7_decade_distribution()
        analysis.viz_8_popularity_vs_rating()
        
        # Generate summary
        analysis.generate_summary_report()
        
        print("\n" + "✨" * 40)
        print("\n" + " " * 8 + "BATCH 1 COMPLETE - 8 VISUALIZATIONS CREATED!")
        print(" " * 10 + "(8 PNG files + 1 interactive HTML)")
        print(" " * 12 + "Check analysis_outputs/visualizations/batch_1/")
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