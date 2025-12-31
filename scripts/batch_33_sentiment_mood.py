#!/usr/bin/env python3
"""
CineScope Batch 33: Sentiment & Mood Clustering
================================================

Analyzes emotional tone and mood from keywords, plot summaries, and themes.
Clusters films by emotional content and sentiment.

Author: CineScope Analytics
Date: 2025-12-31
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime
import ast
from collections import Counter, defaultdict
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
import warnings
warnings.filterwarnings('ignore')

class SentimentMoodAnalyzer:
    """Analyze emotional tone and mood patterns in films."""

    def __init__(self, data_path='data/processed/watched_movies_master.csv'):
        """Initialize analyzer with watched movies dataset."""
        self.data_path = Path(data_path)
        self.output_dir = Path('analysis_outputs/visualizations/batch_33')
        self.report_dir = Path('analysis_outputs/reports')

        # Create output directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)

        # Load data
        print("Loading watched movies data...")
        self.df = pd.read_csv(self.data_path)
        print(f"Loaded {len(self.df)} watched films")

        # Set up plotting
        plt.style.use('default')
        sns.set_palette("husl")

        # Define mood categories
        self._define_mood_categories()
        self._extract_emotional_features()

    def _define_mood_categories(self):
        """Define emotional and mood keyword categories."""
        self.mood_keywords = {
            'Dark': ['dark', 'noir', 'bleak', 'grim', 'somber', 'disturbing', 'twisted'],
            'Uplifting': ['uplifting', 'inspirational', 'heartwarming', 'feel-good', 'positive'],
            'Suspenseful': ['suspense', 'tension', 'thriller', 'mystery', 'intense'],
            'Romantic': ['romance', 'love', 'romantic', 'passion', 'relationship'],
            'Tragic': ['tragic', 'sad', 'melancholy', 'depressing', 'sorrow'],
            'Humorous': ['comedy', 'funny', 'humor', 'satire', 'parody', 'wit'],
            'Action-Packed': ['action', 'explosive', 'adrenaline', 'fast-paced'],
            'Contemplative': ['philosophical', 'meditative', 'introspective', 'thoughtful']
        }

    def _extract_emotional_features(self):
        """Extract emotional features from keywords and plot."""
        print("Extracting emotional features...")

        # Initialize mood scores
        for mood in self.mood_keywords.keys():
            self.df[f'mood_{mood.lower()}'] = 0

        # Process each film
        for idx, row in self.df.iterrows():
            # Combine text sources
            text_content = ''

            # Add plot
            if pd.notna(row.get('omdb_plot')):
                text_content += ' ' + str(row['omdb_plot']).lower()

            # Add keywords
            if pd.notna(row.get('keyword_emotional')):
                text_content += ' ' + str(row['keyword_emotional']).lower()

            if pd.notna(row.get('keyword_themes')):
                text_content += ' ' + str(row['keyword_themes']).lower()

            # Score each mood
            for mood, keywords in self.mood_keywords.items():
                score = sum(1 for kw in keywords if kw in text_content)
                self.df.at[idx, f'mood_{mood.lower()}'] = score

        # Calculate dominant mood
        mood_cols = [f'mood_{m.lower()}' for m in self.mood_keywords.keys()]
        self.df['dominant_mood'] = self.df[mood_cols].idxmax(axis=1).str.replace('mood_', '').str.title()

        # Films with no mood detected
        no_mood_mask = self.df[mood_cols].sum(axis=1) == 0
        self.df.loc[no_mood_mask, 'dominant_mood'] = 'Neutral'

    def visualize_mood_distribution(self):
        """Visualization 1-4: Mood distribution analysis."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Emotional Mood Analysis', fontsize=16, fontweight='bold')

        # 1. Dominant mood distribution
        ax1 = axes[0, 0]
        mood_counts = self.df['dominant_mood'].value_counts()

        colors = plt.cm.Set3(np.linspace(0, 1, len(mood_counts)))
        bars = ax1.bar(range(len(mood_counts)), mood_counts.values, color=colors, edgecolor='black')
        ax1.set_xticks(range(len(mood_counts)))
        ax1.set_xticklabels(mood_counts.index, rotation=45, ha='right', fontsize=9)
        ax1.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
        ax1.set_title('Dominant Mood Distribution', fontsize=12, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)

        # Annotate bars
        for bar, count in zip(bars, mood_counts.values):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'{count}\n({count/len(self.df)*100:.1f}%)',
                    ha='center', va='bottom', fontsize=8)

        # 2. Mood by rating
        ax2 = axes[0, 1]
        mood_ratings = []
        mood_labels = []

        for mood in mood_counts.index:
            if mood != 'Neutral':
                ratings = self.df[self.df['dominant_mood'] == mood]['IMDb Rating'].dropna()
                if len(ratings) > 0:
                    mood_ratings.append(ratings)
                    mood_labels.append(mood)

        if mood_ratings:
            bp = ax2.boxplot(mood_ratings, labels=mood_labels, patch_artist=True)
            for patch, color in zip(bp['boxes'], colors):
                patch.set_facecolor(color)

            ax2.set_xticklabels(mood_labels, rotation=45, ha='right', fontsize=9)
            ax2.set_ylabel('IMDb Rating', fontsize=11, fontweight='bold')
            ax2.set_title('Rating Distribution by Mood', fontsize=12, fontweight='bold')
            ax2.grid(axis='y', alpha=0.3)

        # 3. Mood over decades
        ax3 = axes[1, 0]
        self.df['decade'] = (self.df['year'] // 10) * 10
        decades = sorted(self.df['decade'].dropna().unique())

        # Top 5 moods
        top_moods = mood_counts.head(5).index.tolist()
        if 'Neutral' in top_moods:
            top_moods.remove('Neutral')
            top_moods = mood_counts.head(6).index.tolist()
            if 'Neutral' in top_moods:
                top_moods.remove('Neutral')

        for mood in top_moods[:5]:
            decade_counts = []
            for decade in decades:
                count = len(self.df[(self.df['decade'] == decade) &
                                   (self.df['dominant_mood'] == mood)])
                decade_counts.append(count)

            ax3.plot(decades, decade_counts, marker='o', linewidth=2, label=mood)

        ax3.set_xlabel('Decade', fontsize=11, fontweight='bold')
        ax3.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
        ax3.set_title('Mood Trends Over Time', fontsize=12, fontweight='bold')
        ax3.legend(fontsize=9)
        ax3.grid(True, alpha=0.3)

        # 4. Multi-mood films
        ax4 = axes[1, 1]
        mood_cols = [f'mood_{m.lower()}' for m in self.mood_keywords.keys()]
        self.df['mood_diversity'] = (self.df[mood_cols] > 0).sum(axis=1)

        diversity_counts = self.df['mood_diversity'].value_counts().sort_index()

        bars = ax4.bar(diversity_counts.index, diversity_counts.values,
                      color='#9B59B6', edgecolor='black')
        ax4.set_xlabel('Number of Mood Categories', fontsize=11, fontweight='bold')
        ax4.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
        ax4.set_title('Emotional Complexity (Multi-Mood Films)', fontsize=12, fontweight='bold')
        ax4.grid(axis='y', alpha=0.3)

        # Annotate
        for bar, count in zip(bars, diversity_counts.values):
            height = bar.get_height()
            ax4.text(bar.get_x() + bar.get_width()/2., height,
                    f'{count}', ha='center', va='bottom', fontsize=9)

        plt.tight_layout()
        output_path = self.output_dir / 'mood_distribution.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_mood_clustering(self):
        """Visualization 5-6: K-means clustering of emotional profiles."""
        fig, axes = plt.subplots(1, 2, figsize=(16, 8))
        fig.suptitle('Mood Clustering Analysis', fontsize=16, fontweight='bold')

        # Prepare features
        mood_cols = [f'mood_{m.lower()}' for m in self.mood_keywords.keys()]
        features = self.df[mood_cols].values

        # Remove zero vectors
        non_zero_mask = features.sum(axis=1) > 0
        features_clean = features[non_zero_mask]
        df_clean = self.df[non_zero_mask].copy()

        if len(features_clean) > 10:
            # K-means clustering
            n_clusters = 5
            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            clusters = kmeans.fit_predict(features_clean)

            # PCA for visualization
            pca = PCA(n_components=2, random_state=42)
            features_2d = pca.fit_transform(features_clean)

            # 1. Cluster scatter plot
            ax1 = axes[0]
            scatter = ax1.scatter(features_2d[:, 0], features_2d[:, 1],
                                c=clusters, cmap='tab10', alpha=0.6, s=50, edgecolors='black')
            ax1.set_xlabel(f'PC1 ({pca.explained_variance_ratio_[0]*100:.1f}%)',
                          fontsize=11, fontweight='bold')
            ax1.set_ylabel(f'PC2 ({pca.explained_variance_ratio_[1]*100:.1f}%)',
                          fontsize=11, fontweight='bold')
            ax1.set_title(f'Mood Clusters (K={n_clusters})', fontsize=12, fontweight='bold')
            ax1.grid(True, alpha=0.3)

            # Plot cluster centers
            centers_2d = pca.transform(kmeans.cluster_centers_)
            ax1.scatter(centers_2d[:, 0], centers_2d[:, 1],
                       c='red', marker='X', s=300, edgecolors='black', linewidths=2)

            # 2. Cluster characteristics
            ax2 = axes[1]
            ax2.axis('off')

            cluster_text = "CLUSTER CHARACTERISTICS\n" + "="*50 + "\n\n"

            for i in range(n_clusters):
                cluster_mask = clusters == i
                cluster_data = df_clean[cluster_mask]

                cluster_text += f"CLUSTER {i+1} ({cluster_mask.sum()} films):\n"
                cluster_text += f"Avg Rating: {cluster_data['IMDb Rating'].mean():.2f}\n"

                # Top mood in this cluster
                mood_scores = features_clean[cluster_mask].mean(axis=0)
                top_mood_idx = np.argmax(mood_scores)
                top_mood = list(self.mood_keywords.keys())[top_mood_idx]
                cluster_text += f"Primary Mood: {top_mood}\n"

                # Sample films
                sample_films = cluster_data.nlargest(2, 'IMDb Rating')['title'].values
                cluster_text += "Top Films:\n"
                for film in sample_films:
                    cluster_text += f"  - {str(film)[:35]}\n"
                cluster_text += "\n"

            ax2.text(0.1, 0.9, cluster_text, transform=ax2.transAxes,
                    fontsize=8, verticalalignment='top',
                    family='monospace',
                    bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.5))
            ax2.set_title('Cluster Profiles', fontsize=12, fontweight='bold')

        plt.tight_layout()
        output_path = self.output_dir / 'mood_clustering.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_mood_correlations(self):
        """Visualization 7-8: Mood correlations and co-occurrence."""
        fig, axes = plt.subplots(1, 2, figsize=(16, 8))
        fig.suptitle('Mood Relationships & Co-occurrence', fontsize=16, fontweight='bold')

        # 1. Mood correlation heatmap
        ax1 = axes[0]
        mood_cols = [f'mood_{m.lower()}' for m in self.mood_keywords.keys()]
        correlation = self.df[mood_cols].corr()

        # Rename for display
        labels = list(self.mood_keywords.keys())
        im = ax1.imshow(correlation, cmap='coolwarm', aspect='auto', vmin=-1, vmax=1)
        ax1.set_xticks(np.arange(len(labels)))
        ax1.set_yticks(np.arange(len(labels)))
        ax1.set_xticklabels(labels, rotation=45, ha='right', fontsize=9)
        ax1.set_yticklabels(labels, fontsize=9)
        ax1.set_title('Mood Correlation Matrix', fontsize=12, fontweight='bold')

        # Add colorbar
        cbar = plt.colorbar(im, ax=ax1)
        cbar.set_label('Correlation', fontsize=11, fontweight='bold')

        # Annotate cells
        for i in range(len(labels)):
            for j in range(len(labels)):
                text = ax1.text(j, i, f'{correlation.iloc[i, j]:.2f}',
                              ha="center", va="center", color="black", fontsize=7)

        # 2. Mood co-occurrence network
        ax2 = axes[1]
        ax2.axis('off')

        # Calculate co-occurrence
        cooccurrence = defaultdict(int)
        for idx, row in self.df.iterrows():
            present_moods = [m for m in self.mood_keywords.keys()
                           if row[f'mood_{m.lower()}'] > 0]
            for i, mood1 in enumerate(present_moods):
                for mood2 in present_moods[i+1:]:
                    pair = tuple(sorted([mood1, mood2]))
                    cooccurrence[pair] += 1

        # Show top pairs
        top_pairs = sorted(cooccurrence.items(), key=lambda x: x[1], reverse=True)[:10]

        cooccur_text = "MOOD CO-OCCURRENCE\n" + "="*50 + "\n\n"
        cooccur_text += "Top mood combinations in films:\n\n"

        for i, ((mood1, mood2), count) in enumerate(top_pairs, 1):
            pct = count / len(self.df) * 100
            cooccur_text += f"{i:2d}. {mood1} + {mood2}\n"
            cooccur_text += f"    {count} films ({pct:.1f}%)\n"

        ax2.text(0.1, 0.9, cooccur_text, transform=ax2.transAxes,
                fontsize=10, verticalalignment='top',
                family='monospace',
                bbox=dict(boxstyle='round', facecolor='lightcyan', alpha=0.5))
        ax2.set_title('Mood Combinations', fontsize=12, fontweight='bold')

        plt.tight_layout()
        output_path = self.output_dir / 'mood_correlations.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_mood_by_genre(self):
        """Visualization 9-10: Mood patterns by genre."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Mood Patterns by Genre', fontsize=16, fontweight='bold')

        # Get top genres
        all_genres = []
        for genres_str in self.df['Genres'].dropna():
            all_genres.extend([g.strip() for g in str(genres_str).split(',')])
        top_genres = [g for g, _ in Counter(all_genres).most_common(8)]

        # 1. Mood distribution by genre (stacked bar)
        ax1 = axes[0, 0]
        genre_mood_data = defaultdict(lambda: defaultdict(int))

        for idx, row in self.df.iterrows():
            if pd.isna(row['Genres']):
                continue
            film_genres = [g.strip() for g in str(row['Genres']).split(',')]
            dominant = row['dominant_mood']

            for genre in film_genres:
                if genre in top_genres:
                    genre_mood_data[genre][dominant] += 1

        # Prepare data for stacked bar
        genres_list = top_genres
        moods_list = list(self.mood_keywords.keys()) + ['Neutral']
        mood_colors = plt.cm.Set3(np.linspace(0, 1, len(moods_list)))

        bottom = np.zeros(len(genres_list))
        for i, mood in enumerate(moods_list):
            counts = [genre_mood_data[g][mood] for g in genres_list]
            ax1.bar(range(len(genres_list)), counts, bottom=bottom,
                   label=mood, color=mood_colors[i], edgecolor='white', linewidth=0.5)
            bottom += counts

        ax1.set_xticks(range(len(genres_list)))
        ax1.set_xticklabels(genres_list, rotation=45, ha='right', fontsize=9)
        ax1.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
        ax1.set_title('Mood Distribution by Genre (Stacked)', fontsize=12, fontweight='bold')
        ax1.legend(fontsize=8, loc='upper right')
        ax1.grid(axis='y', alpha=0.3)

        # 2. Average mood intensity by genre
        ax2 = axes[0, 1]
        mood_cols = [f'mood_{m.lower()}' for m in self.mood_keywords.keys()]

        # Calculate average mood scores per genre
        genre_avg_moods = {}
        for genre in top_genres:
            genre_films = []
            for idx, row in self.df.iterrows():
                if pd.notna(row['Genres']) and genre in str(row['Genres']):
                    genre_films.append(idx)

            if genre_films:
                avg_moods = self.df.loc[genre_films, mood_cols].mean()
                genre_avg_moods[genre] = avg_moods.sum()

        sorted_genres = sorted(genre_avg_moods.items(), key=lambda x: x[1], reverse=True)
        genres, intensities = zip(*sorted_genres)

        bars = ax2.barh(range(len(genres)), intensities, color='#E74C3C', edgecolor='black')
        ax2.set_yticks(range(len(genres)))
        ax2.set_yticklabels(genres, fontsize=9)
        ax2.set_xlabel('Average Mood Intensity', fontsize=11, fontweight='bold')
        ax2.set_title('Emotional Intensity by Genre', fontsize=12, fontweight='bold')
        ax2.invert_yaxis()
        ax2.grid(axis='x', alpha=0.3)

        # 3. Dominant mood by genre (pie charts)
        ax3 = axes[1, 0]
        # Pick one genre for detailed pie
        sample_genre = top_genres[0]
        genre_moods = genre_mood_data[sample_genre]

        labels = [m for m in moods_list if genre_moods[m] > 0]
        sizes = [genre_moods[m] for m in labels]
        colors = [mood_colors[moods_list.index(m)] for m in labels]

        ax3.pie(sizes, labels=labels, autopct='%1.1f%%', colors=colors,
               startangle=90, textprops={'fontsize': 9})
        ax3.set_title(f'Mood Breakdown: {sample_genre}', fontsize=12, fontweight='bold')

        # 4. Second genre pie
        ax4 = axes[1, 1]
        if len(top_genres) > 1:
            sample_genre2 = top_genres[1]
            genre_moods2 = genre_mood_data[sample_genre2]

            labels2 = [m for m in moods_list if genre_moods2[m] > 0]
            sizes2 = [genre_moods2[m] for m in labels2]
            colors2 = [mood_colors[moods_list.index(m)] for m in labels2]

            ax4.pie(sizes2, labels=labels2, autopct='%1.1f%%', colors=colors2,
                   startangle=90, textprops={'fontsize': 9})
            ax4.set_title(f'Mood Breakdown: {sample_genre2}', fontsize=12, fontweight='bold')

        plt.tight_layout()
        output_path = self.output_dir / 'mood_by_genre.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_mood_extremes(self):
        """Visualization 11-12: Mood extremes and pure mood films."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Mood Extremes & Pure Mood Films', fontsize=16, fontweight='bold')

        # 1. Most intense films per mood
        ax1 = axes[0, 0]
        ax1.axis('off')

        extreme_text = "MOST INTENSE FILMS PER MOOD\n" + "="*50 + "\n\n"

        for mood in list(self.mood_keywords.keys())[:4]:  # Top 4 moods
            col = f'mood_{mood.lower()}'
            top_film = self.df[self.df[col] > 0].nlargest(1, col)

            if len(top_film) > 0:
                film = top_film.iloc[0]
                extreme_text += f"{mood.upper()}:\n"
                extreme_text += f"  {str(film['title'])[:45]}\n"
                extreme_text += f"  Score: {film[col]:.0f} | Rating: {film['IMDb Rating']:.1f}\n\n"

        ax1.text(0.1, 0.9, extreme_text, transform=ax1.transAxes,
                fontsize=10, verticalalignment='top',
                family='monospace',
                bbox=dict(boxstyle='round', facecolor='#FFE6E6', alpha=0.7))
        ax1.set_title('Mood Intensity Champions', fontsize=12, fontweight='bold')

        # 2. Pure mood films (only one mood)
        ax2 = axes[0, 1]
        pure_mood_counts = defaultdict(int)
        for idx, row in self.df[self.df['mood_diversity'] == 1].iterrows():
            mood = row['dominant_mood']
            pure_mood_counts[mood] += 1

        if pure_mood_counts:
            moods = list(pure_mood_counts.keys())
            counts = list(pure_mood_counts.values())

            bars = ax2.bar(range(len(moods)), counts,
                          color=plt.cm.Pastel1(np.arange(len(moods))),
                          edgecolor='black')
            ax2.set_xticks(range(len(moods)))
            ax2.set_xticklabels(moods, rotation=45, ha='right', fontsize=9)
            ax2.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
            ax2.set_title('Pure Single-Mood Films', fontsize=12, fontweight='bold')
            ax2.grid(axis='y', alpha=0.3)

            # Annotate
            for bar, count in zip(bars, counts):
                height = bar.get_height()
                ax2.text(bar.get_x() + bar.get_width()/2., height,
                        f'{count}', ha='center', va='bottom', fontsize=9)

        # 3. Mood intensity scatter (rating vs complexity)
        ax3 = axes[1, 0]
        mood_cols = [f'mood_{m.lower()}' for m in self.mood_keywords.keys()]
        self.df['total_mood_score'] = self.df[mood_cols].sum(axis=1)

        scatter = ax3.scatter(self.df['mood_diversity'], self.df['total_mood_score'],
                             c=self.df['IMDb Rating'], cmap='RdYlGn',
                             alpha=0.6, s=50, edgecolors='black', linewidth=0.5)
        ax3.set_xlabel('Mood Diversity (# of moods)', fontsize=11, fontweight='bold')
        ax3.set_ylabel('Total Mood Intensity', fontsize=11, fontweight='bold')
        ax3.set_title('Mood Complexity vs Intensity', fontsize=12, fontweight='bold')
        ax3.grid(True, alpha=0.3)

        # Colorbar
        cbar = plt.colorbar(scatter, ax=ax3)
        cbar.set_label('IMDb Rating', fontsize=10, fontweight='bold')

        # 4. Top-rated per mood
        ax4 = axes[1, 1]
        ax4.axis('off')

        toprated_text = "HIGHEST-RATED FILMS PER MOOD\n" + "="*50 + "\n\n"

        for mood in list(self.mood_keywords.keys())[:4]:
            mood_films = self.df[self.df['dominant_mood'] == mood]
            if len(mood_films) > 0:
                top = mood_films.nlargest(1, 'IMDb Rating').iloc[0]
                toprated_text += f"{mood.upper()}:\n"
                toprated_text += f"  {str(top['title'])[:45]}\n"
                toprated_text += f"  Rating: {top['IMDb Rating']:.1f}/10\n\n"

        ax4.text(0.1, 0.9, toprated_text, transform=ax4.transAxes,
                fontsize=10, verticalalignment='top',
                family='monospace',
                bbox=dict(boxstyle='round', facecolor='#E6FFE6', alpha=0.7))
        ax4.set_title('Top-Rated by Mood', fontsize=12, fontweight='bold')

        plt.tight_layout()
        output_path = self.output_dir / 'mood_extremes.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def generate_report(self):
        """Generate comprehensive sentiment and mood report."""
        print("\nGenerating sentiment and mood report...")

        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("CINESCOPE BATCH 33: SENTIMENT & MOOD CLUSTERING")
        report_lines.append("=" * 80)
        report_lines.append("")
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("")

        # Overall stats
        report_lines.append("=" * 80)
        report_lines.append("OVERALL MOOD STATISTICS")
        report_lines.append("=" * 80)
        report_lines.append("")
        report_lines.append(f"Total Films: {len(self.df)}")
        report_lines.append("")

        # Dominant mood distribution
        mood_counts = self.df['dominant_mood'].value_counts()
        report_lines.append("DOMINANT MOOD DISTRIBUTION:")
        report_lines.append("-" * 80)
        for mood, count in mood_counts.items():
            pct = count / len(self.df) * 100
            report_lines.append(f"{mood:20s}: {count:4d} films ({pct:5.1f}%)")

        report_lines.append("")

        # Mood diversity
        report_lines.append("=" * 80)
        report_lines.append("EMOTIONAL COMPLEXITY")
        report_lines.append("=" * 80)
        report_lines.append("")

        diversity_stats = self.df['mood_diversity'].describe()
        report_lines.append(f"Average mood categories per film: {diversity_stats['mean']:.2f}")
        report_lines.append(f"Median: {diversity_stats['50%']:.0f}")
        report_lines.append(f"Max: {diversity_stats['max']:.0f}")
        report_lines.append("")

        # Most complex films
        report_lines.append("MOST EMOTIONALLY COMPLEX FILMS:")
        report_lines.append("-" * 80)
        complex_films = self.df.nlargest(10, 'mood_diversity')[['title', 'mood_diversity', 'IMDb Rating']]
        for i, (idx, row) in enumerate(complex_films.iterrows(), 1):
            report_lines.append(f"{i:2d}. {str(row['title']):50s} - {row['mood_diversity']:.0f} moods (Rating: {row['IMDb Rating']:.1f})")

        report_lines.append("")

        # Top films by mood
        report_lines.append("=" * 80)
        report_lines.append("TOP-RATED FILMS BY MOOD")
        report_lines.append("=" * 80)
        report_lines.append("")

        for mood in mood_counts.head(6).index:
            if mood != 'Neutral':
                report_lines.append(f"\n{mood.upper()}:")
                report_lines.append("-" * 80)
                top_films = self.df[self.df['dominant_mood'] == mood].nlargest(5, 'IMDb Rating')
                for i, (idx, row) in enumerate(top_films.iterrows(), 1):
                    report_lines.append(f"{i}. {str(row['title']):50s} - {row['IMDb Rating']:.1f}/10")

        report_lines.append("")
        report_lines.append("=" * 80)
        report_lines.append("END OF REPORT")
        report_lines.append("=" * 80)

        # Write report
        report_path = self.report_dir / 'batch_33_sentiment_mood_report.txt'
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))

        print(f"Report saved: {report_path}")

    def run_all_analyses(self):
        """Execute all analyses and generate report."""
        print("\n" + "="*80)
        print("BATCH 33: SENTIMENT & MOOD CLUSTERING")
        print("="*80)

        print("\n[1/6] Analyzing mood distribution...")
        self.visualize_mood_distribution()

        print("\n[2/6] Performing mood clustering...")
        self.visualize_mood_clustering()

        print("\n[3/6] Analyzing mood correlations...")
        self.visualize_mood_correlations()

        print("\n[4/6] Analyzing mood patterns by genre...")
        self.visualize_mood_by_genre()

        print("\n[5/6] Identifying mood extremes...")
        self.visualize_mood_extremes()

        print("\n[6/6] Generating comprehensive report...")
        self.generate_report()

        print("\n" + "="*80)
        print("BATCH 33 COMPLETE")
        print("="*80)
        print(f"\nVisualizations saved to: {self.output_dir}")
        print(f"Report saved to: {self.report_dir}/batch_33_sentiment_mood_report.txt")


def main():
    """Main execution function."""
    analyzer = SentimentMoodAnalyzer()
    analyzer.run_all_analyses()


if __name__ == '__main__':
    main()
