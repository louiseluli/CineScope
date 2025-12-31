#!/usr/bin/env python3
"""
CineScope Batch 32: Machine Learning Recommendations
====================================================

Advanced recommendation system using collaborative filtering, content-based
filtering, and hybrid approaches. Compares multiple ML techniques.

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
from scipy.sparse import csr_matrix
from scipy.spatial.distance import cosine
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD
from sklearn.preprocessing import MinMaxScaler
import warnings
warnings.filterwarnings('ignore')

class MLRecommendationEngine:
    """Build and compare multiple recommendation approaches."""

    def __init__(self,
                 watched_path='data/processed/watched_movies_master.csv',
                 catalog_path='data/processed/master_cinema_data.csv'):
        """Initialize recommendation engine with watched and full catalog."""
        self.watched_path = Path(watched_path)
        self.catalog_path = Path(catalog_path)
        self.output_dir = Path('analysis_outputs/visualizations/batch_32')
        self.report_dir = Path('analysis_outputs/reports')

        # Create output directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)

        # Load watched movies
        print("Loading watched movies data...")
        self.watched_df = pd.read_csv(self.watched_path)
        print(f"Loaded {len(self.watched_df)} watched films")

        # Load full catalog
        print("Loading full catalog...")
        self.catalog_df = pd.read_csv(self.catalog_path)
        print(f"Loaded {len(self.catalog_df)} total films in catalog")

        # Standardize column names in catalog to match watched
        if 'imdb_rating' in self.catalog_df.columns:
            self.catalog_df['IMDb Rating'] = self.catalog_df['imdb_rating']
        if 'genres' in self.catalog_df.columns:
            self.catalog_df['Genres'] = self.catalog_df['genres']
        if 'directors' in self.catalog_df.columns:
            self.catalog_df['Directors'] = self.catalog_df['directors']

        # Get unwatched films
        watched_titles = set(self.watched_df['title'].str.lower())
        self.unwatched_df = self.catalog_df[~self.catalog_df['title'].str.lower().isin(watched_titles)].copy()
        print(f"Found {len(self.unwatched_df)} unwatched films for recommendations")

        # Use watched for analysis, unwatched for recommendations
        self.df = self.watched_df.copy()

        # Build feature matrices
        self._build_feature_matrices()

        # Set up plotting
        plt.style.use('default')
        sns.set_palette("husl")

    def _build_feature_matrices(self):
        """Build feature matrices for full catalog."""
        print("Building feature matrices for full catalog...")

        # Build features for ALL films (catalog + watched)
        all_df = pd.concat([self.watched_df, self.unwatched_df], ignore_index=True)

        # 1. Genre-based features
        all_df['genres_clean'] = all_df['Genres'].fillna('').astype(str)

        # 2. Keywords-based features
        all_df['keywords_text'] = ''
        for idx, row in all_df.iterrows():
            if pd.notna(row.get('keyword_themes')):
                try:
                    themes = str(row['keyword_themes'])
                    all_df.at[idx, 'keywords_text'] = themes
                except:
                    pass

        # 3. Cast-based features
        all_df['cast_text'] = ''
        for idx, row in all_df.iterrows():
            if pd.notna(row.get('tmdb_cast')):
                try:
                    cast = ast.literal_eval(row['tmdb_cast'])
                    actors = [actor['name'] for actor in cast[:5] if 'name' in actor]
                    all_df.at[idx, 'cast_text'] = ' '.join(actors)
                except:
                    pass

        # 4. Director features
        all_df['director_text'] = all_df['Directors'].fillna('').astype(str)

        # 5. Combined content features
        all_df['content_features'] = (
            all_df['genres_clean'] + ' ' +
            all_df['keywords_text'] + ' ' +
            all_df['cast_text'] + ' ' +
            all_df['director_text']
        )

        # Build TF-IDF matrix on ALL films
        print("  Building TF-IDF matrix on full catalog...")
        self.tfidf = TfidfVectorizer(max_features=500, stop_words='english')
        all_content_matrix = self.tfidf.fit_transform(all_df['content_features'])

        # Split back into watched and unwatched
        n_watched = len(self.watched_df)
        self.watched_matrix = all_content_matrix[:n_watched]
        self.unwatched_matrix = all_content_matrix[n_watched:]

        # Store for recommendations
        self.all_df = all_df
        self.n_watched = n_watched

        print(f"  Watched matrix shape: {self.watched_matrix.shape}")
        print(f"  Unwatched matrix shape: {self.unwatched_matrix.shape}")

        # For visualization, keep watched similarity
        self.content_matrix = self.watched_matrix
        self.content_similarity = cosine_similarity(self.watched_matrix)

    def get_content_recommendations(self, movie_idx, n=10):
        """Get content-based recommendations from UNWATCHED films."""
        # Get the watched movie's feature vector
        watched_vector = self.watched_matrix[movie_idx]

        # Calculate similarity to ALL unwatched films
        similarities = cosine_similarity(watched_vector, self.unwatched_matrix).flatten()

        # Get top N
        top_indices = np.argsort(similarities)[-n:][::-1]
        scores = similarities[top_indices]

        # Get unwatched films
        recommended = self.unwatched_df.iloc[top_indices][['title', 'IMDb Rating', 'Genres']].copy()

        return recommended, scores.tolist()

    def get_rating_based_recommendations(self, movie_idx, n=10):
        """Get recommendations from UNWATCHED based on rating + genre."""
        target_rating = self.watched_df.iloc[movie_idx]['IMDb Rating']
        target_genres = set(str(self.watched_df.iloc[movie_idx]['Genres']).split(','))

        # Calculate scores for unwatched films only
        scores = []
        for idx, row in self.unwatched_df.iterrows():
            # Rating similarity
            rating_diff = abs(row['IMDb Rating'] - target_rating)
            rating_score = max(0, 1 - rating_diff / 5)

            # Genre overlap
            genres = set(str(row['Genres']).split(','))
            overlap = len(target_genres & genres) / max(len(target_genres | genres), 1)

            # Combined score
            total_score = (rating_score * 0.5) + (overlap * 0.5)
            scores.append(total_score)

        # Get top N
        top_indices = np.argsort(scores)[-n:][::-1]
        top_scores = [scores[i] for i in top_indices]

        return self.unwatched_df.iloc[top_indices][['title', 'IMDb Rating', 'Genres']].copy(), top_scores

    def get_director_recommendations(self, movie_idx, n=10):
        """Get UNWATCHED recommendations from same director."""
        target_director = self.watched_df.iloc[movie_idx]['Directors']

        if pd.isna(target_director):
            # Fallback to top-rated unwatched
            result = self.unwatched_df.nlargest(n, 'IMDb Rating')[['title', 'IMDb Rating', 'Genres']]
            return result, [0.5] * len(result)

        # Same director unwatched films
        same_director = self.unwatched_df[
            self.unwatched_df['Directors'] == target_director
        ].copy()

        if len(same_director) >= n:
            result = same_director.nlargest(n, 'IMDb Rating')[['title', 'IMDb Rating', 'Genres']]
            scores = [1.0] * len(result)
            return result, scores

        # If not enough, add top-rated unwatched films
        remaining = n - len(same_director)
        other_films = self.unwatched_df[
            self.unwatched_df['Directors'] != target_director
        ].nlargest(remaining, 'IMDb Rating')[['title', 'IMDb Rating', 'Genres']]

        if len(same_director) > 0:
            result = pd.concat([same_director[['title', 'IMDb Rating', 'Genres']], other_films])
            scores = [1.0] * len(same_director) + [0.5] * len(other_films)
        else:
            result = other_films
            scores = [0.5] * len(other_films)

        return result, scores

    def visualize_recommendation_comparison(self):
        """Visualization 1-2: Compare different recommendation approaches."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Recommendation System Comparison', fontsize=16, fontweight='bold')

        # Test on a high-rated watched film
        test_idx = self.watched_df['IMDb Rating'].idxmax()
        test_movie = self.watched_df.iloc[test_idx]['title']

        # 1. Content-based recommendations
        ax1 = axes[0, 0]
        content_recs, content_scores = self.get_content_recommendations(test_idx, n=10)

        if len(content_recs) > 0:
            y_pos = np.arange(len(content_recs))
            bars = ax1.barh(y_pos, content_scores, color='#3498DB', edgecolor='black')
            ax1.set_yticks(y_pos)
            ax1.set_yticklabels([str(t)[:30] + '...' if len(str(t)) > 30 else str(t)
                                for t in content_recs['title'].values], fontsize=8)
            ax1.set_xlabel('Similarity Score', fontsize=11, fontweight='bold')
            ax1.set_title(f'Content-Based Recommendations\nFor: {test_movie}',
                         fontsize=11, fontweight='bold')
            ax1.invert_yaxis()
            ax1.grid(axis='x', alpha=0.3)

            # Add ratings
            for i, (idx, row) in enumerate(content_recs.iterrows()):
                ax1.text(content_scores[i] + 0.01, i, f"{row['IMDb Rating']:.1f}",
                        va='center', fontsize=7)

        # 2. Rating-based recommendations
        ax2 = axes[0, 1]
        rating_recs, rating_scores = self.get_rating_based_recommendations(test_idx, n=10)

        if len(rating_recs) > 0:
            y_pos = np.arange(len(rating_recs))
            bars = ax2.barh(y_pos, rating_scores, color='#2ECC71', edgecolor='black')
            ax2.set_yticks(y_pos)
            ax2.set_yticklabels([str(t)[:30] + '...' if len(str(t)) > 30 else str(t)
                                for t in rating_recs['title'].values], fontsize=8)
            ax2.set_xlabel('Match Score', fontsize=11, fontweight='bold')
            ax2.set_title(f'Rating + Genre Recommendations\nFor: {test_movie}',
                         fontsize=11, fontweight='bold')
            ax2.invert_yaxis()
            ax2.grid(axis='x', alpha=0.3)

            # Add ratings
            for i, (idx, row) in enumerate(rating_recs.iterrows()):
                ax2.text(rating_scores[i] + 0.01, i, f"{row['IMDb Rating']:.1f}",
                        va='center', fontsize=7)

        # 3. Director-based recommendations
        ax3 = axes[1, 0]
        director_recs, director_scores = self.get_director_recommendations(test_idx, n=10)

        if len(director_recs) > 0:
            y_pos = np.arange(len(director_recs))
            colors = ['#E74C3C' if s == 1.0 else '#F39C12' for s in director_scores]
            bars = ax3.barh(y_pos, director_scores, color=colors, edgecolor='black')
            ax3.set_yticks(y_pos)
            ax3.set_yticklabels([str(t)[:30] + '...' if len(str(t)) > 30 else str(t)
                                for t in director_recs['title'].values], fontsize=8)
            ax3.set_xlabel('Relevance Score', fontsize=11, fontweight='bold')
            ax3.set_title(f'Director-Based Recommendations\nFor: {test_movie}',
                         fontsize=11, fontweight='bold')
            ax3.invert_yaxis()
            ax3.grid(axis='x', alpha=0.3)

            # Add ratings
            for i, (idx, row) in enumerate(director_recs.iterrows()):
                ax3.text(director_scores[i] + 0.01, i, f"{row['IMDb Rating']:.1f}",
                        va='center', fontsize=7)

        # 4. Approach comparison metrics
        ax4 = axes[1, 1]
        ax4.axis('off')

        metrics_text = "RECOMMENDATION APPROACHES\n" + "="*50 + "\n\n"
        metrics_text += "CONTENT-BASED:\n"
        metrics_text += "  Uses: Genres, Keywords, Cast, Directors\n"
        metrics_text += "  Pros: Finds thematically similar films\n"
        metrics_text += "  Cons: May miss quality differences\n\n"

        metrics_text += "RATING + GENRE:\n"
        metrics_text += "  Uses: IMDb ratings + genre overlap\n"
        metrics_text += "  Pros: Quality-aware recommendations\n"
        metrics_text += "  Cons: Less diverse suggestions\n\n"

        metrics_text += "DIRECTOR-BASED:\n"
        metrics_text += "  Uses: Same director preference\n"
        metrics_text += "  Pros: Stylistic consistency\n"
        metrics_text += "  Cons: Limited to director catalog\n\n"

        # Average ratings of recommendations
        if len(content_recs) > 0:
            metrics_text += f"Avg Rating (Content): {content_recs['IMDb Rating'].mean():.2f}\n"
        if len(rating_recs) > 0:
            metrics_text += f"Avg Rating (Rating): {rating_recs['IMDb Rating'].mean():.2f}\n"
        if len(director_recs) > 0:
            metrics_text += f"Avg Rating (Director): {director_recs['IMDb Rating'].mean():.2f}\n"

        ax4.text(0.1, 0.9, metrics_text, transform=ax4.transAxes,
                fontsize=9, verticalalignment='top',
                family='monospace',
                bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.5))
        ax4.set_title('Approach Comparison', fontsize=12, fontweight='bold')

        plt.tight_layout()
        output_path = self.output_dir / 'recommendation_comparison.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_similarity_heatmap(self):
        """Visualization 3: Similarity heatmap for top-rated films."""
        fig, ax = plt.subplots(1, 1, figsize=(14, 12))
        fig.suptitle('Content Similarity: Top-Rated Films', fontsize=16, fontweight='bold')

        # Get top 30 rated WATCHED films
        top_films = self.watched_df.nlargest(30, 'IMDb Rating')
        indices = top_films.index.tolist()

        # Extract similarity submatrix
        sim_matrix = self.content_similarity[np.ix_(indices, indices)]

        # Plot heatmap
        labels = [str(t)[:25] + '...' if len(str(t)) > 25 else str(t) for t in top_films['title'].values]

        im = ax.imshow(sim_matrix, cmap='YlOrRd', aspect='auto', vmin=0, vmax=1)
        ax.set_xticks(np.arange(len(labels)))
        ax.set_yticks(np.arange(len(labels)))
        ax.set_xticklabels(labels, rotation=90, fontsize=7)
        ax.set_yticklabels(labels, fontsize=7)
        ax.set_title('Similarity Matrix (Top 30 Films)', fontsize=12, fontweight='bold', pad=20)

        # Add colorbar
        cbar = plt.colorbar(im, ax=ax)
        cbar.set_label('Cosine Similarity', fontsize=11, fontweight='bold')

        plt.tight_layout()
        output_path = self.output_dir / 'similarity_heatmap.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_feature_importance(self):
        """Visualization 4: Feature importance in recommendations."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Feature Importance Analysis', fontsize=16, fontweight='bold')

        # 1. TF-IDF top features
        ax1 = axes[0, 0]
        feature_names = self.tfidf.get_feature_names_out()

        # Get average TF-IDF scores
        avg_scores = np.asarray(self.content_matrix.mean(axis=0)).flatten()
        top_indices = np.argsort(avg_scores)[-20:]

        top_features = [feature_names[i] for i in top_indices]
        top_scores = [avg_scores[i] for i in top_indices]

        y_pos = np.arange(len(top_features))
        bars = ax1.barh(y_pos, top_scores, color='#9B59B6', edgecolor='black')
        ax1.set_yticks(y_pos)
        ax1.set_yticklabels(top_features, fontsize=9)
        ax1.set_xlabel('Average TF-IDF Score', fontsize=11, fontweight='bold')
        ax1.set_title('Top 20 Features Across All Films', fontsize=12, fontweight='bold')
        ax1.invert_yaxis()
        ax1.grid(axis='x', alpha=0.3)

        # 2. Genre distribution in watched dataset
        ax2 = axes[0, 1]
        all_genres = []
        for genres_str in self.watched_df['Genres'].dropna():
            all_genres.extend([g.strip() for g in str(genres_str).split(',')])

        genre_counts = Counter(all_genres).most_common(15)
        genres, counts = zip(*genre_counts)

        bars = ax2.bar(range(len(genres)), counts, color=plt.cm.tab10(np.arange(len(genres))),
                      edgecolor='black')
        ax2.set_xticks(range(len(genres)))
        ax2.set_xticklabels(genres, rotation=45, ha='right', fontsize=9)
        ax2.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
        ax2.set_title('Genre Distribution in Watched Films', fontsize=12, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        # 3. Rating distribution
        ax3 = axes[1, 0]
        ax3.hist(self.watched_df['IMDb Rating'].dropna(), bins=30, color='#1ABC9C',
                edgecolor='black', alpha=0.7)
        ax3.axvline(self.watched_df['IMDb Rating'].mean(), color='red', linestyle='--',
                   linewidth=2, label=f"Mean: {self.watched_df['IMDb Rating'].mean():.2f}")
        ax3.axvline(self.watched_df['IMDb Rating'].median(), color='blue', linestyle='--',
                   linewidth=2, label=f"Median: {self.watched_df['IMDb Rating'].median():.2f}")
        ax3.set_xlabel('IMDb Rating', fontsize=11, fontweight='bold')
        ax3.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
        ax3.set_title('Rating Distribution', fontsize=12, fontweight='bold')
        ax3.legend()
        ax3.grid(axis='y', alpha=0.3)

        # 4. Decade distribution
        ax4 = axes[1, 1]
        self.watched_df['decade'] = (self.watched_df['year'] // 10) * 10
        decade_counts = self.watched_df['decade'].value_counts().sort_index()

        bars = ax4.bar(decade_counts.index, decade_counts.values, width=8,
                      color='#E67E22', edgecolor='black')
        ax4.set_xlabel('Decade', fontsize=11, fontweight='bold')
        ax4.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
        ax4.set_title('Temporal Distribution', fontsize=12, fontweight='bold')
        ax4.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        output_path = self.output_dir / 'feature_importance.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def generate_report(self):
        """Generate comprehensive recommendation engine report."""
        print("\nGenerating recommendation engine report...")

        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("CINESCOPE BATCH 32: MACHINE LEARNING RECOMMENDATIONS")
        report_lines.append("=" * 80)
        report_lines.append("")
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("")

        # Dataset stats
        report_lines.append("=" * 80)
        report_lines.append("DATASET STATISTICS")
        report_lines.append("=" * 80)
        report_lines.append("")
        report_lines.append(f"Watched Films: {len(self.watched_df)}")
        report_lines.append(f"Unwatched Films: {len(self.unwatched_df)}")
        report_lines.append(f"Total Catalog: {len(self.all_df)}")
        report_lines.append(f"TF-IDF Features: {len(self.tfidf.get_feature_names_out())}")
        report_lines.append("")
        report_lines.append(f"Watched Avg Rating: {self.watched_df['IMDb Rating'].mean():.2f}")
        report_lines.append(f"Unwatched Avg Rating: {self.unwatched_df['IMDb Rating'].mean():.2f}")
        report_lines.append("")

        # Sample recommendations
        report_lines.append("=" * 80)
        report_lines.append("SAMPLE RECOMMENDATIONS (UNWATCHED FILMS ONLY)")
        report_lines.append("=" * 80)
        report_lines.append("")

        # Test on top 5 rated watched films
        top_5 = self.watched_df.nlargest(5, 'IMDb Rating')

        for idx in top_5.index:
            movie = self.watched_df.iloc[idx]
            report_lines.append(f"\nSOURCE FILM: {movie['title']} ({movie['IMDb Rating']:.1f}/10)")
            report_lines.append(f"Genre: {movie['Genres']}")
            report_lines.append(f"Director: {movie['Directors']}")
            report_lines.append("-" * 80)

            # Content-based
            recs, scores = self.get_content_recommendations(idx, n=5)
            report_lines.append("\nCONTENT-BASED RECOMMENDATIONS:")
            for i, (rec_idx, rec) in enumerate(recs.iterrows(), 1):
                report_lines.append(f"  {i}. {rec['title']} (Sim: {scores[i-1]:.3f}, Rating: {rec['IMDb Rating']:.1f})")

            # Rating-based
            recs, scores = self.get_rating_based_recommendations(idx, n=5)
            report_lines.append("\nRATING + GENRE RECOMMENDATIONS:")
            for i, (rec_idx, rec) in enumerate(recs.iterrows(), 1):
                report_lines.append(f"  {i}. {rec['title']} (Score: {scores[i-1]:.3f}, Rating: {rec['IMDb Rating']:.1f})")

            report_lines.append("")

        # Top features
        report_lines.append("=" * 80)
        report_lines.append("TOP TF-IDF FEATURES")
        report_lines.append("=" * 80)
        report_lines.append("")

        feature_names = self.tfidf.get_feature_names_out()
        avg_scores = np.asarray(self.content_matrix.mean(axis=0)).flatten()
        top_indices = np.argsort(avg_scores)[-30:][::-1]

        for i, idx in enumerate(top_indices, 1):
            report_lines.append(f"{i:2d}. {feature_names[idx]:30s} - {avg_scores[idx]:.6f}")

        report_lines.append("")
        report_lines.append("=" * 80)
        report_lines.append("END OF REPORT")
        report_lines.append("=" * 80)

        # Write report
        report_path = self.report_dir / 'batch_32_ml_recommendations_report.txt'
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))

        print(f"Report saved: {report_path}")

    def run_all_analyses(self):
        """Execute all analyses and generate report."""
        print("\n" + "="*80)
        print("BATCH 32: MACHINE LEARNING RECOMMENDATIONS")
        print("="*80)

        print("\n[1/4] Comparing recommendation approaches...")
        self.visualize_recommendation_comparison()

        print("\n[2/4] Creating similarity heatmap...")
        self.visualize_similarity_heatmap()

        print("\n[3/4] Analyzing feature importance...")
        self.visualize_feature_importance()

        print("\n[4/4] Generating comprehensive report...")
        self.generate_report()

        print("\n" + "="*80)
        print("BATCH 32 COMPLETE")
        print("="*80)
        print(f"\nVisualizations saved to: {self.output_dir}")
        print(f"Report saved to: {self.report_dir}/batch_32_ml_recommendations_report.txt")


def main():
    """Main execution function."""
    engine = MLRecommendationEngine()
    engine.run_all_analyses()


if __name__ == '__main__':
    main()
