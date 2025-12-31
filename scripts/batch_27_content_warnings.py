#!/usr/bin/env python3
"""
CineScope Batch 27: Content Warnings Deep-Dive
Analyzes content ratings, MPAA classifications, and content themes from keywords.
"""

import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict
import re

class ContentWarningsAnalyzer:
    def __init__(self):
        self.data_dir = Path('data/processed')
        self.output_dir = Path('analysis_outputs')
        self.viz_dir = self.output_dir / 'visualizations' / 'batch_27'
        self.reports_dir = self.output_dir / 'reports'

        # Create output directories
        self.viz_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)

        # Load data
        self.movies_df = pd.read_csv(self.data_dir / 'watched_movies_master.csv')

        # Setup plotting style
        plt.style.use('default')
        sns.set_palette("husl")

        # Define content warning categories from keywords
        self.warning_categories = {
            'Violence': [
                'violence', 'murder', 'killing', 'blood', 'gore', 'death', 'war',
                'torture', 'brutality', 'assassination', 'massacre', 'execution',
                'fight', 'fighting', 'combat', 'battle', 'gun', 'weapon', 'bomb'
            ],
            'Sexual Content': [
                'sex', 'sexual', 'nudity', 'rape', 'prostitution', 'adultery',
                'infidelity', 'erotic', 'seduction', 'affair', 'sexuality',
                'lesbian', 'gay', 'lgbt', 'homosexual', 'explicit'
            ],
            'Substance Abuse': [
                'drug', 'drugs', 'alcohol', 'alcoholic', 'alcoholism', 'smoking',
                'cocaine', 'heroin', 'marijuana', 'addiction', 'overdose',
                'drunk', 'drinking', 'junkie', 'substance abuse'
            ],
            'Animal Content': [
                'dog death', 'animal', 'pet', 'horse', 'cat', 'dog', 'animal abuse',
                'animal cruelty', 'dead animal'
            ],
            'Mental Health': [
                'suicide', 'depression', 'mental illness', 'psychosis', 'insanity',
                'schizophrenia', 'trauma', 'ptsd', 'anxiety', 'self harm'
            ],
            'Disturbing Themes': [
                'horror', 'scary', 'fear', 'terror', 'nightmare', 'monster',
                'ghost', 'haunted', 'creepy', 'disturbing', 'dark'
            ],
            'Crime': [
                'crime', 'criminal', 'theft', 'robbery', 'heist', 'corruption',
                'mafia', 'gang', 'organized crime', 'kidnapping', 'blackmail'
            ],
            'Family Issues': [
                'child abuse', 'domestic violence', 'divorce', 'family conflict',
                'dysfunctional family', 'child neglect', 'abandonment'
            ],
            'Discrimination': [
                'racism', 'racist', 'discrimination', 'prejudice', 'segregation',
                'antisemitism', 'homophobia', 'sexism', 'hate crime'
            ],
            'Mature Themes': [
                'adult', 'mature', 'controversial', 'taboo', 'explicit language',
                'profanity', 'strong language'
            ]
        }

        print(f"Loaded {len(self.movies_df)} movies for content analysis")

    def extract_warnings_from_keywords(self, keywords_str):
        """Extract content warnings from keyword string."""
        if pd.isna(keywords_str) or str(keywords_str).strip() == '':
            return {}

        keywords_lower = str(keywords_str).lower()
        warnings = {}

        for category, terms in self.warning_categories.items():
            found_terms = []
            for term in terms:
                if term in keywords_lower:
                    found_terms.append(term)

            if found_terms:
                warnings[category] = found_terms

        return warnings

    def categorize_mpaa_rating(self, rating):
        """Categorize MPAA ratings into groups."""
        if pd.isna(rating):
            return 'Unrated'

        rating_str = str(rating).strip().upper()

        # Map to standard categories
        if rating_str in ['G', 'TV-G']:
            return 'G (General)'
        elif rating_str in ['PG', 'TV-PG']:
            return 'PG (Parental Guidance)'
        elif rating_str in ['PG-13', 'TV-14']:
            return 'PG-13 (Teens)'
        elif rating_str in ['R', 'TV-MA']:
            return 'R (Restricted)'
        elif rating_str in ['NC-17', 'X']:
            return 'NC-17 (Adults Only)'
        elif rating_str in ['APPROVED', 'PASSED', 'M', 'M/PG']:
            return 'Legacy Ratings'
        else:
            return 'Other/Unrated'

    def visualize_mpaa_distribution(self):
        """Analyze MPAA rating distribution."""
        print("Analyzing MPAA rating distribution...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('MPAA Rating Distribution Analysis', fontsize=16, fontweight='bold')

        # Raw rating distribution
        ax1 = axes[0, 0]

        rating_counts = self.movies_df['omdb_rated'].value_counts().head(12)
        colors = plt.cm.Spectral(np.linspace(0.2, 0.8, len(rating_counts)))

        ax1.barh(range(len(rating_counts)), rating_counts.values, color=colors, alpha=0.7, edgecolor='black')
        ax1.set_yticks(range(len(rating_counts)))
        ax1.set_yticklabels(rating_counts.index)
        ax1.invert_yaxis()
        ax1.set_xlabel('Number of Films')
        ax1.set_title('MPAA Rating Distribution (Top 12)')
        ax1.grid(axis='x', alpha=0.3)

        for i, count in enumerate(rating_counts.values):
            pct = (count / len(self.movies_df)) * 100
            ax1.text(count, i, f' {count} ({pct:.1f}%)', va='center', fontsize=9)

        # Categorized ratings pie chart
        ax2 = axes[0, 1]

        self.movies_df['rating_category'] = self.movies_df['omdb_rated'].apply(
            self.categorize_mpaa_rating
        )

        category_counts = self.movies_df['rating_category'].value_counts()
        colors_pie = ['green', 'yellow', 'orange', 'red', 'darkred', 'gray', 'lightgray']

        ax2.pie(category_counts.values, labels=category_counts.index, autopct='%1.1f%%',
               colors=colors_pie[:len(category_counts)], startangle=90)
        ax2.set_title('Ratings by Category')

        # Rating vs IMDb score
        ax3 = axes[1, 0]

        # Get top rating categories
        top_categories = ['G (General)', 'PG (Parental Guidance)', 'PG-13 (Teens)', 'R (Restricted)']
        rating_scores = []
        labels = []

        for cat in top_categories:
            cat_data = self.movies_df[self.movies_df['rating_category'] == cat]
            if len(cat_data) >= 5:
                ratings = cat_data['IMDb Rating'].dropna()
                if len(ratings) > 0:
                    rating_scores.append(ratings.values)
                    labels.append(f'{cat}\n(n={len(ratings)})')

        if rating_scores:
            bp = ax3.boxplot(rating_scores, labels=labels, patch_artist=True)
            for patch in bp['boxes']:
                patch.set_facecolor('lightblue')
                patch.set_alpha(0.7)

            ax3.set_ylabel('IMDb Rating')
            ax3.set_title('Film Quality by MPAA Rating')
            ax3.grid(axis='y', alpha=0.3)

        # Rating evolution by decade
        ax4 = axes[1, 1]

        decade_ratings = {}
        for decade in range(1920, 2030, 10):
            decade_movies = self.movies_df[
                (self.movies_df['Year'] >= decade) &
                (self.movies_df['Year'] < decade + 10) &
                (self.movies_df['omdb_rated'].notna())
            ]

            if len(decade_movies) > 0:
                # Count R-rated percentage
                r_count = len(decade_movies[decade_movies['rating_category'] == 'R (Restricted)'])
                r_pct = (r_count / len(decade_movies)) * 100
                decade_ratings[decade] = r_pct

        if decade_ratings:
            decades = list(decade_ratings.keys())
            percentages = list(decade_ratings.values())
            decade_labels = [f"{d}s" for d in decades]

            ax4.plot(decade_labels, percentages, marker='o', linewidth=2, markersize=8, color='darkred')
            ax4.set_xlabel('Decade')
            ax4.set_ylabel('R-Rated Films (%)')
            ax4.set_title('R-Rating Prevalence Over Time')
            ax4.grid(alpha=0.3)
            ax4.tick_params(axis='x', rotation=45)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'mpaa_rating_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_content_warning_heatmap(self):
        """Create heatmap of content warnings across films."""
        print("Creating content warning heatmap...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Content Warning Analysis', fontsize=16, fontweight='bold')

        # Extract warnings for all films
        warning_presence = {cat: [] for cat in self.warning_categories.keys()}

        for idx, row in self.movies_df.iterrows():
            keywords = row.get('tmdb_keywords', '')
            warnings = self.extract_warnings_from_keywords(keywords)

            for category in self.warning_categories.keys():
                warning_presence[category].append(1 if category in warnings else 0)

        # Overall warning frequency
        ax1 = axes[0, 0]

        warning_counts = {cat: sum(vals) for cat, vals in warning_presence.items()}
        sorted_warnings = sorted(warning_counts.items(), key=lambda x: x[1], reverse=True)

        categories, counts = zip(*sorted_warnings)
        colors = plt.cm.YlOrRd(np.array(counts) / max(counts))

        ax1.barh(range(len(categories)), counts, color=colors, edgecolor='black', alpha=0.8)
        ax1.set_yticks(range(len(categories)))
        ax1.set_yticklabels(categories)
        ax1.invert_yaxis()
        ax1.set_xlabel('Number of Films')
        ax1.set_title('Content Warning Frequency')
        ax1.grid(axis='x', alpha=0.3)

        for i, count in enumerate(counts):
            pct = (count / len(self.movies_df)) * 100
            ax1.text(count, i, f' {count} ({pct:.1f}%)', va='center', fontsize=9)

        # Warning co-occurrence matrix
        ax2 = axes[0, 1]

        # Create co-occurrence matrix
        top_warnings = categories[:8]  # Top 8 warnings
        cooccur_matrix = np.zeros((len(top_warnings), len(top_warnings)))

        for idx, row in self.movies_df.iterrows():
            keywords = row.get('tmdb_keywords', '')
            warnings = self.extract_warnings_from_keywords(keywords)
            present_warnings = [w for w in top_warnings if w in warnings]

            for i, w1 in enumerate(top_warnings):
                for j, w2 in enumerate(top_warnings):
                    if w1 in present_warnings and w2 in present_warnings:
                        cooccur_matrix[i, j] += 1

        # Normalize to percentages
        for i in range(len(top_warnings)):
            if cooccur_matrix[i, i] > 0:
                cooccur_matrix[i, :] = (cooccur_matrix[i, :] / cooccur_matrix[i, i]) * 100

        im = ax2.imshow(cooccur_matrix, cmap='YlOrRd', aspect='auto')
        ax2.set_xticks(range(len(top_warnings)))
        ax2.set_yticks(range(len(top_warnings)))
        ax2.set_xticklabels([w[:12] for w in top_warnings], rotation=45, ha='right', fontsize=8)
        ax2.set_yticklabels([w[:12] for w in top_warnings], fontsize=8)
        ax2.set_title('Warning Co-occurrence (%)')
        plt.colorbar(im, ax=ax2)

        # Films with most warnings
        ax3 = axes[1, 0]

        warning_counts_per_film = []
        for idx, row in self.movies_df.iterrows():
            keywords = row.get('tmdb_keywords', '')
            warnings = self.extract_warnings_from_keywords(keywords)
            warning_counts_per_film.append({
                'title': row['Title'],
                'count': len(warnings),
                'rating': row.get('IMDb Rating', 0),
                'mpaa': row.get('omdb_rated', 'N/A')
            })

        warning_df = pd.DataFrame(warning_counts_per_film)
        top_warned = warning_df.nlargest(15, 'count')

        colors = plt.cm.Reds(np.linspace(0.4, 0.9, len(top_warned)))

        ax3.barh(range(len(top_warned)), top_warned['count'], color=colors, alpha=0.7, edgecolor='black')
        ax3.set_yticks(range(len(top_warned)))
        ax3.set_yticklabels([t[:25] for t in top_warned['title']], fontsize=8)
        ax3.invert_yaxis()
        ax3.set_xlabel('Number of Warning Categories')
        ax3.set_title('Films with Most Content Warnings')
        ax3.grid(axis='x', alpha=0.3)

        # Warning count distribution
        ax4 = axes[1, 1]

        ax4.hist(warning_df['count'], bins=range(0, warning_df['count'].max() + 2),
                color='coral', alpha=0.7, edgecolor='black')
        ax4.axvline(warning_df['count'].median(), color='red', linestyle='--',
                   linewidth=2, label=f'Median: {warning_df["count"].median():.1f}')
        ax4.set_xlabel('Number of Warning Categories per Film')
        ax4.set_ylabel('Frequency')
        ax4.set_title('Distribution of Warning Counts')
        ax4.legend()
        ax4.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'content_warning_heatmap.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_violence_analysis(self):
        """Analyze violence content patterns."""
        print("Analyzing violence content...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Violence Content Analysis', fontsize=16, fontweight='bold')

        # Classify films by violence level
        violence_scores = []
        for idx, row in self.movies_df.iterrows():
            keywords = row.get('tmdb_keywords', '')
            warnings = self.extract_warnings_from_keywords(keywords)

            violence_level = 0
            if 'Violence' in warnings:
                violence_level = len(warnings['Violence'])

            violence_scores.append({
                'title': row['Title'],
                'violence_score': violence_level,
                'rating': row.get('IMDb Rating', 0),
                'mpaa': row.get('omdb_rated', 'N/A'),
                'genre': row.get('Genres', '').split('|')[0] if pd.notna(row.get('Genres')) else 'Unknown'
            })

        violence_df = pd.DataFrame(violence_scores)

        # Violence level distribution
        ax1 = axes[0, 0]

        violence_df['violence_category'] = pd.cut(violence_df['violence_score'],
                                                   bins=[-1, 0, 2, 5, 100],
                                                   labels=['None', 'Low', 'Moderate', 'High'])

        category_counts = violence_df['violence_category'].value_counts()
        colors = ['green', 'yellow', 'orange', 'red']

        ax1.bar(range(len(category_counts)), category_counts.values,
               color=[colors[i] for i in range(len(category_counts))], alpha=0.7, edgecolor='black')
        ax1.set_xticks(range(len(category_counts)))
        ax1.set_xticklabels(category_counts.index)
        ax1.set_ylabel('Number of Films')
        ax1.set_title('Violence Level Distribution')
        ax1.grid(axis='y', alpha=0.3)

        for i, count in enumerate(category_counts.values):
            pct = (count / len(violence_df)) * 100
            ax1.text(i, count, f'{count}\n({pct:.1f}%)', ha='center', va='bottom', fontsize=9)

        # Violence vs rating
        ax2 = axes[0, 1]

        valid_data = violence_df[violence_df['rating'] > 0]
        if len(valid_data) > 10:
            ax2.scatter(valid_data['violence_score'], valid_data['rating'], alpha=0.3)

            if len(valid_data) > 20:
                from scipy import stats
                corr, p_value = stats.pearsonr(valid_data['violence_score'], valid_data['rating'])
                ax2.text(0.05, 0.95, f'Correlation: {corr:.3f}\np-value: {p_value:.4f}',
                        transform=ax2.transAxes, verticalalignment='top',
                        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

            ax2.set_xlabel('Violence Score')
            ax2.set_ylabel('IMDb Rating')
            ax2.set_title('Violence vs Film Quality')
            ax2.grid(alpha=0.3)

        # Most violent films
        ax3 = axes[1, 0]

        most_violent = violence_df.nlargest(15, 'violence_score')
        colors = plt.cm.Reds(np.linspace(0.4, 0.9, len(most_violent)))

        ax3.barh(range(len(most_violent)), most_violent['violence_score'],
                color=colors, alpha=0.7, edgecolor='black')
        ax3.set_yticks(range(len(most_violent)))
        ax3.set_yticklabels([f"{t[:20]} ({m})" for t, m in
                            zip(most_violent['title'], most_violent['mpaa'])], fontsize=8)
        ax3.invert_yaxis()
        ax3.set_xlabel('Violence Keywords Count')
        ax3.set_title('Most Violent Films (Top 15)')
        ax3.grid(axis='x', alpha=0.3)

        # Violence by genre
        ax4 = axes[1, 1]

        genre_violence = violence_df.groupby('genre').agg({
            'violence_score': 'mean'
        }).reset_index()

        genre_violence = genre_violence[genre_violence['violence_score'] > 0].nlargest(12, 'violence_score')

        colors = plt.cm.Oranges(np.linspace(0.4, 0.9, len(genre_violence)))

        ax4.barh(range(len(genre_violence)), genre_violence['violence_score'],
                color=colors, alpha=0.7, edgecolor='black')
        ax4.set_yticks(range(len(genre_violence)))
        ax4.set_yticklabels(genre_violence['genre'], fontsize=9)
        ax4.invert_yaxis()
        ax4.set_xlabel('Average Violence Score')
        ax4.set_title('Violence by Genre')
        ax4.grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'violence_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_adult_content(self):
        """Analyze adult/sexual content."""
        print("Analyzing adult content...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Adult & Sexual Content Analysis', fontsize=16, fontweight='bold')

        # Extract sexual content presence
        sexual_content_data = []
        for idx, row in self.movies_df.iterrows():
            keywords = row.get('tmdb_keywords', '')
            warnings = self.extract_warnings_from_keywords(keywords)

            has_sexual = 'Sexual Content' in warnings
            sexual_score = len(warnings.get('Sexual Content', []))

            sexual_content_data.append({
                'title': row['Title'],
                'has_sexual': has_sexual,
                'sexual_score': sexual_score,
                'tmdb_adult': row.get('tmdb_adult', False),
                'rating': row.get('IMDb Rating', 0),
                'mpaa': row.get('omdb_rated', 'N/A'),
                'year': row.get('Year', 0)
            })

        sexual_df = pd.DataFrame(sexual_content_data)

        # Sexual content presence
        ax1 = axes[0, 0]

        presence_counts = sexual_df['has_sexual'].value_counts()
        labels = ['No Sexual Content', 'Sexual Content']
        colors = ['green', 'red']

        ax1.pie(presence_counts.values, labels=labels, autopct='%1.1f%%',
               colors=colors, startangle=90)
        ax1.set_title('Sexual Content Presence')

        # MPAA rating vs sexual content
        ax2 = axes[0, 1]

        rating_sexual = sexual_df.groupby('mpaa')['has_sexual'].agg(['sum', 'count']).reset_index()
        rating_sexual['pct'] = (rating_sexual['sum'] / rating_sexual['count']) * 100
        rating_sexual = rating_sexual[rating_sexual['count'] >= 10].nlargest(10, 'pct')

        colors = plt.cm.Reds(rating_sexual['pct'].values / 100)

        ax2.barh(range(len(rating_sexual)), rating_sexual['pct'], color=colors, alpha=0.7, edgecolor='black')
        ax2.set_yticks(range(len(rating_sexual)))
        ax2.set_yticklabels([f"{r} (n={c})" for r, c in
                            zip(rating_sexual['mpaa'], rating_sexual['count'])], fontsize=9)
        ax2.invert_yaxis()
        ax2.set_xlabel('% with Sexual Content')
        ax2.set_title('Sexual Content by MPAA Rating')
        ax2.grid(axis='x', alpha=0.3)

        # Sexual content over time
        ax3 = axes[1, 0]

        decade_sexual = {}
        for decade in range(1920, 2030, 10):
            decade_movies = sexual_df[
                (sexual_df['year'] >= decade) &
                (sexual_df['year'] < decade + 10)
            ]

            if len(decade_movies) > 0:
                pct = (decade_movies['has_sexual'].sum() / len(decade_movies)) * 100
                decade_sexual[decade] = pct

        if decade_sexual:
            decades = list(decade_sexual.keys())
            percentages = list(decade_sexual.values())
            decade_labels = [f"{d}s" for d in decades]

            ax3.plot(decade_labels, percentages, marker='o', linewidth=2, markersize=8, color='darkred')
            ax3.set_xlabel('Decade')
            ax3.set_ylabel('Films with Sexual Content (%)')
            ax3.set_title('Sexual Content Prevalence Over Time')
            ax3.grid(alpha=0.3)
            ax3.tick_params(axis='x', rotation=45)

        # Films with most sexual content
        ax4 = axes[1, 1]

        most_sexual = sexual_df[sexual_df['sexual_score'] > 0].nlargest(15, 'sexual_score')

        if len(most_sexual) > 0:
            colors = plt.cm.Purples(np.linspace(0.4, 0.9, len(most_sexual)))

            ax4.barh(range(len(most_sexual)), most_sexual['sexual_score'],
                    color=colors, alpha=0.7, edgecolor='black')
            ax4.set_yticks(range(len(most_sexual)))
            ax4.set_yticklabels([t[:25] for t in most_sexual['title']], fontsize=8)
            ax4.invert_yaxis()
            ax4.set_xlabel('Sexual Content Keywords Count')
            ax4.set_title('Films with Most Sexual Content')
            ax4.grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'adult_content_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_family_friendly(self):
        """Identify family-friendly films."""
        print("Analyzing family-friendly content...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Family-Friendly Content Analysis', fontsize=16, fontweight='bold')

        # Classify films as family-friendly
        family_data = []
        for idx, row in self.movies_df.iterrows():
            keywords = row.get('tmdb_keywords', '')
            warnings = self.extract_warnings_from_keywords(keywords)
            mpaa = row.get('omdb_rated', 'Unrated')

            # Family-friendly criteria:
            # - G, PG, or TV-G, TV-PG rating
            # - No violence, sexual content, substance abuse warnings
            is_pg_rated = mpaa in ['G', 'PG', 'TV-G', 'TV-PG']
            has_warnings = len(warnings) > 0
            has_major_warnings = any(w in warnings for w in
                                    ['Violence', 'Sexual Content', 'Substance Abuse', 'Mental Health'])

            is_family_friendly = is_pg_rated and not has_major_warnings

            family_data.append({
                'title': row['Title'],
                'is_family_friendly': is_family_friendly,
                'is_pg_rated': is_pg_rated,
                'has_warnings': has_warnings,
                'warning_count': len(warnings),
                'rating': row.get('IMDb Rating', 0),
                'year': row.get('Year', 0)
            })

        family_df = pd.DataFrame(family_data)

        # Family-friendly distribution
        ax1 = axes[0, 0]

        ff_counts = family_df['is_family_friendly'].value_counts()
        labels = ['Not Family-Friendly', 'Family-Friendly']
        colors = ['red', 'green']
        explode = (0, 0.1)

        ax1.pie(ff_counts.values, labels=labels, autopct='%1.1f%%',
               colors=colors, explode=explode, startangle=90)
        ax1.set_title('Family-Friendly Film Distribution')

        # Top-rated family-friendly films
        ax2 = axes[0, 1]

        top_ff = family_df[family_df['is_family_friendly']].nlargest(15, 'rating')

        if len(top_ff) > 0:
            colors = plt.cm.Greens(np.linspace(0.4, 0.9, len(top_ff)))

            ax2.barh(range(len(top_ff)), top_ff['rating'], color=colors, alpha=0.7, edgecolor='black')
            ax2.set_yticks(range(len(top_ff)))
            ax2.set_yticklabels([t[:25] for t in top_ff['title']], fontsize=8)
            ax2.invert_yaxis()
            ax2.set_xlabel('IMDb Rating')
            ax2.set_title('Top-Rated Family-Friendly Films')
            ax2.grid(axis='x', alpha=0.3)

        # Family-friendly films by decade
        ax3 = axes[1, 0]

        decade_ff = {}
        for decade in range(1920, 2030, 10):
            decade_movies = family_df[
                (family_df['year'] >= decade) &
                (family_df['year'] < decade + 10)
            ]

            if len(decade_movies) > 0:
                pct = (decade_movies['is_family_friendly'].sum() / len(decade_movies)) * 100
                decade_ff[decade] = pct

        if decade_ff:
            decades = list(decade_ff.keys())
            percentages = list(decade_ff.values())
            decade_labels = [f"{d}s" for d in decades]

            ax3.plot(decade_labels, percentages, marker='o', linewidth=2, markersize=8, color='green')
            ax3.set_xlabel('Decade')
            ax3.set_ylabel('Family-Friendly Films (%)')
            ax3.set_title('Family-Friendly Content Over Time')
            ax3.grid(alpha=0.3)
            ax3.tick_params(axis='x', rotation=45)

        # Rating comparison
        ax4 = axes[1, 1]

        ff_ratings = family_df[family_df['is_family_friendly']]['rating']
        non_ff_ratings = family_df[~family_df['is_family_friendly']]['rating']

        ff_ratings = ff_ratings[ff_ratings > 0]
        non_ff_ratings = non_ff_ratings[non_ff_ratings > 0]

        if len(ff_ratings) > 0 and len(non_ff_ratings) > 0:
            bp = ax4.boxplot([ff_ratings, non_ff_ratings],
                            labels=['Family-Friendly', 'Not Family-Friendly'],
                            patch_artist=True)

            bp['boxes'][0].set_facecolor('lightgreen')
            bp['boxes'][1].set_facecolor('lightcoral')

            for patch in bp['boxes']:
                patch.set_alpha(0.7)

            ax4.set_ylabel('IMDb Rating')
            ax4.set_title('Rating Comparison')
            ax4.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'family_friendly_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_substance_abuse(self):
        """Analyze substance abuse content."""
        print("Analyzing substance abuse content...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Substance Abuse Content Analysis', fontsize=16, fontweight='bold')

        # Extract substance abuse data
        substance_data = []
        for idx, row in self.movies_df.iterrows():
            keywords = row.get('tmdb_keywords', '')
            warnings = self.extract_warnings_from_keywords(keywords)

            has_substance = 'Substance Abuse' in warnings
            substance_score = len(warnings.get('Substance Abuse', []))

            substance_data.append({
                'title': row['Title'],
                'has_substance': has_substance,
                'substance_score': substance_score,
                'rating': row.get('IMDb Rating', 0),
                'year': row.get('Year', 0),
                'genre': row.get('Genres', '').split('|')[0] if pd.notna(row.get('Genres')) else 'Unknown'
            })

        substance_df = pd.DataFrame(substance_data)

        # Presence distribution
        ax1 = axes[0, 0]

        presence_counts = substance_df['has_substance'].value_counts()
        labels = ['No Substance Abuse', 'Substance Abuse']
        colors = ['green', 'orange']

        ax1.pie(presence_counts.values, labels=labels, autopct='%1.1f%%',
               colors=colors, startangle=90)
        ax1.set_title('Substance Abuse Content Presence')

        # Substance abuse over time
        ax2 = axes[0, 1]

        decade_substance = {}
        for decade in range(1920, 2030, 10):
            decade_movies = substance_df[
                (substance_df['year'] >= decade) &
                (substance_df['year'] < decade + 10)
            ]

            if len(decade_movies) > 0:
                pct = (decade_movies['has_substance'].sum() / len(decade_movies)) * 100
                decade_substance[decade] = pct

        if decade_substance:
            decades = list(decade_substance.keys())
            percentages = list(decade_substance.values())
            decade_labels = [f"{d}s" for d in decades]

            ax2.plot(decade_labels, percentages, marker='o', linewidth=2, markersize=8, color='orange')
            ax2.set_xlabel('Decade')
            ax2.set_ylabel('Films with Substance Abuse (%)')
            ax2.set_title('Substance Abuse Content Over Time')
            ax2.grid(alpha=0.3)
            ax2.tick_params(axis='x', rotation=45)

        # Films with most substance abuse content
        ax3 = axes[1, 0]

        most_substance = substance_df[substance_df['substance_score'] > 0].nlargest(15, 'substance_score')

        if len(most_substance) > 0:
            colors = plt.cm.Oranges(np.linspace(0.4, 0.9, len(most_substance)))

            ax3.barh(range(len(most_substance)), most_substance['substance_score'],
                    color=colors, alpha=0.7, edgecolor='black')
            ax3.set_yticks(range(len(most_substance)))
            ax3.set_yticklabels([t[:25] for t in most_substance['title']], fontsize=8)
            ax3.invert_yaxis()
            ax3.set_xlabel('Substance Abuse Keywords Count')
            ax3.set_title('Films with Most Substance Abuse Content')
            ax3.grid(axis='x', alpha=0.3)

        # Substance abuse by genre
        ax4 = axes[1, 1]

        genre_substance = substance_df.groupby('genre').agg({
            'has_substance': ['sum', 'count']
        }).reset_index()

        genre_substance.columns = ['genre', 'has_count', 'total']
        genre_substance['pct'] = (genre_substance['has_count'] / genre_substance['total']) * 100
        genre_substance = genre_substance[genre_substance['total'] >= 10].nlargest(12, 'pct')

        colors = plt.cm.YlOrBr(genre_substance['pct'].values / 100)

        ax4.barh(range(len(genre_substance)), genre_substance['pct'],
                color=colors, alpha=0.7, edgecolor='black')
        ax4.set_yticks(range(len(genre_substance)))
        ax4.set_yticklabels([f"{g} (n={t})" for g, t in
                            zip(genre_substance['genre'], genre_substance['total'])], fontsize=9)
        ax4.invert_yaxis()
        ax4.set_xlabel('% with Substance Abuse Content')
        ax4.set_title('Substance Abuse by Genre')
        ax4.grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'substance_abuse_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_mental_health(self):
        """Analyze mental health content."""
        print("Analyzing mental health content...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Mental Health Content Analysis', fontsize=16, fontweight='bold')

        # Extract mental health data
        mental_health_data = []
        for idx, row in self.movies_df.iterrows():
            keywords = row.get('tmdb_keywords', '')
            warnings = self.extract_warnings_from_keywords(keywords)

            has_mental = 'Mental Health' in warnings
            mental_score = len(warnings.get('Mental Health', []))

            mental_health_data.append({
                'title': row['Title'],
                'has_mental': has_mental,
                'mental_score': mental_score,
                'rating': row.get('IMDb Rating', 0),
                'year': row.get('Year', 0),
                'genre': row.get('Genres', '').split('|')[0] if pd.notna(row.get('Genres')) else 'Unknown'
            })

        mental_df = pd.DataFrame(mental_health_data)

        # Presence distribution
        ax1 = axes[0, 0]

        presence_counts = mental_df['has_mental'].value_counts()
        labels = ['No Mental Health Content', 'Mental Health Content']
        colors = ['lightblue', 'purple']

        ax1.pie(presence_counts.values, labels=labels, autopct='%1.1f%%',
               colors=colors, startangle=90)
        ax1.set_title('Mental Health Content Presence')

        # Mental health content over time
        ax2 = axes[0, 1]

        decade_mental = {}
        for decade in range(1920, 2030, 10):
            decade_movies = mental_df[
                (mental_df['year'] >= decade) &
                (mental_df['year'] < decade + 10)
            ]

            if len(decade_movies) > 0:
                pct = (decade_movies['has_mental'].sum() / len(decade_movies)) * 100
                decade_mental[decade] = pct

        if decade_mental:
            decades = list(decade_mental.keys())
            percentages = list(decade_mental.values())
            decade_labels = [f"{d}s" for d in decades]

            ax2.plot(decade_labels, percentages, marker='o', linewidth=2, markersize=8, color='purple')
            ax2.set_xlabel('Decade')
            ax2.set_ylabel('Films with Mental Health Content (%)')
            ax2.set_title('Mental Health Content Over Time')
            ax2.grid(alpha=0.3)
            ax2.tick_params(axis='x', rotation=45)

        # Films with most mental health content
        ax3 = axes[1, 0]

        most_mental = mental_df[mental_df['mental_score'] > 0].nlargest(15, 'mental_score')

        if len(most_mental) > 0:
            colors = plt.cm.Purples(np.linspace(0.4, 0.9, len(most_mental)))

            ax3.barh(range(len(most_mental)), most_mental['mental_score'],
                    color=colors, alpha=0.7, edgecolor='black')
            ax3.set_yticks(range(len(most_mental)))
            ax3.set_yticklabels([t[:25] for t in most_mental['title']], fontsize=8)
            ax3.invert_yaxis()
            ax3.set_xlabel('Mental Health Keywords Count')
            ax3.set_title('Films with Most Mental Health Content')
            ax3.grid(axis='x', alpha=0.3)

        # Mental health vs rating
        ax4 = axes[1, 1]

        has_mental_ratings = mental_df[mental_df['has_mental']]['rating']
        no_mental_ratings = mental_df[~mental_df['has_mental']]['rating']

        has_mental_ratings = has_mental_ratings[has_mental_ratings > 0]
        no_mental_ratings = no_mental_ratings[no_mental_ratings > 0]

        if len(has_mental_ratings) > 0 and len(no_mental_ratings) > 0:
            bp = ax4.boxplot([has_mental_ratings, no_mental_ratings],
                            labels=['With Mental Health\nContent', 'Without Mental Health\nContent'],
                            patch_artist=True)

            bp['boxes'][0].set_facecolor('mediumpurple')
            bp['boxes'][1].set_facecolor('lightblue')

            for patch in bp['boxes']:
                patch.set_alpha(0.7)

            ax4.set_ylabel('IMDb Rating')
            ax4.set_title('Rating Comparison')
            ax4.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'mental_health_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_animal_content(self):
        """Analyze animal-related content."""
        print("Analyzing animal content...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Animal Content Analysis', fontsize=16, fontweight='bold')

        # Extract animal content data
        animal_data = []
        for idx, row in self.movies_df.iterrows():
            keywords = row.get('tmdb_keywords', '')
            warnings = self.extract_warnings_from_keywords(keywords)

            has_animal = 'Animal Content' in warnings
            animal_score = len(warnings.get('Animal Content', []))

            animal_data.append({
                'title': row['Title'],
                'has_animal': has_animal,
                'animal_score': animal_score,
                'rating': row.get('IMDb Rating', 0),
                'year': row.get('Year', 0)
            })

        animal_df = pd.DataFrame(animal_data)

        # Presence distribution
        ax1 = axes[0, 0]

        presence_counts = animal_df['has_animal'].value_counts()
        labels = ['No Animal Content', 'Animal Content']
        colors = ['lightgray', 'brown']

        ax1.pie(presence_counts.values, labels=labels, autopct='%1.1f%%',
               colors=colors, startangle=90)
        ax1.set_title('Animal Content Presence')

        # Animal content over time
        ax2 = axes[0, 1]

        decade_animal = {}
        for decade in range(1920, 2030, 10):
            decade_movies = animal_df[
                (animal_df['year'] >= decade) &
                (animal_df['year'] < decade + 10)
            ]

            if len(decade_movies) > 0:
                pct = (decade_movies['has_animal'].sum() / len(decade_movies)) * 100
                decade_animal[decade] = pct

        if decade_animal:
            decades = list(decade_animal.keys())
            percentages = list(decade_animal.values())
            decade_labels = [f"{d}s" for d in decades]

            ax2.plot(decade_labels, percentages, marker='o', linewidth=2, markersize=8, color='brown')
            ax2.set_xlabel('Decade')
            ax2.set_ylabel('Films with Animal Content (%)')
            ax2.set_title('Animal Content Over Time')
            ax2.grid(alpha=0.3)
            ax2.tick_params(axis='x', rotation=45)

        # Films with animal content
        ax3 = axes[1, 0]

        has_animal = animal_df[animal_df['has_animal']].nlargest(15, 'rating')

        if len(has_animal) > 0:
            colors = plt.cm.YlOrBr(np.linspace(0.4, 0.9, len(has_animal)))

            ax3.barh(range(len(has_animal)), has_animal['rating'],
                    color=colors, alpha=0.7, edgecolor='black')
            ax3.set_yticks(range(len(has_animal)))
            ax3.set_yticklabels([t[:25] for t in has_animal['title']], fontsize=8)
            ax3.invert_yaxis()
            ax3.set_xlabel('IMDb Rating')
            ax3.set_title('Top-Rated Films with Animal Content')
            ax3.grid(axis='x', alpha=0.3)

        # Animal score distribution
        ax4 = axes[1, 1]

        animal_with_content = animal_df[animal_df['animal_score'] > 0]

        if len(animal_with_content) > 0:
            ax4.hist(animal_with_content['animal_score'],
                    bins=range(1, animal_with_content['animal_score'].max() + 2),
                    color='brown', alpha=0.7, edgecolor='black')
            ax4.set_xlabel('Animal Keywords Count')
            ax4.set_ylabel('Number of Films')
            ax4.set_title('Distribution of Animal Content Intensity')
            ax4.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'animal_content_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_genre_content_profiles(self):
        """Analyze content warning patterns by genre."""
        print("Creating genre content profiles...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Genre Content Warning Profiles', fontsize=16, fontweight='bold')

        # Build genre-warning matrix
        genre_warnings = defaultdict(lambda: defaultdict(int))

        for idx, row in self.movies_df.iterrows():
            genres_str = row.get('Genres', '')
            keywords = row.get('tmdb_keywords', '')
            warnings = self.extract_warnings_from_keywords(keywords)

            if pd.notna(genres_str):
                genres = [g.strip() for g in str(genres_str).split('|')]

                for genre in genres:
                    if genre:
                        genre_warnings[genre]['total'] += 1
                        for warning_cat in warnings.keys():
                            genre_warnings[genre][warning_cat] += 1

        # Get top genres
        top_genres = sorted(genre_warnings.keys(),
                          key=lambda x: genre_warnings[x]['total'], reverse=True)[:12]

        # Top warning categories
        warning_cats = ['Violence', 'Sexual Content', 'Substance Abuse', 'Mental Health',
                       'Crime', 'Disturbing Themes']

        # Create percentage matrix
        matrix = np.zeros((len(top_genres), len(warning_cats)))

        for i, genre in enumerate(top_genres):
            total = genre_warnings[genre]['total']
            for j, warning in enumerate(warning_cats):
                count = genre_warnings[genre][warning]
                matrix[i, j] = (count / total * 100) if total > 0 else 0

        # Heatmap
        ax1 = axes[0, 0]

        im = ax1.imshow(matrix, cmap='YlOrRd', aspect='auto')
        ax1.set_xticks(range(len(warning_cats)))
        ax1.set_yticks(range(len(top_genres)))
        ax1.set_xticklabels([w[:12] for w in warning_cats], rotation=45, ha='right', fontsize=8)
        ax1.set_yticklabels(top_genres, fontsize=9)
        ax1.set_title('Warning Prevalence by Genre (%)')
        plt.colorbar(im, ax=ax1)

        # Violence by genre
        ax2 = axes[0, 1]

        violence_by_genre = []
        for genre in top_genres:
            total = genre_warnings[genre]['total']
            violence = genre_warnings[genre]['Violence']
            pct = (violence / total * 100) if total > 0 else 0
            violence_by_genre.append(pct)

        colors = plt.cm.Reds(np.array(violence_by_genre) / 100)

        ax2.barh(range(len(top_genres)), violence_by_genre, color=colors, alpha=0.7, edgecolor='black')
        ax2.set_yticks(range(len(top_genres)))
        ax2.set_yticklabels(top_genres, fontsize=9)
        ax2.invert_yaxis()
        ax2.set_xlabel('% with Violence')
        ax2.set_title('Violence Prevalence by Genre')
        ax2.grid(axis='x', alpha=0.3)

        # Sexual content by genre
        ax3 = axes[1, 0]

        sexual_by_genre = []
        for genre in top_genres:
            total = genre_warnings[genre]['total']
            sexual = genre_warnings[genre]['Sexual Content']
            pct = (sexual / total * 100) if total > 0 else 0
            sexual_by_genre.append(pct)

        colors = plt.cm.Purples(np.array(sexual_by_genre) / 100)

        ax3.barh(range(len(top_genres)), sexual_by_genre, color=colors, alpha=0.7, edgecolor='black')
        ax3.set_yticks(range(len(top_genres)))
        ax3.set_yticklabels(top_genres, fontsize=9)
        ax3.invert_yaxis()
        ax3.set_xlabel('% with Sexual Content')
        ax3.set_title('Sexual Content Prevalence by Genre')
        ax3.grid(axis='x', alpha=0.3)

        # Overall warning intensity by genre
        ax4 = axes[1, 1]

        total_warnings_by_genre = []
        for genre in top_genres:
            total = genre_warnings[genre]['total']
            all_warnings = sum(genre_warnings[genre][w] for w in warning_cats)
            avg_warnings = (all_warnings / total) if total > 0 else 0
            total_warnings_by_genre.append(avg_warnings)

        colors = plt.cm.YlOrRd(np.array(total_warnings_by_genre) / max(total_warnings_by_genre))

        ax4.barh(range(len(top_genres)), total_warnings_by_genre, color=colors, alpha=0.7, edgecolor='black')
        ax4.set_yticks(range(len(top_genres)))
        ax4.set_yticklabels(top_genres, fontsize=9)
        ax4.invert_yaxis()
        ax4.set_xlabel('Average Warning Categories per Film')
        ax4.set_title('Overall Content Warning Intensity by Genre')
        ax4.grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'genre_content_profiles.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_ddd_coverage(self):
        """Analyze DoesTheDogDie data coverage."""
        print("Analyzing DDD coverage...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Content Warning Data Coverage', fontsize=16, fontweight='bold')

        # DDD ID coverage
        ax1 = axes[0, 0]

        ddd_coverage = {
            'With DDD ID': self.movies_df['ddd_id'].notna().sum(),
            'Without DDD ID': self.movies_df['ddd_id'].isna().sum()
        }

        colors = ['green', 'red']
        ax1.pie(ddd_coverage.values(), labels=ddd_coverage.keys(), autopct='%1.1f%%',
               colors=colors, startangle=90)
        ax1.set_title('DDD ID Coverage')

        # MPAA rating coverage
        ax2 = axes[0, 1]

        mpaa_coverage = {
            'With Rating': self.movies_df['omdb_rated'].notna().sum(),
            'Without Rating': self.movies_df['omdb_rated'].isna().sum()
        }

        colors = ['blue', 'gray']
        ax2.pie(mpaa_coverage.values(), labels=mpaa_coverage.keys(), autopct='%1.1f%%',
               colors=colors, startangle=90)
        ax2.set_title('MPAA Rating Coverage')

        # Keywords coverage
        ax3 = axes[1, 0]

        keywords_coverage = {
            'With Keywords': self.movies_df['tmdb_keywords'].notna().sum(),
            'Without Keywords': self.movies_df['tmdb_keywords'].isna().sum()
        }

        colors = ['purple', 'lightgray']
        ax3.pie(keywords_coverage.values(), labels=keywords_coverage.keys(), autopct='%1.1f%%',
               colors=colors, startangle=90)
        ax3.set_title('Keywords Coverage')

        # Coverage statistics table
        ax4 = axes[1, 1]
        ax4.axis('off')

        coverage_stats = [
            ['Data Source', 'Coverage', 'Count'],
            ['─' * 20, '─' * 10, '─' * 10],
            ['DDD IDs', f"{(ddd_coverage['With DDD ID']/len(self.movies_df)*100):.1f}%",
             f"{ddd_coverage['With DDD ID']}/{len(self.movies_df)}"],
            ['MPAA Ratings', f"{(mpaa_coverage['With Rating']/len(self.movies_df)*100):.1f}%",
             f"{mpaa_coverage['With Rating']}/{len(self.movies_df)}"],
            ['Keywords', f"{(keywords_coverage['With Keywords']/len(self.movies_df)*100):.1f}%",
             f"{keywords_coverage['With Keywords']}/{len(self.movies_df)}"],
        ]

        ax4.text(0.5, 0.95, 'Content Warning Data Coverage Summary',
                ha='center', va='top', fontsize=14, fontweight='bold',
                transform=ax4.transAxes)

        for i, row in enumerate(coverage_stats):
            y_pos = 0.85 - i * 0.12
            ax4.text(0.1, y_pos, row[0], fontsize=10, transform=ax4.transAxes,
                    fontweight='bold' if i == 0 else 'normal')
            ax4.text(0.5, y_pos, row[1], fontsize=10, transform=ax4.transAxes, ha='center',
                    fontweight='bold' if i == 0 else 'normal')
            ax4.text(0.8, y_pos, row[2], fontsize=10, transform=ax4.transAxes, ha='center',
                    fontweight='bold' if i == 0 else 'normal')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'coverage_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_rating_warning_correlation(self):
        """Analyze correlation between MPAA ratings and content warnings."""
        print("Analyzing rating-warning correlation...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('MPAA Rating vs Content Warnings', fontsize=16, fontweight='bold')

        # Build correlation data
        rating_warning_data = []

        for idx, row in self.movies_df.iterrows():
            mpaa = row.get('omdb_rated', 'Unrated')
            keywords = row.get('tmdb_keywords', '')
            warnings = self.extract_warnings_from_keywords(keywords)

            rating_warning_data.append({
                'mpaa': mpaa,
                'rating_category': self.categorize_mpaa_rating(mpaa),
                'warning_count': len(warnings),
                'has_violence': 'Violence' in warnings,
                'has_sexual': 'Sexual Content' in warnings,
                'has_substance': 'Substance Abuse' in warnings
            })

        rw_df = pd.DataFrame(rating_warning_data)

        # Warning count by rating category
        ax1 = axes[0, 0]

        category_warnings = rw_df.groupby('rating_category')['warning_count'].mean().sort_values(ascending=False)
        category_warnings = category_warnings.head(6)

        colors = plt.cm.YlOrRd(category_warnings.values / category_warnings.max())

        ax1.barh(range(len(category_warnings)), category_warnings.values,
                color=colors, alpha=0.7, edgecolor='black')
        ax1.set_yticks(range(len(category_warnings)))
        ax1.set_yticklabels(category_warnings.index, fontsize=9)
        ax1.invert_yaxis()
        ax1.set_xlabel('Average Warning Categories per Film')
        ax1.set_title('Content Warnings by MPAA Rating')
        ax1.grid(axis='x', alpha=0.3)

        # Violence by rating
        ax2 = axes[0, 1]

        violence_by_rating = rw_df.groupby('rating_category')['has_violence'].mean() * 100
        violence_by_rating = violence_by_rating.sort_values(ascending=False).head(6)

        colors = plt.cm.Reds(violence_by_rating.values / 100)

        ax2.barh(range(len(violence_by_rating)), violence_by_rating.values,
                color=colors, alpha=0.7, edgecolor='black')
        ax2.set_yticks(range(len(violence_by_rating)))
        ax2.set_yticklabels(violence_by_rating.index, fontsize=9)
        ax2.invert_yaxis()
        ax2.set_xlabel('% with Violence')
        ax2.set_title('Violence Prevalence by Rating')
        ax2.grid(axis='x', alpha=0.3)

        # Sexual content by rating
        ax3 = axes[1, 0]

        sexual_by_rating = rw_df.groupby('rating_category')['has_sexual'].mean() * 100
        sexual_by_rating = sexual_by_rating.sort_values(ascending=False).head(6)

        colors = plt.cm.Purples(sexual_by_rating.values / 100)

        ax3.barh(range(len(sexual_by_rating)), sexual_by_rating.values,
                color=colors, alpha=0.7, edgecolor='black')
        ax3.set_yticks(range(len(sexual_by_rating)))
        ax3.set_yticklabels(sexual_by_rating.index, fontsize=9)
        ax3.invert_yaxis()
        ax3.set_xlabel('% with Sexual Content')
        ax3.set_title('Sexual Content Prevalence by Rating')
        ax3.grid(axis='x', alpha=0.3)

        # Substance abuse by rating
        ax4 = axes[1, 1]

        substance_by_rating = rw_df.groupby('rating_category')['has_substance'].mean() * 100
        substance_by_rating = substance_by_rating.sort_values(ascending=False).head(6)

        colors = plt.cm.Oranges(substance_by_rating.values / 100)

        ax4.barh(range(len(substance_by_rating)), substance_by_rating.values,
                color=colors, alpha=0.7, edgecolor='black')
        ax4.set_yticks(range(len(substance_by_rating)))
        ax4.set_yticklabels(substance_by_rating.index, fontsize=9)
        ax4.invert_yaxis()
        ax4.set_xlabel('% with Substance Abuse')
        ax4.set_title('Substance Abuse Prevalence by Rating')
        ax4.grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'rating_warning_correlation.png', dpi=300, bbox_inches='tight')
        plt.close()

    def generate_report(self):
        """Generate comprehensive analysis report."""
        print("\nGenerating comprehensive report...")

        report_path = self.reports_dir / 'batch_27_content_warnings_report.txt'

        # Calculate statistics
        total_films = len(self.movies_df)
        ddd_coverage = self.movies_df['ddd_id'].notna().sum()
        mpaa_coverage = self.movies_df['omdb_rated'].notna().sum()
        keywords_coverage = self.movies_df['tmdb_keywords'].notna().sum()

        # Count warnings
        warning_counts = {cat: 0 for cat in self.warning_categories.keys()}
        for idx, row in self.movies_df.iterrows():
            keywords = row.get('tmdb_keywords', '')
            warnings = self.extract_warnings_from_keywords(keywords)
            for cat in warnings.keys():
                warning_counts[cat] += 1

        with open(report_path, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("CINESCOPE BATCH 27: CONTENT WARNINGS DEEP-DIVE\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # Data coverage
            f.write("=" * 80 + "\n")
            f.write("DATA COVERAGE\n")
            f.write("=" * 80 + "\n\n")

            f.write(f"Total Films: {total_films}\n\n")

            f.write(f"DDD IDs: {ddd_coverage}/{total_films} ({ddd_coverage/total_films*100:.1f}%)\n")
            f.write(f"MPAA Ratings: {mpaa_coverage}/{total_films} ({mpaa_coverage/total_films*100:.1f}%)\n")
            f.write(f"Keywords: {keywords_coverage}/{total_films} ({keywords_coverage/total_films*100:.1f}%)\n\n")

            # MPAA distribution
            f.write("=" * 80 + "\n")
            f.write("MPAA RATING DISTRIBUTION\n")
            f.write("=" * 80 + "\n\n")

            rating_counts = self.movies_df['omdb_rated'].value_counts().head(10)
            for rating, count in rating_counts.items():
                pct = (count / total_films) * 100
                f.write(f"{rating}: {count} ({pct:.1f}%)\n")

            # Content warnings
            f.write("\n" + "=" * 80 + "\n")
            f.write("CONTENT WARNING PREVALENCE\n")
            f.write("=" * 80 + "\n\n")

            sorted_warnings = sorted(warning_counts.items(), key=lambda x: x[1], reverse=True)
            for category, count in sorted_warnings:
                pct = (count / total_films) * 100
                f.write(f"{category}: {count} ({pct:.1f}%)\n")

            f.write("\n" + "=" * 80 + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * 80 + "\n")

        print(f"Report saved to {report_path}")

    def run_analysis(self):
        """Execute all analysis steps."""
        print("\n" + "=" * 80)
        print("CINESCOPE BATCH 27: CONTENT WARNINGS DEEP-DIVE")
        print("=" * 80 + "\n")

        # Run all visualizations
        self.visualize_mpaa_distribution()
        self.visualize_content_warning_heatmap()
        self.visualize_violence_analysis()
        self.visualize_adult_content()
        self.visualize_family_friendly()
        self.visualize_substance_abuse()
        self.visualize_mental_health()
        self.visualize_animal_content()
        self.visualize_genre_content_profiles()
        self.visualize_ddd_coverage()
        self.visualize_rating_warning_correlation()

        # Generate report
        self.generate_report()

        print("\n" + "=" * 80)
        print("ANALYSIS COMPLETE")
        print("=" * 80)
        print(f"\nVisualizations saved to: {self.viz_dir}")
        print(f"Report saved to: {self.reports_dir}")

if __name__ == "__main__":
    analyzer = ContentWarningsAnalyzer()
    analyzer.run_analysis()
