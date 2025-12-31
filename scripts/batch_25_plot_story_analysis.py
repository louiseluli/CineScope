#!/usr/bin/env python3
"""
CineScope Batch 25: Plot & Story Analysis
Analyzes plot summaries, narrative complexity, and story structure patterns.
"""

import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime
from collections import Counter
import re
from scipy import stats

class PlotStoryAnalyzer:
    def __init__(self):
        self.data_dir = Path('data/processed')
        self.output_dir = Path('analysis_outputs')
        self.viz_dir = self.output_dir / 'visualizations' / 'batch_25'
        self.reports_dir = self.output_dir / 'reports'

        # Create output directories
        self.viz_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)

        # Load data
        self.movies_df = pd.read_csv(self.data_dir / 'watched_movies_master.csv')

        # Setup plotting style
        plt.style.use('default')
        sns.set_palette("husl")

        print(f"Loaded {len(self.movies_df)} movies for plot analysis")

    def clean_text(self, text):
        """Clean and normalize text data."""
        if pd.isna(text) or str(text).strip() in ['', 'N/A', 'nan']:
            return None
        return str(text).strip()

    def count_words(self, text):
        """Count words in text."""
        if not text:
            return 0
        # Remove special characters and split
        words = re.findall(r'\b\w+\b', text.lower())
        return len(words)

    def extract_keywords(self, text, top_n=100):
        """Extract common keywords from text."""
        if not text:
            return []

        # Common stop words to exclude
        stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
            'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'been',
            'be', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
            'could', 'should', 'may', 'might', 'must', 'can', 'his', 'her', 'their',
            'them', 'they', 'he', 'she', 'it', 'this', 'that', 'these', 'those',
            'when', 'where', 'who', 'what', 'why', 'how', 'all', 'each', 'every',
            'both', 'few', 'more', 'most', 'some', 'such', 'no', 'nor', 'not',
            'only', 'own', 'same', 'so', 'than', 'too', 'very', 'just', 'after',
            'before', 'into', 'through', 'during', 'about', 'against', 'between'
        }

        words = re.findall(r'\b\w+\b', text.lower())
        filtered_words = [w for w in words if w not in stop_words and len(w) > 3]
        return filtered_words

    def visualize_plot_coverage(self):
        """Analyze coverage and availability of plot data."""
        print("Analyzing plot data coverage...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Plot Data Coverage Analysis', fontsize=16, fontweight='bold')

        # Column availability
        plot_columns = {
            'Description': 'Description',
            'TMDB Overview': 'tmdb_overview',
            'OMDB Plot': 'omdb_plot',
            'TMDB Tagline': 'tmdb_tagline'
        }

        coverage_data = {}
        for label, col in plot_columns.items():
            if col in self.movies_df.columns:
                non_null = self.movies_df[col].notna().sum()
                non_empty = sum(self.movies_df[col].apply(
                    lambda x: bool(self.clean_text(x))
                ))
                coverage_data[label] = {
                    'available': non_empty,
                    'missing': len(self.movies_df) - non_empty
                }

        # Coverage bar chart
        ax1 = axes[0, 0]
        labels = list(coverage_data.keys())
        available = [coverage_data[l]['available'] for l in labels]
        missing = [coverage_data[l]['missing'] for l in labels]

        x = np.arange(len(labels))
        width = 0.35

        ax1.bar(x - width/2, available, width, label='Available', color='green', alpha=0.7)
        ax1.bar(x + width/2, missing, width, label='Missing', color='red', alpha=0.7)
        ax1.set_xlabel('Data Source')
        ax1.set_ylabel('Number of Films')
        ax1.set_title('Plot Data Availability by Source')
        ax1.set_xticks(x)
        ax1.set_xticklabels(labels, rotation=45, ha='right')
        ax1.legend()
        ax1.grid(axis='y', alpha=0.3)

        # Coverage percentages
        ax2 = axes[0, 1]
        percentages = [(coverage_data[l]['available'] / len(self.movies_df) * 100)
                      for l in labels]
        colors = ['green' if p > 80 else 'orange' if p > 50 else 'red' for p in percentages]

        ax2.barh(labels, percentages, color=colors, alpha=0.7)
        ax2.set_xlabel('Coverage Percentage (%)')
        ax2.set_title('Plot Data Coverage Rates')
        ax2.grid(axis='x', alpha=0.3)

        for i, (label, pct) in enumerate(zip(labels, percentages)):
            ax2.text(pct + 1, i, f'{pct:.1f}%', va='center')

        # Combined coverage (any plot data available)
        ax3 = axes[1, 0]

        any_plot = self.movies_df.apply(
            lambda row: any(
                bool(self.clean_text(row.get(col, None)))
                for col in plot_columns.values()
                if col in self.movies_df.columns
            ),
            axis=1
        )

        coverage_counts = any_plot.value_counts()

        # Map True/False to labels
        pie_data = []
        pie_labels = []
        pie_colors = []

        if True in coverage_counts.index:
            pie_data.append(coverage_counts[True])
            pie_labels.append('Has Plot Data')
            pie_colors.append('green')

        if False in coverage_counts.index:
            pie_data.append(coverage_counts[False])
            pie_labels.append('No Plot Data')
            pie_colors.append('red')

        ax3.pie(pie_data, labels=pie_labels, autopct='%1.1f%%',
               colors=pie_colors, startangle=90)
        ax3.set_title('Overall Plot Data Availability')

        # Plot data by decade
        ax4 = axes[1, 1]

        decade_coverage = []
        decades = []

        for decade in range(1920, 2030, 10):
            decade_movies = self.movies_df[
                (self.movies_df['Year'] >= decade) &
                (self.movies_df['Year'] < decade + 10)
            ]

            if len(decade_movies) > 0:
                has_plot = decade_movies.apply(
                    lambda row: any(
                        bool(self.clean_text(row.get(col, None)))
                        for col in plot_columns.values()
                        if col in self.movies_df.columns
                    ),
                    axis=1
                ).sum()

                coverage_pct = (has_plot / len(decade_movies)) * 100
                decade_coverage.append(coverage_pct)
                decades.append(f"{decade}s")

        ax4.plot(decades, decade_coverage, marker='o', linewidth=2, markersize=8)
        ax4.set_xlabel('Decade')
        ax4.set_ylabel('Coverage (%)')
        ax4.set_title('Plot Data Coverage by Decade')
        ax4.grid(alpha=0.3)
        ax4.tick_params(axis='x', rotation=45)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'plot_data_coverage.png', dpi=300, bbox_inches='tight')
        plt.close()

        return coverage_data

    def visualize_plot_length(self):
        """Analyze plot description length patterns."""
        print("Analyzing plot length distributions...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Plot Description Length Analysis', fontsize=16, fontweight='bold')

        # Calculate word counts for different sources
        plot_sources = {
            'Description': 'Description',
            'TMDB Overview': 'tmdb_overview',
            'OMDB Plot': 'omdb_plot'
        }

        all_lengths = {}
        for label, col in plot_sources.items():
            if col in self.movies_df.columns:
                lengths = []
                for text in self.movies_df[col]:
                    cleaned = self.clean_text(text)
                    if cleaned:
                        lengths.append(self.count_words(cleaned))
                all_lengths[label] = lengths

        if not all_lengths:
            self._create_insufficient_data_plot(fig, "Insufficient plot text data")
            plt.savefig(self.viz_dir / 'plot_length_analysis.png', dpi=300, bbox_inches='tight')
            plt.close()
            return

        # Distribution comparison
        ax1 = axes[0, 0]
        for label, lengths in all_lengths.items():
            if lengths:
                ax1.hist(lengths, bins=30, alpha=0.5, label=label, edgecolor='black')

        ax1.set_xlabel('Word Count')
        ax1.set_ylabel('Frequency')
        ax1.set_title('Plot Length Distribution by Source')
        ax1.legend()
        ax1.grid(alpha=0.3)

        # Box plot comparison
        ax2 = axes[0, 1]
        data_to_plot = [lengths for lengths in all_lengths.values() if lengths]
        labels_to_plot = [label for label, lengths in all_lengths.items() if lengths]

        bp = ax2.boxplot(data_to_plot, tick_labels=labels_to_plot, patch_artist=True)
        for patch in bp['boxes']:
            patch.set_facecolor('lightblue')
            patch.set_alpha(0.7)

        ax2.set_ylabel('Word Count')
        ax2.set_title('Plot Length Statistics by Source')
        ax2.grid(axis='y', alpha=0.3)
        ax2.tick_params(axis='x', rotation=45)

        # Plot length vs rating
        ax3 = axes[1, 0]

        # Use best available plot source
        primary_col = None
        for col in ['tmdb_overview', 'omdb_plot', 'Description']:
            if col in self.movies_df.columns:
                primary_col = col
                break

        if primary_col:
            plot_lengths = []
            ratings = []

            for idx, row in self.movies_df.iterrows():
                text = self.clean_text(row.get(primary_col, None))
                rating = row.get('IMDb Rating', None)

                if text and pd.notna(rating):
                    try:
                        rating_val = float(rating)
                        if 0 <= rating_val <= 10:
                            plot_lengths.append(self.count_words(text))
                            ratings.append(rating_val)
                    except:
                        pass

            if len(plot_lengths) > 10:
                ax3.scatter(plot_lengths, ratings, alpha=0.3)

                # Add trend line
                if len(plot_lengths) > 20:
                    z = np.polyfit(plot_lengths, ratings, 1)
                    p = np.poly1d(z)
                    ax3.plot(sorted(plot_lengths), p(sorted(plot_lengths)),
                            "r--", linewidth=2, label='Trend')

                    # Calculate correlation
                    corr, p_value = stats.pearsonr(plot_lengths, ratings)
                    ax3.text(0.05, 0.95, f'Correlation: {corr:.3f}\np-value: {p_value:.4f}',
                            transform=ax3.transAxes, verticalalignment='top',
                            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

                ax3.set_xlabel('Plot Length (words)')
                ax3.set_ylabel('IMDb Rating')
                ax3.set_title('Plot Length vs Film Rating')
                ax3.grid(alpha=0.3)
                ax3.legend()

        # Plot length by genre
        ax4 = axes[1, 1]

        if primary_col and 'Genres' in self.movies_df.columns:
            genre_lengths = {}

            for idx, row in self.movies_df.iterrows():
                text = self.clean_text(row.get(primary_col, None))
                genres = row.get('Genres', '')

                if text and pd.notna(genres):
                    word_count = self.count_words(text)
                    for genre in str(genres).split('|'):
                        genre = genre.strip()
                        if genre:
                            if genre not in genre_lengths:
                                genre_lengths[genre] = []
                            genre_lengths[genre].append(word_count)

            # Get top genres by count
            top_genres = sorted(genre_lengths.items(),
                              key=lambda x: len(x[1]), reverse=True)[:10]

            if top_genres:
                genre_names = [g[0] for g in top_genres]
                genre_data = [g[1] for g in top_genres]

                bp = ax4.boxplot(genre_data, tick_labels=genre_names, patch_artist=True)
                for patch in bp['boxes']:
                    patch.set_facecolor('lightgreen')
                    patch.set_alpha(0.7)

                ax4.set_ylabel('Plot Length (words)')
                ax4.set_title('Plot Length by Genre (Top 10)')
                ax4.grid(axis='y', alpha=0.3)
                ax4.tick_params(axis='x', rotation=45)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'plot_length_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_common_keywords(self):
        """Analyze most common keywords in plot descriptions."""
        print("Analyzing common plot keywords...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Plot Keywords Analysis', fontsize=16, fontweight='bold')

        # Collect all keywords
        primary_col = None
        for col in ['tmdb_overview', 'omdb_plot', 'Description']:
            if col in self.movies_df.columns:
                primary_col = col
                break

        if not primary_col:
            self._create_insufficient_data_plot(fig, "Insufficient plot text data")
            plt.savefig(self.viz_dir / 'common_keywords.png', dpi=300, bbox_inches='tight')
            plt.close()
            return

        all_keywords = []
        for text in self.movies_df[primary_col]:
            cleaned = self.clean_text(text)
            if cleaned:
                all_keywords.extend(self.extract_keywords(cleaned))

        if not all_keywords:
            self._create_insufficient_data_plot(fig, "No keywords extracted")
            plt.savefig(self.viz_dir / 'common_keywords.png', dpi=300, bbox_inches='tight')
            plt.close()
            return

        keyword_counts = Counter(all_keywords)

        # Top 20 overall keywords
        ax1 = axes[0, 0]
        top_keywords = keyword_counts.most_common(20)
        words, counts = zip(*top_keywords)

        ax1.barh(range(len(words)), counts, color='steelblue', alpha=0.7)
        ax1.set_yticks(range(len(words)))
        ax1.set_yticklabels(words)
        ax1.invert_yaxis()
        ax1.set_xlabel('Frequency')
        ax1.set_title('Top 20 Most Common Plot Keywords')
        ax1.grid(axis='x', alpha=0.3)

        # Action words
        ax2 = axes[0, 1]
        action_words = ['find', 'must', 'help', 'save', 'fight', 'kill', 'escape',
                       'discover', 'search', 'survive', 'become', 'revenge', 'face',
                       'journey', 'battle', 'struggle', 'return', 'lost', 'seek']

        action_counts = [(word, keyword_counts.get(word, 0)) for word in action_words]
        action_counts = sorted(action_counts, key=lambda x: x[1], reverse=True)[:15]

        if action_counts:
            words, counts = zip(*action_counts)
            ax2.barh(range(len(words)), counts, color='coral', alpha=0.7)
            ax2.set_yticks(range(len(words)))
            ax2.set_yticklabels(words)
            ax2.invert_yaxis()
            ax2.set_xlabel('Frequency')
            ax2.set_title('Common Action/Plot Driver Words')
            ax2.grid(axis='x', alpha=0.3)

        # Character descriptors
        ax3 = axes[1, 0]
        character_words = ['young', 'family', 'life', 'love', 'friend', 'father',
                          'mother', 'woman', 'detective', 'police', 'criminal',
                          'doctor', 'teacher', 'soldier', 'hero', 'villain']

        char_counts = [(word, keyword_counts.get(word, 0)) for word in character_words]
        char_counts = sorted(char_counts, key=lambda x: x[1], reverse=True)[:15]

        if char_counts:
            words, counts = zip(*char_counts)
            ax3.barh(range(len(words)), counts, color='mediumseagreen', alpha=0.7)
            ax3.set_yticks(range(len(words)))
            ax3.set_yticklabels(words)
            ax3.invert_yaxis()
            ax3.set_xlabel('Frequency')
            ax3.set_title('Common Character/Relationship Words')
            ax3.grid(axis='x', alpha=0.3)

        # Setting/location words
        ax4 = axes[1, 1]
        setting_words = ['world', 'town', 'city', 'home', 'school', 'house',
                        'night', 'death', 'past', 'future', 'secret', 'truth',
                        'dead', 'dark', 'strange', 'mysterious']

        setting_counts = [(word, keyword_counts.get(word, 0)) for word in setting_words]
        setting_counts = sorted(setting_counts, key=lambda x: x[1], reverse=True)[:15]

        if setting_counts:
            words, counts = zip(*setting_counts)
            ax4.barh(range(len(words)), counts, color='mediumpurple', alpha=0.7)
            ax4.set_yticks(range(len(words)))
            ax4.set_yticklabels(words)
            ax4.invert_yaxis()
            ax4.set_xlabel('Frequency')
            ax4.set_title('Common Setting/Theme Words')
            ax4.grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'common_keywords.png', dpi=300, bbox_inches='tight')
        plt.close()

        return keyword_counts

    def visualize_tagline_analysis(self):
        """Analyze tagline characteristics."""
        print("Analyzing taglines...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Tagline Analysis', fontsize=16, fontweight='bold')

        if 'tmdb_tagline' not in self.movies_df.columns:
            self._create_insufficient_data_plot(fig, "No tagline data available")
            plt.savefig(self.viz_dir / 'tagline_analysis.png', dpi=300, bbox_inches='tight')
            plt.close()
            return

        # Tagline length distribution
        ax1 = axes[0, 0]

        tagline_lengths = []
        for tagline in self.movies_df['tmdb_tagline']:
            cleaned = self.clean_text(tagline)
            if cleaned:
                tagline_lengths.append(self.count_words(cleaned))

        if tagline_lengths:
            ax1.hist(tagline_lengths, bins=30, color='skyblue', edgecolor='black', alpha=0.7)
            ax1.axvline(np.median(tagline_lengths), color='red', linestyle='--',
                       linewidth=2, label=f'Median: {np.median(tagline_lengths):.1f}')
            ax1.set_xlabel('Word Count')
            ax1.set_ylabel('Frequency')
            ax1.set_title('Tagline Length Distribution')
            ax1.legend()
            ax1.grid(alpha=0.3)

        # Tagline length by decade
        ax2 = axes[0, 1]

        decade_taglines = {}
        for idx, row in self.movies_df.iterrows():
            year = row.get('Year', None)
            tagline = self.clean_text(row.get('tmdb_tagline', None))

            if pd.notna(year) and tagline:
                try:
                    decade = (int(year) // 10) * 10
                    if decade not in decade_taglines:
                        decade_taglines[decade] = []
                    decade_taglines[decade].append(self.count_words(tagline))
                except:
                    pass

        if decade_taglines:
            sorted_decades = sorted(decade_taglines.keys())
            decade_labels = [f"{d}s" for d in sorted_decades]
            decade_data = [decade_taglines[d] for d in sorted_decades]

            bp = ax2.boxplot(decade_data, tick_labels=decade_labels, patch_artist=True)
            for patch in bp['boxes']:
                patch.set_facecolor('lightcoral')
                patch.set_alpha(0.7)

            ax2.set_xlabel('Decade')
            ax2.set_ylabel('Word Count')
            ax2.set_title('Tagline Length Evolution')
            ax2.grid(axis='y', alpha=0.3)
            ax2.tick_params(axis='x', rotation=45)

        # Tagline presence by genre
        ax3 = axes[1, 0]

        if 'Genres' in self.movies_df.columns:
            genre_tagline_counts = {}

            for idx, row in self.movies_df.iterrows():
                genres = row.get('Genres', '')
                has_tagline = bool(self.clean_text(row.get('tmdb_tagline', None)))

                if pd.notna(genres):
                    for genre in str(genres).split('|'):
                        genre = genre.strip()
                        if genre:
                            if genre not in genre_tagline_counts:
                                genre_tagline_counts[genre] = {'has': 0, 'total': 0}
                            genre_tagline_counts[genre]['total'] += 1
                            if has_tagline:
                                genre_tagline_counts[genre]['has'] += 1

            genre_percentages = []
            for genre, counts in genre_tagline_counts.items():
                if counts['total'] >= 10:  # At least 10 movies
                    pct = (counts['has'] / counts['total']) * 100
                    genre_percentages.append((genre, pct, counts['total']))

            genre_percentages.sort(key=lambda x: x[1], reverse=True)
            top_genres = genre_percentages[:12]

            if top_genres:
                genres, pcts, totals = zip(*top_genres)
                colors = ['green' if p > 60 else 'orange' if p > 30 else 'red' for p in pcts]

                ax3.barh(range(len(genres)), pcts, color=colors, alpha=0.7)
                ax3.set_yticks(range(len(genres)))
                ax3.set_yticklabels([f"{g} (n={t})" for g, t in zip(genres, totals)])
                ax3.invert_yaxis()
                ax3.set_xlabel('Tagline Presence (%)')
                ax3.set_title('Tagline Coverage by Genre')
                ax3.grid(axis='x', alpha=0.3)

        # Common tagline words
        ax4 = axes[1, 1]

        tagline_keywords = []
        for tagline in self.movies_df['tmdb_tagline']:
            cleaned = self.clean_text(tagline)
            if cleaned:
                tagline_keywords.extend(self.extract_keywords(cleaned))

        if tagline_keywords:
            tagline_counter = Counter(tagline_keywords)
            top_20 = tagline_counter.most_common(20)
            words, counts = zip(*top_20)

            ax4.barh(range(len(words)), counts, color='mediumpurple', alpha=0.7)
            ax4.set_yticks(range(len(words)))
            ax4.set_yticklabels(words)
            ax4.invert_yaxis()
            ax4.set_xlabel('Frequency')
            ax4.set_title('Top 20 Tagline Keywords')
            ax4.grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'tagline_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_genre_narrative_patterns(self):
        """Analyze narrative patterns by genre."""
        print("Analyzing genre narrative patterns...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Genre Narrative Patterns', fontsize=16, fontweight='bold')

        if 'Genres' not in self.movies_df.columns:
            self._create_insufficient_data_plot(fig, "No genre data available")
            plt.savefig(self.viz_dir / 'genre_narrative_patterns.png', dpi=300, bbox_inches='tight')
            plt.close()
            return

        # Get primary plot column
        primary_col = None
        for col in ['tmdb_overview', 'omdb_plot', 'Description']:
            if col in self.movies_df.columns:
                primary_col = col
                break

        if not primary_col:
            self._create_insufficient_data_plot(fig, "No plot data available")
            plt.savefig(self.viz_dir / 'genre_narrative_patterns.png', dpi=300, bbox_inches='tight')
            plt.close()
            return

        # Genre-specific keywords
        genre_keywords = {}

        for idx, row in self.movies_df.iterrows():
            genres = row.get('Genres', '')
            plot_text = self.clean_text(row.get(primary_col, None))

            if pd.notna(genres) and plot_text:
                keywords = self.extract_keywords(plot_text)
                for genre in str(genres).split('|'):
                    genre = genre.strip()
                    if genre:
                        if genre not in genre_keywords:
                            genre_keywords[genre] = []
                        genre_keywords[genre].extend(keywords)

        # Top genres by movie count (limit to 4 for 2x2 grid)
        top_genres = sorted(genre_keywords.items(),
                          key=lambda x: len(x[1]), reverse=True)[:4]

        if not top_genres:
            self._create_insufficient_data_plot(fig, "Insufficient genre data")
            plt.savefig(self.viz_dir / 'genre_narrative_patterns.png', dpi=300, bbox_inches='tight')
            plt.close()
            return

        # Create word clouds (as bar charts)
        for idx, (genre, keywords) in enumerate(top_genres):
            if idx >= 4:
                break

            row = idx // 2
            col = idx % 2
            ax = axes[row, col]

            keyword_counter = Counter(keywords)
            top_10 = keyword_counter.most_common(10)

            if top_10:
                words, counts = zip(*top_10)
                ax.barh(range(len(words)), counts, alpha=0.7)
                ax.set_yticks(range(len(words)))
                ax.set_yticklabels(words)
                ax.invert_yaxis()
                ax.set_xlabel('Frequency')
                ax.set_title(f'{genre} (Top Keywords)')
                ax.grid(axis='x', alpha=0.3)

        # Hide unused subplots
        if len(top_genres) < 4:
            for idx in range(len(top_genres), 4):
                row = idx // 2
                col = idx % 2
                axes[row, col].axis('off')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'genre_narrative_patterns.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_plot_complexity(self):
        """Analyze narrative complexity metrics."""
        print("Analyzing plot complexity...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Narrative Complexity Analysis', fontsize=16, fontweight='bold')

        # Get primary plot column
        primary_col = None
        for col in ['tmdb_overview', 'omdb_plot', 'Description']:
            if col in self.movies_df.columns:
                primary_col = col
                break

        if not primary_col:
            self._create_insufficient_data_plot(fig, "No plot data available")
            plt.savefig(self.viz_dir / 'plot_complexity.png', dpi=300, bbox_inches='tight')
            plt.close()
            return

        # Calculate complexity metrics
        complexity_data = []

        for idx, row in self.movies_df.iterrows():
            plot_text = self.clean_text(row.get(primary_col, None))

            if plot_text:
                # Metrics
                word_count = self.count_words(plot_text)
                unique_words = len(set(self.extract_keywords(plot_text)))

                # Sentence count (approximate)
                sentences = len([s for s in re.split(r'[.!?]+', plot_text) if s.strip()])

                # Lexical diversity
                all_words = re.findall(r'\b\w+\b', plot_text.lower())
                lexical_diversity = unique_words / len(all_words) if len(all_words) > 0 else 0

                # Average sentence length
                avg_sentence_length = word_count / sentences if sentences > 0 else 0

                complexity_data.append({
                    'word_count': word_count,
                    'unique_words': unique_words,
                    'sentences': sentences,
                    'lexical_diversity': lexical_diversity,
                    'avg_sentence_length': avg_sentence_length,
                    'rating': row.get('IMDb Rating', None),
                    'year': row.get('Year', None)
                })

        if not complexity_data:
            self._create_insufficient_data_plot(fig, "Insufficient complexity data")
            plt.savefig(self.viz_dir / 'plot_complexity.png', dpi=300, bbox_inches='tight')
            plt.close()
            return

        complexity_df = pd.DataFrame(complexity_data)

        # Lexical diversity distribution
        ax1 = axes[0, 0]
        ax1.hist(complexity_df['lexical_diversity'], bins=30,
                color='steelblue', edgecolor='black', alpha=0.7)
        ax1.axvline(complexity_df['lexical_diversity'].median(),
                   color='red', linestyle='--', linewidth=2,
                   label=f'Median: {complexity_df["lexical_diversity"].median():.3f}')
        ax1.set_xlabel('Lexical Diversity (Unique/Total)')
        ax1.set_ylabel('Frequency')
        ax1.set_title('Lexical Diversity Distribution')
        ax1.legend()
        ax1.grid(alpha=0.3)

        # Sentence length distribution
        ax2 = axes[0, 1]
        ax2.hist(complexity_df['avg_sentence_length'], bins=30,
                color='coral', edgecolor='black', alpha=0.7)
        ax2.axvline(complexity_df['avg_sentence_length'].median(),
                   color='red', linestyle='--', linewidth=2,
                   label=f'Median: {complexity_df["avg_sentence_length"].median():.1f}')
        ax2.set_xlabel('Average Sentence Length (words)')
        ax2.set_ylabel('Frequency')
        ax2.set_title('Sentence Length Distribution')
        ax2.legend()
        ax2.grid(alpha=0.3)

        # Complexity vs rating
        ax3 = axes[1, 0]

        valid_data = complexity_df[complexity_df['rating'].notna()]
        if len(valid_data) > 10:
            ax3.scatter(valid_data['lexical_diversity'], valid_data['rating'], alpha=0.3)

            if len(valid_data) > 20:
                corr, p_value = stats.pearsonr(valid_data['lexical_diversity'],
                                               valid_data['rating'])
                ax3.text(0.05, 0.95, f'Correlation: {corr:.3f}\np-value: {p_value:.4f}',
                        transform=ax3.transAxes, verticalalignment='top',
                        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

            ax3.set_xlabel('Lexical Diversity')
            ax3.set_ylabel('IMDb Rating')
            ax3.set_title('Plot Complexity vs Film Rating')
            ax3.grid(alpha=0.3)

        # Complexity evolution over time
        ax4 = axes[1, 1]

        valid_years = complexity_df[complexity_df['year'].notna()]
        if len(valid_years) > 10:
            decade_complexity = {}

            for _, row in valid_years.iterrows():
                try:
                    decade = (int(row['year']) // 10) * 10
                    if decade not in decade_complexity:
                        decade_complexity[decade] = []
                    decade_complexity[decade].append(row['lexical_diversity'])
                except:
                    pass

            if decade_complexity:
                sorted_decades = sorted(decade_complexity.keys())
                avg_complexity = [np.mean(decade_complexity[d]) for d in sorted_decades]
                decade_labels = [f"{d}s" for d in sorted_decades]

                ax4.plot(decade_labels, avg_complexity, marker='o',
                        linewidth=2, markersize=8, color='darkgreen')
                ax4.set_xlabel('Decade')
                ax4.set_ylabel('Average Lexical Diversity')
                ax4.set_title('Narrative Complexity Evolution')
                ax4.grid(alpha=0.3)
                ax4.tick_params(axis='x', rotation=45)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'plot_complexity.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_sentiment_patterns(self):
        """Analyze sentiment patterns in plot descriptions."""
        print("Analyzing sentiment patterns...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Plot Sentiment Analysis', fontsize=16, fontweight='bold')

        # Get primary plot column
        primary_col = None
        for col in ['tmdb_overview', 'omdb_plot', 'Description']:
            if col in self.movies_df.columns:
                primary_col = col
                break

        if not primary_col:
            self._create_insufficient_data_plot(fig, "No plot data available")
            plt.savefig(self.viz_dir / 'sentiment_patterns.png', dpi=300, bbox_inches='tight')
            plt.close()
            return

        # Simple sentiment word lists
        positive_words = {
            'love', 'hope', 'happy', 'friendship', 'help', 'save', 'beautiful',
            'success', 'win', 'dream', 'freedom', 'joy', 'peace', 'triumph',
            'hero', 'celebrate', 'miracle', 'amazing', 'wonderful', 'brilliant'
        }

        negative_words = {
            'death', 'kill', 'murder', 'dead', 'die', 'war', 'fight', 'crime',
            'criminal', 'evil', 'dark', 'fear', 'terror', 'horror', 'tragedy',
            'revenge', 'dangerous', 'threat', 'attack', 'destroy', 'victim'
        }

        conflict_words = {
            'battle', 'struggle', 'conflict', 'fight', 'confront', 'against',
            'enemy', 'defeat', 'challenge', 'obstacle', 'compete', 'oppose'
        }

        # Calculate sentiment scores
        sentiment_data = []

        for idx, row in self.movies_df.iterrows():
            plot_text = self.clean_text(row.get(primary_col, None))

            if plot_text:
                words = set(re.findall(r'\b\w+\b', plot_text.lower()))

                pos_count = len(words & positive_words)
                neg_count = len(words & negative_words)
                conf_count = len(words & conflict_words)

                total_sentiment_words = pos_count + neg_count + conf_count

                if total_sentiment_words > 0:
                    sentiment_data.append({
                        'positive': pos_count,
                        'negative': neg_count,
                        'conflict': conf_count,
                        'total': total_sentiment_words,
                        'pos_ratio': pos_count / total_sentiment_words,
                        'neg_ratio': neg_count / total_sentiment_words,
                        'conf_ratio': conf_count / total_sentiment_words,
                        'rating': row.get('IMDb Rating', None),
                        'genre': row.get('Genres', '').split('|')[0] if pd.notna(row.get('Genres', None)) else None,
                        'year': row.get('Year', None)
                    })

        if not sentiment_data:
            self._create_insufficient_data_plot(fig, "Insufficient sentiment data")
            plt.savefig(self.viz_dir / 'sentiment_patterns.png', dpi=300, bbox_inches='tight')
            plt.close()
            return

        sentiment_df = pd.DataFrame(sentiment_data)

        # Sentiment distribution
        ax1 = axes[0, 0]

        categories = ['Positive', 'Negative', 'Conflict']
        avg_counts = [
            sentiment_df['positive'].mean(),
            sentiment_df['negative'].mean(),
            sentiment_df['conflict'].mean()
        ]

        colors = ['green', 'red', 'orange']
        ax1.bar(categories, avg_counts, color=colors, alpha=0.7, edgecolor='black')
        ax1.set_ylabel('Average Word Count per Plot')
        ax1.set_title('Average Sentiment Word Usage')
        ax1.grid(axis='y', alpha=0.3)

        # Sentiment balance distribution
        ax2 = axes[0, 1]

        # Create sentiment balance score (-1 to 1)
        sentiment_df['balance'] = (sentiment_df['positive'] - sentiment_df['negative']) / \
                                  (sentiment_df['positive'] + sentiment_df['negative'] + 1)

        ax2.hist(sentiment_df['balance'], bins=30, color='mediumpurple',
                edgecolor='black', alpha=0.7)
        ax2.axvline(0, color='black', linestyle='--', linewidth=2, label='Neutral')
        ax2.axvline(sentiment_df['balance'].median(), color='red',
                   linestyle='--', linewidth=2, label=f'Median: {sentiment_df["balance"].median():.3f}')
        ax2.set_xlabel('Sentiment Balance (Positive - Negative)')
        ax2.set_ylabel('Frequency')
        ax2.set_title('Plot Sentiment Balance Distribution')
        ax2.legend()
        ax2.grid(alpha=0.3)

        # Sentiment by genre
        ax3 = axes[1, 0]

        valid_genres = sentiment_df[sentiment_df['genre'].notna()]
        if len(valid_genres) > 0:
            genre_sentiment = valid_genres.groupby('genre').agg({
                'pos_ratio': 'mean',
                'neg_ratio': 'mean',
                'conf_ratio': 'mean'
            }).reset_index()

            # Get top genres
            genre_counts = valid_genres['genre'].value_counts()
            top_genres = genre_counts.head(10).index
            genre_sentiment = genre_sentiment[genre_sentiment['genre'].isin(top_genres)]

            if len(genre_sentiment) > 0:
                x = np.arange(len(genre_sentiment))
                width = 0.25

                ax3.bar(x - width, genre_sentiment['pos_ratio'], width,
                       label='Positive', color='green', alpha=0.7)
                ax3.bar(x, genre_sentiment['neg_ratio'], width,
                       label='Negative', color='red', alpha=0.7)
                ax3.bar(x + width, genre_sentiment['conf_ratio'], width,
                       label='Conflict', color='orange', alpha=0.7)

                ax3.set_ylabel('Ratio of Sentiment Words')
                ax3.set_title('Sentiment Patterns by Genre')
                ax3.set_xticks(x)
                ax3.set_xticklabels(genre_sentiment['genre'], rotation=45, ha='right')
                ax3.legend()
                ax3.grid(axis='y', alpha=0.3)

        # Sentiment vs rating
        ax4 = axes[1, 1]

        valid_ratings = sentiment_df[sentiment_df['rating'].notna()]
        if len(valid_ratings) > 10:
            ax4.scatter(valid_ratings['balance'], valid_ratings['rating'], alpha=0.3)

            if len(valid_ratings) > 20:
                corr, p_value = stats.pearsonr(valid_ratings['balance'],
                                               valid_ratings['rating'])
                ax4.text(0.05, 0.95, f'Correlation: {corr:.3f}\np-value: {p_value:.4f}',
                        transform=ax4.transAxes, verticalalignment='top',
                        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

            ax4.set_xlabel('Sentiment Balance')
            ax4.set_ylabel('IMDb Rating')
            ax4.set_title('Plot Sentiment vs Film Rating')
            ax4.grid(alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'sentiment_patterns.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_description_source_comparison(self):
        """Compare different plot description sources."""
        print("Comparing description sources...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Plot Description Source Comparison', fontsize=16, fontweight='bold')

        sources = {
            'Description': 'Description',
            'TMDB Overview': 'tmdb_overview',
            'OMDB Plot': 'omdb_plot'
        }

        available_sources = {label: col for label, col in sources.items()
                           if col in self.movies_df.columns}

        if len(available_sources) < 2:
            self._create_insufficient_data_plot(fig, "Need at least 2 plot sources")
            plt.savefig(self.viz_dir / 'source_comparison.png', dpi=300, bbox_inches='tight')
            plt.close()
            return

        # Word count comparison
        ax1 = axes[0, 0]

        source_lengths = {}
        for label, col in available_sources.items():
            lengths = []
            for text in self.movies_df[col]:
                cleaned = self.clean_text(text)
                if cleaned:
                    lengths.append(self.count_words(cleaned))
            source_lengths[label] = lengths

        if source_lengths:
            labels = list(source_lengths.keys())
            data = [source_lengths[l] for l in labels]

            bp = ax1.boxplot(data, tick_labels=labels, patch_artist=True)
            for patch in bp['boxes']:
                patch.set_facecolor('lightblue')
                patch.set_alpha(0.7)

            ax1.set_ylabel('Word Count')
            ax1.set_title('Word Count by Source')
            ax1.grid(axis='y', alpha=0.3)
            ax1.tick_params(axis='x', rotation=45)

        # Overlap analysis
        ax2 = axes[0, 1]

        if len(available_sources) >= 2:
            # Check how many films have multiple sources
            overlap_data = []

            for idx, row in self.movies_df.iterrows():
                sources_present = []
                for label, col in available_sources.items():
                    if self.clean_text(row.get(col, None)):
                        sources_present.append(label)

                if len(sources_present) > 0:
                    overlap_data.append(len(sources_present))

            if overlap_data:
                overlap_counts = Counter(overlap_data)
                labels = [f'{i} sources' for i in sorted(overlap_counts.keys())]
                values = [overlap_counts[i] for i in sorted(overlap_counts.keys())]

                ax2.pie(values, labels=labels, autopct='%1.1f%%', startangle=90)
                ax2.set_title('Films by Number of Plot Sources')

        # Keyword overlap between sources
        ax3 = axes[1, 0]

        if len(available_sources) >= 2:
            source_keywords = {}

            for label, col in available_sources.items():
                keywords = []
                for text in self.movies_df[col]:
                    cleaned = self.clean_text(text)
                    if cleaned:
                        keywords.extend(self.extract_keywords(cleaned))
                source_keywords[label] = set(Counter(keywords).most_common(100))

            # Create overlap matrix
            source_list = list(source_keywords.keys())
            overlap_matrix = np.zeros((len(source_list), len(source_list)))

            for i, src1 in enumerate(source_list):
                for j, src2 in enumerate(source_list):
                    if i == j:
                        overlap_matrix[i, j] = 100
                    else:
                        kw1 = set(k[0] for k in source_keywords[src1])
                        kw2 = set(k[0] for k in source_keywords[src2])
                        overlap = len(kw1 & kw2) / len(kw1 | kw2) * 100 if len(kw1 | kw2) > 0 else 0
                        overlap_matrix[i, j] = overlap

            im = ax3.imshow(overlap_matrix, cmap='YlOrRd', aspect='auto')
            ax3.set_xticks(np.arange(len(source_list)))
            ax3.set_yticks(np.arange(len(source_list)))
            ax3.set_xticklabels(source_list)
            ax3.set_yticklabels(source_list)
            plt.setp(ax3.get_xticklabels(), rotation=45, ha="right")

            # Add values
            for i in range(len(source_list)):
                for j in range(len(source_list)):
                    text = ax3.text(j, i, f'{overlap_matrix[i, j]:.1f}%',
                                  ha="center", va="center", color="black")

            ax3.set_title('Keyword Overlap Between Sources (%)')
            plt.colorbar(im, ax=ax3)

        # Quality comparison (avg rating for films with each source)
        ax4 = axes[1, 1]

        source_ratings = {}

        for label, col in available_sources.items():
            ratings = []
            for idx, row in self.movies_df.iterrows():
                if self.clean_text(row.get(col, None)):
                    rating = row.get('IMDb Rating', None)
                    if pd.notna(rating):
                        try:
                            rating_val = float(rating)
                            if 0 <= rating_val <= 10:
                                ratings.append(rating_val)
                        except:
                            pass
            source_ratings[label] = ratings

        if source_ratings:
            labels = list(source_ratings.keys())
            data = [source_ratings[l] for l in labels]

            bp = ax4.boxplot(data, tick_labels=labels, patch_artist=True)
            for patch in bp['boxes']:
                patch.set_facecolor('lightgreen')
                patch.set_alpha(0.7)

            ax4.set_ylabel('IMDb Rating')
            ax4.set_title('Rating Distribution by Plot Source')
            ax4.grid(axis='y', alpha=0.3)
            ax4.tick_params(axis='x', rotation=45)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'source_comparison.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_plot_themes(self):
        """Identify and analyze common plot themes."""
        print("Analyzing plot themes...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Plot Themes Analysis', fontsize=16, fontweight='bold')

        # Get primary plot column
        primary_col = None
        for col in ['tmdb_overview', 'omdb_plot', 'Description']:
            if col in self.movies_df.columns:
                primary_col = col
                break

        if not primary_col:
            self._create_insufficient_data_plot(fig, "No plot data available")
            plt.savefig(self.viz_dir / 'plot_themes.png', dpi=300, bbox_inches='tight')
            plt.close()
            return

        # Define theme keywords
        themes = {
            'Love & Romance': ['love', 'romance', 'relationship', 'marriage', 'heart', 'romantic'],
            'Crime & Justice': ['crime', 'murder', 'detective', 'police', 'criminal', 'investigation'],
            'War & Conflict': ['war', 'battle', 'soldier', 'military', 'army', 'fight'],
            'Family': ['family', 'father', 'mother', 'son', 'daughter', 'parent', 'child'],
            'Survival': ['survive', 'survival', 'escape', 'trapped', 'danger', 'threat'],
            'Revenge': ['revenge', 'vengeance', 'payback', 'retribution'],
            'Coming of Age': ['young', 'youth', 'growing', 'childhood', 'adolescent'],
            'Mystery': ['mystery', 'mysterious', 'secret', 'hidden', 'disappear'],
            'Power & Politics': ['power', 'political', 'government', 'king', 'throne', 'empire'],
            'Redemption': ['redemption', 'forgiveness', 'atone', 'second chance'],
            'Death & Loss': ['death', 'dead', 'dying', 'loss', 'grief', 'funeral'],
            'Journey & Quest': ['journey', 'quest', 'adventure', 'travel', 'search', 'find']
        }

        # Count theme occurrences
        theme_counts = {theme: 0 for theme in themes}
        theme_ratings = {theme: [] for theme in themes}
        theme_by_decade = {theme: {} for theme in themes}

        for idx, row in self.movies_df.iterrows():
            plot_text = self.clean_text(row.get(primary_col, None))

            if plot_text:
                words = set(re.findall(r'\b\w+\b', plot_text.lower()))
                rating = row.get('IMDb Rating', None)
                year = row.get('Year', None)

                for theme, keywords in themes.items():
                    if any(keyword in words for keyword in keywords):
                        theme_counts[theme] += 1

                        if pd.notna(rating):
                            try:
                                rating_val = float(rating)
                                if 0 <= rating_val <= 10:
                                    theme_ratings[theme].append(rating_val)
                            except:
                                pass

                        if pd.notna(year):
                            try:
                                decade = (int(year) // 10) * 10
                                if decade not in theme_by_decade[theme]:
                                    theme_by_decade[theme][decade] = 0
                                theme_by_decade[theme][decade] += 1
                            except:
                                pass

        # Theme frequency
        ax1 = axes[0, 0]

        sorted_themes = sorted(theme_counts.items(), key=lambda x: x[1], reverse=True)
        top_themes = sorted_themes[:12]

        if top_themes:
            theme_names, counts = zip(*top_themes)
            ax1.barh(range(len(theme_names)), counts, color='steelblue', alpha=0.7)
            ax1.set_yticks(range(len(theme_names)))
            ax1.set_yticklabels(theme_names)
            ax1.invert_yaxis()
            ax1.set_xlabel('Number of Films')
            ax1.set_title('Most Common Plot Themes')
            ax1.grid(axis='x', alpha=0.3)

        # Theme vs rating
        ax2 = axes[0, 1]

        theme_avg_ratings = []
        for theme, ratings in theme_ratings.items():
            if len(ratings) >= 5:  # At least 5 films
                theme_avg_ratings.append((theme, np.mean(ratings), len(ratings)))

        theme_avg_ratings.sort(key=lambda x: x[1], reverse=True)
        top_rated_themes = theme_avg_ratings[:10]

        if top_rated_themes:
            theme_names, avg_ratings, counts = zip(*top_rated_themes)
            colors = ['green' if r > 7 else 'orange' if r > 6 else 'red' for r in avg_ratings]

            ax2.barh(range(len(theme_names)), avg_ratings, color=colors, alpha=0.7)
            ax2.set_yticks(range(len(theme_names)))
            ax2.set_yticklabels([f"{name} (n={count})" for name, count in zip(theme_names, counts)])
            ax2.invert_yaxis()
            ax2.set_xlabel('Average IMDb Rating')
            ax2.set_title('Highest Rated Themes')
            ax2.grid(axis='x', alpha=0.3)
            ax2.set_xlim(0, 10)

        # Theme evolution over time
        ax3 = axes[1, 0]

        # Pick top 5 themes to show trends
        top_5_themes = [t[0] for t in sorted_themes[:5]]

        for theme in top_5_themes:
            if theme_by_decade[theme]:
                decades = sorted(theme_by_decade[theme].keys())
                counts = [theme_by_decade[theme][d] for d in decades]
                decade_labels = [f"{d}s" for d in decades]
                ax3.plot(decade_labels, counts, marker='o', label=theme, linewidth=2)

        ax3.set_xlabel('Decade')
        ax3.set_ylabel('Number of Films')
        ax3.set_title('Theme Popularity Over Time')
        ax3.legend(fontsize=8)
        ax3.grid(alpha=0.3)
        ax3.tick_params(axis='x', rotation=45)

        # Theme co-occurrence
        ax4 = axes[1, 1]

        # Count how often themes appear together
        top_6_themes = [t[0] for t in sorted_themes[:6]]
        cooccurrence = np.zeros((len(top_6_themes), len(top_6_themes)))

        for idx, row in self.movies_df.iterrows():
            plot_text = self.clean_text(row.get(primary_col, None))

            if plot_text:
                words = set(re.findall(r'\b\w+\b', plot_text.lower()))
                present_themes = []

                for theme in top_6_themes:
                    if any(keyword in words for keyword in themes[theme]):
                        present_themes.append(theme)

                # Count co-occurrences
                for i, theme1 in enumerate(top_6_themes):
                    for j, theme2 in enumerate(top_6_themes):
                        if theme1 in present_themes and theme2 in present_themes:
                            cooccurrence[i, j] += 1

        # Normalize to percentages
        for i in range(len(top_6_themes)):
            if cooccurrence[i, i] > 0:
                cooccurrence[i, :] = (cooccurrence[i, :] / cooccurrence[i, i]) * 100

        im = ax4.imshow(cooccurrence, cmap='YlOrRd', aspect='auto')
        ax4.set_xticks(np.arange(len(top_6_themes)))
        ax4.set_yticks(np.arange(len(top_6_themes)))
        ax4.set_xticklabels([t[:15] for t in top_6_themes], fontsize=8)
        ax4.set_yticklabels([t[:15] for t in top_6_themes], fontsize=8)
        plt.setp(ax4.get_xticklabels(), rotation=45, ha="right")

        ax4.set_title('Theme Co-occurrence (%)')
        plt.colorbar(im, ax=ax4)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'plot_themes.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_narrative_structure(self):
        """Analyze narrative structure patterns."""
        print("Analyzing narrative structure...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Narrative Structure Analysis', fontsize=16, fontweight='bold')

        # Get primary plot column
        primary_col = None
        for col in ['tmdb_overview', 'omdb_plot', 'Description']:
            if col in self.movies_df.columns:
                primary_col = col
                break

        if not primary_col:
            self._create_insufficient_data_plot(fig, "No plot data available")
            plt.savefig(self.viz_dir / 'narrative_structure.png', dpi=300, bbox_inches='tight')
            plt.close()
            return

        # Narrative markers
        setup_words = ['young', 'ordinary', 'living', 'works', 'meets', 'discovers']
        conflict_words = ['but', 'however', 'when', 'suddenly', 'until', 'must', 'forced']
        resolution_words = ['finally', 'eventually', 'realizes', 'learns', 'becomes', 'finds']

        structure_data = []

        for idx, row in self.movies_df.iterrows():
            plot_text = self.clean_text(row.get(primary_col, None))

            if plot_text:
                words = re.findall(r'\b\w+\b', plot_text.lower())

                setup_count = sum(1 for w in words if w in setup_words)
                conflict_count = sum(1 for w in words if w in conflict_words)
                resolution_count = sum(1 for w in words if w in resolution_words)

                total = setup_count + conflict_count + resolution_count

                if total > 0:
                    structure_data.append({
                        'setup': setup_count,
                        'conflict': conflict_count,
                        'resolution': resolution_count,
                        'total': total,
                        'setup_pct': setup_count / total,
                        'conflict_pct': conflict_count / total,
                        'resolution_pct': resolution_count / total,
                        'rating': row.get('IMDb Rating', None),
                        'genre': row.get('Genres', '').split('|')[0] if pd.notna(row.get('Genres', None)) else None
                    })

        if not structure_data:
            self._create_insufficient_data_plot(fig, "Insufficient structure data")
            plt.savefig(self.viz_dir / 'narrative_structure.png', dpi=300, bbox_inches='tight')
            plt.close()
            return

        structure_df = pd.DataFrame(structure_data)

        # Average structure distribution
        ax1 = axes[0, 0]

        avg_structure = {
            'Setup': structure_df['setup'].mean(),
            'Conflict': structure_df['conflict'].mean(),
            'Resolution': structure_df['resolution'].mean()
        }

        colors = ['lightblue', 'orange', 'lightgreen']
        ax1.bar(avg_structure.keys(), avg_structure.values(),
               color=colors, alpha=0.7, edgecolor='black')
        ax1.set_ylabel('Average Word Count')
        ax1.set_title('Average Narrative Structure Elements')
        ax1.grid(axis='y', alpha=0.3)

        # Structure balance
        ax2 = axes[0, 1]

        structure_df['balance_score'] = (
            abs(structure_df['setup_pct'] - 0.33) +
            abs(structure_df['conflict_pct'] - 0.33) +
            abs(structure_df['resolution_pct'] - 0.33)
        )

        ax2.hist(structure_df['balance_score'], bins=30,
                color='mediumpurple', edgecolor='black', alpha=0.7)
        ax2.axvline(structure_df['balance_score'].median(), color='red',
                   linestyle='--', linewidth=2,
                   label=f'Median: {structure_df["balance_score"].median():.3f}')
        ax2.set_xlabel('Structure Imbalance Score (lower = more balanced)')
        ax2.set_ylabel('Frequency')
        ax2.set_title('Narrative Structure Balance')
        ax2.legend()
        ax2.grid(alpha=0.3)

        # Structure by genre
        ax3 = axes[1, 0]

        valid_genres = structure_df[structure_df['genre'].notna()]
        if len(valid_genres) > 0:
            genre_structure = valid_genres.groupby('genre').agg({
                'setup_pct': 'mean',
                'conflict_pct': 'mean',
                'resolution_pct': 'mean'
            }).reset_index()

            # Get top genres
            genre_counts = valid_genres['genre'].value_counts()
            top_genres = genre_counts.head(8).index
            genre_structure = genre_structure[genre_structure['genre'].isin(top_genres)]

            if len(genre_structure) > 0:
                x = np.arange(len(genre_structure))
                width = 0.25

                ax3.bar(x - width, genre_structure['setup_pct'], width,
                       label='Setup', color='lightblue', alpha=0.7)
                ax3.bar(x, genre_structure['conflict_pct'], width,
                       label='Conflict', color='orange', alpha=0.7)
                ax3.bar(x + width, genre_structure['resolution_pct'], width,
                       label='Resolution', color='lightgreen', alpha=0.7)

                ax3.set_ylabel('Proportion')
                ax3.set_title('Narrative Structure by Genre')
                ax3.set_xticks(x)
                ax3.set_xticklabels(genre_structure['genre'], rotation=45, ha='right')
                ax3.legend()
                ax3.grid(axis='y', alpha=0.3)

        # Structure balance vs rating
        ax4 = axes[1, 1]

        valid_ratings = structure_df[structure_df['rating'].notna()]
        if len(valid_ratings) > 10:
            ax4.scatter(valid_ratings['balance_score'], valid_ratings['rating'], alpha=0.3)

            if len(valid_ratings) > 20:
                corr, p_value = stats.pearsonr(valid_ratings['balance_score'],
                                               valid_ratings['rating'])
                ax4.text(0.05, 0.95, f'Correlation: {corr:.3f}\np-value: {p_value:.4f}',
                        transform=ax4.transAxes, verticalalignment='top',
                        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

            ax4.set_xlabel('Structure Imbalance Score')
            ax4.set_ylabel('IMDb Rating')
            ax4.set_title('Narrative Balance vs Film Rating')
            ax4.grid(alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'narrative_structure.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_plot_uniqueness(self):
        """Analyze plot uniqueness and originality."""
        print("Analyzing plot uniqueness...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Plot Uniqueness Analysis', fontsize=16, fontweight='bold')

        # Get primary plot column
        primary_col = None
        for col in ['tmdb_overview', 'omdb_plot', 'Description']:
            if col in self.movies_df.columns:
                primary_col = col
                break

        if not primary_col:
            self._create_insufficient_data_plot(fig, "No plot data available")
            plt.savefig(self.viz_dir / 'plot_uniqueness.png', dpi=300, bbox_inches='tight')
            plt.close()
            return

        # Calculate uniqueness scores
        all_plots = []
        for text in self.movies_df[primary_col]:
            cleaned = self.clean_text(text)
            if cleaned:
                all_plots.append(cleaned)

        if len(all_plots) < 10:
            self._create_insufficient_data_plot(fig, "Insufficient plot data")
            plt.savefig(self.viz_dir / 'plot_uniqueness.png', dpi=300, bbox_inches='tight')
            plt.close()
            return

        # Build global keyword frequency
        global_keywords = []
        for plot in all_plots:
            global_keywords.extend(self.extract_keywords(plot))

        global_freq = Counter(global_keywords)
        total_plots = len(all_plots)

        uniqueness_data = []

        for idx, row in self.movies_df.iterrows():
            plot_text = self.clean_text(row.get(primary_col, None))

            if plot_text:
                keywords = self.extract_keywords(plot_text)

                if keywords:
                    # Calculate uniqueness score based on keyword rarity
                    uniqueness_scores = []
                    for keyword in keywords:
                        # How rare is this keyword?
                        rarity = 1 - (global_freq[keyword] / total_plots)
                        uniqueness_scores.append(rarity)

                    avg_uniqueness = np.mean(uniqueness_scores)

                    uniqueness_data.append({
                        'uniqueness': avg_uniqueness,
                        'rating': row.get('IMDb Rating', None),
                        'year': row.get('Year', None),
                        'genre': row.get('Genres', '').split('|')[0] if pd.notna(row.get('Genres', None)) else None
                    })

        if not uniqueness_data:
            self._create_insufficient_data_plot(fig, "Insufficient uniqueness data")
            plt.savefig(self.viz_dir / 'plot_uniqueness.png', dpi=300, bbox_inches='tight')
            plt.close()
            return

        uniqueness_df = pd.DataFrame(uniqueness_data)

        # Uniqueness distribution
        ax1 = axes[0, 0]

        ax1.hist(uniqueness_df['uniqueness'], bins=30,
                color='teal', edgecolor='black', alpha=0.7)
        ax1.axvline(uniqueness_df['uniqueness'].median(), color='red',
                   linestyle='--', linewidth=2,
                   label=f'Median: {uniqueness_df["uniqueness"].median():.3f}')
        ax1.set_xlabel('Uniqueness Score (0=common, 1=unique)')
        ax1.set_ylabel('Frequency')
        ax1.set_title('Plot Uniqueness Distribution')
        ax1.legend()
        ax1.grid(alpha=0.3)

        # Uniqueness vs rating
        ax2 = axes[0, 1]

        valid_ratings = uniqueness_df[uniqueness_df['rating'].notna()]
        if len(valid_ratings) > 10:
            ax2.scatter(valid_ratings['uniqueness'], valid_ratings['rating'], alpha=0.3)

            if len(valid_ratings) > 20:
                corr, p_value = stats.pearsonr(valid_ratings['uniqueness'],
                                               valid_ratings['rating'])
                ax2.text(0.05, 0.95, f'Correlation: {corr:.3f}\np-value: {p_value:.4f}',
                        transform=ax2.transAxes, verticalalignment='top',
                        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

            ax2.set_xlabel('Uniqueness Score')
            ax2.set_ylabel('IMDb Rating')
            ax2.set_title('Plot Uniqueness vs Film Rating')
            ax2.grid(alpha=0.3)

        # Uniqueness by decade
        ax3 = axes[1, 0]

        valid_years = uniqueness_df[uniqueness_df['year'].notna()]
        if len(valid_years) > 10:
            decade_uniqueness = {}

            for _, row in valid_years.iterrows():
                try:
                    decade = (int(row['year']) // 10) * 10
                    if decade not in decade_uniqueness:
                        decade_uniqueness[decade] = []
                    decade_uniqueness[decade].append(row['uniqueness'])
                except:
                    pass

            if decade_uniqueness:
                sorted_decades = sorted(decade_uniqueness.keys())
                avg_uniqueness = [np.mean(decade_uniqueness[d]) for d in sorted_decades]
                decade_labels = [f"{d}s" for d in sorted_decades]

                ax3.plot(decade_labels, avg_uniqueness, marker='o',
                        linewidth=2, markersize=8, color='darkviolet')
                ax3.set_xlabel('Decade')
                ax3.set_ylabel('Average Uniqueness Score')
                ax3.set_title('Plot Uniqueness Over Time')
                ax3.grid(alpha=0.3)
                ax3.tick_params(axis='x', rotation=45)

        # Uniqueness by genre
        ax4 = axes[1, 1]

        valid_genres = uniqueness_df[uniqueness_df['genre'].notna()]
        if len(valid_genres) > 0:
            genre_uniqueness = valid_genres.groupby('genre')['uniqueness'].agg(['mean', 'count']).reset_index()
            genre_uniqueness = genre_uniqueness[genre_uniqueness['count'] >= 10]
            genre_uniqueness = genre_uniqueness.sort_values('mean', ascending=False).head(12)

            if len(genre_uniqueness) > 0:
                colors = ['green' if u > 0.7 else 'orange' if u > 0.6 else 'red'
                         for u in genre_uniqueness['mean']]

                ax4.barh(range(len(genre_uniqueness)), genre_uniqueness['mean'],
                        color=colors, alpha=0.7)
                ax4.set_yticks(range(len(genre_uniqueness)))
                ax4.set_yticklabels([f"{g} (n={c})" for g, c in
                                    zip(genre_uniqueness['genre'], genre_uniqueness['count'])])
                ax4.invert_yaxis()
                ax4.set_xlabel('Average Uniqueness Score')
                ax4.set_title('Plot Uniqueness by Genre')
                ax4.grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'plot_uniqueness.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_runtime_plot_correlation(self):
        """Analyze relationship between runtime and plot characteristics."""
        print("Analyzing runtime-plot correlations...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Runtime vs Plot Characteristics', fontsize=16, fontweight='bold')

        if 'Runtime' not in self.movies_df.columns:
            self._create_insufficient_data_plot(fig, "No runtime data available")
            plt.savefig(self.viz_dir / 'runtime_plot_correlation.png', dpi=300, bbox_inches='tight')
            plt.close()
            return

        # Get primary plot column
        primary_col = None
        for col in ['tmdb_overview', 'omdb_plot', 'Description']:
            if col in self.movies_df.columns:
                primary_col = col
                break

        if not primary_col:
            self._create_insufficient_data_plot(fig, "No plot data available")
            plt.savefig(self.viz_dir / 'runtime_plot_correlation.png', dpi=300, bbox_inches='tight')
            plt.close()
            return

        # Collect data
        runtime_plot_data = []

        for idx, row in self.movies_df.iterrows():
            runtime = row.get('Runtime', None)
            plot_text = self.clean_text(row.get(primary_col, None))

            if pd.notna(runtime) and plot_text:
                try:
                    runtime_val = int(runtime)
                    if 30 <= runtime_val <= 300:  # Reasonable runtime range
                        word_count = self.count_words(plot_text)
                        unique_words = len(set(self.extract_keywords(plot_text)))

                        runtime_plot_data.append({
                            'runtime': runtime_val,
                            'word_count': word_count,
                            'unique_words': unique_words
                        })
                except:
                    pass

        if not runtime_plot_data:
            self._create_insufficient_data_plot(fig, "Insufficient runtime-plot data")
            plt.savefig(self.viz_dir / 'runtime_plot_correlation.png', dpi=300, bbox_inches='tight')
            plt.close()
            return

        runtime_df = pd.DataFrame(runtime_plot_data)

        # Runtime vs plot length
        ax1 = axes[0, 0]

        ax1.scatter(runtime_df['runtime'], runtime_df['word_count'], alpha=0.3)

        if len(runtime_df) > 20:
            z = np.polyfit(runtime_df['runtime'], runtime_df['word_count'], 1)
            p = np.poly1d(z)
            ax1.plot(sorted(runtime_df['runtime']), p(sorted(runtime_df['runtime'])),
                    "r--", linewidth=2, label='Trend')

            corr, p_value = stats.pearsonr(runtime_df['runtime'], runtime_df['word_count'])
            ax1.text(0.05, 0.95, f'Correlation: {corr:.3f}\np-value: {p_value:.4f}',
                    transform=ax1.transAxes, verticalalignment='top',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        ax1.set_xlabel('Runtime (minutes)')
        ax1.set_ylabel('Plot Description Length (words)')
        ax1.set_title('Runtime vs Plot Length')
        ax1.legend()
        ax1.grid(alpha=0.3)

        # Runtime categories vs plot length
        ax2 = axes[0, 1]

        runtime_df['runtime_category'] = pd.cut(runtime_df['runtime'],
                                                bins=[0, 90, 120, 150, 300],
                                                labels=['Short (<90m)', 'Medium (90-120m)',
                                                       'Long (120-150m)', 'Very Long (>150m)'])

        category_data = [runtime_df[runtime_df['runtime_category'] == cat]['word_count'].values
                        for cat in ['Short (<90m)', 'Medium (90-120m)',
                                   'Long (120-150m)', 'Very Long (>150m)']
                        if len(runtime_df[runtime_df['runtime_category'] == cat]) > 0]

        category_labels = [cat for cat in ['Short (<90m)', 'Medium (90-120m)',
                                           'Long (120-150m)', 'Very Long (>150m)']
                          if len(runtime_df[runtime_df['runtime_category'] == cat]) > 0]

        if category_data:
            bp = ax2.boxplot(category_data, tick_labels=category_labels, patch_artist=True)
            for patch in bp['boxes']:
                patch.set_facecolor('lightcoral')
                patch.set_alpha(0.7)

            ax2.set_ylabel('Plot Length (words)')
            ax2.set_title('Plot Length by Runtime Category')
            ax2.grid(axis='y', alpha=0.3)
            ax2.tick_params(axis='x', rotation=45)

        # Runtime vs unique keywords
        ax3 = axes[1, 0]

        ax3.scatter(runtime_df['runtime'], runtime_df['unique_words'], alpha=0.3, color='green')

        if len(runtime_df) > 20:
            z = np.polyfit(runtime_df['runtime'], runtime_df['unique_words'], 1)
            p = np.poly1d(z)
            ax3.plot(sorted(runtime_df['runtime']), p(sorted(runtime_df['runtime'])),
                    "r--", linewidth=2, label='Trend')

            corr, p_value = stats.pearsonr(runtime_df['runtime'], runtime_df['unique_words'])
            ax3.text(0.05, 0.95, f'Correlation: {corr:.3f}\np-value: {p_value:.4f}',
                    transform=ax3.transAxes, verticalalignment='top',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        ax3.set_xlabel('Runtime (minutes)')
        ax3.set_ylabel('Unique Keywords Count')
        ax3.set_title('Runtime vs Plot Complexity')
        ax3.legend()
        ax3.grid(alpha=0.3)

        # Word density (words per minute of runtime)
        ax4 = axes[1, 1]

        runtime_df['word_density'] = runtime_df['word_count'] / runtime_df['runtime']

        ax4.hist(runtime_df['word_density'], bins=30,
                color='mediumpurple', edgecolor='black', alpha=0.7)
        ax4.axvline(runtime_df['word_density'].median(), color='red',
                   linestyle='--', linewidth=2,
                   label=f'Median: {runtime_df["word_density"].median():.2f} words/min')
        ax4.set_xlabel('Plot Description Density (words per minute)')
        ax4.set_ylabel('Frequency')
        ax4.set_title('Plot Description Density Distribution')
        ax4.legend()
        ax4.grid(alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'runtime_plot_correlation.png', dpi=300, bbox_inches='tight')
        plt.close()

    def _create_insufficient_data_plot(self, fig, message):
        """Create a placeholder visualization for insufficient data."""
        fig.text(0.5, 0.5, message, ha='center', va='center',
                fontsize=20, color='red', weight='bold')
        for ax in fig.get_axes():
            ax.set_visible(False)

    def generate_report(self):
        """Generate comprehensive analysis report."""
        print("\nGenerating comprehensive report...")

        report_path = self.reports_dir / 'batch_25_plot_story_report.txt'

        with open(report_path, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("CINESCOPE BATCH 25: PLOT & STORY ANALYSIS\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # Data coverage
            f.write("=" * 80 + "\n")
            f.write("PLOT DATA COVERAGE\n")
            f.write("=" * 80 + "\n\n")

            plot_columns = {
                'Description': 'Description',
                'TMDB Overview': 'tmdb_overview',
                'OMDB Plot': 'omdb_plot',
                'TMDB Tagline': 'tmdb_tagline'
            }

            for label, col in plot_columns.items():
                if col in self.movies_df.columns:
                    non_empty = sum(self.movies_df[col].apply(
                        lambda x: bool(self.clean_text(x))
                    ))
                    pct = (non_empty / len(self.movies_df)) * 100
                    f.write(f"{label}: {non_empty}/{len(self.movies_df)} ({pct:.1f}%)\n")

            # Any plot data
            any_plot = self.movies_df.apply(
                lambda row: any(
                    bool(self.clean_text(row.get(col, None)))
                    for col in plot_columns.values()
                    if col in self.movies_df.columns
                ),
                axis=1
            ).sum()

            any_pct = (any_plot / len(self.movies_df)) * 100
            f.write(f"\nFilms with ANY plot data: {any_plot}/{len(self.movies_df)} ({any_pct:.1f}%)\n")

            f.write("\n" + "=" * 80 + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * 80 + "\n")

        print(f"Report saved to {report_path}")

    def run_analysis(self):
        """Execute all analysis steps."""
        print("\n" + "=" * 80)
        print("CINESCOPE BATCH 25: PLOT & STORY ANALYSIS")
        print("=" * 80 + "\n")

        # Run all visualizations
        self.visualize_plot_coverage()
        self.visualize_plot_length()
        self.visualize_common_keywords()
        self.visualize_tagline_analysis()
        self.visualize_genre_narrative_patterns()
        self.visualize_plot_complexity()
        self.visualize_sentiment_patterns()
        self.visualize_description_source_comparison()
        self.visualize_plot_themes()
        self.visualize_narrative_structure()
        self.visualize_plot_uniqueness()
        self.visualize_runtime_plot_correlation()

        # Generate report
        self.generate_report()

        print("\n" + "=" * 80)
        print("ANALYSIS COMPLETE")
        print("=" * 80)
        print(f"\nVisualizations saved to: {self.viz_dir}")
        print(f"Report saved to: {self.reports_dir}")

if __name__ == "__main__":
    analyzer = PlotStoryAnalyzer()
    analyzer.run_analysis()
