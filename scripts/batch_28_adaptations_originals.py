#!/usr/bin/env python3
"""
CineScope Batch 28: Adaptations & Originals Analysis
Analyzes source materials, adaptations, remakes, sequels, and franchise patterns.
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

class AdaptationsOriginalsAnalyzer:
    def __init__(self):
        self.data_dir = Path('data/processed')
        self.output_dir = Path('analysis_outputs')
        self.viz_dir = self.output_dir / 'visualizations' / 'batch_28'
        self.reports_dir = self.output_dir / 'reports'

        # Create output directories
        self.viz_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)

        # Load data
        self.movies_df = pd.read_csv(self.data_dir / 'watched_movies_master.csv')

        # Setup plotting style
        plt.style.use('default')
        sns.set_palette("husl")

        # Define source material patterns
        self.source_patterns = {
            'Novel': ['based on novel', 'based on book', 'novel', 'book adaptation'],
            'Play': ['based on play', 'based on musical', 'stage play', 'broadway'],
            'True Story': ['based on true story', 'true story', 'biographical', 'biography'],
            'Comic': ['based on comic', 'comic book', 'graphic novel', 'manga', 'superhero'],
            'Video Game': ['video game', 'based on video game'],
            'Remake': ['remake', 'reboot', 'reimagining'],
            'Sequel': ['sequel', 'part ii', 'part 2', ' 2', ' ii'],
            'Short Film': ['based on short film', 'short film'],
            'TV Series': ['based on tv', 'television'],
            'Fairy Tale': ['fairy tale', 'folktale', 'legend']
        }

        print(f"Loaded {len(self.movies_df)} movies for adaptation analysis")

    def classify_source_material(self, keywords_str, title):
        """Classify film's source material based on keywords and title."""
        if pd.isna(keywords_str):
            keywords_lower = ''
        else:
            keywords_lower = str(keywords_str).lower()

        title_lower = str(title).lower()

        sources = []

        for source_type, patterns in self.source_patterns.items():
            for pattern in patterns:
                if pattern in keywords_lower:
                    sources.append(source_type)
                    break

        # Additional title-based detection
        if not sources:
            # Check for sequel patterns in title
            if re.search(r'\b(ii|iii|iv|v|vi|vii|viii|ix|x)\b', title_lower):
                sources.append('Sequel')
            elif re.search(r'\bpart\s+\d+\b', title_lower):
                sources.append('Sequel')
            elif re.search(r':\s+', title):  # Colon often indicates sequel/series
                if len(title.split(':')[1].strip()) > 3:  # Not just a subtitle
                    sources.append('Sequel')

        return sources if sources else ['Original']

    def is_remake(self, keywords_str, title):
        """Check if film is a remake."""
        if pd.isna(keywords_str):
            return False

        keywords_lower = str(keywords_str).lower()
        return any(pattern in keywords_lower for pattern in self.source_patterns['Remake'])

    def is_sequel(self, keywords_str, title):
        """Check if film is a sequel."""
        keywords_lower = str(keywords_str).lower() if pd.notna(keywords_str) else ''
        title_lower = str(title).lower()

        # Check keywords
        if any(pattern in keywords_lower for pattern in self.source_patterns['Sequel']):
            return True

        # Check title patterns
        if re.search(r'\b(ii|iii|iv|v|2|3|4|5|6|7|8|9|10)\b', title_lower):
            return True
        if re.search(r'\bpart\s+\d+\b', title_lower):
            return True

        return False

    def visualize_original_vs_adaptation(self):
        """Analyze original vs adaptation ratio."""
        print("Analyzing original vs adaptation distribution...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Original vs Adaptation Analysis', fontsize=16, fontweight='bold')

        # Classify all films
        adaptation_data = []
        for idx, row in self.movies_df.iterrows():
            keywords = row.get('tmdb_keywords', '')
            title = row.get('Title', '')
            sources = self.classify_source_material(keywords, title)

            is_adaptation = 'Original' not in sources
            adaptation_data.append({
                'title': title,
                'is_adaptation': is_adaptation,
                'sources': sources,
                'rating': row.get('IMDb Rating', 0),
                'year': row.get('Year', 0)
            })

        adapt_df = pd.DataFrame(adaptation_data)

        # Overall pie chart
        ax1 = axes[0, 0]

        counts = adapt_df['is_adaptation'].value_counts()
        labels = ['Adaptations', 'Originals']
        colors = ['#FF6B6B', '#4ECDC4']
        explode = (0.05, 0)

        # Reorder to match labels
        values = [counts.get(True, 0), counts.get(False, 0)]

        ax1.pie(values, labels=labels, autopct='%1.1f%%', colors=colors,
               explode=explode, startangle=90, textprops={'fontsize': 11, 'fontweight': 'bold'})
        ax1.set_title('Original vs Adaptation Distribution')

        # Rating comparison
        ax2 = axes[0, 1]

        original_ratings = adapt_df[~adapt_df['is_adaptation']]['rating']
        adaptation_ratings = adapt_df[adapt_df['is_adaptation']]['rating']

        original_ratings = original_ratings[original_ratings > 0]
        adaptation_ratings = adaptation_ratings[adaptation_ratings > 0]

        if len(original_ratings) > 0 and len(adaptation_ratings) > 0:
            bp = ax2.boxplot([original_ratings, adaptation_ratings],
                            labels=['Originals', 'Adaptations'],
                            patch_artist=True)

            bp['boxes'][0].set_facecolor('#4ECDC4')
            bp['boxes'][1].set_facecolor('#FF6B6B')

            for patch in bp['boxes']:
                patch.set_alpha(0.7)

            ax2.set_ylabel('IMDb Rating')
            ax2.set_title('Quality Comparison')
            ax2.grid(axis='y', alpha=0.3)

            # Add statistical test
            from scipy import stats
            t_stat, p_value = stats.ttest_ind(original_ratings, adaptation_ratings)
            ax2.text(0.5, 0.95, f'p-value: {p_value:.4f}',
                    transform=ax2.transAxes, ha='center', va='top',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        # Evolution over decades
        ax3 = axes[1, 0]

        decade_adaptation = {}
        for decade in range(1920, 2030, 10):
            decade_movies = adapt_df[
                (adapt_df['year'] >= decade) &
                (adapt_df['year'] < decade + 10)
            ]

            if len(decade_movies) > 0:
                pct = (decade_movies['is_adaptation'].sum() / len(decade_movies)) * 100
                decade_adaptation[decade] = pct

        if decade_adaptation:
            decades = list(decade_adaptation.keys())
            percentages = list(decade_adaptation.values())
            decade_labels = [f"{d}s" for d in decades]

            ax3.plot(decade_labels, percentages, marker='o', linewidth=2, markersize=8, color='#FF6B6B')
            ax3.set_xlabel('Decade')
            ax3.set_ylabel('Adaptation Percentage (%)')
            ax3.set_title('Adaptation Prevalence Over Time')
            ax3.grid(alpha=0.3)
            ax3.tick_params(axis='x', rotation=45)

        # Count by number of sources
        ax4 = axes[1, 1]

        source_counts = adapt_df['sources'].apply(len).value_counts().sort_index()

        ax4.bar(source_counts.index, source_counts.values, color='#95E1D3', alpha=0.7, edgecolor='black')
        ax4.set_xlabel('Number of Source Types')
        ax4.set_ylabel('Number of Films')
        ax4.set_title('Films by Source Material Complexity')
        ax4.grid(axis='y', alpha=0.3)

        for i, count in zip(source_counts.index, source_counts.values):
            ax4.text(i, count, str(count), ha='center', va='bottom', fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'original_vs_adaptation.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_source_material_breakdown(self):
        """Analyze different types of source materials."""
        print("Analyzing source material breakdown...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Source Material Breakdown', fontsize=16, fontweight='bold')

        # Count each source type
        source_counts = defaultdict(int)
        source_ratings = defaultdict(list)

        for idx, row in self.movies_df.iterrows():
            keywords = row.get('tmdb_keywords', '')
            title = row.get('Title', '')
            sources = self.classify_source_material(keywords, title)
            rating = row.get('IMDb Rating', 0)

            for source in sources:
                if source != 'Original':
                    source_counts[source] += 1
                    if rating > 0:
                        source_ratings[source].append(rating)

        # Overall source distribution
        ax1 = axes[0, 0]

        sorted_sources = sorted(source_counts.items(), key=lambda x: x[1], reverse=True)
        if sorted_sources:
            sources, counts = zip(*sorted_sources)
            colors = plt.cm.Spectral(np.linspace(0.2, 0.8, len(sources)))

            ax1.barh(range(len(sources)), counts, color=colors, alpha=0.7, edgecolor='black')
            ax1.set_yticks(range(len(sources)))
            ax1.set_yticklabels(sources)
            ax1.invert_yaxis()
            ax1.set_xlabel('Number of Films')
            ax1.set_title('Adaptation Source Types')
            ax1.grid(axis='x', alpha=0.3)

            for i, count in enumerate(counts):
                pct = (count / len(self.movies_df)) * 100
                ax1.text(count, i, f' {count} ({pct:.1f}%)', va='center', fontsize=9)

        # Pie chart of top sources
        ax2 = axes[0, 1]

        top_sources = sorted_sources[:6] if len(sorted_sources) >= 6 else sorted_sources
        if top_sources:
            labels, values = zip(*top_sources)
            colors = plt.cm.Set3(np.linspace(0, 1, len(labels)))

            ax2.pie(values, labels=labels, autopct='%1.1f%%', colors=colors, startangle=90)
            ax2.set_title('Top Source Materials (Distribution)')

        # Average rating by source type
        ax3 = axes[1, 0]

        source_avg_ratings = []
        for source, ratings in source_ratings.items():
            if len(ratings) >= 3:
                source_avg_ratings.append((source, np.mean(ratings), len(ratings)))

        source_avg_ratings.sort(key=lambda x: x[1], reverse=True)

        if source_avg_ratings:
            sources, avg_ratings, counts = zip(*source_avg_ratings)
            colors = plt.cm.RdYlGn(np.array(avg_ratings) / 10)

            ax3.barh(range(len(sources)), avg_ratings, color=colors, alpha=0.7, edgecolor='black')
            ax3.set_yticks(range(len(sources)))
            ax3.set_yticklabels([f"{s} (n={c})" for s, c in zip(sources, counts)], fontsize=9)
            ax3.invert_yaxis()
            ax3.set_xlabel('Average IMDb Rating')
            ax3.set_title('Quality by Source Material')
            ax3.set_xlim(0, 10)
            ax3.grid(axis='x', alpha=0.3)

            for i, rating in enumerate(avg_ratings):
                ax3.text(rating, i, f' {rating:.2f}', va='center', fontsize=9)

        # Source material by decade
        ax4 = axes[1, 1]

        # Track top 4 sources over time
        top_4_sources = [s[0] for s in sorted_sources[:4]]
        decade_source_data = {source: {} for source in top_4_sources}

        for decade in range(1920, 2030, 10):
            decade_movies = self.movies_df[
                (self.movies_df['Year'] >= decade) &
                (self.movies_df['Year'] < decade + 10)
            ]

            for idx, row in decade_movies.iterrows():
                keywords = row.get('tmdb_keywords', '')
                title = row.get('Title', '')
                sources = self.classify_source_material(keywords, title)

                for source in sources:
                    if source in top_4_sources:
                        if decade not in decade_source_data[source]:
                            decade_source_data[source][decade] = 0
                        decade_source_data[source][decade] += 1

        for source in top_4_sources:
            if decade_source_data[source]:
                decades = sorted(decade_source_data[source].keys())
                counts = [decade_source_data[source][d] for d in decades]
                decade_labels = [f"{d}s" for d in decades]
                ax4.plot(decade_labels, counts, marker='o', label=source, linewidth=2)

        ax4.set_xlabel('Decade')
        ax4.set_ylabel('Number of Films')
        ax4.set_title('Source Material Evolution')
        ax4.legend(fontsize=8)
        ax4.grid(alpha=0.3)
        ax4.tick_params(axis='x', rotation=45)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'source_material_breakdown.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_remake_analysis(self):
        """Analyze remake patterns."""
        print("Analyzing remakes...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Remake Analysis', fontsize=16, fontweight='bold')

        # Identify remakes
        remake_data = []
        for idx, row in self.movies_df.iterrows():
            keywords = row.get('tmdb_keywords', '')
            title = row.get('Title', '')
            is_remake_flag = self.is_remake(keywords, title)

            remake_data.append({
                'title': title,
                'is_remake': is_remake_flag,
                'rating': row.get('IMDb Rating', 0),
                'year': row.get('Year', 0),
                'genre': row.get('Genres', '').split('|')[0] if pd.notna(row.get('Genres')) else 'Unknown'
            })

        remake_df = pd.DataFrame(remake_data)

        # Remake vs original distribution
        ax1 = axes[0, 0]

        remake_counts = remake_df['is_remake'].value_counts()
        labels = ['Originals', 'Remakes']
        colors = ['#4ECDC4', '#FFD93D']

        values = [remake_counts.get(False, 0), remake_counts.get(True, 0)]

        ax1.pie(values, labels=labels, autopct='%1.1f%%', colors=colors, startangle=90,
               textprops={'fontsize': 11, 'fontweight': 'bold'})
        ax1.set_title('Remake Distribution')

        # Remakes over time
        ax2 = axes[0, 1]

        decade_remakes = {}
        for decade in range(1920, 2030, 10):
            decade_movies = remake_df[
                (remake_df['year'] >= decade) &
                (remake_df['year'] < decade + 10)
            ]

            if len(decade_movies) > 0:
                remake_count = decade_movies['is_remake'].sum()
                decade_remakes[decade] = remake_count

        if decade_remakes:
            decades = list(decade_remakes.keys())
            counts = list(decade_remakes.values())
            decade_labels = [f"{d}s" for d in decades]

            ax2.bar(range(len(decades)), counts, color='#FFD93D', alpha=0.7, edgecolor='black')
            ax2.set_xticks(range(len(decades)))
            ax2.set_xticklabels(decade_labels, rotation=45)
            ax2.set_ylabel('Number of Remakes')
            ax2.set_title('Remakes by Decade')
            ax2.grid(axis='y', alpha=0.3)

        # Rating comparison
        ax3 = axes[1, 0]

        remake_ratings = remake_df[remake_df['is_remake']]['rating']
        original_ratings = remake_df[~remake_df['is_remake']]['rating']

        remake_ratings = remake_ratings[remake_ratings > 0]
        original_ratings = original_ratings[original_ratings > 0]

        if len(remake_ratings) > 0 and len(original_ratings) > 0:
            bp = ax3.boxplot([original_ratings, remake_ratings],
                            labels=['Originals', 'Remakes'],
                            patch_artist=True)

            bp['boxes'][0].set_facecolor('#4ECDC4')
            bp['boxes'][1].set_facecolor('#FFD93D')

            for patch in bp['boxes']:
                patch.set_alpha(0.7)

            ax3.set_ylabel('IMDb Rating')
            ax3.set_title('Quality Comparison: Originals vs Remakes')
            ax3.grid(axis='y', alpha=0.3)

            # Statistical test
            from scipy import stats
            if len(remake_ratings) >= 2 and len(original_ratings) >= 2:
                t_stat, p_value = stats.ttest_ind(original_ratings, remake_ratings)
                ax3.text(0.5, 0.95, f'p-value: {p_value:.4f}',
                        transform=ax3.transAxes, ha='center', va='top',
                        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        # Top-rated remakes
        ax4 = axes[1, 1]

        top_remakes = remake_df[remake_df['is_remake']].nlargest(15, 'rating')

        if len(top_remakes) > 0:
            colors = plt.cm.YlOrRd(np.linspace(0.3, 0.8, len(top_remakes)))

            ax4.barh(range(len(top_remakes)), top_remakes['rating'],
                    color=colors, alpha=0.7, edgecolor='black')
            ax4.set_yticks(range(len(top_remakes)))
            ax4.set_yticklabels([f"{t[:22]} ({y})" for t, y in
                                zip(top_remakes['title'], top_remakes['year'])], fontsize=8)
            ax4.invert_yaxis()
            ax4.set_xlabel('IMDb Rating')
            ax4.set_title('Top-Rated Remakes')
            ax4.set_xlim(0, 10)
            ax4.grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'remake_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_sequel_analysis(self):
        """Analyze sequel patterns."""
        print("Analyzing sequels...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Sequel Analysis', fontsize=16, fontweight='bold')

        # Identify sequels
        sequel_data = []
        for idx, row in self.movies_df.iterrows():
            keywords = row.get('tmdb_keywords', '')
            title = row.get('Title', '')
            is_sequel_flag = self.is_sequel(keywords, title)

            sequel_data.append({
                'title': title,
                'is_sequel': is_sequel_flag,
                'rating': row.get('IMDb Rating', 0),
                'year': row.get('Year', 0)
            })

        sequel_df = pd.DataFrame(sequel_data)

        # Sequel distribution
        ax1 = axes[0, 0]

        sequel_counts = sequel_df['is_sequel'].value_counts()
        labels = ['Non-Sequels', 'Sequels']
        colors = ['#95E1D3', '#F38181']

        values = [sequel_counts.get(False, 0), sequel_counts.get(True, 0)]

        ax1.pie(values, labels=labels, autopct='%1.1f%%', colors=colors, startangle=90,
               textprops={'fontsize': 11, 'fontweight': 'bold'})
        ax1.set_title('Sequel Distribution')

        # Sequels over time
        ax2 = axes[0, 1]

        decade_sequels = {}
        for decade in range(1920, 2030, 10):
            decade_movies = sequel_df[
                (sequel_df['year'] >= decade) &
                (sequel_df['year'] < decade + 10)
            ]

            if len(decade_movies) > 0:
                pct = (decade_movies['is_sequel'].sum() / len(decade_movies)) * 100
                decade_sequels[decade] = pct

        if decade_sequels:
            decades = list(decade_sequels.keys())
            percentages = list(decade_sequels.values())
            decade_labels = [f"{d}s" for d in decades]

            ax2.plot(decade_labels, percentages, marker='o', linewidth=2, markersize=8, color='#F38181')
            ax2.set_xlabel('Decade')
            ax2.set_ylabel('Sequel Percentage (%)')
            ax2.set_title('Sequel Prevalence Over Time')
            ax2.grid(alpha=0.3)
            ax2.tick_params(axis='x', rotation=45)

        # Rating comparison
        ax3 = axes[1, 0]

        sequel_ratings = sequel_df[sequel_df['is_sequel']]['rating']
        non_sequel_ratings = sequel_df[~sequel_df['is_sequel']]['rating']

        sequel_ratings = sequel_ratings[sequel_ratings > 0]
        non_sequel_ratings = non_sequel_ratings[non_sequel_ratings > 0]

        if len(sequel_ratings) > 0 and len(non_sequel_ratings) > 0:
            bp = ax3.boxplot([non_sequel_ratings, sequel_ratings],
                            labels=['Non-Sequels', 'Sequels'],
                            patch_artist=True)

            bp['boxes'][0].set_facecolor('#95E1D3')
            bp['boxes'][1].set_facecolor('#F38181')

            for patch in bp['boxes']:
                patch.set_alpha(0.7)

            ax3.set_ylabel('IMDb Rating')
            ax3.set_title('Quality Comparison')
            ax3.grid(axis='y', alpha=0.3)

            # Statistical test
            from scipy import stats
            if len(sequel_ratings) >= 2 and len(non_sequel_ratings) >= 2:
                t_stat, p_value = stats.ttest_ind(non_sequel_ratings, sequel_ratings)
                result = "Sequels ARE" if np.mean(sequel_ratings) > np.mean(non_sequel_ratings) else "Sequels are NOT"
                ax3.text(0.5, 0.95, f'{result} better\np-value: {p_value:.4f}',
                        transform=ax3.transAxes, ha='center', va='top',
                        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        # Top-rated sequels
        ax4 = axes[1, 1]

        top_sequels = sequel_df[sequel_df['is_sequel']].nlargest(15, 'rating')

        if len(top_sequels) > 0:
            colors = plt.cm.Reds(np.linspace(0.3, 0.8, len(top_sequels)))

            ax4.barh(range(len(top_sequels)), top_sequels['rating'],
                    color=colors, alpha=0.7, edgecolor='black')
            ax4.set_yticks(range(len(top_sequels)))
            ax4.set_yticklabels([f"{t[:25]}" for t in top_sequels['title']], fontsize=8)
            ax4.invert_yaxis()
            ax4.set_xlabel('IMDb Rating')
            ax4.set_title('Top-Rated Sequels')
            ax4.set_xlim(0, 10)
            ax4.grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'sequel_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_book_adaptations(self):
        """Analyze book and novel adaptations."""
        print("Analyzing book adaptations...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Book & Novel Adaptations', fontsize=16, fontweight='bold')

        # Identify book adaptations
        book_data = []
        for idx, row in self.movies_df.iterrows():
            keywords = row.get('tmdb_keywords', '')
            if pd.isna(keywords):
                keywords_lower = ''
            else:
                keywords_lower = str(keywords).lower()

            is_book = any(pattern in keywords_lower for pattern in
                         ['based on novel', 'based on book', 'novel', 'book adaptation'])

            book_data.append({
                'title': row.get('Title', ''),
                'is_book': is_book,
                'rating': row.get('IMDb Rating', 0),
                'year': row.get('Year', 0),
                'genre': row.get('Genres', '').split('|')[0] if pd.notna(row.get('Genres')) else 'Unknown'
            })

        book_df = pd.DataFrame(book_data)

        # Book adaptation distribution
        ax1 = axes[0, 0]

        book_counts = book_df['is_book'].value_counts()
        labels = ['Non-Book Films', 'Book Adaptations']
        colors = ['#A8DADC', '#E63946']

        values = [book_counts.get(False, 0), book_counts.get(True, 0)]

        ax1.pie(values, labels=labels, autopct='%1.1f%%', colors=colors, startangle=90,
               textprops={'fontsize': 11, 'fontweight': 'bold'})
        ax1.set_title('Book Adaptation Distribution')

        # Book adaptations over time
        ax2 = axes[0, 1]

        decade_books = {}
        for decade in range(1920, 2030, 10):
            decade_movies = book_df[
                (book_df['year'] >= decade) &
                (book_df['year'] < decade + 10)
            ]

            if len(decade_movies) > 0:
                book_count = decade_movies['is_book'].sum()
                decade_books[decade] = book_count

        if decade_books:
            decades = list(decade_books.keys())
            counts = list(decade_books.values())
            decade_labels = [f"{d}s" for d in decades]

            ax2.bar(range(len(decades)), counts, color='#E63946', alpha=0.7, edgecolor='black')
            ax2.set_xticks(range(len(decades)))
            ax2.set_xticklabels(decade_labels, rotation=45)
            ax2.set_ylabel('Number of Book Adaptations')
            ax2.set_title('Book Adaptations by Decade')
            ax2.grid(axis='y', alpha=0.3)

        # Rating comparison
        ax3 = axes[1, 0]

        book_ratings = book_df[book_df['is_book']]['rating']
        non_book_ratings = book_df[~book_df['is_book']]['rating']

        book_ratings = book_ratings[book_ratings > 0]
        non_book_ratings = non_book_ratings[non_book_ratings > 0]

        if len(book_ratings) > 0 and len(non_book_ratings) > 0:
            bp = ax3.boxplot([non_book_ratings, book_ratings],
                            labels=['Non-Book', 'Book Adaptation'],
                            patch_artist=True)

            bp['boxes'][0].set_facecolor('#A8DADC')
            bp['boxes'][1].set_facecolor('#E63946')

            for patch in bp['boxes']:
                patch.set_alpha(0.7)

            ax3.set_ylabel('IMDb Rating')
            ax3.set_title('Quality: Book Adaptations vs Others')
            ax3.grid(axis='y', alpha=0.3)

            # Stats
            from scipy import stats
            t_stat, p_value = stats.ttest_ind(non_book_ratings, book_ratings)
            avg_book = np.mean(book_ratings)
            avg_non = np.mean(non_book_ratings)
            diff = avg_book - avg_non
            ax3.text(0.5, 0.05, f'Book: {avg_book:.2f}, Non-Book: {avg_non:.2f}\nDiff: {diff:+.2f}, p={p_value:.4f}',
                    transform=ax3.transAxes, ha='center', va='bottom',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5), fontsize=9)

        # Top-rated book adaptations
        ax4 = axes[1, 1]

        top_books = book_df[book_df['is_book']].nlargest(15, 'rating')

        if len(top_books) > 0:
            colors = plt.cm.Reds(np.linspace(0.3, 0.8, len(top_books)))

            ax4.barh(range(len(top_books)), top_books['rating'],
                    color=colors, alpha=0.7, edgecolor='black')
            ax4.set_yticks(range(len(top_books)))
            ax4.set_yticklabels([f"{t[:23]} ({y})" for t, y in
                                zip(top_books['title'], top_books['year'])], fontsize=8)
            ax4.invert_yaxis()
            ax4.set_xlabel('IMDb Rating')
            ax4.set_title('Top-Rated Book Adaptations')
            ax4.set_xlim(0, 10)
            ax4.grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'book_adaptations.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_true_story_films(self):
        """Analyze true story and biographical films."""
        print("Analyzing true story films...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('True Story & Biographical Films', fontsize=16, fontweight='bold')

        # Identify true story films
        true_story_data = []
        for idx, row in self.movies_df.iterrows():
            keywords = row.get('tmdb_keywords', '')
            if pd.isna(keywords):
                keywords_lower = ''
            else:
                keywords_lower = str(keywords).lower()

            is_true_story = any(pattern in keywords_lower for pattern in
                               ['based on true story', 'true story', 'biographical', 'biography', 'biopic'])

            true_story_data.append({
                'title': row.get('Title', ''),
                'is_true_story': is_true_story,
                'rating': row.get('IMDb Rating', 0),
                'year': row.get('Year', 0),
                'genre': row.get('Genres', '').split('|')[0] if pd.notna(row.get('Genres')) else 'Unknown'
            })

        true_df = pd.DataFrame(true_story_data)

        # Distribution
        ax1 = axes[0, 0]

        true_counts = true_df['is_true_story'].value_counts()
        labels = ['Fiction', 'Based on True Story']
        colors = ['#457B9D', '#E76F51']

        values = [true_counts.get(False, 0), true_counts.get(True, 0)]

        ax1.pie(values, labels=labels, autopct='%1.1f%%', colors=colors, startangle=90,
               textprops={'fontsize': 11, 'fontweight': 'bold'})
        ax1.set_title('True Story Distribution')

        # True story films over time
        ax2 = axes[0, 1]

        decade_true = {}
        for decade in range(1920, 2030, 10):
            decade_movies = true_df[
                (true_df['year'] >= decade) &
                (true_df['year'] < decade + 10)
            ]

            if len(decade_movies) > 0:
                pct = (decade_movies['is_true_story'].sum() / len(decade_movies)) * 100
                decade_true[decade] = pct

        if decade_true:
            decades = list(decade_true.keys())
            percentages = list(decade_true.values())
            decade_labels = [f"{d}s" for d in decades]

            ax2.plot(decade_labels, percentages, marker='o', linewidth=2, markersize=8, color='#E76F51')
            ax2.set_xlabel('Decade')
            ax2.set_ylabel('True Story Films (%)')
            ax2.set_title('True Story Film Prevalence Over Time')
            ax2.grid(alpha=0.3)
            ax2.tick_params(axis='x', rotation=45)

        # Rating comparison
        ax3 = axes[1, 0]

        true_ratings = true_df[true_df['is_true_story']]['rating']
        fiction_ratings = true_df[~true_df['is_true_story']]['rating']

        true_ratings = true_ratings[true_ratings > 0]
        fiction_ratings = fiction_ratings[fiction_ratings > 0]

        if len(true_ratings) > 0 and len(fiction_ratings) > 0:
            bp = ax3.boxplot([fiction_ratings, true_ratings],
                            labels=['Fiction', 'True Story'],
                            patch_artist=True)

            bp['boxes'][0].set_facecolor('#457B9D')
            bp['boxes'][1].set_facecolor('#E76F51')

            for patch in bp['boxes']:
                patch.set_alpha(0.7)

            ax3.set_ylabel('IMDb Rating')
            ax3.set_title('Quality: True Story vs Fiction')
            ax3.grid(axis='y', alpha=0.3)

            # Stats
            from scipy import stats
            t_stat, p_value = stats.ttest_ind(fiction_ratings, true_ratings)
            ax3.text(0.5, 0.95, f'p-value: {p_value:.4f}',
                    transform=ax3.transAxes, ha='center', va='top',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        # Top-rated true story films
        ax4 = axes[1, 1]

        top_true = true_df[true_df['is_true_story']].nlargest(15, 'rating')

        if len(top_true) > 0:
            colors = plt.cm.Oranges(np.linspace(0.3, 0.8, len(top_true)))

            ax4.barh(range(len(top_true)), top_true['rating'],
                    color=colors, alpha=0.7, edgecolor='black')
            ax4.set_yticks(range(len(top_true)))
            ax4.set_yticklabels([f"{t[:23]} ({y})" for t, y in
                                zip(top_true['title'], top_true['year'])], fontsize=8)
            ax4.invert_yaxis()
            ax4.set_xlabel('IMDb Rating')
            ax4.set_title('Top-Rated True Story Films')
            ax4.set_xlim(0, 10)
            ax4.grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'true_story_films.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_comic_adaptations(self):
        """Analyze comic book and superhero adaptations."""
        print("Analyzing comic book adaptations...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Comic Book & Superhero Adaptations', fontsize=16, fontweight='bold')

        # Identify comic adaptations
        comic_data = []
        for idx, row in self.movies_df.iterrows():
            keywords = row.get('tmdb_keywords', '')
            if pd.isna(keywords):
                keywords_lower = ''
            else:
                keywords_lower = str(keywords).lower()

            is_comic = any(pattern in keywords_lower for pattern in
                          ['based on comic', 'comic book', 'superhero', 'graphic novel', 'marvel', 'dc comics'])

            comic_data.append({
                'title': row.get('Title', ''),
                'is_comic': is_comic,
                'rating': row.get('IMDb Rating', 0),
                'year': row.get('Year', 0)
            })

        comic_df = pd.DataFrame(comic_data)

        # Distribution
        ax1 = axes[0, 0]

        comic_counts = comic_df['is_comic'].value_counts()
        labels = ['Non-Comic Films', 'Comic Adaptations']
        colors = ['#6C757D', '#DC3545']

        values = [comic_counts.get(False, 0), comic_counts.get(True, 0)]

        ax1.pie(values, labels=labels, autopct='%1.1f%%', colors=colors, startangle=90,
               textprops={'fontsize': 11, 'fontweight': 'bold'})
        ax1.set_title('Comic Book Adaptation Distribution')

        # Comic adaptations over time
        ax2 = axes[0, 1]

        decade_comic = {}
        for decade in range(1920, 2030, 10):
            decade_movies = comic_df[
                (comic_df['year'] >= decade) &
                (comic_df['year'] < decade + 10)
            ]

            if len(decade_movies) > 0:
                comic_count = decade_movies['is_comic'].sum()
                decade_comic[decade] = comic_count

        if decade_comic:
            decades = list(decade_comic.keys())
            counts = list(decade_comic.values())
            decade_labels = [f"{d}s" for d in decades]

            ax2.bar(range(len(decades)), counts, color='#DC3545', alpha=0.7, edgecolor='black')
            ax2.set_xticks(range(len(decades)))
            ax2.set_xticklabels(decade_labels, rotation=45)
            ax2.set_ylabel('Number of Comic Adaptations')
            ax2.set_title('Comic Book Adaptations by Decade')
            ax2.grid(axis='y', alpha=0.3)

        # Rating comparison
        ax3 = axes[1, 0]

        comic_ratings = comic_df[comic_df['is_comic']]['rating']
        non_comic_ratings = comic_df[~comic_df['is_comic']]['rating']

        comic_ratings = comic_ratings[comic_ratings > 0]
        non_comic_ratings = non_comic_ratings[non_comic_ratings > 0]

        if len(comic_ratings) > 0 and len(non_comic_ratings) > 0:
            bp = ax3.boxplot([non_comic_ratings, comic_ratings],
                            labels=['Non-Comic', 'Comic Adaptation'],
                            patch_artist=True)

            bp['boxes'][0].set_facecolor('#6C757D')
            bp['boxes'][1].set_facecolor('#DC3545')

            for patch in bp['boxes']:
                patch.set_alpha(0.7)

            ax3.set_ylabel('IMDb Rating')
            ax3.set_title('Quality: Comic Adaptations vs Others')
            ax3.grid(axis='y', alpha=0.3)

        # Top-rated comic adaptations
        ax4 = axes[1, 1]

        top_comic = comic_df[comic_df['is_comic']].nlargest(15, 'rating')

        if len(top_comic) > 0:
            colors = plt.cm.Reds(np.linspace(0.3, 0.8, len(top_comic)))

            ax4.barh(range(len(top_comic)), top_comic['rating'],
                    color=colors, alpha=0.7, edgecolor='black')
            ax4.set_yticks(range(len(top_comic)))
            ax4.set_yticklabels([f"{t[:23]} ({y})" for t, y in
                                zip(top_comic['title'], top_comic['year'])], fontsize=8)
            ax4.invert_yaxis()
            ax4.set_xlabel('IMDb Rating')
            ax4.set_title('Top-Rated Comic Book Adaptations')
            ax4.set_xlim(0, 10)
            ax4.grid(axis='x', alpha=0.3)
        else:
            ax4.text(0.5, 0.5, 'No comic book adaptations found',
                    ha='center', va='center', transform=ax4.transAxes, fontsize=14)
            ax4.axis('off')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'comic_adaptations.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_play_adaptations(self):
        """Analyze play and musical adaptations."""
        print("Analyzing play adaptations...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Play & Musical Adaptations', fontsize=16, fontweight='bold')

        # Identify play adaptations
        play_data = []
        for idx, row in self.movies_df.iterrows():
            keywords = row.get('tmdb_keywords', '')
            if pd.isna(keywords):
                keywords_lower = ''
            else:
                keywords_lower = str(keywords).lower()

            is_play = any(pattern in keywords_lower for pattern in
                         ['based on play', 'based on musical', 'stage play', 'broadway', 'theatre'])

            play_data.append({
                'title': row.get('Title', ''),
                'is_play': is_play,
                'rating': row.get('IMDb Rating', 0),
                'year': row.get('Year', 0)
            })

        play_df = pd.DataFrame(play_data)

        # Distribution
        ax1 = axes[0, 0]

        play_counts = play_df['is_play'].value_counts()
        labels = ['Non-Play Films', 'Play Adaptations']
        colors = ['#A8DADC', '#F4A261']

        values = [play_counts.get(False, 0), play_counts.get(True, 0)]

        ax1.pie(values, labels=labels, autopct='%1.1f%%', colors=colors, startangle=90,
               textprops={'fontsize': 11, 'fontweight': 'bold'})
        ax1.set_title('Play Adaptation Distribution')

        # Play adaptations over time
        ax2 = axes[0, 1]

        decade_play = {}
        for decade in range(1920, 2030, 10):
            decade_movies = play_df[
                (play_df['year'] >= decade) &
                (play_df['year'] < decade + 10)
            ]

            if len(decade_movies) > 0:
                play_count = decade_movies['is_play'].sum()
                decade_play[decade] = play_count

        if decade_play:
            decades = list(decade_play.keys())
            counts = list(decade_play.values())
            decade_labels = [f"{d}s" for d in decades]

            ax2.bar(range(len(decades)), counts, color='#F4A261', alpha=0.7, edgecolor='black')
            ax2.set_xticks(range(len(decades)))
            ax2.set_xticklabels(decade_labels, rotation=45)
            ax2.set_ylabel('Number of Play Adaptations')
            ax2.set_title('Play Adaptations by Decade')
            ax2.grid(axis='y', alpha=0.3)

        # Rating comparison
        ax3 = axes[1, 0]

        play_ratings = play_df[play_df['is_play']]['rating']
        non_play_ratings = play_df[~play_df['is_play']]['rating']

        play_ratings = play_ratings[play_ratings > 0]
        non_play_ratings = non_play_ratings[non_play_ratings > 0]

        if len(play_ratings) > 0 and len(non_play_ratings) > 0:
            bp = ax3.boxplot([non_play_ratings, play_ratings],
                            labels=['Non-Play', 'Play Adaptation'],
                            patch_artist=True)

            bp['boxes'][0].set_facecolor('#A8DADC')
            bp['boxes'][1].set_facecolor('#F4A261')

            for patch in bp['boxes']:
                patch.set_alpha(0.7)

            ax3.set_ylabel('IMDb Rating')
            ax3.set_title('Quality: Play Adaptations vs Others')
            ax3.grid(axis='y', alpha=0.3)

        # Top-rated play adaptations
        ax4 = axes[1, 1]

        top_play = play_df[play_df['is_play']].nlargest(15, 'rating')

        if len(top_play) > 0:
            colors = plt.cm.YlOrBr(np.linspace(0.3, 0.8, len(top_play)))

            ax4.barh(range(len(top_play)), top_play['rating'],
                    color=colors, alpha=0.7, edgecolor='black')
            ax4.set_yticks(range(len(top_play)))
            ax4.set_yticklabels([f"{t[:23]} ({y})" for t, y in
                                zip(top_play['title'], top_play['year'])], fontsize=8)
            ax4.invert_yaxis()
            ax4.set_xlabel('IMDb Rating')
            ax4.set_title('Top-Rated Play Adaptations')
            ax4.set_xlim(0, 10)
            ax4.grid(axis='x', alpha=0.3)
        else:
            ax4.text(0.5, 0.5, 'No play adaptations found',
                    ha='center', va='center', transform=ax4.transAxes, fontsize=14)
            ax4.axis('off')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'play_adaptations.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_franchise_patterns(self):
        """Analyze franchise patterns using collection data."""
        print("Analyzing franchise patterns...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Franchise Patterns Analysis', fontsize=16, fontweight='bold')

        # Use collection data
        franchise_data = []
        for idx, row in self.movies_df.iterrows():
            collection = row.get('tmdb_belongs_to_collection', '')
            has_franchise = pd.notna(collection) and str(collection).lower() != 'nan' and str(collection).strip() != ''

            franchise_data.append({
                'title': row.get('Title', ''),
                'has_franchise': has_franchise,
                'collection': str(collection) if has_franchise else 'Standalone',
                'rating': row.get('IMDb Rating', 0),
                'year': row.get('Year', 0)
            })

        franchise_df = pd.DataFrame(franchise_data)

        # Franchise distribution
        ax1 = axes[0, 0]

        franchise_counts = franchise_df['has_franchise'].value_counts()
        labels = ['Standalone', 'Part of Franchise']
        colors = ['#90E0EF', '#0077B6']

        values = [franchise_counts.get(False, 0), franchise_counts.get(True, 0)]

        ax1.pie(values, labels=labels, autopct='%1.1f%%', colors=colors, startangle=90,
               textprops={'fontsize': 11, 'fontweight': 'bold'})
        ax1.set_title('Franchise vs Standalone Distribution')

        # Franchises over time
        ax2 = axes[0, 1]

        decade_franchise = {}
        for decade in range(1920, 2030, 10):
            decade_movies = franchise_df[
                (franchise_df['year'] >= decade) &
                (franchise_df['year'] < decade + 10)
            ]

            if len(decade_movies) > 0:
                pct = (decade_movies['has_franchise'].sum() / len(decade_movies)) * 100
                decade_franchise[decade] = pct

        if decade_franchise:
            decades = list(decade_franchise.keys())
            percentages = list(decade_franchise.values())
            decade_labels = [f"{d}s" for d in decades]

            ax2.plot(decade_labels, percentages, marker='o', linewidth=2, markersize=8, color='#0077B6')
            ax2.set_xlabel('Decade')
            ax2.set_ylabel('Franchise Films (%)')
            ax2.set_title('Franchise Film Prevalence Over Time')
            ax2.grid(alpha=0.3)
            ax2.tick_params(axis='x', rotation=45)

        # Rating comparison
        ax3 = axes[1, 0]

        franchise_ratings = franchise_df[franchise_df['has_franchise']]['rating']
        standalone_ratings = franchise_df[~franchise_df['has_franchise']]['rating']

        franchise_ratings = franchise_ratings[franchise_ratings > 0]
        standalone_ratings = standalone_ratings[standalone_ratings > 0]

        if len(franchise_ratings) > 0 and len(standalone_ratings) > 0:
            bp = ax3.boxplot([standalone_ratings, franchise_ratings],
                            labels=['Standalone', 'Franchise'],
                            patch_artist=True)

            bp['boxes'][0].set_facecolor('#90E0EF')
            bp['boxes'][1].set_facecolor('#0077B6')

            for patch in bp['boxes']:
                patch.set_alpha(0.7)

            ax3.set_ylabel('IMDb Rating')
            ax3.set_title('Quality: Standalone vs Franchise')
            ax3.grid(axis='y', alpha=0.3)

            # Stats
            from scipy import stats
            t_stat, p_value = stats.ttest_ind(standalone_ratings, franchise_ratings)
            ax3.text(0.5, 0.95, f'p-value: {p_value:.4f}',
                    transform=ax3.transAxes, ha='center', va='top',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        # Top franchises by film count
        ax4 = axes[1, 1]

        franchise_films = franchise_df[franchise_df['has_franchise']]
        top_franchises = franchise_films['collection'].value_counts().head(15)

        if len(top_franchises) > 0:
            colors = plt.cm.Blues(np.linspace(0.3, 0.8, len(top_franchises)))

            ax4.barh(range(len(top_franchises)), top_franchises.values,
                    color=colors, alpha=0.7, edgecolor='black')
            ax4.set_yticks(range(len(top_franchises)))
            ax4.set_yticklabels([f[:28] for f in top_franchises.index], fontsize=8)
            ax4.invert_yaxis()
            ax4.set_xlabel('Number of Films in Collection')
            ax4.set_title('Largest Franchises in Collection')
            ax4.grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'franchise_patterns.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_adaptation_genre_patterns(self):
        """Analyze adaptation patterns by genre."""
        print("Analyzing adaptation patterns by genre...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Adaptation Patterns by Genre', fontsize=16, fontweight='bold')

        # Build genre-adaptation matrix
        genre_adaptations = defaultdict(lambda: {'total': 0, 'adaptations': 0, 'sources': defaultdict(int)})

        for idx, row in self.movies_df.iterrows():
            genres_str = row.get('Genres', '')
            keywords = row.get('tmdb_keywords', '')
            title = row.get('Title', '')
            sources = self.classify_source_material(keywords, title)

            if pd.notna(genres_str):
                genres = [g.strip() for g in str(genres_str).split('|')]

                for genre in genres:
                    if genre:
                        genre_adaptations[genre]['total'] += 1
                        if 'Original' not in sources:
                            genre_adaptations[genre]['adaptations'] += 1
                            for source in sources:
                                genre_adaptations[genre]['sources'][source] += 1

        # Adaptation percentage by genre
        ax1 = axes[0, 0]

        genre_adapt_pct = []
        for genre, data in genre_adaptations.items():
            if data['total'] >= 10:
                pct = (data['adaptations'] / data['total']) * 100
                genre_adapt_pct.append((genre, pct, data['total']))

        genre_adapt_pct.sort(key=lambda x: x[1], reverse=True)
        top_genres = genre_adapt_pct[:12]

        if top_genres:
            genres, pcts, totals = zip(*top_genres)
            colors = plt.cm.RdYlGn_r(np.array(pcts) / 100)

            ax1.barh(range(len(genres)), pcts, color=colors, alpha=0.7, edgecolor='black')
            ax1.set_yticks(range(len(genres)))
            ax1.set_yticklabels([f"{g} (n={t})" for g, t in zip(genres, totals)], fontsize=9)
            ax1.invert_yaxis()
            ax1.set_xlabel('Adaptation Percentage (%)')
            ax1.set_title('Genres Most Likely to Be Adaptations')
            ax1.grid(axis='x', alpha=0.3)

        # Source material by genre (heatmap)
        ax2 = axes[0, 1]

        # Get top genres and sources
        top_genres_list = [g[0] for g in genre_adapt_pct[:8]]
        top_sources = ['Novel', 'Play', 'True Story', 'Comic']

        # Build matrix
        matrix = np.zeros((len(top_genres_list), len(top_sources)))
        for i, genre in enumerate(top_genres_list):
            total = genre_adaptations[genre]['total']
            for j, source in enumerate(top_sources):
                count = genre_adaptations[genre]['sources'][source]
                matrix[i, j] = (count / total * 100) if total > 0 else 0

        im = ax2.imshow(matrix, cmap='YlOrRd', aspect='auto')
        ax2.set_xticks(range(len(top_sources)))
        ax2.set_yticks(range(len(top_genres_list)))
        ax2.set_xticklabels(top_sources, rotation=45, ha='right')
        ax2.set_yticklabels(top_genres_list, fontsize=9)
        ax2.set_title('Source Material Prevalence by Genre (%)')
        plt.colorbar(im, ax=ax2)

        # Book adaptations by genre
        ax3 = axes[1, 0]

        genre_book_pct = []
        for genre, data in genre_adaptations.items():
            if data['total'] >= 10:
                book_count = data['sources']['Novel']
                pct = (book_count / data['total']) * 100
                if pct > 0:
                    genre_book_pct.append((genre, pct, data['total']))

        genre_book_pct.sort(key=lambda x: x[1], reverse=True)
        top_book_genres = genre_book_pct[:10]

        if top_book_genres:
            genres, pcts, totals = zip(*top_book_genres)
            colors = plt.cm.Reds(np.array(pcts) / max(pcts))

            ax3.barh(range(len(genres)), pcts, color=colors, alpha=0.7, edgecolor='black')
            ax3.set_yticks(range(len(genres)))
            ax3.set_yticklabels([f"{g} (n={t})" for g, t in zip(genres, totals)], fontsize=9)
            ax3.invert_yaxis()
            ax3.set_xlabel('% Adapted from Books')
            ax3.set_title('Genres with Most Book Adaptations')
            ax3.grid(axis='x', alpha=0.3)

        # True story films by genre
        ax4 = axes[1, 1]

        genre_true_pct = []
        for genre, data in genre_adaptations.items():
            if data['total'] >= 10:
                true_count = data['sources']['True Story']
                pct = (true_count / data['total']) * 100
                if pct > 0:
                    genre_true_pct.append((genre, pct, data['total']))

        genre_true_pct.sort(key=lambda x: x[1], reverse=True)
        top_true_genres = genre_true_pct[:10]

        if top_true_genres:
            genres, pcts, totals = zip(*top_true_genres)
            colors = plt.cm.Oranges(np.array(pcts) / max(pcts))

            ax4.barh(range(len(genres)), pcts, color=colors, alpha=0.7, edgecolor='black')
            ax4.set_yticks(range(len(genres)))
            ax4.set_yticklabels([f"{g} (n={t})" for g, t in zip(genres, totals)], fontsize=9)
            ax4.invert_yaxis()
            ax4.set_xlabel('% Based on True Stories')
            ax4.set_title('Genres with Most True Story Films')
            ax4.grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'adaptation_genre_patterns.png', dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_adaptation_success_factors(self):
        """Analyze what makes adaptations successful."""
        print("Analyzing adaptation success factors...")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Adaptation Success Factors', fontsize=16, fontweight='bold')

        # Classify adaptations by source and rating
        adaptation_success = []

        for idx, row in self.movies_df.iterrows():
            keywords = row.get('tmdb_keywords', '')
            title = row.get('Title', '')
            sources = self.classify_source_material(keywords, title)
            rating = row.get('IMDb Rating', 0)
            year = row.get('Year', 0)

            if 'Original' not in sources and rating > 0:
                primary_source = sources[0] if sources else 'Unknown'
                adaptation_success.append({
                    'source': primary_source,
                    'rating': rating,
                    'year': year,
                    'is_high_rated': rating >= 7.0
                })

        success_df = pd.DataFrame(adaptation_success)

        # Success rate by source type
        ax1 = axes[0, 0]

        if len(success_df) > 0:
            source_success = success_df.groupby('source').agg({
                'is_high_rated': ['sum', 'count']
            }).reset_index()

            source_success.columns = ['source', 'high_count', 'total']
            source_success['success_rate'] = (source_success['high_count'] / source_success['total']) * 100
            source_success = source_success[source_success['total'] >= 5].sort_values('success_rate', ascending=False)

            if len(source_success) > 0:
                colors = plt.cm.RdYlGn(source_success['success_rate'].values / 100)

                ax1.barh(range(len(source_success)), source_success['success_rate'],
                        color=colors, alpha=0.7, edgecolor='black')
                ax1.set_yticks(range(len(source_success)))
                ax1.set_yticklabels([f"{s} (n={t})" for s, t in
                                    zip(source_success['source'], source_success['total'])], fontsize=9)
                ax1.invert_yaxis()
                ax1.set_xlabel('% Rated 7.0+')
                ax1.set_title('Success Rate by Source Material')
                ax1.grid(axis='x', alpha=0.3)

        # Rating distribution by source
        ax2 = axes[0, 1]

        if len(success_df) > 0:
            top_sources = success_df['source'].value_counts().head(5).index
            source_ratings = [success_df[success_df['source'] == s]['rating'].values
                            for s in top_sources]

            bp = ax2.boxplot(source_ratings, labels=[s[:12] for s in top_sources],
                            patch_artist=True)

            for patch in bp['boxes']:
                patch.set_facecolor('lightblue')
                patch.set_alpha(0.7)

            ax2.set_ylabel('IMDb Rating')
            ax2.set_title('Rating Distribution by Source')
            ax2.grid(axis='y', alpha=0.3)
            ax2.tick_params(axis='x', rotation=45)

        # Adaptation quality over time
        ax3 = axes[1, 0]

        if len(success_df) > 0:
            decade_quality = {}
            for decade in range(1920, 2030, 10):
                decade_adaptations = success_df[
                    (success_df['year'] >= decade) &
                    (success_df['year'] < decade + 10)
                ]

                if len(decade_adaptations) > 0:
                    avg_rating = decade_adaptations['rating'].mean()
                    decade_quality[decade] = avg_rating

            if decade_quality:
                decades = list(decade_quality.keys())
                avg_ratings = list(decade_quality.values())
                decade_labels = [f"{d}s" for d in decades]

                ax3.plot(decade_labels, avg_ratings, marker='o', linewidth=2, markersize=8, color='#E63946')
                ax3.set_xlabel('Decade')
                ax3.set_ylabel('Average Rating')
                ax3.set_title('Adaptation Quality Over Time')
                ax3.set_ylim(0, 10)
                ax3.grid(alpha=0.3)
                ax3.tick_params(axis='x', rotation=45)

        # High vs low rated adaptations count
        ax4 = axes[1, 1]

        if len(success_df) > 0:
            rating_bins = pd.cut(success_df['rating'], bins=[0, 5, 6, 7, 8, 10],
                                labels=['Poor (0-5)', 'Below Avg (5-6)', 'Good (6-7)',
                                       'Great (7-8)', 'Excellent (8-10)'])

            bin_counts = rating_bins.value_counts().sort_index()

            colors = ['#D32F2F', '#F57C00', '#FBC02D', '#7CB342', '#388E3C']
            ax4.bar(range(len(bin_counts)), bin_counts.values,
                   color=colors, alpha=0.7, edgecolor='black')
            ax4.set_xticks(range(len(bin_counts)))
            ax4.set_xticklabels(bin_counts.index, rotation=45, ha='right')
            ax4.set_ylabel('Number of Adaptations')
            ax4.set_title('Adaptation Quality Distribution')
            ax4.grid(axis='y', alpha=0.3)

            for i, count in enumerate(bin_counts.values):
                ax4.text(i, count, str(count), ha='center', va='bottom', fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'adaptation_success_factors.png', dpi=300, bbox_inches='tight')
        plt.close()

    def generate_report(self):
        """Generate comprehensive analysis report."""
        print("\nGenerating comprehensive report...")

        report_path = self.reports_dir / 'batch_28_adaptations_originals_report.txt'

        # Calculate statistics
        total_films = len(self.movies_df)

        # Count source types
        source_counts = defaultdict(int)
        for idx, row in self.movies_df.iterrows():
            keywords = row.get('tmdb_keywords', '')
            title = row.get('Title', '')
            sources = self.classify_source_material(keywords, title)

            for source in sources:
                source_counts[source] += 1

        with open(report_path, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("CINESCOPE BATCH 28: ADAPTATIONS & ORIGINALS ANALYSIS\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # Overall stats
            f.write("=" * 80 + "\n")
            f.write("OVERALL STATISTICS\n")
            f.write("=" * 80 + "\n\n")

            f.write(f"Total Films: {total_films}\n\n")

            adaptations = total_films - source_counts.get('Original', 0)
            originals = source_counts.get('Original', 0)
            adapt_pct = (adaptations / total_films) * 100

            f.write(f"Original Screenplays: {originals} ({(originals/total_films)*100:.1f}%)\n")
            f.write(f"Adaptations: {adaptations} ({adapt_pct:.1f}%)\n\n")

            # Source material breakdown
            f.write("=" * 80 + "\n")
            f.write("SOURCE MATERIAL BREAKDOWN\n")
            f.write("=" * 80 + "\n\n")

            sorted_sources = sorted(source_counts.items(), key=lambda x: x[1], reverse=True)
            for source, count in sorted_sources:
                if source != 'Original':
                    pct = (count / total_films) * 100
                    f.write(f"{source}: {count} ({pct:.1f}%)\n")

            f.write("\n" + "=" * 80 + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * 80 + "\n")

        print(f"Report saved to {report_path}")

    def run_analysis(self):
        """Execute all analysis steps."""
        print("\n" + "=" * 80)
        print("CINESCOPE BATCH 28: ADAPTATIONS & ORIGINALS ANALYSIS")
        print("=" * 80 + "\n")

        # Run all visualizations
        self.visualize_original_vs_adaptation()
        self.visualize_source_material_breakdown()
        self.visualize_remake_analysis()
        self.visualize_sequel_analysis()
        self.visualize_book_adaptations()
        self.visualize_true_story_films()
        self.visualize_comic_adaptations()
        self.visualize_play_adaptations()
        self.visualize_franchise_patterns()
        self.visualize_adaptation_genre_patterns()
        self.visualize_adaptation_success_factors()

        # Generate report
        self.generate_report()

        print("\n" + "=" * 80)
        print("ANALYSIS COMPLETE")
        print("=" * 80)
        print(f"\nVisualizations saved to: {self.viz_dir}")
        print(f"Report saved to: {self.reports_dir}")

if __name__ == "__main__":
    analyzer = AdaptationsOriginalsAnalyzer()
    analyzer.run_analysis()
