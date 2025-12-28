#!/usr/bin/env python3
"""
CineScope Batch 14: Family & Relationships
Analyzes family structures, marriages, and dynasties in cinema.

Key Questions:
1. What are the marriage and family patterns in cinema?
2. How many children do cinema people have?
3. Are there cinema "dynasties" (family members in same profession)?
4. Do family relationships affect career success?
5. What are the patterns of spousal collaborations?

Data Sources:
- ext_spouses: Spouse names (pipe-separated if multiple)
- ext_num_children: Number of children
- imdb_profession: Professions to detect dynasties
- Movie ratings and careers

Statistical Methods:
- Family size distribution analysis
- Dynasty detection (shared surnames + profession)
- Spousal collaboration analysis
- Coverage: ~5,935 people with spouse data (13.9%)
- Coverage: ~2,601 people with children data (6.1%)
"""

import os
import sys
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter, defaultdict
from datetime import datetime
import warnings
from scipy import stats
from typing import Dict, List, Tuple, Set

warnings.filterwarnings('ignore')

# Set style for professional visualizations
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

class FamilyRelationshipsAnalyzer:
    """Analyze family structures and relationships in cinema."""

    def __init__(self, data_dir: str = 'data/processed', output_dir: str = 'analysis_outputs'):
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.vis_dir = os.path.join(output_dir, 'visualizations', 'batch_14')
        self.report_path = os.path.join(output_dir, 'reports', 'batch_14_family_relationships_report.txt')

        # Create output directories
        os.makedirs(self.vis_dir, exist_ok=True)
        os.makedirs(os.path.join(output_dir, 'reports'), exist_ok=True)

        # Load data
        self.movies_df = None
        self.people_cache = None
        self.family_df = None

        # Analysis results storage
        self.stats = {}

    def load_data(self):
        """Load movie and people data."""
        print("Loading data...")

        # Load movies
        movies_path = os.path.join(self.data_dir, 'watched_movies_master.csv')
        self.movies_df = pd.read_csv(movies_path)
        print(f"  Loaded {len(self.movies_df)} movies")

        # Load people cache
        cache_path = os.path.join(self.data_dir, 'people_cache.json')
        with open(cache_path, 'r') as f:
            self.people_cache = json.load(f)
        print(f"  Loaded {len(self.people_cache)} people")

    def extract_family_data(self):
        """Extract and structure family data from people cache."""
        print("\nExtracting family data...")

        family_records = []

        for person_id, person_data in self.people_cache.items():
            # Get family fields
            spouses_str = person_data.get('ext_spouses', '')
            num_children = person_data.get('ext_num_children')

            # Validate data
            has_spouse_data = spouses_str and str(spouses_str) not in ['', 'None']
            has_children_data = num_children and str(num_children) not in ['', 'None', '0']

            if not has_spouse_data and not has_children_data:
                continue

            # Parse spouses (pipe-separated)
            spouses = []
            if has_spouse_data:
                spouses = [s.strip() for s in str(spouses_str).split('|') if s.strip()]

            # Parse number of children
            try:
                num_children_int = int(num_children) if has_children_data else 0
            except (ValueError, TypeError):
                num_children_int = 0

            # Extract person metadata
            name = person_data.get('imdb_name', 'Unknown')
            professions_str = person_data.get('imdb_profession') or ''
            professions = str(professions_str).split(',') if professions_str else []
            professions = [p.strip() for p in professions if p.strip()]

            # Determine primary profession
            primary_profession = 'Other'
            if 'actor' in professions or 'actress' in professions:
                primary_profession = 'Actor'
            elif 'director' in professions:
                primary_profession = 'Director'
            elif 'producer' in professions:
                primary_profession = 'Producer'
            elif 'writer' in professions:
                primary_profession = 'Writer'

            # Get additional metadata
            birth_year = person_data.get('imdb_birth_year')
            gender = person_data.get('gender', 0)  # 1=female, 2=male, 0=unknown
            popularity = person_data.get('popularity', 0)

            family_records.append({
                'person_id': person_id,
                'name': name,
                'primary_profession': primary_profession,
                'all_professions': '|'.join(professions),
                'birth_year': birth_year,
                'gender': gender,
                'popularity': popularity,
                'num_spouses': len(spouses),
                'spouses': '|'.join(spouses) if spouses else '',
                'num_children': num_children_int,
                'has_spouse_data': has_spouse_data,
                'has_children_data': has_children_data
            })

        self.family_df = pd.DataFrame(family_records)

        # Ensure boolean columns are proper booleans
        self.family_df['has_spouse_data'] = self.family_df['has_spouse_data'].astype(bool)
        self.family_df['has_children_data'] = self.family_df['has_children_data'].astype(bool)

        print(f"  Extracted {len(self.family_df)} family records")

        # Store stats
        self.stats['total_people_with_family_data'] = len(self.family_df)
        self.stats['people_with_spouse_data'] = int(self.family_df['has_spouse_data'].sum())
        self.stats['people_with_children_data'] = int(self.family_df['has_children_data'].sum())
        self.stats['total_spouses_recorded'] = int(self.family_df['num_spouses'].sum())
        self.stats['total_children'] = int(self.family_df['num_children'].sum())

    def visualize_family_size_distribution(self):
        """Visualize distribution of number of children."""
        print("\nGenerating family size distribution...")

        # Filter to people with children data
        df_children = self.family_df[self.family_df['has_children_data']].copy()

        # Get distribution
        children_dist = df_children['num_children'].value_counts().sort_index()

        # Create figure
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Left: Bar chart of distribution
        children_dist.plot(kind='bar', ax=ax1, color='steelblue')
        ax1.set_title('Distribution of Number of Children', fontsize=13, fontweight='bold')
        ax1.set_xlabel('Number of Children', fontsize=11, fontweight='bold')
        ax1.set_ylabel('Number of People', fontsize=11, fontweight='bold')
        ax1.tick_params(axis='x', rotation=0)

        # Add value labels
        for i, v in enumerate(children_dist.values):
            ax1.text(i, v + 5, str(v), ha='center', fontsize=9)

        # Right: Statistics box
        mean_children = df_children['num_children'].mean()
        median_children = df_children['num_children'].median()
        mode_children = df_children['num_children'].mode()[0] if len(df_children['num_children'].mode()) > 0 else 0
        max_children = df_children['num_children'].max()

        stats_text = f"""
        Family Size Statistics
        ─────────────────────────

        Total People: {len(df_children):,}
        Total Children: {df_children['num_children'].sum():,}

        Mean: {mean_children:.2f} children
        Median: {median_children:.1f} children
        Mode: {mode_children} children

        Maximum: {max_children} children

        No Children: {(df_children['num_children'] == 0).sum():,}
        1-2 Children: {((df_children['num_children'] >= 1) & (df_children['num_children'] <= 2)).sum():,}
        3+ Children: {(df_children['num_children'] >= 3).sum():,}
        """

        ax2.text(0.1, 0.5, stats_text, fontsize=11, verticalalignment='center',
                fontfamily='monospace', transform=ax2.transAxes)
        ax2.axis('off')

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '01_family_size_distribution.png'), dpi=300, bbox_inches='tight')
        plt.close()

        print(f"  Mean children: {mean_children:.2f}, Max: {max_children}")

    def visualize_marriage_patterns(self):
        """Visualize marriage patterns (number of spouses)."""
        print("\nGenerating marriage patterns visualization...")

        # Filter to people with spouse data
        df_spouses = self.family_df[self.family_df['has_spouse_data']].copy()

        # Get distribution
        spouse_dist = df_spouses['num_spouses'].value_counts().sort_index()

        # Create figure
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Left: Bar chart
        spouse_dist.plot(kind='bar', ax=ax1, color='coral')
        ax1.set_title('Distribution of Number of Marriages/Partnerships', fontsize=13, fontweight='bold')
        ax1.set_xlabel('Number of Spouses', fontsize=11, fontweight='bold')
        ax1.set_ylabel('Number of People', fontsize=11, fontweight='bold')
        ax1.tick_params(axis='x', rotation=0)

        # Add value labels
        for i, v in enumerate(spouse_dist.values):
            ax1.text(i, v + 20, str(v), ha='center', fontsize=9)

        # Right: By profession
        profession_spouse_avg = df_spouses.groupby('primary_profession')['num_spouses'].agg(['mean', 'count']).reset_index()
        profession_spouse_avg = profession_spouse_avg[profession_spouse_avg['count'] >= 10].sort_values('mean', ascending=False)

        if len(profession_spouse_avg) > 0:
            profession_spouse_avg.plot(x='primary_profession', y='mean', kind='bar', ax=ax2,
                                      color=sns.color_palette("pastel", len(profession_spouse_avg)), legend=False)
            ax2.set_title('Average Marriages by Profession', fontsize=13, fontweight='bold')
            ax2.set_xlabel('Profession', fontsize=11, fontweight='bold')
            ax2.set_ylabel('Average Number of Spouses', fontsize=11, fontweight='bold')
            ax2.tick_params(axis='x', rotation=45)

            # Add value labels
            for i, v in enumerate(profession_spouse_avg['mean'].values):
                ax2.text(i, v + 0.02, f'{v:.2f}', ha='center', fontsize=9)

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '02_marriage_patterns.png'), dpi=300, bbox_inches='tight')
        plt.close()

        print(f"  Most common: {spouse_dist.index[0]} spouse(s) ({spouse_dist.values[0]} people)")

    def visualize_children_by_profession(self):
        """Analyze children patterns by profession."""
        print("\nGenerating children by profession visualization...")

        # Filter to people with children data
        df_children = self.family_df[self.family_df['has_children_data']].copy()

        # Group by profession
        profession_children = df_children.groupby('primary_profession').agg({
            'num_children': ['mean', 'median', 'sum', 'count']
        }).reset_index()
        profession_children.columns = ['profession', 'mean', 'median', 'total', 'count']
        profession_children = profession_children[profession_children['count'] >= 10].sort_values('mean', ascending=False)

        # Create figure
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Left: Average children
        profession_children.plot(x='profession', y='mean', kind='bar', ax=ax1,
                                 color=sns.color_palette("muted", len(profession_children)), legend=False)
        ax1.set_title('Average Number of Children by Profession', fontsize=13, fontweight='bold')
        ax1.set_xlabel('Profession', fontsize=11, fontweight='bold')
        ax1.set_ylabel('Average Children', fontsize=11, fontweight='bold')
        ax1.tick_params(axis='x', rotation=45)

        # Add value labels
        for i, v in enumerate(profession_children['mean'].values):
            ax1.text(i, v + 0.05, f'{v:.2f}', ha='center', fontsize=9)

        # Right: Total children
        profession_children.plot(x='profession', y='total', kind='bar', ax=ax2,
                                 color=sns.color_palette("rocket", len(profession_children)), legend=False)
        ax2.set_title('Total Children by Profession', fontsize=13, fontweight='bold')
        ax2.set_xlabel('Profession', fontsize=11, fontweight='bold')
        ax2.set_ylabel('Total Children', fontsize=11, fontweight='bold')
        ax2.tick_params(axis='x', rotation=45)

        # Add value labels
        for i, v in enumerate(profession_children['total'].values):
            ax2.text(i, v + 20, str(int(v)), ha='center', fontsize=9)

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '03_children_by_profession.png'), dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_children_by_gender(self):
        """Analyze children patterns by gender."""
        print("\nGenerating children by gender visualization...")

        # Filter to people with children data and known gender
        df_children = self.family_df[
            (self.family_df['has_children_data']) &
            (self.family_df['gender'].isin([1, 2]))
        ].copy()

        # Map gender
        df_children['gender_label'] = df_children['gender'].map({1: 'Female', 2: 'Male'})

        # Group by gender
        gender_stats = df_children.groupby('gender_label').agg({
            'num_children': ['mean', 'median', 'sum', 'count']
        }).reset_index()
        gender_stats.columns = ['gender', 'mean', 'median', 'total', 'count']

        # Create figure
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

        # Left: Average children by gender
        colors = ['#FF69B4', '#4169E1']  # Pink for female, blue for male
        gender_stats.plot(x='gender', y='mean', kind='bar', ax=ax1, color=colors, legend=False)
        ax1.set_title('Average Number of Children by Gender', fontsize=13, fontweight='bold')
        ax1.set_xlabel('Gender', fontsize=11, fontweight='bold')
        ax1.set_ylabel('Average Children', fontsize=11, fontweight='bold')
        ax1.tick_params(axis='x', rotation=0)

        # Add value labels
        for i, v in enumerate(gender_stats['mean'].values):
            ax1.text(i, v + 0.05, f'{v:.2f}', ha='center', fontsize=10, fontweight='bold')

        # Right: Distribution violin plot
        sns.violinplot(data=df_children, x='gender_label', y='num_children', ax=ax2, palette=colors)
        ax2.set_title('Distribution of Children by Gender', fontsize=13, fontweight='bold')
        ax2.set_xlabel('Gender', fontsize=11, fontweight='bold')
        ax2.set_ylabel('Number of Children', fontsize=11, fontweight='bold')

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '04_children_by_gender.png'), dpi=300, bbox_inches='tight')
        plt.close()

    def detect_cinema_dynasties(self):
        """Detect potential cinema dynasties (shared surnames + profession)."""
        print("\nDetecting cinema dynasties...")

        # Extract surnames
        def get_surname(name):
            parts = str(name).split()
            if len(parts) > 1:
                return parts[-1].strip().lower()
            return name.lower()

        # Build surname groups
        surname_groups = defaultdict(list)

        for person_id, person_data in self.people_cache.items():
            name = person_data.get('imdb_name', '')
            if not name or name == 'Unknown':
                continue

            surname = get_surname(name)
            if len(surname) < 3:  # Skip very short surnames
                continue

            professions_str = person_data.get('imdb_profession') or ''
            professions = str(professions_str).split(',') if professions_str else []
            professions = [p.strip() for p in professions if p.strip()]

            # Only consider cinema-related professions
            cinema_profs = [p for p in professions if p in ['actor', 'actress', 'director', 'producer', 'writer']]
            if not cinema_profs:
                continue

            surname_groups[surname].append({
                'person_id': person_id,
                'name': name,
                'professions': cinema_profs
            })

        # Find potential dynasties (3+ people with same surname in cinema)
        dynasties = []
        for surname, people in surname_groups.items():
            if len(people) >= 3:
                dynasties.append({
                    'surname': surname.title(),
                    'count': len(people),
                    'people': people
                })

        dynasties.sort(key=lambda x: x['count'], reverse=True)

        self.stats['potential_dynasties'] = len(dynasties)
        self.stats['top_dynasty'] = dynasties[0] if dynasties else None

        # Visualize top dynasties
        fig, ax = plt.subplots(figsize=(14, 10))

        if len(dynasties) >= 20:
            top_dynasties = dynasties[:20]
            surnames = [d['surname'] for d in top_dynasties]
            counts = [d['count'] for d in top_dynasties]

            y_pos = np.arange(len(surnames))
            bars = ax.barh(y_pos, counts, color=sns.color_palette("viridis", len(surnames)))

            ax.set_yticks(y_pos)
            ax.set_yticklabels(surnames, fontsize=10)
            ax.invert_yaxis()
            ax.set_xlabel('Number of Cinema Professionals', fontsize=11, fontweight='bold')
            ax.set_title('Top 20 Potential Cinema Dynasties (by Surname)',
                         fontsize=13, fontweight='bold', pad=15)

            # Add value labels
            for i, v in enumerate(counts):
                ax.text(v + 0.3, i, str(v), va='center', fontsize=9)

            print(f"  Top dynasty: {surnames[0]} with {counts[0]} people")
        else:
            ax.text(0.5, 0.5, f'Found {len(dynasties)} potential dynasties\n(surnames with 3+ cinema professionals)',
                   ha='center', va='center', fontsize=14, transform=ax.transAxes)
            ax.axis('off')

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '05_cinema_dynasties.png'), dpi=300, bbox_inches='tight')
        plt.close()

    def analyze_children_over_time(self):
        """Analyze how family size changed over birth cohorts."""
        print("\nAnalyzing family size evolution over time...")

        # Filter to people with children data and birth year
        df_temporal = self.family_df[
            (self.family_df['has_children_data']) &
            (self.family_df['birth_year'].notna())
        ].copy()

        df_temporal = df_temporal[(df_temporal['birth_year'] >= 1880) & (df_temporal['birth_year'] <= 2000)]

        # Create decade bins
        df_temporal['birth_decade'] = (df_temporal['birth_year'] // 10) * 10

        # Calculate average by decade
        decade_avg = df_temporal.groupby('birth_decade')['num_children'].mean().reset_index()

        # Create visualization
        fig, ax = plt.subplots(figsize=(14, 8))

        ax.plot(decade_avg['birth_decade'], decade_avg['num_children'], marker='o',
               linewidth=3, markersize=8, color='darkblue')

        ax.set_xlabel('Birth Decade', fontsize=11, fontweight='bold')
        ax.set_ylabel('Average Number of Children', fontsize=11, fontweight='bold')
        ax.set_title('Evolution of Family Size Over Time (Cinema Professionals)',
                     fontsize=13, fontweight='bold', pad=15)
        ax.grid(True, alpha=0.3)

        # Add value labels
        for _, row in decade_avg.iterrows():
            ax.text(row['birth_decade'], row['num_children'] + 0.05,
                   f'{row["num_children"]:.2f}', ha='center', fontsize=9)

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '06_family_size_evolution.png'), dpi=300, bbox_inches='tight')
        plt.close()

    def analyze_spousal_collaborations(self):
        """Analyze if spouses are also in cinema (based on surname matching)."""
        print("\nAnalyzing potential spousal collaborations...")

        # This is tricky - we'd need to match spouse names to people in our cache
        # For now, just analyze patterns

        df_spouses = self.family_df[self.family_df['has_spouse_data']].copy()

        # Create figure showing spouse count distribution by profession
        fig, ax = plt.subplots(figsize=(12, 8))

        profession_spouse_dist = df_spouses.groupby(['primary_profession', 'num_spouses']).size().unstack(fill_value=0)

        profession_spouse_dist.plot(kind='bar', stacked=True, ax=ax,
                                     color=sns.color_palette("Set3", profession_spouse_dist.shape[1]))

        ax.set_title('Marriage Pattern Distribution by Profession', fontsize=13, fontweight='bold', pad=15)
        ax.set_xlabel('Profession', fontsize=11, fontweight='bold')
        ax.set_ylabel('Number of People', fontsize=11, fontweight='bold')
        ax.legend(title='Number of Spouses', bbox_to_anchor=(1.05, 1), loc='upper left')
        ax.tick_params(axis='x', rotation=45)

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '07_spousal_patterns_by_profession.png'), dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_large_families(self):
        """Highlight people with largest families."""
        print("\nGenerating large families leaderboard...")

        # Get top 20 by children
        df_children = self.family_df[self.family_df['has_children_data']].copy()
        top_families = df_children.nlargest(20, 'num_children')[['name', 'num_children', 'primary_profession']]

        # Create visualization
        fig, ax = plt.subplots(figsize=(14, 10))

        y_pos = np.arange(len(top_families))
        bars = ax.barh(y_pos, top_families['num_children'],
                       color=sns.color_palette("coolwarm_r", len(top_families)))

        # Create labels with profession
        labels = [f"{row['name']} ({row['primary_profession']})"
                 for _, row in top_families.iterrows()]

        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels, fontsize=9)
        ax.invert_yaxis()
        ax.set_xlabel('Number of Children', fontsize=11, fontweight='bold')
        ax.set_title('Top 20 Largest Families in Cinema', fontsize=13, fontweight='bold', pad=15)

        # Add value labels
        for i, v in enumerate(top_families['num_children']):
            ax.text(v + 0.2, i, str(v), va='center', fontsize=9)

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '08_largest_families.png'), dpi=300, bbox_inches='tight')
        plt.close()

        if len(top_families) > 0:
            print(f"  Largest family: {top_families.iloc[0]['name']} with {top_families.iloc[0]['num_children']} children")

    def visualize_marriage_vs_children(self):
        """Analyze correlation between marriages and children."""
        print("\nAnalyzing marriage vs children correlation...")

        # Filter to people with both data points
        df_both = self.family_df[
            (self.family_df['has_spouse_data']) &
            (self.family_df['has_children_data'])
        ].copy()

        if len(df_both) < 10:
            print("  Insufficient data for correlation")
            return

        # Calculate correlation
        correlation = df_both['num_spouses'].corr(df_both['num_children'])

        # Create scatter plot
        fig, ax = plt.subplots(figsize=(12, 8))

        ax.scatter(df_both['num_spouses'], df_both['num_children'], alpha=0.5, s=50)
        ax.set_xlabel('Number of Marriages/Partnerships', fontsize=11, fontweight='bold')
        ax.set_ylabel('Number of Children', fontsize=11, fontweight='bold')
        ax.set_title(f'Marriages vs Children (correlation: {correlation:.3f})',
                     fontsize=13, fontweight='bold', pad=15)
        ax.grid(True, alpha=0.3)

        # Add trend line
        z = np.polyfit(df_both['num_spouses'], df_both['num_children'], 1)
        p = np.poly1d(z)
        ax.plot(df_both['num_spouses'].unique(), p(df_both['num_spouses'].unique()),
               "r--", alpha=0.8, linewidth=2, label=f'Trend line')
        ax.legend()

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '09_marriage_children_correlation.png'), dpi=300, bbox_inches='tight')
        plt.close()

        print(f"  Correlation: {correlation:.3f}")

    def visualize_coverage_summary(self):
        """Create comprehensive coverage summary."""
        print("\nGenerating coverage summary...")

        total_people = len(self.people_cache)

        # Calculate coverage metrics
        metrics = {
            'Spouse Data': (self.stats['people_with_spouse_data'], total_people),
            'Children Data': (self.stats['people_with_children_data'], total_people),
            'Both Spouse & Children': (
                len(self.family_df[
                    (self.family_df['has_spouse_data']) &
                    (self.family_df['has_children_data'])
                ]),
                total_people
            )
        }

        # Create figure
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Left: Absolute counts
        labels = list(metrics.keys())
        values = [m[0] for m in metrics.values()]

        ax1.bar(range(len(labels)), values, color=sns.color_palette("viridis", len(labels)))
        ax1.set_xticks(range(len(labels)))
        ax1.set_xticklabels(labels, fontsize=10)
        ax1.set_ylabel('Number of People', fontsize=11, fontweight='bold')
        ax1.set_title('Family Data Coverage: Absolute Counts', fontsize=12, fontweight='bold')

        # Add value labels
        for i, v in enumerate(values):
            ax1.text(i, v + 100, f'{v:,}', ha='center', fontsize=10, fontweight='bold')

        # Right: Percentage coverage
        percentages = [(m[0] / m[1]) * 100 for m in metrics.values()]

        ax2.bar(range(len(labels)), percentages, color=sns.color_palette("rocket", len(labels)))
        ax2.set_xticks(range(len(labels)))
        ax2.set_xticklabels(labels, fontsize=10)
        ax2.set_ylabel('Coverage (%)', fontsize=11, fontweight='bold')
        ax2.set_title('Family Data Coverage: Percentage of Total', fontsize=12, fontweight='bold')
        ax2.set_ylim(0, 20)

        # Add percentage labels
        for i, v in enumerate(percentages):
            ax2.text(i, v + 0.3, f'{v:.1f}%', ha='center', fontsize=10, fontweight='bold')

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '10_coverage_summary.png'), dpi=300, bbox_inches='tight')
        plt.close()

    def generate_report(self):
        """Generate comprehensive text report."""
        print("\nGenerating text report...")

        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("CINESCOPE BATCH 14: FAMILY & RELATIONSHIPS ANALYSIS")
        report_lines.append("=" * 80)
        report_lines.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"\nTotal People in Dataset: {len(self.people_cache):,}")

        report_lines.append("\n" + "=" * 80)
        report_lines.append("FAMILY DATA SUMMARY")
        report_lines.append("=" * 80)
        report_lines.append(f"\nPeople with Family Data: {self.stats['total_people_with_family_data']:,}")
        report_lines.append(f"People with Spouse Data: {self.stats['people_with_spouse_data']:,} ({self.stats['people_with_spouse_data']/len(self.people_cache)*100:.1f}%)")
        report_lines.append(f"People with Children Data: {self.stats['people_with_children_data']:,} ({self.stats['people_with_children_data']/len(self.people_cache)*100:.1f}%)")
        report_lines.append(f"\nTotal Marriages/Partnerships Recorded: {self.stats['total_spouses_recorded']:,}")
        report_lines.append(f"Total Children: {self.stats['total_children']:,}")

        # Children statistics
        df_children = self.family_df[self.family_df['has_children_data']]
        if len(df_children) > 0:
            report_lines.append(f"\nAverage Children: {df_children['num_children'].mean():.2f}")
            report_lines.append(f"Median Children: {df_children['num_children'].median():.1f}")
            report_lines.append(f"Maximum Children: {df_children['num_children'].max()}")

        # Marriage statistics
        df_spouses = self.family_df[self.family_df['has_spouse_data']]
        if len(df_spouses) > 0:
            report_lines.append(f"\nAverage Marriages: {df_spouses['num_spouses'].mean():.2f}")
            report_lines.append(f"Median Marriages: {df_spouses['num_spouses'].median():.1f}")
            report_lines.append(f"Maximum Marriages: {df_spouses['num_spouses'].max()}")

        # Dynasty information
        if self.stats.get('potential_dynasties'):
            report_lines.append("\n" + "=" * 80)
            report_lines.append("CINEMA DYNASTIES")
            report_lines.append("=" * 80)
            report_lines.append(f"\nPotential Dynasties Found: {self.stats['potential_dynasties']} (surnames with 3+ cinema professionals)")

            if self.stats.get('top_dynasty'):
                top = self.stats['top_dynasty']
                report_lines.append(f"Top Dynasty: {top['surname']} ({top['count']} people)")

        report_lines.append("\n" + "=" * 80)
        report_lines.append("END OF REPORT")
        report_lines.append("=" * 80)

        # Write report
        with open(self.report_path, 'w') as f:
            f.write('\n'.join(report_lines))

        print(f"  Report saved to: {self.report_path}")

    def run_analysis(self):
        """Execute complete analysis pipeline."""
        print("\n" + "=" * 80)
        print("CINESCOPE BATCH 14: FAMILY & RELATIONSHIPS ANALYSIS")
        print("=" * 80)

        # Load data
        self.load_data()

        # Extract structured data
        self.extract_family_data()

        # Generate visualizations
        self.visualize_family_size_distribution()
        self.visualize_marriage_patterns()
        self.visualize_children_by_profession()
        self.visualize_children_by_gender()
        self.detect_cinema_dynasties()
        self.analyze_children_over_time()
        self.analyze_spousal_collaborations()
        self.visualize_large_families()
        self.visualize_marriage_vs_children()
        self.visualize_coverage_summary()

        # Generate report
        self.generate_report()

        print("\n" + "=" * 80)
        print("ANALYSIS COMPLETE")
        print("=" * 80)
        print(f"Visualizations saved to: {self.vis_dir}")
        print(f"Report saved to: {self.report_path}")
        print(f"\nGenerated {len([f for f in os.listdir(self.vis_dir) if f.endswith('.png')])} visualizations")


if __name__ == '__main__':
    analyzer = FamilyRelationshipsAnalyzer()
    analyzer.run_analysis()
