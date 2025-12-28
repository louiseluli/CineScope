#!/usr/bin/env python3
"""
CineScope Batch 15: Height & Physical Attributes
Analyzes height patterns and physical characteristics in cinema.

Key Questions:
1. What is the average height distribution in cinema?
2. Are there height differences between professions (actors vs directors)?
3. Are there gender-based height patterns?
4. Does height correlate with career success or popularity?
5. How have height standards evolved over time?

Data Sources:
- ext_height_cm: Height in centimeters
- ext_height_imperial: Height in feet/inches format
- gender: Gender (1=Female, 2=Male)
- imdb_profession: Profession
- popularity: TMDB popularity score

Coverage: ~1,556 people with height data (3.7%)

Statistical Methods:
- Height distribution analysis with outlier filtering
- Gender and profession comparisons (t-tests)
- Temporal trends analysis
- Height-popularity correlation
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

class HeightPhysicalAnalyzer:
    """Analyze height and physical attributes in cinema."""

    def __init__(self, data_dir: str = 'data/processed', output_dir: str = 'analysis_outputs'):
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.vis_dir = os.path.join(output_dir, 'visualizations', 'batch_15')
        self.report_path = os.path.join(output_dir, 'reports', 'batch_15_height_physical_report.txt')

        # Create output directories
        os.makedirs(self.vis_dir, exist_ok=True)
        os.makedirs(os.path.join(output_dir, 'reports'), exist_ok=True)

        # Load data
        self.movies_df = None
        self.people_cache = None
        self.height_df = None

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

    def extract_height_data(self):
        """Extract and validate height data from people cache."""
        print("\nExtracting height data...")

        height_records = []

        for person_id, person_data in self.people_cache.items():
            # Get height in cm
            height_cm = person_data.get('ext_height_cm')

            # Validate height (must be reasonable: 50-250cm)
            if not height_cm or pd.isna(height_cm):
                continue

            try:
                height_cm = float(height_cm)
            except (ValueError, TypeError):
                continue

            # Filter outliers (realistic height range)
            if height_cm < 50 or height_cm > 250:
                continue

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
            gender = person_data.get('gender', 0)  # 1=female, 2=male
            popularity = person_data.get('popularity', 0)
            height_imperial = person_data.get('ext_height_imperial', '')

            # Gender label
            gender_label = 'Unknown'
            if gender == 1:
                gender_label = 'Female'
            elif gender == 2:
                gender_label = 'Male'

            height_records.append({
                'person_id': person_id,
                'name': name,
                'height_cm': height_cm,
                'height_imperial': height_imperial,
                'primary_profession': primary_profession,
                'all_professions': '|'.join(professions),
                'birth_year': birth_year,
                'gender': gender,
                'gender_label': gender_label,
                'popularity': popularity
            })

        self.height_df = pd.DataFrame(height_records)
        print(f"  Extracted {len(self.height_df)} height records (after filtering outliers)")

        # Store stats
        self.stats['total_with_height'] = len(self.height_df)
        self.stats['mean_height'] = self.height_df['height_cm'].mean()
        self.stats['median_height'] = self.height_df['height_cm'].median()
        self.stats['min_height'] = self.height_df['height_cm'].min()
        self.stats['max_height'] = self.height_df['height_cm'].max()

    def visualize_height_distribution(self):
        """Visualize overall height distribution."""
        print("\nGenerating height distribution visualization...")

        # Create figure
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Left: Histogram
        ax1.hist(self.height_df['height_cm'], bins=30, color='steelblue', edgecolor='black', alpha=0.7)
        ax1.axvline(self.stats['mean_height'], color='red', linestyle='--', linewidth=2, label=f'Mean: {self.stats["mean_height"]:.1f}cm')
        ax1.axvline(self.stats['median_height'], color='green', linestyle='--', linewidth=2, label=f'Median: {self.stats["median_height"]:.1f}cm')
        ax1.set_xlabel('Height (cm)', fontsize=11, fontweight='bold')
        ax1.set_ylabel('Frequency', fontsize=11, fontweight='bold')
        ax1.set_title('Height Distribution in Cinema', fontsize=13, fontweight='bold')
        ax1.legend()
        ax1.grid(True, alpha=0.3)

        # Right: Box plot
        ax2.boxplot(self.height_df['height_cm'], vert=True)
        ax2.set_ylabel('Height (cm)', fontsize=11, fontweight='bold')
        ax2.set_title('Height Distribution (Box Plot)', fontsize=13, fontweight='bold')
        ax2.grid(True, alpha=0.3, axis='y')

        # Add statistics text
        stats_text = f"""
        Statistics:
        Mean: {self.stats['mean_height']:.1f} cm
        Median: {self.stats['median_height']:.1f} cm
        Range: {self.stats['min_height']:.0f} - {self.stats['max_height']:.0f} cm
        """
        ax2.text(1.3, self.stats['mean_height'], stats_text, fontsize=10,
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '01_height_distribution.png'), dpi=300, bbox_inches='tight')
        plt.close()

        print(f"  Mean height: {self.stats['mean_height']:.1f}cm, Median: {self.stats['median_height']:.1f}cm")

    def visualize_height_by_gender(self):
        """Analyze height differences by gender."""
        print("\nGenerating height by gender visualization...")

        # Filter to people with known gender
        df_gender = self.height_df[self.height_df['gender'].isin([1, 2])].copy()

        if len(df_gender) < 10:
            print("  Insufficient gender data")
            return

        # Calculate statistics by gender
        gender_stats = df_gender.groupby('gender_label')['height_cm'].agg(['mean', 'median', 'std', 'count']).reset_index()

        # Perform t-test
        female_heights = df_gender[df_gender['gender'] == 1]['height_cm']
        male_heights = df_gender[df_gender['gender'] == 2]['height_cm']

        if len(female_heights) > 0 and len(male_heights) > 0:
            t_stat, p_value = stats.ttest_ind(female_heights, male_heights)
            self.stats['gender_ttest_pvalue'] = p_value
        else:
            p_value = None

        # Create visualization
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Left: Violin plot
        colors = ['#FF69B4', '#4169E1']
        parts = ax1.violinplot([female_heights, male_heights], positions=[0, 1], widths=0.7,
                               showmeans=True, showmedians=True)

        for pc, color in zip(parts['bodies'], colors):
            pc.set_facecolor(color)
            pc.set_alpha(0.7)

        ax1.set_xticks([0, 1])
        ax1.set_xticklabels(['Female', 'Male'])
        ax1.set_ylabel('Height (cm)', fontsize=11, fontweight='bold')
        ax1.set_title('Height Distribution by Gender', fontsize=13, fontweight='bold')
        ax1.grid(True, alpha=0.3, axis='y')

        # Add p-value
        if p_value is not None:
            sig_text = f'p = {p_value:.4f}' + (' ***' if p_value < 0.001 else (' **' if p_value < 0.01 else (' *' if p_value < 0.05 else ' (n.s.)')))
            ax1.text(0.5, ax1.get_ylim()[1] * 0.95, sig_text, ha='center', fontsize=10,
                    bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.5))

        # Right: Bar chart of means
        gender_stats.plot(x='gender_label', y='mean', kind='bar', ax=ax2, color=colors, legend=False)
        ax2.set_xlabel('Gender', fontsize=11, fontweight='bold')
        ax2.set_ylabel('Average Height (cm)', fontsize=11, fontweight='bold')
        ax2.set_title('Average Height by Gender', fontsize=13, fontweight='bold')
        ax2.tick_params(axis='x', rotation=0)

        # Add value labels and sample sizes
        for i, row in gender_stats.iterrows():
            ax2.text(i, row['mean'] + 1, f'{row["mean"]:.1f}cm\n(n={int(row["count"])})',
                    ha='center', fontsize=10, fontweight='bold')

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '02_height_by_gender.png'), dpi=300, bbox_inches='tight')
        plt.close()

        print(f"  Female avg: {gender_stats[gender_stats['gender_label']=='Female']['mean'].values[0]:.1f}cm")
        print(f"  Male avg: {gender_stats[gender_stats['gender_label']=='Male']['mean'].values[0]:.1f}cm")
        if p_value:
            print(f"  T-test p-value: {p_value:.4f}")

    def visualize_height_by_profession(self):
        """Analyze height differences by profession."""
        print("\nGenerating height by profession visualization...")

        # Filter to professions with sufficient data
        profession_counts = self.height_df['primary_profession'].value_counts()
        valid_professions = profession_counts[profession_counts >= 10].index.tolist()

        df_prof = self.height_df[self.height_df['primary_profession'].isin(valid_professions)].copy()

        # Calculate statistics by profession
        prof_stats = df_prof.groupby('primary_profession')['height_cm'].agg(['mean', 'median', 'std', 'count']).reset_index()
        prof_stats = prof_stats.sort_values('mean', ascending=False)

        # Create visualization
        fig, ax = plt.subplots(figsize=(12, 8))

        # Box plot by profession
        profession_data = [df_prof[df_prof['primary_profession'] == prof]['height_cm'].values
                          for prof in prof_stats['primary_profession']]

        bp = ax.boxplot(profession_data, labels=prof_stats['primary_profession'], patch_artist=True)

        # Color boxes
        colors = sns.color_palette("Set2", len(prof_stats))
        for patch, color in zip(bp['boxes'], colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)

        ax.set_xlabel('Profession', fontsize=11, fontweight='bold')
        ax.set_ylabel('Height (cm)', fontsize=11, fontweight='bold')
        ax.set_title('Height Distribution by Profession', fontsize=13, fontweight='bold')
        ax.tick_params(axis='x', rotation=45)
        ax.grid(True, alpha=0.3, axis='y')

        # Add sample sizes
        for i, row in prof_stats.iterrows():
            ax.text(i+1, ax.get_ylim()[0], f'n={int(row["count"])}',
                   ha='center', fontsize=8, rotation=45)

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '03_height_by_profession.png'), dpi=300, bbox_inches='tight')
        plt.close()

    def analyze_height_evolution(self):
        """Analyze how heights have evolved over birth cohorts."""
        print("\nAnalyzing height evolution over time...")

        # Filter to people with birth years
        df_temporal = self.height_df[self.height_df['birth_year'].notna()].copy()
        df_temporal = df_temporal[(df_temporal['birth_year'] >= 1900) & (df_temporal['birth_year'] <= 2010)]

        if len(df_temporal) < 20:
            print("  Insufficient temporal data")
            return

        # Create decade bins
        df_temporal['birth_decade'] = (df_temporal['birth_year'] // 10) * 10

        # Calculate average by decade
        decade_stats = df_temporal.groupby('birth_decade')['height_cm'].agg(['mean', 'count']).reset_index()

        # Create visualization
        fig, ax = plt.subplots(figsize=(14, 8))

        ax.plot(decade_stats['birth_decade'], decade_stats['mean'], marker='o',
               linewidth=3, markersize=8, color='darkblue')

        # Add trend line
        z = np.polyfit(decade_stats['birth_decade'], decade_stats['mean'], 1)
        p = np.poly1d(z)
        ax.plot(decade_stats['birth_decade'], p(decade_stats['birth_decade']),
               "r--", alpha=0.8, linewidth=2, label=f'Trend: {z[0]:.3f}cm/decade')

        ax.set_xlabel('Birth Decade', fontsize=11, fontweight='bold')
        ax.set_ylabel('Average Height (cm)', fontsize=11, fontweight='bold')
        ax.set_title('Evolution of Height Over Time (Cinema Professionals)',
                     fontsize=13, fontweight='bold', pad=15)
        ax.legend()
        ax.grid(True, alpha=0.3)

        # Add value labels
        for _, row in decade_stats.iterrows():
            ax.text(row['birth_decade'], row['mean'] + 0.3,
                   f'{row["mean"]:.1f}\n(n={int(row["count"])})',
                   ha='center', fontsize=8)

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '04_height_evolution.png'), dpi=300, bbox_inches='tight')
        plt.close()

        self.stats['height_trend_per_decade'] = z[0]

    def analyze_height_popularity_correlation(self):
        """Analyze correlation between height and popularity."""
        print("\nAnalyzing height-popularity correlation...")

        # Filter to people with popularity data
        df_pop = self.height_df[self.height_df['popularity'] > 0].copy()

        if len(df_pop) < 20:
            print("  Insufficient popularity data")
            return

        # Calculate correlation
        correlation = df_pop['height_cm'].corr(df_pop['popularity'])
        self.stats['height_popularity_correlation'] = correlation

        # Create scatter plot
        fig, ax = plt.subplots(figsize=(12, 8))

        ax.scatter(df_pop['height_cm'], df_pop['popularity'], alpha=0.5, s=50)
        ax.set_xlabel('Height (cm)', fontsize=11, fontweight='bold')
        ax.set_ylabel('Popularity Score', fontsize=11, fontweight='bold')
        ax.set_title(f'Height vs Popularity (correlation: {correlation:.3f})',
                     fontsize=13, fontweight='bold', pad=15)
        ax.grid(True, alpha=0.3)

        # Add trend line
        z = np.polyfit(df_pop['height_cm'], df_pop['popularity'], 1)
        p = np.poly1d(z)
        height_range = np.linspace(df_pop['height_cm'].min(), df_pop['height_cm'].max(), 100)
        ax.plot(height_range, p(height_range), "r--", alpha=0.8, linewidth=2, label='Trend line')
        ax.legend()

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '05_height_popularity_correlation.png'), dpi=300, bbox_inches='tight')
        plt.close()

        print(f"  Correlation: {correlation:.3f}")

    def visualize_tallest_shortest(self):
        """Highlight tallest and shortest people."""
        print("\nGenerating tallest/shortest leaderboards...")

        # Get top 15 tallest and shortest
        tallest = self.height_df.nlargest(15, 'height_cm')[['name', 'height_cm', 'height_imperial', 'primary_profession']]
        shortest = self.height_df.nsmallest(15, 'height_cm')[['name', 'height_cm', 'height_imperial', 'primary_profession']]

        # Create visualization
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 10))

        # Left: Tallest
        y_pos = np.arange(len(tallest))
        ax1.barh(y_pos, tallest['height_cm'], color=sns.color_palette("Blues_r", len(tallest)))

        labels_tall = [f"{row['name']} ({row['primary_profession']})" for _, row in tallest.iterrows()]
        ax1.set_yticks(y_pos)
        ax1.set_yticklabels(labels_tall, fontsize=9)
        ax1.invert_yaxis()
        ax1.set_xlabel('Height (cm)', fontsize=11, fontweight='bold')
        ax1.set_title('15 Tallest People in Cinema', fontsize=13, fontweight='bold', pad=15)

        # Add value labels
        for i, row in tallest.iterrows():
            ax1.text(row['height_cm'] + 1, list(tallest.index).index(i),
                    f'{row["height_cm"]:.0f}cm ({row["height_imperial"]})',
                    va='center', fontsize=8)

        # Right: Shortest
        y_pos = np.arange(len(shortest))
        ax2.barh(y_pos, shortest['height_cm'], color=sns.color_palette("Oranges", len(shortest)))

        labels_short = [f"{row['name']} ({row['primary_profession']})" for _, row in shortest.iterrows()]
        ax2.set_yticks(y_pos)
        ax2.set_yticklabels(labels_short, fontsize=9)
        ax2.invert_yaxis()
        ax2.set_xlabel('Height (cm)', fontsize=11, fontweight='bold')
        ax2.set_title('15 Shortest People in Cinema', fontsize=13, fontweight='bold', pad=15)

        # Add value labels
        for i, row in shortest.iterrows():
            ax2.text(row['height_cm'] + 1, list(shortest.index).index(i),
                    f'{row["height_cm"]:.0f}cm ({row["height_imperial"]})',
                    va='center', fontsize=8)

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '06_tallest_shortest.png'), dpi=300, bbox_inches='tight')
        plt.close()

        print(f"  Tallest: {tallest.iloc[0]['name']} ({tallest.iloc[0]['height_cm']}cm)")
        print(f"  Shortest: {shortest.iloc[0]['name']} ({shortest.iloc[0]['height_cm']}cm)")

    def visualize_height_percentiles(self):
        """Visualize height percentiles."""
        print("\nGenerating height percentiles visualization...")

        # Calculate percentiles
        percentiles = [10, 25, 50, 75, 90, 95, 99]
        percentile_values = [np.percentile(self.height_df['height_cm'], p) for p in percentiles]

        # Create visualization
        fig, ax = plt.subplots(figsize=(12, 8))

        ax.bar(range(len(percentiles)), percentile_values, color=sns.color_palette("viridis", len(percentiles)))
        ax.set_xticks(range(len(percentiles)))
        ax.set_xticklabels([f'{p}th' for p in percentiles])
        ax.set_xlabel('Percentile', fontsize=11, fontweight='bold')
        ax.set_ylabel('Height (cm)', fontsize=11, fontweight='bold')
        ax.set_title('Height Percentiles in Cinema', fontsize=13, fontweight='bold')

        # Add value labels
        for i, v in enumerate(percentile_values):
            ax.text(i, v + 1, f'{v:.1f}cm', ha='center', fontsize=10, fontweight='bold')

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '07_height_percentiles.png'), dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_gender_profession_height(self):
        """Combined gender and profession height analysis."""
        print("\nGenerating gender-profession height heatmap...")

        # Filter to valid data
        df_combined = self.height_df[
            (self.height_df['gender'].isin([1, 2])) &
            (self.height_df['primary_profession'].isin(['Actor', 'Director', 'Producer', 'Writer']))
        ].copy()

        if len(df_combined) < 20:
            print("  Insufficient data for combined analysis")
            return

        # Create pivot table
        pivot = df_combined.pivot_table(values='height_cm',
                                        index='primary_profession',
                                        columns='gender_label',
                                        aggfunc='mean')

        # Create heatmap
        fig, ax = plt.subplots(figsize=(10, 8))

        sns.heatmap(pivot, annot=True, fmt='.1f', cmap='YlOrRd', ax=ax,
                   cbar_kws={'label': 'Average Height (cm)'})

        ax.set_title('Average Height by Gender and Profession', fontsize=13, fontweight='bold', pad=15)
        ax.set_xlabel('Gender', fontsize=11, fontweight='bold')
        ax.set_ylabel('Profession', fontsize=11, fontweight='bold')

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '08_gender_profession_heatmap.png'), dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_height_ranges(self):
        """Visualize common height ranges."""
        print("\nGenerating height range distribution...")

        # Define height ranges
        bins = [0, 150, 160, 170, 180, 190, 200, 300]
        labels = ['<150cm', '150-160cm', '160-170cm', '170-180cm', '180-190cm', '190-200cm', '>200cm']

        self.height_df['height_range'] = pd.cut(self.height_df['height_cm'], bins=bins, labels=labels)
        range_counts = self.height_df['height_range'].value_counts().sort_index()

        # Create visualization
        fig, ax = plt.subplots(figsize=(12, 8))

        range_counts.plot(kind='bar', ax=ax, color=sns.color_palette("Set3", len(range_counts)))
        ax.set_xlabel('Height Range', fontsize=11, fontweight='bold')
        ax.set_ylabel('Number of People', fontsize=11, fontweight='bold')
        ax.set_title('Distribution of Height Ranges', fontsize=13, fontweight='bold')
        ax.tick_params(axis='x', rotation=45)

        # Add value labels and percentages
        total = len(self.height_df)
        for i, v in enumerate(range_counts.values):
            pct = (v / total) * 100
            ax.text(i, v + 2, f'{v}\n({pct:.1f}%)', ha='center', fontsize=9)

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '09_height_ranges.png'), dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_coverage_summary(self):
        """Create coverage summary visualization."""
        print("\nGenerating coverage summary...")

        total_people = len(self.people_cache)

        # Calculate metrics
        metrics = {
            'Height Data': (self.stats['total_with_height'], total_people),
            'Height + Gender': (
                len(self.height_df[self.height_df['gender'].isin([1, 2])]),
                total_people
            ),
            'Height + Popularity': (
                len(self.height_df[self.height_df['popularity'] > 0]),
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
        ax1.set_xticklabels(labels, fontsize=10, rotation=15)
        ax1.set_ylabel('Number of People', fontsize=11, fontweight='bold')
        ax1.set_title('Height Data Coverage: Absolute Counts', fontsize=12, fontweight='bold')

        # Add value labels
        for i, v in enumerate(values):
            ax1.text(i, v + 20, f'{v:,}', ha='center', fontsize=10, fontweight='bold')

        # Right: Percentage coverage
        percentages = [(m[0] / m[1]) * 100 for m in metrics.values()]

        ax2.bar(range(len(labels)), percentages, color=sns.color_palette("rocket", len(labels)))
        ax2.set_xticks(range(len(labels)))
        ax2.set_xticklabels(labels, fontsize=10, rotation=15)
        ax2.set_ylabel('Coverage (%)', fontsize=11, fontweight='bold')
        ax2.set_title('Height Data Coverage: Percentage', fontsize=12, fontweight='bold')
        ax2.set_ylim(0, 10)

        # Add percentage labels
        for i, v in enumerate(percentages):
            ax2.text(i, v + 0.1, f'{v:.1f}%', ha='center', fontsize=10, fontweight='bold')

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '10_coverage_summary.png'), dpi=300, bbox_inches='tight')
        plt.close()

    def generate_report(self):
        """Generate comprehensive text report."""
        print("\nGenerating text report...")

        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("CINESCOPE BATCH 15: HEIGHT & PHYSICAL ATTRIBUTES ANALYSIS")
        report_lines.append("=" * 80)
        report_lines.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"\nTotal People in Dataset: {len(self.people_cache):,}")

        report_lines.append("\n" + "=" * 80)
        report_lines.append("HEIGHT DATA SUMMARY")
        report_lines.append("=" * 80)
        report_lines.append(f"\nPeople with Height Data: {self.stats['total_with_height']:,}")
        report_lines.append(f"Coverage: {(self.stats['total_with_height'] / len(self.people_cache)) * 100:.1f}%")

        report_lines.append(f"\nMean Height: {self.stats['mean_height']:.1f} cm")
        report_lines.append(f"Median Height: {self.stats['median_height']:.1f} cm")
        report_lines.append(f"Height Range: {self.stats['min_height']:.0f} - {self.stats['max_height']:.0f} cm")

        # Gender differences
        if 'gender_ttest_pvalue' in self.stats:
            report_lines.append("\n" + "=" * 80)
            report_lines.append("GENDER ANALYSIS")
            report_lines.append("=" * 80)
            report_lines.append(f"\nT-test p-value: {self.stats['gender_ttest_pvalue']:.4f}")
            if self.stats['gender_ttest_pvalue'] < 0.05:
                report_lines.append("Result: Significant height difference between genders")
            else:
                report_lines.append("Result: No significant height difference")

        # Temporal trends
        if 'height_trend_per_decade' in self.stats:
            report_lines.append("\n" + "=" * 80)
            report_lines.append("TEMPORAL TRENDS")
            report_lines.append("=" * 80)
            report_lines.append(f"\nHeight change per decade: {self.stats['height_trend_per_decade']:.3f} cm")

        # Popularity correlation
        if 'height_popularity_correlation' in self.stats:
            report_lines.append("\n" + "=" * 80)
            report_lines.append("HEIGHT-POPULARITY CORRELATION")
            report_lines.append("=" * 80)
            report_lines.append(f"\nCorrelation coefficient: {self.stats['height_popularity_correlation']:.3f}")

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
        print("CINESCOPE BATCH 15: HEIGHT & PHYSICAL ATTRIBUTES ANALYSIS")
        print("=" * 80)

        # Load data
        self.load_data()

        # Extract structured data
        self.extract_height_data()

        # Generate visualizations
        self.visualize_height_distribution()
        self.visualize_height_by_gender()
        self.visualize_height_by_profession()
        self.analyze_height_evolution()
        self.analyze_height_popularity_correlation()
        self.visualize_tallest_shortest()
        self.visualize_height_percentiles()
        self.visualize_gender_profession_height()
        self.visualize_height_ranges()
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
    analyzer = HeightPhysicalAnalyzer()
    analyzer.run_analysis()
