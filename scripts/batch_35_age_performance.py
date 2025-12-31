#!/usr/bin/env python3
"""
CineScope Batch 35: Age at Performance Analysis
================================================

Analyzes actor ages during filming, age ranges by role type,
and career trajectories based on age.

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
import json
from collections import Counter, defaultdict
import warnings
warnings.filterwarnings('ignore')

class AgePerformanceAnalyzer:
    """Analyze actor ages during film performances."""

    def __init__(self, data_path='data/processed/watched_movies_master.csv',
                 people_path='data/processed/people_cache.json'):
        """Initialize analyzer with watched movies and people data."""
        self.data_path = Path(data_path)
        self.people_path = Path(people_path)
        self.output_dir = Path('analysis_outputs/visualizations/batch_35')
        self.report_dir = Path('analysis_outputs/reports')

        # Create output directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)

        # Load data
        print("Loading watched movies data...")
        self.df = pd.read_csv(self.data_path)
        print(f"Loaded {len(self.df)} watched films")

        print("Loading people cache...")
        # Load JSON and transform to DataFrame
        with open(self.people_path) as f:
            people_data = json.load(f)

        # Convert dictionary to list of records
        people_records = []
        for person_id, person_info in people_data.items():
            person_info['id'] = person_id
            people_records.append(person_info)

        self.people_df = pd.DataFrame(people_records)
        print(f"Loaded {len(self.people_df)} people records")

        # Set up plotting
        plt.style.use('default')
        sns.set_palette("husl")

        # Calculate ages at performance
        self._calculate_ages()

    def _calculate_ages(self):
        """Calculate actor ages at time of filming."""
        print("Calculating ages at performance...")

        # Create lookup dictionary for birth years
        self.birth_years = {}
        for idx, row in self.people_df.iterrows():
            # Try ext_birth_date first, then birthday, then imdb_birth_year
            birth_year = None

            if pd.notna(row.get('ext_birth_date')):
                try:
                    birth_date = pd.to_datetime(row['ext_birth_date'])
                    birth_year = birth_date.year
                except:
                    pass
            elif pd.notna(row.get('birthday')):
                try:
                    birth_date = pd.to_datetime(row['birthday'])
                    birth_year = birth_date.year
                except:
                    pass
            elif pd.notna(row.get('imdb_birth_year')):
                try:
                    birth_year = int(row['imdb_birth_year'])
                except:
                    pass

            if birth_year and pd.notna(row.get('imdb_name')):
                self.birth_years[row['imdb_name']] = birth_year

        # Calculate ages for each film
        self.performance_data = []

        for idx, row in self.df.iterrows():
            if pd.isna(row.get('tmdb_cast')) or pd.isna(row.get('year')):
                continue

            try:
                film_year = int(row['year'])
                cast = ast.literal_eval(row['tmdb_cast'])

                for i, actor in enumerate(cast[:20]):  # Top 20 cast
                    actor_name = actor.get('name', '')
                    character = actor.get('character', '')

                    if actor_name in self.birth_years:
                        birth_year = self.birth_years[actor_name]
                        age_at_filming = film_year - birth_year

                        # Sanity check (ages between 5 and 100)
                        if 5 <= age_at_filming <= 100:
                            self.performance_data.append({
                                'actor': actor_name,
                                'film': row['title'],
                                'year': film_year,
                                'age': age_at_filming,
                                'character': character,
                                'billing_order': i + 1,
                                'rating': row['IMDb Rating'],
                                'genre': row['Genres']
                            })
            except:
                continue

        self.perf_df = pd.DataFrame(self.performance_data)
        print(f"  Calculated {len(self.perf_df)} age-at-performance records")
        print(f"  Covering {self.perf_df['actor'].nunique()} unique actors")

    def visualize_age_distribution(self):
        """Visualization 1-4: Age distribution analysis."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Age at Performance Distribution', fontsize=16, fontweight='bold')

        # 1. Overall age distribution
        ax1 = axes[0, 0]
        ax1.hist(self.perf_df['age'], bins=40, color='#3498DB', edgecolor='black', alpha=0.7)
        ax1.axvline(self.perf_df['age'].mean(), color='red', linestyle='--',
                   linewidth=2, label=f"Mean: {self.perf_df['age'].mean():.1f}")
        ax1.axvline(self.perf_df['age'].median(), color='blue', linestyle='--',
                   linewidth=2, label=f"Median: {self.perf_df['age'].median():.1f}")
        ax1.set_xlabel('Age at Performance', fontsize=11, fontweight='bold')
        ax1.set_ylabel('Number of Performances', fontsize=11, fontweight='bold')
        ax1.set_title('Overall Age Distribution', fontsize=12, fontweight='bold')
        ax1.legend()
        ax1.grid(axis='y', alpha=0.3)

        # 2. Age by billing order
        ax2 = axes[0, 1]
        billing_groups = [1, 2, 3, 4, 5]
        age_by_billing = []

        for billing in billing_groups:
            ages = self.perf_df[self.perf_df['billing_order'] == billing]['age'].dropna()
            if len(ages) > 0:
                age_by_billing.append(ages)

        if age_by_billing:
            bp = ax2.boxplot(age_by_billing, labels=[f'#{i}' for i in billing_groups],
                            patch_artist=True)
            for patch in bp['boxes']:
                patch.set_facecolor('#2ECC71')

            ax2.set_xlabel('Billing Position', fontsize=11, fontweight='bold')
            ax2.set_ylabel('Age', fontsize=11, fontweight='bold')
            ax2.set_title('Age by Billing Order', fontsize=12, fontweight='bold')
            ax2.grid(axis='y', alpha=0.3)

        # 3. Age vs rating scatter
        ax3 = axes[1, 0]
        scatter = ax3.scatter(self.perf_df['age'], self.perf_df['rating'],
                             alpha=0.3, s=20, c=self.perf_df['billing_order'],
                             cmap='viridis', edgecolors='none')
        ax3.set_xlabel('Age at Performance', fontsize=11, fontweight='bold')
        ax3.set_ylabel('Film Rating', fontsize=11, fontweight='bold')
        ax3.set_title('Age vs Film Quality', fontsize=12, fontweight='bold')
        ax3.grid(True, alpha=0.3)

        # Add colorbar
        cbar = plt.colorbar(scatter, ax=ax3)
        cbar.set_label('Billing Order', fontsize=10, fontweight='bold')

        # 4. Age statistics by decade
        ax4 = axes[1, 1]
        ax4.axis('off')

        stats_text = "AGE STATISTICS\n" + "="*50 + "\n\n"
        stats_text += f"Total Performances: {len(self.perf_df):,}\n"
        stats_text += f"Unique Actors: {self.perf_df['actor'].nunique():,}\n\n"

        stats_text += f"Mean Age: {self.perf_df['age'].mean():.1f} years\n"
        stats_text += f"Median Age: {self.perf_df['age'].median():.1f} years\n"
        stats_text += f"Std Dev: {self.perf_df['age'].std():.1f} years\n\n"

        stats_text += f"Youngest: {self.perf_df['age'].min():.0f} years\n"
        stats_text += f"Oldest: {self.perf_df['age'].max():.0f} years\n\n"

        # Age ranges
        stats_text += "AGE RANGES:\n"
        ranges = [
            ('Child/Teen', 5, 17),
            ('Young Adult', 18, 30),
            ('Adult', 31, 50),
            ('Mature', 51, 70),
            ('Senior', 71, 100)
        ]

        for label, min_age, max_age in ranges:
            count = len(self.perf_df[(self.perf_df['age'] >= min_age) &
                                     (self.perf_df['age'] <= max_age)])
            pct = count / len(self.perf_df) * 100
            stats_text += f"  {label:12s} ({min_age:2d}-{max_age:2d}): {count:5d} ({pct:4.1f}%)\n"

        ax4.text(0.1, 0.9, stats_text, transform=ax4.transAxes,
                fontsize=10, verticalalignment='top',
                family='monospace',
                bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.5))
        ax4.set_title('Performance Statistics', fontsize=12, fontweight='bold')

        plt.tight_layout()
        output_path = self.output_dir / 'age_distribution.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_career_trajectories(self):
        """Visualization 5-6: Career trajectory analysis."""
        fig, axes = plt.subplots(1, 2, figsize=(16, 8))
        fig.suptitle('Career Trajectory Analysis', fontsize=16, fontweight='bold')

        # Get actors with 5+ performances
        actor_counts = self.perf_df['actor'].value_counts()
        prolific_actors = actor_counts[actor_counts >= 5].index.tolist()[:20]

        # 1. Age progression for top actors
        ax1 = axes[0]

        for i, actor in enumerate(prolific_actors[:10]):
            actor_data = self.perf_df[self.perf_df['actor'] == actor].sort_values('year')
            ax1.plot(actor_data['year'], actor_data['age'],
                    marker='o', linewidth=1, markersize=4, alpha=0.7,
                    label=actor[:20])

        ax1.set_xlabel('Year', fontsize=11, fontweight='bold')
        ax1.set_ylabel('Age', fontsize=11, fontweight='bold')
        ax1.set_title('Career Trajectories (Top 10 Actors)', fontsize=12, fontweight='bold')
        ax1.legend(fontsize=7, loc='upper left')
        ax1.grid(True, alpha=0.3)

        # 2. Average age over time
        ax2 = axes[1]

        # Group by decade
        self.perf_df['decade'] = (self.perf_df['year'] // 10) * 10
        decade_ages = self.perf_df.groupby('decade')['age'].agg(['mean', 'median', 'std'])

        decades = decade_ages.index
        ax2.plot(decades, decade_ages['mean'], marker='o', linewidth=2,
                markersize=8, label='Mean Age', color='#E74C3C')
        ax2.plot(decades, decade_ages['median'], marker='s', linewidth=2,
                markersize=8, label='Median Age', color='#3498DB')

        # Fill between std
        ax2.fill_between(decades,
                        decade_ages['mean'] - decade_ages['std'],
                        decade_ages['mean'] + decade_ages['std'],
                        alpha=0.2, color='#E74C3C')

        ax2.set_xlabel('Decade', fontsize=11, fontweight='bold')
        ax2.set_ylabel('Age', fontsize=11, fontweight='bold')
        ax2.set_title('Average Actor Age Over Time', fontsize=12, fontweight='bold')
        ax2.legend()
        ax2.grid(True, alpha=0.3)

        plt.tight_layout()
        output_path = self.output_dir / 'career_trajectories.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_age_by_genre(self):
        """Visualization 7-8: Age patterns by genre."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Age Patterns by Genre', fontsize=16, fontweight='bold')

        # Extract genre for each performance
        genre_ages = defaultdict(list)

        for idx, row in self.perf_df.iterrows():
            if pd.notna(row['genre']):
                genres = [g.strip() for g in str(row['genre']).split(',')]
                for genre in genres:
                    genre_ages[genre].append(row['age'])

        # Get top genres
        genre_counts = {g: len(ages) for g, ages in genre_ages.items()}
        top_genres = sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)[:12]

        # 1. Average age by genre
        ax1 = axes[0, 0]
        genres = [g for g, _ in top_genres]
        avg_ages = [np.mean(genre_ages[g]) for g in genres]

        bars = ax1.barh(range(len(genres)), avg_ages,
                       color=plt.cm.tab10(np.linspace(0, 1, len(genres))),
                       edgecolor='black')
        ax1.set_yticks(range(len(genres)))
        ax1.set_yticklabels(genres, fontsize=9)
        ax1.set_xlabel('Average Age', fontsize=11, fontweight='bold')
        ax1.set_title('Average Actor Age by Genre', fontsize=12, fontweight='bold')
        ax1.invert_yaxis()
        ax1.grid(axis='x', alpha=0.3)

        # Annotate
        for i, age in enumerate(avg_ages):
            ax1.text(age + 0.5, i, f'{age:.1f}', va='center', fontsize=8)

        # 2. Age distribution by genre (box plot)
        ax2 = axes[0, 1]
        age_data = [genre_ages[g] for g in genres[:8]]

        bp = ax2.boxplot(age_data, labels=genres[:8], patch_artist=True)
        for patch, color in zip(bp['boxes'], plt.cm.tab10(np.linspace(0, 1, 8))):
            patch.set_facecolor(color)

        ax2.set_xticklabels(genres[:8], rotation=45, ha='right', fontsize=8)
        ax2.set_ylabel('Age', fontsize=11, fontweight='bold')
        ax2.set_title('Age Distribution by Genre (Top 8)', fontsize=12, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        # 3. Genre performance count by age range
        ax3 = axes[1, 0]

        age_ranges = ['5-17', '18-30', '31-50', '51-70', '71+']
        top_4_genres = genres[:4]

        # Create stacked bar
        bottom = np.zeros(len(age_ranges))
        colors_stack = plt.cm.Set3(np.linspace(0, 1, len(top_4_genres)))

        for i, genre in enumerate(top_4_genres):
            counts = []
            for age_range in age_ranges:
                if age_range == '5-17':
                    count = sum(1 for age in genre_ages[genre] if 5 <= age <= 17)
                elif age_range == '18-30':
                    count = sum(1 for age in genre_ages[genre] if 18 <= age <= 30)
                elif age_range == '31-50':
                    count = sum(1 for age in genre_ages[genre] if 31 <= age <= 50)
                elif age_range == '51-70':
                    count = sum(1 for age in genre_ages[genre] if 51 <= age <= 70)
                else:  # 71+
                    count = sum(1 for age in genre_ages[genre] if age >= 71)
                counts.append(count)

            ax3.bar(range(len(age_ranges)), counts, bottom=bottom,
                   label=genre, color=colors_stack[i], edgecolor='white', linewidth=0.5)
            bottom += counts

        ax3.set_xticks(range(len(age_ranges)))
        ax3.set_xticklabels(age_ranges)
        ax3.set_xlabel('Age Range', fontsize=11, fontweight='bold')
        ax3.set_ylabel('Number of Performances', fontsize=11, fontweight='bold')
        ax3.set_title('Age Range Distribution (Top 4 Genres)', fontsize=12, fontweight='bold')
        ax3.legend(fontsize=8)
        ax3.grid(axis='y', alpha=0.3)

        # 4. Youngest and oldest performers by genre
        ax4 = axes[1, 1]
        ax4.axis('off')

        extremes_text = "AGE EXTREMES BY GENRE\n" + "="*50 + "\n\n"

        for genre in genres[:6]:
            ages = genre_ages[genre]
            if ages:
                youngest = min(ages)
                oldest = max(ages)
                extremes_text += f"{genre}:\n"
                extremes_text += f"  Youngest: {youngest:.0f} | Oldest: {oldest:.0f}\n"
                extremes_text += f"  Range: {oldest - youngest:.0f} years\n\n"

        ax4.text(0.1, 0.9, extremes_text, transform=ax4.transAxes,
                fontsize=10, verticalalignment='top',
                family='monospace',
                bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.6))
        ax4.set_title('Age Extremes', fontsize=12, fontweight='bold')

        plt.tight_layout()
        output_path = self.output_dir / 'age_by_genre.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_notable_performances(self):
        """Visualization 9-10: Notable age performances."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Notable Age Performances', fontsize=16, fontweight='bold')

        # 1. Youngest performances
        ax1 = axes[0, 0]
        ax1.axis('off')

        youngest = self.perf_df.nsmallest(15, 'age')
        young_text = "YOUNGEST PERFORMANCES\n" + "="*50 + "\n\n"

        for i, (idx, row) in enumerate(youngest.iterrows(), 1):
            young_text += f"{i:2d}. {row['actor'][:30]}\n"
            young_text += f"    Age {row['age']:.0f} in '{row['film'][:35]}'\n"
            young_text += f"    ({row['year']:.0f}) Rating: {row['rating']:.1f}\n\n"

        ax1.text(0.1, 0.9, young_text, transform=ax1.transAxes,
                fontsize=8, verticalalignment='top',
                family='monospace',
                bbox=dict(boxstyle='round', facecolor='#E6F3FF', alpha=0.7))
        ax1.set_title('Youngest Performances', fontsize=12, fontweight='bold')

        # 2. Oldest performances
        ax2 = axes[0, 1]
        ax2.axis('off')

        oldest = self.perf_df.nlargest(15, 'age')
        old_text = "OLDEST PERFORMANCES\n" + "="*50 + "\n\n"

        for i, (idx, row) in enumerate(oldest.iterrows(), 1):
            old_text += f"{i:2d}. {row['actor'][:30]}\n"
            old_text += f"    Age {row['age']:.0f} in '{row['film'][:35]}'\n"
            old_text += f"    ({row['year']:.0f}) Rating: {row['rating']:.1f}\n\n"

        ax2.text(0.1, 0.9, old_text, transform=ax2.transAxes,
                fontsize=8, verticalalignment='top',
                family='monospace',
                bbox=dict(boxstyle='round', facecolor='#FFF0E6', alpha=0.7))
        ax2.set_title('Oldest Performances', fontsize=12, fontweight='bold')

        # 3. Most prolific actors
        ax3 = axes[1, 0]
        actor_counts = self.perf_df['actor'].value_counts().head(15)

        y_pos = np.arange(len(actor_counts))
        bars = ax3.barh(y_pos, actor_counts.values, color='#9B59B6', edgecolor='black')
        ax3.set_yticks(y_pos)
        ax3.set_yticklabels([name[:25] for name in actor_counts.index], fontsize=8)
        ax3.set_xlabel('Number of Performances', fontsize=11, fontweight='bold')
        ax3.set_title('Most Prolific Actors in Collection', fontsize=12, fontweight='bold')
        ax3.invert_yaxis()
        ax3.grid(axis='x', alpha=0.3)

        # 4. Career span analysis
        ax4 = axes[1, 1]

        # Calculate career spans for prolific actors
        career_spans = []
        for actor in actor_counts.head(10).index:
            actor_data = self.perf_df[self.perf_df['actor'] == actor]
            span = actor_data['age'].max() - actor_data['age'].min()
            career_spans.append({
                'actor': actor,
                'span': span,
                'min_age': actor_data['age'].min(),
                'max_age': actor_data['age'].max()
            })

        career_df = pd.DataFrame(career_spans).sort_values('span', ascending=False)

        y_pos = np.arange(len(career_df))
        bars = ax4.barh(y_pos, career_df['span'].values, color='#F39C12', edgecolor='black')
        ax4.set_yticks(y_pos)
        ax4.set_yticklabels([name[:25] for name in career_df['actor'].values], fontsize=8)
        ax4.set_xlabel('Career Span (years)', fontsize=11, fontweight='bold')
        ax4.set_title('Longest Career Spans (In Collection)', fontsize=12, fontweight='bold')
        ax4.invert_yaxis()
        ax4.grid(axis='x', alpha=0.3)

        # Annotate with age ranges
        for i, row in career_df.iterrows():
            ax4.text(row['span'] + 0.5, list(career_df.index).index(i),
                    f"{row['min_age']:.0f}-{row['max_age']:.0f}",
                    va='center', fontsize=7)

        plt.tight_layout()
        output_path = self.output_dir / 'notable_performances.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def generate_report(self):
        """Generate comprehensive age analysis report."""
        print("\nGenerating age at performance report...")

        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("CINESCOPE BATCH 35: AGE AT PERFORMANCE ANALYSIS")
        report_lines.append("=" * 80)
        report_lines.append("")
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("")

        # Overall stats
        report_lines.append("=" * 80)
        report_lines.append("OVERALL STATISTICS")
        report_lines.append("=" * 80)
        report_lines.append("")
        report_lines.append(f"Total Performances Analyzed: {len(self.perf_df):,}")
        report_lines.append(f"Unique Actors: {self.perf_df['actor'].nunique():,}")
        report_lines.append(f"Unique Films: {self.perf_df['film'].nunique():,}")
        report_lines.append("")
        report_lines.append(f"Average Age: {self.perf_df['age'].mean():.1f} years")
        report_lines.append(f"Median Age: {self.perf_df['age'].median():.1f} years")
        report_lines.append(f"Youngest Performance: {self.perf_df['age'].min():.0f} years")
        report_lines.append(f"Oldest Performance: {self.perf_df['age'].max():.0f} years")
        report_lines.append("")

        # Youngest performances
        report_lines.append("=" * 80)
        report_lines.append("YOUNGEST PERFORMANCES")
        report_lines.append("=" * 80)
        report_lines.append("")

        youngest = self.perf_df.nsmallest(20, 'age')
        for i, (idx, row) in enumerate(youngest.iterrows(), 1):
            report_lines.append(f"{i:2d}. {row['actor']:40s} - Age {row['age']:2.0f}")
            report_lines.append(f"    '{row['film']}'  ({row['year']:.0f}) - Rating: {row['rating']:.1f}")

        report_lines.append("")

        # Oldest performances
        report_lines.append("=" * 80)
        report_lines.append("OLDEST PERFORMANCES")
        report_lines.append("=" * 80)
        report_lines.append("")

        oldest = self.perf_df.nlargest(20, 'age')
        for i, (idx, row) in enumerate(oldest.iterrows(), 1):
            report_lines.append(f"{i:2d}. {row['actor']:40s} - Age {row['age']:2.0f}")
            report_lines.append(f"    '{row['film']}' ({row['year']:.0f}) - Rating: {row['rating']:.1f}")

        report_lines.append("")

        # Most prolific actors
        report_lines.append("=" * 80)
        report_lines.append("MOST PROLIFIC ACTORS (In Collection)")
        report_lines.append("=" * 80)
        report_lines.append("")

        actor_counts = self.perf_df['actor'].value_counts().head(30)
        for i, (actor, count) in enumerate(actor_counts.items(), 1):
            actor_data = self.perf_df[self.perf_df['actor'] == actor]
            avg_age = actor_data['age'].mean()
            age_range = f"{actor_data['age'].min():.0f}-{actor_data['age'].max():.0f}"
            report_lines.append(f"{i:2d}. {actor:40s} - {count:3d} performances (Age range: {age_range}, Avg: {avg_age:.1f})")

        report_lines.append("")
        report_lines.append("=" * 80)
        report_lines.append("END OF REPORT")
        report_lines.append("=" * 80)

        # Write report
        report_path = self.report_dir / 'batch_35_age_performance_report.txt'
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))

        print(f"Report saved: {report_path}")

    def visualize_age_rating_patterns(self):
        """Visualization 13-14: Age vs performance quality patterns."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Age & Performance Quality Patterns', fontsize=16, fontweight='bold')

        # 1. Age bins vs average rating
        ax1 = axes[0, 0]
        age_bins = [0, 20, 30, 40, 50, 60, 100]
        age_labels = ['<20', '20-29', '30-39', '40-49', '50-59', '60+']
        self.perf_df['age_bin'] = pd.cut(self.perf_df['age'], bins=age_bins, labels=age_labels)

        bin_stats = self.perf_df.groupby('age_bin')['rating'].agg(['mean', 'count'])

        x_pos = np.arange(len(bin_stats))
        bars = ax1.bar(x_pos, bin_stats['mean'], color='#3498DB', edgecolor='black', alpha=0.7)
        ax1.set_xticks(x_pos)
        ax1.set_xticklabels(age_labels)
        ax1.set_ylabel('Average Film Rating', fontsize=11, fontweight='bold')
        ax1.set_xlabel('Actor Age Range', fontsize=11, fontweight='bold')
        ax1.set_title('Film Quality by Actor Age Range', fontsize=12, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)
        ax1.set_ylim(0, 10)

        # Add count annotations
        for i, (bar, count) in enumerate(zip(bars, bin_stats['count'])):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                    f'{height:.2f}\n({count} perf)',
                    ha='center', va='bottom', fontsize=8)

        # 2. Lead vs supporting age patterns
        ax2 = axes[0, 1]
        lead_ages = self.perf_df[self.perf_df['billing_order'] == 1]['age']
        supporting_ages = self.perf_df[self.perf_df['billing_order'] > 1]['age']

        bp = ax2.boxplot([lead_ages, supporting_ages],
                         labels=['Lead (Billing #1)', 'Supporting (Billing 2+)'],
                         patch_artist=True)
        bp['boxes'][0].set_facecolor('#E74C3C')
        bp['boxes'][1].set_facecolor('#2ECC71')

        ax2.set_ylabel('Age', fontsize=11, fontweight='bold')
        ax2.set_title('Lead vs Supporting Actor Ages', fontsize=12, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        # Add mean lines
        ax2.axhline(lead_ages.mean(), color='red', linestyle='--', alpha=0.5,
                   label=f'Lead Mean: {lead_ages.mean():.1f}')
        ax2.axhline(supporting_ages.mean(), color='green', linestyle='--', alpha=0.5,
                   label=f'Support Mean: {supporting_ages.mean():.1f}')
        ax2.legend(fontsize=8)

        # 3. Age gap analysis (top billed actors)
        ax3 = axes[1, 0]
        age_gaps = []
        for idx, row in self.df.iterrows():
            if pd.isna(row.get('tmdb_cast')) or pd.isna(row.get('year')):
                continue
            try:
                film_year = int(row['year'])
                cast = ast.literal_eval(row['tmdb_cast'])
                ages = []

                for i, actor in enumerate(cast[:2]):  # Top 2
                    actor_name = actor.get('name', '')
                    if actor_name in self.birth_years:
                        age = film_year - self.birth_years[actor_name]
                        if 5 <= age <= 100:
                            ages.append(age)

                if len(ages) == 2:
                    gap = abs(ages[0] - ages[1])
                    age_gaps.append({'film': row['title'], 'gap': gap, 'rating': row['IMDb Rating']})
            except:
                continue

        if age_gaps:
            gap_df = pd.DataFrame(age_gaps)
            ax3.scatter(gap_df['gap'], gap_df['rating'], alpha=0.5, s=30, color='#9B59B6')
            ax3.set_xlabel('Age Gap (Top 2 Billing)', fontsize=11, fontweight='bold')
            ax3.set_ylabel('Film Rating', fontsize=11, fontweight='bold')
            ax3.set_title('Age Gap vs Film Quality', fontsize=12, fontweight='bold')
            ax3.grid(True, alpha=0.3)

            # Add trend line
            z = np.polyfit(gap_df['gap'], gap_df['rating'], 1)
            p = np.poly1d(z)
            ax3.plot(gap_df['gap'].sort_values(), p(gap_df['gap'].sort_values()),
                    "r--", alpha=0.8, linewidth=2, label=f'Trend')
            ax3.legend()

        # 4. Prime performance age analysis
        ax4 = axes[1, 1]
        ax4.axis('off')

        # Find optimal age ranges
        high_rated = self.perf_df[self.perf_df['rating'] >= 8.0]
        age_dist = high_rated['age'].value_counts().sort_index()

        prime_text = "PRIME PERFORMANCE AGES\n" + "="*50 + "\n\n"
        prime_text += "Ages with most high-rated performances (8.0+):\n\n"

        top_ages = high_rated['age'].value_counts().head(10)
        for i, (age, count) in enumerate(top_ages.items(), 1):
            prime_text += f"{i:2d}. Age {age:.0f}: {count} high-rated performances\n"

        prime_text += f"\n\nAverage age in high-rated films: {high_rated['age'].mean():.1f}\n"
        prime_text += f"Median age in high-rated films: {high_rated['age'].median():.1f}\n\n"

        prime_text += f"Overall average age: {self.perf_df['age'].mean():.1f}\n"
        prime_text += f"Difference: {high_rated['age'].mean() - self.perf_df['age'].mean():.1f} years\n"

        ax4.text(0.1, 0.9, prime_text, transform=ax4.transAxes,
                fontsize=10, verticalalignment='top',
                family='monospace',
                bbox=dict(boxstyle='round', facecolor='#E8F8F5', alpha=0.7))
        ax4.set_title('Prime Performance Analysis', fontsize=12, fontweight='bold')

        plt.tight_layout()
        output_path = self.output_dir / 'age_rating_patterns.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_decade_age_evolution(self):
        """Visualization 15-16: How casting ages evolved over decades."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Age Evolution Across Cinema History', fontsize=16, fontweight='bold')

        # 1. Average age by decade with gender breakdown (if available)
        ax1 = axes[0, 0]
        self.perf_df['decade'] = (self.perf_df['year'] // 10) * 10
        decade_age = self.perf_df.groupby('decade')['age'].mean().sort_index()

        ax1.plot(decade_age.index, decade_age.values, marker='o', linewidth=3,
                markersize=10, color='#E74C3C', label='Average Age')
        ax1.fill_between(decade_age.index, decade_age.values, alpha=0.3, color='#E74C3C')
        ax1.set_xlabel('Decade', fontsize=11, fontweight='bold')
        ax1.set_ylabel('Average Actor Age', fontsize=11, fontweight='bold')
        ax1.set_title('Age Trends Over Decades', fontsize=12, fontweight='bold')
        ax1.grid(True, alpha=0.3)
        ax1.legend()

        # 2. Age range expansion over time
        ax2 = axes[0, 1]
        decade_stats = self.perf_df.groupby('decade')['age'].agg(['min', 'max', 'mean'])

        ax2.fill_between(decade_stats.index, decade_stats['min'], decade_stats['max'],
                        alpha=0.3, color='#3498DB', label='Age Range')
        ax2.plot(decade_stats.index, decade_stats['mean'], marker='o', linewidth=2,
                color='#E74C3C', label='Mean Age', markersize=8)
        ax2.set_xlabel('Decade', fontsize=11, fontweight='bold')
        ax2.set_ylabel('Age', fontsize=11, fontweight='bold')
        ax2.set_title('Age Range Evolution', fontsize=12, fontweight='bold')
        ax2.grid(True, alpha=0.3)
        ax2.legend()

        # 3. Lead actor age by decade
        ax3 = axes[1, 0]
        lead_only = self.perf_df[self.perf_df['billing_order'] == 1]
        lead_decade_age = lead_only.groupby('decade')['age'].mean().sort_index()

        bars = ax3.bar(lead_decade_age.index, lead_decade_age.values, width=8,
                      color='#9B59B6', edgecolor='black', alpha=0.7)
        ax3.set_xlabel('Decade', fontsize=11, fontweight='bold')
        ax3.set_ylabel('Average Lead Actor Age', fontsize=11, fontweight='bold')
        ax3.set_title('Lead Actor Age by Decade', fontsize=12, fontweight='bold')
        ax3.grid(axis='y', alpha=0.3)

        # Annotate bars
        for bar in bars:
            height = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.1f}', ha='center', va='bottom', fontsize=8)

        # 4. Performance count by age group over decades
        ax4 = axes[1, 1]
        age_groups = {
            'Young (18-30)': (18, 30),
            'Adult (31-45)': (31, 45),
            'Mature (46-60)': (46, 60),
            'Senior (61+)': (61, 100)
        }

        decades = sorted(self.perf_df['decade'].unique())
        bottom = np.zeros(len(decades))
        colors = ['#3498DB', '#2ECC71', '#F39C12', '#E74C3C']

        for i, (label, (min_age, max_age)) in enumerate(age_groups.items()):
            counts = []
            for decade in decades:
                count = len(self.perf_df[(self.perf_df['decade'] == decade) &
                                         (self.perf_df['age'] >= min_age) &
                                         (self.perf_df['age'] <= max_age)])
                counts.append(count)

            ax4.bar(decades, counts, bottom=bottom, label=label,
                   color=colors[i], edgecolor='white', linewidth=0.5)
            bottom += counts

        ax4.set_xlabel('Decade', fontsize=11, fontweight='bold')
        ax4.set_ylabel('Number of Performances', fontsize=11, fontweight='bold')
        ax4.set_title('Age Group Distribution Over Time', fontsize=12, fontweight='bold')
        ax4.legend(fontsize=9)
        ax4.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        output_path = self.output_dir / 'decade_age_evolution.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def run_all_analyses(self):
        """Execute all analyses and generate report."""
        print("\n" + "="*80)
        print("BATCH 35: AGE AT PERFORMANCE ANALYSIS")
        print("="*80)

        print("\n[1/7] Analyzing age distribution...")
        self.visualize_age_distribution()

        print("\n[2/7] Analyzing career trajectories...")
        self.visualize_career_trajectories()

        print("\n[3/7] Analyzing age patterns by genre...")
        self.visualize_age_by_genre()

        print("\n[4/7] Identifying notable performances...")
        self.visualize_notable_performances()

        print("\n[5/7] Analyzing age-rating patterns...")
        self.visualize_age_rating_patterns()

        print("\n[6/7] Analyzing age evolution over decades...")
        self.visualize_decade_age_evolution()

        print("\n[7/7] Generating comprehensive report...")
        self.generate_report()

        print("\n" + "="*80)
        print("BATCH 35 COMPLETE")
        print("="*80)
        print(f"\nVisualizations saved to: {self.output_dir}")
        print(f"Report saved to: {self.report_dir}/batch_35_age_performance_report.txt")


def main():
    """Main execution function."""
    analyzer = AgePerformanceAnalyzer()
    analyzer.run_all_analyses()


if __name__ == '__main__':
    main()
