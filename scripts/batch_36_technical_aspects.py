#!/usr/bin/env python3
"""
CineScope Batch 36: Technical Aspects Analysis
===============================================

Analyzes cinematography, editing, sound design, and visual effects
patterns across watched films.

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
import warnings
warnings.filterwarnings('ignore')

class TechnicalAspectsAnalyzer:
    """Analyze technical filmmaking aspects."""

    def __init__(self, data_path='data/processed/watched_movies_master.csv'):
        """Initialize analyzer with watched movies data."""
        self.data_path = Path(data_path)
        self.output_dir = Path('analysis_outputs/visualizations/batch_36')
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

        # Extract technical keywords
        self._extract_technical_indicators()

    def _extract_technical_indicators(self):
        """Extract technical aspect indicators from keywords and crew."""
        print("Extracting technical indicators...")

        # Technical keyword categories
        self.technical_keywords = {
            'Cinematography': ['cinematography', 'camera', 'visual', 'shot', 'lighting',
                              'photography', 'color grading', 'black and white', 'widescreen'],
            'Editing': ['editing', 'montage', 'cut', 'pace', 'flashback', 'nonlinear',
                       'parallel editing', 'cross-cutting'],
            'Sound': ['sound', 'music', 'score', 'soundtrack', 'audio', 'silence',
                     'sound design', 'diegetic', 'ambient'],
            'VFX': ['visual effects', 'vfx', 'cgi', 'special effects', 'animation',
                   'motion capture', 'green screen', 'practical effects'],
            'Production': ['production design', 'set design', 'costume', 'makeup',
                          'props', 'art direction', 'period piece']
        }

        # Count technical indicators per film
        for category, keywords in self.technical_keywords.items():
            self.df[f'tech_{category.lower()}'] = 0

            for idx, row in self.df.iterrows():
                score = 0

                # Check keywords
                if pd.notna(row.get('keyword_themes')):
                    themes_str = str(row['keyword_themes']).lower()
                    for keyword in keywords:
                        if keyword in themes_str:
                            score += 1

                # Check plot
                if pd.notna(row.get('plot')):
                    plot_str = str(row['plot']).lower()
                    for keyword in keywords:
                        if keyword in plot_str:
                            score += 0.5

                self.df.at[idx, f'tech_{category.lower()}'] = score

        print(f"  Extracted technical indicators for {len(self.df)} films")

    def visualize_technical_distribution(self):
        """Visualization 1-4: Technical aspects distribution."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Technical Aspects Distribution', fontsize=16, fontweight='bold')

        categories = list(self.technical_keywords.keys())
        tech_cols = [f'tech_{cat.lower()}' for cat in categories]

        # 1. Overall technical emphasis
        ax1 = axes[0, 0]
        avg_scores = [self.df[col].mean() for col in tech_cols]

        bars = ax1.barh(range(len(categories)), avg_scores,
                       color=plt.cm.viridis(np.linspace(0, 1, len(categories))),
                       edgecolor='black')
        ax1.set_yticks(range(len(categories)))
        ax1.set_yticklabels(categories)
        ax1.set_xlabel('Average Emphasis Score', fontsize=11, fontweight='bold')
        ax1.set_title('Technical Emphasis Across Films', fontsize=12, fontweight='bold')
        ax1.invert_yaxis()
        ax1.grid(axis='x', alpha=0.3)

        # Annotate
        for i, score in enumerate(avg_scores):
            ax1.text(score + 0.02, i, f'{score:.2f}', va='center', fontsize=9)

        # 2. Technical complexity over time
        ax2 = axes[0, 1]
        self.df['decade'] = (self.df['year'] // 10) * 10
        self.df['total_tech'] = self.df[tech_cols].sum(axis=1)

        decade_tech = self.df.groupby('decade')['total_tech'].mean().sort_index()

        ax2.plot(decade_tech.index, decade_tech.values, marker='o', linewidth=3,
                markersize=10, color='#E74C3C')
        ax2.fill_between(decade_tech.index, decade_tech.values, alpha=0.3, color='#E74C3C')
        ax2.set_xlabel('Decade', fontsize=11, fontweight='bold')
        ax2.set_ylabel('Average Technical Complexity', fontsize=11, fontweight='bold')
        ax2.set_title('Technical Complexity Evolution', fontsize=12, fontweight='bold')
        ax2.grid(True, alpha=0.3)

        # 3. Technical aspects by rating
        ax3 = axes[1, 0]
        rating_bins = [0, 6, 7, 8, 9, 10]
        rating_labels = ['<6', '6-7', '7-8', '8-9', '9-10']
        self.df['rating_bin'] = pd.cut(self.df['IMDb Rating'], bins=rating_bins, labels=rating_labels)

        tech_by_rating = self.df.groupby('rating_bin')['total_tech'].mean()

        bars = ax3.bar(range(len(tech_by_rating)), tech_by_rating.values,
                      color='#2ECC71', edgecolor='black', alpha=0.7)
        ax3.set_xticks(range(len(tech_by_rating)))
        ax3.set_xticklabels(rating_labels)
        ax3.set_xlabel('Rating Range', fontsize=11, fontweight='bold')
        ax3.set_ylabel('Average Technical Score', fontsize=11, fontweight='bold')
        ax3.set_title('Technical Complexity vs Rating', fontsize=12, fontweight='bold')
        ax3.grid(axis='y', alpha=0.3)

        # Annotate
        for bar in bars:
            height = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.2f}', ha='center', va='bottom', fontsize=9)

        # 4. Correlation heatmap
        ax4 = axes[1, 1]
        correlation = self.df[tech_cols].corr()

        im = ax4.imshow(correlation, cmap='coolwarm', aspect='auto', vmin=-1, vmax=1)
        ax4.set_xticks(np.arange(len(categories)))
        ax4.set_yticks(np.arange(len(categories)))
        ax4.set_xticklabels(categories, rotation=45, ha='right', fontsize=9)
        ax4.set_yticklabels(categories, fontsize=9)
        ax4.set_title('Technical Aspects Correlation', fontsize=12, fontweight='bold')

        # Colorbar
        cbar = plt.colorbar(im, ax=ax4)
        cbar.set_label('Correlation', fontsize=10, fontweight='bold')

        # Annotate cells
        for i in range(len(categories)):
            for j in range(len(categories)):
                text = ax4.text(j, i, f'{correlation.iloc[i, j]:.2f}',
                              ha="center", va="center", color="black", fontsize=8)

        plt.tight_layout()
        output_path = self.output_dir / 'technical_distribution.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_cinematography_patterns(self):
        """Visualization 5-6: Cinematography analysis."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Cinematography Patterns', fontsize=16, fontweight='bold')

        # 1. Top cinematography-focused films
        ax1 = axes[0, 0]
        ax1.axis('off')

        top_cinematography = self.df.nlargest(15, 'tech_cinematography')
        cine_text = "TOP CINEMATOGRAPHY FILMS\n" + "="*50 + "\n\n"

        for i, (idx, row) in enumerate(top_cinematography.iterrows(), 1):
            cine_text += f"{i:2d}. {str(row['title'])[:40]}\n"
            cine_text += f"    Score: {row['tech_cinematography']:.1f} | Rating: {row['IMDb Rating']:.1f}\n"
            if pd.notna(row.get('Directors')):
                cine_text += f"    Dir: {str(row['Directors'])[:35]}\n"
            cine_text += "\n"

        ax1.text(0.1, 0.9, cine_text, transform=ax1.transAxes,
                fontsize=8, verticalalignment='top',
                family='monospace',
                bbox=dict(boxstyle='round', facecolor='#E6F3FF', alpha=0.7))
        ax1.set_title('Cinematography Excellence', fontsize=12, fontweight='bold')

        # 2. Cinematography by genre
        ax2 = axes[0, 1]
        genre_cine = defaultdict(list)

        for idx, row in self.df.iterrows():
            if pd.notna(row.get('Genres')):
                genres = [g.strip() for g in str(row['Genres']).split(',')]
                for genre in genres:
                    genre_cine[genre].append(row['tech_cinematography'])

        genre_avg = {g: np.mean(scores) for g, scores in genre_cine.items()
                     if len(scores) >= 10}
        sorted_genres = sorted(genre_avg.items(), key=lambda x: x[1], reverse=True)[:12]
        genres, scores = zip(*sorted_genres)

        bars = ax2.barh(range(len(genres)), scores,
                       color=plt.cm.plasma(np.linspace(0, 1, len(genres))),
                       edgecolor='black')
        ax2.set_yticks(range(len(genres)))
        ax2.set_yticklabels(genres, fontsize=9)
        ax2.set_xlabel('Average Cinematography Score', fontsize=11, fontweight='bold')
        ax2.set_title('Cinematography Emphasis by Genre', fontsize=12, fontweight='bold')
        ax2.invert_yaxis()
        ax2.grid(axis='x', alpha=0.3)

        # 3. Cinematography vs rating scatter
        ax3 = axes[1, 0]
        scatter = ax3.scatter(self.df['tech_cinematography'], self.df['IMDb Rating'],
                             alpha=0.5, s=30, c=self.df['year'],
                             cmap='viridis', edgecolors='black', linewidth=0.5)
        ax3.set_xlabel('Cinematography Score', fontsize=11, fontweight='bold')
        ax3.set_ylabel('IMDb Rating', fontsize=11, fontweight='bold')
        ax3.set_title('Cinematography vs Film Quality', fontsize=12, fontweight='bold')
        ax3.grid(True, alpha=0.3)

        # Colorbar
        cbar = plt.colorbar(scatter, ax=ax3)
        cbar.set_label('Year', fontsize=10, fontweight='bold')

        # Trend line
        if len(self.df[self.df['tech_cinematography'] > 0]) > 0:
            z = np.polyfit(self.df['tech_cinematography'], self.df['IMDb Rating'], 1)
            p = np.poly1d(z)
            x_line = np.linspace(0, self.df['tech_cinematography'].max(), 100)
            ax3.plot(x_line, p(x_line), "r--", alpha=0.8, linewidth=2, label='Trend')
            ax3.legend()

        # 4. Decade-by-decade cinematography
        ax4 = axes[1, 1]
        decade_cine = self.df.groupby('decade')['tech_cinematography'].mean().sort_index()

        bars = ax4.bar(decade_cine.index, decade_cine.values, width=8,
                      color='#9B59B6', edgecolor='black', alpha=0.7)
        ax4.set_xlabel('Decade', fontsize=11, fontweight='bold')
        ax4.set_ylabel('Average Cinematography Score', fontsize=11, fontweight='bold')
        ax4.set_title('Cinematography Evolution', fontsize=12, fontweight='bold')
        ax4.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        output_path = self.output_dir / 'cinematography_patterns.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_sound_vfx(self):
        """Visualization 7-8: Sound design and VFX analysis."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Sound Design & Visual Effects', fontsize=16, fontweight='bold')

        # 1. Sound vs VFX emphasis
        ax1 = axes[0, 0]
        scatter = ax1.scatter(self.df['tech_sound'], self.df['tech_vfx'],
                             alpha=0.5, s=40, c=self.df['IMDb Rating'],
                             cmap='RdYlGn', edgecolors='black', linewidth=0.5,
                             vmin=5, vmax=10)
        ax1.set_xlabel('Sound Design Score', fontsize=11, fontweight='bold')
        ax1.set_ylabel('VFX Score', fontsize=11, fontweight='bold')
        ax1.set_title('Sound vs VFX Emphasis', fontsize=12, fontweight='bold')
        ax1.grid(True, alpha=0.3)

        # Colorbar
        cbar = plt.colorbar(scatter, ax=ax1)
        cbar.set_label('IMDb Rating', fontsize=10, fontweight='bold')

        # 2. VFX evolution over time
        ax2 = axes[0, 1]
        decade_vfx = self.df.groupby('decade')['tech_vfx'].agg(['mean', 'max']).sort_index()

        ax2.plot(decade_vfx.index, decade_vfx['mean'], marker='o', linewidth=2,
                markersize=8, color='#3498DB', label='Average VFX')
        ax2.plot(decade_vfx.index, decade_vfx['max'], marker='s', linewidth=2,
                markersize=8, color='#E74C3C', label='Max VFX')
        ax2.fill_between(decade_vfx.index, decade_vfx['mean'], alpha=0.3, color='#3498DB')
        ax2.set_xlabel('Decade', fontsize=11, fontweight='bold')
        ax2.set_ylabel('VFX Score', fontsize=11, fontweight='bold')
        ax2.set_title('VFX Technology Evolution', fontsize=12, fontweight='bold')
        ax2.legend()
        ax2.grid(True, alpha=0.3)

        # 3. Top sound design films
        ax3 = axes[1, 0]
        top_sound = self.df.nlargest(10, 'tech_sound')[['title', 'tech_sound', 'IMDb Rating']]

        y_pos = np.arange(len(top_sound))
        bars = ax3.barh(y_pos, top_sound['tech_sound'].values,
                       color='#2ECC71', edgecolor='black')
        ax3.set_yticks(y_pos)
        ax3.set_yticklabels([str(t)[:30] for t in top_sound['title'].values], fontsize=8)
        ax3.set_xlabel('Sound Design Score', fontsize=11, fontweight='bold')
        ax3.set_title('Top Sound Design Films', fontsize=12, fontweight='bold')
        ax3.invert_yaxis()
        ax3.grid(axis='x', alpha=0.3)

        # Add ratings
        for i, (score, rating) in enumerate(zip(top_sound['tech_sound'].values,
                                                top_sound['IMDb Rating'].values)):
            ax3.text(score + 0.1, i, f'{rating:.1f}', va='center', fontsize=8)

        # 4. Top VFX films
        ax4 = axes[1, 1]
        top_vfx = self.df.nlargest(10, 'tech_vfx')[['title', 'tech_vfx', 'IMDb Rating']]

        y_pos = np.arange(len(top_vfx))
        bars = ax4.barh(y_pos, top_vfx['tech_vfx'].values,
                       color='#E67E22', edgecolor='black')
        ax4.set_yticks(y_pos)
        ax4.set_yticklabels([str(t)[:30] for t in top_vfx['title'].values], fontsize=8)
        ax4.set_xlabel('VFX Score', fontsize=11, fontweight='bold')
        ax4.set_title('Top VFX Films', fontsize=12, fontweight='bold')
        ax4.invert_yaxis()
        ax4.grid(axis='x', alpha=0.3)

        # Add ratings
        for i, (score, rating) in enumerate(zip(top_vfx['tech_vfx'].values,
                                                top_vfx['IMDb Rating'].values)):
            ax4.text(score + 0.1, i, f'{rating:.1f}', va='center', fontsize=8)

        plt.tight_layout()
        output_path = self.output_dir / 'sound_vfx.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def visualize_production_editing(self):
        """Visualization 9-10: Production design and editing."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Production Design & Editing', fontsize=16, fontweight='bold')

        # 1. Production vs Editing emphasis
        ax1 = axes[0, 0]
        scatter = ax1.scatter(self.df['tech_production'], self.df['tech_editing'],
                             alpha=0.5, s=40, c=self.df['decade'],
                             cmap='rainbow', edgecolors='black', linewidth=0.5)
        ax1.set_xlabel('Production Design Score', fontsize=11, fontweight='bold')
        ax1.set_ylabel('Editing Score', fontsize=11, fontweight='bold')
        ax1.set_title('Production vs Editing Emphasis', fontsize=12, fontweight='bold')
        ax1.grid(True, alpha=0.3)

        cbar = plt.colorbar(scatter, ax=ax1)
        cbar.set_label('Decade', fontsize=10, fontweight='bold')

        # 2. Top production design films
        ax2 = axes[0, 1]
        top_prod = self.df.nlargest(12, 'tech_production')[['title', 'tech_production', 'Genres']]

        y_pos = np.arange(len(top_prod))
        bars = ax2.barh(y_pos, top_prod['tech_production'].values,
                       color='#F39C12', edgecolor='black')
        ax2.set_yticks(y_pos)
        ax2.set_yticklabels([str(t)[:25] for t in top_prod['title'].values], fontsize=8)
        ax2.set_xlabel('Production Design Score', fontsize=11, fontweight='bold')
        ax2.set_title('Top Production Design Films', fontsize=12, fontweight='bold')
        ax2.invert_yaxis()
        ax2.grid(axis='x', alpha=0.3)

        # 3. Top editing films
        ax3 = axes[1, 0]
        top_edit = self.df.nlargest(12, 'tech_editing')[['title', 'tech_editing', 'IMDb Rating']]

        y_pos = np.arange(len(top_edit))
        bars = ax3.barh(y_pos, top_edit['tech_editing'].values,
                       color='#1ABC9C', edgecolor='black')
        ax3.set_yticks(y_pos)
        ax3.set_yticklabels([str(t)[:25] for t in top_edit['title'].values], fontsize=8)
        ax3.set_xlabel('Editing Score', fontsize=11, fontweight='bold')
        ax3.set_title('Top Editing Films', fontsize=12, fontweight='bold')
        ax3.invert_yaxis()
        ax3.grid(axis='x', alpha=0.3)

        # 4. Technical aspect combinations
        ax4 = axes[1, 1]
        ax4.axis('off')

        # Find films with high scores in multiple categories
        tech_cols = [f'tech_{cat.lower()}' for cat in self.technical_keywords.keys()]
        self.df['multi_tech'] = (self.df[tech_cols] > self.df[tech_cols].median()).sum(axis=1)

        top_multi = self.df.nlargest(15, 'multi_tech')

        multi_text = "TECHNICAL EXCELLENCE ACROSS CATEGORIES\n" + "="*50 + "\n\n"
        multi_text += "Films excelling in multiple technical areas:\n\n"

        for i, (idx, row) in enumerate(top_multi.iterrows(), 1):
            multi_text += f"{i:2d}. {str(row['title'])[:35]}\n"
            multi_text += f"    Areas: {row['multi_tech']:.0f}/5 | Rating: {row['IMDb Rating']:.1f}\n"
            multi_text += f"    Total Tech: {row['total_tech']:.1f}\n\n"

        ax4.text(0.1, 0.9, multi_text, transform=ax4.transAxes,
                fontsize=8, verticalalignment='top',
                family='monospace',
                bbox=dict(boxstyle='round', facecolor='#FFF9E6', alpha=0.7))
        ax4.set_title('Multi-Category Excellence', fontsize=12, fontweight='bold')

        plt.tight_layout()
        output_path = self.output_dir / 'production_editing.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")

    def generate_report(self):
        """Generate comprehensive technical aspects report."""
        print("\nGenerating technical aspects report...")

        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("CINESCOPE BATCH 36: TECHNICAL ASPECTS ANALYSIS")
        report_lines.append("=" * 80)
        report_lines.append("")
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("")

        # Overall stats
        report_lines.append("=" * 80)
        report_lines.append("TECHNICAL EMPHASIS OVERVIEW")
        report_lines.append("=" * 80)
        report_lines.append("")

        categories = list(self.technical_keywords.keys())
        tech_cols = [f'tech_{cat.lower()}' for cat in categories]

        for category, col in zip(categories, tech_cols):
            avg = self.df[col].mean()
            max_val = self.df[col].max()
            films_with = len(self.df[self.df[col] > 0])
            report_lines.append(f"{category:20s}: Avg {avg:.2f} | Max {max_val:.1f} | {films_with} films ({films_with/len(self.df)*100:.1f}%)")

        report_lines.append("")

        # Top films per category
        for category, col in zip(categories, tech_cols):
            report_lines.append("=" * 80)
            report_lines.append(f"TOP {category.upper()} FILMS")
            report_lines.append("=" * 80)
            report_lines.append("")

            top_films = self.df.nlargest(15, col)
            for i, (idx, row) in enumerate(top_films.iterrows(), 1):
                report_lines.append(f"{i:2d}. {row['title']:50s} - Score: {row[col]:.2f} (Rating: {row['IMDb Rating']:.1f})")

            report_lines.append("")

        # Technical excellence
        report_lines.append("=" * 80)
        report_lines.append("OVERALL TECHNICAL EXCELLENCE")
        report_lines.append("=" * 80)
        report_lines.append("")

        self.df['total_tech'] = self.df[tech_cols].sum(axis=1)
        top_overall = self.df.nlargest(20, 'total_tech')

        for i, (idx, row) in enumerate(top_overall.iterrows(), 1):
            report_lines.append(f"{i:2d}. {row['title']:50s} - Total: {row['total_tech']:.2f} (Rating: {row['IMDb Rating']:.1f})")

        report_lines.append("")
        report_lines.append("=" * 80)
        report_lines.append("END OF REPORT")
        report_lines.append("=" * 80)

        # Write report
        report_path = self.report_dir / 'batch_36_technical_aspects_report.txt'
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))

        print(f"Report saved: {report_path}")

    def run_all_analyses(self):
        """Execute all analyses and generate report."""
        print("\n" + "="*80)
        print("BATCH 36: TECHNICAL ASPECTS ANALYSIS")
        print("="*80)

        print("\n[1/5] Analyzing technical distribution...")
        self.visualize_technical_distribution()

        print("\n[2/5] Analyzing cinematography patterns...")
        self.visualize_cinematography_patterns()

        print("\n[3/5] Analyzing sound and VFX...")
        self.visualize_sound_vfx()

        print("\n[4/5] Analyzing production and editing...")
        self.visualize_production_editing()

        print("\n[5/5] Generating comprehensive report...")
        self.generate_report()

        print("\n" + "="*80)
        print("BATCH 36 COMPLETE")
        print("="*80)
        print(f"\nVisualizations saved to: {self.output_dir}")
        print(f"Report saved to: {self.report_dir}/batch_36_technical_aspects_report.txt")


def main():
    """Main execution function."""
    analyzer = TechnicalAspectsAnalyzer()
    analyzer.run_all_analyses()


if __name__ == '__main__':
    main()
