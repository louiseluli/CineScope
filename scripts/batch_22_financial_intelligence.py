"""
CineScope Batch 22: Financial Intelligence Analysis

COMPREHENSIVE BUDGET & REVENUE ANALYSIS
========================================
This batch analyzes:

1. Budget Distribution
   - Budget ranges across collection
   - Budget by decade/genre
   - Budget inflation adjustment

2. Revenue Analysis
   - Box office performance
   - Revenue by genre/decade
   - ROI calculations

3. Profitability
   - Most profitable films (ROI)
   - Biggest box office hits
   - Budget efficiency

4. Financial Trends
   - Budget evolution over time
   - Revenue trends by decade
   - Genre financial performance

5. Cost vs Quality
   - Does higher budget = higher rating?
   - Value for money analysis

Data Coverage: 99.3% (2,275/2,289 films with budget/revenue data)

Usage:
    python scripts/batch_22_financial_intelligence.py
"""
import sys
import json
import logging
import csv
from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
import numpy as np
import pandas as pd

from src.core.config import settings

settings.ensure_directories()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(settings.LOG_FILE)
    ]
)
logger = logging.getLogger(__name__)


class FinancialAnalyzer:
    """
    Analyzes financial data (budgets, revenue, ROI) across cinema collection.
    """

    def __init__(self):
        self.master_csv = settings.PROCESSED_DATA_DIR / "watched_movies_master.csv"
        self.output_dir = settings.VISUALIZATIONS_DIR / "batch_22"
        self.report_dir = settings.BASE_DIR / "analysis_outputs" / "reports"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)

        # Load data
        self.df = self._load_data()

        # Stats tracking
        self.stats = {}

        # Set matplotlib style
        plt.style.use('seaborn-v0_8-darkgrid')
        plt.rcParams['figure.facecolor'] = '#f8f9fa'
        plt.rcParams['axes.facecolor'] = '#ffffff'
        plt.rcParams['font.family'] = 'sans-serif'

    def _load_data(self) -> pd.DataFrame:
        """Load and prepare data."""
        logger.info("Loading data...")

        df = pd.read_csv(self.master_csv)
        logger.info(f"✓ Loaded {len(df):,} films")

        # Normalize column names
        df['budget'] = df['tmdb_budget'].fillna(0)
        df['revenue'] = df['tmdb_revenue'].fillna(0)
        df['title'] = df.get('title', df.get('Title', ''))
        df['year'] = df.get('release_year', df.get('Year', df.get('year', 2000)))
        df['rating'] = df.get('imdb_rating', df.get('IMDb Rating', 0))
        df['genres'] = df.get('genres', df.get('Genres', ''))
        df['directors'] = df.get('directors', df.get('Directors', ''))

        # Convert to numeric
        df['budget'] = pd.to_numeric(df['budget'], errors='coerce').fillna(0)
        df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce').fillna(0)
        df['year'] = pd.to_numeric(df['year'], errors='coerce').fillna(2000).astype(int)
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce').fillna(0)

        # Calculate decade
        df['decade'] = (df['year'] // 10) * 10

        # Filter to only films with financial data
        df_with_data = df[(df['budget'] > 0) | (df['revenue'] > 0)]

        logger.info(f"Films with budget data: {len(df[df['budget'] > 0]):,} ({len(df[df['budget'] > 0])/len(df)*100:.1f}%)")
        logger.info(f"Films with revenue data: {len(df[df['revenue'] > 0]):,} ({len(df[df['revenue'] > 0])/len(df)*100:.1f}%)")

        return df

    def analyze(self):
        """Run full financial analysis."""
        logger.info("=" * 80)
        logger.info("CINESCOPE BATCH 22: FINANCIAL INTELLIGENCE ANALYSIS")
        logger.info("=" * 80)
        logger.info("")

        logger.info("=" * 80)
        logger.info("GENERATING VISUALIZATIONS")
        logger.info("=" * 80)
        logger.info("")

        # Generate all visualizations
        self._viz_01_budget_distribution()
        self._viz_02_revenue_distribution()
        self._viz_03_roi_leaderboard()
        self._viz_04_budget_evolution()
        self._viz_05_revenue_evolution()
        self._viz_06_genre_financials()
        self._viz_07_budget_vs_rating()
        self._viz_08_roi_vs_budget()
        self._viz_09_decade_comparison()
        self._viz_10_coverage_overview()

        # Generate report
        self._generate_report()

        logger.info("")
        logger.info("=" * 80)
        logger.info("BATCH 22 COMPLETE!")
        logger.info("=" * 80)
        logger.info("")
        logger.info(f"✓ Generated 10 visualizations in: {self.output_dir}")
        logger.info(f"✓ Generated report in: {self.report_dir / 'batch_22_financial_intelligence_report.txt'}")
        logger.info("")
        logger.info("Key Findings:")
        logger.info(f"  • {self.stats.get('films_with_budget', 0):,} films with budget data")
        logger.info(f"  • {self.stats.get('films_with_revenue', 0):,} films with revenue data")
        logger.info(f"  • Total box office: ${self.stats.get('total_revenue', 0)/1e9:.2f}B")
        logger.info(f"  • Average budget: ${self.stats.get('avg_budget', 0)/1e6:.1f}M")
        logger.info("")
        logger.info("Check the visualizations folder for all generated images!")
        logger.info("=" * 80)

    def _viz_01_budget_distribution(self):
        """Visualize budget distribution."""
        logger.info("Creating Visualization 1: Budget Distribution")

        df_budget = self.df[self.df['budget'] > 0].copy()

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))

        # 1. Overall distribution (histogram)
        ax = axes[0, 0]
        budgets_millions = df_budget['budget'] / 1e6
        ax.hist(budgets_millions, bins=50, color='#3498DB', edgecolor='white', alpha=0.8)
        ax.set_xlabel('Budget (Millions USD)', fontsize=11)
        ax.set_ylabel('Number of Films', fontsize=11)
        ax.set_title('Budget Distribution', fontsize=12, fontweight='bold')
        ax.axvline(budgets_millions.median(), color='red', linestyle='--', linewidth=2, label=f'Median: ${budgets_millions.median():.1f}M')
        ax.legend()

        # 2. Budget ranges (pie chart)
        ax = axes[0, 1]
        budget_ranges = {
            'Low (<$10M)': len(df_budget[df_budget['budget'] < 10e6]),
            'Medium ($10-50M)': len(df_budget[(df_budget['budget'] >= 10e6) & (df_budget['budget'] < 50e6)]),
            'High ($50-100M)': len(df_budget[(df_budget['budget'] >= 50e6) & (df_budget['budget'] < 100e6)]),
            'Blockbuster (>$100M)': len(df_budget[df_budget['budget'] >= 100e6])
        }
        colors = ['#2ECC71', '#3498DB', '#F39C12', '#E74C3C']
        ax.pie(budget_ranges.values(), labels=budget_ranges.keys(), autopct='%1.1f%%',
               colors=colors, startangle=90)
        ax.set_title('Budget Ranges', fontsize=12, fontweight='bold')

        # 3. Top 15 most expensive films
        ax = axes[1, 0]
        top_budget = df_budget.nlargest(15, 'budget')
        y_pos = np.arange(len(top_budget))
        budgets = top_budget['budget'] / 1e6
        colors_grad = plt.cm.Reds(np.linspace(0.4, 0.9, len(top_budget)))
        ax.barh(y_pos, budgets, color=colors_grad, edgecolor='white')
        ax.set_yticks(y_pos)
        ax.set_yticklabels([f"{row['title'][:30]}... ({row['year']})" if len(row['title']) > 30
                            else f"{row['title']} ({row['year']})"
                            for _, row in top_budget.iterrows()], fontsize=9)
        ax.invert_yaxis()
        ax.set_xlabel('Budget (Millions USD)', fontsize=11)
        ax.set_title('Top 15 Most Expensive Films', fontsize=12, fontweight='bold')

        # 4. Budget statistics
        ax = axes[1, 1]
        ax.axis('off')
        stats_text = f"""
BUDGET STATISTICS

Total Films with Budget Data: {len(df_budget):,}
Coverage: {len(df_budget)/len(self.df)*100:.1f}%

Total Budget: ${df_budget['budget'].sum()/1e9:.2f}B
Average Budget: ${df_budget['budget'].mean()/1e6:.1f}M
Median Budget: ${df_budget['budget'].median()/1e6:.1f}M

Minimum: ${df_budget['budget'].min()/1e6:.1f}M
Maximum: ${df_budget['budget'].max()/1e6:.1f}M

Budget Ranges:
  Low (<$10M): {budget_ranges['Low (<$10M)']} films
  Medium ($10-50M): {budget_ranges['Medium ($10-50M)']} films
  High ($50-100M): {budget_ranges['High ($50-100M)']} films
  Blockbuster (>$100M): {budget_ranges['Blockbuster (>$100M)']} films
"""
        ax.text(0.1, 0.9, stats_text, fontsize=11, verticalalignment='top',
                fontfamily='monospace', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

        plt.suptitle('Budget Analysis', fontsize=18, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.output_dir / '01_budget_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 01_budget_distribution.png")

        # Store stats
        self.stats['films_with_budget'] = len(df_budget)
        self.stats['avg_budget'] = df_budget['budget'].mean()
        self.stats['median_budget'] = df_budget['budget'].median()

    def _viz_02_revenue_distribution(self):
        """Visualize revenue distribution."""
        logger.info("Creating Visualization 2: Revenue Distribution")

        df_revenue = self.df[self.df['revenue'] > 0].copy()

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))

        # 1. Overall distribution
        ax = axes[0, 0]
        revenue_millions = df_revenue['revenue'] / 1e6
        ax.hist(revenue_millions, bins=50, color='#2ECC71', edgecolor='white', alpha=0.8)
        ax.set_xlabel('Revenue (Millions USD)', fontsize=11)
        ax.set_ylabel('Number of Films', fontsize=11)
        ax.set_title('Revenue Distribution', fontsize=12, fontweight='bold')
        ax.axvline(revenue_millions.median(), color='red', linestyle='--', linewidth=2,
                   label=f'Median: ${revenue_millions.median():.1f}M')
        ax.legend()

        # 2. Revenue ranges
        ax = axes[0, 1]
        revenue_ranges = {
            'Flop (<$10M)': len(df_revenue[df_revenue['revenue'] < 10e6]),
            'Modest ($10-100M)': len(df_revenue[(df_revenue['revenue'] >= 10e6) & (df_revenue['revenue'] < 100e6)]),
            'Hit ($100-500M)': len(df_revenue[(df_revenue['revenue'] >= 100e6) & (df_revenue['revenue'] < 500e6)]),
            'Mega-Hit (>$500M)': len(df_revenue[df_revenue['revenue'] >= 500e6])
        }
        colors = ['#E74C3C', '#F39C12', '#3498DB', '#2ECC71']
        ax.pie(revenue_ranges.values(), labels=revenue_ranges.keys(), autopct='%1.1f%%',
               colors=colors, startangle=90)
        ax.set_title('Revenue Ranges', fontsize=12, fontweight='bold')

        # 3. Top 15 highest grossing
        ax = axes[1, 0]
        top_revenue = df_revenue.nlargest(15, 'revenue')
        y_pos = np.arange(len(top_revenue))
        revenues = top_revenue['revenue'] / 1e6
        colors_grad = plt.cm.Greens(np.linspace(0.4, 0.9, len(top_revenue)))
        ax.barh(y_pos, revenues, color=colors_grad, edgecolor='white')
        ax.set_yticks(y_pos)
        ax.set_yticklabels([f"{row['title'][:30]}... ({row['year']})" if len(row['title']) > 30
                            else f"{row['title']} ({row['year']})"
                            for _, row in top_revenue.iterrows()], fontsize=9)
        ax.invert_yaxis()
        ax.set_xlabel('Revenue (Millions USD)', fontsize=11)
        ax.set_title('Top 15 Highest Grossing Films', fontsize=12, fontweight='bold')

        # 4. Statistics
        ax = axes[1, 1]
        ax.axis('off')
        stats_text = f"""
REVENUE STATISTICS

Total Films with Revenue Data: {len(df_revenue):,}
Coverage: {len(df_revenue)/len(self.df)*100:.1f}%

Total Revenue: ${df_revenue['revenue'].sum()/1e9:.2f}B
Average Revenue: ${df_revenue['revenue'].mean()/1e6:.1f}M
Median Revenue: ${df_revenue['revenue'].median()/1e6:.1f}M

Minimum: ${df_revenue['revenue'].min()/1e6:.1f}M
Maximum: ${df_revenue['revenue'].max()/1e6:.1f}M

Revenue Ranges:
  Flop (<$10M): {revenue_ranges['Flop (<$10M)']} films
  Modest ($10-100M): {revenue_ranges['Modest ($10-100M)']} films
  Hit ($100-500M): {revenue_ranges['Hit ($100-500M)']} films
  Mega-Hit (>$500M): {revenue_ranges['Mega-Hit (>$500M)']} films
"""
        ax.text(0.1, 0.9, stats_text, fontsize=11, verticalalignment='top',
                fontfamily='monospace', bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.3))

        plt.suptitle('Revenue Analysis', fontsize=18, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.output_dir / '02_revenue_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 02_revenue_distribution.png")

        self.stats['films_with_revenue'] = len(df_revenue)
        self.stats['total_revenue'] = df_revenue['revenue'].sum()

    def _viz_03_roi_leaderboard(self):
        """Visualize ROI (Return on Investment) leaderboard."""
        logger.info("Creating Visualization 3: ROI Leaderboard")

        # Filter films with both budget and revenue
        df_complete = self.df[(self.df['budget'] > 0) & (self.df['revenue'] > 0)].copy()

        # Calculate ROI
        df_complete['roi'] = ((df_complete['revenue'] - df_complete['budget']) / df_complete['budget']) * 100
        df_complete['profit'] = df_complete['revenue'] - df_complete['budget']

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))

        # 1. Top 20 ROI
        ax = axes[0, 0]
        top_roi = df_complete.nlargest(20, 'roi')
        y_pos = np.arange(len(top_roi))
        colors_grad = plt.cm.YlGn(np.linspace(0.4, 0.9, len(top_roi)))
        ax.barh(y_pos, top_roi['roi'], color=colors_grad, edgecolor='white')
        ax.set_yticks(y_pos)
        ax.set_yticklabels([f"{row['title'][:25]}..." if len(row['title']) > 25
                            else row['title']
                            for _, row in top_roi.iterrows()], fontsize=8)
        ax.invert_yaxis()
        ax.set_xlabel('ROI (%)', fontsize=11)
        ax.set_title('Top 20 Films by ROI', fontsize=12, fontweight='bold')

        # 2. Top 20 by absolute profit
        ax = axes[0, 1]
        top_profit = df_complete.nlargest(20, 'profit')
        y_pos = np.arange(len(top_profit))
        profits = top_profit['profit'] / 1e6
        colors_grad = plt.cm.Blues(np.linspace(0.4, 0.9, len(top_profit)))
        ax.barh(y_pos, profits, color=colors_grad, edgecolor='white')
        ax.set_yticks(y_pos)
        ax.set_yticklabels([f"{row['title'][:25]}..." if len(row['title']) > 25
                            else row['title']
                            for _, row in top_profit.iterrows()], fontsize=8)
        ax.invert_yaxis()
        ax.set_xlabel('Profit (Millions USD)', fontsize=11)
        ax.set_title('Top 20 Films by Absolute Profit', fontsize=12, fontweight='bold')

        # 3. ROI distribution
        ax = axes[1, 0]
        ax.hist(df_complete['roi'], bins=50, color='#9B59B6', edgecolor='white', alpha=0.8)
        ax.set_xlabel('ROI (%)', fontsize=11)
        ax.set_ylabel('Number of Films', fontsize=11)
        ax.set_title('ROI Distribution', fontsize=12, fontweight='bold')
        ax.axvline(df_complete['roi'].median(), color='red', linestyle='--', linewidth=2,
                   label=f'Median: {df_complete["roi"].median():.0f}%')
        ax.axvline(0, color='black', linestyle='-', linewidth=1, alpha=0.3)
        ax.legend()

        # 4. Statistics
        ax = axes[1, 1]
        ax.axis('off')
        profitable = len(df_complete[df_complete['profit'] > 0])
        unprofitable = len(df_complete[df_complete['profit'] <= 0])
        stats_text = f"""
ROI & PROFITABILITY STATISTICS

Films with Complete Data: {len(df_complete):,}

Profitability:
  Profitable Films: {profitable} ({profitable/len(df_complete)*100:.1f}%)
  Unprofitable Films: {unprofitable} ({unprofitable/len(df_complete)*100:.1f}%)

ROI Statistics:
  Average ROI: {df_complete['roi'].mean():.1f}%
  Median ROI: {df_complete['roi'].median():.1f}%
  Best ROI: {df_complete['roi'].max():.1f}%
  Worst ROI: {df_complete['roi'].min():.1f}%

Profit Statistics:
  Total Profit: ${df_complete['profit'].sum()/1e9:.2f}B
  Average Profit: ${df_complete['profit'].mean()/1e6:.1f}M
  Biggest Winner: ${df_complete['profit'].max()/1e6:.1f}M
  Biggest Loser: ${df_complete['profit'].min()/1e6:.1f}M
"""
        ax.text(0.1, 0.9, stats_text, fontsize=11, verticalalignment='top',
                fontfamily='monospace', bbox=dict(boxstyle='round', facecolor='lavender', alpha=0.3))

        plt.suptitle('Return on Investment (ROI) Analysis', fontsize=18, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.output_dir / '03_roi_leaderboard.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 03_roi_leaderboard.png")

        self.stats['profitable_films'] = profitable
        self.stats['avg_roi'] = df_complete['roi'].mean()

    def _viz_04_budget_evolution(self):
        """Visualize budget evolution over decades."""
        logger.info("Creating Visualization 4: Budget Evolution")

        df_budget = self.df[self.df['budget'] > 0].copy()

        # Group by decade
        decade_stats = df_budget.groupby('decade').agg({
            'budget': ['mean', 'median', 'count']
        }).reset_index()
        decade_stats.columns = ['decade', 'mean_budget', 'median_budget', 'count']
        decade_stats = decade_stats[decade_stats['decade'] >= 1920]

        fig, axes = plt.subplots(2, 1, figsize=(14, 10))

        # 1. Average budget by decade
        ax = axes[0]
        ax.plot(decade_stats['decade'], decade_stats['mean_budget']/1e6,
                marker='o', linewidth=3, markersize=10, color='#3498DB', label='Mean Budget')
        ax.plot(decade_stats['decade'], decade_stats['median_budget']/1e6,
                marker='s', linewidth=3, markersize=8, color='#E74C3C', label='Median Budget')
        ax.set_xlabel('Decade', fontsize=12)
        ax.set_ylabel('Budget (Millions USD)', fontsize=12)
        ax.set_title('Budget Evolution Over Time', fontsize=14, fontweight='bold')
        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3)

        # 2. Number of films by decade
        ax = axes[1]
        ax.bar(decade_stats['decade'], decade_stats['count'],
               color='#2ECC71', edgecolor='white', width=8)
        ax.set_xlabel('Decade', fontsize=12)
        ax.set_ylabel('Number of Films', fontsize=12)
        ax.set_title('Films with Budget Data by Decade', fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3, axis='y')

        plt.tight_layout()
        plt.savefig(self.output_dir / '04_budget_evolution.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 04_budget_evolution.png")

    def _viz_05_revenue_evolution(self):
        """Visualize revenue evolution over decades."""
        logger.info("Creating Visualization 5: Revenue Evolution")

        df_revenue = self.df[self.df['revenue'] > 0].copy()

        # Group by decade
        decade_stats = df_revenue.groupby('decade').agg({
            'revenue': ['mean', 'median', 'sum', 'count']
        }).reset_index()
        decade_stats.columns = ['decade', 'mean_revenue', 'median_revenue', 'total_revenue', 'count']
        decade_stats = decade_stats[decade_stats['decade'] >= 1920]

        fig, axes = plt.subplots(2, 1, figsize=(14, 10))

        # 1. Average revenue by decade
        ax = axes[0]
        ax.plot(decade_stats['decade'], decade_stats['mean_revenue']/1e6,
                marker='o', linewidth=3, markersize=10, color='#2ECC71', label='Mean Revenue')
        ax.plot(decade_stats['decade'], decade_stats['median_revenue']/1e6,
                marker='s', linewidth=3, markersize=8, color='#F39C12', label='Median Revenue')
        ax.set_xlabel('Decade', fontsize=12)
        ax.set_ylabel('Revenue (Millions USD)', fontsize=12)
        ax.set_title('Revenue Evolution Over Time', fontsize=14, fontweight='bold')
        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3)

        # 2. Total revenue by decade
        ax = axes[1]
        ax.bar(decade_stats['decade'], decade_stats['total_revenue']/1e9,
               color='#9B59B6', edgecolor='white', width=8)
        ax.set_xlabel('Decade', fontsize=12)
        ax.set_ylabel('Total Revenue (Billions USD)', fontsize=12)
        ax.set_title('Total Box Office by Decade', fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3, axis='y')

        plt.tight_layout()
        plt.savefig(self.output_dir / '05_revenue_evolution.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 05_revenue_evolution.png")

    def _viz_06_genre_financials(self):
        """Visualize financial performance by genre."""
        logger.info("Creating Visualization 6: Genre Financial Performance")

        df_complete = self.df[(self.df['budget'] > 0) & (self.df['revenue'] > 0)].copy()
        df_complete['roi'] = ((df_complete['revenue'] - df_complete['budget']) / df_complete['budget']) * 100

        # Parse genres (handle pipe-separated or comma-separated)
        genre_data = []
        for _, row in df_complete.iterrows():
            genres_str = row['genres']
            if not genres_str or pd.isna(genres_str):
                continue

            if '|' in str(genres_str):
                genres = [g.strip() for g in str(genres_str).split('|') if g.strip()]
            elif ',' in str(genres_str):
                # Remove brackets, quotes, and split
                genres_str = str(genres_str).strip("[]'\"")
                genres = [g.strip().strip("'\"").strip("[]") for g in genres_str.split(',') if g.strip()]
            else:
                genres = [str(genres_str).strip().strip("[]'\"")]

            # Clean up any remaining brackets/quotes
            genres = [g.strip().strip("[]'\"") for g in genres if g and g.strip().strip("[]'\"")]

            for genre in genres:
                genre_data.append({
                    'genre': genre,
                    'budget': row['budget'],
                    'revenue': row['revenue'],
                    'roi': row['roi']
                })

        df_genres = pd.DataFrame(genre_data)

        # Aggregate by genre
        genre_stats = df_genres.groupby('genre').agg({
            'budget': 'mean',
            'revenue': 'mean',
            'roi': 'mean'
        }).reset_index()

        # Sort by ROI
        genre_stats = genre_stats.sort_values('roi', ascending=False).head(15)

        fig, axes = plt.subplots(1, 3, figsize=(18, 6))

        # 1. Average budget by genre
        ax = axes[0]
        y_pos = np.arange(len(genre_stats))
        ax.barh(y_pos, genre_stats['budget']/1e6, color='#3498DB', edgecolor='white')
        ax.set_yticks(y_pos)
        ax.set_yticklabels(genre_stats['genre'], fontsize=10)
        ax.invert_yaxis()
        ax.set_xlabel('Avg Budget (Millions USD)', fontsize=11)
        ax.set_title('Average Budget by Genre', fontsize=12, fontweight='bold')

        # 2. Average revenue by genre
        ax = axes[1]
        ax.barh(y_pos, genre_stats['revenue']/1e6, color='#2ECC71', edgecolor='white')
        ax.set_yticks(y_pos)
        ax.set_yticklabels(genre_stats['genre'], fontsize=10)
        ax.invert_yaxis()
        ax.set_xlabel('Avg Revenue (Millions USD)', fontsize=11)
        ax.set_title('Average Revenue by Genre', fontsize=12, fontweight='bold')

        # 3. Average ROI by genre
        ax = axes[2]
        ax.barh(y_pos, genre_stats['roi'], color='#9B59B6', edgecolor='white')
        ax.set_yticks(y_pos)
        ax.set_yticklabels(genre_stats['genre'], fontsize=10)
        ax.invert_yaxis()
        ax.set_xlabel('Avg ROI (%)', fontsize=11)
        ax.set_title('Average ROI by Genre', fontsize=12, fontweight='bold')

        plt.suptitle('Financial Performance by Genre (Top 15)', fontsize=16, fontweight='bold', y=1.02)
        plt.tight_layout()
        plt.savefig(self.output_dir / '06_genre_financials.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 06_genre_financials.png")

    def _viz_07_budget_vs_rating(self):
        """Analyze budget vs rating correlation."""
        logger.info("Creating Visualization 7: Budget vs Rating")

        df_analysis = self.df[(self.df['budget'] > 0) & (self.df['rating'] > 0)].copy()

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))

        # 1. Scatter plot
        ax = axes[0, 0]
        ax.scatter(df_analysis['budget']/1e6, df_analysis['rating'],
                  alpha=0.4, s=50, c='#3498DB', edgecolors='white')

        # Add trend line
        z = np.polyfit(df_analysis['budget']/1e6, df_analysis['rating'], 1)
        p = np.poly1d(z)
        x_trend = np.linspace(df_analysis['budget'].min()/1e6, df_analysis['budget'].max()/1e6, 100)
        ax.plot(x_trend, p(x_trend), "r--", linewidth=2, label=f'Trend: y={z[0]:.4f}x+{z[1]:.2f}')

        ax.set_xlabel('Budget (Millions USD)', fontsize=11)
        ax.set_ylabel('IMDb Rating', fontsize=11)
        ax.set_title('Budget vs Rating Correlation', fontsize=12, fontweight='bold')
        ax.legend()
        ax.grid(True, alpha=0.3)

        # Calculate correlation
        corr = df_analysis['budget'].corr(df_analysis['rating'])
        ax.text(0.05, 0.95, f'Correlation: {corr:.3f}', transform=ax.transAxes,
                fontsize=11, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        # 2. Rating by budget range
        ax = axes[0, 1]
        budget_ranges = ['<$10M', '$10-50M', '$50-100M', '>$100M']
        avg_ratings = [
            df_analysis[df_analysis['budget'] < 10e6]['rating'].mean(),
            df_analysis[(df_analysis['budget'] >= 10e6) & (df_analysis['budget'] < 50e6)]['rating'].mean(),
            df_analysis[(df_analysis['budget'] >= 50e6) & (df_analysis['budget'] < 100e6)]['rating'].mean(),
            df_analysis[df_analysis['budget'] >= 100e6]['rating'].mean()
        ]
        colors = ['#2ECC71', '#3498DB', '#F39C12', '#E74C3C']
        ax.bar(budget_ranges, avg_ratings, color=colors, edgecolor='white')
        ax.set_ylabel('Average IMDb Rating', fontsize=11)
        ax.set_title('Average Rating by Budget Range', fontsize=12, fontweight='bold')
        ax.axhline(df_analysis['rating'].mean(), color='red', linestyle='--', linewidth=2,
                   label=f'Overall Avg: {df_analysis["rating"].mean():.2f}')
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')

        # 3. Best value films (high rating, low budget)
        ax = axes[1, 0]
        # Define "value" as rating / (budget in millions)
        df_analysis['value_score'] = df_analysis['rating'] / (df_analysis['budget'] / 1e6)
        best_value = df_analysis.nlargest(15, 'value_score')
        y_pos = np.arange(len(best_value))
        colors_grad = plt.cm.Greens(np.linspace(0.4, 0.9, len(best_value)))
        ax.barh(y_pos, best_value['value_score'], color=colors_grad, edgecolor='white')
        ax.set_yticks(y_pos)
        ax.set_yticklabels([f"{row['title'][:25]}..." if len(row['title']) > 25
                            else row['title']
                            for _, row in best_value.iterrows()], fontsize=8)
        ax.invert_yaxis()
        ax.set_xlabel('Value Score (Rating/Budget)', fontsize=11)
        ax.set_title('Best Value Films (High Quality, Low Cost)', fontsize=12, fontweight='bold')

        # 4. Statistics
        ax = axes[1, 1]
        ax.axis('off')
        stats_text = f"""
BUDGET vs RATING ANALYSIS

Total Films Analyzed: {len(df_analysis):,}

Correlation: {corr:.3f}
  {'Strong positive' if corr > 0.7 else 'Moderate positive' if corr > 0.3 else 'Weak positive' if corr > 0 else 'Negative'}

Average Ratings by Budget:
  Low Budget (<$10M): {avg_ratings[0]:.2f}
  Medium ($10-50M): {avg_ratings[1]:.2f}
  High ($50-100M): {avg_ratings[2]:.2f}
  Blockbuster (>$100M): {avg_ratings[3]:.2f}

Overall Average Rating: {df_analysis['rating'].mean():.2f}

Insight:
  {"Higher budgets correlate with higher ratings" if corr > 0.3
   else "Budget has minimal impact on quality"}
"""
        ax.text(0.1, 0.9, stats_text, fontsize=11, verticalalignment='top',
                fontfamily='monospace', bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.3))

        plt.suptitle('Does Money Buy Quality?', fontsize=18, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.output_dir / '07_budget_vs_rating.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 07_budget_vs_rating.png")

    def _viz_08_roi_vs_budget(self):
        """Analyze ROI vs budget relationship."""
        logger.info("Creating Visualization 8: ROI vs Budget")

        df_analysis = self.df[(self.df['budget'] > 0) & (self.df['revenue'] > 0)].copy()
        df_analysis['roi'] = ((df_analysis['revenue'] - df_analysis['budget']) / df_analysis['budget']) * 100

        fig, axes = plt.subplots(1, 2, figsize=(16, 6))

        # 1. Scatter plot
        ax = axes[0]
        ax.scatter(df_analysis['budget']/1e6, df_analysis['roi'],
                  alpha=0.5, s=60, c='#9B59B6', edgecolors='white')
        ax.set_xlabel('Budget (Millions USD)', fontsize=12)
        ax.set_ylabel('ROI (%)', fontsize=12)
        ax.set_title('ROI vs Budget', fontsize=14, fontweight='bold')
        ax.axhline(0, color='red', linestyle='--', linewidth=2, alpha=0.5, label='Break-even')
        ax.legend()
        ax.grid(True, alpha=0.3)

        # Calculate correlation
        corr = df_analysis['budget'].corr(df_analysis['roi'])
        ax.text(0.05, 0.95, f'Correlation: {corr:.3f}', transform=ax.transAxes,
                fontsize=12, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='lavender', alpha=0.7))

        # 2. Average ROI by budget range
        ax = axes[1]
        budget_ranges = ['<$10M', '$10-50M', '$50-100M', '>$100M']
        avg_roi = [
            df_analysis[df_analysis['budget'] < 10e6]['roi'].mean(),
            df_analysis[(df_analysis['budget'] >= 10e6) & (df_analysis['budget'] < 50e6)]['roi'].mean(),
            df_analysis[(df_analysis['budget'] >= 50e6) & (df_analysis['budget'] < 100e6)]['roi'].mean(),
            df_analysis[df_analysis['budget'] >= 100e6]['roi'].mean()
        ]
        colors = ['#2ECC71', '#3498DB', '#F39C12', '#E74C3C']
        bars = ax.bar(budget_ranges, avg_roi, color=colors, edgecolor='white')
        ax.set_ylabel('Average ROI (%)', fontsize=12)
        ax.set_title('Average ROI by Budget Range', fontsize=14, fontweight='bold')
        ax.axhline(0, color='black', linestyle='-', linewidth=1, alpha=0.3)
        ax.grid(True, alpha=0.3, axis='y')

        # Add value labels
        for bar, val in zip(bars, avg_roi):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{val:.0f}%', ha='center', va='bottom' if val > 0 else 'top', fontsize=11, fontweight='bold')

        plt.suptitle('ROI vs Budget Analysis', fontsize=18, fontweight='bold')
        plt.tight_layout()
        plt.savefig(self.output_dir / '08_roi_vs_budget.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 08_roi_vs_budget.png")

    def _viz_09_decade_comparison(self):
        """Compare financial metrics across decades."""
        logger.info("Creating Visualization 9: Decade Comparison")

        df_complete = self.df[(self.df['budget'] > 0) & (self.df['revenue'] > 0)].copy()
        df_complete['roi'] = ((df_complete['revenue'] - df_complete['budget']) / df_complete['budget']) * 100

        # Group by decade
        decade_stats = df_complete.groupby('decade').agg({
            'budget': 'mean',
            'revenue': 'mean',
            'roi': 'mean'
        }).reset_index()
        decade_stats = decade_stats[decade_stats['decade'] >= 1920]

        fig, axes = plt.subplots(3, 1, figsize=(14, 14))

        # 1. Average budget by decade
        ax = axes[0]
        ax.bar(decade_stats['decade'], decade_stats['budget']/1e6,
               color='#3498DB', edgecolor='white', width=8)
        ax.set_ylabel('Avg Budget (Millions USD)', fontsize=12)
        ax.set_title('Average Budget by Decade', fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3, axis='y')

        # 2. Average revenue by decade
        ax = axes[1]
        ax.bar(decade_stats['decade'], decade_stats['revenue']/1e6,
               color='#2ECC71', edgecolor='white', width=8)
        ax.set_ylabel('Avg Revenue (Millions USD)', fontsize=12)
        ax.set_title('Average Revenue by Decade', fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3, axis='y')

        # 3. Average ROI by decade
        ax = axes[2]
        ax.bar(decade_stats['decade'], decade_stats['roi'],
               color='#9B59B6', edgecolor='white', width=8)
        ax.set_xlabel('Decade', fontsize=12)
        ax.set_ylabel('Avg ROI (%)', fontsize=12)
        ax.set_title('Average ROI by Decade', fontsize=14, fontweight='bold')
        ax.axhline(0, color='red', linestyle='--', linewidth=2, alpha=0.5)
        ax.grid(True, alpha=0.3, axis='y')

        plt.suptitle('Financial Metrics by Decade', fontsize=18, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.output_dir / '09_decade_comparison.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 09_decade_comparison.png")

    def _viz_10_coverage_overview(self):
        """Create coverage overview dashboard."""
        logger.info("Creating Visualization 10: Coverage Overview")

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))

        # 1. Data coverage pie chart
        ax = axes[0, 0]
        coverage_data = {
            'Both Budget & Revenue': len(self.df[(self.df['budget'] > 0) & (self.df['revenue'] > 0)]),
            'Budget Only': len(self.df[(self.df['budget'] > 0) & (self.df['revenue'] == 0)]),
            'Revenue Only': len(self.df[(self.df['budget'] == 0) & (self.df['revenue'] > 0)]),
            'No Financial Data': len(self.df[(self.df['budget'] == 0) & (self.df['revenue'] == 0)])
        }
        colors = ['#2ECC71', '#3498DB', '#F39C12', '#E74C3C']
        ax.pie(coverage_data.values(), labels=coverage_data.keys(), autopct='%1.1f%%',
               colors=colors, startangle=90)
        ax.set_title('Financial Data Coverage', fontsize=12, fontweight='bold')

        # 2. Coverage by decade
        ax = axes[0, 1]
        decades = sorted([d for d in self.df['decade'].unique() if d >= 1920 and d <= 2020])
        coverage_by_decade = []
        for decade in decades:
            df_decade = self.df[self.df['decade'] == decade]
            pct = len(df_decade[(df_decade['budget'] > 0) | (df_decade['revenue'] > 0)]) / len(df_decade) * 100
            coverage_by_decade.append(pct)

        ax.bar(decades, coverage_by_decade, color='#9B59B6', edgecolor='white', width=8)
        ax.set_xlabel('Decade', fontsize=11)
        ax.set_ylabel('Coverage (%)', fontsize=11)
        ax.set_title('Financial Data Coverage by Decade', fontsize=12, fontweight='bold')
        ax.axhline(100, color='green', linestyle='--', linewidth=1, alpha=0.5)
        ax.grid(True, alpha=0.3, axis='y')

        # 3. Key statistics
        ax = axes[1, 0]
        ax.axis('off')

        total_films = len(self.df)
        with_budget = len(self.df[self.df['budget'] > 0])
        with_revenue = len(self.df[self.df['revenue'] > 0])
        complete = len(self.df[(self.df['budget'] > 0) & (self.df['revenue'] > 0)])

        stats_text = f"""
COVERAGE STATISTICS

Total Films: {total_films:,}

Budget Data:
  Films with Budget: {with_budget:,}
  Coverage: {with_budget/total_films*100:.1f}%

Revenue Data:
  Films with Revenue: {with_revenue:,}
  Coverage: {with_revenue/total_films*100:.1f}%

Complete Data (Both):
  Films: {complete:,}
  Coverage: {complete/total_films*100:.1f}%

Missing Both:
  Films: {coverage_data['No Financial Data']:,}
  Percentage: {coverage_data['No Financial Data']/total_films*100:.1f}%
"""
        ax.text(0.1, 0.9, stats_text, fontsize=12, verticalalignment='top',
                fontfamily='monospace', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

        # 4. Top 5 most profitable directors
        ax = axes[1, 1]
        df_directors = self.df[(self.df['budget'] > 0) & (self.df['revenue'] > 0)].copy()
        df_directors['profit'] = df_directors['revenue'] - df_directors['budget']

        director_profits = defaultdict(float)
        for _, row in df_directors.iterrows():
            directors_str = row['directors']
            if not directors_str or pd.isna(directors_str):
                continue

            if '|' in str(directors_str):
                directors = [d.strip() for d in str(directors_str).split('|') if d.strip()]
            elif ',' in str(directors_str):
                # Remove brackets, quotes, and split
                directors_str = str(directors_str).strip("[]'\"")
                directors = [d.strip().strip("'\"").strip("[]") for d in directors_str.split(',') if d.strip()]
            else:
                directors = [str(directors_str).strip().strip("[]'\"")]

            # Clean up any remaining brackets/quotes
            directors = [d.strip().strip("[]'\"") for d in directors if d and d.strip().strip("[]'\"")]

            for director in directors:
                if director:
                    director_profits[director] += row['profit']

        top_directors = sorted(director_profits.items(), key=lambda x: x[1], reverse=True)[:10]

        if top_directors:
            directors, profits = zip(*top_directors)
            y_pos = np.arange(len(directors))
            colors_grad = plt.cm.Greens(np.linspace(0.4, 0.9, len(directors)))
            ax.barh(y_pos, [p/1e6 for p in profits], color=colors_grad, edgecolor='white')
            ax.set_yticks(y_pos)
            ax.set_yticklabels(directors, fontsize=9)
            ax.invert_yaxis()
            ax.set_xlabel('Total Profit (Millions USD)', fontsize=11)
            ax.set_title('Top 10 Most Profitable Directors', fontsize=12, fontweight='bold')

        plt.suptitle('Financial Data Overview', fontsize=18, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.output_dir / '10_coverage_overview.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 10_coverage_overview.png")

    def _generate_report(self):
        """Generate comprehensive text report."""
        logger.info("Generating comprehensive report...")

        report_file = self.report_dir / 'batch_22_financial_intelligence_report.txt'

        df_budget = self.df[self.df['budget'] > 0]
        df_revenue = self.df[self.df['revenue'] > 0]
        df_complete = self.df[(self.df['budget'] > 0) & (self.df['revenue'] > 0)]
        df_complete['roi'] = ((df_complete['revenue'] - df_complete['budget']) / df_complete['budget']) * 100
        df_complete['profit'] = df_complete['revenue'] - df_complete['budget']

        with open(report_file, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("CINESCOPE BATCH 22: FINANCIAL INTELLIGENCE ANALYSIS\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # Overview
            f.write("=" * 80 + "\n")
            f.write("OVERVIEW STATISTICS\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Total Films: {len(self.df):,}\n\n")
            f.write(f"Budget Coverage: {len(df_budget)/len(self.df)*100:.1f}%\n")
            f.write(f"  • Films with Budget Data: {len(df_budget):,}\n\n")
            f.write(f"Revenue Coverage: {len(df_revenue)/len(self.df)*100:.1f}%\n")
            f.write(f"  • Films with Revenue Data: {len(df_revenue):,}\n\n")
            f.write(f"Complete Financial Data: {len(df_complete)/len(self.df)*100:.1f}%\n")
            f.write(f"  • Films with Both Budget & Revenue: {len(df_complete):,}\n\n")

            # Budget stats
            f.write("=" * 80 + "\n")
            f.write("BUDGET STATISTICS\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Total Budget: ${df_budget['budget'].sum()/1e9:.2f}B\n")
            f.write(f"Average Budget: ${df_budget['budget'].mean()/1e6:.1f}M\n")
            f.write(f"Median Budget: ${df_budget['budget'].median()/1e6:.1f}M\n")
            f.write(f"Min Budget: ${df_budget['budget'].min()/1e6:.1f}M\n")
            f.write(f"Max Budget: ${df_budget['budget'].max()/1e6:.1f}M\n\n")

            # Revenue stats
            f.write("=" * 80 + "\n")
            f.write("REVENUE STATISTICS\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Total Revenue: ${df_revenue['revenue'].sum()/1e9:.2f}B\n")
            f.write(f"Average Revenue: ${df_revenue['revenue'].mean()/1e6:.1f}M\n")
            f.write(f"Median Revenue: ${df_revenue['revenue'].median()/1e6:.1f}M\n")
            f.write(f"Min Revenue: ${df_revenue['revenue'].min()/1e6:.1f}M\n")
            f.write(f"Max Revenue: ${df_revenue['revenue'].max()/1e6:.1f}M\n\n")

            # ROI stats
            f.write("=" * 80 + "\n")
            f.write("PROFITABILITY & ROI\n")
            f.write("=" * 80 + "\n\n")
            profitable = len(df_complete[df_complete['profit'] > 0])
            f.write(f"Profitable Films: {profitable} ({profitable/len(df_complete)*100:.1f}%)\n")
            f.write(f"Unprofitable Films: {len(df_complete) - profitable} ({(len(df_complete)-profitable)/len(df_complete)*100:.1f}%)\n\n")
            f.write(f"Average ROI: {df_complete['roi'].mean():.1f}%\n")
            f.write(f"Median ROI: {df_complete['roi'].median():.1f}%\n")
            f.write(f"Best ROI: {df_complete['roi'].max():.1f}%\n")
            f.write(f"Worst ROI: {df_complete['roi'].min():.1f}%\n\n")
            f.write(f"Total Profit: ${df_complete['profit'].sum()/1e9:.2f}B\n")
            f.write(f"Average Profit: ${df_complete['profit'].mean()/1e6:.1f}M\n\n")

            # Top lists
            f.write("=" * 80 + "\n")
            f.write("TOP 20 MOST EXPENSIVE FILMS\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"{'Rank':<6}{'Title':<45}{'Year':<8}{'Budget':<15}\n")
            f.write("-" * 80 + "\n")
            for i, (_, row) in enumerate(df_budget.nlargest(20, 'budget').iterrows(), 1):
                title = row['title'][:42] + "..." if len(row['title']) > 42 else row['title']
                f.write(f"{i:<6}{title:<45}{int(row['year']):<8}${row['budget']/1e6:.1f}M\n")

            f.write("\n" + "=" * 80 + "\n")
            f.write("TOP 20 HIGHEST GROSSING FILMS\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"{'Rank':<6}{'Title':<45}{'Year':<8}{'Revenue':<15}\n")
            f.write("-" * 80 + "\n")
            for i, (_, row) in enumerate(df_revenue.nlargest(20, 'revenue').iterrows(), 1):
                title = row['title'][:42] + "..." if len(row['title']) > 42 else row['title']
                f.write(f"{i:<6}{title:<45}{int(row['year']):<8}${row['revenue']/1e6:.1f}M\n")

            f.write("\n" + "=" * 80 + "\n")
            f.write("TOP 20 BEST ROI (RETURN ON INVESTMENT)\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"{'Rank':<6}{'Title':<40}{'Year':<8}{'ROI':<12}{'Profit':<15}\n")
            f.write("-" * 80 + "\n")
            for i, (_, row) in enumerate(df_complete.nlargest(20, 'roi').iterrows(), 1):
                title = row['title'][:37] + "..." if len(row['title']) > 37 else row['title']
                f.write(f"{i:<6}{title:<40}{int(row['year']):<8}{row['roi']:.0f}%{' ':<8}${row['profit']/1e6:.1f}M\n")

            f.write("\n" + "=" * 80 + "\n")
            f.write("TOP 20 ABSOLUTE PROFIT\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"{'Rank':<6}{'Title':<45}{'Year':<8}{'Profit':<15}\n")
            f.write("-" * 80 + "\n")
            for i, (_, row) in enumerate(df_complete.nlargest(20, 'profit').iterrows(), 1):
                title = row['title'][:42] + "..." if len(row['title']) > 42 else row['title']
                f.write(f"{i:<6}{title:<45}{int(row['year']):<8}${row['profit']/1e6:.1f}M\n")

        logger.info(f"✓ Report saved: {report_file}")


def main():
    analyzer = FinancialAnalyzer()
    analyzer.analyze()


if __name__ == '__main__':
    main()
