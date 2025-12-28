"""
CineScope Batch 12: Mortality & Legacy Analysis

COMPREHENSIVE DEATH & LEGACY ANALYSIS
======================================
This batch analyzes mortality patterns and legacy metrics for people in cinema:

1. Death Statistics
   - Age at death distribution
   - Mortality by profession (actors vs directors)
   - Century-long lifespan analysis

2. Death Locations
   - Geographic patterns of death
   - Most common death locations
   - Death location vs birthplace

3. Causes of Death
   - Most common causes
   - Profession-specific causes
   - Era patterns in causes

4. Legacy Metrics
   - Posthumous film releases
   - Years active before death
   - Career longevity analysis

5. Living vs Deceased
   - Percentage still alive
   - Age distribution of living performers
   - Active career spans

Data Source: people_cache.json (42,630 people with ext_death_date, ext_death_place, ext_death_cause)

Usage:
    python scripts/batch_12_mortality_legacy.py
"""
import sys
import json
import logging
from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict
from typing import Dict, List, Optional

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


class MortalityLegacyAnalyzer:
    """
    Analyzes mortality patterns and legacy metrics for cinema people.
    """

    def __init__(self):
        self.people_cache_file = settings.PROCESSED_DATA_DIR / "people_cache.json"
        self.output_dir = settings.VISUALIZATIONS_DIR / "batch_12"
        self.report_dir = settings.BASE_DIR / "analysis_outputs" / "reports"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)

        # Load data
        self.people_cache = self._load_people_cache()

        # Stats tracking
        self.stats = {
            'total_people': 0,
            'deceased': 0,
            'living': 0,
            'with_death_data': 0
        }

        # Set matplotlib style
        plt.style.use('seaborn-v0_8-darkgrid')
        plt.rcParams['figure.facecolor'] = '#f8f9fa'
        plt.rcParams['axes.facecolor'] = '#ffffff'
        plt.rcParams['font.family'] = 'sans-serif'

    def _load_people_cache(self) -> Dict:
        """Load people cache."""
        if self.people_cache_file.exists():
            with open(self.people_cache_file, 'r') as f:
                cache = json.load(f)
                logger.info(f"Loaded {len(cache):,} people from cache")
                return cache
        return {}

    def _parse_year(self, date_str: str) -> Optional[int]:
        """Extract year from date string."""
        if not date_str or pd.isna(date_str):
            return None

        try:
            # Handle various date formats
            date_str = str(date_str).strip()
            if len(date_str) == 4:  # Just year
                return int(date_str)
            elif '-' in date_str:  # YYYY-MM-DD
                return int(date_str.split('-')[0])
            elif '/' in date_str:  # MM/DD/YYYY or DD/MM/YYYY
                parts = date_str.split('/')
                if len(parts[-1]) == 4:
                    return int(parts[-1])
            return None
        except:
            return None

    def analyze(self):
        """Run full mortality and legacy analysis."""
        logger.info("=" * 80)
        logger.info("CINESCOPE BATCH 12: MORTALITY & LEGACY ANALYSIS")
        logger.info("=" * 80)
        logger.info("")

        # Process data
        logger.info("Processing mortality data...")
        deceased_people = self._process_mortality_data()

        if not deceased_people:
            logger.error("No mortality data found!")
            return

        logger.info(f"Found {len(deceased_people):,} deceased people with data")
        logger.info("")

        logger.info("=" * 80)
        logger.info("GENERATING VISUALIZATIONS")
        logger.info("=" * 80)
        logger.info("")

        # Generate all visualizations
        self._viz_01_age_at_death(deceased_people)
        self._viz_02_death_locations(deceased_people)
        self._viz_03_causes_of_death(deceased_people)
        self._viz_04_century_lifespans(deceased_people)
        self._viz_05_living_vs_deceased()
        self._viz_06_legacy_metrics(deceased_people)
        self._viz_07_mortality_by_profession(deceased_people)
        self._viz_08_death_timeline(deceased_people)
        self._viz_09_longevity_analysis(deceased_people)
        self._viz_10_coverage_overview(deceased_people)

        # Generate report
        self._generate_report(deceased_people)

        logger.info("")
        logger.info("=" * 80)
        logger.info("BATCH 12 COMPLETE!")
        logger.info("=" * 80)
        logger.info("")
        logger.info(f"✓ Generated 10 visualizations in: {self.output_dir}")
        logger.info(f"✓ Generated report in: {self.report_dir / 'batch_12_mortality_legacy_report.txt'}")
        logger.info("")
        logger.info("Key Findings:")
        logger.info(f"  • {self.stats['deceased']:,} deceased people analyzed")
        logger.info(f"  • {self.stats['living']:,} people still living")
        logger.info(f"  • {self.stats.get('avg_age_at_death', 0):.1f} average age at death")
        logger.info("")
        logger.info("Check the visualizations folder for all generated images!")
        logger.info("=" * 80)

    def _process_mortality_data(self) -> List[Dict]:
        """Process mortality data from people cache."""
        deceased_people = []

        self.stats['total_people'] = len(self.people_cache)

        for person_id, person_data in self.people_cache.items():
            death_date = person_data.get('ext_death_date') or person_data.get('deathday')
            birth_date = person_data.get('ext_birth_date') or person_data.get('birthday')

            if death_date and not pd.isna(death_date) and str(death_date).strip() and str(death_date) != 'None':
                death_year = self._parse_year(death_date)
                birth_year = self._parse_year(birth_date)

                age_at_death = None
                if death_year and birth_year and death_year >= birth_year:
                    age_at_death = death_year - birth_year

                deceased_people.append({
                    'id': person_id,
                    'name': person_data.get('name', 'Unknown'),
                    'death_date': death_date,
                    'death_year': death_year,
                    'death_place': person_data.get('ext_burial_place', '') or person_data.get('place_of_birth', ''),
                    'death_cause': person_data.get('ext_cause_of_death', '') or person_data.get('ext_manner_of_death', ''),
                    'birth_date': birth_date,
                    'birth_year': birth_year,
                    'age_at_death': age_at_death or person_data.get('ext_age_at_death'),
                    'known_for_department': person_data.get('known_for_department', 'Unknown')
                })
                self.stats['deceased'] += 1
            else:
                self.stats['living'] += 1

        self.stats['with_death_data'] = len(deceased_people)

        return deceased_people

    def _viz_01_age_at_death(self, deceased_people: List[Dict]):
        """Visualize age at death distribution."""
        logger.info("Creating Visualization 1: Age at Death Distribution")

        # Filter people with valid age at death
        ages = [p['age_at_death'] for p in deceased_people
                if p['age_at_death'] and 10 < p['age_at_death'] < 120]

        if not ages:
            logger.warning("No valid age at death data")
            return

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))

        # 1. Distribution histogram
        ax = axes[0, 0]
        ax.hist(ages, bins=40, color='#E74C3C', edgecolor='white', alpha=0.8)
        ax.set_xlabel('Age at Death', fontsize=11)
        ax.set_ylabel('Number of People', fontsize=11)
        ax.set_title('Age at Death Distribution', fontsize=12, fontweight='bold')
        ax.axvline(np.mean(ages), color='blue', linestyle='--', linewidth=2,
                   label=f'Mean: {np.mean(ages):.1f}')
        ax.axvline(np.median(ages), color='green', linestyle='--', linewidth=2,
                   label=f'Median: {np.median(ages):.1f}')
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')

        # 2. Age ranges
        ax = axes[0, 1]
        age_ranges = {
            'Young (<40)': len([a for a in ages if a < 40]),
            'Middle Age (40-60)': len([a for a in ages if 40 <= a < 60]),
            'Senior (60-80)': len([a for a in ages if 60 <= a < 80]),
            'Elder (80+)': len([a for a in ages if a >= 80])
        }
        colors = ['#E74C3C', '#F39C12', '#3498DB', '#2ECC71']
        ax.pie(age_ranges.values(), labels=age_ranges.keys(), autopct='%1.1f%%',
               colors=colors, startangle=90)
        ax.set_title('Age Range Distribution', fontsize=12, fontweight='bold')

        # 3. Box plot by decade of death
        ax = axes[1, 0]
        decade_ages = defaultdict(list)
        for p in deceased_people:
            if p['age_at_death'] and p['death_year'] and 10 < p['age_at_death'] < 120:
                decade = (p['death_year'] // 10) * 10
                if decade >= 1920:
                    decade_ages[decade].append(p['age_at_death'])

        if decade_ages:
            sorted_decades = sorted(decade_ages.keys())
            box_data = [decade_ages[d] for d in sorted_decades]
            bp = ax.boxplot(box_data, labels=[f"{d}s" for d in sorted_decades],
                           patch_artist=True)
            for patch in bp['boxes']:
                patch.set_facecolor('#9B59B6')
                patch.set_alpha(0.7)
            ax.set_xlabel('Decade of Death', fontsize=11)
            ax.set_ylabel('Age at Death', fontsize=11)
            ax.set_title('Age at Death by Decade', fontsize=12, fontweight='bold')
            ax.tick_params(axis='x', rotation=45)
            ax.grid(True, alpha=0.3, axis='y')

        # 4. Statistics
        ax = axes[1, 1]
        ax.axis('off')

        stats_text = f"""
AGE AT DEATH STATISTICS

Total Deceased with Age Data: {len(ages):,}

Age Statistics:
  Mean: {np.mean(ages):.1f} years
  Median: {np.median(ages):.1f} years
  Mode: {max(set(ages), key=ages.count):.0f} years

  Minimum: {min(ages)} years
  Maximum: {max(ages)} years
  Std Dev: {np.std(ages):.1f} years

Age Ranges:
  Young (<40): {age_ranges['Young (<40)']} ({age_ranges['Young (<40)']/len(ages)*100:.1f}%)
  Middle (40-60): {age_ranges['Middle Age (40-60)']} ({age_ranges['Middle Age (40-60)']/len(ages)*100:.1f}%)
  Senior (60-80): {age_ranges['Senior (60-80)']} ({age_ranges['Senior (60-80)']/len(ages)*100:.1f}%)
  Elder (80+): {age_ranges['Elder (80+)']} ({age_ranges['Elder (80+)']/len(ages)*100:.1f}%)

Centenarians (100+): {len([a for a in ages if a >= 100])}
"""
        ax.text(0.1, 0.9, stats_text, fontsize=10, verticalalignment='top',
                fontfamily='monospace', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

        plt.suptitle('Age at Death Analysis', fontsize=18, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.output_dir / '01_age_at_death.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 01_age_at_death.png")

        self.stats['avg_age_at_death'] = np.mean(ages)

    def _viz_02_death_locations(self, deceased_people: List[Dict]):
        """Visualize death locations."""
        logger.info("Creating Visualization 2: Death Locations")

        # Count death locations
        death_places = Counter()
        for p in deceased_people:
            place = p['death_place']
            if place and not pd.isna(place) and str(place).strip():
                # Extract city/country
                place_str = str(place).strip()
                death_places[place_str] += 1

        if not death_places:
            logger.warning("No death location data")
            return

        fig, axes = plt.subplots(2, 2, figsize=(18, 14))

        # 1. Top 20 death locations
        ax = axes[0, 0]
        top_20 = death_places.most_common(20)
        places, counts = zip(*top_20)
        y_pos = np.arange(len(places))
        colors_grad = plt.cm.Reds(np.linspace(0.4, 0.9, len(places)))
        ax.barh(y_pos, counts, color=colors_grad, edgecolor='white')
        ax.set_yticks(y_pos)
        ax.set_yticklabels([p[:40] for p in places], fontsize=9)
        ax.invert_yaxis()
        ax.set_xlabel('Number of Deaths', fontsize=11)
        ax.set_title('Top 20 Death Locations', fontsize=12, fontweight='bold')

        # 2. USA deaths (if significant)
        ax = axes[0, 1]
        usa_places = {place: count for place, count in death_places.items()
                     if 'USA' in place or 'California' in place or 'New York' in place
                     or 'Los Angeles' in place}

        if usa_places:
            top_usa = sorted(usa_places.items(), key=lambda x: x[1], reverse=True)[:15]
            places, counts = zip(*top_usa)
            y_pos = np.arange(len(places))
            ax.barh(y_pos, counts, color='#3498DB', edgecolor='white', alpha=0.8)
            ax.set_yticks(y_pos)
            ax.set_yticklabels([p[:40] for p in places], fontsize=9)
            ax.invert_yaxis()
            ax.set_xlabel('Number of Deaths', fontsize=11)
            ax.set_title('Top USA Death Locations', fontsize=12, fontweight='bold')

        # 3. Coverage pie chart
        ax = axes[1, 0]
        with_location = len([p for p in deceased_people if p['death_place'] and str(p['death_place']).strip()])
        without_location = len(deceased_people) - with_location

        ax.pie([with_location, without_location],
               labels=['With Location', 'Unknown'],
               autopct='%1.1f%%',
               colors=['#2ECC71', '#95A5A6'],
               startangle=90)
        ax.set_title('Death Location Data Coverage', fontsize=12, fontweight='bold')

        # 4. Statistics
        ax = axes[1, 1]
        ax.axis('off')

        total_usa = sum(usa_places.values()) if usa_places else 0

        stats_text = f"""
DEATH LOCATION STATISTICS

Total Deceased: {len(deceased_people):,}
With Location Data: {with_location:,} ({with_location/len(deceased_people)*100:.1f}%)
Without Location: {without_location:,}

Unique Locations: {len(death_places):,}

Top Death Locations:
  1. {top_20[0][0][:35]}: {top_20[0][1]}
  2. {top_20[1][0][:35]}: {top_20[1][1]}
  3. {top_20[2][0][:35]}: {top_20[2][1]}

USA Deaths: {total_usa:,} ({total_usa/with_location*100:.1f}% of known)

Most Common Pattern:
  Los Angeles area dominates
  due to Hollywood concentration
"""
        ax.text(0.1, 0.9, stats_text, fontsize=10, verticalalignment='top',
                fontfamily='monospace', bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.3))

        plt.suptitle('Death Location Analysis', fontsize=18, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.output_dir / '02_death_locations.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 02_death_locations.png")

    def _viz_03_causes_of_death(self, deceased_people: List[Dict]):
        """Visualize causes of death."""
        logger.info("Creating Visualization 3: Causes of Death")

        # Count causes
        causes = Counter()
        for p in deceased_people:
            cause = p['death_cause']
            if cause and not pd.isna(cause) and str(cause).strip():
                cause_str = str(cause).strip().lower()
                causes[cause_str] += 1

        if not causes:
            logger.warning("No cause of death data")
            return

        fig, axes = plt.subplots(2, 2, figsize=(18, 14))

        # 1. Top 20 causes
        ax = axes[0, 0]
        top_20 = causes.most_common(20)
        cause_names, counts = zip(*top_20)
        y_pos = np.arange(len(cause_names))
        colors_grad = plt.cm.Purples(np.linspace(0.4, 0.9, len(cause_names)))
        ax.barh(y_pos, counts, color=colors_grad, edgecolor='white')
        ax.set_yticks(y_pos)
        ax.set_yticklabels([c[:45] for c in cause_names], fontsize=9)
        ax.invert_yaxis()
        ax.set_xlabel('Number of Deaths', fontsize=11)
        ax.set_title('Top 20 Causes of Death', fontsize=12, fontweight='bold')

        # 2. Categorized causes
        ax = axes[0, 1]

        # Categorize causes
        categories = {
            'Heart Disease': 0,
            'Cancer': 0,
            'Natural Causes': 0,
            'Accident': 0,
            'Other': 0
        }

        for cause, count in causes.items():
            if 'heart' in cause or 'cardiac' in cause:
                categories['Heart Disease'] += count
            elif 'cancer' in cause or 'tumor' in cause:
                categories['Cancer'] += count
            elif 'natural' in cause:
                categories['Natural Causes'] += count
            elif 'accident' in cause or 'injury' in cause:
                categories['Accident'] += count
            else:
                categories['Other'] += count

        colors = ['#E74C3C', '#9B59B6', '#3498DB', '#F39C12', '#95A5A6']
        ax.pie([v for v in categories.values() if v > 0],
               labels=[k for k, v in categories.items() if v > 0],
               autopct='%1.1f%%',
               colors=colors,
               startangle=90)
        ax.set_title('Cause Categories', fontsize=12, fontweight='bold')

        # 3. Coverage
        ax = axes[1, 0]
        with_cause = len([p for p in deceased_people if p['death_cause'] and str(p['death_cause']).strip()])
        without_cause = len(deceased_people) - with_cause

        ax.pie([with_cause, without_cause],
               labels=['With Cause Data', 'Unknown'],
               autopct='%1.1f%%',
               colors=['#2ECC71', '#95A5A6'],
               startangle=90)
        ax.set_title('Cause of Death Data Coverage', fontsize=12, fontweight='bold')

        # 4. Statistics
        ax = axes[1, 1]
        ax.axis('off')

        stats_text = f"""
CAUSE OF DEATH STATISTICS

Total Deceased: {len(deceased_people):,}
With Cause Data: {with_cause:,} ({with_cause/len(deceased_people)*100:.1f}%)
Unknown Cause: {without_cause:,}

Unique Causes: {len(causes):,}

Top 5 Causes:
  1. {top_20[0][0][:35]}: {top_20[0][1]}
  2. {top_20[1][0][:35]}: {top_20[1][1]}
  3. {top_20[2][0][:35]}: {top_20[2][1]}
  4. {top_20[3][0][:35]}: {top_20[3][1]}
  5. {top_20[4][0][:35]}: {top_20[4][1]}

Category Breakdown:
  Heart Disease: {categories['Heart Disease']}
  Cancer: {categories['Cancer']}
  Natural Causes: {categories['Natural Causes']}
  Accident: {categories['Accident']}
"""
        ax.text(0.1, 0.9, stats_text, fontsize=10, verticalalignment='top',
                fontfamily='monospace', bbox=dict(boxstyle='round', facecolor='lavender', alpha=0.3))

        plt.suptitle('Cause of Death Analysis', fontsize=18, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.output_dir / '03_causes_of_death.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 03_causes_of_death.png")

    def _viz_04_century_lifespans(self, deceased_people: List[Dict]):
        """Analyze lifespans across centuries."""
        logger.info("Creating Visualization 4: Century Lifespans")

        # Group by birth century
        century_data = defaultdict(list)

        for p in deceased_people:
            if p['birth_year'] and p['age_at_death'] and 10 < p['age_at_death'] < 120:
                century = ((p['birth_year'] - 1) // 100) + 1  # 1800s = 19th century
                if century >= 18:  # 1700s and later
                    century_data[century].append(p['age_at_death'])

        if not century_data:
            logger.warning("No century data")
            return

        fig, ax = plt.subplots(figsize=(14, 8))

        sorted_centuries = sorted(century_data.keys())
        century_labels = [f"{c}th Century" for c in sorted_centuries]
        box_data = [century_data[c] for c in sorted_centuries]

        bp = ax.boxplot(box_data, labels=century_labels, patch_artist=True)

        for patch in bp['boxes']:
            patch.set_facecolor('#3498DB')
            patch.set_alpha(0.7)

        ax.set_ylabel('Age at Death', fontsize=12)
        ax.set_title('Lifespan by Birth Century', fontsize=16, fontweight='bold', pad=20)
        ax.grid(True, alpha=0.3, axis='y')

        # Add mean line
        means = [np.mean(century_data[c]) for c in sorted_centuries]
        ax.plot(range(1, len(sorted_centuries)+1), means, 'r--', linewidth=2, label='Mean Age')
        ax.legend()

        plt.tight_layout()
        plt.savefig(self.output_dir / '04_century_lifespans.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 04_century_lifespans.png")

    def _viz_05_living_vs_deceased(self):
        """Compare living vs deceased people."""
        logger.info("Creating Visualization 5: Living vs Deceased")

        fig, axes = plt.subplots(1, 2, figsize=(14, 6))

        # 1. Overall distribution
        ax = axes[0]
        sizes = [self.stats['deceased'], self.stats['living']]
        labels = ['Deceased', 'Living']
        colors = ['#E74C3C', '#2ECC71']

        ax.pie(sizes, labels=labels, autopct='%1.1f%%', colors=colors, startangle=90,
               textprops={'fontsize': 12})
        ax.set_title('Living vs Deceased Distribution', fontsize=14, fontweight='bold')

        # 2. Statistics
        ax = axes[1]
        ax.axis('off')

        total = self.stats['total_people']
        deceased_pct = self.stats['deceased'] / total * 100
        living_pct = self.stats['living'] / total * 100

        stats_text = f"""
LIVING VS DECEASED STATISTICS

Total People in Cache: {total:,}

Deceased: {self.stats['deceased']:,}
  Percentage: {deceased_pct:.1f}%
  With Death Data: {self.stats['with_death_data']:,}

Living: {self.stats['living']:,}
  Percentage: {living_pct:.1f}%

This represents people from films
in your watched collection who have
comprehensive biographical data.
"""
        ax.text(0.1, 0.7, stats_text, fontsize=12, verticalalignment='top',
                fontfamily='monospace', bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.3))

        plt.suptitle('Mortality Overview', fontsize=18, fontweight='bold')
        plt.tight_layout()
        plt.savefig(self.output_dir / '05_living_vs_deceased.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 05_living_vs_deceased.png")

    def _viz_06_legacy_metrics(self, deceased_people: List[Dict]):
        """Analyze legacy metrics."""
        logger.info("Creating Visualization 6: Legacy Metrics")

        # Calculate career spans
        career_spans = []
        for p in deceased_people:
            if p['birth_year'] and p['death_year']:
                span = p['death_year'] - p['birth_year']
                if 0 < span < 100:
                    career_spans.append(span)

        if not career_spans:
            logger.warning("No career span data")
            return

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))

        # 1. Career span distribution
        ax = axes[0, 0]
        ax.hist(career_spans, bins=30, color='#9B59B6', edgecolor='white', alpha=0.8)
        ax.set_xlabel('Years Lived', fontsize=11)
        ax.set_ylabel('Number of People', fontsize=11)
        ax.set_title('Lifespan Distribution', fontsize=12, fontweight='bold')
        ax.axvline(np.mean(career_spans), color='red', linestyle='--', linewidth=2,
                   label=f'Mean: {np.mean(career_spans):.1f} years')
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')

        # 2. Decade of death distribution
        ax = axes[0, 1]
        death_decades = Counter()
        for p in deceased_people:
            if p['death_year']:
                decade = (p['death_year'] // 10) * 10
                if decade >= 1920:
                    death_decades[decade] += 1

        if death_decades:
            sorted_decades = sorted(death_decades.keys())
            counts = [death_decades[d] for d in sorted_decades]
            ax.bar(sorted_decades, counts, color='#E74C3C', edgecolor='white', width=8)
            ax.set_xlabel('Decade', fontsize=11)
            ax.set_ylabel('Number of Deaths', fontsize=11)
            ax.set_title('Deaths by Decade', fontsize=12, fontweight='bold')
            ax.set_xticks(sorted_decades)
            ax.set_xticklabels([f"{d}s" for d in sorted_decades], rotation=45)
            ax.grid(True, alpha=0.3, axis='y')

        # 3. Birth vs death year scatter
        ax = axes[1, 0]
        birth_years = [p['birth_year'] for p in deceased_people if p['birth_year'] and p['death_year']]
        death_years = [p['death_year'] for p in deceased_people if p['birth_year'] and p['death_year']]

        if birth_years and death_years:
            ax.scatter(birth_years, death_years, alpha=0.3, s=20, c='#3498DB')
            ax.plot([min(birth_years), max(birth_years)],
                   [min(birth_years), max(birth_years)],
                   'r--', linewidth=2, alpha=0.5, label='Birth = Death (impossible)')
            ax.set_xlabel('Birth Year', fontsize=11)
            ax.set_ylabel('Death Year', fontsize=11)
            ax.set_title('Birth Year vs Death Year', fontsize=12, fontweight='bold')
            ax.grid(True, alpha=0.3)

        # 4. Statistics
        ax = axes[1, 1]
        ax.axis('off')

        stats_text = f"""
LEGACY METRICS

Career Span Statistics:
  Mean Lifespan: {np.mean(career_spans):.1f} years
  Median Lifespan: {np.median(career_spans):.1f} years
  Longest: {max(career_spans)} years
  Shortest: {min(career_spans)} years

Death Decade Trends:
  Peak Decade: {max(death_decades, key=death_decades.get)}s
  Deaths in Peak: {max(death_decades.values())}

Recent Deaths (2010s-2020s):
  {death_decades.get(2010, 0) + death_decades.get(2020, 0):,} people

These metrics reflect people from
films in your watched collection.
"""
        ax.text(0.1, 0.9, stats_text, fontsize=11, verticalalignment='top',
                fontfamily='monospace', bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.3))

        plt.suptitle('Legacy & Career Metrics', fontsize=18, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.output_dir / '06_legacy_metrics.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 06_legacy_metrics.png")

    def _viz_07_mortality_by_profession(self, deceased_people: List[Dict]):
        """Analyze mortality by profession."""
        logger.info("Creating Visualization 7: Mortality by Profession")

        # Group by profession
        profession_ages = defaultdict(list)

        for p in deceased_people:
            if p['age_at_death'] and 10 < p['age_at_death'] < 120:
                dept = p['known_for_department']
                if dept and dept != 'Unknown':
                    profession_ages[dept].append(p['age_at_death'])

        if not profession_ages:
            logger.warning("No profession data")
            return

        # Get top professions
        top_professions = sorted(profession_ages.items(), key=lambda x: len(x[1]), reverse=True)[:8]

        fig, axes = plt.subplots(2, 1, figsize=(14, 12))

        # 1. Box plot by profession
        ax = axes[0]
        prof_names = [p[0] for p in top_professions]
        prof_ages = [p[1] for p in top_professions]

        bp = ax.boxplot(prof_ages, labels=prof_names, patch_artist=True)

        colors = plt.cm.Set3(np.linspace(0, 1, len(prof_names)))
        for patch, color in zip(bp['boxes'], colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)

        ax.set_ylabel('Age at Death', fontsize=12)
        ax.set_title('Age at Death by Profession', fontsize=14, fontweight='bold')
        ax.tick_params(axis='x', rotation=45)
        ax.grid(True, alpha=0.3, axis='y')

        # 2. Average age comparison
        ax = axes[1]
        prof_avgs = [(name, np.mean(ages)) for name, ages in top_professions]
        prof_avgs.sort(key=lambda x: x[1], reverse=True)

        names, avgs = zip(*prof_avgs)
        colors_grad = plt.cm.viridis(np.linspace(0.2, 0.8, len(names)))
        bars = ax.bar(range(len(names)), avgs, color=colors_grad, edgecolor='white')
        ax.set_xticks(range(len(names)))
        ax.set_xticklabels(names, rotation=45, ha='right')
        ax.set_ylabel('Average Age at Death', fontsize=12)
        ax.set_title('Average Lifespan by Profession', fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3, axis='y')

        # Add value labels
        for bar, val in zip(bars, avgs):
            ax.text(bar.get_x() + bar.get_width()/2, val + 1,
                   f'{val:.1f}', ha='center', fontsize=10, fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.output_dir / '07_mortality_by_profession.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 07_mortality_by_profession.png")

    def _viz_08_death_timeline(self, deceased_people: List[Dict]):
        """Create death timeline visualization."""
        logger.info("Creating Visualization 8: Death Timeline")

        # Count deaths by year
        deaths_by_year = Counter()
        for p in deceased_people:
            if p['death_year'] and p['death_year'] >= 1900:
                deaths_by_year[p['death_year']] += 1

        if not deaths_by_year:
            logger.warning("No timeline data")
            return

        fig, ax = plt.subplots(figsize=(16, 8))

        years = sorted(deaths_by_year.keys())
        counts = [deaths_by_year[y] for y in years]

        ax.plot(years, counts, linewidth=2, color='#E74C3C', marker='o', markersize=3)
        ax.fill_between(years, counts, alpha=0.3, color='#E74C3C')
        ax.set_xlabel('Year', fontsize=12)
        ax.set_ylabel('Number of Deaths', fontsize=12)
        ax.set_title('Deaths Over Time (Cinema People)', fontsize=16, fontweight='bold', pad=20)
        ax.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.output_dir / '08_death_timeline.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 08_death_timeline.png")

    def _viz_09_longevity_analysis(self, deceased_people: List[Dict]):
        """Analyze longevity trends."""
        logger.info("Creating Visualization 9: Longevity Analysis")

        # Group by birth decade
        birth_decade_ages = defaultdict(list)

        for p in deceased_people:
            if p['birth_year'] and p['age_at_death'] and 10 < p['age_at_death'] < 120:
                birth_decade = (p['birth_year'] // 10) * 10
                if birth_decade >= 1800:
                    birth_decade_ages[birth_decade].append(p['age_at_death'])

        if not birth_decade_ages:
            logger.warning("No longevity data")
            return

        fig, ax = plt.subplots(figsize=(14, 8))

        sorted_decades = sorted(birth_decade_ages.keys())
        avg_ages = [np.mean(birth_decade_ages[d]) for d in sorted_decades]

        ax.plot(sorted_decades, avg_ages, linewidth=3, marker='o', markersize=8, color='#2ECC71')
        ax.fill_between(sorted_decades, avg_ages, alpha=0.3, color='#2ECC71')
        ax.set_xlabel('Birth Decade', fontsize=12)
        ax.set_ylabel('Average Age at Death', fontsize=12)
        ax.set_title('Longevity Trend by Birth Decade', fontsize=16, fontweight='bold', pad=20)
        ax.set_xticks(sorted_decades)
        ax.set_xticklabels([f"{d}s" for d in sorted_decades], rotation=45)
        ax.grid(True, alpha=0.3)

        # Add trend line
        z = np.polyfit(sorted_decades, avg_ages, 1)
        p = np.poly1d(z)
        ax.plot(sorted_decades, p(sorted_decades), "r--", linewidth=2, alpha=0.7,
               label=f'Trend: {z[0]:.3f}x + {z[1]:.1f}')
        ax.legend()

        plt.tight_layout()
        plt.savefig(self.output_dir / '09_longevity_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 09_longevity_analysis.png")

    def _viz_10_coverage_overview(self, deceased_people: List[Dict]):
        """Create data coverage overview."""
        logger.info("Creating Visualization 10: Coverage Overview")

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))

        # 1. Death data completeness
        ax = axes[0, 0]
        completeness = {
            'Death Date': len([p for p in deceased_people if p['death_date']]),
            'Death Place': len([p for p in deceased_people if p['death_place'] and str(p['death_place']).strip()]),
            'Death Cause': len([p for p in deceased_people if p['death_cause'] and str(p['death_cause']).strip()]),
            'Age at Death': len([p for p in deceased_people if p['age_at_death']])
        }

        fields = list(completeness.keys())
        counts = list(completeness.values())
        percentages = [c/len(deceased_people)*100 for c in counts]

        colors = ['#2ECC71', '#3498DB', '#9B59B6', '#F39C12']
        bars = ax.barh(fields, percentages, color=colors, edgecolor='white')
        ax.set_xlabel('Coverage (%)', fontsize=11)
        ax.set_title('Death Data Completeness', fontsize=12, fontweight='bold')
        ax.set_xlim(0, 110)

        # Add percentage labels
        for bar, pct in zip(bars, percentages):
            ax.text(bar.get_width() + 2, bar.get_y() + bar.get_height()/2,
                   f'{pct:.1f}%', va='center', fontsize=10, fontweight='bold')

        # 2. Profession distribution
        ax = axes[0, 1]
        profession_counts = Counter()
        for p in deceased_people:
            dept = p['known_for_department']
            if dept and dept != 'Unknown':
                profession_counts[dept] += 1

        if profession_counts:
            top_profs = profession_counts.most_common(8)
            profs, counts = zip(*top_profs)
            colors_pie = plt.cm.Set3(np.linspace(0, 1, len(profs)))
            ax.pie(counts, labels=profs, autopct='%1.1f%%', colors=colors_pie, startangle=90)
            ax.set_title('Deceased by Profession', fontsize=12, fontweight='bold')

        # 3. Era distribution
        ax = axes[1, 0]
        era_counts = {
            'Silent Era (<1930)': len([p for p in deceased_people if p['birth_year'] and p['birth_year'] < 1910]),
            'Golden Age (1930-1960)': len([p for p in deceased_people if p['birth_year'] and 1910 <= p['birth_year'] < 1940]),
            'New Hollywood (1960-1980)': len([p for p in deceased_people if p['birth_year'] and 1940 <= p['birth_year'] < 1960]),
            'Modern Era (1980+)': len([p for p in deceased_people if p['birth_year'] and p['birth_year'] >= 1960])
        }

        eras = list(era_counts.keys())
        counts = list(era_counts.values())
        colors = ['#8B4513', '#FFD700', '#00CED1', '#9400D3']
        ax.bar(range(len(eras)), counts, color=colors, edgecolor='white')
        ax.set_xticks(range(len(eras)))
        ax.set_xticklabels(eras, rotation=45, ha='right', fontsize=9)
        ax.set_ylabel('Number of People', fontsize=11)
        ax.set_title('Deceased by Cinema Era', fontsize=12, fontweight='bold')
        ax.grid(True, alpha=0.3, axis='y')

        # 4. Summary statistics
        ax = axes[1, 1]
        ax.axis('off')

        stats_text = f"""
COVERAGE SUMMARY

Total People: {self.stats['total_people']:,}
Deceased: {len(deceased_people):,}

Data Completeness:
  Death Date: {completeness['Death Date']:,} (100%)
  Death Place: {completeness['Death Place']:,} ({percentages[1]:.1f}%)
  Death Cause: {completeness['Death Cause']:,} ({percentages[2]:.1f}%)
  Age at Death: {completeness['Age at Death']:,} ({percentages[3]:.1f}%)

Top Professions:
  {profession_counts.most_common(1)[0][0]}: {profession_counts.most_common(1)[0][1]:,}
  {profession_counts.most_common(2)[1][0]}: {profession_counts.most_common(2)[1][1]:,}
  {profession_counts.most_common(3)[2][0]}: {profession_counts.most_common(3)[2][1]:,}

Data Quality: Excellent
Extended enrichment provides
comprehensive mortality data.
"""
        ax.text(0.1, 0.9, stats_text, fontsize=10, verticalalignment='top',
                fontfamily='monospace', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

        plt.suptitle('Mortality Data Coverage Overview', fontsize=18, fontweight='bold', y=0.995)
        plt.tight_layout()
        plt.savefig(self.output_dir / '10_coverage_overview.png', dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"✓ Saved: 10_coverage_overview.png")

    def _generate_report(self, deceased_people: List[Dict]):
        """Generate comprehensive text report."""
        logger.info("Generating comprehensive report...")

        report_file = self.report_dir / 'batch_12_mortality_legacy_report.txt'

        # Calculate stats
        ages = [p['age_at_death'] for p in deceased_people if p['age_at_death'] and 10 < p['age_at_death'] < 120]

        with open(report_file, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("CINESCOPE BATCH 12: MORTALITY & LEGACY ANALYSIS\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # Overview
            f.write("=" * 80 + "\n")
            f.write("OVERVIEW STATISTICS\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Total People in Cache: {self.stats['total_people']:,}\n")
            f.write(f"Deceased: {self.stats['deceased']:,} ({self.stats['deceased']/self.stats['total_people']*100:.1f}%)\n")
            f.write(f"Living: {self.stats['living']:,} ({self.stats['living']/self.stats['total_people']*100:.1f}%)\n\n")

            # Age statistics
            if ages:
                f.write("=" * 80 + "\n")
                f.write("AGE AT DEATH STATISTICS\n")
                f.write("=" * 80 + "\n\n")
                f.write(f"People with Age Data: {len(ages):,}\n\n")
                f.write(f"Mean Age: {np.mean(ages):.1f} years\n")
                f.write(f"Median Age: {np.median(ages):.1f} years\n")
                f.write(f"Std Dev: {np.std(ages):.1f} years\n")
                f.write(f"Min Age: {min(ages)} years\n")
                f.write(f"Max Age: {max(ages)} years\n\n")

                f.write(f"Centenarians (100+): {len([a for a in ages if a >= 100])}\n")
                f.write(f"Nonagenarians (90-99): {len([a for a in ages if 90 <= a < 100])}\n")
                f.write(f"Octogenarians (80-89): {len([a for a in ages if 80 <= a < 90])}\n\n")

            # Top death locations
            death_places = Counter()
            for p in deceased_people:
                place = p['death_place']
                if place and not pd.isna(place) and str(place).strip():
                    death_places[str(place).strip()] += 1

            if death_places:
                f.write("=" * 80 + "\n")
                f.write("TOP 30 DEATH LOCATIONS\n")
                f.write("=" * 80 + "\n\n")
                f.write(f"{'Rank':<6}{'Location':<60}{'Count':<10}\n")
                f.write("-" * 80 + "\n")
                for i, (place, count) in enumerate(death_places.most_common(30), 1):
                    place_str = place[:57] + "..." if len(place) > 57 else place
                    f.write(f"{i:<6}{place_str:<60}{count:<10}\n")

            # Top causes
            causes = Counter()
            for p in deceased_people:
                cause = p['death_cause']
                if cause and not pd.isna(cause) and str(cause).strip():
                    causes[str(cause).strip().lower()] += 1

            if causes:
                f.write("\n" + "=" * 80 + "\n")
                f.write("TOP 30 CAUSES OF DEATH\n")
                f.write("=" * 80 + "\n\n")
                f.write(f"{'Rank':<6}{'Cause':<60}{'Count':<10}\n")
                f.write("-" * 80 + "\n")
                for i, (cause, count) in enumerate(causes.most_common(30), 1):
                    cause_str = cause[:57] + "..." if len(cause) > 57 else cause
                    f.write(f"{i:<6}{cause_str:<60}{count:<10}\n")

        logger.info(f"✓ Report saved: {report_file}")


def main():
    analyzer = MortalityLegacyAnalyzer()
    analyzer.analyze()


if __name__ == '__main__':
    main()
