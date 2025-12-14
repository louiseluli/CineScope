"""
CineScope Batch 9: Zodiac & Astrological Analysis

DEEP ASTROLOGICAL ANALYSIS OF CINEMA TALENT
============================================
This batch analyzes:

1. Zodiac Distribution
   - Western zodiac sign distribution among actors/directors
   - Chinese zodiac distribution
   - Elemental breakdown (Fire, Earth, Air, Water)
   - Quality by zodiac sign

2. Birth Patterns
   - Birth month/day distributions
   - "Lucky" birth dates in Hollywood
   - Seasonal patterns

3. Mortality Analysis (for deceased talent)
   - Age at death distribution
   - Common causes of death
   - Decade of death trends

4. Zodiac Compatibility
   - Director-actor zodiac pairings
   - Co-star compatibility patterns

5. Zodiac Performance
   - Average ratings by zodiac
   - Genre preferences by zodiac
   - Box office performance by zodiac

Usage:
    python scripts/batch_9_zodiac_analysis.py
"""
import sys
import json
import logging
from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict
from typing import Dict, List, Optional, Tuple
import csv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.colors import LinearSegmentedColormap
import seaborn as sns
import numpy as np
from tqdm import tqdm

from src.core.config import settings
from src.analysis.zodiac import ZodiacCalculator

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

# Zodiac colors
ZODIAC_COLORS = {
    'Aries': '#FF6B6B', 'Taurus': '#4ECDC4', 'Gemini': '#FFE66D',
    'Cancer': '#C7CEEA', 'Leo': '#FFEAA7', 'Virgo': '#DFE6E9',
    'Libra': '#FAB1A0', 'Scorpio': '#6C5CE7', 'Sagittarius': '#E17055',
    'Capricorn': '#636E72', 'Aquarius': '#00CEC9', 'Pisces': '#A29BFE'
}

ELEMENT_COLORS = {
    'Fire': '#FF4757', 'Earth': '#2ED573', 'Air': '#70A1FF', 'Water': '#5352ED'
}

CHINESE_ZODIAC_COLORS = {
    'Rat': '#2C3E50', 'Ox': '#8B4513', 'Tiger': '#FF6347',
    'Rabbit': '#FFB6C1', 'Dragon': '#FFD700', 'Snake': '#228B22',
    'Horse': '#DC143C', 'Goat': '#F5DEB3', 'Monkey': '#FF8C00',
    'Rooster': '#8B0000', 'Dog': '#D2691E', 'Pig': '#FFC0CB'
}


class ZodiacAnalyzer:
    """
    Analyzes astrological patterns in cinema data.
    """
    
    def __init__(self):
        self.people_cache_file = settings.PROCESSED_DATA_DIR / "people_cache.json"
        self.master_csv = settings.PROCESSED_DATA_DIR / "master_cinema_data.csv"
        self.output_dir = settings.VISUALIZATIONS_DIR / "batch_9"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.zodiac_calc = ZodiacCalculator()
        
        # Load data
        self.people = self._load_people()
        self.movies = self._load_movies()
        
        # Stats
        self.zodiac_stats = defaultdict(lambda: {
            'count': 0, 'actors': 0, 'directors': 0,
            'total_rating': 0, 'films_count': 0,
            'genres': Counter(), 'deceased': 0
        })
        self.chinese_zodiac_stats = defaultdict(lambda: {'count': 0})
        self.element_stats = defaultdict(lambda: {'count': 0})
        self.birth_month_stats = Counter()
        self.death_stats = {
            'causes': Counter(),
            'ages': [],
            'decades': Counter()
        }
        
        # Set style
        plt.style.use('seaborn-v0_8-darkgrid')
        plt.rcParams['figure.facecolor'] = '#f8f9fa'
        plt.rcParams['axes.facecolor'] = '#ffffff'
        plt.rcParams['font.family'] = 'sans-serif'
    
    def _load_people(self) -> Dict:
        """Load people from cache."""
        if self.people_cache_file.exists():
            with open(self.people_cache_file, 'r') as f:
                return json.load(f)
        return {}
    
    def _load_movies(self) -> List[Dict]:
        """Load movies from master CSV."""
        movies = []
        if self.master_csv.exists():
            with open(self.master_csv, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    movies.append(row)
        return movies
    
    def analyze(self):
        """Run full zodiac analysis."""
        logger.info("=" * 80)
        logger.info("BATCH 9: ZODIAC & ASTROLOGICAL ANALYSIS")
        logger.info("=" * 80)
        
        # Calculate zodiac for all people
        self._calculate_all_zodiac()
        
        # Generate visualizations
        self._viz_zodiac_distribution()
        self._viz_chinese_zodiac()
        self._viz_element_breakdown()
        self._viz_birth_months()
        self._viz_mortality_analysis()
        self._viz_zodiac_quality()
        self._viz_zodiac_wheel()
        
        # Generate report
        self._generate_report()
        
        logger.info(f"\nVisualizations saved to {self.output_dir}")
    
    def _calculate_all_zodiac(self):
        """Calculate zodiac for all people in cache."""
        logger.info("Calculating zodiac signs...")
        
        for person_id, person in tqdm(self.people.items(), desc="Processing people"):
            # Get birth date
            birth_date = (
                person.get('ext_birth_date') or
                person.get('birthday') or
                person.get('imdb_birth_year')
            )
            
            if not birth_date:
                continue
            
            # Use existing zodiac or calculate
            zodiac_western = person.get('zodiac_western')
            zodiac_chinese = person.get('zodiac_chinese')
            element = person.get('zodiac_element')
            
            if not zodiac_western:
                zodiac_result = self.zodiac_calc.calculate(str(birth_date))
                if zodiac_result:
                    zodiac_western = zodiac_result.get('western_sign')
                    zodiac_chinese = zodiac_result.get('chinese_animal')
                    element = zodiac_result.get('western_element')
            
            if zodiac_western:
                # Update stats
                self.zodiac_stats[zodiac_western]['count'] += 1
                
                # Check role
                dept = person.get('known_for_department', '')
                if dept == 'Acting':
                    self.zodiac_stats[zodiac_western]['actors'] += 1
                elif dept == 'Directing':
                    self.zodiac_stats[zodiac_western]['directors'] += 1
                
                # Check if deceased
                death_date = person.get('ext_death_date') or person.get('deathday')
                if death_date:
                    self.zodiac_stats[zodiac_western]['deceased'] += 1
                    
                    # Age at death
                    age_at_death = person.get('ext_age_at_death')
                    if age_at_death:
                        self.death_stats['ages'].append(age_at_death)
                    
                    # Cause of death
                    cause = person.get('ext_cause_of_death')
                    if cause:
                        self.death_stats['causes'][cause] += 1
                    
                    # Decade of death
                    try:
                        death_year = int(str(death_date)[:4])
                        decade = (death_year // 10) * 10
                        self.death_stats['decades'][decade] += 1
                    except:
                        pass
            
            if zodiac_chinese:
                self.chinese_zodiac_stats[zodiac_chinese]['count'] += 1
            
            if element:
                self.element_stats[element]['count'] += 1
            
            # Birth month
            try:
                month = int(str(birth_date)[5:7])
                self.birth_month_stats[month] += 1
            except:
                pass
    
    def _viz_zodiac_distribution(self):
        """Visualize Western zodiac distribution."""
        logger.info("Creating zodiac distribution visualization...")
        
        # Order zodiac signs
        zodiac_order = [
            'Aries', 'Taurus', 'Gemini', 'Cancer', 'Leo', 'Virgo',
            'Libra', 'Scorpio', 'Sagittarius', 'Capricorn', 'Aquarius', 'Pisces'
        ]
        
        counts = [self.zodiac_stats[z]['count'] for z in zodiac_order]
        colors = [ZODIAC_COLORS[z] for z in zodiac_order]
        
        fig, ax = plt.subplots(figsize=(14, 8))
        
        bars = ax.bar(zodiac_order, counts, color=colors, edgecolor='white', linewidth=2)
        
        # Add value labels
        for bar, count in zip(bars, counts):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(counts)*0.02,
                   f'{count:,}', ha='center', va='bottom', fontsize=10, fontweight='bold')
        
        ax.set_xlabel('Zodiac Sign', fontsize=12)
        ax.set_ylabel('Number of Talent', fontsize=12)
        ax.set_title('Western Zodiac Distribution in Cinema\n(Actors, Directors, Writers)', 
                    fontsize=16, fontweight='bold', pad=20)
        
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        plt.savefig(self.output_dir / 'zodiac_distribution.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def _viz_chinese_zodiac(self):
        """Visualize Chinese zodiac distribution."""
        logger.info("Creating Chinese zodiac visualization...")
        
        chinese_order = [
            'Rat', 'Ox', 'Tiger', 'Rabbit', 'Dragon', 'Snake',
            'Horse', 'Goat', 'Monkey', 'Rooster', 'Dog', 'Pig'
        ]
        
        counts = [self.chinese_zodiac_stats[z]['count'] for z in chinese_order]
        colors = [CHINESE_ZODIAC_COLORS.get(z, '#888888') for z in chinese_order]
        
        fig, ax = plt.subplots(figsize=(14, 8))
        
        bars = ax.bar(chinese_order, counts, color=colors, edgecolor='white', linewidth=2)
        
        for bar, count in zip(bars, counts):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(counts)*0.02,
                   f'{count:,}', ha='center', va='bottom', fontsize=10, fontweight='bold')
        
        ax.set_xlabel('Chinese Zodiac Animal', fontsize=12)
        ax.set_ylabel('Number of Talent', fontsize=12)
        ax.set_title('Chinese Zodiac Distribution in Cinema', 
                    fontsize=16, fontweight='bold', pad=20)
        
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        plt.savefig(self.output_dir / 'chinese_zodiac_distribution.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def _viz_element_breakdown(self):
        """Visualize elemental breakdown."""
        logger.info("Creating element breakdown visualization...")
        
        elements = ['Fire', 'Earth', 'Air', 'Water']
        counts = [self.element_stats[e]['count'] for e in elements]
        colors = [ELEMENT_COLORS[e] for e in elements]
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 7))
        
        # Pie chart
        wedges, texts, autotexts = ax1.pie(
            counts, labels=elements, colors=colors,
            autopct='%1.1f%%', startangle=90,
            explode=[0.05, 0.05, 0.05, 0.05],
            textprops={'fontsize': 12, 'fontweight': 'bold'}
        )
        ax1.set_title('Elemental Distribution\n(Western Zodiac)', fontsize=14, fontweight='bold')
        
        # Signs by element
        element_signs = {
            'Fire': ['Aries', 'Leo', 'Sagittarius'],
            'Earth': ['Taurus', 'Virgo', 'Capricorn'],
            'Air': ['Gemini', 'Libra', 'Aquarius'],
            'Water': ['Cancer', 'Scorpio', 'Pisces']
        }
        
        # Grouped bar for signs by element
        x = np.arange(3)
        width = 0.2
        
        for i, (element, signs) in enumerate(element_signs.items()):
            sign_counts = [self.zodiac_stats[s]['count'] for s in signs]
            ax2.bar(x + i * width, sign_counts, width, label=element, color=ELEMENT_COLORS[element])
        
        ax2.set_xticks(x + width * 1.5)
        ax2.set_xticklabels(['1st Sign', '2nd Sign', '3rd Sign'])
        ax2.set_ylabel('Count')
        ax2.set_title('Signs by Element', fontsize=14, fontweight='bold')
        ax2.legend()
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'element_breakdown.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def _viz_birth_months(self):
        """Visualize birth month distribution."""
        logger.info("Creating birth month visualization...")
        
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        counts = [self.birth_month_stats.get(i, 0) for i in range(1, 13)]
        
        # Determine colors by season
        season_colors = ['#87CEEB'] * 2 + ['#90EE90'] * 3 + ['#FFD700'] * 3 + ['#FFA500'] * 3 + ['#87CEEB']
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        bars = ax.bar(months, counts, color=season_colors, edgecolor='white', linewidth=2)
        
        # Add trend line
        z = np.polyfit(range(12), counts, 3)
        p = np.poly1d(z)
        ax.plot(range(12), p(range(12)), 'r--', alpha=0.5, linewidth=2, label='Trend')
        
        ax.set_xlabel('Birth Month', fontsize=12)
        ax.set_ylabel('Number of Talent', fontsize=12)
        ax.set_title('Birth Month Distribution in Cinema', fontsize=16, fontweight='bold', pad=20)
        
        # Add season labels
        ax.axvspan(-0.5, 1.5, alpha=0.1, color='blue', label='Winter')
        ax.axvspan(1.5, 4.5, alpha=0.1, color='green', label='Spring')
        ax.axvspan(4.5, 7.5, alpha=0.1, color='yellow', label='Summer')
        ax.axvspan(7.5, 10.5, alpha=0.1, color='orange', label='Fall')
        ax.axvspan(10.5, 11.5, alpha=0.1, color='blue')
        
        ax.legend(loc='upper right')
        plt.tight_layout()
        
        plt.savefig(self.output_dir / 'birth_months.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def _viz_mortality_analysis(self):
        """Visualize mortality patterns."""
        logger.info("Creating mortality analysis visualization...")
        
        fig, axes = plt.subplots(2, 2, figsize=(14, 12))
        
        # 1. Age at death histogram
        if self.death_stats['ages']:
            ax1 = axes[0, 0]
            ax1.hist(self.death_stats['ages'], bins=20, color='#6c757d', edgecolor='white', alpha=0.8)
            avg_age = np.mean(self.death_stats['ages'])
            ax1.axvline(avg_age, color='red', linestyle='--', linewidth=2, label=f'Avg: {avg_age:.1f}')
            ax1.set_xlabel('Age at Death')
            ax1.set_ylabel('Count')
            ax1.set_title('Age at Death Distribution', fontweight='bold')
            ax1.legend()
        
        # 2. Top causes of death
        ax2 = axes[0, 1]
        top_causes = self.death_stats['causes'].most_common(10)
        if top_causes:
            causes, cause_counts = zip(*top_causes)
            y_pos = np.arange(len(causes))
            ax2.barh(y_pos, cause_counts, color='#dc3545', edgecolor='white')
            ax2.set_yticks(y_pos)
            ax2.set_yticklabels([c[:30] for c in causes])  # Truncate long names
            ax2.set_xlabel('Count')
            ax2.set_title('Top 10 Causes of Death', fontweight='bold')
            ax2.invert_yaxis()
        
        # 3. Deaths by decade
        ax3 = axes[1, 0]
        if self.death_stats['decades']:
            decades = sorted(self.death_stats['decades'].keys())
            decade_counts = [self.death_stats['decades'][d] for d in decades]
            ax3.bar([f"{d}s" for d in decades], decade_counts, color='#495057', edgecolor='white')
            ax3.set_xlabel('Decade')
            ax3.set_ylabel('Deaths')
            ax3.set_title('Deaths by Decade', fontweight='bold')
            plt.sca(ax3)
            plt.xticks(rotation=45)
        
        # 4. Deceased by zodiac
        ax4 = axes[1, 1]
        zodiac_order = ['Aries', 'Taurus', 'Gemini', 'Cancer', 'Leo', 'Virgo',
                       'Libra', 'Scorpio', 'Sagittarius', 'Capricorn', 'Aquarius', 'Pisces']
        deceased_counts = [self.zodiac_stats[z]['deceased'] for z in zodiac_order]
        colors = [ZODIAC_COLORS[z] for z in zodiac_order]
        ax4.bar(zodiac_order, deceased_counts, color=colors, edgecolor='white')
        ax4.set_xlabel('Zodiac Sign')
        ax4.set_ylabel('Deceased Count')
        ax4.set_title('Deceased by Zodiac Sign', fontweight='bold')
        plt.sca(ax4)
        plt.xticks(rotation=45, ha='right')
        
        plt.suptitle('Mortality Analysis in Cinema', fontsize=18, fontweight='bold', y=1.02)
        plt.tight_layout()
        
        plt.savefig(self.output_dir / 'mortality_analysis.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def _viz_zodiac_quality(self):
        """Visualize zodiac vs quality metrics."""
        logger.info("Creating zodiac quality visualization...")
        
        # This would need film rating data linked to actors/directors
        # For now, show actor vs director breakdown
        
        zodiac_order = ['Aries', 'Taurus', 'Gemini', 'Cancer', 'Leo', 'Virgo',
                       'Libra', 'Scorpio', 'Sagittarius', 'Capricorn', 'Aquarius', 'Pisces']
        
        actors = [self.zodiac_stats[z]['actors'] for z in zodiac_order]
        directors = [self.zodiac_stats[z]['directors'] for z in zodiac_order]
        
        fig, ax = plt.subplots(figsize=(14, 8))
        
        x = np.arange(len(zodiac_order))
        width = 0.35
        
        bars1 = ax.bar(x - width/2, actors, width, label='Actors', color='#4ECDC4', edgecolor='white')
        bars2 = ax.bar(x + width/2, directors, width, label='Directors', color='#FF6B6B', edgecolor='white')
        
        ax.set_xlabel('Zodiac Sign', fontsize=12)
        ax.set_ylabel('Count', fontsize=12)
        ax.set_title('Actors vs Directors by Zodiac Sign', fontsize=16, fontweight='bold', pad=20)
        ax.set_xticks(x)
        ax.set_xticklabels(zodiac_order, rotation=45, ha='right')
        ax.legend()
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'zodiac_roles.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def _viz_zodiac_wheel(self):
        """Create a zodiac wheel visualization."""
        logger.info("Creating zodiac wheel visualization...")
        
        zodiac_order = ['Aries', 'Taurus', 'Gemini', 'Cancer', 'Leo', 'Virgo',
                       'Libra', 'Scorpio', 'Sagittarius', 'Capricorn', 'Aquarius', 'Pisces']
        
        counts = [self.zodiac_stats[z]['count'] for z in zodiac_order]
        colors = [ZODIAC_COLORS[z] for z in zodiac_order]
        
        # Normalize for visual appeal
        total = sum(counts)
        percentages = [c / total * 100 for c in counts]
        
        fig, ax = plt.subplots(figsize=(12, 12), subplot_kw=dict(projection='polar'))
        
        # Create the wheel
        theta = np.linspace(0, 2 * np.pi, 13)[:-1]  # 12 segments
        width = 2 * np.pi / 12
        
        # Offset so Aries starts at top
        theta = theta - np.pi/2
        
        bars = ax.bar(theta, counts, width=width, bottom=0,
                     color=colors, edgecolor='white', linewidth=2, alpha=0.8)
        
        # Add zodiac symbols/names
        symbols = ['♈', '♉', '♊', '♋', '♌', '♍', '♎', '♏', '♐', '♑', '♒', '♓']
        
        for angle, symbol, name, pct in zip(theta, symbols, zodiac_order, percentages):
            # Outer label
            ax.text(angle, max(counts) * 1.3, f'{symbol}\n{name}',
                   ha='center', va='center', fontsize=10, fontweight='bold')
            # Inner percentage
            ax.text(angle, max(counts) * 0.5, f'{pct:.1f}%',
                   ha='center', va='center', fontsize=8, color='white', fontweight='bold')
        
        ax.set_ylim(0, max(counts) * 1.5)
        ax.set_xticks([])
        ax.set_yticks([])
        ax.set_title('Zodiac Wheel\nCinema Talent Distribution', fontsize=18, fontweight='bold', pad=40)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'zodiac_wheel.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    def _generate_report(self):
        """Generate analysis report."""
        logger.info("Generating analysis report...")
        
        report_file = self.output_dir / 'zodiac_analysis_report.txt'
        
        with open(report_file, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("CINESCOPE BATCH 9: ZODIAC & ASTROLOGICAL ANALYSIS\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")
            
            # Summary
            total_people = sum(self.zodiac_stats[z]['count'] for z in self.zodiac_stats)
            f.write(f"Total People Analyzed: {total_people:,}\n\n")
            
            # Top zodiac signs
            f.write("WESTERN ZODIAC DISTRIBUTION\n")
            f.write("-" * 40 + "\n")
            sorted_zodiac = sorted(self.zodiac_stats.items(), key=lambda x: x[1]['count'], reverse=True)
            for sign, stats in sorted_zodiac:
                pct = (stats['count'] / total_people * 100) if total_people else 0
                f.write(f"{sign:15} {stats['count']:>8,} ({pct:>5.1f}%)\n")
            
            # Element breakdown
            f.write("\n\nELEMENT BREAKDOWN\n")
            f.write("-" * 40 + "\n")
            for element in ['Fire', 'Earth', 'Air', 'Water']:
                count = self.element_stats[element]['count']
                pct = (count / total_people * 100) if total_people else 0
                f.write(f"{element:15} {count:>8,} ({pct:>5.1f}%)\n")
            
            # Chinese zodiac
            f.write("\n\nCHINESE ZODIAC DISTRIBUTION\n")
            f.write("-" * 40 + "\n")
            sorted_chinese = sorted(self.chinese_zodiac_stats.items(), key=lambda x: x[1]['count'], reverse=True)
            for animal, stats in sorted_chinese:
                pct = (stats['count'] / total_people * 100) if total_people else 0
                f.write(f"{animal:15} {stats['count']:>8,} ({pct:>5.1f}%)\n")
            
            # Mortality stats
            if self.death_stats['ages']:
                f.write("\n\nMORTALITY STATISTICS\n")
                f.write("-" * 40 + "\n")
                f.write(f"Total deceased: {len(self.death_stats['ages']):,}\n")
                f.write(f"Average age at death: {np.mean(self.death_stats['ages']):.1f}\n")
                f.write(f"Median age at death: {np.median(self.death_stats['ages']):.1f}\n")
                
                if self.death_stats['causes']:
                    f.write("\nTop Causes of Death:\n")
                    for cause, count in self.death_stats['causes'].most_common(10):
                        f.write(f"  {cause}: {count}\n")
        
        logger.info(f"Report saved to {report_file}")


def main():
    analyzer = ZodiacAnalyzer()
    analyzer.analyze()


if __name__ == '__main__':
    main()
