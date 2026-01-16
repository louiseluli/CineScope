"""
CineScope Batch 6: PRODUCTION NETWORKS & AWARDS RECOGNITION
===========================================================

Comprehensive Production and Awards Analysis covering Questions 271-370:

PRODUCTION NETWORKS (Q271-280):
- Q271: Which studios dominate my collection?
- Q272: Do I follow production companies?
- Q273: Which producers appear across films?
- Q274: Do I watch certain cinematographers?
- Q275: Which editors cut films I love?
- Q276: Do costume designers influence my viewing?
- Q277: Which composers' work do I follow?
- Q278: Do I watch based on production design?
- Q279: Which special effects teams do I appreciate?
- Q280: Do I follow technical innovators?

AWARDS & RECOGNITION (Q361-370):
- Q361: How many Oscar Best Pictures have I seen?
- Q362: Do I watch nominees before ceremonies?
- Q363: Which Best Actor winners have I watched?
- Q364: Do I follow Golden Globe winners?
- Q365: Which Cannes winners appear?
- Q366: Do I watch BAFTA winners?
- Q367: Which indie awards do I follow?
- Q368: Do awards influence my viewing?
- Q369: Which snubs do I agree with?
- Q370: Do I watch "for your consideration" campaigns?

14 Professional Visualizations
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from pathlib import Path
import logging
from collections import Counter, defaultdict
import re
from typing import Dict, List, Tuple
import warnings
warnings.filterwarnings('ignore')

# Setup
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "processed"
OUTPUT_DIR = BASE_DIR / "analysis_outputs" / "visualizations" / "batch_6"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Professional color palette
COLORS = {
    'primary': '#2C3E50',
    'secondary': '#E74C3C',
    'accent': '#3498DB',
    'success': '#27AE60',
    'warning': '#F39C12',
    'info': '#9B59B6',
    'teal': '#1ABC9C',
    'gradient': ['#3498DB', '#9B59B6', '#E74C3C', '#F39C12', '#27AE60', '#1ABC9C']
}

# High-quality output settings
plt.rcParams['figure.dpi'] = 300
plt.rcParams['savefig.dpi'] = 300
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica']


class ProductionAwardsAnalyzer:
    """Comprehensive production networks and awards analysis."""
    
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.production_data = self._process_production_credits()
        self.awards_data = self._process_awards()
        logger.info(f"Analyzing {len(self.df)} films")
        logger.info(f"Found {len(self.production_data.get('production_companies', []))} production company mentions")
        logger.info(f"Found {len(self.awards_data)} films with awards data")
    
    def _parse_pipe_separated(self, value: str) -> List[str]:
        """Parse pipe-separated values."""
        if pd.isna(value):
            return []
        value = str(value).strip()
        if not value or value.lower() in ['unknown', 'n/a', 'none']:
            return []
        # Handle both pipe and comma separation
        if '|' in value:
            items = [x.strip() for x in value.split('|') if x.strip()]
        else:
            items = [x.strip() for x in value.split(',') if x.strip()]
        return items
    
    def _process_production_credits(self) -> Dict:
        """Extract all production-related credits."""
        production_credits = {
            'studios': [],
            'producers': [],
            'cinematographers': [],
            'composers': [],
            'editors': [],
            'production_companies': []
        }
        
        for _, film in self.df.iterrows():
            film_data = {
                'title': film.get('title', film.get('Title', 'Unknown')),
                'year': film.get('year', film.get('Year')),
                'rating': float(film.get('imdb_rating', film.get('IMDb Rating', 0)))
            }
            
            # Production companies (TMDB, OMDb, Wikidata)
            companies = []
            for col in ['tmdb_production_companies', 'omdb_production_co', 'wd_production_companies']:
                companies.extend(self._parse_pipe_separated(film.get(col, '')))
            
            for company in companies:
                production_credits['production_companies'].append({
                    **film_data,
                    'company': company
                })
            
            # Producers (TMDB, Wikidata)
            producers = []
            for col in ['tmdb_producers', 'wd_producers']:
                producers.extend(self._parse_pipe_separated(film.get(col, '')))
            
            for producer in producers:
                production_credits['producers'].append({
                    **film_data,
                    'producer': producer
                })
            
            # Cinematographers (TMDB, Wikidata)
            cinematographers = []
            for col in ['tmdb_cinematographers', 'wd_cinematographers']:
                cinematographers.extend(self._parse_pipe_separated(film.get(col, '')))
            
            for dp in cinematographers:
                production_credits['cinematographers'].append({
                    **film_data,
                    'cinematographer': dp
                })
            
            # Composers (TMDB, Wikidata)
            composers = []
            for col in ['tmdb_composers', 'wd_composers']:
                composers.extend(self._parse_pipe_separated(film.get(col, '')))
            
            for composer in composers:
                production_credits['composers'].append({
                    **film_data,
                    'composer': composer
                })
            
            # Editors (TMDB, Wikidata)
            editors = []
            for col in ['tmdb_editors', 'wd_editors']:
                editors.extend(self._parse_pipe_separated(film.get(col, '')))
            
            for editor in editors:
                production_credits['editors'].append({
                    **film_data,
                    'editor': editor
                })
        
        return production_credits
    
    def _process_awards(self) -> pd.DataFrame:
        """Extract and parse awards information."""
        awards_records = []
        
        # Oscar Best Picture winners by RELEASE YEAR (not ceremony year)
        # Key = Film Release Year, Value = Film Title
        oscar_bp_winners = {
            2024: "Anora",
            2023: "Oppenheimer",
            2022: "Everything Everywhere All at Once",
            2021: "CODA",
            2020: "Nomadland",
            2019: "Parasite",
            2018: "Green Book",
            2017: "The Shape of Water",
            2016: "Moonlight",
            2015: "Spotlight",
            2014: "Birdman",
            2013: "12 Years a Slave",
            2012: "Argo",
            2011: "The Artist",
            2010: "The King's Speech",
            2009: "The Hurt Locker",
            2008: "Slumdog Millionaire",
            2007: "No Country for Old Men",
            2006: "The Departed",
            2005: "Crash",
            2004: "Million Dollar Baby",
            2003: "The Lord of the Rings: The Return of the King",
            2002: "Chicago",
            2001: "A Beautiful Mind",
            2000: "Gladiator",
            1999: "American Beauty",
            1998: "Shakespeare in Love",
            1997: "Titanic",
            1996: "The English Patient",
            1995: "Braveheart",
            1994: "Forrest Gump",
            1993: "Schindler's List",
            1992: "Unforgiven",
            1991: "The Silence of the Lambs",
            1990: "Dances with Wolves",
            1989: "Driving Miss Daisy",
            1988: "Rain Man",
            1987: "The Last Emperor",
            1986: "Platoon",
            1985: "Out Of Africa",
            1984: "Amadeus",
            1983: "Terms Of Endearment",
            1982: "Gandhi",
            1981: "Chariots of Fire",
            1980: "Ordinary People",
            1979: "Kramer Vs. Kramer",
            1978: "The Deer Hunter",
            1977: "Annie Hall",
            1976: "Rocky",
            1975: "One Flew Over The Cuckoo’s Nest",
            1974: "The Godfather: Part II",
            1973: "The Sting",
            1972: "The Godfather",
            1971: "The French Connection",
            1970: "Patton",
            1969: "Midnight Cowboy",
            1968: "Oliver!",
            1967: "In The Heat of The Night",
            1966: "A Man For All Seasons",
            1965: "The Sound Of Music",
            1964: "My Fair Lady",
            1963: "Tom Jones",
            1962: "Lawrence of Arabia",
            1961: "West Side Story",
            1960: "The Apartment",
            1959: "Ben-Hur",
            1958: "Gigi",
            1957: "The Bridge On The River Kwai",
            1956: "Around The World In 80 Days",
            1955: "Marty",
            1954: "On The Waterfront",
            1953: "From Here To Eternity",
            1952: "The Greatest Show On Earth",
            1951: "An American In Paris",
            1950: "All About Eve",
            1949: "All The King’s Men",
            1948: "Hamlet",
            1947: "Gentleman’s Agreement",
            1946: "The Best Years Of Our Lives",
            1945: "The Lost Weekend",
            1944: "Going My Way",
            1943: "Casablanca",
            1942: "Mrs. Miniver",
            1941: "How Green Was My Valley",
            1940: "Rebecca",
            1939: "Gone With The Wind",
            1938: "You Can’t Take It With You",
            1937: "The Life Of Emile Zola",
            1936: "The Great Ziegfeld",
            1935: "Mutiny On The Bounty",
            1934: "It Happened One Night",
            1933: "Cavalcade",
            1932: "Grandhotel",
            1931: "Cimarron",
            1930: "All Quiet On The Western Front",
            1929: "The Broadway Melody",
            1927: "Wings"
        }
        
        for _, film in self.df.iterrows():
            title = film.get('title', film.get('Title', 'Unknown'))
            year = film.get('year', film.get('Year'))
            rating = float(film.get('imdb_rating', film.get('IMDb Rating', 0)))
            
            record = {
                'title': title,
                'year': year,
                'rating': rating,
                'has_awards': False,
                'oscar_bp': False,
                'oscar_winner': False,
                'oscar_nominee': False,
                'awards_count': 0,
                'nominations_count': 0,
                'awards_text': ''
            }
            
            # Check Oscar Best Picture
            if pd.notna(year) and title:
                try:
                    year_int = int(float(year))  # Handle cases like 2022.0 or "2022"
                    bp_winner = oscar_bp_winners.get(year_int)
                    if bp_winner and bp_winner.lower() in str(title).lower():
                        record['oscar_bp'] = True
                        record['has_awards'] = True
                        record['awards_count'] += 1
                except (ValueError, TypeError):
                    pass # Skip if year is malformed
                if bp_winner and bp_winner.lower() in title.lower():
                    record['oscar_bp'] = True
                    record['has_awards'] = True
                    record['awards_count'] += 1
            
            # OMDb awards parsing
            omdb_awards = film.get('omdb_awards', '')
            if pd.notna(omdb_awards) and str(omdb_awards).strip():
                awards_text = str(omdb_awards).lower()
                record['awards_text'] = str(omdb_awards)
                record['has_awards'] = True
                
                # Parse wins and nominations
                if 'won' in awards_text:
                    wins_match = re.search(r'won (\d+)', awards_text)
                    if wins_match:
                        record['awards_count'] = int(wins_match.group(1))
                        if 'oscar' in awards_text:
                            record['oscar_winner'] = True
                
                if 'nominated' in awards_text or 'nomination' in awards_text:
                    nom_match = re.search(r'(\d+) nomination', awards_text)
                    if nom_match:
                        record['nominations_count'] = int(nom_match.group(1))
                        if 'oscar' in awards_text:
                            record['oscar_nominee'] = True
            
            # Wikidata awards
            wd_awards = film.get('wd_awards', '')
            wd_noms = film.get('wd_nominations', '')
            
            if pd.notna(wd_awards) and str(wd_awards).strip():
                awards_list = self._parse_pipe_separated(wd_awards)
                if awards_list:
                    record['has_awards'] = True
                    record['awards_count'] += len(awards_list)
                    
                    # Check for specific awards
                    awards_lower = str(wd_awards).lower()
                    if 'academy award' in awards_lower or 'oscar' in awards_lower:
                        record['oscar_winner'] = True
            
            if pd.notna(wd_noms) and str(wd_noms).strip():
                noms_list = self._parse_pipe_separated(wd_noms)
                if noms_list:
                    record['nominations_count'] += len(noms_list)
                    noms_lower = str(wd_noms).lower()
                    if 'academy award' in noms_lower or 'oscar' in noms_lower:
                        record['oscar_nominee'] = True
            
            awards_records.append(record)
        
        return pd.DataFrame(awards_records)
    
    def get_top_studios(self, n: int = 20) -> pd.DataFrame:
        """Get top production companies by film count."""
        companies = pd.DataFrame(self.production_data['production_companies'])
        if companies.empty:
            return pd.DataFrame()
        
        company_stats = companies.groupby('company').agg({
            'title': 'count',
            'rating': 'mean'
        }).reset_index()
        company_stats.columns = ['company', 'film_count', 'avg_rating']
        company_stats = company_stats.sort_values('film_count', ascending=False).head(n)
        
        return company_stats
    
    def get_top_crew(self, role: str, n: int = 15) -> pd.DataFrame:
        """Get top crew members by role."""
        crew_data = pd.DataFrame(self.production_data[role])
        if crew_data.empty:
            return pd.DataFrame()
        
        role_singular = role.rstrip('s')  # Remove trailing 's'
        crew_stats = crew_data.groupby(role_singular).agg({
            'title': 'count',
            'rating': 'mean'
        }).reset_index()
        crew_stats.columns = [role_singular, 'film_count', 'avg_rating']
        crew_stats = crew_stats.sort_values('film_count', ascending=False).head(n)
        
        return crew_stats
    
    def get_awards_summary(self) -> Dict:
        """Get comprehensive awards statistics."""
        return {
            'total_films': len(self.awards_data),
            'films_with_awards': self.awards_data['has_awards'].sum(),
            'oscar_bp_winners': self.awards_data['oscar_bp'].sum(),
            'oscar_winners': self.awards_data['oscar_winner'].sum(),
            'oscar_nominees': self.awards_data['oscar_nominee'].sum(),
            'total_awards': self.awards_data['awards_count'].sum(),
            'total_nominations': self.awards_data['nominations_count'].sum(),
            'avg_awards_per_film': self.awards_data['awards_count'].mean(),
            'pct_with_awards': (self.awards_data['has_awards'].sum() / len(self.awards_data) * 100)
        }


# =============================================================================
# PRODUCTION NETWORK VISUALIZATIONS
# =============================================================================

def viz_1_studio_leaderboard(analyzer: ProductionAwardsAnalyzer):
    """Top 20 production companies."""
    logger.info("Creating Viz 1: Studio Leaderboard...")
    
    studios = analyzer.get_top_studios(20)
    
    if studios.empty:
        logger.warning("No studio data available")
        return
    
    fig, ax = plt.subplots(figsize=(12, 10))
    
    # Plot horizontal bars
    bars = ax.barh(studios['company'], studios['film_count'], 
                   color=COLORS['accent'], alpha=0.8)
    
    # Add rating indicators
    for idx, (company, count, rating) in enumerate(zip(
        studios['company'], studios['film_count'], studios['avg_rating']
    )):
        ax.text(count + 0.5, idx, f"Score: {rating:.1f}", 
              va='center', fontsize=9, color=COLORS['primary'])
    
    # IMPROVEMENT: Invert y-axis so the #1 studio is at the top, not bottom
    ax.invert_yaxis()
    
    ax.set_xlabel('Number of Films', fontsize=12, fontweight='bold')
    ax.set_title('Top 20 Production Companies in My Collection', 
                fontsize=14, fontweight='bold', pad=20)
    
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    ax.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    
    # Safety check: Ensure directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    plt.savefig(OUTPUT_DIR / "01_studio_leaderboard.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 1 complete")

def viz_2_producer_network(analyzer: ProductionAwardsAnalyzer):
    """Top producers and their impact."""
    logger.info("Creating Viz 2: Producer Network...")
    
    producers = analyzer.get_top_crew('producers', 15)
    
    if producers.empty:
        logger.warning("No producer data available")
        return
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    # Film count
    ax1.barh(producers['producer'], producers['film_count'],
            color=COLORS['success'], alpha=0.8)
    ax1.set_xlabel('Films Produced', fontsize=11, fontweight='bold')
    ax1.set_title('Most Prolific Producers', fontsize=12, fontweight='bold')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.grid(axis='x', alpha=0.3)
    
    # Average rating
    colors_rating = [COLORS['success'] if r >= 7.0 else COLORS['warning'] 
                    for r in producers['avg_rating']]
    ax2.barh(producers['producer'], producers['avg_rating'],
            color=colors_rating, alpha=0.8)
    ax2.set_xlabel('Average IMDb Rating', fontsize=11, fontweight='bold')
    ax2.set_title('Producer Quality Score', fontsize=12, fontweight='bold')
    ax2.set_xlim(0, 10)
    ax2.axvline(x=7.0, color='red', linestyle='--', alpha=0.5, linewidth=1)
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    ax2.grid(axis='x', alpha=0.3)
    
    plt.suptitle('Top 15 Producers in My Collection', 
                fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "02_producer_network.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 2 complete")


def viz_3_cinematographer_excellence(analyzer: ProductionAwardsAnalyzer):
    """Top cinematographers (directors of photography)."""
    logger.info("Creating Viz 3: Cinematographer Excellence...")
    
    dps = analyzer.get_top_crew('cinematographers', 12)
    
    if dps.empty:
        logger.warning("No cinematographer data available")
        return
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Scatter plot: film count vs rating
    scatter = ax.scatter(dps['film_count'], dps['avg_rating'],
                        s=dps['film_count']*50, 
                        c=dps['avg_rating'], cmap='RdYlGn',
                        alpha=0.7, edgecolors='black', linewidth=1.5,
                        vmin=6, vmax=8.5)
    
    # Labels
    for _, row in dps.iterrows():
        ax.annotate(row['cinematographer'], 
                   (row['film_count'], row['avg_rating']),
                   xytext=(5, 5), textcoords='offset points',
                   fontsize=9, alpha=0.8)
    
    ax.set_xlabel('Number of Films', fontsize=12, fontweight='bold')
    ax.set_ylabel('Average IMDb Rating', fontsize=12, fontweight='bold')
    ax.set_title('Top Cinematographers: Prolific vs. Acclaimed', 
                fontsize=14, fontweight='bold', pad=20)
    ax.grid(alpha=0.3)
    
    # Colorbar
    cbar = plt.colorbar(scatter, ax=ax)
    cbar.set_label('Quality Score', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "03_cinematographer_excellence.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 3 complete")


def viz_4_composer_leaderboard(analyzer: ProductionAwardsAnalyzer):
    """Top film composers."""
    logger.info("Creating Viz 4: Composer Leaderboard...")
    
    composers = analyzer.get_top_crew('composers', 15)
    
    if composers.empty:
        logger.warning("No composer data available")
        return
    
    fig, ax = plt.subplots(figsize=(12, 10))
    
    # Create gradient colors based on rating
    colors = plt.cm.viridis(composers['avg_rating'] / 10)
    
    bars = ax.barh(composers['composer'], composers['film_count'],
                  color=colors, alpha=0.8, edgecolor='black', linewidth=0.5)
    
    # Add counts and ratings
    for idx, (composer, count, rating) in enumerate(zip(
        composers['composer'], composers['film_count'], composers['avg_rating']
    )):
        ax.text(count + 0.3, idx, f"{int(count)} | {rating:.1f}",
               va='center', fontsize=9, fontweight='bold')
    
    ax.set_xlabel('Number of Film Scores', fontsize=12, fontweight='bold')
    ax.set_title('Top 15 Film Composers in My Collection',
                fontsize=14, fontweight='bold', pad=20)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "04_composer_leaderboard.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 4 complete")


def viz_5_editor_craftsmen(analyzer: ProductionAwardsAnalyzer):
    """Top film editors."""
    logger.info("Creating Viz 5: Editor Craftsmen...")
    
    editors = analyzer.get_top_crew('editors', 12)
    
    if editors.empty:
        logger.warning("No editor data available")
        return
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Lollipop chart
    ax.hlines(y=editors['editor'], xmin=0, xmax=editors['film_count'],
             color=COLORS['primary'], alpha=0.4, linewidth=2)
    ax.scatter(editors['film_count'], editors['editor'],
              s=editors['avg_rating']*80, c=editors['avg_rating'],
              cmap='coolwarm', alpha=0.8, edgecolors='black', linewidth=1.5,
              vmin=6, vmax=8.5, zorder=3)
    
    # Add labels
    for idx, (editor, count, rating) in enumerate(zip(
        editors['editor'], editors['film_count'], editors['avg_rating']
    )):
        ax.text(count + 0.3, idx, f"{rating:.1f}", fontsize=9, va='center')
    
    ax.set_xlabel('Number of Films Edited', fontsize=12, fontweight='bold')
    ax.set_title('Top Film Editors: The Invisible Storytellers',
                fontsize=14, fontweight='bold', pad=20)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "05_editor_craftsmen.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 5 complete")


def viz_6_production_network_heatmap(analyzer: ProductionAwardsAnalyzer):
    """Production crew collaboration heatmap."""
    logger.info("Creating Viz 6: Production Network Heatmap...")
    
    # Get top entities from each category
    top_studios = analyzer.get_top_studios(10)
    top_producers = analyzer.get_top_crew('producers', 10)
    top_composers = analyzer.get_top_crew('composers', 10)
    
    if top_studios.empty or top_producers.empty or top_composers.empty:
        logger.warning("Insufficient data for network heatmap")
        return
    
    fig, axes = plt.subplots(1, 3, figsize=(18, 8))
    
    categories = [
        (top_studios, 'Studios', 'company', 0),
        (top_producers, 'Producers', 'producer', 1),
        (top_composers, 'Composers', 'composer', 2)
    ]
    
    for data, title, col_name, idx in categories:
        ax = axes[idx]
        
        # Create data for heatmap
        sorted_data = data.sort_values('avg_rating', ascending=False)
        
        # Create 2D array for heatmap visualization
        matrix = np.zeros((len(sorted_data), 2))
        matrix[:, 0] = sorted_data['film_count'].values
        matrix[:, 1] = sorted_data['avg_rating'].values
        
        im = ax.imshow(matrix, cmap='YlGnBu', aspect='auto')
        
        ax.set_yticks(range(len(sorted_data)))
        ax.set_yticklabels(sorted_data[col_name], fontsize=9)
        ax.set_xticks([0, 1])
        ax.set_xticklabels(['Films', 'Rating'], fontsize=10)
        ax.set_title(f'Top 10 {title}', fontsize=11, fontweight='bold', pad=10)
        
        # Add text annotations
        for i in range(len(sorted_data)):
            for j in range(2):
                text = ax.text(j, i, f'{matrix[i, j]:.1f}',
                             ha="center", va="center", color="black", fontsize=8)
    
    plt.suptitle('Production Networks: Key Contributors',
                fontsize=14, fontweight='bold', y=0.98)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "06_production_network_heatmap.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 6 complete")


# =============================================================================
# AWARDS & RECOGNITION VISUALIZATIONS
# =============================================================================

def viz_7_awards_overview(analyzer: ProductionAwardsAnalyzer):
    """Awards collection overview."""
    logger.info("Creating Viz 7: Awards Overview...")
    
    summary = analyzer.get_awards_summary()
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(14, 10))
    
    # 1. Films with vs without awards
    awards_data = [
        summary['films_with_awards'],
        summary['total_films'] - summary['films_with_awards']
    ]
    colors = [COLORS['success'], COLORS['secondary']]
    explode = (0.05, 0)
    
    ax1.pie(awards_data, labels=['With Awards', 'Without Awards'],
           autopct='%1.1f%%', colors=colors, explode=explode,
           startangle=90, textprops={'fontsize': 11, 'fontweight': 'bold'})
    ax1.set_title('Films with Awards Recognition', fontsize=12, fontweight='bold', pad=15)
    
    # 2. Oscar categories
    oscar_cats = [
        summary['oscar_bp_winners'],
        summary['oscar_winners'] - summary['oscar_bp_winners'],
        summary['oscar_nominees']
    ]
    oscar_labels = ['Best Picture Winners', 'Other Oscar Winners', 'Oscar Nominees']
    oscar_colors = ['#FFD700', '#C0C0C0', '#CD7F32']
    
    ax2.bar(oscar_labels, oscar_cats, color=oscar_colors, alpha=0.8, edgecolor='black', linewidth=1.5)
    ax2.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
    ax2.set_title('Oscar Recognition Breakdown', fontsize=12, fontweight='bold', pad=15)
    ax2.tick_params(axis='x', rotation=15)
    ax2.grid(axis='y', alpha=0.3)
    
    # 3. Awards distribution
    awards_dist = analyzer.awards_data[analyzer.awards_data['awards_count'] > 0]['awards_count']
    if len(awards_dist) > 0:
        ax3.hist(awards_dist, bins=20, color=COLORS['accent'], alpha=0.7, edgecolor='black')
        ax3.set_xlabel('Number of Awards', fontsize=11, fontweight='bold')
        ax3.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
        ax3.set_title('Awards Distribution', fontsize=12, fontweight='bold', pad=15)
        ax3.axvline(awards_dist.mean(), color='red', linestyle='--', 
                   linewidth=2, label=f'Mean: {awards_dist.mean():.1f}')
        ax3.legend()
        ax3.grid(axis='y', alpha=0.3)
    
    # 4. Key statistics
    stats_text = f"""
    AWARDS STATISTICS
    
    Total Films: {summary['total_films']:,}
    Films with Awards: {summary['films_with_awards']:,}
    Coverage: {summary['pct_with_awards']:.1f}%
    
    Oscar Best Picture Winners: {summary['oscar_bp_winners']}
    Total Oscar Winners: {summary['oscar_winners']}
    Total Oscar Nominees: {summary['oscar_nominees']}
    
    Total Awards Won: {int(summary['total_awards']):,}
    Total Nominations: {int(summary['total_nominations']):,}
    Avg Awards/Film: {summary['avg_awards_per_film']:.2f}
    """
    
    ax4.text(0.1, 0.5, stats_text, fontsize=10, verticalalignment='center',
            fontfamily='monospace', bbox=dict(boxstyle='round', 
            facecolor=COLORS['teal'], alpha=0.2))
    ax4.axis('off')
    
    plt.suptitle('Awards & Recognition Overview', fontsize=15, fontweight='bold', y=0.98)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "07_awards_overview.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 7 complete")


def viz_8_oscar_timeline(analyzer: ProductionAwardsAnalyzer):
    """Oscar winners and nominees over time."""
    logger.info("Creating Viz 8: Oscar Timeline...")
    
    oscar_films = analyzer.awards_data[
        (analyzer.awards_data['oscar_bp']) | 
        (analyzer.awards_data['oscar_winner']) | 
        (analyzer.awards_data['oscar_nominee'])
    ].copy()
    
    if oscar_films.empty:
        logger.warning("No Oscar data available")
        return
    
    oscar_films['decade'] = (oscar_films['year'] // 10) * 10
    
    decade_stats = oscar_films.groupby('decade').agg({
        'oscar_bp': 'sum',
        'oscar_winner': 'sum',
        'oscar_nominee': 'sum'
    }).reset_index()
    
    fig, ax = plt.subplots(figsize=(14, 7))
    
    x = decade_stats['decade']
    width = 3
    
    ax.bar(x - width, decade_stats['oscar_bp'], width, 
          label='Best Picture Winners', color='#FFD700', alpha=0.9, edgecolor='black')
    ax.bar(x, decade_stats['oscar_winner'], width,
          label='Other Oscar Winners', color='#C0C0C0', alpha=0.9, edgecolor='black')
    ax.bar(x + width, decade_stats['oscar_nominee'], width,
          label='Oscar Nominees', color='#CD7F32', alpha=0.9, edgecolor='black')
    
    ax.set_xlabel('Decade', fontsize=12, fontweight='bold')
    ax.set_ylabel('Number of Films', fontsize=12, fontweight='bold')
    ax.set_title('Oscar Recognition Across Decades', fontsize=14, fontweight='bold', pad=20)
    ax.legend(fontsize=10)
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "08_oscar_timeline.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 8 complete")


def viz_9_awards_vs_rating(analyzer: ProductionAwardsAnalyzer):
    """Awards count vs IMDb rating correlation."""
    logger.info("Creating Viz 9: Awards vs Rating...")
    
    awarded_films = analyzer.awards_data[analyzer.awards_data['awards_count'] > 0].copy()
    
    if awarded_films.empty:
        logger.warning("No awarded films data")
        return
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))
    
    # Scatter: awards vs rating
    scatter = ax1.scatter(awarded_films['awards_count'], awarded_films['rating'],
                         s=50, alpha=0.6, c=awarded_films['rating'],
                         cmap='RdYlGn', vmin=5, vmax=9, edgecolors='black', linewidth=0.5)
    
    # Trend line
    z = np.polyfit(awarded_films['awards_count'], awarded_films['rating'], 1)
    p = np.poly1d(z)
    ax1.plot(awarded_films['awards_count'], p(awarded_films['awards_count']),
            "r--", alpha=0.8, linewidth=2, label=f'Trend: y={z[0]:.2f}x+{z[1]:.2f}')
    
    ax1.set_xlabel('Number of Awards Won', fontsize=12, fontweight='bold')
    ax1.set_ylabel('IMDb Rating', fontsize=12, fontweight='bold')
    ax1.set_title('Awards vs. Quality', fontsize=13, fontweight='bold', pad=15)
    ax1.legend()
    ax1.grid(alpha=0.3)
    
    cbar = plt.colorbar(scatter, ax=ax1)
    cbar.set_label('Rating', fontsize=10)
    
    # Boxplot: awards categories
    categories = []
    ratings = []
    
    for _, film in awarded_films.iterrows():
        if film['oscar_bp']:
            categories.append('Best Picture')
            ratings.append(film['rating'])
        elif film['oscar_winner']:
            categories.append('Oscar Winner')
            ratings.append(film['rating'])
        elif film['oscar_nominee']:
            categories.append('Oscar Nominee')
            ratings.append(film['rating'])
        elif film['awards_count'] >= 5:
            categories.append('5+ Awards')
            ratings.append(film['rating'])
        else:
            categories.append('Other Awards')
            ratings.append(film['rating'])
    
    data_df = pd.DataFrame({'category': categories, 'rating': ratings})
    cat_order = ['Best Picture', 'Oscar Winner', 'Oscar Nominee', '5+ Awards', 'Other Awards']
    
    box_data = [data_df[data_df['category'] == cat]['rating'].values 
               for cat in cat_order if cat in data_df['category'].values]
    box_labels = [cat for cat in cat_order if cat in data_df['category'].values]
    
    bp = ax2.boxplot(box_data, labels=box_labels, patch_artist=True)
    
    colors_box = ['#FFD700', '#C0C0C0', '#CD7F32', COLORS['accent'], COLORS['warning']]
    for patch, color in zip(bp['boxes'], colors_box[:len(bp['boxes'])]):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    
    ax2.set_ylabel('IMDb Rating', fontsize=12, fontweight='bold')
    ax2.set_title('Rating Distribution by Award Category', fontsize=13, fontweight='bold', pad=15)
    ax2.tick_params(axis='x', rotation=20)
    ax2.grid(axis='y', alpha=0.3)
    ax2.axhline(y=7.0, color='red', linestyle='--', alpha=0.5, linewidth=1)
    
    plt.suptitle('Awards and Quality Correlation', fontsize=15, fontweight='bold', y=0.98)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "09_awards_vs_rating.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 9 complete")


def viz_10_top_awarded_films(analyzer: ProductionAwardsAnalyzer):
    """Most awarded films in collection."""
    logger.info("Creating Viz 10: Top Awarded Films...")
    
    top_awarded = analyzer.awards_data.nlargest(20, 'awards_count')[
        ['title', 'year', 'awards_count', 'nominations_count', 'rating', 'oscar_bp']
    ]
    
    if top_awarded.empty:
        logger.warning("No awarded films data")
        return
    
    fig, ax = plt.subplots(figsize=(14, 10))
    
    # Create labels with year and ensure integer formatting
    labels = [f"{row['title']} ({int(row['year'])})" 
             for _, row in top_awarded.iterrows()]
    
    # Ensure integer display for awards and nominations
    top_awarded = top_awarded.copy()
    top_awarded['awards_count'] = top_awarded['awards_count'].astype(int)
    top_awarded['nominations_count'] = top_awarded['nominations_count'].astype(int)
    
    # Color bars based on Oscar BP status
    colors = [COLORS['success'] if bp else COLORS['accent'] 
             for bp in top_awarded['oscar_bp']]
    
    bars = ax.barh(labels, top_awarded['awards_count'], 
                  color=colors, alpha=0.8, edgecolor='black', linewidth=0.5)
    
    # Add nominations and ratings
    for idx, (title, awards, noms, rating, oscar) in enumerate(zip(
        labels, top_awarded['awards_count'], top_awarded['nominations_count'],
        top_awarded['rating'], top_awarded['oscar_bp']
    )):
        label_text = f"{int(awards)} wins, {int(noms)} noms | {rating:.1f}"
        if oscar:
            label_text += " [Oscar BP]"
        ax.text(awards + 0.5, idx, label_text, va='center', fontsize=8)
    
    ax.set_xlabel('Number of Awards Won', fontsize=12, fontweight='bold')
    ax.set_title('Top 20 Most Awarded Films in My Collection',
                fontsize=14, fontweight='bold', pad=20)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(axis='x', alpha=0.3)
    
    # Legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=COLORS['success'], label='Oscar Best Picture'),
        Patch(facecolor=COLORS['accent'], label='Other Awards')
    ]
    ax.legend(handles=legend_elements, loc='lower right')
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "10_top_awarded_films.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 10 complete")


# =============================================================================
# INTERACTIVE DASHBOARDS
# =============================================================================

def viz_11_interactive_production_dashboard(analyzer: ProductionAwardsAnalyzer):
    """Interactive production crew dashboard."""
    logger.info("Creating Viz 11: Interactive Production Dashboard...")
    
    # Get data for all crew types
    studios = analyzer.get_top_studios(25)
    producers = analyzer.get_top_crew('producers', 20)
    cinematographers = analyzer.get_top_crew('cinematographers', 15)
    composers = analyzer.get_top_crew('composers', 20)
    editors = analyzer.get_top_crew('editors', 15)
    
    # Create subplots
    fig = make_subplots(
        rows=3, cols=2,
        subplot_titles=('Production Companies', 'Producers', 
                       'Cinematographers', 'Composers', 
                       'Editors', 'Production Quality Overview'),
        specs=[[{'type': 'bar'}, {'type': 'bar'}],
               [{'type': 'scatter'}, {'type': 'bar'}],
               [{'type': 'bar'}, {'type': 'scatter'}]]
    )
    
    # 1. Studios
    if not studios.empty:
        fig.add_trace(
            go.Bar(x=studios['film_count'].astype(int), y=studios['company'],
                  orientation='h', name='Studios',
                  marker_color=COLORS['accent'], text=studios['avg_rating'].round(1),
                  textposition='outside', texttemplate='%{text}'),
            row=1, col=1
        )
    
    # 2. Producers
    if not producers.empty:
        fig.add_trace(
            go.Bar(x=producers['film_count'].astype(int), y=producers['producer'],
                  orientation='h', name='Producers',
                  marker_color=COLORS['success'], text=producers['avg_rating'].round(1),
                  textposition='outside', texttemplate='%{text}'),
            row=1, col=2
        )
    
    # 3. Cinematographers
    if not cinematographers.empty:
        fig.add_trace(
            go.Scatter(x=cinematographers['film_count'].astype(int), 
                      y=cinematographers['avg_rating'],
                      mode='markers+text',
                      text=cinematographers['cinematographer'],
                      textposition='top center',
                      marker=dict(size=cinematographers['film_count']*3,
                                color=cinematographers['avg_rating'],
                                colorscale='Viridis', showscale=True,
                                line=dict(width=1, color='black')),
                      name='Cinematographers'),
            row=2, col=1
        )
    
    # 4. Composers
    if not composers.empty:
        fig.add_trace(
            go.Bar(x=composers['film_count'].astype(int), y=composers['composer'],
                  orientation='h', name='Composers',
                  marker_color=COLORS['info'], text=composers['avg_rating'].round(1),
                  textposition='outside', texttemplate='%{text}'),
            row=2, col=2
        )
    
    # 5. Editors
    if not editors.empty:
        fig.add_trace(
            go.Bar(x=editors['film_count'].astype(int), y=editors['editor'],
                  orientation='h', name='Editors',
                  marker_color=COLORS['warning'], text=editors['avg_rating'].round(1),
                  textposition='outside', texttemplate='%{text}'),
            row=3, col=1
        )
    
    # 6. Quality overview scatter
    all_crew = []
    for crew_type, data in [
        ('Studios', studios.rename(columns={'company': 'name'})),
        ('Producers', producers.rename(columns={'producer': 'name'})),
        ('Composers', composers.rename(columns={'composer': 'name'}))
    ]:
        if not data.empty:
            data_copy = data.copy()
            data_copy['type'] = crew_type
            all_crew.append(data_copy)
    
    if all_crew:
        combined = pd.concat(all_crew, ignore_index=True)
        
        for crew_type in combined['type'].unique():
            subset = combined[combined['type'] == crew_type]
            fig.add_trace(
                go.Scatter(x=subset['film_count'].astype(int), y=subset['avg_rating'],
                          mode='markers', name=crew_type,
                          marker=dict(size=10, line=dict(width=1, color='black'))),
                row=3, col=2
            )
    
    # Update layout
    fig.update_layout(
        height=1200,
        title_text="Production Networks: Key Contributors Dashboard",
        title_font_size=18,
        showlegend=True,
        template='plotly_white'
    )
    
    # Update axes
    fig.update_xaxes(title_text="Films", row=1, col=1)
    fig.update_xaxes(title_text="Films", row=1, col=2)
    fig.update_xaxes(title_text="Films", row=2, col=1)
    fig.update_yaxes(title_text="Rating", row=2, col=1)
    fig.update_xaxes(title_text="Films", row=2, col=2)
    fig.update_xaxes(title_text="Films", row=3, col=1)
    fig.update_xaxes(title_text="Films", row=3, col=2)
    fig.update_yaxes(title_text="Rating", row=3, col=2)
    
    fig.write_html(OUTPUT_DIR / "11_interactive_production_dashboard.html")
    logger.info("✅ Viz 11 complete")


def viz_12_interactive_awards_explorer(analyzer: ProductionAwardsAnalyzer):
    """Interactive awards explorer."""
    logger.info("Creating Viz 12: Interactive Awards Explorer...")
    
    awarded_films = analyzer.awards_data[analyzer.awards_data['has_awards']].copy()
    
    if awarded_films.empty:
        logger.warning("No awards data for interactive visualization")
        return
    
    # Add decade
    awarded_films['decade'] = (awarded_films['year'] // 10) * 10
    
    # Create award categories
    def categorize_awards(row):
        if row['oscar_bp']:
            return 'Oscar Best Picture'
        elif row['oscar_winner']:
            return 'Oscar Winner'
        elif row['oscar_nominee']:
            return 'Oscar Nominee'
        elif row['awards_count'] >= 10:
            return '10+ Awards'
        elif row['awards_count'] >= 5:
            return '5-9 Awards'
        else:
            return '1-4 Awards'
    
    awarded_films['category'] = awarded_films.apply(categorize_awards, axis=1)
    
    # Create figure
    fig = go.Figure()
    
    # Add traces for each category
    categories = ['Oscar Best Picture', 'Oscar Winner', 'Oscar Nominee', 
                 '10+ Awards', '5-9 Awards', '1-4 Awards']
    colors_map = {
        'Oscar Best Picture': '#FFD700',
        'Oscar Winner': '#C0C0C0',
        'Oscar Nominee': '#CD7F32',
        '10+ Awards': COLORS['success'],
        '5-9 Awards': COLORS['accent'],
        '1-4 Awards': COLORS['warning']
    }
    
    for category in categories:
        subset = awarded_films[awarded_films['category'] == category]
        if not subset.empty:
            fig.add_trace(go.Scatter(
                x=subset['year'],
                y=subset['rating'],
                mode='markers',
                name=category,
                text=subset.apply(lambda row: 
                    f"{row['title']}<br>Year: {int(row['year'])}<br>"
                    f"Awards: {int(row['awards_count'])}<br>"
                    f"Nominations: {int(row['nominations_count'])}<br>"
                    f"Rating: {row['rating']:.1f}", axis=1),
                marker=dict(
                    size=subset['awards_count']*2 + 5,
                    color=colors_map[category],
                    line=dict(width=1, color='black'),
                    opacity=0.7
                ),
                hovertemplate='%{text}<extra></extra>'
            ))
    
    fig.update_layout(
        title="Awards Explorer: Recognition Over Time",
        xaxis_title="Year",
        yaxis_title="IMDb Rating",
        height=700,
        template='plotly_white',
        hovermode='closest',
        legend=dict(
            orientation="v",
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        )
    )
    
    fig.write_html(OUTPUT_DIR / "12_interactive_awards_explorer.html")
    logger.info("✅ Viz 12 complete")


def viz_13_crew_collaboration_network(analyzer: ProductionAwardsAnalyzer):
    """Crew collaboration network (simplified)."""
    logger.info("Creating Viz 13: Crew Collaboration Network...")
    
    # Get top entities
    top_studios = analyzer.get_top_studios(10)
    top_producers = analyzer.get_top_crew('producers', 10)
    top_composers = analyzer.get_top_crew('composers', 10)
    
    if top_studios.empty or top_producers.empty or top_composers.empty:
        logger.warning("Insufficient data for collaboration network")
        return
    
    fig = go.Figure()
    
    # Create positions for network layout
    studio_x = [0] * len(top_studios)
    studio_y = list(range(len(top_studios)))
    producer_x = [1] * len(top_producers)
    producer_y = list(range(len(top_producers)))
    composer_x = [2] * len(top_composers)
    composer_y = list(range(len(top_composers)))
    
    # Add nodes
    fig.add_trace(go.Scatter(
        x=studio_x, y=studio_y,
        mode='markers+text',
        name='Studios',
        text=top_studios['company'],
        textposition='middle left',
        marker=dict(size=top_studios['film_count']*3, color=COLORS['accent'],
                   line=dict(width=2, color='black'))
    ))
    
    fig.add_trace(go.Scatter(
        x=producer_x, y=producer_y,
        mode='markers+text',
        name='Producers',
        text=top_producers['producer'],
        textposition='middle center',
        marker=dict(size=top_producers['film_count']*3, color=COLORS['success'],
                   line=dict(width=2, color='black'))
    ))
    
    fig.add_trace(go.Scatter(
        x=composer_x, y=composer_y,
        mode='markers+text',
        name='Composers',
        text=top_composers['composer'],
        textposition='middle right',
        marker=dict(size=top_composers['film_count']*3, color=COLORS['info'],
                   line=dict(width=2, color='black'))
    ))
    
    fig.update_layout(
        title="Production Network: Key Players",
        showlegend=True,
        height=800,
        template='plotly_white',
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
    )
    
    fig.write_html(OUTPUT_DIR / "13_crew_collaboration_network.html")
    logger.info("✅ Viz 13 complete")


def viz_14_awards_decade_evolution(analyzer: ProductionAwardsAnalyzer):
    """Awards trends across decades."""
    logger.info("Creating Viz 14: Awards Decade Evolution...")
    
    awarded_films = analyzer.awards_data[analyzer.awards_data['has_awards']].copy()
    
    if awarded_films.empty:
        logger.warning("No awards data for decade evolution")
        return
    
    awarded_films['decade'] = (awarded_films['year'] // 10) * 10
    
    decade_stats = awarded_films.groupby('decade').agg({
        'title': 'count',
        'awards_count': 'sum',
        'nominations_count': 'sum',
        'rating': 'mean',
        'oscar_bp': 'sum',
        'oscar_winner': 'sum'
    }).reset_index()
    
    decade_stats.columns = ['decade', 'films', 'total_awards', 'total_noms', 
                           'avg_rating', 'bp_count', 'oscar_count']
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Films with awards per decade
    ax1.plot(decade_stats['decade'], decade_stats['films'], 
            marker='o', linewidth=2, markersize=8, color=COLORS['accent'])
    ax1.fill_between(decade_stats['decade'], decade_stats['films'], 
                     alpha=0.3, color=COLORS['accent'])
    ax1.set_xlabel('Decade', fontsize=11, fontweight='bold')
    ax1.set_ylabel('Awarded Films', fontsize=11, fontweight='bold')
    ax1.set_title('Awarded Films Per Decade', fontsize=12, fontweight='bold', pad=15)
    ax1.grid(alpha=0.3)
    
    # 2. Total awards won
    ax2.bar(decade_stats['decade'], decade_stats['total_awards'],
           color=COLORS['success'], alpha=0.8, edgecolor='black', width=5)
    ax2.set_xlabel('Decade', fontsize=11, fontweight='bold')
    ax2.set_ylabel('Total Awards', fontsize=11, fontweight='bold')
    ax2.set_title('Total Awards Won Per Decade', fontsize=12, fontweight='bold', pad=15)
    ax2.grid(axis='y', alpha=0.3)
    
    # 3. Oscar winners
    ax3.bar(decade_stats['decade'], decade_stats['oscar_count'],
           color='#FFD700', alpha=0.9, edgecolor='black', width=5, label='Oscar Winners')
    ax3.bar(decade_stats['decade'], decade_stats['bp_count'],
           color='#C0C0C0', alpha=0.9, edgecolor='black', width=5, label='Best Picture')
    ax3.set_xlabel('Decade', fontsize=11, fontweight='bold')
    ax3.set_ylabel('Count', fontsize=11, fontweight='bold')
    ax3.set_title('Oscar Recognition Per Decade', fontsize=12, fontweight='bold', pad=15)
    ax3.legend()
    ax3.grid(axis='y', alpha=0.3)
    
    # 4. Average rating of awarded films
    ax4.plot(decade_stats['decade'], decade_stats['avg_rating'],
            marker='s', linewidth=2, markersize=8, color=COLORS['secondary'])
    ax4.axhline(y=7.0, color='red', linestyle='--', alpha=0.5, linewidth=1, label='7.0 baseline')
    ax4.set_xlabel('Decade', fontsize=11, fontweight='bold')
    ax4.set_ylabel('Average Rating', fontsize=11, fontweight='bold')
    ax4.set_title('Quality of Awarded Films', fontsize=12, fontweight='bold', pad=15)
    ax4.set_ylim(6, 9)
    ax4.legend()
    ax4.grid(alpha=0.3)
    
    plt.suptitle('Awards Evolution Across Decades', fontsize=15, fontweight='bold', y=0.98)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "14_awards_decade_evolution.png", dpi=300, bbox_inches='tight')
    plt.close()
    logger.info("✅ Viz 14 complete")


# =============================================================================
# SUMMARY & MAIN EXECUTION
# =============================================================================

def generate_summary(analyzer: ProductionAwardsAnalyzer) -> str:
    """Generate comprehensive summary."""
    studios = analyzer.get_top_studios(20)
    producers = analyzer.get_top_crew('producers', 15)
    cinematographers = analyzer.get_top_crew('cinematographers', 12)
    composers = analyzer.get_top_crew('composers', 15)
    editors = analyzer.get_top_crew('editors', 12)
    awards_summary = analyzer.get_awards_summary()
    
    top_awarded = analyzer.awards_data.nlargest(5, 'awards_count')
    
    # Helper to safely extract stats without crashing on empty dataframes
    def get_stats(df, name_col):
        if df.empty:
            return 'N/A', 0, 0.0
        row = df.iloc[0]
        return row[name_col], int(row['film_count']), row['avg_rating']

    # Pre-calculate values to avoid f-string syntax errors
    s_name, s_count, s_rating = get_stats(studios, 'company')
    p_name, p_count, p_rating = get_stats(producers, 'producer')
    dp_name, dp_count, dp_rating = get_stats(cinematographers, 'cinematographer')
    c_name, c_count, c_rating = get_stats(composers, 'composer')
    e_name, e_count, e_rating = get_stats(editors, 'editor')

    summary = f"""
{'='*80}
BATCH 6: PRODUCTION NETWORKS & AWARDS RECOGNITION - COMPLETE
{'='*80}

📊 PRODUCTION NETWORKS ANALYSIS

Top Production Company: {s_name}
   Films: {s_count} | Avg Rating: {s_rating:.2f}

Top Producer: {p_name}
   Films: {p_count} | Avg Rating: {p_rating:.2f}

Top Cinematographer: {dp_name}
   Films: {dp_count} | Avg Rating: {dp_rating:.2f}

Top Composer: {c_name}
   Films: {c_count} | Avg Rating: {c_rating:.2f}

Top Editor: {e_name}
   Films: {e_count} | Avg Rating: {e_rating:.2f}

🏆 AWARDS & RECOGNITION ANALYSIS

Collection Size: {awards_summary['total_films']:,} films
Films with Awards: {awards_summary['films_with_awards']:,} ({awards_summary['pct_with_awards']:.1f}%)

Oscar Statistics:
   Best Picture Winners: {awards_summary['oscar_bp_winners']}
   Other Oscar Winners: {awards_summary['oscar_winners']}
   Oscar Nominees: {awards_summary['oscar_nominees']}

Awards Totals:
   Total Awards Won: {int(awards_summary['total_awards']):,}
   Total Nominations: {int(awards_summary['total_nominations']):,}
   Average per Film: {awards_summary['avg_awards_per_film']:.2f}

Most Awarded Films:
"""
    
    for i, (_, film) in enumerate(top_awarded.iterrows(), 1):
        oscar_mark = " 🏆" if film['oscar_bp'] else ""
        summary += f"   {i}. {film['title']} ({int(film['year'])}) - {int(film['awards_count'])} awards{oscar_mark}\n"
    
    summary += f"""
🎬 VISUALIZATIONS CREATED:

PRODUCTION NETWORKS:
1. 01_studio_leaderboard.png - Top 20 production companies
2. 02_producer_network.png - Top producers analysis
3. 03_cinematographer_excellence.png - Directors of photography
4. 04_composer_leaderboard.png - Top film composers
5. 05_editor_craftsmen.png - Top film editors
6. 06_production_network_heatmap.png - Crew collaboration overview

AWARDS & RECOGNITION:
7. 07_awards_overview.png - Awards collection overview
8. 08_oscar_timeline.png - Oscar recognition across decades
9. 09_awards_vs_rating.png - Awards and quality correlation
10. 10_top_awarded_films.png - Most awarded films
11. 11_interactive_production_dashboard.html - Interactive crew dashboard
12. 12_interactive_awards_explorer.html - Interactive awards explorer
13. 13_crew_collaboration_network.html - Collaboration network
14. 14_awards_decade_evolution.png - Awards trends over time

✅ Batch 6 Complete!
{'='*80}
"""
    
    return summary


def main():
    logger.info("="*80)
    logger.info("BATCH 6: PRODUCTION NETWORKS & AWARDS RECOGNITION")
    logger.info("="*80)
    
    # Load data
    master_file = DATA_DIR / "watched_movies_master.csv"
    if not master_file.exists():
        logger.error(f"Master file not found: {master_file}")
        logger.error("Please run batch_0_filter_watched.py first")
        return
    
    df = pd.read_csv(master_file, low_memory=False)
    logger.info(f"Loaded {len(df)} films")
    
    # Initialize analyzer
    analyzer = ProductionAwardsAnalyzer(df)
    
    # Generate visualizations
    logger.info("\n" + "="*80)
    logger.info("GENERATING VISUALIZATIONS")
    logger.info("="*80)
    
    # Production Networks (6 visualizations)
    viz_1_studio_leaderboard(analyzer)
    viz_2_producer_network(analyzer)
    viz_3_cinematographer_excellence(analyzer)
    viz_4_composer_leaderboard(analyzer)
    viz_5_editor_craftsmen(analyzer)
    viz_6_production_network_heatmap(analyzer)
    
    # Awards & Recognition (8 visualizations)
    viz_7_awards_overview(analyzer)
    viz_8_oscar_timeline(analyzer)
    viz_9_awards_vs_rating(analyzer)
    viz_10_top_awarded_films(analyzer)
    viz_11_interactive_production_dashboard(analyzer)
    viz_12_interactive_awards_explorer(analyzer)
    viz_13_crew_collaboration_network(analyzer)
    viz_14_awards_decade_evolution(analyzer)
    
    # Generate and save summary
    summary = generate_summary(analyzer)
    print(summary)
    
    with open(OUTPUT_DIR / "BATCH_6_SUMMARY.txt", 'w') as f:
        f.write(summary)
    
    logger.info(f"\n✅ All visualizations saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()