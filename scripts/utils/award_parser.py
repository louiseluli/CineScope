"""
Award Parser Utility for CineScope Analysis
============================================

Comprehensive parser for extracting awards from both OMDB and Wikidata sources.
Combines data from multiple sources to maximize coverage.

Author: CineScope Analytics
Date: 2025-12-30
"""

import re
from typing import Dict, List, Set, Optional
import pandas as pd


class AwardParser:
    """Parse and extract award information from OMDB and Wikidata sources."""

    def __init__(self):
        """Initialize award patterns for extraction."""

        # Academy Awards patterns
        self.oscar_patterns = {
            'won': r'Won\s+(\d+)\s+Oscar',
            'nominated': r'Nominated for\s+(\d+)\s+Oscar',
            'best_picture': r'Academy Award for Best Picture',
            'best_director': r'Academy Award for Best Director',
            'best_actor': r'Academy Award for Best Actor',
            'best_actress': r'Academy Award for Best Actress',
        }

        # BAFTA patterns
        self.bafta_patterns = {
            'won': r'Won\s+(\d+)\s+BAFTA',
            'nominated': r'Nominated for\s+(\d+)\s+BAFTA',
            'general': r'BAFTA Award for',
        }

        # Golden Globe patterns
        self.golden_globe_patterns = {
            'won': r'won.*Golden Globe',
            'nominated': r'nominated.*Golden Globe',
            'general': r'Golden Globe Award',
        }

        # Film Festival patterns
        self.festival_patterns = {
            'Cannes': [r'Cannes', r'Palme d\'?Or'],
            'Venice': [r'Venice', r'Golden Lion'],
            'Berlin': [r'Berlin', r'Golden Bear'],
            'Sundance': [r'Sundance'],
            'Toronto': [r'Toronto', r'TIFF'],
            'Tribeca': [r'Tribeca'],
        }

        # Other major awards
        self.other_awards = {
            'SAG': [r'Screen Actors Guild', r'SAG Award'],
            'César': [r'César Award'],
            'Goya': [r'Goya Award'],
            'Critics Choice': [r'Critics[\'\']?\s+Choice'],
            'National Board of Review': [r'National Board of Review'],
        }

    def parse_omdb_awards(self, omdb_text: str) -> Dict:
        """Parse awards from OMDB awards text."""
        if pd.isna(omdb_text):
            return self._empty_result()

        omdb_text = str(omdb_text)
        result = self._empty_result()

        # Extract Oscar wins
        oscar_won = re.search(self.oscar_patterns['won'], omdb_text)
        if oscar_won:
            result['oscar_wins'] = int(oscar_won.group(1))
            result['has_oscar'] = True

        # Extract Oscar nominations
        oscar_nom = re.search(self.oscar_patterns['nominated'], omdb_text)
        if oscar_nom:
            result['oscar_noms'] = int(oscar_nom.group(1))

        # Extract BAFTA
        bafta_won = re.search(self.bafta_patterns['won'], omdb_text, re.IGNORECASE)
        if bafta_won:
            result['bafta_wins'] = int(bafta_won.group(1))
            result['has_bafta'] = True

        bafta_nom = re.search(self.bafta_patterns['nominated'], omdb_text, re.IGNORECASE)
        if bafta_nom:
            result['bafta_noms'] = int(bafta_nom.group(1))
        elif 'BAFTA' in omdb_text:
            result['has_bafta'] = True

        # Extract total wins/nominations
        total_wins = re.search(r'(\d+)\s+win(?:s)?(?:\s+&|\s+total|\.)', omdb_text)
        if total_wins:
            result['total_wins'] = int(total_wins.group(1))

        total_noms = re.search(r'(\d+)\s+nomination(?:s)?(?:\s+total|\.)', omdb_text)
        if total_noms:
            result['total_noms'] = int(total_noms.group(1))

        return result

    def parse_wikidata_awards(self, wd_text: str) -> Dict:
        """Parse awards from Wikidata awards text."""
        if pd.isna(wd_text):
            return self._empty_result()

        wd_text = str(wd_text)
        result = self._empty_result()

        # Split by comma to get individual awards
        awards_list = [a.strip() for a in wd_text.split(',')]

        # Count Academy Awards
        academy_awards = [a for a in awards_list if 'Academy Award' in a]
        if academy_awards:
            result['oscar_wins'] = len(academy_awards)
            result['has_oscar'] = True

            # Check for specific categories
            if any('Best Picture' in a for a in academy_awards):
                result['has_best_picture'] = True
            if any('Best Director' in a for a in academy_awards):
                result['has_best_director'] = True

        # Count Golden Globes
        golden_globes = [a for a in awards_list if 'Golden Globe' in a]
        if golden_globes:
            result['golden_globe_count'] = len(golden_globes)
            result['has_golden_globe'] = True

        # Count BAFTAs
        baftas = [a for a in awards_list if 'BAFTA' in a]
        if baftas:
            result['bafta_wins'] = len(baftas)
            result['has_bafta'] = True

        # Check for festivals
        for festival, patterns in self.festival_patterns.items():
            if any(re.search(pattern, wd_text, re.IGNORECASE) for pattern in patterns):
                result['festivals'].add(festival)
                result['has_festival'] = True

        # Check for other awards
        for award_name, patterns in self.other_awards.items():
            if any(re.search(pattern, wd_text, re.IGNORECASE) for pattern in patterns):
                result['other_awards'].add(award_name)

        # Special checks
        if any('SAG' in a or 'Screen Actors Guild' in a for a in awards_list):
            result['has_sag'] = True

        if any('César' in a for a in awards_list):
            result['has_cesar'] = True

        if any('Goya' in a for a in awards_list):
            result['has_goya'] = True

        # National Board of Review
        if any('National Board of Review' in a for a in awards_list):
            result['has_nbr'] = True

        return result

    def combine_sources(self, omdb_result: Dict, wd_result: Dict) -> Dict:
        """Combine results from OMDB and Wikidata, preferring higher values."""
        combined = self._empty_result()

        # Combine Oscar data (use max)
        combined['oscar_wins'] = max(omdb_result['oscar_wins'], wd_result['oscar_wins'])
        combined['oscar_noms'] = max(omdb_result['oscar_noms'], wd_result['oscar_noms'])
        combined['has_oscar'] = omdb_result['has_oscar'] or wd_result['has_oscar']

        # Combine BAFTA data
        combined['bafta_wins'] = max(omdb_result['bafta_wins'], wd_result['bafta_wins'])
        combined['bafta_noms'] = max(omdb_result['bafta_noms'], wd_result['bafta_noms'])
        combined['has_bafta'] = omdb_result['has_bafta'] or wd_result['has_bafta']

        # Combine Golden Globe
        combined['golden_globe_count'] = max(omdb_result['golden_globe_count'],
                                             wd_result['golden_globe_count'])
        combined['has_golden_globe'] = omdb_result['has_golden_globe'] or wd_result['has_golden_globe']

        # Combine totals (prefer OMDB as it has overall counts)
        combined['total_wins'] = max(omdb_result['total_wins'], wd_result['total_wins'])
        combined['total_noms'] = max(omdb_result['total_noms'], wd_result['total_noms'])

        # Combine festivals (union of sets)
        combined['festivals'] = omdb_result['festivals'] | wd_result['festivals']
        combined['has_festival'] = omdb_result['has_festival'] or wd_result['has_festival']

        # Combine other awards
        combined['other_awards'] = omdb_result['other_awards'] | wd_result['other_awards']
        combined['has_sag'] = omdb_result['has_sag'] or wd_result['has_sag']
        combined['has_cesar'] = omdb_result['has_cesar'] or wd_result['has_cesar']
        combined['has_goya'] = omdb_result['has_goya'] or wd_result['has_goya']
        combined['has_nbr'] = omdb_result['has_nbr'] or wd_result['has_nbr']

        # Special flags from Wikidata
        combined['has_best_picture'] = wd_result['has_best_picture']
        combined['has_best_director'] = wd_result['has_best_director']

        return combined

    def _empty_result(self) -> Dict:
        """Return empty result structure."""
        return {
            'oscar_wins': 0,
            'oscar_noms': 0,
            'has_oscar': False,
            'bafta_wins': 0,
            'bafta_noms': 0,
            'has_bafta': False,
            'golden_globe_count': 0,
            'has_golden_globe': False,
            'total_wins': 0,
            'total_noms': 0,
            'festivals': set(),
            'has_festival': False,
            'other_awards': set(),
            'has_sag': False,
            'has_cesar': False,
            'has_goya': False,
            'has_nbr': False,
            'has_best_picture': False,
            'has_best_director': False,
        }

    def parse_row(self, row: pd.Series) -> Dict:
        """Parse a single row with both OMDB and Wikidata awards."""
        omdb_result = self.parse_omdb_awards(row.get('omdb_awards'))
        wd_result = self.parse_wikidata_awards(row.get('wd_awards'))

        return self.combine_sources(omdb_result, wd_result)

    def calculate_award_diversity(self, parsed: Dict) -> int:
        """Calculate award diversity score (number of different award types)."""
        diversity = 0

        if parsed['has_oscar']:
            diversity += 1
        if parsed['has_bafta']:
            diversity += 1
        if parsed['has_golden_globe']:
            diversity += 1
        if parsed['has_festival']:
            diversity += len(parsed['festivals'])
        if parsed['has_sag']:
            diversity += 1
        if parsed['has_cesar']:
            diversity += 1
        if parsed['has_goya']:
            diversity += 1
        if parsed['has_nbr']:
            diversity += 1

        return diversity
