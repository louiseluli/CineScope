"""
CineScope Extended People Enrichment Script (Step 7)

EXTENDED BIOGRAPHICAL DATA
==========================
This script enriches the people cache with additional data from Wikidata:

- Full birth/death dates (day, month, year)
- Cause of death (P509)
- Zodiac signs (Western + Chinese, calculated)
- Height (P2048)
- Spouses (P26)
- Number of children (P1971)
- Education/alma mater (P69)
- Burial location (P119)

Usage:
    python scripts/enrich/07_enrich_people_extended.py                  # Enrich all
    python scripts/enrich/07_enrich_people_extended.py --limit 100      # Limited batch
    python scripts/enrich/07_enrich_people_extended.py --force          # Re-enrich all
"""
import sys
import json
import time
import argparse
import logging
from pathlib import Path
from typing import Dict, Optional, List
from datetime import datetime
from tqdm import tqdm
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.core.config import settings
from src.analysis.zodiac import ZodiacCalculator, calculate_age

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


class ExtendedPeopleEnricher:
    """
    Enriches people cache with extended biographical data from Wikidata.
    """
    
    SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
    USER_AGENT = "CineScope/1.0 (Cinema Analytics; Educational Project)"
    
    def __init__(self):
        self.people_cache_file = settings.PROCESSED_DATA_DIR / "people_cache.json"
        self.people_cache = self._load_cache()
        self.zodiac_calc = ZodiacCalculator()
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.USER_AGENT,
            'Accept': 'application/sparql-results+json'
        })
        self._last_query_time = 0
        
        self.stats = {
            'total': 0,
            'enriched': 0,
            'with_cause_of_death': 0,
            'with_height': 0,
            'with_zodiac': 0,
            'errors': 0
        }
    
    def _load_cache(self) -> Dict:
        """Load existing people cache."""
        if self.people_cache_file.exists():
            with open(self.people_cache_file, 'r') as f:
                return json.load(f)
        return {}
    
    def _save_cache(self):
        """Save people cache."""
        # Backup first
        backup = self.people_cache_file.with_suffix('.json.bak')
        if self.people_cache_file.exists():
            import shutil
            shutil.copy(self.people_cache_file, backup)
        
        with open(self.people_cache_file, 'w') as f:
            json.dump(self.people_cache, f, indent=2)
        logger.info(f"Saved {len(self.people_cache):,} people to cache")
    
    def _rate_limit(self):
        """Rate limit: 1 request per second for Wikidata."""
        elapsed = time.time() - self._last_query_time
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
        self._last_query_time = time.time()
    
    def _query_sparql(self, sparql: str) -> Optional[List[Dict]]:
        """Execute SPARQL query against Wikidata."""
        self._rate_limit()
        
        try:
            response = self.session.post(
                self.SPARQL_ENDPOINT,
                data={'query': sparql},
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            return data.get('results', {}).get('bindings', [])
        except requests.exceptions.Timeout:
            logger.warning("Wikidata query timeout")
            return None
        except Exception as e:
            logger.debug(f"Wikidata query failed: {e}")
            return None
    
    def fetch_extended_data(self, imdb_id: str) -> Optional[Dict]:
        """
        Fetch extended biographical data from Wikidata.
        
        Args:
            imdb_id: IMDB person ID (e.g., 'nm0000138')
            
        Returns:
            Dictionary with extended data or None
        """
        if not imdb_id:
            return None
        
        # Ensure nm prefix
        if not imdb_id.startswith('nm'):
            imdb_id = f'nm{imdb_id}'
        
        sparql = f"""
        SELECT DISTINCT
            ?person ?personLabel
            ?birthDate ?deathDate
            ?causeOfDeath ?causeOfDeathLabel
            ?mannerOfDeath ?mannerOfDeathLabel
            ?height
            ?burialPlace ?burialPlaceLabel
            (GROUP_CONCAT(DISTINCT ?educationLabel; SEPARATOR="|") AS ?education)
            (GROUP_CONCAT(DISTINCT ?spouseLabel; SEPARATOR="|") AS ?spouses)
            ?numberOfChildren
        WHERE {{
            ?person wdt:P345 "{imdb_id}" .
            
            OPTIONAL {{ ?person wdt:P569 ?birthDate . }}
            OPTIONAL {{ ?person wdt:P570 ?deathDate . }}
            OPTIONAL {{ ?person wdt:P509 ?causeOfDeath . }}
            OPTIONAL {{ ?person wdt:P1196 ?mannerOfDeath . }}
            OPTIONAL {{ ?person wdt:P2048 ?height . }}
            OPTIONAL {{ ?person wdt:P119 ?burialPlace . }}
            OPTIONAL {{ 
                ?person wdt:P69 ?educationInst .
                ?educationInst rdfs:label ?educationLabel .
                FILTER(LANG(?educationLabel) = "en")
            }}
            OPTIONAL {{ 
                ?person wdt:P26 ?spouse .
                ?spouse rdfs:label ?spouseLabel .
                FILTER(LANG(?spouseLabel) = "en")
            }}
            OPTIONAL {{ ?person wdt:P1971 ?numberOfChildren . }}
            
            SERVICE wikibase:label {{ 
                bd:serviceParam wikibase:language "en" .
            }}
        }}
        GROUP BY ?person ?personLabel ?birthDate ?deathDate 
                 ?causeOfDeath ?causeOfDeathLabel
                 ?mannerOfDeath ?mannerOfDeathLabel
                 ?height ?burialPlace ?burialPlaceLabel ?numberOfChildren
        LIMIT 1
        """
        
        results = self._query_sparql(sparql)
        
        if not results:
            return None
        
        r = results[0]
        
        def get_val(key: str) -> Optional[str]:
            return r.get(key, {}).get('value')
        
        # Parse height (comes in meters from Wikidata)
        height_val = get_val('height')
        height_cm = None
        height_imperial = None
        if height_val:
            try:
                height_m = float(height_val)
                height_cm = int(height_m * 100)
                feet = int(height_cm / 30.48)
                inches = int((height_cm / 2.54) % 12)
                height_imperial = f"{feet}'{inches}\""
            except (ValueError, TypeError):
                pass
        
        # Parse dates
        birth_date_full = get_val('birthDate')
        death_date_full = get_val('deathDate')
        
        # Calculate zodiac if we have birth date
        zodiac_data = {}
        if birth_date_full:
            zodiac_result = self.zodiac_calc.calculate(birth_date_full)
            if zodiac_result:
                zodiac_data = {
                    'zodiac_western': zodiac_result.get('western_sign'),
                    'zodiac_symbol': zodiac_result.get('western_symbol'),
                    'zodiac_element': zodiac_result.get('western_element'),
                    'zodiac_chinese': zodiac_result.get('chinese_animal'),
                    'zodiac_chinese_symbol': zodiac_result.get('chinese_symbol'),
                }
        
        # Calculate ages
        current_age = None
        age_at_death = None
        
        if birth_date_full:
            if death_date_full:
                age_at_death = calculate_age(birth_date_full, death_date_full)
            else:
                current_age = calculate_age(birth_date_full)
        
        return {
            # Full dates
            'ext_birth_date': birth_date_full,
            'ext_death_date': death_date_full,
            
            # Mortality
            'ext_cause_of_death': get_val('causeOfDeathLabel'),
            'ext_manner_of_death': get_val('mannerOfDeathLabel'),
            'ext_age_at_death': age_at_death,
            'ext_current_age': current_age,
            
            # Physical
            'ext_height_cm': height_cm,
            'ext_height_imperial': height_imperial,
            
            # Personal
            'ext_spouses': get_val('spouses'),
            'ext_num_children': get_val('numberOfChildren'),
            'ext_education': get_val('education'),
            'ext_burial_place': get_val('burialPlaceLabel'),
            
            # Zodiac
            **zodiac_data,
            
            # Meta
            'ext_enriched': True,
            'ext_enriched_date': datetime.now().isoformat()
        }
    
    def enrich_all(self, limit: int = None, force: bool = False):
        """
        Enrich all people in cache with extended data.
        
        Args:
            limit: Maximum number of people to process
            force: Re-enrich even if already enriched
        """
        logger.info("=" * 80)
        logger.info("EXTENDED PEOPLE ENRICHMENT")
        logger.info("=" * 80)
        
        # Find people with IMDB IDs who need enrichment
        to_enrich = []
        for tmdb_id, person in self.people_cache.items():
            imdb_id = person.get('imdb_id')
            if not imdb_id:
                continue
            if not force and person.get('ext_enriched'):
                continue
            to_enrich.append((tmdb_id, imdb_id, person))
        
        if limit:
            to_enrich = to_enrich[:limit]
        
        logger.info(f"People to enrich: {len(to_enrich):,}")
        
        if not to_enrich:
            logger.info("No people need enrichment!")
            return
        
        self.stats['total'] = len(to_enrich)
        
        for tmdb_id, imdb_id, person in tqdm(to_enrich, desc="Enriching people"):
            try:
                ext_data = self.fetch_extended_data(imdb_id)
                
                if ext_data:
                    # Update person in cache
                    for key, value in ext_data.items():
                        if value is not None:
                            person[key] = value
                    
                    self.stats['enriched'] += 1
                    
                    if ext_data.get('ext_cause_of_death'):
                        self.stats['with_cause_of_death'] += 1
                    if ext_data.get('ext_height_cm'):
                        self.stats['with_height'] += 1
                    if ext_data.get('zodiac_western'):
                        self.stats['with_zodiac'] += 1
                
                # Checkpoint every 50
                if self.stats['enriched'] % 50 == 0 and self.stats['enriched'] > 0:
                    self._save_cache()
                    logger.info(f"Progress: {self.stats['enriched']}/{self.stats['total']}")
                    
            except Exception as e:
                logger.debug(f"Error enriching {imdb_id}: {e}")
                self.stats['errors'] += 1
        
        self._save_cache()
        self._print_summary()
    
    def _print_summary(self):
        """Print enrichment summary."""
        logger.info("\n" + "=" * 80)
        logger.info("ENRICHMENT SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total processed:        {self.stats['total']:,}")
        logger.info(f"Successfully enriched:  {self.stats['enriched']:,}")
        logger.info(f"With cause of death:    {self.stats['with_cause_of_death']:,}")
        logger.info(f"With height:            {self.stats['with_height']:,}")
        logger.info(f"With zodiac:            {self.stats['with_zodiac']:,}")
        logger.info(f"Errors:                 {self.stats['errors']:,}")
    
    def calculate_zodiac_for_all(self):
        """Calculate zodiac signs for all people with birth dates."""
        logger.info("Calculating zodiac signs for all people...")
        
        updated = 0
        for tmdb_id, person in tqdm(self.people_cache.items(), desc="Calculating zodiac"):
            # Skip if already has zodiac
            if person.get('zodiac_western'):
                continue
            
            # Try to get birth date from various sources
            birth_date = (
                person.get('ext_birth_date') or 
                person.get('birthday') or
                person.get('imdb_birth_year')
            )
            
            if birth_date:
                zodiac = self.zodiac_calc.calculate(str(birth_date))
                if zodiac and zodiac.get('western_sign'):
                    person['zodiac_western'] = zodiac['western_sign']
                    person['zodiac_symbol'] = zodiac['western_symbol']
                    person['zodiac_element'] = zodiac['western_element']
                    person['zodiac_chinese'] = zodiac.get('chinese_animal')
                    person['zodiac_chinese_symbol'] = zodiac.get('chinese_symbol')
                    updated += 1
        
        self._save_cache()
        logger.info(f"Updated zodiac for {updated:,} people")


def main():
    parser = argparse.ArgumentParser(description='Extended people enrichment')
    parser.add_argument('--limit', type=int, help='Max people to process')
    parser.add_argument('--force', action='store_true', help='Re-enrich all')
    parser.add_argument('--zodiac-only', action='store_true', help='Only calculate zodiac')
    args = parser.parse_args()
    
    enricher = ExtendedPeopleEnricher()
    
    if args.zodiac_only:
        enricher.calculate_zodiac_for_all()
    else:
        enricher.enrich_all(limit=args.limit, force=args.force)


if __name__ == '__main__':
    main()
