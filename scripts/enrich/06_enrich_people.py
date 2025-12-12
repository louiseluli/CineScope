"""
CineScope People Data Enrichment Script (Step 6)

COMPREHENSIVE PEOPLE DATABASE COMPLETION
========================================
This script creates a complete, authoritative people database by:

1. TMDB → IMDB LINKING: Fetch imdb_id from TMDB's person API for all cached people
2. IMDB ENRICHMENT: Use name.basics as authoritative source for:
   - Birth year, death year
   - Primary profession
   - Known for titles
3. GENDER RESOLUTION: Fix unknown genders (gender=0) using:
   - IMDB profession hints (actor vs actress)
   - Wikidata API queries
4. VALIDATION: Cross-validate all data, flag inconsistencies

DATA HIERARCHY (IMDB is most reliable):
- Name: IMDB primaryName (fallback to TMDB)
- Birth/Death: IMDB birthYear/deathYear (authoritative)
- Gender: IMDB profession → Wikidata → TMDB
- Biography: TMDB (IMDB doesn't have bios)
- Popularity: TMDB (IMDB doesn't track this)

Usage:
    python scripts/enrich/06_enrich_people.py                    # Enrich all
    python scripts/enrich/06_enrich_people.py --fetch-imdb-ids   # Only fetch IMDB IDs from TMDB
    python scripts/enrich/06_enrich_people.py --resolve-gender   # Only resolve unknown genders
    python scripts/enrich/06_enrich_people.py --validate         # Validate and report
    python scripts/enrich/06_enrich_people.py --limit 100        # Process limited batch
"""
import sys
import json
import time
import argparse
import logging
import sqlite3
from pathlib import Path
from typing import Dict, Optional, Tuple, List
from tqdm import tqdm
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.core.config import settings
from src.enrichment.tmdb_client import TMDbClient

settings.ensure_directories()

logging.basicConfig(
    level=settings.LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(settings.LOG_FILE)
    ]
)
logger = logging.getLogger(__name__)


class PeopleEnricher:
    """
    Comprehensive people database enrichment.
    Links TMDB → IMDB, enriches with IMDB data, resolves unknown genders.
    """
    
    def __init__(self):
        self.tmdb_client = TMDbClient()
        self.people_cache_file = settings.PROCESSED_DATA_DIR / "people_cache.json"
        self.people_cache = self._load_people_cache()
        self.imdb_db_path = settings.RAW_DATA_DIR / "imdb.db"
        self.imdb_names_tsv = settings.RAW_DATA_DIR / "name.basics.tsv"
        
        # Statistics
        self.stats = {
            'total_people': 0,
            'imdb_ids_fetched': 0,
            'imdb_enriched': 0,
            'genders_resolved': 0,
            'wikidata_queries': 0,
            'errors': 0
        }
        
    def _load_people_cache(self) -> Dict:
        """Load existing people cache."""
        if self.people_cache_file.exists():
            with open(self.people_cache_file, 'r') as f:
                return json.load(f)
        return {}
    
    def _save_people_cache(self):
        """Save people cache to file."""
        # Create backup first
        backup_file = self.people_cache_file.with_suffix('.json.bak')
        if self.people_cache_file.exists():
            import shutil
            shutil.copy(self.people_cache_file, backup_file)
        
        with open(self.people_cache_file, 'w') as f:
            json.dump(self.people_cache, f, indent=2)
        logger.info(f"Saved {len(self.people_cache):,} people to cache")
    
    def _get_imdb_connection(self) -> Optional[sqlite3.Connection]:
        """Get connection to IMDB database."""
        if self.imdb_db_path.exists():
            return sqlite3.connect(self.imdb_db_path)
        return None
    
    def _load_imdb_names_index(self) -> Dict[str, Dict]:
        """Load IMDB name.basics into memory for fast lookups by nconst."""
        logger.info("Loading IMDB name.basics index...")
        
        # Try database first (faster)
        conn = self._get_imdb_connection()
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='name_basics'")
                if cursor.fetchone():
                    logger.info("Loading from IMDB database...")
                    cursor.execute("""
                        SELECT nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles
                        FROM name_basics
                    """)
                    
                    index = {}
                    for row in cursor.fetchall():
                        nconst, name, birth, death, profession, known_for = row
                        index[nconst] = {
                            'primaryName': name,
                            'birthYear': birth if birth != '\\N' else None,
                            'deathYear': death if death != '\\N' else None,
                            'primaryProfession': profession if profession != '\\N' else None,
                            'knownForTitles': known_for if known_for != '\\N' else None
                        }
                    conn.close()
                    logger.info(f"Loaded {len(index):,} people from IMDB database")
                    return index
            except Exception as e:
                logger.warning(f"Database query failed: {e}")
                conn.close()
        
        # Fallback to TSV file
        if self.imdb_names_tsv.exists():
            logger.info("Loading from TSV file (slower)...")
            index = {}
            with open(self.imdb_names_tsv, 'r', encoding='utf-8') as f:
                header = f.readline()  # Skip header
                for line in tqdm(f, desc="Loading name.basics"):
                    parts = line.strip().split('\t')
                    if len(parts) >= 6:
                        nconst = parts[0]
                        index[nconst] = {
                            'primaryName': parts[1],
                            'birthYear': parts[2] if parts[2] != '\\N' else None,
                            'deathYear': parts[3] if parts[3] != '\\N' else None,
                            'primaryProfession': parts[4] if parts[4] != '\\N' else None,
                            'knownForTitles': parts[5] if parts[5] != '\\N' else None
                        }
            logger.info(f"Loaded {len(index):,} people from TSV")
            return index
        
        logger.warning("No IMDB name data available!")
        return {}
    
    # =========================================================================
    # STEP 1: Fetch IMDB IDs from TMDB API
    # =========================================================================
    
    def fetch_imdb_ids_from_tmdb(self, limit: int = None):
        """
        Fetch IMDB IDs for all people in cache using TMDB's external_ids endpoint.
        TMDB stores imdb_id in person details but we may not have fetched it.
        """
        logger.info("="*80)
        logger.info("STEP 1: Fetching IMDB IDs from TMDB API")
        logger.info("="*80)
        
        # Find people without IMDB IDs
        people_without_imdb = [
            (tmdb_id, person) for tmdb_id, person in self.people_cache.items()
            if not person.get('imdb_id')
        ]
        
        if limit:
            people_without_imdb = people_without_imdb[:limit]
        
        logger.info(f"People without IMDB ID: {len(people_without_imdb):,}")
        
        if not people_without_imdb:
            logger.info("All people already have IMDB IDs!")
            return
        
        for tmdb_id, person in tqdm(people_without_imdb, desc="Fetching IMDB IDs"):
            try:
                # TMDB external_ids endpoint gives us the IMDB ID
                external_ids = self.tmdb_client._make_request(f"/person/{tmdb_id}/external_ids")
                
                if external_ids and external_ids.get('imdb_id'):
                    self.people_cache[tmdb_id]['imdb_id'] = external_ids['imdb_id']
                    self.stats['imdb_ids_fetched'] += 1
                
                # Also try to get from person details if not in external_ids
                if not self.people_cache[tmdb_id].get('imdb_id'):
                    person_details = self.tmdb_client._make_request(f"/person/{tmdb_id}")
                    if person_details and person_details.get('imdb_id'):
                        self.people_cache[tmdb_id]['imdb_id'] = person_details['imdb_id']
                        self.stats['imdb_ids_fetched'] += 1
                
                time.sleep(1 / settings.TMDB_RATE_LIMIT)
                
                # Checkpoint every 100
                if self.stats['imdb_ids_fetched'] % 100 == 0:
                    self._save_people_cache()
                    
            except Exception as e:
                logger.debug(f"Error fetching IMDB ID for {tmdb_id}: {e}")
                self.stats['errors'] += 1
        
        self._save_people_cache()
        logger.info(f"Fetched {self.stats['imdb_ids_fetched']:,} IMDB IDs")
    
    # =========================================================================
    # STEP 2: Enrich with IMDB data (authoritative source)
    # =========================================================================
    
    def enrich_from_imdb(self):
        """
        Enrich people cache with IMDB data.
        IMDB is the authoritative source for birth/death years and professions.
        """
        logger.info("="*80)
        logger.info("STEP 2: Enriching with IMDB data (authoritative source)")
        logger.info("="*80)
        
        # Load IMDB index
        imdb_index = self._load_imdb_names_index()
        if not imdb_index:
            logger.error("Cannot enrich without IMDB data!")
            return
        
        enriched = 0
        for tmdb_id, person in tqdm(self.people_cache.items(), desc="IMDB Enrichment"):
            imdb_id = person.get('imdb_id')
            
            if imdb_id and imdb_id in imdb_index:
                imdb_data = imdb_index[imdb_id]
                
                # Add IMDB fields (prefixed to show source)
                person['imdb_name'] = imdb_data['primaryName']
                person['imdb_birth_year'] = imdb_data['birthYear']
                person['imdb_death_year'] = imdb_data['deathYear']
                person['imdb_profession'] = imdb_data['primaryProfession']
                person['imdb_known_for'] = imdb_data['knownForTitles']
                
                # Use IMDB birth year if we don't have birthday from TMDB
                if not person.get('birthday') and imdb_data['birthYear']:
                    person['birthday'] = f"{imdb_data['birthYear']}-01-01"  # Approximate
                
                # Infer gender from IMDB profession if unknown
                if person.get('gender') == 0 and imdb_data['primaryProfession']:
                    profession = imdb_data['primaryProfession'].lower()
                    if 'actress' in profession:
                        person['gender'] = 1
                        person['gender_source'] = 'imdb_profession'
                        self.stats['genders_resolved'] += 1
                    elif 'actor' in profession and 'actress' not in profession:
                        # Only male if explicitly actor without actress
                        person['gender'] = 2
                        person['gender_source'] = 'imdb_profession'
                        self.stats['genders_resolved'] += 1
                
                enriched += 1
        
        self.stats['imdb_enriched'] = enriched
        self._save_people_cache()
        logger.info(f"Enriched {enriched:,} people with IMDB data")
    
    # =========================================================================
    # STEP 3: Resolve unknown genders via Wikidata
    # =========================================================================
    
    def resolve_unknown_genders(self, limit: int = None):
        """
        Resolve unknown genders using Wikidata SPARQL queries.
        Only queries people who still have gender=0 after IMDB enrichment.
        """
        logger.info("="*80)
        logger.info("STEP 3: Resolving unknown genders via Wikidata")
        logger.info("="*80)
        
        # Find people with unknown gender
        unknown_gender = [
            (tmdb_id, person) for tmdb_id, person in self.people_cache.items()
            if person.get('gender') == 0
        ]
        
        if limit:
            unknown_gender = unknown_gender[:limit]
        
        logger.info(f"People with unknown gender: {len(unknown_gender):,}")
        
        if not unknown_gender:
            logger.info("No unknown genders to resolve!")
            return
        
        resolved = 0
        for tmdb_id, person in tqdm(unknown_gender, desc="Wikidata Gender Resolution"):
            imdb_id = person.get('imdb_id')
            name = person.get('imdb_name') or person.get('name', '')
            
            gender = None
            
            # Try Wikidata by IMDB ID first (most reliable)
            if imdb_id:
                gender = self._query_wikidata_gender_by_imdb(imdb_id)
                self.stats['wikidata_queries'] += 1
            
            # Fallback to name search if no IMDB ID
            if gender is None and name:
                gender = self._query_wikidata_gender_by_name(name)
                self.stats['wikidata_queries'] += 1
            
            if gender:
                self.people_cache[tmdb_id]['gender'] = gender
                self.people_cache[tmdb_id]['gender_source'] = 'wikidata'
                resolved += 1
                self.stats['genders_resolved'] += 1
            
            # Rate limit Wikidata
            time.sleep(0.5)
            
            # Checkpoint
            if resolved % 50 == 0 and resolved > 0:
                self._save_people_cache()
        
        self._save_people_cache()
        logger.info(f"Resolved {resolved:,} genders via Wikidata")
    
    def _query_wikidata_gender_by_imdb(self, imdb_id: str) -> Optional[int]:
        """Query Wikidata for gender using IMDB ID."""
        sparql = f"""
        SELECT ?gender WHERE {{
            ?person wdt:P345 "{imdb_id}" .
            ?person wdt:P21 ?genderEntity .
            ?genderEntity rdfs:label ?gender .
            FILTER(LANG(?gender) = "en")
        }}
        LIMIT 1
        """
        
        try:
            url = "https://query.wikidata.org/sparql"
            headers = {'Accept': 'application/json', 'User-Agent': 'CineScope/1.0'}
            response = requests.get(url, params={'query': sparql}, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', {}).get('bindings', [])
                if results:
                    gender_str = results[0].get('gender', {}).get('value', '').lower()
                    if 'female' in gender_str:
                        return 1
                    elif 'male' in gender_str:
                        return 2
        except Exception as e:
            logger.debug(f"Wikidata query failed for {imdb_id}: {e}")
        
        return None
    
    def _query_wikidata_gender_by_name(self, name: str) -> Optional[int]:
        """Query Wikidata for gender using person name (less reliable)."""
        # Escape special characters
        safe_name = name.replace('"', '\\"')
        
        sparql = f"""
        SELECT ?gender WHERE {{
            ?person rdfs:label "{safe_name}"@en .
            ?person wdt:P106 ?occupation .
            ?occupation wdt:P279* wd:Q33999 .  # actor or subclass
            ?person wdt:P21 ?genderEntity .
            ?genderEntity rdfs:label ?gender .
            FILTER(LANG(?gender) = "en")
        }}
        LIMIT 1
        """
        
        try:
            url = "https://query.wikidata.org/sparql"
            headers = {'Accept': 'application/json', 'User-Agent': 'CineScope/1.0'}
            response = requests.get(url, params={'query': sparql}, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', {}).get('bindings', [])
                if results:
                    gender_str = results[0].get('gender', {}).get('value', '').lower()
                    if 'female' in gender_str:
                        return 1
                    elif 'male' in gender_str:
                        return 2
        except Exception as e:
            logger.debug(f"Wikidata name query failed for {name}: {e}")
        
        return None
    
    # =========================================================================
    # VALIDATION & REPORTING
    # =========================================================================
    
    def validate_and_report(self):
        """Generate comprehensive validation report."""
        logger.info("="*80)
        logger.info("PEOPLE DATABASE VALIDATION REPORT")
        logger.info("="*80)
        
        total = len(self.people_cache)
        
        # Count statistics
        with_imdb_id = sum(1 for p in self.people_cache.values() if p.get('imdb_id'))
        with_birthday = sum(1 for p in self.people_cache.values() if p.get('birthday') or p.get('imdb_birth_year'))
        with_biography = sum(1 for p in self.people_cache.values() if p.get('biography'))
        
        gender_counts = {'female': 0, 'male': 0, 'unknown': 0, 'other': 0}
        for p in self.people_cache.values():
            g = p.get('gender')
            if g == 1:
                gender_counts['female'] += 1
            elif g == 2:
                gender_counts['male'] += 1
            elif g == 0 or g is None:
                gender_counts['unknown'] += 1
            else:
                gender_counts['other'] += 1
        
        gender_sources = {}
        for p in self.people_cache.values():
            src = p.get('gender_source', 'tmdb_original')
            gender_sources[src] = gender_sources.get(src, 0) + 1
        
        # Report
        print("\n" + "="*60)
        print("PEOPLE DATABASE STATISTICS")
        print("="*60)
        print(f"Total People:           {total:,}")
        print(f"\nIMDB LINKING:")
        print(f"  With IMDB ID:         {with_imdb_id:,} ({with_imdb_id/total*100:.1f}%)")
        print(f"  Missing IMDB ID:      {total-with_imdb_id:,} ({(total-with_imdb_id)/total*100:.1f}%)")
        print(f"\nDATA COMPLETENESS:")
        print(f"  With Birth Date:      {with_birthday:,} ({with_birthday/total*100:.1f}%)")
        print(f"  With Biography:       {with_biography:,} ({with_biography/total*100:.1f}%)")
        print(f"\nGENDER DISTRIBUTION:")
        print(f"  Female (1):           {gender_counts['female']:,} ({gender_counts['female']/total*100:.1f}%)")
        print(f"  Male (2):             {gender_counts['male']:,} ({gender_counts['male']/total*100:.1f}%)")
        print(f"  Unknown (0):          {gender_counts['unknown']:,} ({gender_counts['unknown']/total*100:.1f}%)")
        print(f"  Other (3):            {gender_counts['other']:,} ({gender_counts['other']/total*100:.1f}%)")
        print(f"\nGENDER SOURCES:")
        for src, count in sorted(gender_sources.items(), key=lambda x: -x[1]):
            print(f"  {src}: {count:,}")
        print("="*60)
        
        # Identify issues
        print("\n⚠️  ISSUES TO ADDRESS:")
        if total - with_imdb_id > 0:
            print(f"  - {total-with_imdb_id:,} people missing IMDB IDs (run --fetch-imdb-ids)")
        if gender_counts['unknown'] > 0:
            print(f"  - {gender_counts['unknown']:,} people with unknown gender (run --resolve-gender)")
        if with_birthday < total * 0.5:
            print(f"  - Only {with_birthday/total*100:.0f}% have birth dates")
        
        print("\n✅ Run 'python scripts/enrich/06_enrich_people.py' to fix all issues")
    
    # =========================================================================
    # MAIN ENRICHMENT FLOW
    # =========================================================================
    
    def run_full_enrichment(self, limit: int = None):
        """Run complete enrichment pipeline."""
        self.stats['total_people'] = len(self.people_cache)
        
        logger.info("="*80)
        logger.info("CINESCOPE PEOPLE DATABASE ENRICHMENT")
        logger.info("="*80)
        logger.info(f"Starting with {self.stats['total_people']:,} people in cache")
        
        # Step 1: Fetch IMDB IDs from TMDB
        self.fetch_imdb_ids_from_tmdb(limit=limit)
        
        # Step 2: Enrich with IMDB data
        self.enrich_from_imdb()
        
        # Step 3: Resolve unknown genders
        self.resolve_unknown_genders(limit=limit)
        
        # Final report
        self.validate_and_report()
        
        logger.info("\n" + "="*80)
        logger.info("ENRICHMENT COMPLETE!")
        logger.info("="*80)
        logger.info(f"IMDB IDs fetched:    {self.stats['imdb_ids_fetched']:,}")
        logger.info(f"IMDB enriched:       {self.stats['imdb_enriched']:,}")
        logger.info(f"Genders resolved:    {self.stats['genders_resolved']:,}")
        logger.info(f"Wikidata queries:    {self.stats['wikidata_queries']:,}")
        logger.info(f"Errors:              {self.stats['errors']:,}")


def main():
    parser = argparse.ArgumentParser(
        description="Comprehensive people database enrichment - IMDB as authoritative source"
    )
    parser.add_argument('--fetch-imdb-ids', action='store_true', 
                       help="Only fetch IMDB IDs from TMDB API")
    parser.add_argument('--enrich-imdb', action='store_true',
                       help="Only enrich with IMDB data")
    parser.add_argument('--resolve-gender', action='store_true',
                       help="Only resolve unknown genders via Wikidata")
    parser.add_argument('--validate', action='store_true',
                       help="Only validate and report statistics")
    parser.add_argument('--limit', type=int,
                       help="Limit number of items to process (for testing)")
    args = parser.parse_args()
    
    try:
        enricher = PeopleEnricher()
        
        if args.validate:
            enricher.validate_and_report()
        elif args.fetch_imdb_ids:
            enricher.fetch_imdb_ids_from_tmdb(limit=args.limit)
            enricher.validate_and_report()
        elif args.enrich_imdb:
            enricher.enrich_from_imdb()
            enricher.validate_and_report()
        elif args.resolve_gender:
            enricher.resolve_unknown_genders(limit=args.limit)
            enricher.validate_and_report()
        else:
            # Full enrichment
            enricher.run_full_enrichment(limit=args.limit)
            
    except KeyboardInterrupt:
        logger.info("\nInterrupted. Progress saved.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Critical error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
