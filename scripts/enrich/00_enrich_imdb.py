#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CineScope IMDb Enrichment Script (Step 0)

Parses raw IMDb TSV files and creates a comprehensive IMDb enrichment dataset.
This is the FOUNDATION for all subsequent enrichments.

Includes:
- Title data: primaryTitle, originalTitle, titleType, year, runtime
- Crew: directors (names + IDs), writers (names + IDs)
- Cast: full principals with roles and billing order
- Ratings: IMDb ratings and vote counts
- Alternative titles (AKAs): including original titles in multiple languages

Data sources:
- title.basics.tsv: Primary movie/show metadata
- title.crew.tsv: Directors and writers (IDs only)
- title.principals.tsv: Full cast/crew with roles (4GB file)
- title.ratings.tsv: IMDb ratings and vote counts
- title.akas.tsv.gz: Alternative titles including originals
- name.basics.tsv: People metadata (names, professions, birth/death years)

Usage:
    python scripts/enrich/00_enrich_imdb.py
    python scripts/enrich/00_enrich_imdb.py --limit 1000 (process first 1000)
"""
import sys
import pandas as pd
import gzip
import logging
from pathlib import Path
from typing import Dict, List, Optional
from tqdm import tqdm
import argparse

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.config import PROCESSED_DATA_DIR, BASE_DIR

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(PROCESSED_DATA_DIR / "imdb_enrichment.log")
    ]
)
logger = logging.getLogger(__name__)


class IMDbEnricher:
    """Parses raw IMDb TSV files into a comprehensive enrichment dataset."""

    def __init__(self):
        self.raw_dir = BASE_DIR / "data" / "raw"
        self.output_file = PROCESSED_DATA_DIR / "00_imdb_enriched_media.csv"
        
        # IMDb file paths
        self.basics_file = self.raw_dir / "title.basics.tsv"
        self.crew_file = self.raw_dir / "title.crew.tsv"
        self.principals_file = self.raw_dir / "title.principals.tsv"
        self.ratings_file = self.raw_dir / "title.ratings.tsv"
        self.akas_file = self.raw_dir / "title.akas.tsv.gz"
        self.names_file = self.raw_dir / "name.basics.tsv"

        # Data caches
        self.names_df = None
        self.akas_df = None
        self.crew_df = None
        self.principals_df = None
        self.ratings_df = None

    def run(self, limit: Optional[int] = None):
        """Execute the full IMDb enrichment pipeline."""
        logger.info("=" * 80)
        logger.info("Starting my IMDb Enrichment Pipeline")
        logger.info("=" * 80)
        
        # Load source data
        basics_df = self._load_basics(limit)
        
        if basics_df.empty:
            logger.error("No data loaded from title.basics. Exiting.")
            return
        
        logger.info(f"Loaded {len(basics_df)} titles from IMDb basics file")
        
        # Load supporting data
        logger.info("Loading supporting IMDb data files...")
        self.names_df = self._load_names()
        self.akas_df = self._load_akas()
        self.crew_df = self._load_crew()
        self.principals_df = self._load_principals()
        self.ratings_df = self._load_ratings()
        
        # Enrich the basics data
        logger.info("Enriching titles with crew, cast, ratings, and alternative titles...")
        enriched_df = self._enrich_titles(basics_df)
        
        # Save output
        enriched_df.to_csv(self.output_file, index=False)
        logger.info(f"✅ IMDb enrichment complete!")
        logger.info(f"Total enriched titles: {len(enriched_df)}")
        logger.info(f"Output saved to: {self.output_file}")
        logger.info("=" * 80)

    def _load_basics(self, limit: Optional[int] = None) -> pd.DataFrame:
        """Load title.basics with proper type handling."""
        logger.info(f"Loading title.basics from {self.basics_file}")
        
        df = pd.read_csv(
            self.basics_file,
            sep='\t',
            dtype={
                'tconst': str,
                'titleType': str,
                'primaryTitle': str,
                'originalTitle': str,
                'isAdult': int,
                'startYear': str,
                'endYear': str,
                'runtimeMinutes': str,
                'genres': str
            },
            na_values=['\\N', 'NULL', 'None'],
            nrows=limit
        )
        
        # Rename for consistency
        df.rename(columns={'tconst': 'const'}, inplace=True)
        
        # Convert years to integers where possible
        df['startYear'] = pd.to_numeric(df['startYear'], errors='coerce')
        df['endYear'] = pd.to_numeric(df['endYear'], errors='coerce')
        df['runtimeMinutes'] = pd.to_numeric(df['runtimeMinutes'], errors='coerce')
        
        # Parse genres into lists
        df['genres'] = df['genres'].apply(
            lambda x: x.split(',') if pd.notna(x) else []
        )
        
        logger.info(f"Loaded {len(df)} titles")
        return df

    def _load_names(self) -> pd.DataFrame:
        """Load name.basics for crew/cast enrichment."""
        logger.info(f"Loading name.basics from {self.names_file}")
        
        df = pd.read_csv(
            self.names_file,
            sep='\t',
            dtype={
                'nconst': str,
                'primaryName': str,
                'birthYear': str,
                'deathYear': str,
                'primaryProfession': str,
                'knownForTitles': str
            },
            na_values=['\\N', 'NULL', 'None']
        )
        
        logger.info(f"Loaded {len(df)} people")
        return df

    def _load_crew(self) -> pd.DataFrame:
        """Load title.crew (directors and writers)."""
        logger.info(f"Loading title.crew from {self.crew_file}")
        
        df = pd.read_csv(
            self.crew_file,
            sep='\t',
            dtype={'tconst': str, 'directors': str, 'writers': str},
            na_values=['\\N', 'NULL', 'None']
        )
        
        df.rename(columns={'tconst': 'const'}, inplace=True)
        logger.info(f"Loaded crew data for {len(df)} titles")
        return df

    def _load_principals(self) -> pd.DataFrame:
        """Load title.principals (cast and crew with roles) - selective loading."""
        logger.info(f"Loading title.principals from {self.principals_file}")
        logger.info("Note: This is a large file (3.8GB), parsing may take time...")
        
        df = pd.read_csv(
            self.principals_file,
            sep='\t',
            dtype={
                'tconst': str,
                'ordering': int,
                'nconst': str,
                'category': str,
                'job': str,
                'characters': str
            },
            na_values=['\\N', 'NULL', 'None']
        )
        
        df.rename(columns={'tconst': 'const'}, inplace=True)
        logger.info(f"Loaded {len(df)} principal entries")
        return df

    def _load_ratings(self) -> pd.DataFrame:
        """Load title.ratings (IMDb ratings and vote counts)."""
        logger.info(f"Loading title.ratings from {self.ratings_file}")
        
        df = pd.read_csv(
            self.ratings_file,
            sep='\t',
            dtype={
                'tconst': str,
                'averageRating': float,
                'numVotes': int
            }
        )
        
        df.rename(columns={
            'tconst': 'const',
            'averageRating': 'imdb_rating',
            'numVotes': 'imdb_num_votes'
        }, inplace=True)
        
        logger.info(f"Loaded ratings for {len(df)} titles")
        return df

    def _load_akas(self) -> pd.DataFrame:
        """Load title.akas (alternative titles, including originals in other languages)."""
        logger.info(f"Loading title.akas from {self.akas_file}")
        
        # Handle gzipped file
        with gzip.open(self.akas_file, 'rt') as f:
            df = pd.read_csv(
                f,
                sep='\t',
                dtype={
                    'titleId': str,
                    'ordering': int,
                    'title': str,
                    'region': str,
                    'language': str,
                    'types': str,
                    'attributes': str,
                    'isOriginalTitle': int
                },
                na_values=['\\N', 'NULL', 'None']
            )
        
        df.rename(columns={'titleId': 'const'}, inplace=True)
        logger.info(f"Loaded {len(df)} alternative titles")
        return df

    def _enrich_titles(self, basics_df: pd.DataFrame) -> pd.DataFrame:
        """Enrich titles with crew, cast, ratings, and alternative titles."""
        df = basics_df.copy()
        
        # Add ratings
        if self.ratings_df is not None:
            df = df.merge(self.ratings_df, on='const', how='left')
        
        # Add crew (directors and writers)
        if self.crew_df is not None:
            df = df.merge(self.crew_df, on='const', how='left')
            # Enrich director and writer names
            df = self._enrich_crew_names(df)
        
        # Add cast and crew with roles
        if self.principals_df is not None:
            df = self._add_cast_data(df)
        
        # Add alternative titles
        if self.akas_df is not None:
            df = self._add_alternative_titles(df)
        
        return df

    def _enrich_crew_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert director/writer IDs to names using name.basics."""
        if self.names_df is None:
            return df
        
        # Create lookup dictionary for names
        name_lookup = dict(zip(self.names_df['nconst'], self.names_df['primaryName']))
        
        def resolve_names(id_str):
            """Convert comma-separated IDs to comma-separated names."""
            if pd.isna(id_str):
                return None
            ids = str(id_str).split(',')
            names = [name_lookup.get(id_.strip(), id_.strip()) for id_ in ids]
            return ','.join(names)
        
        if 'directors' in df.columns:
            df['director_names'] = df['directors'].apply(resolve_names)
        if 'writers' in df.columns:
            df['writer_names'] = df['writers'].apply(resolve_names)
        
        return df

    def _add_cast_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add cast data from principals (actors with their top roles)."""
        logger.info("Processing cast data from principals...")
        
        # Filter for actors only, sort by billing order
        actors = self.principals_df[self.principals_df['category'] == 'actor'].copy()
        actors = actors.sort_values(['const', 'ordering'])
        
        # Get top 5 actors per title
        top_actors = actors.groupby('const').head(5)
        
        # Create actor aggregations
        actor_aggs = []
        for const, group in tqdm(top_actors.groupby('const'), desc="Aggregating cast"):
            cast_list = []
            for _, row in group.iterrows():
                person_name = self.names_df[self.names_df['nconst'] == row['nconst']]['primaryName'].values
                person_name = person_name[0] if len(person_name) > 0 else row['nconst']
                
                character = row.get('characters', '').strip('[]"').split(',')[0] if pd.notna(row.get('characters')) else 'N/A'
                cast_list.append(f"{person_name} ({character})")
            
            actor_aggs.append({
                'const': const,
                'top_cast': ' | '.join(cast_list)
            })
        
        if actor_aggs:
            cast_df = pd.DataFrame(actor_aggs)
            df = df.merge(cast_df, on='const', how='left')
        
        return df

    def _add_alternative_titles(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add alternative titles, preferring original titles."""
        logger.info("Processing alternative titles...")
        
        alt_titles = []
        for const, group in tqdm(self.akas_df.groupby('const'), desc="Processing AKAs"):
            # Get original title
            originals = group[group['isOriginalTitle'] == 1]
            if not originals.empty:
                original = originals.iloc[0]['title']
            else:
                original = None
            
            # Get all titles joined
            all_titles = ' | '.join(group['title'].unique())
            
            alt_titles.append({
                'const': const,
                'original_title_aka': original,
                'all_alternate_titles': all_titles
            })
        
        if alt_titles:
            akas_df = pd.DataFrame(alt_titles)
            df = df.merge(akas_df, on='const', how='left')
        
        return df


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Enrich my movie data with comprehensive IMDb information'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit number of titles to process (for testing)'
    )
    
    args = parser.parse_args()
    
    enricher = IMDbEnricher()
    enricher.run(limit=args.limit)


if __name__ == '__main__':
    main()
