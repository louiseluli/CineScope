"""
Completeness Database Builder
Builds a comprehensive database of actor, director, genre, country, decade, and studio completeness.
This only needs to be run once, then updated incrementally when new movies are watched.
"""

import pandas as pd
import sqlite3
from collections import defaultdict
import ast
from pathlib import Path
import numpy as np
from datetime import datetime

class CompletenessDBBuilder:
    def __init__(self,
                 watched_path='data/processed/watched_movies_master.csv',
                 title_basics_path='data/raw/title.basics.tsv',
                 title_principals_path='data/raw/title.principals.tsv',
                 title_crew_path='data/raw/title.crew.tsv',
                 name_basics_path='data/raw/name.basics.tsv',
                 title_ratings_path='data/raw/title.ratings.tsv',
                 catalog_path='data/processed/master_cinema_data.csv'):

        self.watched_path = watched_path
        self.db_path = Path('data/processed/completeness.db')

        print("=" * 80)
        print("BUILDING COMPLETENESS DATABASE")
        print("=" * 80)

        # Load all data
        print("\nLoading datasets...")
        self.watched_df = pd.read_csv(watched_path)
        self.catalog_df = pd.read_csv(catalog_path)

        print("Loading IMDB data...")
        self.titles_df = pd.read_csv(title_basics_path, sep='\t', low_memory=False)
        self.titles_df = self.titles_df[
            (self.titles_df['titleType'].isin(['movie', 'tvMovie'])) &
            (self.titles_df['isAdult'] == '0')
        ]

        self.ratings_df = pd.read_csv(title_ratings_path, sep='\t')
        self.ratings_df = self.ratings_df[
            (self.ratings_df['averageRating'] >= 6.5) &
            (self.ratings_df['numVotes'] >= 1000)
        ]

        self.principals_df = pd.read_csv(title_principals_path, sep='\t', low_memory=False)
        self.principals_df = self.principals_df[
            self.principals_df['category'].isin(['actor', 'actress'])
        ]

        self.crew_df = pd.read_csv(title_crew_path, sep='\t', low_memory=False)
        self.names_df = pd.read_csv(name_basics_path, sep='\t', low_memory=False)

        # Merge quality titles
        self.quality_titles = pd.merge(self.titles_df, self.ratings_df, on='tconst', how='inner')

        # Initialize database
        self.conn = sqlite3.connect(self.db_path)
        self._create_tables()

    def _create_tables(self):
        """Create database tables."""
        cursor = self.conn.cursor()

        # Actor completeness table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS actor_completeness (
                actor_id TEXT PRIMARY KEY,
                actor_name TEXT,
                total_quality_films INTEGER,
                watched_count INTEGER,
                completeness_pct REAL,
                missing_count INTEGER,
                watched_avg_rating REAL,
                catalog_avg_rating REAL,
                watched_films TEXT,
                missing_films TEXT,
                last_updated TIMESTAMP
            )
        ''')

        # Director completeness table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS director_completeness (
                director_id TEXT PRIMARY KEY,
                director_name TEXT,
                total_quality_films INTEGER,
                watched_count INTEGER,
                completeness_pct REAL,
                missing_count INTEGER,
                watched_avg_rating REAL,
                catalog_avg_rating REAL,
                watched_films TEXT,
                missing_films TEXT,
                last_updated TIMESTAMP
            )
        ''')

        # Genre completeness table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS genre_completeness (
                genre TEXT PRIMARY KEY,
                total_quality_films INTEGER,
                watched_count INTEGER,
                completeness_pct REAL,
                missing_count INTEGER,
                watched_avg_rating REAL,
                catalog_avg_rating REAL,
                missing_films TEXT,
                last_updated TIMESTAMP
            )
        ''')

        # Country completeness table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS country_completeness (
                country TEXT PRIMARY KEY,
                total_quality_films INTEGER,
                watched_count INTEGER,
                completeness_pct REAL,
                missing_count INTEGER,
                watched_avg_rating REAL,
                catalog_avg_rating REAL,
                missing_films TEXT,
                last_updated TIMESTAMP
            )
        ''')

        # Decade completeness table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS decade_completeness (
                decade TEXT PRIMARY KEY,
                total_quality_films INTEGER,
                watched_count INTEGER,
                completeness_pct REAL,
                missing_count INTEGER,
                watched_avg_rating REAL,
                catalog_avg_rating REAL,
                missing_films TEXT,
                last_updated TIMESTAMP
            )
        ''')

        # Studio completeness table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS studio_completeness (
                studio TEXT PRIMARY KEY,
                total_quality_films INTEGER,
                watched_count INTEGER,
                completeness_pct REAL,
                missing_count INTEGER,
                watched_avg_rating REAL,
                catalog_avg_rating REAL,
                missing_films TEXT,
                last_updated TIMESTAMP
            )
        ''')

        # Metadata table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT,
                last_updated TIMESTAMP
            )
        ''')

        self.conn.commit()

    def _get_person_id_from_name(self, person_name):
        """Get person ID from IMDB name basics."""
        matches = self.names_df[self.names_df['primaryName'] == person_name]['nconst'].values
        return matches[0] if len(matches) > 0 else None

    def build_actor_completeness(self):
        """Build actor completeness data."""
        print("\n" + "-" * 80)
        print("Building Actor Completeness...")
        print("-" * 80)

        # Extract actors from watched movies
        watched_actors = defaultdict(list)
        for idx, row in self.watched_df.iterrows():
            if pd.notna(row.get('tmdb_cast')):
                try:
                    cast = ast.literal_eval(row['tmdb_cast'])
                    for actor in cast[:10]:
                        actor_name = actor.get('name', '')
                        if actor_name:
                            watched_actors[actor_name].append({
                                'title': row['Title'],
                                'year': row['Year'],
                                'rating': row['IMDb Rating']
                            })
                except:
                    continue

        print(f"Found {len(watched_actors)} actors in watched movies")

        # Check existing progress
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM actor_completeness")
        existing_count = cursor.fetchone()[0]
        if existing_count > 0:
            print(f"  (Database has {existing_count} existing actors - will update/add to them)")

        print(f"  Processing actors (commits every 50, safe to Ctrl+C and resume)...")
        processed = 0
        updated = 0
        added = 0

        for actor_name, watched_films in list(watched_actors.items()):
            actor_id = self._get_person_id_from_name(actor_name)
            if not actor_id:
                continue

            # Get complete filmography from IMDB
            actor_imdb_films = self.principals_df[self.principals_df['nconst'] == actor_id]
            actor_quality_films = actor_imdb_films[actor_imdb_films['tconst'].isin(self.quality_titles['tconst'])]

            if len(actor_quality_films) < 5:
                continue

            # Get film details
            total_films = []
            for _, film_row in actor_quality_films.iterrows():
                film_info = self.quality_titles[self.quality_titles['tconst'] == film_row['tconst']]
                if len(film_info) > 0:
                    film = film_info.iloc[0]
                    total_films.append({
                        'title': film['primaryTitle'],
                        'year': film.get('startYear', 'N/A'),
                        'rating': film.get('averageRating', 0)
                    })

            # Calculate completeness
            watched_titles_lower = set(f['title'].lower() for f in watched_films)
            missing_films = [f for f in total_films if f['title'].lower() not in watched_titles_lower]
            missing_films = sorted(missing_films, key=lambda x: x['rating'], reverse=True)[:20]

            total_count = len(total_films)
            watched_count = len(watched_films)
            completeness_pct = (watched_count / total_count * 100) if total_count > 0 else 0

            watched_avg = np.mean([f['rating'] for f in watched_films]) if watched_films else 0
            catalog_avg = np.mean([f['rating'] for f in total_films])

            # Insert into database
            cursor.execute('''
                INSERT OR REPLACE INTO actor_completeness VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                actor_id,
                actor_name,
                total_count,
                watched_count,
                completeness_pct,
                len(missing_films),
                watched_avg,
                catalog_avg,
                str(watched_films),
                str(missing_films),
                datetime.now()
            ))

            processed += 1
            if processed % 50 == 0:
                print(f"  Processed {processed} actors...")
                self.conn.commit()

        self.conn.commit()
        print(f"✓ Completed: {processed} actors stored")

    def build_director_completeness(self):
        """Build director completeness data."""
        print("\n" + "-" * 80)
        print("Building Director Completeness...")
        print("-" * 80)

        # Extract directors from watched movies
        watched_directors = defaultdict(list)
        for idx, row in self.watched_df.iterrows():
            directors = row.get('Directors', '')
            if pd.notna(directors):
                director_list = [d.strip() for d in str(directors).split(',')] if ',' in str(directors) else [str(directors).strip()]
                for director_name in director_list:
                    if director_name:
                        watched_directors[director_name].append({
                            'title': row['Title'],
                            'year': row['Year'],
                            'rating': row['IMDb Rating']
                        })

        print(f"Found {len(watched_directors)} directors in watched movies")

        # Check existing progress
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM director_completeness")
        existing_count = cursor.fetchone()[0]
        if existing_count > 0:
            print(f"  (Database has {existing_count} existing directors - will update/add to them)")

        print(f"  Processing directors (commits every 50, safe to Ctrl+C and resume)...")
        processed = 0

        for director_name, watched_films in list(watched_directors.items()):
            director_id = self._get_person_id_from_name(director_name)
            if not director_id:
                continue

            # Get complete filmography
            director_film_ids = []
            for _, crew_row in self.crew_df.iterrows():
                directors_str = crew_row.get('directors', '')
                if pd.notna(directors_str) and directors_str != '\\N':
                    if director_id in directors_str.split(','):
                        director_film_ids.append(crew_row['tconst'])

            if len(director_film_ids) < 3:
                continue

            quality_director_films = self.quality_titles[self.quality_titles['tconst'].isin(director_film_ids)]

            total_films = []
            for _, film in quality_director_films.iterrows():
                total_films.append({
                    'title': film['primaryTitle'],
                    'year': film.get('startYear', 'N/A'),
                    'rating': film.get('averageRating', 0)
                })

            # Calculate completeness
            watched_titles_lower = set(f['title'].lower() for f in watched_films)
            missing_films = [f for f in total_films if f['title'].lower() not in watched_titles_lower]
            missing_films = sorted(missing_films, key=lambda x: x['rating'], reverse=True)[:20]

            total_count = len(total_films)
            watched_count = len(watched_films)
            completeness_pct = (watched_count / total_count * 100) if total_count > 0 else 0

            watched_avg = np.mean([f['rating'] for f in watched_films]) if watched_films else 0
            catalog_avg = np.mean([f['rating'] for f in total_films])

            cursor.execute('''
                INSERT OR REPLACE INTO director_completeness VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                director_id,
                director_name,
                total_count,
                watched_count,
                completeness_pct,
                len(missing_films),
                watched_avg,
                catalog_avg,
                str(watched_films),
                str(missing_films),
                datetime.now()
            ))

            processed += 1
            if processed % 50 == 0:
                print(f"  Processed {processed} directors...")
                self.conn.commit()

        self.conn.commit()
        print(f"✓ Completed: {processed} directors stored")

    def build_genre_completeness(self):
        """Build genre completeness data."""
        print("\n" + "-" * 80)
        print("Building Genre Completeness...")
        print("-" * 80)

        # Extract genres from both datasets
        watched_genres = defaultdict(list)
        catalog_genres = defaultdict(list)

        for idx, row in self.watched_df.iterrows():
            if pd.notna(row.get('Genres')):
                genres = [g.strip() for g in str(row['Genres']).split(',')]
                for genre in genres:
                    watched_genres[genre].append({
                        'title': row['Title'],
                        'year': row['Year'],
                        'rating': row['IMDb Rating']
                    })

        for idx, row in self.catalog_df.iterrows():
            if pd.notna(row.get('genres')) and row.get('imdb_rating', 0) >= 6.5:
                genres = [g.strip() for g in str(row['genres']).split(',')]
                for genre in genres:
                    catalog_genres[genre].append({
                        'title': row['title'],
                        'year': row['year'],
                        'rating': row['imdb_rating']
                    })

        cursor = self.conn.cursor()
        processed = 0

        for genre in watched_genres.keys():
            if genre not in catalog_genres or len(catalog_genres[genre]) < 10:
                continue

            watched_films = watched_genres[genre]
            catalog_films = catalog_genres[genre]

            watched_titles_lower = set(f['title'].lower() for f in watched_films)
            missing_films = [f for f in catalog_films if f['title'].lower() not in watched_titles_lower]
            missing_films = sorted(missing_films, key=lambda x: x['rating'], reverse=True)[:20]

            total_count = len(catalog_films)
            watched_count = len(watched_films)
            completeness_pct = (watched_count / total_count * 100) if total_count > 0 else 0

            watched_avg = np.mean([f['rating'] for f in watched_films])
            catalog_avg = np.mean([f['rating'] for f in catalog_films])

            cursor.execute('''
                INSERT OR REPLACE INTO genre_completeness VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                genre,
                total_count,
                watched_count,
                completeness_pct,
                len(missing_films),
                watched_avg,
                catalog_avg,
                str(missing_films),
                datetime.now()
            ))

            processed += 1

        self.conn.commit()
        print(f"✓ Completed: {processed} genres stored")

    def build_country_completeness(self):
        """Build country completeness data."""
        print("\n" + "-" * 80)
        print("Building Country Completeness...")
        print("-" * 80)

        # Extract countries from both datasets
        watched_countries = defaultdict(list)
        catalog_countries = defaultdict(list)

        for idx, row in self.watched_df.iterrows():
            if pd.notna(row.get('tmdb_production_countries')):
                try:
                    countries = ast.literal_eval(row['tmdb_production_countries'])
                    for country in countries:
                        country_name = country.get('name', '')
                        if country_name:
                            watched_countries[country_name].append({
                                'title': row['Title'],
                                'year': row['Year'],
                                'rating': row['IMDb Rating']
                            })
                except:
                    continue

        for idx, row in self.catalog_df.iterrows():
            if pd.notna(row.get('tmdb_production_countries')) and row.get('imdb_rating', 0) >= 6.5:
                try:
                    countries = ast.literal_eval(row['tmdb_production_countries'])
                    for country in countries:
                        country_name = country.get('name', '')
                        if country_name:
                            catalog_countries[country_name].append({
                                'title': row['title'],
                                'year': row['year'],
                                'rating': row['imdb_rating']
                            })
                except:
                    continue

        cursor = self.conn.cursor()
        processed = 0

        for country in watched_countries.keys():
            if country not in catalog_countries or len(catalog_countries[country]) < 10:
                continue

            watched_films = watched_countries[country]
            catalog_films = catalog_countries[country]

            watched_titles_lower = set(f['title'].lower() for f in watched_films)
            missing_films = [f for f in catalog_films if f['title'].lower() not in watched_titles_lower]
            missing_films = sorted(missing_films, key=lambda x: x['rating'], reverse=True)[:20]

            total_count = len(catalog_films)
            watched_count = len(watched_films)
            completeness_pct = (watched_count / total_count * 100) if total_count > 0 else 0

            watched_avg = np.mean([f['rating'] for f in watched_films])
            catalog_avg = np.mean([f['rating'] for f in catalog_films])

            cursor.execute('''
                INSERT OR REPLACE INTO country_completeness VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                country,
                total_count,
                watched_count,
                completeness_pct,
                len(missing_films),
                watched_avg,
                catalog_avg,
                str(missing_films),
                datetime.now()
            ))

            processed += 1

        self.conn.commit()
        print(f"✓ Completed: {processed} countries stored")

    def build_decade_completeness(self):
        """Build decade completeness data."""
        print("\n" + "-" * 80)
        print("Building Decade Completeness...")
        print("-" * 80)

        # Define decades
        decades = {}
        for year in range(1920, 2030, 10):
            decade_label = f"{year}s"
            decades[decade_label] = (year, year + 9)

        watched_decades = defaultdict(list)
        catalog_decades = defaultdict(list)

        for idx, row in self.watched_df.iterrows():
            year = row.get('Year', 0)
            if year > 0:
                decade = f"{(year // 10) * 10}s"
                watched_decades[decade].append({
                    'title': row['Title'],
                    'year': year,
                    'rating': row['IMDb Rating']
                })

        for idx, row in self.catalog_df.iterrows():
            if row.get('imdb_rating', 0) >= 6.5:
                year = row.get('year', 0)
                if year > 0:
                    decade = f"{(year // 10) * 10}s"
                    catalog_decades[decade].append({
                        'title': row['title'],
                        'year': year,
                        'rating': row['imdb_rating']
                    })

        cursor = self.conn.cursor()
        processed = 0

        for decade in watched_decades.keys():
            if decade not in catalog_decades or len(catalog_decades[decade]) < 10:
                continue

            watched_films = watched_decades[decade]
            catalog_films = catalog_decades[decade]

            watched_titles_lower = set(f['title'].lower() for f in watched_films)
            missing_films = [f for f in catalog_films if f['title'].lower() not in watched_titles_lower]
            missing_films = sorted(missing_films, key=lambda x: x['rating'], reverse=True)[:20]

            total_count = len(catalog_films)
            watched_count = len(watched_films)
            completeness_pct = (watched_count / total_count * 100) if total_count > 0 else 0

            watched_avg = np.mean([f['rating'] for f in watched_films])
            catalog_avg = np.mean([f['rating'] for f in catalog_films])

            cursor.execute('''
                INSERT OR REPLACE INTO decade_completeness VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                decade,
                total_count,
                watched_count,
                completeness_pct,
                len(missing_films),
                watched_avg,
                catalog_avg,
                str(missing_films),
                datetime.now()
            ))

            processed += 1

        self.conn.commit()
        print(f"✓ Completed: {processed} decades stored")

    def build_studio_completeness(self):
        """Build studio completeness data."""
        print("\n" + "-" * 80)
        print("Building Studio Completeness...")
        print("-" * 80)

        # Extract studios from both datasets
        watched_studios = defaultdict(list)
        catalog_studios = defaultdict(list)

        for idx, row in self.watched_df.iterrows():
            if pd.notna(row.get('tmdb_production_companies')):
                try:
                    companies = ast.literal_eval(row['tmdb_production_companies'])
                    for company in companies:
                        studio_name = company.get('name', '')
                        if studio_name:
                            watched_studios[studio_name].append({
                                'title': row['Title'],
                                'year': row['Year'],
                                'rating': row['IMDb Rating']
                            })
                except:
                    continue

        for idx, row in self.catalog_df.iterrows():
            if pd.notna(row.get('tmdb_production_companies')) and row.get('imdb_rating', 0) >= 6.5:
                try:
                    companies = ast.literal_eval(row['tmdb_production_companies'])
                    for company in companies:
                        studio_name = company.get('name', '')
                        if studio_name:
                            catalog_studios[studio_name].append({
                                'title': row['title'],
                                'year': row['year'],
                                'rating': row['imdb_rating']
                            })
                except:
                    continue

        cursor = self.conn.cursor()
        processed = 0

        for studio in watched_studios.keys():
            if studio not in catalog_studios or len(catalog_studios[studio]) < 5:
                continue

            watched_films = watched_studios[studio]
            catalog_films = catalog_studios[studio]

            watched_titles_lower = set(f['title'].lower() for f in watched_films)
            missing_films = [f for f in catalog_films if f['title'].lower() not in watched_titles_lower]
            missing_films = sorted(missing_films, key=lambda x: x['rating'], reverse=True)[:20]

            total_count = len(catalog_films)
            watched_count = len(watched_films)
            completeness_pct = (watched_count / total_count * 100) if total_count > 0 else 0

            watched_avg = np.mean([f['rating'] for f in watched_films])
            catalog_avg = np.mean([f['rating'] for f in catalog_films])

            cursor.execute('''
                INSERT OR REPLACE INTO studio_completeness VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                studio,
                total_count,
                watched_count,
                completeness_pct,
                len(missing_films),
                watched_avg,
                catalog_avg,
                str(missing_films),
                datetime.now()
            ))

            processed += 1

        self.conn.commit()
        print(f"✓ Completed: {processed} studios stored")

    def save_metadata(self):
        """Save metadata about the database."""
        cursor = self.conn.cursor()

        cursor.execute('''
            INSERT OR REPLACE INTO metadata VALUES (?, ?, ?)
        ''', ('total_watched_films', str(len(self.watched_df)), datetime.now()))

        cursor.execute('''
            INSERT OR REPLACE INTO metadata VALUES (?, ?, ?)
        ''', ('last_built', str(datetime.now()), datetime.now()))

        cursor.execute('''
            INSERT OR REPLACE INTO metadata VALUES (?, ?, ?)
        ''', ('watched_file_path', self.watched_path, datetime.now()))

        self.conn.commit()

    def build_all(self):
        """Build complete database."""
        print("\nStarting database build...")
        print(f"Database will be saved to: {self.db_path}")

        self.build_actor_completeness()
        self.build_director_completeness()
        self.build_genre_completeness()
        self.build_country_completeness()
        self.build_decade_completeness()
        self.build_studio_completeness()
        self.save_metadata()

        print("\n" + "=" * 80)
        print("DATABASE BUILD COMPLETE!")
        print("=" * 80)
        print(f"\nDatabase saved to: {self.db_path}")
        print(f"Size: {self.db_path.stat().st_size / 1024 / 1024:.2f} MB")

        # Print summary
        cursor = self.conn.cursor()

        print("\nDatabase Summary:")
        print("-" * 80)

        cursor.execute("SELECT COUNT(*) FROM actor_completeness")
        print(f"Actors: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM director_completeness")
        print(f"Directors: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM genre_completeness")
        print(f"Genres: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM country_completeness")
        print(f"Countries: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM decade_completeness")
        print(f"Decades: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM studio_completeness")
        print(f"Studios: {cursor.fetchone()[0]}")

        self.conn.close()

if __name__ == "__main__":
    builder = CompletenessDBBuilder()
    builder.build_all()
