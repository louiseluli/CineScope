#!/usr/bin/env python3
"""
CineScope Batch 13: Education & Origins
Analyzes educational backgrounds and geographic birthplaces of cinema people.

Key Questions:
1. Which universities/schools produce the most cinema talent?
2. What's the geographic distribution of cinema talent origins?
3. Do education patterns correlate with success (ratings, awards)?
4. Are there "alumni networks" (same-school collaborations)?
5. How do education levels vary by profession (actors vs directors)?

Data Sources:
- ext_education: Pipe-separated list of schools/universities
- place_of_birth: Full birthplace string (City, State/Province, Country)
- wd_birthplace: Wikidata birthplace (often city only)
- imdb_profession: Comma-separated professions
- Movie ratings and awards data

Statistical Methods:
- T-tests for education impact on quality
- Geographic clustering analysis
- Network analysis of alumni collaborations
- Coverage: ~10,877 people with education data (25.5%)
"""

import os
import sys
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter, defaultdict
from datetime import datetime
import warnings
from scipy import stats
from typing import Dict, List, Tuple, Set

warnings.filterwarnings('ignore')

# Set style for professional visualizations
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

class EducationOriginsAnalyzer:
    """Analyze education and geographic origins of cinema people."""

    def __init__(self, data_dir: str = 'data/processed', output_dir: str = 'analysis_outputs'):
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.vis_dir = os.path.join(output_dir, 'visualizations', 'batch_13')
        self.report_path = os.path.join(output_dir, 'reports', 'batch_13_education_origins_report.txt')

        # Create output directories
        os.makedirs(self.vis_dir, exist_ok=True)
        os.makedirs(os.path.join(output_dir, 'reports'), exist_ok=True)

        # Load data
        self.movies_df = None
        self.people_cache = None
        self.education_df = None
        self.birthplace_df = None

        # Analysis results storage
        self.stats = {}

    def load_data(self):
        """Load movie and people data."""
        print("Loading data...")

        # Load movies
        movies_path = os.path.join(self.data_dir, 'watched_movies_master.csv')
        self.movies_df = pd.read_csv(movies_path)
        print(f"  Loaded {len(self.movies_df)} movies")

        # Load people cache
        cache_path = os.path.join(self.data_dir, 'people_cache.json')
        with open(cache_path, 'r') as f:
            self.people_cache = json.load(f)
        print(f"  Loaded {len(self.people_cache)} people")

    def extract_education_data(self):
        """Extract and structure education data from people cache."""
        print("\nExtracting education data...")

        education_records = []

        for person_id, person_data in self.people_cache.items():
            # Get education field
            education_str = person_data.get('ext_education', '')

            # Validate education data
            if not education_str or pd.isna(education_str) or str(education_str).strip() == '' or str(education_str) == 'None':
                continue

            # Parse pipe-separated schools
            schools = [s.strip() for s in str(education_str).split('|') if s.strip()]

            if not schools:
                continue

            # Extract person metadata
            name = person_data.get('imdb_name', 'Unknown')
            professions_str = person_data.get('imdb_profession') or ''
            professions = str(professions_str).split(',') if professions_str else []
            professions = [p.strip() for p in professions if p.strip()]

            # Determine primary profession
            primary_profession = 'Other'
            if 'actor' in professions or 'actress' in professions:
                primary_profession = 'Actor'
            elif 'director' in professions:
                primary_profession = 'Director'
            elif 'producer' in professions:
                primary_profession = 'Producer'
            elif 'writer' in professions:
                primary_profession = 'Writer'

            # Get additional metadata
            birth_year = person_data.get('imdb_birth_year')
            popularity = person_data.get('popularity', 0)

            # Create record for each school
            for school in schools:
                education_records.append({
                    'person_id': person_id,
                    'name': name,
                    'school': school,
                    'primary_profession': primary_profession,
                    'all_professions': '|'.join(professions),
                    'birth_year': birth_year,
                    'popularity': popularity
                })

        self.education_df = pd.DataFrame(education_records)
        print(f"  Extracted {len(self.education_df)} education records from {self.education_df['person_id'].nunique()} people")

        # Store stats
        self.stats['total_people_with_education'] = self.education_df['person_id'].nunique()
        self.stats['total_education_records'] = len(self.education_df)
        self.stats['total_unique_schools'] = self.education_df['school'].nunique()

    def extract_birthplace_data(self):
        """Extract and structure birthplace data from people cache."""
        print("\nExtracting birthplace data...")

        birthplace_records = []

        for person_id, person_data in self.people_cache.items():
            # Get birthplace (prefer full place_of_birth, fallback to wd_birthplace)
            birthplace = person_data.get('place_of_birth', '') or person_data.get('wd_birthplace', '')

            # Validate birthplace data
            if not birthplace or pd.isna(birthplace) or str(birthplace).strip() == '' or str(birthplace) == 'None':
                continue

            birthplace = str(birthplace).strip()

            # Extract person metadata
            name = person_data.get('imdb_name', 'Unknown')
            professions_str = person_data.get('imdb_profession') or ''
            professions = str(professions_str).split(',') if professions_str else []
            professions = [p.strip() for p in professions if p.strip()]

            # Determine primary profession
            primary_profession = 'Other'
            if 'actor' in professions or 'actress' in professions:
                primary_profession = 'Actor'
            elif 'director' in professions:
                primary_profession = 'Director'
            elif 'producer' in professions:
                primary_profession = 'Producer'
            elif 'writer' in professions:
                primary_profession = 'Writer'

            # Parse birthplace into components
            # Format is typically: "City, State/Province, Country" or "City, Country"
            parts = [p.strip() for p in birthplace.split(',')]

            city = parts[0] if len(parts) > 0 else 'Unknown'
            country = parts[-1] if len(parts) > 0 else 'Unknown'
            state_province = parts[1] if len(parts) > 2 else ''

            # Get additional metadata
            birth_year = person_data.get('imdb_birth_year')
            popularity = person_data.get('popularity', 0)

            birthplace_records.append({
                'person_id': person_id,
                'name': name,
                'birthplace_full': birthplace,
                'city': city,
                'country': country,
                'state_province': state_province,
                'primary_profession': primary_profession,
                'all_professions': '|'.join(professions),
                'birth_year': birth_year,
                'popularity': popularity
            })

        self.birthplace_df = pd.DataFrame(birthplace_records)
        print(f"  Extracted {len(self.birthplace_df)} birthplace records")

        # Store stats
        self.stats['total_people_with_birthplace'] = len(self.birthplace_df)
        self.stats['total_unique_cities'] = self.birthplace_df['city'].nunique()
        self.stats['total_unique_countries'] = self.birthplace_df['country'].nunique()

    def visualize_top_schools(self):
        """Visualize top universities/schools producing cinema talent."""
        print("\nGenerating top schools visualization...")

        # Count people per school
        school_counts = self.education_df['school'].value_counts().head(20)

        # Create figure
        fig, ax = plt.subplots(figsize=(14, 10))

        # Horizontal bar chart
        y_pos = np.arange(len(school_counts))
        bars = ax.barh(y_pos, school_counts.values, color=sns.color_palette("viridis", len(school_counts)))

        ax.set_yticks(y_pos)
        ax.set_yticklabels(school_counts.index, fontsize=10)
        ax.invert_yaxis()
        ax.set_xlabel('Number of People', fontsize=12, fontweight='bold')
        ax.set_title('Top 20 Schools/Universities Producing Cinema Talent',
                     fontsize=14, fontweight='bold', pad=20)

        # Add value labels
        for i, v in enumerate(school_counts.values):
            ax.text(v + 0.5, i, str(v), va='center', fontsize=9)

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '01_top_schools.png'), dpi=300, bbox_inches='tight')
        plt.close()

        print(f"  Top school: {school_counts.index[0]} with {school_counts.values[0]} people")

    def visualize_education_by_profession(self):
        """Visualize education patterns by profession."""
        print("\nGenerating education by profession visualization...")

        # Count people with education by profession
        profession_edu_counts = self.education_df.groupby('primary_profession')['person_id'].nunique().sort_values(ascending=False)

        # Count total people by profession (from people_cache)
        total_by_profession = defaultdict(int)
        for person_data in self.people_cache.values():
            professions_str = person_data.get('imdb_profession') or ''
            professions = str(professions_str).split(',') if professions_str else []
            professions = [p.strip() for p in professions if p.strip()]

            if 'actor' in professions or 'actress' in professions:
                total_by_profession['Actor'] += 1
            elif 'director' in professions:
                total_by_profession['Director'] += 1
            elif 'producer' in professions:
                total_by_profession['Producer'] += 1
            elif 'writer' in professions:
                total_by_profession['Writer'] += 1
            else:
                total_by_profession['Other'] += 1

        # Calculate percentages
        profession_percentages = {}
        for prof in profession_edu_counts.index:
            if prof in total_by_profession and total_by_profession[prof] > 0:
                profession_percentages[prof] = (profession_edu_counts[prof] / total_by_profession[prof]) * 100

        # Create figure with two subplots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Left: Absolute counts
        profession_edu_counts.plot(kind='bar', ax=ax1, color=sns.color_palette("muted", len(profession_edu_counts)))
        ax1.set_title('Number of People with Education Data by Profession', fontsize=12, fontweight='bold')
        ax1.set_xlabel('Profession', fontsize=11, fontweight='bold')
        ax1.set_ylabel('Count', fontsize=11, fontweight='bold')
        ax1.tick_params(axis='x', rotation=45)

        # Add value labels
        for i, v in enumerate(profession_edu_counts.values):
            ax1.text(i, v + 10, str(v), ha='center', fontsize=9)

        # Right: Percentage coverage
        if profession_percentages:
            prof_pct_sorted = sorted(profession_percentages.items(), key=lambda x: x[1], reverse=True)
            profs, pcts = zip(*prof_pct_sorted)

            ax2.bar(range(len(profs)), pcts, color=sns.color_palette("pastel", len(profs)))
            ax2.set_xticks(range(len(profs)))
            ax2.set_xticklabels(profs, rotation=45, ha='right')
            ax2.set_title('Education Data Coverage by Profession', fontsize=12, fontweight='bold')
            ax2.set_xlabel('Profession', fontsize=11, fontweight='bold')
            ax2.set_ylabel('Coverage (%)', fontsize=11, fontweight='bold')

            # Add percentage labels
            for i, v in enumerate(pcts):
                ax2.text(i, v + 0.5, f'{v:.1f}%', ha='center', fontsize=9)

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '02_education_by_profession.png'), dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_top_birthplaces(self):
        """Visualize top cities and countries producing cinema talent."""
        print("\nGenerating birthplace visualizations...")

        # Top cities
        city_counts = self.birthplace_df['city'].value_counts().head(20)

        # Top countries
        country_counts = self.birthplace_df['country'].value_counts().head(15)

        # Create figure with two subplots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))

        # Left: Top cities
        y_pos = np.arange(len(city_counts))
        ax1.barh(y_pos, city_counts.values, color=sns.color_palette("rocket", len(city_counts)))
        ax1.set_yticks(y_pos)
        ax1.set_yticklabels(city_counts.index, fontsize=9)
        ax1.invert_yaxis()
        ax1.set_xlabel('Number of People', fontsize=11, fontweight='bold')
        ax1.set_title('Top 20 Cities by Cinema Talent Birthplace', fontsize=12, fontweight='bold')

        # Add value labels
        for i, v in enumerate(city_counts.values):
            ax1.text(v + 5, i, str(v), va='center', fontsize=8)

        # Right: Top countries
        country_counts.plot(kind='bar', ax=ax2, color=sns.color_palette("mako", len(country_counts)))
        ax2.set_title('Top 15 Countries by Cinema Talent Birthplace', fontsize=12, fontweight='bold')
        ax2.set_xlabel('Country', fontsize=11, fontweight='bold')
        ax2.set_ylabel('Count', fontsize=11, fontweight='bold')
        ax2.tick_params(axis='x', rotation=45)

        # Add value labels
        for i, v in enumerate(country_counts.values):
            ax2.text(i, v + 20, str(v), ha='center', fontsize=8)

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '03_top_birthplaces.png'), dpi=300, bbox_inches='tight')
        plt.close()

        print(f"  Top city: {city_counts.index[0]} with {city_counts.values[0]} people")
        print(f"  Top country: {country_counts.index[0]} with {country_counts.values[0]} people")

    def visualize_geographic_distribution(self):
        """Visualize geographic distribution by profession."""
        print("\nGenerating geographic distribution by profession...")

        # Get top 10 countries
        top_countries = self.birthplace_df['country'].value_counts().head(10).index.tolist()

        # Filter to top countries
        df_top = self.birthplace_df[self.birthplace_df['country'].isin(top_countries)]

        # Create pivot table
        pivot = df_top.groupby(['country', 'primary_profession']).size().unstack(fill_value=0)

        # Sort by total
        pivot = pivot.loc[pivot.sum(axis=1).sort_values(ascending=False).index]

        # Create stacked bar chart
        fig, ax = plt.subplots(figsize=(14, 8))
        pivot.plot(kind='bar', stacked=True, ax=ax,
                   color=sns.color_palette("Set2", len(pivot.columns)))

        ax.set_title('Geographic Distribution of Cinema Talent by Profession (Top 10 Countries)',
                     fontsize=13, fontweight='bold', pad=15)
        ax.set_xlabel('Country', fontsize=11, fontweight='bold')
        ax.set_ylabel('Number of People', fontsize=11, fontweight='bold')
        ax.legend(title='Profession', bbox_to_anchor=(1.05, 1), loc='upper left')
        ax.tick_params(axis='x', rotation=45)

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '04_geographic_by_profession.png'), dpi=300, bbox_inches='tight')
        plt.close()

    def analyze_school_quality_correlation(self):
        """Analyze if certain schools correlate with higher movie ratings."""
        print("\nAnalyzing school-quality correlations...")

        # Build person ID to movies mapping using IMDb IDs
        person_id_movies = defaultdict(list)

        for _, movie in self.movies_df.iterrows():
            movie_rating = movie.get('IMDb Rating')
            if pd.isna(movie_rating) or movie_rating == 0:
                continue

            # Get cast IDs
            cast_ids = []
            imdb_cast_str = movie.get('imdb_cast_ids', '')
            if imdb_cast_str and not pd.isna(imdb_cast_str):
                # Parse list format like "['nm0000209', 'nm0000151']"
                import ast
                try:
                    cast_ids_list = ast.literal_eval(str(imdb_cast_str))
                    if isinstance(cast_ids_list, list):
                        cast_ids.extend([str(id).strip() for id in cast_ids_list if id])
                except:
                    # Fallback to split
                    cast_ids.extend([id.strip() for id in str(imdb_cast_str).split('|') if id.strip()])

            # Get director IDs
            director_ids = []
            imdb_director_str = movie.get('imdb_director_ids', '')
            if imdb_director_str and not pd.isna(imdb_director_str):
                try:
                    director_ids_list = ast.literal_eval(str(imdb_director_str))
                    if isinstance(director_ids_list, list):
                        director_ids.extend([str(id).strip() for id in director_ids_list if id])
                except:
                    director_ids.extend([id.strip() for id in str(imdb_director_str).split('|') if id.strip()])

            all_person_ids = cast_ids + director_ids

            for person_id in all_person_ids:
                person_id_movies[person_id].append(movie_rating)

        # Calculate average rating per person ID
        person_id_avg_ratings = {pid: np.mean(ratings) for pid, ratings in person_id_movies.items() if ratings}

        # Build mapping from person cache ID to IMDb ID
        cache_id_to_imdb = {}
        for cache_id, person_data in self.people_cache.items():
            imdb_id = person_data.get('imdb_id')
            if imdb_id:
                cache_id_to_imdb[cache_id] = imdb_id

        # Join with education data using IDs
        education_quality = []

        for _, row in self.education_df.iterrows():
            cache_person_id = row['person_id']
            imdb_id = cache_id_to_imdb.get(cache_person_id)

            if imdb_id and imdb_id in person_id_avg_ratings:
                education_quality.append({
                    'school': row['school'],
                    'person': row['name'],
                    'avg_rating': person_id_avg_ratings[imdb_id]
                })

        if not education_quality:
            print("  No education-quality correlation data available")
            return

        edu_quality_df = pd.DataFrame(education_quality)

        # Calculate average rating by school (minimum 3 people)
        school_ratings = edu_quality_df.groupby('school').agg({
            'avg_rating': ['mean', 'count']
        }).reset_index()
        school_ratings.columns = ['school', 'avg_rating', 'count']
        school_ratings = school_ratings[school_ratings['count'] >= 3].sort_values('avg_rating', ascending=False)

        # Store stats
        self.stats['schools_with_quality_data'] = len(school_ratings)

        # Create visualization
        fig, ax = plt.subplots(figsize=(14, 10))

        if len(school_ratings) > 0:
            top_schools = school_ratings.head(20)

            y_pos = np.arange(len(top_schools))
            bars = ax.barh(y_pos, top_schools['avg_rating'],
                           color=sns.color_palette("coolwarm", len(top_schools)))

            ax.set_yticks(y_pos)
            ax.set_yticklabels(top_schools['school'], fontsize=9)
            ax.invert_yaxis()
            ax.set_xlabel('Average Movie Rating', fontsize=11, fontweight='bold')
            ax.set_title('Top 20 Schools by Average Movie Quality (min. 3 alumni)',
                         fontsize=13, fontweight='bold', pad=15)
            ax.axvline(x=6.5, color='red', linestyle='--', linewidth=1, alpha=0.5, label='Dataset Average')

            # Add value labels with count
            for i, (rating, count) in enumerate(zip(top_schools['avg_rating'], top_schools['count'])):
                ax.text(rating + 0.05, i, f'{rating:.2f} (n={int(count)})', va='center', fontsize=8)

            ax.legend()

            print(f"  Top school by quality: {top_schools.iloc[0]['school']} (avg: {top_schools.iloc[0]['avg_rating']:.2f})")
            print(f"  Total people matched: {len(person_id_avg_ratings)}, Education records matched: {len(education_quality)}")
        else:
            # No data - create placeholder visualization
            ax.text(0.5, 0.5, 'Insufficient data for school-quality correlation\n(minimum 3 alumni per school required)',
                   ha='center', va='center', fontsize=14, transform=ax.transAxes)
            ax.set_xlim(0, 1)
            ax.set_ylim(0, 1)
            ax.axis('off')
            print(f"  No schools with sufficient data (min 3 alumni)")
            print(f"  Total people matched: {len(person_id_avg_ratings)}, Education records matched: {len(education_quality)}")

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '05_school_quality_correlation.png'), dpi=300, bbox_inches='tight')
        plt.close()

    def analyze_alumni_networks(self):
        """Analyze same-school collaborations in movies."""
        print("\nAnalyzing alumni networks and collaborations...")

        # Build school-to-people mapping
        school_people = defaultdict(set)
        for _, row in self.education_df.iterrows():
            school_people[row['school']].add(row['name'])

        # Find movies with multiple alumni from same school
        collaborations = []

        for _, movie in self.movies_df.iterrows():
            # Get cast and crew
            cast = str(movie.get('cast', '')).split('|') if movie.get('cast') else []
            directors = str(movie.get('Directors', '')).split(',') if movie.get('Directors') else []

            all_people = cast + directors
            all_people = set([p.strip() for p in all_people if p.strip()])

            # Check each school for multiple alumni in this movie
            for school, alumni in school_people.items():
                common = all_people.intersection(alumni)
                if len(common) >= 2:
                    collaborations.append({
                        'movie': movie.get('Title', 'Unknown'),
                        'year': movie.get('Year', 'Unknown'),
                        'school': school,
                        'num_alumni': len(common),
                        'alumni': list(common)
                    })

        if not collaborations:
            print("  No alumni collaborations found")
            return

        collab_df = pd.DataFrame(collaborations)

        # Count collaborations per school
        school_collabs = collab_df.groupby('school').agg({
            'movie': 'count',
            'num_alumni': 'sum'
        }).reset_index()
        school_collabs.columns = ['school', 'num_movies', 'total_alumni_instances']
        school_collabs = school_collabs.sort_values('num_movies', ascending=False).head(15)

        # Store stats
        self.stats['total_alumni_collaborations'] = len(collab_df)
        self.stats['schools_with_collaborations'] = collab_df['school'].nunique()

        # Create visualization
        fig, ax = plt.subplots(figsize=(14, 8))

        x = np.arange(len(school_collabs))
        width = 0.35

        bars1 = ax.bar(x - width/2, school_collabs['num_movies'], width,
                       label='Number of Movies', color='steelblue')
        bars2 = ax.bar(x + width/2, school_collabs['total_alumni_instances'], width,
                       label='Total Alumni Instances', color='coral')

        ax.set_xlabel('School', fontsize=11, fontweight='bold')
        ax.set_ylabel('Count', fontsize=11, fontweight='bold')
        ax.set_title('Top 15 Schools by Alumni Collaborations in Movies',
                     fontsize=13, fontweight='bold', pad=15)
        ax.set_xticks(x)
        ax.set_xticklabels(school_collabs['school'], rotation=45, ha='right', fontsize=8)
        ax.legend()

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '06_alumni_networks.png'), dpi=300, bbox_inches='tight')
        plt.close()

        print(f"  Found {len(collab_df)} movies with alumni collaborations")
        print(f"  Top school: {school_collabs.iloc[0]['school']} with {school_collabs.iloc[0]['num_movies']} movies")

    def visualize_education_types(self):
        """Categorize and visualize types of educational institutions."""
        print("\nCategorizing educational institutions...")

        # Categorize schools
        def categorize_school(school_name):
            school_lower = school_name.lower()

            # Universities
            if any(word in school_lower for word in ['university', 'college', 'institut']):
                if any(word in school_lower for word in ['drama', 'theatre', 'theater', 'acting', 'film', 'cinema']):
                    return 'Drama/Film School'
                elif any(word in school_lower for word in ['art', 'design', 'music']):
                    return 'Arts College'
                else:
                    return 'University'

            # High schools
            elif any(word in school_lower for word in ['high school', 'secondary', 'grammar school']):
                return 'High School'

            # Drama/acting schools
            elif any(word in school_lower for word in ['drama', 'theatre', 'theater', 'acting', 'studio', 'conservatory']):
                return 'Drama/Acting School'

            # Other
            else:
                return 'Other'

        self.education_df['institution_type'] = self.education_df['school'].apply(categorize_school)

        # Count by type
        type_counts = self.education_df.groupby('institution_type')['person_id'].nunique().sort_values(ascending=False)

        # Create pie chart
        fig, ax = plt.subplots(figsize=(12, 8))

        colors = sns.color_palette("pastel", len(type_counts))
        wedges, texts, autotexts = ax.pie(type_counts.values, labels=type_counts.index,
                                            autopct='%1.1f%%', startangle=90,
                                            colors=colors, textprops={'fontsize': 10})

        # Make percentage text bold
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')

        ax.set_title('Distribution of Educational Institution Types',
                     fontsize=14, fontweight='bold', pad=20)

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '07_institution_types.png'), dpi=300, bbox_inches='tight')
        plt.close()

        print(f"  Most common type: {type_counts.index[0]} ({type_counts.values[0]} people)")

    def visualize_birthplace_evolution(self):
        """Visualize how birthplace patterns evolved over time."""
        print("\nAnalyzing birthplace evolution over time...")

        # Filter to people with birth years
        df_with_years = self.birthplace_df[self.birthplace_df['birth_year'].notna()].copy()
        df_with_years['birth_year'] = df_with_years['birth_year'].astype(int)

        # Filter to reasonable years (1880-2010)
        df_with_years = df_with_years[(df_with_years['birth_year'] >= 1880) &
                                       (df_with_years['birth_year'] <= 2010)]

        # Create decade bins
        df_with_years['birth_decade'] = (df_with_years['birth_year'] // 10) * 10

        # Get top 5 countries
        top_countries = self.birthplace_df['country'].value_counts().head(5).index.tolist()

        # Filter to top countries
        df_top = df_with_years[df_with_years['country'].isin(top_countries)]

        # Count by decade and country
        decade_country = df_top.groupby(['birth_decade', 'country']).size().unstack(fill_value=0)

        # Create line plot
        fig, ax = plt.subplots(figsize=(14, 8))

        for country in decade_country.columns:
            ax.plot(decade_country.index, decade_country[country], marker='o',
                   linewidth=2, label=country)

        ax.set_xlabel('Birth Decade', fontsize=11, fontweight='bold')
        ax.set_ylabel('Number of People', fontsize=11, fontweight='bold')
        ax.set_title('Evolution of Cinema Talent Birthplaces Over Time (Top 5 Countries)',
                     fontsize=13, fontweight='bold', pad=15)
        ax.legend(title='Country', fontsize=10)
        ax.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '08_birthplace_evolution.png'), dpi=300, bbox_inches='tight')
        plt.close()

    def visualize_us_state_distribution(self):
        """Analyze US state distribution for American talent."""
        print("\nAnalyzing US state distribution...")

        # Filter to USA birthplaces
        us_df = self.birthplace_df[self.birthplace_df['country'].str.contains('USA|United States', na=False, case=False)].copy()

        if len(us_df) == 0:
            print("  No US birthplace data found")
            return

        # Extract state from state_province field
        us_df['state'] = us_df['state_province'].str.strip()

        # Count by state
        state_counts = us_df['state'].value_counts().head(20)

        # Filter out empty states
        state_counts = state_counts[state_counts.index != '']

        if len(state_counts) == 0:
            print("  No state-level data available")
            return

        # Create visualization
        fig, ax = plt.subplots(figsize=(14, 10))

        y_pos = np.arange(len(state_counts))
        bars = ax.barh(y_pos, state_counts.values,
                       color=sns.color_palette("Blues_r", len(state_counts)))

        ax.set_yticks(y_pos)
        ax.set_yticklabels(state_counts.index, fontsize=10)
        ax.invert_yaxis()
        ax.set_xlabel('Number of People', fontsize=11, fontweight='bold')
        ax.set_title('Top 20 US States by Cinema Talent Birthplace',
                     fontsize=13, fontweight='bold', pad=15)

        # Add value labels
        for i, v in enumerate(state_counts.values):
            ax.text(v + 2, i, str(v), va='center', fontsize=9)

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '09_us_state_distribution.png'), dpi=300, bbox_inches='tight')
        plt.close()

        print(f"  Top US state: {state_counts.index[0]} with {state_counts.values[0]} people")

    def visualize_coverage_summary(self):
        """Create comprehensive coverage summary visualization."""
        print("\nGenerating coverage summary...")

        total_people = len(self.people_cache)

        # Calculate coverage metrics
        metrics = {
            'Education Data': (self.stats['total_people_with_education'], total_people),
            'Birthplace Data': (self.stats['total_people_with_birthplace'], total_people),
            'Education + Birthplace': (
                len(set(self.education_df['person_id'].unique()).intersection(
                    set(self.birthplace_df['person_id'].unique()))),
                total_people
            )
        }

        # Create figure
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Left: Absolute counts
        labels = list(metrics.keys())
        values = [m[0] for m in metrics.values()]

        ax1.bar(range(len(labels)), values, color=sns.color_palette("viridis", len(labels)))
        ax1.set_xticks(range(len(labels)))
        ax1.set_xticklabels(labels, fontsize=10)
        ax1.set_ylabel('Number of People', fontsize=11, fontweight='bold')
        ax1.set_title('Data Coverage: Absolute Counts', fontsize=12, fontweight='bold')

        # Add value labels
        for i, v in enumerate(values):
            ax1.text(i, v + 100, f'{v:,}', ha='center', fontsize=10, fontweight='bold')

        # Right: Percentage coverage
        percentages = [(m[0] / m[1]) * 100 for m in metrics.values()]

        ax2.bar(range(len(labels)), percentages, color=sns.color_palette("rocket", len(labels)))
        ax2.set_xticks(range(len(labels)))
        ax2.set_xticklabels(labels, fontsize=10)
        ax2.set_ylabel('Coverage (%)', fontsize=11, fontweight='bold')
        ax2.set_title('Data Coverage: Percentage of Total', fontsize=12, fontweight='bold')
        ax2.set_ylim(0, 100)

        # Add percentage labels
        for i, v in enumerate(percentages):
            ax2.text(i, v + 2, f'{v:.1f}%', ha='center', fontsize=10, fontweight='bold')

        plt.tight_layout()
        plt.savefig(os.path.join(self.vis_dir, '10_coverage_summary.png'), dpi=300, bbox_inches='tight')
        plt.close()

    def generate_report(self):
        """Generate comprehensive text report."""
        print("\nGenerating text report...")

        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("CINESCOPE BATCH 13: EDUCATION & ORIGINS ANALYSIS")
        report_lines.append("=" * 80)
        report_lines.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"\nTotal People in Dataset: {len(self.people_cache):,}")
        report_lines.append(f"Total Movies in Dataset: {len(self.movies_df):,}")

        report_lines.append("\n" + "=" * 80)
        report_lines.append("EDUCATION DATA SUMMARY")
        report_lines.append("=" * 80)
        report_lines.append(f"\nPeople with Education Data: {self.stats['total_people_with_education']:,}")
        report_lines.append(f"Coverage: {(self.stats['total_people_with_education'] / len(self.people_cache)) * 100:.1f}%")
        report_lines.append(f"Total Education Records: {self.stats['total_education_records']:,}")
        report_lines.append(f"Unique Schools/Universities: {self.stats['total_unique_schools']:,}")

        # Top schools
        report_lines.append("\nTop 10 Schools by Number of People:")
        top_schools = self.education_df['school'].value_counts().head(10)
        for i, (school, count) in enumerate(top_schools.items(), 1):
            report_lines.append(f"  {i:2d}. {school:<60} {count:>4} people")

        report_lines.append("\n" + "=" * 80)
        report_lines.append("BIRTHPLACE DATA SUMMARY")
        report_lines.append("=" * 80)
        report_lines.append(f"\nPeople with Birthplace Data: {self.stats['total_people_with_birthplace']:,}")
        report_lines.append(f"Coverage: {(self.stats['total_people_with_birthplace'] / len(self.people_cache)) * 100:.1f}%")
        report_lines.append(f"Unique Cities: {self.stats['total_unique_cities']:,}")
        report_lines.append(f"Unique Countries: {self.stats['total_unique_countries']:,}")

        # Top cities
        report_lines.append("\nTop 10 Cities by Number of People:")
        top_cities = self.birthplace_df['city'].value_counts().head(10)
        for i, (city, count) in enumerate(top_cities.items(), 1):
            report_lines.append(f"  {i:2d}. {city:<50} {count:>4} people")

        # Top countries
        report_lines.append("\nTop 10 Countries by Number of People:")
        top_countries = self.birthplace_df['country'].value_counts().head(10)
        for i, (country, count) in enumerate(top_countries.items(), 1):
            report_lines.append(f"  {i:2d}. {country:<50} {count:>4} people")

        if 'total_alumni_collaborations' in self.stats:
            report_lines.append("\n" + "=" * 80)
            report_lines.append("ALUMNI NETWORK ANALYSIS")
            report_lines.append("=" * 80)
            report_lines.append(f"\nTotal Movies with Alumni Collaborations: {self.stats['total_alumni_collaborations']:,}")
            report_lines.append(f"Schools with Collaborations: {self.stats['schools_with_collaborations']:,}")

        report_lines.append("\n" + "=" * 80)
        report_lines.append("END OF REPORT")
        report_lines.append("=" * 80)

        # Write report
        with open(self.report_path, 'w') as f:
            f.write('\n'.join(report_lines))

        print(f"  Report saved to: {self.report_path}")

    def run_analysis(self):
        """Execute complete analysis pipeline."""
        print("\n" + "=" * 80)
        print("CINESCOPE BATCH 13: EDUCATION & ORIGINS ANALYSIS")
        print("=" * 80)

        # Load data
        self.load_data()

        # Extract structured data
        self.extract_education_data()
        self.extract_birthplace_data()

        # Generate visualizations
        self.visualize_top_schools()
        self.visualize_education_by_profession()
        self.visualize_top_birthplaces()
        self.visualize_geographic_distribution()
        self.analyze_school_quality_correlation()
        self.analyze_alumni_networks()
        self.visualize_education_types()
        self.visualize_birthplace_evolution()
        self.visualize_us_state_distribution()
        self.visualize_coverage_summary()

        # Generate report
        self.generate_report()

        print("\n" + "=" * 80)
        print("ANALYSIS COMPLETE")
        print("=" * 80)
        print(f"Visualizations saved to: {self.vis_dir}")
        print(f"Report saved to: {self.report_path}")
        print(f"\nGenerated {len([f for f in os.listdir(self.vis_dir) if f.endswith('.png')])} visualizations")


if __name__ == '__main__':
    analyzer = EducationOriginsAnalyzer()
    analyzer.run_analysis()
