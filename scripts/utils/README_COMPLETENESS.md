# Completeness Database System

## Overview

The completeness database system provides a persistent, efficient way to track your movie collection completeness across actors, directors, genres, countries, decades, and studios. Instead of processing large IMDB datasets every time you run an analysis, the database is built once and can be updated incrementally.

## Quick Start

### 1. Build the Database (First Time Only)

```bash
cd /Users/louisesfer/Documents/Programming/CineScope
python scripts/utils/build_completeness_database.py
```

This will:
- Process all IMDB raw data files
- Extract actors, directors, genres, countries, decades, and studios
- Calculate completeness percentages for each
- Store results in `data/processed/completeness.db`
- **Runtime:** Approximately 10-30 minutes depending on your machine

### 2. Run Enhanced Analyses

Once the database is built, you can run the enhanced batch analyses:

```bash
# Enhanced Actor Completeness Analysis (Batch 34)
python scripts/batch_34_actor_completeness_enhanced.py

# Director Completeness Analysis (Batch 38) - coming soon
python scripts/batch_38_director_completeness_enhanced.py
```

These scripts will:
- Load data from the database (much faster!)
- Generate 20+ creative visualizations
- Create detailed reports
- **Runtime:** 2-5 minutes

## Database Structure

The completeness database (`data/processed/completeness.db`) contains the following tables:

### actor_completeness
- `actor_id` - IMDB nconst ID
- `actor_name` - Actor's name
- `total_quality_films` - Total quality films in their filmography
- `watched_count` - Number you've watched
- `completeness_pct` - Completion percentage
- `missing_count` - Number of films you haven't watched
- `watched_avg_rating` - Average rating of films you watched
- `catalog_avg_rating` - Average rating of their complete filmography
- `watched_films` - JSON list of films you've watched
- `missing_films` - JSON list of top 20 missing films (by rating)
- `last_updated` - Timestamp

### director_completeness
Similar structure to actor_completeness

### genre_completeness
- `genre` - Genre name
- `total_quality_films` - Total quality films in this genre
- `watched_count` - Number you've watched
- `completeness_pct` - Completion percentage
- `missing_count` - Films you haven't watched
- `watched_avg_rating` - Average rating of watched
- `catalog_avg_rating` - Average rating of all quality films in genre
- `missing_films` - Top 20 missing films
- `last_updated` - Timestamp

### country_completeness, decade_completeness, studio_completeness
Similar structures for countries, decades, and studios

### metadata
- Stores information about when the database was built
- Number of watched films
- Source file paths

## Quality Filters

The database uses the following quality filters:
- **IMDB Rating:** >= 6.5
- **Number of Votes:** >= 1,000 (for actors/directors), 5,000+ for genres
- **Title Type:** Movies and TV movies only (no series, shorts, etc.)
- **Adult Content:** Excluded

## Updating the Database

When you watch new movies, you can rebuild the database to update your completeness stats:

```bash
python scripts/utils/build_completeness_database.py
```

The database will be completely rebuilt with your updated watched list.

## What Makes the Enhanced Batches Special?

### Batch 34 Enhanced - Actor Completeness

Creates **20+ visualizations** including:

1. **Overview Dashboard** (7 panels in one)
   - Completeness distribution
   - Top 10 most complete actors
   - Actors with most missing films
   - Rating comparisons
   - Completion milestones
   - Film count distributions
   - Comprehensive statistics

2. **Top 5 Detailed Actor Breakdowns** (5 separate images)
   - Individual completion progress bars
   - Career timeline scatter plots
   - Rating distributions
   - Top watched films lists
   - Top missing films recommendations
   - Personalized statistics

3. **Career Trajectories** (6 actors)
   - Year-by-year rating trends
   - Film count bubbles
   - Trend line analysis

4. **Completion Heatmap**
   - Visual completion matrix
   - Watched vs total film counts

5. **Missing Films Gallery**
   - Top 50 highest-rated missing films
   - Most frequently missing (across actors)

6. **Quality Analysis** (6 panels)
   - Rating comparison scatter plots
   - Cherry-picking analysis
   - Quality gap identification
   - Selection patterns

## File Locations

```
CineScope/
├── data/
│   └── processed/
│       └── completeness.db          # The main database
├── scripts/
│   ├── utils/
│   │   └── build_completeness_database.py    # Database builder
│   ├── batch_34_actor_completeness_enhanced.py
│   └── batch_38_director_completeness_enhanced.py (coming soon)
└── analysis_outputs/
    ├── visualizations/
    │   ├── batch_34/                # Actor visualizations
    │   └── batch_38/                # Director visualizations
    └── reports/
        ├── batch_34_actor_completeness_report.txt
        └── batch_38_director_completeness_report.txt
```

## Troubleshooting

### "Database not found" error
Run the database builder first:
```bash
python scripts/utils/build_completeness_database.py
```

### Database takes too long to build
This is normal for the first run. The script processes millions of IMDB records. Subsequent analyses using the database will be much faster!

### Missing data for some actors
This can happen if:
- Actor names don't match exactly between TMDB and IMDB
- Actor has fewer than 5 quality films in IMDB
- Name mapping couldn't find a match

### Want to rebuild from scratch
Simply delete the database and run the builder again:
```bash
rm data/processed/completeness.db
python scripts/utils/build_completeness_database.py
```

## Performance Notes

**First-time database build:**
- Runtime: 10-30 minutes
- Memory: ~2-4 GB
- Disk space: ~50-100 MB for final database

**Enhanced batch analyses (using database):**
- Runtime: 2-5 minutes
- Memory: ~500 MB - 1 GB
- Much faster than processing raw IMDB data!

## Benefits of the Database Approach

1. **Speed:** Analyses run 5-10x faster
2. **Consistency:** All batches use the same processed data
3. **Incrementality:** Easy to update when you watch new movies
4. **Efficiency:** No need to reload massive IMDB files repeatedly
5. **Portability:** Single database file contains everything
6. **Scalability:** Can easily add new completeness dimensions

## Future Enhancements

Planned features:
- [ ] Incremental updates (only process new watched movies)
- [ ] Language completeness tracking
- [ ] Writer completeness tracking
- [ ] Franchise/series completeness
- [ ] Web UI for exploring completeness data
- [ ] Export to CSV/JSON for external tools
- [ ] Recommendation engine based on completeness gaps
