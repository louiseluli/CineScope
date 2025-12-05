# 🎬 CineScope – Personal Cinema Analytics

> _A data-driven love letter to my own film taste._  
> CineScope turns years of carefully curated watchlists and a movie collection into a full analytics pipeline, exploring my cinematic patterns, biases, and obsessions.

For years I've been exporting IMDb lists, maintaining spreadsheets, and organizing my collection. CineScope is where that quiet obsession becomes something rigorous: a modular Python project that ingests my own watch history and collection, enriches each title with external data, and generates hundreds of visualizations about:

- **What I actually watch**
- **Who keeps appearing on screen**
- **How my taste moves across decades & genres**
- **Where the gaps and blind spots in my collection are**

---

## 1. Project at a Glance

**CineScope is:**

- 🧠 **Quantified self for cinema**  
  Treats my watch history and collection as a research dataset.

- 🔗 **Multi-source enrichment**  
  Combines personal data with:
  - IMDb non-commercial datasets (TSV + IMDb IDs)
  - TMDb metadata
  - OMDb ratings & details
  - DoesTheDogDie content warnings
  - Wikidata entities & extra facts

- 📊 **Rich visual analytics**  
  Generates static and interactive visualizations about:
  - Ratings, decades, runtimes
  - Genres, hybrids, and evolution over time
  - Actors, actresses, ensembles, representation
  - Directors, studios, awards, and critical alignment
  - Patterns, recommendations, and diversity of my collection

- 🧱 **Modular, inspectable pipeline**  
  Each step is a separate batch/script, so you can see exactly how data flows and transforms.

---

## 2. Repository Structure & Pipeline

The project is organized as a multi-stage data pipeline:

```text
data/
  raw/                    # Personal exports + IMDb non-commercial datasets
  processed/              # Enriched and merged tables (CSV, Parquet, caches)
  logs/, reports/, user/  # Logs, reports, user-specific inputs

scripts/
  00_create_master_list.py              # Build base list of titles
  batch_0_*                             # Data engineering & cleaning
  batch_1_quantified_self.py            # Ratings, decades, runtimes
  batch_2_content_genome.py             # Genres, hybrids, cast networks
  batch_3_*                             # Actors, representation, careers
  batch_4_directors_writers.py          # Directors & writers
  batch_5_genres.py                     # Genre-specific analytics
  batch_6_production_awards.py          # Studios & awards
  batch_7_critical_alignment.py         # Critics & ratings sources
  batch_8_patterns_recommendations.py   # Patterns, gaps, recommendations

scripts/enrich/
  00_enrich_imdb.py     # Build IMDb backbone
  01_enrich_tmdb.py     # TMDb enrichment
  02_enrich_omdb.py     # OMDb enrichment
  03_enrich_ddd.py      # DoesTheDogDie enrichment
  04_enrich_cast.py     # Cast and crew details
  05_enrich_wikidata.py # Wikidata enrichment

analysis_outputs/
  visualizations/       # PNG & HTML charts/dashboards by batch
  exports/              # CSV and JSON summaries
  reports/              # Text reports (per batch)
  README_visualizations/# Curated "hero" plots for documentation

ui_research/
  index.html, style.css, app.js        # Experimental UI for browsing
  backend/ (Python API)                # Tiny backend over the enriched data
```

**Core enriched tables** live under `data/processed/`:
- `master_media_list.csv` – all titles in scope
- `master_cinema_data.csv` – fully enriched movie-level data
- `actors_master.parquet` – actor-centric view

---

## 3. Visual Story of My Cinema Universe

This section mirrors the same narrative structure used in my portfolio: taste & time, genres, people, careers, behind the camera, critics, and patterns & gaps.

All thumbnails below are real outputs from `analysis_outputs/**` and render directly on GitHub.

---

### 3.1. My Taste, Over Time

<p align="center">
  <img src="analysis_outputs/README_visualizations/01_batch1_01_rating_distribution.png" alt="Rating distribution of my watched movies" width="45%">
  <img src="analysis_outputs/README_visualizations/01_batch1_07_decade_distribution.png" alt="Decade distribution of my watched movies" width="45%">
</p>

**Rating distribution** – Whether I'm generous or harsh, and where my scores cluster.  
**Decade distribution** – Which eras dominate my watch history and which are surprisingly empty.

<p align="center">
  <img src="analysis_outputs/README_visualizations/01_batch1_05_runtime_sweet_spot.png" alt="Runtime sweet spot chart" width="45%">
  <img src="analysis_outputs/README_visualizations/01_batch1_08_popularity_vs_rating.png" alt="Popularity vs my ratings" width="45%">
</p>

**Runtime sweet spot** – The lengths where I consistently rate films higher.  
**Popularity vs my ratings** – Where I align with the crowd and where I quietly rebel.

---

### 3.2. Genres, Hybrids & Evolution

<p align="center">
  <img src="analysis_outputs/README_visualizations/01_batch1_04_genre_distribution.png" alt="Genre distribution" width="45%">
  <img src="analysis_outputs/README_visualizations/01_batch1_02_decade_genre_heatmap.png" alt="Decade vs genre heatmap" width="45%">
</p>

**Genre distribution** – The backbone of my collection, from mainstream to niche.  
**Decade × genre heatmap** – Where genres live in time (e.g. 40s noir, 80s horror).

<p align="center">
  <img src="analysis_outputs/visualizations/batch_5/05_genre_combinations.png" alt="Genre combinations heatmap" width="45%">
  <img src="analysis_outputs/visualizations/batch_5/04_pure_vs_hybrid.png" alt="Pure vs hybrid genre comparison" width="45%">
</p>

**Genre combinations** – Hybrids like horror-comedy, noir-thriller, sci-fi-drama.  
**Pure vs hybrid genres** – How single-genre titles compare to hybrids in ratings and frequency.

<p align="center">
  <img src="analysis_outputs/visualizations/batch_5/02_genre_quality_runtime.png" alt="Genre quality vs runtime" width="45%">
  <img src="analysis_outputs/visualizations/batch_5/06_genre_decade_heatmap.png" alt="Genre evolution across decades" width="45%">
</p>

**Genre × runtime × quality** – Which genres work best short and which need room to breathe.  
**Genre evolution** – How my favourite genres travel across decades.

---

### 3.3. Who I Watch: People & Representation

<p align="center">
  <img src="analysis_outputs/visualizations/batch_2/01_top_actors_leaderboard.png" alt="Top actors leaderboard" width="45%">
  <img src="analysis_outputs/visualizations/batch_2/02_top_actresses_leaderboard.png" alt="Top actresses leaderboard" width="45%">
</p>

**Top actors & actresses** – The people who silently dominate my nights at the movies.

<p align="center">
  <img src="analysis_outputs/visualizations/batch_2/16_cast_gender_balance.png" alt="Cast gender balance" width="45%">
  <img src="analysis_outputs/visualizations/batch_2/14_actor_diversity_score.png" alt="Actor diversity score" width="45%">
</p>

**Cast gender balance** – How equitable (or not) casts are across the films I watch.  
**Actor diversity score** – A synthesized metric capturing how varied on-screen presence actually is.

<p align="center">
  <img src="analysis_outputs/visualizations/batch_2/17_largest_ensembles.png" alt="Largest ensembles" width="45%">
  <img src="analysis_outputs/visualizations/batch_3/part_3/viz_18_actors_in_top_films.png" alt="Actors in top rated films" width="45%">
</p>

**Largest ensembles** – Huge casts where dozens of careers intersect in a single film.  
**Actors in my top-rated films** – Who shows up when I give something my highest scores.

*(Several of these also have interactive `.html` versions under `analysis_outputs/visualizations/batch_2/` and `batch_3/`.)*

---

### 3.4. Careers, Longevity & Classic vs Modern

<p align="center">
  <img src="analysis_outputs/visualizations/batch_3/parts_4_5/viz_23_career_evolution.png" alt="Actor career evolution" width="45%">
  <img src="analysis_outputs/visualizations/batch_3/parts_4_5/19_career_length_distribution.png" alt="Career length distribution" width="45%">
</p>

**Career evolution** – How actors move across time and quality inside the films I've chosen.  
**Career length distribution** – Short vs decades-long careers and how that shapes visibility.

<p align="center">
  <img src="analysis_outputs/visualizations/batch_3/part_3/viz_17_classic_vs_modern.png" alt="Classic vs modern films comparison" width="45%">
  <img src="analysis_outputs/visualizations/batch_3/parts_4_5/viz_22_age_distribution.png" alt="Actor age distribution" width="45%">
</p>

**Classic vs modern** – How my ratings shift between older films and contemporary ones.  
**Age distribution** – The generational spread of actors in my collection.

<p align="center">
  <img src="analysis_outputs/visualizations/batch_3/parts_4_5/viz_27_actor_type_classification.png" alt="Actor type classification" width="45%">
  <img src="analysis_outputs/visualizations/batch_3/parts_4_5/viz_28_genre_crossing.png" alt="Genre crossing analysis" width="45%">
</p>

**Actor type classification** – Categorizing performers by their career patterns and versatility.  
**Genre crossing** – Which actors traverse genres and which stay in their lane.

---

### 3.5. Behind the Camera & Awards

<p align="center">
  <img src="analysis_outputs/visualizations/batch_4/01_director_leaderboard.png" alt="Director leaderboard" width="45%">
  <img src="analysis_outputs/visualizations/batch_4/02_director_quality.png" alt="Director quality chart" width="45%">
</p>

**Director leaderboard** – Which directors are most present in my watch history.  
**Director quality** – How consistent different directors are in the films I watch.

<p align="center">
  <img src="analysis_outputs/visualizations/batch_6/01_studio_leaderboard.png" alt="Studio leaderboard" width="45%">
  <img src="analysis_outputs/visualizations/batch_6/08_oscar_timeline.png" alt="Oscar timeline and awards" width="45%">
</p>

**Studio leaderboard** – The production houses that quietly shape my collection.  
**Oscar timeline** – How Oscar-winning/nominated titles are distributed in my universe.

<p align="center">
  <img src="analysis_outputs/visualizations/batch_6/10_top_awarded_films.png" alt="Top awarded films" width="45%">
  <img src="analysis_outputs/visualizations/batch_6/07_awards_overview.png" alt="Awards overview" width="45%">
</p>

**Top awarded films** – Titles that accumulate the most recognition and how they sit within my taste.  
**Awards overview** – A comprehensive look at how decorated my collection really is.

---

### 3.6. How Critics & Sources See My Films

<p align="center">
  <img src="analysis_outputs/visualizations/batch_7/01_sources_overview.png" alt="Sources overview" width="45%">
  <img src="analysis_outputs/visualizations/batch_7/06_source_divergence.png" alt="Source divergence chart" width="45%">
</p>

**Sources overview** – Coverage from IMDb, Rotten Tomatoes, Metacritic, etc.  
**Source divergence** – Where different critics/platforms disagree the most on the same films.

<p align="center">
  <img src="analysis_outputs/visualizations/batch_7/09_rotten_tomatoes_analysis.png" alt="Rotten Tomatoes analysis" width="45%">
  <img src="analysis_outputs/visualizations/batch_7/10_metacritic_analysis.png" alt="Metacritic analysis" width="45%">
</p>

**Rotten Tomatoes vs me** – Where critics and I align or diverge.  
**Metacritic vs me** – Two complementary views of how "critical consensus" aligns—or doesn't—with my own ratings.

<p align="center">
  <img src="analysis_outputs/visualizations/batch_7/02_source_correlations.png" alt="Source correlations" width="45%">
  <img src="analysis_outputs/visualizations/batch_7/05_decade_quality.png" alt="Decade quality by source" width="45%">
</p>

**Source correlations** – How different rating sources relate to each other.  
**Decade quality by source** – How critical reception varies across eras.

---

### 3.7. Patterns, Gaps & Recommendations

<p align="center">
  <img src="analysis_outputs/visualizations/batch_8/01_pattern_overview.png" alt="Pattern overview" width="45%">
  <img src="analysis_outputs/visualizations/batch_8/06_similar_films_recommendations.png" alt="Similar films recommendations" width="45%">
</p>

**Pattern overview** – High-level patterns across genres, decades, casts, and ratings.  
**Similar films recommendations** – Data-driven suggestions of unwatched titles I'm likely to enjoy.

<p align="center">
  <img src="analysis_outputs/visualizations/batch_8/11_collection_gaps_analysis.png" alt="Collection gaps analysis" width="45%">
  <img src="analysis_outputs/visualizations/batch_8/12_collection_diversity_score.png" alt="Collection diversity score" width="45%">
</p>

**Collection gaps** – Genres, eras, or styles that I under-explore, even though they connect to what I already love.  
**Collection diversity score** – A single view summarizing how varied my personal cinema universe really is.

<p align="center">
  <img src="analysis_outputs/visualizations/batch_8/03_similarity_clusters.png" alt="Similarity clusters" width="45%">
  <img src="analysis_outputs/visualizations/batch_8/04_feature_correlations.png" alt="Feature correlations" width="45%">
</p>

**Similarity clusters** – Films grouped by shared characteristics.  
**Feature correlations** – What predicts my ratings and viewing choices.

---

## 4. How to Run CineScope

> ⚠️ **This project uses personal data** (my own watchlists and collection).  
> The pipeline is reusable, but you should plug in your own exports and API keys.

### 4.1. Requirements

- **Python 3.11+** (tested with 3.12)
- `pip` or `conda`
- Access to:
  - IMDb non-commercial datasets
  - TMDb API key
  - OMDb API key
  - DoesTheDogDie API key (optional, for content warnings)
  - Wikidata (via HTTP/SPARQL; no key required)

**Install dependencies:**

```bash
python -m venv .venv
source .venv/bin/activate      # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 4.2. Prepare Raw Data

Place your raw files under `data/raw/`:

- `Watched-Dec.csv` – your watched history
- `Watchlist_IMDB.csv` – IMDb watchlist export
- `collection_movies.db` / `watched_movies.db` – local collection databases (or your own equivalents)
- **IMDb TSVs** (gzipped or not), at minimum:
  - `name.basics.tsv.gz`
  - `title.basics.tsv.gz`
  - `title.crew.tsv.gz`
  - `title.principals.tsv.gz`
  - `title.ratings.tsv.gz`

You can adjust file names and paths in the scripts or through config modules under `src/core/`.

### 4.3. Build the Master Dataset

**Create initial master list of titles:**

```bash
python scripts/00_create_master_list.py
```

**Run enrichment steps** (enable only the APIs you've configured):

```bash
python scripts/enrich/00_enrich_imdb.py
python scripts/enrich/01_enrich_tmdb.py
python scripts/enrich/02_enrich_omdb.py
python scripts/enrich/03_enrich_ddd.py
python scripts/enrich/04_enrich_cast.py
python scripts/enrich/05_enrich_wikidata.py
```

**Merge into unified tables:**

```bash
python scripts/merge_all_enriched.py
```

You should now see:
- `data/processed/master_media_list.csv`
- `data/processed/master_cinema_data.csv`
- `data/processed/actors_master.parquet`

### 4.4. Generate Analytics & Visualizations

Run whichever analysis batches you want:

```bash
# Ratings, decades, runtimes
python scripts/batch_1_quantified_self.py

# Genre patterns, hybrids, content genome
python scripts/batch_2_content_genome.py

# Actors, representation, careers
python scripts/batch_3_part_1_top_performers.py
python scripts/batch_3_part_2_filmography_completion.py
python scripts/batch_3_part_3_comparisons_quality.py
python scripts/batch_3_parts_4_5_age_career_character.py

# Directors & writers
python scripts/batch_4_directors_writers.py

# Genres (deep dive)
python scripts/batch_5_genres.py

# Studios & awards
python scripts/batch_6_production_awards.py

# Ratings sources & critic alignment
python scripts/batch_7_critical_alignment.py

# Patterns, recommendations, collection gaps
python scripts/batch_8_patterns_recommendations.py
```

**Outputs appear under:**
- `analysis_outputs/visualizations/` – PNGs and HTML dashboards
- `analysis_outputs/exports/` – CSVs and JSON summaries
- `analysis_outputs/reports/` – batch summaries and logs

---

## 5. UI Prototype

The `ui_research/` folder is a small experimental interface that hints at how CineScope could become a full web app:

- `ui_research/index.html` – front-end layout
- `ui_research/style.css` – styling
- `ui_research/app.js` – simple interactions
- `ui_research/backend/main.py` – tiny Flask-style backend to expose enriched data
- `ui_research/backend/imdb_query.py` – helper for querying titles and people

It's not production-ready, but it shows how all these analytics could one day become an interactive "CineScope Explorer" for my personal cinema universe.

---

## 6. How This Fits My Portfolio

On my portfolio site, CineScope appears as:

- **A visual, narrative project page** (`project-cinescope.html`) showing selected charts and storylines.
- **A technical foundation** that demonstrates:
  - Data engineering on messy, multi-source inputs
  - API-driven enrichment
  - Statistical analysis + visualization
  - Early recommender-system thinking
- **A lens on representation and diversity**, even in something as personal as "what I watch."

This repository is the engine room behind that story.

---

## 📬 Get in Touch

If you're interested in adapting CineScope to your own watch history or in connecting this to research on AI, media, and fairness, feel free to explore the code or reach out.

---

<p align="center">
  <em>CineScope – because even movie night deserves a data pipeline.</em>
</p>
