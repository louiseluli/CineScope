# 🎬 CineScope

> **Personal Cinema Analytics Platform** — Deep statistical analysis of 2,289 watched films through multi-source data enrichment and advanced visualization.

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://python.org)
[![React](https://img.shields.io/badge/React-19.0+-61DAFB.svg)](https://reactjs.org)
[![Flask](https://img.shields.io/badge/Flask-2.0+-000000.svg)](https://flask.palletsprojects.com)
[![Status](https://img.shields.io/badge/Status-Ongoing-orange.svg)]()

**CineScope** transforms personal cinema data into actionable insights through comprehensive enrichment pipelines and rigorous statistical analysis. This ongoing project demonstrates advanced data engineering, API integration, and visualization techniques applied to a personal watch history of 2,289 films.

---

## 📊 Project Overview

**Core Data:**
- **2,289 watched films** with 113+ enriched columns
- **42,630 people** (actors, directors, crew) with extended biographical data
- **8,147 unique keywords** across the collection
- **99.9% enrichment coverage** for critical metadata fields

**Data Sources:**
- **IMDb** — Ratings, cast, crew, non-commercial datasets
- **TMDB** — Budget, revenue, keywords, cinematographers, composers
- **OMDb** — Awards, additional metadata
- **Wikidata** — Biographical data (education, death, family)
- **DoesTheDogDie** — Content warnings

### 🎯 Key Capabilities

- **Multi-source enrichment** with intelligent fallbacks and caching
- **Statistical analysis** (t-tests, correlation, entropy, Gini coefficient)
- **Network analysis** of collaborations and keyword co-occurrence
- **Financial intelligence** (ROI analysis, profitability patterns)
- **Biographical insights** (mortality patterns, education, legacy)
- **Advanced visualizations** (60+ charts across 12 batches at 300 DPI)

---

## 🔬 Analysis Batches (Ongoing)

CineScope is organized into modular "batches" — each exploring a specific analytical dimension.

### ✅ Recently Completed Batches

#### **Batch 11: Behind the Camera**
*Cinematographers & Composers — The invisible artists shaping cinema*

<p align="center">
  <img src="analysis_outputs/visualizations/batch_11/03_director_cinematographer_heatmap.png" width="49%" />
  <img src="analysis_outputs/visualizations/batch_11/05_cinematographer_genre_heatmap.png" width="49%" />
</p>

**Insights:**
- **1,041 cinematographers** (95.5% coverage) | **1,633 composers** (93.3% coverage)
- Director-DP collaboration heatmaps reveal auteur signatures
- Genre preferences show cinematographers specialize while composers diversify

---

#### **Batch 12: Mortality & Legacy**
*Life spans, death patterns, and legacy in cinema*

<p align="center">
  <img src="analysis_outputs/visualizations/batch_12/01_age_at_death.png" width="49%" />
  <img src="analysis_outputs/visualizations/batch_12/09_longevity_analysis.png" width="49%" />
</p>

**Insights:**
- **11,497 deceased** (27% of dataset) | **31,133 living** (73%)
- **Average age at death: 73.9 years** (median: 76.0 years, σ = 14.5)
- **101 centenarians** documented | Oldest: 117 years
- **Lifespan increasing** +0.2 years per decade by birth cohort
- **Top causes:** Myocardial infarction (751), cancer (454), pneumonia (251)

<p align="center">
  <img src="analysis_outputs/visualizations/batch_12/03_causes_of_death.png" width="49%" />
  <img src="analysis_outputs/visualizations/batch_12/07_mortality_by_profession.png" width="49%" />
</p>

---

#### **Batch 18: International Cinema**
*Language diversity and cultural representation*

<p align="center">
  <img src="analysis_outputs/visualizations/batch_18/01_language_distribution.png" width="49%" />
  <img src="analysis_outputs/visualizations/batch_18/06_language_genre_heatmap.png" width="49%" />
</p>

**Insights:**
- **23 languages** across 2,287 films (99.9% coverage)
- **89% English** (2,037) vs **11% non-English** (252 films)
- **European cinema** averages 6.67 rating vs North American 6.38
- Top non-English: **French (50)**, **German (29)**, **Chinese (29)**, **Korean (25)**

<p align="center">
  <img src="analysis_outputs/visualizations/batch_18/04_foreign_vs_hollywood.png" width="49%" />
  <img src="analysis_outputs/visualizations/batch_18/09_diversity_score.png" width="49%" />
</p>

---

#### **Batch 22: Financial Intelligence**
*Budget, revenue, and profitability analysis*

<p align="center">
  <img src="analysis_outputs/visualizations/batch_22/03_roi_leaderboard.png" width="49%" />
  <img src="analysis_outputs/visualizations/batch_22/06_genre_financials.png" width="49%" />
</p>

**Insights:**
- **$148.27B total box office** | **$41.21B total budget**
- **82.7% profitable** (1,116 out of 1,349 films with complete data)
- **Average ROI: 688.5%** (median: 191.7%) | **Total profit: $106.46B**
- **Weak budget-rating correlation** (r = 0.12): Money ≠ Quality
- Low-budget films achieve exceptional ROI (>10,000% documented)

<p align="center">
  <img src="analysis_outputs/visualizations/batch_22/07_budget_vs_rating.png" width="49%" />
  <img src="analysis_outputs/visualizations/batch_22/08_roi_vs_budget.png" width="49%" />
</p>

---

#### **Batch 26: Keyword Deep-Dive**
*Advanced keyword intelligence with statistical rigor*

<p align="center">
  <img src="analysis_outputs/visualizations/batch_26/02_prediction_power.png" width="49%" />
  <img src="analysis_outputs/visualizations/batch_26/03_cooccurrence_network.png" width="49%" />
</p>

**Insights:**
- **8,147 unique keywords** analyzed | **25,779 total occurrences**
- **91.3% diversity score** (Shannon entropy)
- **"Film noir"** predicts highest quality (avg 7.70, p < 0.05)
- **1,417 keywords** correlated with high ratings (>7.5)
- **Statistical significance** testing for prediction power

<p align="center">
  <img src="analysis_outputs/visualizations/batch_26/05_diversity_metrics.png" width="49%" />
  <img src="analysis_outputs/visualizations/batch_26/08_era_signatures.png" width="49%" />
</p>

---

### 📋 Earlier Batches (Completed)

<details>
<summary><strong>Batch 1-10: Core Analytics</strong> (click to expand)</summary>

#### **Batch 1: Quantified Self**
<p align="center">
  <img src="analysis_outputs/README_visualizations/01_batch1_01_rating_distribution.png" width="32%" />
  <img src="analysis_outputs/README_visualizations/01_batch1_07_decade_distribution.png" width="32%" />
  <img src="analysis_outputs/README_visualizations/01_batch1_05_runtime_sweet_spot.png" width="32%" />
</p>

**Rating distribution, decade patterns, runtime sweet spots**

---

#### **Batch 2: Content Genome**
<p align="center">
  <img src="analysis_outputs/visualizations/batch_2/01_top_actors_leaderboard.png" width="32%" />
  <img src="analysis_outputs/visualizations/batch_2/02_top_actresses_leaderboard.png" width="32%" />
  <img src="analysis_outputs/visualizations/batch_2/16_cast_gender_balance.png" width="32%" />
</p>

**Cast analysis, gender balance, representation metrics**

---

#### **Batch 4-5: Directors & Genres**
<p align="center">
  <img src="analysis_outputs/visualizations/batch_4/01_director_leaderboard.png" width="49%" />
  <img src="analysis_outputs/visualizations/batch_5/05_genre_combinations.png" width="49%" />
</p>

**Director patterns, genre evolution, hybrid analysis**

---

#### **Batch 6-7: Production & Critics**
<p align="center">
  <img src="analysis_outputs/visualizations/batch_6/01_studio_leaderboard.png" width="49%" />
  <img src="analysis_outputs/visualizations/batch_7/06_source_divergence.png" width="49%" />
</p>

**Studio economics, awards analysis, critical alignment**

---

#### **Batch 10: Keywords & Themes**
<p align="center">
  <img src="analysis_outputs/visualizations/batch_10/keywords_by_genre.png" width="49%" />
  <img src="analysis_outputs/visualizations/batch_10/wordcloud_overall.png" width="49%" />
</p>

**Keyword frequency, theme distribution, genre patterns**

</details>

---

## 🛠️ Technical Architecture

### Data Pipeline

```
IMDb Export → Multi-Source Enrichment → Statistical Analysis → Visualization
                       ↓                        ↓                    ↓
                (TMDB + OMDb            (Pandas/NumPy/SciPy)   (Matplotlib/
                 + Wikidata)            T-tests, Correlation    Seaborn/Plotly
                 + Caching)             Network Analysis)       300 DPI PNG)
```

### Tech Stack

**Backend:**
- **Python 3.11+** (pandas, numpy, scipy, scikit-learn)
- **Flask 2.0+** (REST API with 40+ endpoints)
- **CSV-based storage** (no database — portable and inspectable)

**Frontend:**
- **React 19** with TypeScript
- **TanStack Query** for server state
- **Tailwind CSS** for responsive design
- **Recharts** for interactive visualizations

**Analysis:**
- **Statistical testing** (t-tests, p-values, correlation matrices)
- **Information theory** (Shannon entropy, Gini coefficient, Lorenz curves)
- **Network analysis** (NetworkX for collaboration graphs)
- **Machine learning** (k-means clustering, similarity scoring)

---

## 📈 Data Quality & Coverage

| Field | Coverage | Count | Source |
|-------|----------|-------|--------|
| **Budget** | 63.8% | 1,460 films | TMDB |
| **Revenue** | 65.7% | 1,505 films | TMDB |
| **Keywords** | 99.9% | 2,287 films | TMDB |
| **Languages** | 99.9% | 2,287 films | TMDB + OMDb |
| **People (Extended)** | 63.9% | 27,254 people | Wikidata |
| **Death Data** | 27.0% | 11,497 people | Wikidata |
| **Education** | 25.5% | 10,877 people | Wikidata |
| **Cinematographers** | 95.5% | 2,186 films | TMDB |
| **Composers** | 93.3% | 2,135 films | TMDB |

---

## 🚀 Getting Started

### Prerequisites

```bash
# Python 3.11+
pip install -r requirements.txt

# Node 16+ (for web UI)
npm install
```

### Run Analysis Batches

```bash
# Behind the Camera
python scripts/batch_11_behind_the_camera.py

# Mortality & Legacy
python scripts/batch_12_mortality_legacy.py

# International Cinema
python scripts/batch_18_international_cinema.py

# Financial Intelligence
python scripts/batch_22_financial_intelligence.py

# Keyword Deep-Dive
python scripts/batch_26_keyword_deepdive.py
```

**Output:** `analysis_outputs/visualizations/batch_XX/` (300 DPI PNGs)

### Run Web Application

```bash
# Backend (Flask API)
cd api && python app.py  # http://localhost:5001

# Frontend (React)
cd ui && npm run dev     # http://localhost:3000
```

---

## 📁 Project Structure

```
CineScope/
├── data/
│   ├── raw/                          # IMDb exports, personal data
│   └── processed/
│       ├── watched_movies_master.csv # 2,289 films × 113 columns
│       ├── people_cache.json         # 42,630 people with enrichment
│       └── keywords_cache.json       # 2,287 movies with keywords
├── scripts/
│   ├── enrich/                       # Data enrichment pipeline
│   │   ├── 01_enrich_tmdb.py
│   │   ├── 07_enrich_people_extended.py
│   │   └── 08_enrich_keywords.py
│   └── batch_*.py                    # Analysis batches (modular)
├── analysis_outputs/
│   ├── visualizations/               # Generated charts (300 DPI)
│   └── reports/                      # Text reports
├── api/                              # Flask REST API
└── ui/                               # React TypeScript frontend
```

---

## 📊 Statistical Highlights

### Diversity Metrics
- **Shannon Entropy:** 11.86 (91.3% of theoretical maximum)
- **Gini Coefficient:** 0.566 (moderate keyword concentration)
- **Language Diversity:** 23 languages across 6 continents

### Financial Intelligence
- **Total Analyzed:** $41.21B budget | $148.27B revenue
- **Aggregate Profit:** $106.46B across 1,349 films
- **Profitability Rate:** 82.7% (1,116 profitable films)
- **Best ROI:** 99,900% (extreme outlier, likely low-budget success)

### Mortality Patterns
- **Mean Lifespan:** 73.9 years (σ = 14.5)
- **Centenarians:** 101 people (0.9% of deceased)
- **Longevity Trend:** +0.2 years per decade by birth cohort
- **Geographic:** Hollywood concentration (Forest Lawn: 498 burials)

---

## 🔮 Roadmap (Ongoing)

### Planned Batches

- **Batch 13:** Education & Origins (universities, birthplaces)
- **Batch 14:** Family & Relationships (spouses, children, dynasties)
- **Batch 15:** Height & Physical Attributes (casting patterns)
- **Batch 29:** Awards Deep-Dive (parse Oscar/BAFTA/Cannes data)
- **Batch 31:** Network Analysis (graph theory, centrality measures)
- **Batch 32:** ML Recommendations (collaborative filtering)

### Future Enhancements

- **PostgreSQL migration** (currently CSV-based for portability)
- **Real-time API updates** with webhooks
- **PDF report exports** for batch analyses
- **Social sharing** of insights and visualizations
- **Advanced ML models** (neural collaborative filtering)

---

## 📝 Methodology

### Data Enrichment Process
1. **IMDb export** → Basic metadata (title, year, rating)
2. **TMDB API** → Budget, revenue, cast, crew, keywords
3. **OMDb API** → Awards, additional metadata
4. **Wikidata SPARQL** → Biographical data (death, education, family)
5. **Derived fields** → Zodiac signs, age calculations, diversity scores

### Statistical Rigor
- **Hypothesis testing:** T-tests with p < 0.05 threshold
- **Correlation analysis:** Pearson correlation coefficients
- **Diversity metrics:** Shannon entropy, Gini coefficient, Lorenz curves
- **Trend analysis:** Linear regression, moving averages
- **Network analysis:** Graph centrality, community detection

### Quality Assurance
- Null handling with intelligent fallbacks
- Outlier detection and filtering (age: 10-120 years)
- Data validation at each enrichment step
- Statistical significance thresholds enforced

---

## 🤝 Contributing

This is a personal learning project, but feedback is welcome:
- **Issues:** Bug reports or feature suggestions
- **Discussions:** Analytical approaches or visualization ideas
- **Forks:** Adapt to your own cinema data

---

## 📜 License & Data Sources

**Educational/Personal Use**

Movie data sources:
- **IMDb** (personal export, non-commercial use)
- **TMDB** (API usage compliant with terms)
- **OMDb** (API usage compliant with terms)
- **Wikidata** (CC0 public domain license)

---

## 🙏 Acknowledgments

- **TMDB** for comprehensive API access
- **Wikidata** for biographical enrichment
- **Python data science ecosystem** (pandas, scipy, matplotlib, seaborn)
- **React community** for excellent UI libraries

---

## 📫 Project Status

**🟠 Ongoing Development** (Active as of December 2025)

---

<p align="center">
  <em>Transforming personal cinema history into statistical insights, one batch at a time.</em>
</p>

<p align="center">
  <img src="analysis_outputs/visualizations/batch_10/wordcloud_overall.png" width="700" alt="Cinema Keywords Word Cloud" />
</p>
