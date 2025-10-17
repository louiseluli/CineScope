# CineScope: My Cinematic DNA

CineScope is a deeply personal and professional data analysis project designed to explore, enrich, and visualize a lifetime of movie-watching history. It transforms your personal watchlist into a rich, interconnected database, uncovering insights and patterns in your cinematic journey. This is not just a data project; it's a personalized movie discovery engine and a living archive of your relationship with film.

## 🎬 Project Goals

- **Personalized Data Hub:** To create a centralized and enduring database of every movie you've ever watched, enriched with a comprehensive set of data from multiple sources.
- **Deep Analysis & Insights:** To analyze your viewing habits, discover your unique cinematic tastes, and understand the nuances of your film preferences.
- **Custom Recommendation Engine:** To build a recommendation system that is tailored specifically to your "cinematic DNA," suggesting films you're highly likely to enjoy.
- **Professional Web Interface:** To present the findings and recommendations through a clean, professional, and customizable web interface (HTML, CSS, JavaScript).

## ✨ Features

- **Automated Data Enrichment:** Scripts to automatically fetch and integrate data from IMDb, TMDb, OMDb, Does the Dog Die?, and Wikidata.
- **Comprehensive Database:** A robust SQLite database schema that links your watchlist with detailed movie information, including ratings, cast, crew, keywords, and more.
- **Jupyter Notebooks for Analysis:** A series of notebooks for exploratory data analysis, visualization, and building the recommendation model.
- **Modular and Scalable Codebase:** A well-structured Python project that is easy to maintain and extend.

## 🛠️ Tech Stack

- **Backend:** Python
- **Data Manipulation:** Pandas
- **Database:** SQLite (with the option to migrate to PostgreSQL)
- **APIs:** TMDb, OMDb, Does the Dog Die?, Wikidata
- **Frontend:** HTML, CSS, JavaScript (for the final web interface)

## 🚀 Getting Started

### Prerequisites

- Python 3.9+
- Git
- An IDE (like VS Code)

### Installation

1.  **Clone the repository:**

    ```bash
    git clone git@github.com:louiseluli/CineScope.git
    cd CineScope
    ```

2.  **Create a virtual environment:**

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3.  **Install the dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

4.  **Set up your environment variables:**
    - Rename `.envexample` to `.env`.
    - Add your API keys and configure the paths in the `.env` file.


. ├── README.md ├── data │ ├── processed │ └── raw ├── notebooks ├── requirements.txt ├── scripts │ └── enrich └── src ├── analysis ├── core ├── data_processing └── enrichment

## Usage

1.  **Setup the IMDb Database:**

    ```bash
    python scripts/setup_database.py
    ```

2.  **Run the enrichment scripts:**

    ```bash
    python scripts/enrich/01_enrich_tmdb.py
    python scripts/enrich/02_enrich_omdb.py
    # ... and so on for the other enrichment scripts
    ```

3.  **Explore the data in the notebooks.**

## Contributing

This is a personal project, but contributions and suggestions are welcome. Please open an issue to discuss any changes.

## License

This project is for personal and non-commercial use. Please refer to the terms and conditions of the IMDb Non-Commercial Datasets.
