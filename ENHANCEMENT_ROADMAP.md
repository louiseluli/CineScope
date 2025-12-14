# 🎬 CineScope Enhancement Roadmap

> **Comprehensive plan for evolving CineScope into a professional-grade cinema analytics platform**

---

## 📋 Table of Contents

1. [Advanced Recommendation System](#1-advanced-recommendation-system)
2. [Deep People Analytics](#2-deep-people-analytics)
3. [Keywords & Thematic Analysis](#3-keywords--thematic-analysis)
4. [Network & Connections Analysis](#4-network--connections-analysis)
5. [Implementation Phases](#5-implementation-phases)

---

## 1. Advanced Recommendation System

### Current State
- Basic content-based filtering using genre overlap (Jaccard similarity)
- Simple decade/rating matching
- Pattern-based suggestions in batch_8

### Proposed Enhancements

#### 1.1 Multi-Signal Hybrid Recommender

```
┌─────────────────────────────────────────────────────────────────┐
│                    HYBRID RECOMMENDATION ENGINE                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐        │
│  │  Content     │   │ Collaborative│   │  Knowledge   │        │
│  │  Based       │   │  Filtering   │   │  Graph       │        │
│  │  (Genres,    │   │  (User       │   │  (Wikidata,  │        │
│  │   Keywords,  │   │   Patterns,  │   │   Cast       │        │
│  │   Themes)    │   │   Clusters)  │   │   Networks)  │        │
│  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘        │
│         │                  │                  │                 │
│         └──────────────────┼──────────────────┘                 │
│                            ▼                                    │
│                   ┌────────────────┐                           │
│                   │  ENSEMBLE      │                           │
│                   │  SCORING       │                           │
│                   │  (Weighted     │                           │
│                   │   Average)     │                           │
│                   └────────────────┘                           │
│                            │                                    │
│                            ▼                                    │
│                   ┌────────────────┐                           │
│                   │  EXPLAINABLE   │                           │
│                   │  RECOMMENDATIONS│                          │
│                   │  "Because you  │                           │
│                   │   liked X..."  │                           │
│                   └────────────────┘                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### 1.2 Recommendation Features

| Feature | Description | Data Source |
|---------|-------------|-------------|
| **Genre DNA** | Weighted genre preferences from ratings | User ratings + genres |
| **Era Affinity** | Decade preferences with quality weighting | Watched films by decade |
| **Director Trust Score** | Directors whose films you consistently enjoy | Your ratings + directors |
| **Actor Chemistry** | Actors in your highest-rated films | Cast data + ratings |
| **Runtime Sweet Spot** | Preferred film lengths | Runtime + ratings correlation |
| **Mood Matching** | Content warning patterns (DDD) | DoesTheDogDie API |
| **Thematic Resonance** | Keywords and themes that predict high ratings | Keywords + ratings |
| **Critical Alignment** | How much you agree with RT/Metacritic | Rating comparisons |
| **Network Proximity** | Films connected through shared cast/crew | Collaboration network |

#### 1.3 Recommendation Categories

```python
class RecommendationEngine:
    """
    Professional-grade recommendation system with multiple strategies.
    """
    
    def get_recommendations(self, user_profile: UserProfile) -> Dict[str, List[Recommendation]]:
        return {
            # Core recommendations
            "perfect_matches": self._find_perfect_matches(),      # Multi-signal overlap
            "hidden_gems": self._find_hidden_gems(),              # High quality, low popularity
            "director_deep_dives": self._director_exploration(),  # More from loved directors
            "actor_journeys": self._actor_filmography_gaps(),     # Complete actor collections
            
            # Discovery recommendations
            "genre_expansion": self._genre_adjacent(),            # Similar but new genres
            "era_exploration": self._decade_gaps(),               # Unexplored decades
            "international_cinema": self._country_diversity(),    # Underrepresented countries
            
            # Mood-based recommendations
            "comfort_watches": self._comfort_films(),             # Reliable feel-good
            "challenging_cinema": self._stretch_recommendations(), # Outside comfort zone
            "quick_watches": self._short_films(),                 # Under 100 min gems
            
            # Network-based recommendations
            "collaboration_chains": self._follow_collaborations(), # Director-actor pairs
            "cinematic_universes": self._thematic_connections(),   # Related works
        }
```

#### 1.4 Explainable AI

Each recommendation includes:
- **Why this film**: Clear explanation of matching factors
- **Confidence score**: How strong the recommendation is (0-100%)
- **Key matching factors**: Top 3 reasons for recommendation
- **Similar films you loved**: Reference points from your collection
- **Risk factors**: Potential reasons you might not like it

---

## 2. Deep People Analytics

### Current State
- Basic birth/death years from IMDB
- Gender, profession from IMDB
- Birthplace, nationality, awards from Wikidata

### Proposed Enhancements

#### 2.1 Extended Biographical Data

| Field | Source | Example |
|-------|--------|---------|
| **Full Birth Date** | TMDB/Wikidata | November 11, 1974 |
| **Death Date** | TMDB/Wikidata | August 11, 2014 |
| **Cause of Death** | Wikidata P509 | Cardiac arrest, Suicide, Cancer |
| **Zodiac Sign** | Calculated | Scorpio ♏ |
| **Chinese Zodiac** | Calculated | Year of the Tiger 🐅 |
| **Age at Death** | Calculated | 63 years |
| **Current Age** | Calculated | 50 years |
| **Career Span** | IMDB known_for | 1993-present (32 years) |
| **Height** | TMDB/Wikidata | 183 cm / 6'0" |
| **Birth City** | Wikidata | Los Angeles, California |
| **Burial Location** | Wikidata P119 | Forest Lawn Memorial Park |
| **Spouses** | Wikidata P26 | Previous marriages |
| **Children** | Wikidata P40 | Number of children |
| **Education** | Wikidata P69 | Drama school, university |
| **Alma Mater** | Wikidata | Yale School of Drama |

#### 2.2 Zodiac & Astrology Analysis (Fun Feature!)

```python
ZODIAC_SIGNS = {
    (1, 20): ('Aquarius', '♒', 'Air', 'Fixed'),
    (2, 19): ('Pisces', '♓', 'Water', 'Mutable'),
    (3, 21): ('Aries', '♈', 'Fire', 'Cardinal'),
    # ... full zodiac mapping
}

CHINESE_ZODIAC = {
    0: ('Monkey', '🐵'), 1: ('Rooster', '🐔'), 2: ('Dog', '🐕'),
    3: ('Pig', '🐷'), 4: ('Rat', '🐀'), 5: ('Ox', '🐂'),
    6: ('Tiger', '🐅'), 7: ('Rabbit', '🐇'), 8: ('Dragon', '🐉'),
    9: ('Snake', '🐍'), 10: ('Horse', '🐴'), 11: ('Goat', '🐐')
}

# Potential visualizations:
# - "Your Collection by Actor Zodiac Sign"
# - "Do Scorpios make better villains?"
# - "Zodiac sign distribution in Oscar winners"
```

#### 2.3 Mortality & Legacy Analysis

```python
class MortalityAnalytics:
    """Analyze the legacy and circumstances of deceased actors."""
    
    def analyze_cause_of_death(self) -> Dict:
        """
        Wikidata P509 (cause of death) categories:
        - Natural causes (disease, old age)
        - Accidents (car crash, overdose)
        - Suicide
        - Homicide
        - Unknown/Not disclosed
        """
        
    def calculate_career_posthumous_impact(self, person_id: str) -> Dict:
        """
        - Films released after death
        - Posthumous Oscar wins/nominations
        - Career renaissance after death
        - Unfinished projects
        """
        
    def generate_in_memoriam(self) -> List[Dict]:
        """
        - Actors in your collection who passed away
        - Their last film in your collection
        - Their age at death
        - Years since passing
        """
```

#### 2.4 Career Trajectory Analysis

```python
class CareerAnalyzer:
    """Deep analysis of actor/director careers."""
    
    def analyze_career_phases(self, person_id: str) -> Dict:
        return {
            'breakthrough_film': self._find_breakthrough(),
            'peak_period': self._identify_peak_years(),
            'genre_evolution': self._track_genre_changes(),
            'quality_trajectory': self._rating_over_time(),
            'collaboration_patterns': self._recurring_partners(),
            'award_momentum': self._award_timeline(),
            'career_gaps': self._identify_hiatuses(),
            'comeback_films': self._find_comebacks(),
            'final_performances': self._last_films(),
        }
        
    def compare_career_arcs(self, person_ids: List[str]) -> Dict:
        """Compare careers of multiple actors side by side."""
```

---

## 3. Keywords & Thematic Analysis

### Current State
- DoesTheDogDie content warnings (emotional triggers)
- Basic genre categorization
- No keyword extraction or thematic analysis

### Proposed Enhancements

#### 3.1 Comprehensive Keyword System

```python
KEYWORD_CATEGORIES = {
    # Themes
    'themes': [
        'redemption', 'revenge', 'love_triangle', 'coming_of_age', 
        'loss_of_innocence', 'identity_crisis', 'corruption', 'survival'
    ],
    
    # Settings
    'settings': [
        'prison', 'space', 'underwater', 'desert', 'jungle', 
        'small_town', 'new_york', 'paris', 'dystopia', 'historical'
    ],
    
    # Character Types
    'characters': [
        'anti_hero', 'femme_fatale', 'mentor', 'villain', 
        'underdog', 'everyman', 'unreliable_narrator'
    ],
    
    # Narrative Devices
    'narrative': [
        'non_linear', 'twist_ending', 'flashback', 'multiple_perspectives',
        'unreliable_narrator', 'fourth_wall_break', 'dream_sequence'
    ],
    
    # Visual Style
    'style': [
        'black_and_white', 'found_footage', 'single_take', 
        'animation_live_action', 'documentary_style'
    ],
    
    # Emotional Tone
    'tone': [
        'dark', 'uplifting', 'melancholic', 'satirical', 
        'whimsical', 'gritty', 'surreal'
    ],
    
    # Content Elements
    'content': [
        'based_on_true_story', 'book_adaptation', 'remake', 
        'sequel', 'biopic', 'musical_numbers'
    ]
}
```

#### 3.2 Keyword Sources

| Source | Keywords Available | Method |
|--------|-------------------|--------|
| **TMDB** | Plot keywords | API `/movie/{id}/keywords` |
| **OMDB** | Plot summary | NLP extraction |
| **Wikidata** | Genre, themes | SPARQL P136, P921 |
| **DoesTheDogDie** | Content warnings as keywords | API |
| **IMDb** | Plot keywords | Web scraping (non-commercial) |

#### 3.3 Keyword Analytics

```python
class KeywordAnalyzer:
    """Analyze thematic patterns across your collection."""
    
    def get_keyword_profile(self) -> Dict:
        """Your keyword DNA - what themes resonate with you."""
        
    def find_thematic_clusters(self) -> List[Dict]:
        """Group films by shared themes."""
        
    def keyword_quality_correlation(self) -> Dict:
        """Which keywords predict high ratings for you?"""
        
    def underexplored_themes(self) -> List[str]:
        """Themes present in highly-rated films you might explore more."""
        
    def theme_evolution(self) -> Dict:
        """How your thematic preferences have changed over time."""
```

#### 3.4 Visualizations

- **Keyword Cloud**: Size by frequency, color by avg rating
- **Theme Heatmap**: Keywords × Decades
- **Thematic Network**: Connected keywords that appear together
- **Keyword Quality Matrix**: Rating distribution per keyword

---

## 4. Network & Connections Analysis

### Current State
- Basic collaboration counts (director-actor pairs)
- Cast lists per film

### Proposed Enhancements

#### 4.1 Comprehensive Network Graph

```
┌─────────────────────────────────────────────────────────────────┐
│                    CINEMATIC KNOWLEDGE GRAPH                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│     🎬 Films ────────┬───────── 👤 People                       │
│         │           │              │                            │
│         │    acted_in/directed     │                            │
│         │           │              │                            │
│         ├───────────┼──────────────┤                            │
│         │           │              │                            │
│     🎭 Genres      🏷️ Keywords    🏢 Studios                    │
│         │           │              │                            │
│         └───────────┴──────────────┘                            │
│                     │                                           │
│              🌐 Wikidata Entities                               │
│                                                                 │
│  Edge Types:                                                    │
│  - acted_in (role, billing_order)                              │
│  - directed                                                     │
│  - wrote                                                        │
│  - composed_for                                                 │
│  - produced                                                     │
│  - collaborated_with (person-person)                           │
│  - similar_to (film-film)                                      │
│  - sequel_of / remake_of                                       │
│  - based_on (source material)                                  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### 4.2 Connection Analytics

```python
class ConnectionAnalyzer:
    """Analyze relationships and networks in your cinema universe."""
    
    def find_collaboration_chains(self) -> List[Dict]:
        """
        Actor A worked with Director B, who also worked with Actor C,
        who was in a film with Actor A → collaboration triangle
        """
        
    def calculate_bacon_numbers(self, target_person: str = "Kevin Bacon") -> Dict:
        """
        Degrees of separation from any actor to target.
        "Your collection's average Bacon number is 2.3"
        """
        
    def find_cinematic_couples(self) -> List[Dict]:
        """
        Real-life couples who appeared together:
        - Married actors in same films
        - Director-actor romantic partnerships
        """
        
    def identify_repertory_companies(self) -> List[Dict]:
        """
        Directors with recurring casts:
        - Scorsese-DiCaprio (7 films)
        - Wes Anderson regulars
        - Christopher Nolan ensemble
        """
        
    def trace_career_connections(self, person_id: str) -> Dict:
        """
        Who gave them their break?
        Who do they keep working with?
        Mentor-protégé relationships
        """
```

#### 4.3 Six Degrees Visualization

```python
def visualize_network(self, center_person: str, depth: int = 2) -> go.Figure:
    """
    Interactive network graph showing:
    - Center: Selected person
    - Level 1: Direct collaborators
    - Level 2: Collaborators of collaborators
    - Edge thickness: Number of collaborations
    - Node size: Number of films in collection
    - Node color: Profession (actor/director/writer)
    """
```

---

## 5. Implementation Phases

### Phase 1: Data Foundation (Week 1-2)

#### 1.1 Extended People Enrichment Script
```bash
python scripts/enrich/07_enrich_people_extended.py
```

New fields to fetch:
- [ ] Full birth/death dates (day, month, year)
- [ ] Cause of death (Wikidata P509)
- [ ] Height (Wikidata P2048)
- [ ] Spouses (Wikidata P26)
- [ ] Education (Wikidata P69)
- [ ] Burial location (Wikidata P119)

#### 1.2 Zodiac Calculation Module
```python
# src/analysis/zodiac.py
def calculate_zodiac(birth_date: str) -> Dict:
    return {
        'western_sign': 'Scorpio',
        'western_symbol': '♏',
        'western_element': 'Water',
        'chinese_zodiac': 'Tiger',
        'chinese_symbol': '🐅',
    }
```

#### 1.3 Keyword Enrichment Script
```bash
python scripts/enrich/08_enrich_keywords.py
```

- [ ] TMDB keywords API integration
- [ ] Keyword normalization and categorization
- [ ] Keyword frequency analysis

### Phase 2: Recommendation Engine (Week 3-4)

#### 2.1 New Module Structure
```
src/recommender/
├── __init__.py
├── engine.py           # Main recommendation engine
├── content_based.py    # Genre, keyword, theme matching
├── collaborative.py    # User pattern analysis
├── knowledge_graph.py  # Network-based recommendations
├── explainer.py        # Generate explanations
└── evaluator.py        # Recommendation quality metrics
```

#### 2.2 API Endpoints
```python
# api/app.py additions

@app.route('/api/recommendations/personalized')
def get_personalized_recommendations():
    """Multi-signal personalized recommendations."""

@app.route('/api/recommendations/similar/<movie_id>')
def get_similar_movies(movie_id):
    """Find similar movies with explanations."""

@app.route('/api/recommendations/explore')
def get_exploration_recommendations():
    """Recommendations to expand your horizons."""

@app.route('/api/recommendations/explain/<movie_id>')
def explain_recommendation(movie_id):
    """Why this movie was recommended."""
```

### Phase 3: Analytics Batch Scripts (Week 5-6)

#### 3.1 New Batch Scripts
```bash
# Zodiac and astrology analysis
python scripts/batch_9_zodiac_analysis.py

# Keywords and themes deep dive
python scripts/batch_10_keywords_themes.py

# Network and connections analysis
python scripts/batch_11_network_analysis.py

# Mortality and legacy analysis
python scripts/batch_12_legacy_analysis.py
```

### Phase 4: UI Integration (Week 7-8)

#### 4.1 New UI Pages
- [ ] `/recommendations` - Enhanced recommendation page
- [ ] `/person/:id/zodiac` - Zodiac info on person detail
- [ ] `/network` - Interactive collaboration network
- [ ] `/themes` - Keyword/theme explorer
- [ ] `/legacy` - In memoriam / legacy section

#### 4.2 New Components
- [ ] `ZodiacBadge` - Display zodiac signs
- [ ] `RecommendationCard` - With explanation
- [ ] `NetworkGraph` - D3/Vis.js network visualization
- [ ] `KeywordCloud` - Interactive word cloud

---

## 📊 New Visualizations Roadmap

### Batch 9: Zodiac & Astrology
1. `09_zodiac_distribution.png` - Actor zodiac sign distribution
2. `09_zodiac_quality.png` - Average rating by zodiac sign
3. `09_chinese_zodiac.png` - Chinese zodiac distribution
4. `09_zodiac_genres.png` - Zodiac × genre preferences
5. `09_birth_month_heatmap.png` - Actor births by month

### Batch 10: Keywords & Themes
1. `10_keyword_cloud.png` - Overall keyword frequency
2. `10_keyword_quality.png` - Keywords predicting high ratings
3. `10_theme_evolution.png` - Theme preferences over time
4. `10_theme_network.png` - Connected themes graph
5. `10_keyword_genre_heatmap.png` - Keywords × genres

### Batch 11: Network Analysis
1. `11_collaboration_network.html` - Interactive network graph
2. `11_bacon_numbers.png` - Degrees of separation distribution
3. `11_repertory_companies.png` - Director ensembles
4. `11_actor_connections.png` - Most connected actors
5. `11_cinematic_families.png` - Real-life couples/families

### Batch 12: Legacy & Mortality
1. `12_in_memoriam.png` - Deceased actors in collection
2. `12_cause_of_death.png` - Mortality statistics
3. `12_posthumous_releases.png` - Films released after death
4. `12_career_cut_short.png` - Lost potential analysis
5. `12_legacy_scores.png` - Lasting impact metrics

---

## 🔧 Technical Requirements

### New Dependencies
```
# requirements.txt additions
networkx>=3.0           # Network/graph analysis
pyvis>=0.3.0            # Interactive network visualization
wordcloud>=1.9.0        # Keyword clouds
spacy>=3.7.0            # NLP for keyword extraction
scikit-learn>=1.4.0     # ML for recommendations
```

### Database Additions
```sql
-- New tables for recommendations
CREATE TABLE user_preferences (
    id INTEGER PRIMARY KEY,
    genre_weights JSON,
    decade_weights JSON,
    runtime_preference JSON,
    created_at TIMESTAMP
);

CREATE TABLE recommendations_log (
    id INTEGER PRIMARY KEY,
    movie_id TEXT,
    score REAL,
    reasons JSON,
    created_at TIMESTAMP,
    clicked BOOLEAN,
    watched BOOLEAN
);
```

---

## 🚀 Quick Start for Implementation

```bash
# 1. Create new branch for enhancements
git checkout -b feature/advanced-recommendations

# 2. Run extended people enrichment
python scripts/enrich/07_enrich_people_extended.py

# 3. Run keyword enrichment
python scripts/enrich/08_enrich_keywords.py

# 4. Generate new visualizations
python scripts/batch_9_zodiac_analysis.py
python scripts/batch_10_keywords_themes.py

# 5. Test recommendation engine
python -m src.recommender.engine --test

# 6. Start API and UI
cd api && python app.py &
cd ui && npm run dev
```

---

*This roadmap transforms CineScope from a personal analytics project into a professional-grade cinema intelligence platform.*
