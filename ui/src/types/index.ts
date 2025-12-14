// Movie types
export interface Movie {
  id?: string;
  const: string; // IMDb ID (same as tconst)
  title: string;
  original_title?: string;
  year: number;
  imdbRating?: number;
  yourRating?: number;
  runtime?: number;
  genres: string | string[];
  directors?: string | string[];
  numVotes?: number;
  dateRated?: string;
  plot?: string;
  overview?: string;
  posterUrl?: string;
  backdropUrl?: string;
  cast?: string;
  countries?: string;
  awards?: string;
  releaseDate?: string;

  // TMDB enrichment
  tmdb_id?: number;
  tmdb_vote_average?: number;
  tmdb_vote_count?: number;
  tmdb_popularity?: number;
  tmdb_tagline?: string;
  tmdb_budget?: number;
  tmdb_revenue?: number;

  // OMDB enrichment
  omdb_awards?: string;
  omdb_metascore?: number;
  omdb_rotten_tomatoes?: string;
  omdb_rated?: string;
  omdb_language?: string;
  omdb_country?: string;

  // DDD enrichment
  ddd_dog_dies?: boolean;
  ddd_dog_survives?: boolean;

  // Wikidata enrichment
  wd_box_office?: string;
  wd_filming_location?: string;
}

// Person types
export interface Person {
  // API response format
  id?: string | number;
  name?: string;
  profilePath?: string;
  knownFor?: string;
  birthYear?: number | string;
  biography?: string;
  birthplace?: string;
  imdbId?: string;
  movieCount?: number;
  avgRating?: number;

  // Legacy format
  nconst?: string; // IMDB person ID
  tmdb_id?: number;
  primaryName?: string;
  deathYear?: number;
  primaryProfession?: string;
  knownForTitles?: string;
  tmdb_profile_path?: string;
  tmdb_biography?: string;

  // Wikidata enrichment
  wd_birthplace?: string;
  wd_nationality?: string;
  wd_awards_count?: number;
  wd_wikipedia_url?: string;
}

// Recommendation types
export interface Recommendation {
  movie: Movie;
  reason: string;
  score: number;
  type: "similar" | "gap" | "pattern" | "exploration";
}

export interface RecommendationSet {
  title: string;
  description: string;
  recommendations: Recommendation[];
}

// Stats types
export interface CollectionStats {
  totalMovies: number;
  avgRating: number;
  totalHours: number;
  topYear?: number;
  totalGenres?: number;
}

// Filter types
export interface MovieFilters {
  search?: string;
  genre?: string;
  genres?: string[];
  minYear?: number;
  maxYear?: number;
  minRating?: number;
  sortBy?: "rating" | "year" | "title" | "date_rated";
  sortOrder?: "asc" | "desc";
}

// API response types
export interface PaginatedResponse<T> {
  results: T[];
  data?: T[]; // Alternative property name
  total: number;
  page: number;
  pageSize: number;
  totalPages: number;
}

// Person Analytics types
export interface PersonAnalytics {
  person: {
    id: string;
    name: string;
    knownFor: string;
    profilePath?: string;
    biography?: string;
    birthYear?: string;
    birthplace?: string;
    awardsCount?: number;
    wikipediaUrl?: string;
    imdbId?: string;
  };
  filmCount: number;
  analytics: {
    avgRating: number | null;
    highestRating: number | null;
    lowestRating: number | null;
    totalRuntime: number;
    avgRuntime: number | null;
    careerSpan: {
      start: number;
      end: number;
      years: number;
    } | null;
    genreDistribution: Array<{
      genre: string;
      count: number;
      percentage: number;
    }>;
    decadeDistribution: Array<{
      decade: string;
      count: number;
      avgRating: number | null;
    }>;
    careerTimeline: Array<{
      year: number;
      count: number;
      avgRating: number | null;
      movies: Array<{ title: string; rating: number | null; const: string }>;
    }>;
    ratingDistribution: Array<{ rating: number; count: number }>;
    topCollaborations: Array<{ name: string; count: number }>;
    bestFilms: Array<{
      title: string;
      year: number;
      rating: number;
      const: string;
    }>;
    worstFilms: Array<{
      title: string;
      year: number;
      rating: number;
      const: string;
    }>;
    roleBreakdown: {
      actor: number;
      director: number;
      writer: number;
    };
  };
  films: Array<{
    title: string;
    year: number;
    rating: number | null;
    genres: string;
    runtime: number | null;
    role: string;
    const: string;
    poster?: string;
  }>;
}

export interface ActorStats {
  id: string | null;
  name: string;
  profilePath: string | null;
  filmCount: number;
  avgRating: number | null;
  highestRating: number | null;
  careerSpan: string | null;
  topGenre: string | null;
}

export interface ActorsAnalytics {
  actors: ActorStats[];
  summary: {
    totalActors: number;
    actorsWithMultipleFilms: number;
    avgFilmsPerActor: number;
    avgRatingAcrossActors: number;
    topActorFilmCount: number;
  };
}
