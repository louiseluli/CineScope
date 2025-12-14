import axios from "axios";
import type {
  Movie,
  Person,
  CollectionStats,
  MovieFilters,
  PaginatedResponse,
  RecommendationSet,
  PersonAnalytics,
  ActorsAnalytics,
} from "../types";

const api = axios.create({
  baseURL: "/api",
  headers: {
    "Content-Type": "application/json",
  },
});

// Movies API
export const moviesApi = {
  getAll: async (
    filters?: MovieFilters,
    page = 1,
    pageSize = 24
  ): Promise<PaginatedResponse<Movie>> => {
    const response = await api.get("/movies", {
      params: { ...filters, page, pageSize },
    });
    return response.data;
  },

  getById: async (id: string): Promise<Movie> => {
    const response = await api.get(`/movies/${id}`);
    return response.data;
  },

  getSimilar: async (id: string, limit = 10): Promise<Movie[]> => {
    const response = await api.get(`/movies/${id}/similar`, {
      params: { limit },
    });
    return response.data;
  },

  search: async (query: string, limit = 20): Promise<Movie[]> => {
    const response = await api.get("/movies/search", {
      params: { q: query, limit },
    });
    return response.data;
  },
};

// People API
export const peopleApi = {
  getAll: async (filters?: {
    search?: string;
    page?: number;
    role?: string;
  }): Promise<PaginatedResponse<Person>> => {
    const params: Record<string, string | number> = {};
    if (filters?.search) params.q = filters.search;
    if (filters?.page) params.page = filters.page;
    if (filters?.role && filters.role !== "all") params.role = filters.role;
    const response = await api.get("/people", { params });
    return response.data;
  },

  getById: async (id: string): Promise<Person> => {
    const response = await api.get(`/people/${id}`);
    return response.data;
  },

  search: async (query: string, limit = 20): Promise<Person[]> => {
    const response = await api.get("/people/search", {
      params: { q: query, limit },
    });
    return response.data;
  },

  getFilmography: async (id: string): Promise<Movie[]> => {
    const response = await api.get(`/people/${id}/filmography`);
    return response.data;
  },

  getAnalytics: async (id: string): Promise<PersonAnalytics> => {
    const response = await api.get(`/people/${id}/analytics`);
    return response.data;
  },
};

// Analytics API
export const analyticsApi = {
  getActors: async (): Promise<ActorsAnalytics> => {
    const response = await api.get("/analytics/actors");
    return response.data;
  },

  compareActors: async (actor1: string, actor2: string): Promise<unknown> => {
    const response = await api.get("/analytics/actors/compare", {
      params: { actor1, actor2 },
    });
    return response.data;
  },
};

// Recommendations API
export const recommendationsApi = {
  getForYou: async (): Promise<RecommendationSet[]> => {
    const response = await api.get("/recommendations");
    return response.data;
  },

  getGaps: async (): Promise<RecommendationSet[]> => {
    const response = await api.get("/recommendations/gaps");
    return response.data;
  },

  getSimilarTo: async (movieId: string): Promise<Movie[]> => {
    const response = await api.get(`/recommendations/similar/${movieId}`);
    return response.data;
  },
};

// Stats API
export const statsApi = {
  getOverview: async (): Promise<CollectionStats> => {
    const response = await api.get("/stats");
    return response.data;
  },

  getGenres: async (): Promise<
    { genre: string; count: number; avgRating: number }[]
  > => {
    const response = await api.get("/stats/genres");
    return response.data;
  },

  getDecades: async (): Promise<
    { decade: string; count: number; avgRating: number }[]
  > => {
    const response = await api.get("/stats/decades");
    return response.data;
  },

  getDirectors: async (): Promise<
    { name: string; movieCount: number; avgRating: number }[]
  > => {
    const response = await api.get("/stats/directors");
    return response.data;
  },
};

// Filters API
export const filtersApi = {
  getGenres: async (): Promise<string[]> => {
    const response = await api.get("/filters/genres");
    return response.data;
  },

  getYearRange: async (): Promise<{ min: number; max: number }> => {
    const response = await api.get("/filters/years");
    return response.data;
  },
};

export default api;
