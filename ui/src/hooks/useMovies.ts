import { useQuery } from "@tanstack/react-query";
import {
  moviesApi,
  peopleApi,
  recommendationsApi,
  statsApi,
  filtersApi,
  analyticsApi,
} from "../api";
import type { MovieFilters } from "../types";

// Movies hooks
export function useMovies(filters?: MovieFilters, page = 1, pageSize = 24) {
  return useQuery({
    queryKey: ["movies", filters, page, pageSize],
    queryFn: () => moviesApi.getAll(filters, page, pageSize),
  });
}

export function useMovie(id: string) {
  return useQuery({
    queryKey: ["movie", id],
    queryFn: () => moviesApi.getById(id),
    enabled: !!id,
  });
}

export function useSimilarMovies(id: string) {
  return useQuery({
    queryKey: ["movie", id, "similar"],
    queryFn: () => moviesApi.getSimilar(id),
    enabled: !!id,
  });
}

export function useMovieSearch(query: string) {
  return useQuery({
    queryKey: ["movies", "search", query],
    queryFn: () => moviesApi.search(query),
    enabled: query.length >= 2,
  });
}

// People hooks
export function usePerson(id: string) {
  return useQuery({
    queryKey: ["person", id],
    queryFn: () => peopleApi.getById(id),
    enabled: !!id,
  });
}

export function usePersonSearch(query: string) {
  return useQuery({
    queryKey: ["people", "search", query],
    queryFn: () => peopleApi.search(query),
    enabled: query.length >= 2,
  });
}

export function useFilmography(personId: string) {
  return useQuery({
    queryKey: ["person", personId, "filmography"],
    queryFn: () => peopleApi.getFilmography(personId),
    enabled: !!personId,
  });
}

// Recommendations hooks
export function useRecommendations() {
  return useQuery({
    queryKey: ["recommendations"],
    queryFn: () => recommendationsApi.getForYou(),
  });
}

export function useGapRecommendations() {
  return useQuery({
    queryKey: ["recommendations", "gaps"],
    queryFn: () => recommendationsApi.getGaps(),
  });
}

// Stats hooks
export function useStats() {
  return useQuery({
    queryKey: ["stats"],
    queryFn: () => statsApi.getOverview(),
  });
}

export function useGenreStats() {
  return useQuery({
    queryKey: ["stats", "genres"],
    queryFn: () => statsApi.getGenres(),
  });
}

export function useDecadeStats() {
  return useQuery({
    queryKey: ["stats", "decades"],
    queryFn: () => statsApi.getDecades(),
  });
}

export function useDirectorStats() {
  return useQuery({
    queryKey: ["stats", "directors"],
    queryFn: () => statsApi.getDirectors(),
  });
}

export function usePeople(filters?: {
  search?: string;
  page?: number;
  role?: string;
}) {
  return useQuery({
    queryKey: ["people", filters],
    queryFn: () => peopleApi.getAll(filters),
  });
}

export function usePersonFilmography(id: string) {
  return useQuery({
    queryKey: ["person", id, "filmography"],
    queryFn: () => peopleApi.getFilmography(id),
    enabled: !!id,
  });
}

// Filters hooks
export function useGenreOptions() {
  return useQuery({
    queryKey: ["filters", "genres"],
    queryFn: () => filtersApi.getGenres(),
  });
}

export function useYearRange() {
  return useQuery({
    queryKey: ["filters", "years"],
    queryFn: () => filtersApi.getYearRange(),
  });
}

// Actor Analytics hooks
export function usePersonAnalytics(id: string) {
  return useQuery({
    queryKey: ["person", id, "analytics"],
    queryFn: () => peopleApi.getAnalytics(id),
    enabled: !!id,
  });
}

export function useActorsAnalytics() {
  return useQuery({
    queryKey: ["analytics", "actors"],
    queryFn: () => analyticsApi.getActors(),
  });
}
