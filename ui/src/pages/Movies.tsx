import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { Grid, List, SlidersHorizontal } from "lucide-react";
import { clsx } from "clsx";
import { useMovies, useGenreOptions } from "../hooks/useMovies";
import { MovieCard, MovieListCard } from "../components/movies/MovieCard";
import { DebouncedSearch } from "../components/common/SearchBar";
import type { MovieFilters, Movie } from "../types";

export function MoviesPage() {
  const navigate = useNavigate();
  const [viewMode, setViewMode] = useState<"grid" | "list">("grid");
  const [page, setPage] = useState(1);
  const [showFilters, setShowFilters] = useState(false);
  const [filters, setFilters] = useState<MovieFilters>({
    search: "",
    genres: [],
    sortBy: "date_rated",
    sortOrder: "desc",
  });

  const { data: genres } = useGenreOptions();
  const { data, isLoading, isError } = useMovies(filters, page, 24);

  const handleFilterChange = (key: keyof MovieFilters, value: any) => {
    setFilters(prev => ({ ...prev, [key]: value }));
    setPage(1);
  };

  const toggleGenre = (genre: string) => {
    const current = filters.genres || [];
    const updated = current.includes(genre)
      ? current.filter(g => g !== genre)
      : [...current, genre];
    handleFilterChange("genres", updated);
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-[var(--text)]">Movies</h1>
          <p className="text-[var(--text-muted)]">
            {data?.total?.toLocaleString() || 0} movies in your collection
          </p>
        </div>

        <div className="flex items-center gap-2">
          {/* View Toggle */}
          <div className="flex bg-[var(--surface)] rounded-lg p-1 border border-[var(--border)]">
            <button
              onClick={() => setViewMode("grid")}
              className={clsx(
                "p-2 rounded",
                viewMode === "grid"
                  ? "bg-[var(--primary)] text-white"
                  : "text-[var(--text-muted)]"
              )}
            >
              <Grid className="w-4 h-4" />
            </button>
            <button
              onClick={() => setViewMode("list")}
              className={clsx(
                "p-2 rounded",
                viewMode === "list"
                  ? "bg-[var(--primary)] text-white"
                  : "text-[var(--text-muted)]"
              )}
            >
              <List className="w-4 h-4" />
            </button>
          </div>

          {/* Filter Toggle */}
          <button
            onClick={() => setShowFilters(!showFilters)}
            className={clsx(
              "flex items-center gap-2 px-4 py-2 rounded-lg border transition-colors",
              showFilters
                ? "bg-[var(--primary)] text-white border-[var(--primary)]"
                : "bg-[var(--surface)] text-[var(--text)] border-[var(--border)] hover:border-[var(--primary)]/50"
            )}
          >
            <SlidersHorizontal className="w-4 h-4" />
            Filters
          </button>
        </div>
      </div>

      {/* Search & Filters */}
      <div className="space-y-4">
        <DebouncedSearch
          value={filters.search || ""}
          onChange={value => handleFilterChange("search", value)}
          placeholder="Search by title, director, actor..."
        />

        {/* Expanded Filters */}
        {showFilters && (
          <div className="p-4 bg-[var(--surface)] rounded-xl border border-[var(--border)] space-y-4">
            {/* Sort */}
            <div>
              <label className="text-sm font-medium text-[var(--text-muted)] mb-2 block">
                Sort by
              </label>
              <div className="flex flex-wrap gap-2">
                {[
                  { value: "date_rated", label: "Date Rated" },
                  { value: "rating", label: "Rating" },
                  { value: "year", label: "Year" },
                  { value: "title", label: "Title" },
                  { value: "popularity", label: "Popularity" },
                ].map(option => (
                  <button
                    key={option.value}
                    onClick={() => handleFilterChange("sortBy", option.value)}
                    className={clsx(
                      "px-3 py-1.5 rounded-lg text-sm transition-colors",
                      filters.sortBy === option.value
                        ? "bg-[var(--primary)] text-white"
                        : "bg-[var(--border)] text-[var(--text-muted)] hover:text-[var(--text)]"
                    )}
                  >
                    {option.label}
                  </button>
                ))}
              </div>
            </div>

            {/* Genres */}
            <div>
              <label className="text-sm font-medium text-[var(--text-muted)] mb-2 block">
                Genres
              </label>
              <div className="flex flex-wrap gap-2">
                {genres?.slice(0, 15).map(genre => (
                  <button
                    key={genre}
                    onClick={() => toggleGenre(genre)}
                    className={clsx(
                      "px-3 py-1.5 rounded-lg text-sm transition-colors",
                      filters.genres?.includes(genre)
                        ? "bg-[var(--secondary)] text-white"
                        : "bg-[var(--border)] text-[var(--text-muted)] hover:text-[var(--text)]"
                    )}
                  >
                    {genre}
                  </button>
                ))}
              </div>
            </div>

            {/* Clear Filters */}
            {(filters.genres?.length || filters.search) && (
              <button
                onClick={() =>
                  setFilters({ sortBy: "date_rated", sortOrder: "desc" })
                }
                className="text-sm text-[var(--primary)] hover:underline"
              >
                Clear all filters
              </button>
            )}
          </div>
        )}
      </div>

      {/* Results */}
      {isLoading ? (
        <div className="flex items-center justify-center py-20">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]" />
        </div>
      ) : isError ? (
        <div className="text-center py-20 text-[var(--text-muted)]">
          <p>Failed to load movies. Please try again.</p>
        </div>
      ) : (
        <>
          {viewMode === "grid" ? (
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-4">
              {(data?.results || data?.data || []).map((movie: Movie) => (
                <MovieCard
                  key={movie.const}
                  movie={movie}
                  onClick={() => navigate("/movies/" + movie.const)}
                />
              ))}
            </div>
          ) : (
            <div className="space-y-2">
              {(data?.results || data?.data || []).map((movie: Movie) => (
                <MovieListCard
                  key={movie.const}
                  movie={movie}
                  onClick={() => navigate("/movies/" + movie.const)}
                />
              ))}
            </div>
          )}

          {/* Pagination */}
          {data && data.totalPages > 1 && (
            <div className="flex items-center justify-center gap-2 pt-6">
              <button
                onClick={() => setPage(p => Math.max(1, p - 1))}
                disabled={page === 1}
                className="px-4 py-2 bg-[var(--surface)] border border-[var(--border)] rounded-lg text-sm disabled:opacity-50 disabled:cursor-not-allowed hover:border-[var(--primary)]/50 transition-colors"
              >
                Previous
              </button>
              <span className="text-sm text-[var(--text-muted)]">
                Page {page} of {data.totalPages}
              </span>
              <button
                onClick={() => setPage(p => Math.min(data.totalPages, p + 1))}
                disabled={page === data.totalPages}
                className="px-4 py-2 bg-[var(--surface)] border border-[var(--border)] rounded-lg text-sm disabled:opacity-50 disabled:cursor-not-allowed hover:border-[var(--primary)]/50 transition-colors"
              >
                Next
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
}
