import { Star, Clock, Calendar } from "lucide-react";
import { clsx } from "clsx";
import type { Movie } from "../../types";

interface MovieCardProps {
  movie: Movie;
  onClick?: () => void;
  size?: "sm" | "md" | "lg";
}

const TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p";

function getRatingClass(rating: number): string {
  if (rating >= 8) return "rating-excellent";
  if (rating >= 7) return "rating-good";
  if (rating >= 6) return "rating-average";
  if (rating >= 5) return "rating-poor";
  return "rating-bad";
}

// Helper to ensure genres is an array
function getGenresArray(genres: string | string[] | undefined): string[] {
  if (!genres) return [];
  if (Array.isArray(genres)) return genres;
  return genres
    .split(",")
    .map(g => g.trim())
    .filter(Boolean);
}

export function MovieCard({ movie, onClick, size = "md" }: MovieCardProps) {
  const posterUrl =
    movie.posterUrl ||
    (movie.posterUrl?.startsWith("/")
      ? `${TMDB_IMAGE_BASE}/w${size === "lg" ? "500" : "342"}${movie.posterUrl}`
      : null);

  const cardSizes = {
    sm: "w-32",
    md: "w-44",
    lg: "w-56",
  };

  return (
    <div
      className={clsx(
        "group relative rounded-lg overflow-hidden bg-[var(--surface)] card-hover cursor-pointer",
        cardSizes[size]
      )}
      onClick={onClick}
    >
      {/* Poster */}
      <div className="aspect-[2/3] bg-[var(--surface-hover)]">
        {posterUrl ? (
          <img
            src={posterUrl}
            alt={movie.title}
            className="w-full h-full object-cover"
            loading="lazy"
          />
        ) : (
          <div className="w-full h-full flex items-center justify-center text-[var(--text-muted)]">
            <span className="text-xs text-center px-2">{movie.title}</span>
          </div>
        )}
      </div>

      {/* Rating Badge */}
      {movie.imdbRating && (
        <div
          className={clsx(
            "absolute top-2 right-2 px-2 py-0.5 rounded text-xs font-bold text-white",
            getRatingClass(movie.imdbRating)
          )}
        >
          {movie.imdbRating.toFixed(1)}
        </div>
      )}

      {/* Your Rating Badge */}
      {movie.yourRating && (
        <div className="absolute top-2 left-2 px-2 py-0.5 rounded text-xs font-bold bg-[var(--primary)] text-white">
          ★ {movie.yourRating}
        </div>
      )}

      {/* Hover Overlay */}
      <div className="absolute inset-0 bg-gradient-to-t from-black/90 via-black/50 to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-300">
        <div className="absolute bottom-0 left-0 right-0 p-3">
          <h3 className="font-semibold text-white text-sm line-clamp-2 mb-1">
            {movie.title}
          </h3>
          <div className="flex items-center gap-2 text-xs text-gray-300">
            {movie.year && (
              <span className="flex items-center gap-1">
                <Calendar className="w-3 h-3" />
                {movie.year}
              </span>
            )}
            {movie.runtime && (
              <span className="flex items-center gap-1">
                <Clock className="w-3 h-3" />
                {movie.runtime}m
              </span>
            )}
          </div>
          {movie.genres && (
            <div className="flex flex-wrap gap-1 mt-2">
              {getGenresArray(movie.genres)
                .slice(0, 2)
                .map((genre: string) => (
                  <span
                    key={genre}
                    className="px-1.5 py-0.5 bg-white/20 rounded text-xs text-white"
                  >
                    {genre}
                  </span>
                ))}
            </div>
          )}
        </div>
      </div>

      {/* Info below poster */}
      <div className="p-2">
        <h3 className="font-medium text-sm line-clamp-1 text-[var(--text)]">
          {movie.title}
        </h3>
        <p className="text-xs text-[var(--text-muted)]">
          {movie.year}{" "}
          {movie.directors &&
            movie.directors.length > 0 &&
            `• ${movie.directors[0]}`}
        </p>
      </div>
    </div>
  );
}

// Compact list view card
export function MovieListCard({ movie, onClick }: MovieCardProps) {
  const posterUrl = movie.posterUrl || null;

  return (
    <div
      className="flex gap-3 p-3 rounded-lg bg-[var(--surface)] hover:bg-[var(--surface-hover)] cursor-pointer transition-colors"
      onClick={onClick}
    >
      {/* Small Poster */}
      <div className="w-12 h-18 flex-shrink-0 rounded overflow-hidden bg-[var(--surface-hover)]">
        {posterUrl ? (
          <img
            src={posterUrl}
            alt={movie.title}
            className="w-full h-full object-cover"
          />
        ) : (
          <div className="w-full h-full flex items-center justify-center text-[var(--text-muted)] text-xs">
            N/A
          </div>
        )}
      </div>

      {/* Info */}
      <div className="flex-1 min-w-0">
        <div className="flex items-start justify-between gap-2">
          <h3 className="font-medium text-sm text-[var(--text)] line-clamp-1">
            {movie.title}
          </h3>
          <div className="flex items-center gap-1 flex-shrink-0">
            <Star className="w-3 h-3 text-yellow-500 fill-yellow-500" />
            <span className="text-sm font-medium">
              {movie.imdbRating?.toFixed(1) || "N/A"}
            </span>
          </div>
        </div>
        <p className="text-xs text-[var(--text-muted)] mt-0.5">
          {movie.year} • {movie.runtime}m •{" "}
          {Array.isArray(movie.directors)
            ? movie.directors[0]
            : movie.directors}
        </p>
        {movie.genres && (
          <div className="flex flex-wrap gap-1 mt-1">
            {getGenresArray(movie.genres)
              .slice(0, 3)
              .map((genre: string) => (
                <span
                  key={genre}
                  className="px-1.5 py-0.5 bg-[var(--border)] rounded text-xs text-[var(--text-muted)]"
                >
                  {genre}
                </span>
              ))}
          </div>
        )}
      </div>

      {/* Your Rating */}
      {movie.yourRating && (
        <div className="flex items-center gap-1 text-[var(--primary)]">
          <Star className="w-4 h-4 fill-current" />
          <span className="font-bold">{movie.yourRating}</span>
        </div>
      )}
    </div>
  );
}
