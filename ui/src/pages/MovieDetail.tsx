import { useParams, Link, useNavigate } from "react-router-dom";
import {
  ArrowLeft,
  Star,
  Clock,
  Calendar,
  Globe,
  Award,
  Play,
  Users,
  Film,
} from "lucide-react";
import { useMovie, useSimilarMovies } from "../hooks/useMovies";
import { MovieCard } from "../components/movies/MovieCard";
import type { Movie } from "../types";

export function MovieDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { data: movie, isLoading, error } = useMovie(id || "");
  const { data: similarMovies } = useSimilarMovies(id || "");

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]" />
      </div>
    );
  }

  if (error || !movie) {
    return (
      <div className="text-center py-20">
        <Film className="w-16 h-16 text-[var(--error)] mx-auto mb-4" />
        <h2 className="text-xl font-semibold text-[var(--text)] mb-2">
          Movie not found
        </h2>
        <p className="text-[var(--text-muted)] mb-4">
          The movie you're looking for doesn't exist or couldn't be loaded.
        </p>
        <Link
          to="/movies"
          className="text-[var(--primary)] hover:underline inline-flex items-center gap-2"
        >
          <ArrowLeft className="w-4 h-4" />
          Back to Movies
        </Link>
      </div>
    );
  }

  const placeholderPoster =
    "data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjAwIiBoZWlnaHQ9IjMwMCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48cmVjdCB3aWR0aD0iMjAwIiBoZWlnaHQ9IjMwMCIgZmlsbD0iIzM3M2Y0NyIvPjx0ZXh0IHg9IjEwMCIgeT0iMTUwIiBmb250LWZhbWlseT0ic2Fucy1zZXJpZiIgZm9udC1zaXplPSI0MCIgZmlsbD0iIzZiN2Q4YSIgdGV4dC1hbmNob3I9Im1pZGRsZSI+8J+OrDwvdGV4dD48L3N2Zz4=";

  // Get genres as array
  const getGenresArray = (genres: string | string[] | undefined): string[] => {
    if (!genres) return [];
    if (Array.isArray(genres)) return genres;
    return genres.split(",").map(g => g.trim());
  };

  const genresArray = getGenresArray(movie.genres);
  const runtime = movie.runtime || 0;

  return (
    <div className="space-y-8">
      {/* Back Button */}
      <Link
        to="/movies"
        className="inline-flex items-center gap-2 text-[var(--text-muted)] hover:text-[var(--text)] transition-colors"
      >
        <ArrowLeft className="w-5 h-5" />
        Back to Movies
      </Link>

      {/* Hero Section with Backdrop */}
      <div className="relative rounded-2xl overflow-hidden">
        {movie.backdropUrl && (
          <div className="absolute inset-0">
            <img
              src={"https://image.tmdb.org/t/p/w1280" + movie.backdropUrl}
              alt=""
              className="w-full h-full object-cover opacity-30"
            />
            <div className="absolute inset-0 bg-gradient-to-t from-[var(--bg-dark)] via-[var(--bg-dark)]/90 to-[var(--bg-dark)]/70" />
          </div>
        )}

        <div className="relative p-8 flex gap-8">
          {/* Poster */}
          <div className="flex-shrink-0 w-64">
            <img
              src={
                movie.posterUrl
                  ? "https://image.tmdb.org/t/p/w500" + movie.posterUrl
                  : placeholderPoster
              }
              alt={movie.title}
              className="w-full rounded-xl shadow-2xl"
              onError={e => {
                (e.target as HTMLImageElement).src = placeholderPoster;
              }}
            />
          </div>

          {/* Info */}
          <div className="flex-1 space-y-6">
            <div>
              <h1 className="text-4xl font-bold text-[var(--text)] mb-2">
                {movie.title}
              </h1>
              {movie.original_title && movie.original_title !== movie.title && (
                <p className="text-lg text-[var(--text-muted)] italic">
                  {movie.original_title}
                </p>
              )}
            </div>

            {/* Meta Info */}
            <div className="flex flex-wrap gap-4 text-sm">
              {movie.year && (
                <div className="flex items-center gap-2 text-[var(--text-muted)]">
                  <Calendar className="w-4 h-4" />
                  {movie.year}
                </div>
              )}
              {runtime > 0 && (
                <div className="flex items-center gap-2 text-[var(--text-muted)]">
                  <Clock className="w-4 h-4" />
                  {Math.floor(runtime / 60)}h {runtime % 60}m
                </div>
              )}
              {movie.countries && (
                <div className="flex items-center gap-2 text-[var(--text-muted)]">
                  <Globe className="w-4 h-4" />
                  {movie.countries}
                </div>
              )}
            </div>

            {/* Ratings */}
            <div className="flex gap-6">
              {movie.imdbRating && (
                <div className="bg-[var(--surface)] rounded-lg px-4 py-3">
                  <div className="flex items-center gap-2 mb-1">
                    <Star className="w-5 h-5 text-yellow-500" />
                    <span className="text-lg font-bold text-[var(--text)]">
                      {movie.imdbRating}
                    </span>
                  </div>
                  <p className="text-xs text-[var(--text-muted)]">
                    IMDb Rating
                  </p>
                </div>
              )}
              {movie.yourRating && (
                <div className="bg-[var(--primary)]/20 rounded-lg px-4 py-3">
                  <div className="flex items-center gap-2 mb-1">
                    <Star className="w-5 h-5 text-[var(--primary)]" />
                    <span className="text-lg font-bold text-[var(--text)]">
                      {movie.yourRating}
                    </span>
                  </div>
                  <p className="text-xs text-[var(--text-muted)]">
                    Your Rating
                  </p>
                </div>
              )}
            </div>

            {/* Genres */}
            {genresArray.length > 0 && (
              <div className="flex flex-wrap gap-2">
                {genresArray.map(genre => (
                  <span
                    key={genre}
                    className="px-3 py-1 text-sm rounded-full bg-[var(--surface)] text-[var(--text)]"
                  >
                    {genre}
                  </span>
                ))}
              </div>
            )}

            {/* Overview */}
            {movie.overview && (
              <p className="text-[var(--text-muted)] leading-relaxed max-w-2xl">
                {movie.overview}
              </p>
            )}

            {/* Director */}
            {movie.directors && (
              <div className="flex items-center gap-2">
                <Users className="w-5 h-5 text-[var(--text-muted)]" />
                <span className="text-[var(--text-muted)]">Directed by:</span>
                <span className="text-[var(--text)] font-medium">
                  {Array.isArray(movie.directors)
                    ? movie.directors.join(", ")
                    : movie.directors}
                </span>
              </div>
            )}

            {/* Awards */}
            {movie.awards && (
              <div className="flex items-center gap-2 text-yellow-500">
                <Award className="w-5 h-5" />
                <span>{movie.awards}</span>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Cast Section */}
      {movie.cast && (
        <section>
          <h2 className="text-2xl font-bold text-[var(--text)] mb-4 flex items-center gap-2">
            <Users className="w-6 h-6" />
            Cast
          </h2>
          <div className="flex gap-4 overflow-x-auto pb-4">
            {movie.cast
              .split(",")
              .slice(0, 10)
              .map((actor: string) => (
                <div key={actor.trim()} className="flex-shrink-0 text-center">
                  <div className="w-20 h-20 rounded-full bg-[var(--surface)] flex items-center justify-center mb-2">
                    <Users className="w-8 h-8 text-[var(--text-muted)]" />
                  </div>
                  <p className="text-sm text-[var(--text)] truncate max-w-[80px]">
                    {actor.trim()}
                  </p>
                </div>
              ))}
          </div>
        </section>
      )}

      {/* Similar Movies */}
      {similarMovies && similarMovies.length > 0 && (
        <section>
          <h2 className="text-2xl font-bold text-[var(--text)] mb-4 flex items-center gap-2">
            <Film className="w-6 h-6" />
            Similar Movies
          </h2>
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-4">
            {similarMovies.slice(0, 6).map((m: Movie) => (
              <MovieCard
                key={m.const || m.id}
                movie={m}
                onClick={() => navigate("/movies/" + (m.const || m.id))}
              />
            ))}
          </div>
        </section>
      )}

      {/* Watch Links */}
      {movie.const && (
        <section>
          <h2 className="text-2xl font-bold text-[var(--text)] mb-4 flex items-center gap-2">
            <Play className="w-6 h-6" />
            External Links
          </h2>
          <div className="flex gap-4">
            <a
              href={"https://www.imdb.com/title/" + movie.const}
              target="_blank"
              rel="noopener noreferrer"
              className="px-6 py-3 bg-yellow-500 text-black font-bold rounded-lg hover:bg-yellow-400 transition-colors"
            >
              View on IMDb
            </a>
            {movie.tmdb_id && (
              <a
                href={"https://www.themoviedb.org/movie/" + movie.tmdb_id}
                target="_blank"
                rel="noopener noreferrer"
                className="px-6 py-3 bg-[var(--surface)] text-[var(--text)] font-medium rounded-lg hover:bg-[var(--surface-hover)] transition-colors"
              >
                View on TMDb
              </a>
            )}
          </div>
        </section>
      )}
    </div>
  );
}
