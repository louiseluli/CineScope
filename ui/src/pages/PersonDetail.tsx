import { useParams, Link, useNavigate } from "react-router-dom";
import { ArrowLeft, Film, Star, Calendar, MapPin, User } from "lucide-react";
import { usePerson, usePersonFilmography } from "../hooks/useMovies";
import { MovieCard } from "../components/movies/MovieCard";
import type { Movie } from "../types";

export function PersonDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { data: person, isLoading, error } = usePerson(id || "");
  const { data: filmography } = usePersonFilmography(id || "");

  const placeholderImage =
    "data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjAwIiBoZWlnaHQ9IjMwMCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48cmVjdCB3aWR0aD0iMjAwIiBoZWlnaHQ9IjMwMCIgZmlsbD0iIzM3M2Y0NyIvPjx0ZXh0IHg9IjEwMCIgeT0iMTUwIiBmb250LWZhbWlseT0ic2Fucy1zZXJpZiIgZm9udC1zaXplPSI0MCIgZmlsbD0iIzZiN2Q4YSIgdGV4dC1hbmNob3I9Im1pZGRsZSI+8J+RpDwvdGV4dD48L3N2Zz4=";

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]" />
      </div>
    );
  }

  if (error || !person) {
    return (
      <div className="text-center py-20">
        <User className="w-16 h-16 text-[var(--error)] mx-auto mb-4" />
        <h2 className="text-xl font-semibold text-[var(--text)] mb-2">
          Person not found
        </h2>
        <p className="text-[var(--text-muted)] mb-4">
          The person you're looking for doesn't exist or couldn't be loaded.
        </p>
        <Link
          to="/people"
          className="text-[var(--primary)] hover:underline inline-flex items-center gap-2"
        >
          <ArrowLeft className="w-4 h-4" />
          Back to People
        </Link>
      </div>
    );
  }

  const imageUrl = person.profilePath
    ? `https://image.tmdb.org/t/p/w300${person.profilePath}`
    : placeholderImage;

  const getName = () => person.name || person.primaryName || "Unknown";
  const getKnownFor = () => person.knownFor || person.primaryProfession || "";
  const getBirthYear = () => person.birthYear || "";
  const getBirthplace = () => person.birthplace || person.wd_birthplace || "";
  const getBiography = () => person.biography || person.tmdb_biography || "";

  return (
    <div className="space-y-8">
      {/* Back Button */}
      <Link
        to="/people"
        className="inline-flex items-center gap-2 text-[var(--text-muted)] hover:text-[var(--text)] transition-colors"
      >
        <ArrowLeft className="w-5 h-5" />
        Back to People
      </Link>

      {/* Person Header */}
      <div className="flex gap-8">
        {/* Profile Image */}
        <div className="flex-shrink-0 w-64">
          <img
            src={imageUrl}
            alt={getName()}
            className="w-full rounded-xl shadow-2xl"
            onError={e => {
              (e.target as HTMLImageElement).src = placeholderImage;
            }}
          />
        </div>

        {/* Info */}
        <div className="flex-1 space-y-6">
          <div>
            <h1 className="text-4xl font-bold text-[var(--text)] mb-2">
              {getName()}
            </h1>
            {getKnownFor() && (
              <p className="text-lg text-[var(--primary)] capitalize">
                {getKnownFor()}
              </p>
            )}
          </div>

          {/* Details */}
          <div className="flex flex-wrap gap-6">
            {getBirthYear() && (
              <div className="flex items-center gap-2 text-[var(--text-muted)]">
                <Calendar className="w-5 h-5" />
                <span>Born {getBirthYear()}</span>
              </div>
            )}
            {getBirthplace() && (
              <div className="flex items-center gap-2 text-[var(--text-muted)]">
                <MapPin className="w-5 h-5" />
                <span>{getBirthplace()}</span>
              </div>
            )}
            {person.movieCount && (
              <div className="flex items-center gap-2 text-[var(--text-muted)]">
                <Film className="w-5 h-5" />
                <span>{person.movieCount} films in your collection</span>
              </div>
            )}
            {person.avgRating && (
              <div className="flex items-center gap-2 text-yellow-400">
                <Star className="w-5 h-5" />
                <span>{person.avgRating.toFixed(1)} avg rating</span>
              </div>
            )}
          </div>

          {/* Biography */}
          {getBiography() && (
            <div className="space-y-2">
              <h2 className="text-lg font-semibold text-[var(--text)]">
                Biography
              </h2>
              <p className="text-[var(--text-muted)] leading-relaxed">
                {getBiography()}
              </p>
            </div>
          )}

          {/* External Links */}
          <div className="flex gap-4">
            {person.imdbId && (
              <a
                href={`https://www.imdb.com/name/${person.imdbId}`}
                target="_blank"
                rel="noopener noreferrer"
                className="px-6 py-3 bg-yellow-500 text-black font-bold rounded-lg hover:bg-yellow-400 transition-colors"
              >
                View on IMDb
              </a>
            )}
            {person.id && (
              <a
                href={`https://www.themoviedb.org/person/${person.id}`}
                target="_blank"
                rel="noopener noreferrer"
                className="px-6 py-3 bg-[var(--surface)] text-[var(--text)] font-medium rounded-lg hover:bg-[var(--surface-hover)] transition-colors"
              >
                View on TMDb
              </a>
            )}
          </div>
        </div>
      </div>

      {/* Filmography */}
      {filmography && filmography.length > 0 && (
        <section>
          <h2 className="text-2xl font-bold text-[var(--text)] mb-4 flex items-center gap-2">
            <Film className="w-6 h-6" />
            Filmography in Your Collection
          </h2>
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-4">
            {filmography.map((movie: Movie) => (
              <MovieCard
                key={movie.const || movie.id}
                movie={movie}
                onClick={() => navigate("/movies/" + (movie.const || movie.id))}
              />
            ))}
          </div>
        </section>
      )}
    </div>
  );
}
