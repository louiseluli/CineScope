import { useNavigate } from "react-router-dom";
import { Sparkles, Compass, Film } from "lucide-react";
import { useRecommendations, useGapRecommendations } from "../hooks/useMovies";
import { MovieCard } from "../components/movies/MovieCard";

export function RecommendationsPage() {
  const navigate = useNavigate();
  const { data: recommendations, isLoading: recsLoading } =
    useRecommendations();
  const { data: gaps, isLoading: gapsLoading } = useGapRecommendations();

  if (recsLoading || gapsLoading) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]" />
      </div>
    );
  }

  return (
    <div className="space-y-10">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold gradient-text flex items-center gap-3">
          <Sparkles className="w-8 h-8" />
          For You
        </h1>
        <p className="text-[var(--text-muted)] mt-2">
          Personalized recommendations based on your watching patterns
        </p>
      </div>

      {/* Recommendation Sections */}
      {recommendations?.map((section, index) => (
        <section key={index} className="space-y-4">
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-xl font-semibold text-[var(--text)]">
                {section.title}
              </h2>
              <p className="text-sm text-[var(--text-muted)]">
                {section.description}
              </p>
            </div>
          </div>

          <div className="flex gap-4 overflow-x-auto pb-4 scrollbar-thin">
            {section.recommendations.map(rec => (
              <div key={rec.movie.const} className="flex-shrink-0">
                <MovieCard
                  movie={rec.movie}
                  size="md"
                  onClick={() => navigate(`/movies/${rec.movie.const}`)}
                />
                {rec.reason && (
                  <p className="text-xs text-[var(--text-muted)] mt-2 max-w-[176px] line-clamp-2">
                    {rec.reason}
                  </p>
                )}
              </div>
            ))}
          </div>
        </section>
      ))}

      {/* Collection Gaps */}
      {gaps && gaps.length > 0 && (
        <section className="space-y-4">
          <div className="flex items-center gap-3">
            <Compass className="w-6 h-6 text-[var(--secondary)]" />
            <div>
              <h2 className="text-xl font-semibold text-[var(--text)]">
                Expand Your Horizons
              </h2>
              <p className="text-sm text-[var(--text-muted)]">
                Areas of cinema you haven't explored yet
              </p>
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {gaps.map((gap, index) => (
              <div
                key={index}
                className="bg-[var(--surface)] rounded-xl p-5 border border-[var(--border)] hover:border-[var(--secondary)]/50 transition-colors"
              >
                <h3 className="font-semibold text-[var(--text)] mb-2">
                  {gap.title}
                </h3>
                <p className="text-sm text-[var(--text-muted)] mb-4">
                  {gap.description}
                </p>

                <div className="flex gap-3 overflow-x-auto pb-2">
                  {gap.recommendations.slice(0, 4).map(rec => (
                    <MovieCard
                      key={rec.movie.const}
                      movie={rec.movie}
                      size="sm"
                      onClick={() => navigate(`/movies/${rec.movie.const}`)}
                    />
                  ))}
                </div>
              </div>
            ))}
          </div>
        </section>
      )}

      {/* Empty State */}
      {(!recommendations || recommendations.length === 0) &&
        (!gaps || gaps.length === 0) && (
          <div className="text-center py-20">
            <Film className="w-16 h-16 text-[var(--text-muted)] mx-auto mb-4" />
            <h2 className="text-xl font-semibold text-[var(--text)] mb-2">
              No recommendations yet
            </h2>
            <p className="text-[var(--text-muted)] mb-6">
              We need more data to generate personalized recommendations.
              <br />
              Make sure the API server is running.
            </p>
            <button
              onClick={() => navigate("/movies")}
              className="px-6 py-3 bg-[var(--primary)] text-white rounded-lg font-medium hover:bg-[var(--primary-dark)] transition-colors"
            >
              Browse Movies
            </button>
          </div>
        )}
    </div>
  );
}
