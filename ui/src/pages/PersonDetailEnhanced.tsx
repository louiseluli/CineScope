import { useParams, Link, useNavigate } from "react-router-dom";
import {
  ArrowLeft,
  Film,
  Star,
  Calendar,
  MapPin,
  User,
  Clock,
  Award,
  TrendingUp,
  Users,
  ExternalLink,
  BarChart3,
} from "lucide-react";
import { usePersonAnalytics } from "../hooks/useMovies";
import { AnimatedStat, GlassCard } from "../components/ui/StatCard";
import { DonutChart, LineChart, BarChart } from "../components/charts";
import { FullPageLoader } from "../components/ui/Skeleton";

export function PersonDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { data, isLoading, error } = usePersonAnalytics(id || "");

  const placeholderImage =
    "data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjAwIiBoZWlnaHQ9IjMwMCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48cmVjdCB3aWR0aD0iMjAwIiBoZWlnaHQ9IjMwMCIgZmlsbD0iIzM3M2Y0NyIvPjx0ZXh0IHg9IjEwMCIgeT0iMTUwIiBmb250LWZhbWlseT0ic2Fucy1zZXJpZiIgZm9udC1zaXplPSI0MCIgZmlsbD0iIzZiN2Q4YSIgdGV4dC1hbmNob3I9Im1pZGRsZSI+8J+RpDwvdGV4dD48L3N2Zz4=";

  if (isLoading) {
    return <FullPageLoader />;
  }

  if (error || !data) {
    return (
      <div className="text-center py-20">
        <User className="w-16 h-16 text-red-500 mx-auto mb-4" />
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

  const { person, filmCount, analytics, films } = data;

  const imageUrl = person.profilePath
    ? `https://image.tmdb.org/t/p/w300${person.profilePath}`
    : placeholderImage;

  // Prepare chart data
  const genreChartData =
    analytics.genreDistribution?.map((g: { genre: string; count: number }) => ({
      label: g.genre,
      value: g.count,
    })) || [];

  const timelineData =
    analytics.careerTimeline?.map(
      (t: { year: number; count: number; avgRating: number | null }) => ({
        x: t.year,
        y: t.count,
        secondary: t.avgRating || undefined,
        label: String(t.year),
      })
    ) || [];

  const decadeBarData =
    analytics.decadeDistribution?.map(
      (d: { decade: string; count: number; avgRating: number | null }) => ({
        label: d.decade,
        value: d.count,
        rating: d.avgRating || undefined,
      })
    ) || [];

  return (
    <div className="space-y-8">
      {/* Back Button */}
      <div className="flex items-center justify-between">
        <Link
          to="/people"
          className="inline-flex items-center gap-2 text-[var(--text-muted)] hover:text-[var(--text)] transition-colors"
        >
          <ArrowLeft className="w-5 h-5" />
          Back to People
        </Link>
        <Link
          to="/actor-analytics"
          className="inline-flex items-center gap-2 text-[var(--primary)] hover:underline text-sm"
        >
          <BarChart3 className="w-4 h-4" />
          All Actor Analytics
        </Link>
      </div>

      {/* Person Header */}
      <div className="flex flex-col md:flex-row gap-8">
        {/* Profile Image */}
        <div className="flex-shrink-0 w-48 md:w-64">
          <img
            src={imageUrl}
            alt={person.name}
            className="w-full rounded-xl shadow-2xl"
            onError={e => {
              (e.target as HTMLImageElement).src = placeholderImage;
            }}
          />

          {/* External Links */}
          <div className="flex gap-2 mt-4">
            {person.imdbId && (
              <a
                href={`https://www.imdb.com/name/${person.imdbId}`}
                target="_blank"
                rel="noopener noreferrer"
                className="flex-1 px-4 py-2 bg-yellow-500 text-black font-bold rounded-lg hover:bg-yellow-400 transition-colors text-center text-sm flex items-center justify-center gap-1"
              >
                IMDb <ExternalLink className="w-3 h-3" />
              </a>
            )}
            {person.id && (
              <a
                href={`https://www.themoviedb.org/person/${person.id}`}
                target="_blank"
                rel="noopener noreferrer"
                className="flex-1 px-4 py-2 bg-[var(--surface)] text-[var(--text)] font-medium rounded-lg hover:bg-[var(--surface-hover)] transition-colors text-center text-sm flex items-center justify-center gap-1"
              >
                TMDb <ExternalLink className="w-3 h-3" />
              </a>
            )}
          </div>
        </div>

        {/* Info */}
        <div className="flex-1 space-y-6">
          <div>
            <h1 className="text-4xl font-bold text-[var(--text)] mb-2">
              {person.name}
            </h1>
            {person.knownFor && (
              <p className="text-lg text-[var(--primary)] capitalize">
                {person.knownFor}
              </p>
            )}
          </div>

          {/* Quick Stats Cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <AnimatedStat
              value={filmCount}
              label="Films in Collection"
              icon={<Film className="w-4 h-4" />}
              size="sm"
            />
            {analytics.avgRating && (
              <AnimatedStat
                value={analytics.avgRating}
                label="Average Rating"
                icon={<Star className="w-4 h-4" />}
                decimals={1}
                size="sm"
                color="#eab308"
              />
            )}
            {analytics.careerSpan && (
              <AnimatedStat
                value={analytics.careerSpan.years}
                label="Career Years"
                suffix=" yrs"
                icon={<Calendar className="w-4 h-4" />}
                size="sm"
                color="var(--secondary)"
              />
            )}
            {analytics.totalRuntime > 0 && (
              <AnimatedStat
                value={Math.round(analytics.totalRuntime / 60)}
                label="Total Hours"
                suffix="h"
                icon={<Clock className="w-4 h-4" />}
                size="sm"
                color="#f59e0b"
              />
            )}
          </div>

          {/* Details */}
          <div className="flex flex-wrap gap-4 text-sm">
            {person.birthYear && (
              <div className="flex items-center gap-2 text-[var(--text-muted)] bg-[var(--surface)] px-3 py-1.5 rounded-full">
                <Calendar className="w-4 h-4" />
                <span>Born {person.birthYear}</span>
              </div>
            )}
            {person.birthplace && (
              <div className="flex items-center gap-2 text-[var(--text-muted)] bg-[var(--surface)] px-3 py-1.5 rounded-full">
                <MapPin className="w-4 h-4" />
                <span>{person.birthplace}</span>
              </div>
            )}
            {person.awardsCount && person.awardsCount > 0 && (
              <div className="flex items-center gap-2 text-yellow-400 bg-[var(--surface)] px-3 py-1.5 rounded-full">
                <Award className="w-4 h-4" />
                <span>{person.awardsCount} Awards</span>
              </div>
            )}
          </div>

          {/* Biography */}
          {person.biography && (
            <div className="space-y-2">
              <h2 className="text-lg font-semibold text-[var(--text)]">
                Biography
              </h2>
              <p className="text-[var(--text-muted)] leading-relaxed text-sm line-clamp-6">
                {person.biography}
              </p>
            </div>
          )}
        </div>
      </div>

      {/* Analytics Section */}
      {filmCount > 0 && (
        <section className="space-y-6">
          <h2 className="text-2xl font-bold text-[var(--text)] flex items-center gap-2">
            <TrendingUp className="w-6 h-6 text-[var(--primary)]" />
            Career Analytics
          </h2>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {/* Career Timeline */}
            {timelineData.length > 1 && (
              <GlassCard className="p-6">
                <LineChart
                  data={timelineData}
                  title="Career Timeline"
                  showSecondary={true}
                  height={220}
                  formatY={v => String(Math.round(v))}
                />
              </GlassCard>
            )}

            {/* Genre Distribution */}
            {genreChartData.length > 0 && (
              <GlassCard className="p-6">
                <DonutChart
                  data={genreChartData}
                  title="Genre Distribution"
                  centerValue={genreChartData.length}
                  centerLabel="genres"
                  size={180}
                />
              </GlassCard>
            )}

            {/* Decade Distribution */}
            {decadeBarData.length > 0 && (
              <GlassCard className="p-6">
                <BarChart
                  data={decadeBarData}
                  title="Films by Decade"
                  color="var(--secondary)"
                  showRating
                />
              </GlassCard>
            )}

            {/* Top Collaborations */}
            {analytics.topCollaborations &&
              analytics.topCollaborations.length > 0 && (
                <GlassCard className="p-6">
                  <h3 className="text-lg font-semibold text-[var(--text)] mb-4 flex items-center gap-2">
                    <Users className="w-5 h-5" />
                    Top Collaborations
                  </h3>
                  <div className="space-y-3">
                    {analytics.topCollaborations
                      .slice(0, 8)
                      .map(
                        (
                          collab: { name: string; count: number },
                          i: number
                        ) => (
                          <div
                            key={i}
                            className="flex items-center justify-between"
                          >
                            <span className="text-[var(--text-muted)] truncate">
                              {collab.name}
                            </span>
                            <span className="text-[var(--text)] font-medium">
                              {collab.count} films
                            </span>
                          </div>
                        )
                      )}
                  </div>
                </GlassCard>
              )}
          </div>

          {/* Best & Worst Films */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {/* Best Films */}
            {analytics.bestFilms && analytics.bestFilms.length > 0 && (
              <GlassCard className="p-6">
                <h3 className="text-lg font-semibold text-[var(--text)] mb-4 flex items-center gap-2">
                  <Star className="w-5 h-5 text-yellow-400" />
                  Best Rated Films
                </h3>
                <div className="space-y-2">
                  {analytics.bestFilms.map(
                    (
                      film: {
                        title: string;
                        year: number;
                        rating: number;
                        const: string;
                      },
                      i: number
                    ) => (
                      <div
                        key={i}
                        className="flex items-center justify-between p-2 rounded-lg hover:bg-[var(--surface-hover)] cursor-pointer transition-colors"
                        onClick={() => navigate(`/movies/${film.const}`)}
                      >
                        <div className="flex-1 min-w-0">
                          <p className="text-[var(--text)] truncate">
                            {film.title}
                          </p>
                          <p className="text-xs text-[var(--text-muted)]">
                            {film.year}
                          </p>
                        </div>
                        <span className="text-yellow-400 font-bold flex items-center gap-1">
                          <Star className="w-4 h-4 fill-current" />
                          {film.rating.toFixed(1)}
                        </span>
                      </div>
                    )
                  )}
                </div>
              </GlassCard>
            )}

            {/* Worst Films */}
            {analytics.worstFilms && analytics.worstFilms.length > 0 && (
              <GlassCard className="p-6">
                <h3 className="text-lg font-semibold text-[var(--text)] mb-4 flex items-center gap-2">
                  <TrendingUp className="w-5 h-5 text-red-400 rotate-180" />
                  Lower Rated Films
                </h3>
                <div className="space-y-2">
                  {analytics.worstFilms.map(
                    (
                      film: {
                        title: string;
                        year: number;
                        rating: number;
                        const: string;
                      },
                      i: number
                    ) => (
                      <div
                        key={i}
                        className="flex items-center justify-between p-2 rounded-lg hover:bg-[var(--surface-hover)] cursor-pointer transition-colors"
                        onClick={() => navigate(`/movies/${film.const}`)}
                      >
                        <div className="flex-1 min-w-0">
                          <p className="text-[var(--text)] truncate">
                            {film.title}
                          </p>
                          <p className="text-xs text-[var(--text-muted)]">
                            {film.year}
                          </p>
                        </div>
                        <span className="text-orange-400 font-bold flex items-center gap-1">
                          <Star className="w-4 h-4" />
                          {film.rating.toFixed(1)}
                        </span>
                      </div>
                    )
                  )}
                </div>
              </GlassCard>
            )}
          </div>
        </section>
      )}

      {/* Filmography */}
      {films && films.length > 0 && (
        <section>
          <h2 className="text-2xl font-bold text-[var(--text)] mb-4 flex items-center gap-2">
            <Film className="w-6 h-6" />
            Filmography in Your Collection ({films.length})
          </h2>
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-4">
            {films.map(
              (
                film: {
                  title: string;
                  year: number;
                  rating: number | null;
                  poster?: string;
                  role: string;
                  const: string;
                },
                index: number
              ) => (
                <div
                  key={index}
                  onClick={() => navigate(`/movies/${film.const}`)}
                  className="cursor-pointer group"
                >
                  <div className="bg-[var(--surface)] rounded-lg overflow-hidden border border-[var(--border)] hover:border-[var(--primary)]/50 transition-all card-hover">
                    <div className="aspect-[2/3] bg-[var(--surface-hover)] relative">
                      {film.poster ? (
                        <img
                          src={`https://image.tmdb.org/t/p/w200${film.poster}`}
                          alt={film.title}
                          className="w-full h-full object-cover"
                        />
                      ) : (
                        <div className="w-full h-full flex items-center justify-center text-[var(--text-muted)]">
                          <Film className="w-8 h-8" />
                        </div>
                      )}
                      {film.rating && (
                        <div className="absolute top-2 right-2 bg-black/70 px-2 py-1 rounded text-xs text-yellow-400 flex items-center gap-1">
                          <Star className="w-3 h-3 fill-current" />
                          {film.rating.toFixed(1)}
                        </div>
                      )}
                      <div className="absolute bottom-2 left-2 bg-[var(--primary)]/90 px-2 py-0.5 rounded text-xs text-white capitalize">
                        {film.role}
                      </div>
                    </div>
                    <div className="p-3">
                      <p className="text-sm font-medium text-[var(--text)] truncate group-hover:text-[var(--primary)] transition-colors">
                        {film.title}
                      </p>
                      <p className="text-xs text-[var(--text-muted)]">
                        {film.year}
                      </p>
                    </div>
                  </div>
                </div>
              )
            )}
          </div>
        </section>
      )}
    </div>
  );
}
