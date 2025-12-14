import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import {
  Users,
  Film,
  Star,
  TrendingUp,
  Award,
  BarChart3,
  Search,
  ChevronRight,
  Clapperboard,
} from "lucide-react";
import { useActorsAnalytics } from "../hooks/useMovies";
import { AnimatedStat, GlassCard } from "../components/ui/StatCard";
import { CardSkeleton, TableSkeleton } from "../components/ui/Skeleton";
import { BarChart } from "../components/charts";
import type { ActorStats } from "../types";

export function ActorAnalyticsPage() {
  const navigate = useNavigate();
  const { data, isLoading, error } = useActorsAnalytics();
  const [searchQuery, setSearchQuery] = useState("");
  const [sortBy, setSortBy] = useState<"filmCount" | "avgRating">("filmCount");

  if (isLoading) {
    return (
      <div className="space-y-8">
        <div>
          <h1 className="text-3xl font-bold gradient-text flex items-center gap-3">
            <Users className="w-8 h-8" />
            Actor Analytics
          </h1>
        </div>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <CardSkeleton key={i} />
          ))}
        </div>
        <TableSkeleton rows={10} />
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="text-center py-20">
        <Users className="w-16 h-16 text-red-500 mx-auto mb-4" />
        <h2 className="text-xl font-semibold text-[var(--text)] mb-2">
          Error loading analytics
        </h2>
        <p className="text-[var(--text-muted)]">
          Make sure the API server is running.
        </p>
      </div>
    );
  }

  const { actors, summary } = data;

  // Filter and sort actors
  const filteredActors = actors
    .filter((a: ActorStats) =>
      a.name.toLowerCase().includes(searchQuery.toLowerCase())
    )
    .sort((a: ActorStats, b: ActorStats) => {
      if (sortBy === "filmCount") return b.filmCount - a.filmCount;
      return (b.avgRating || 0) - (a.avgRating || 0);
    });

  const topActorsByRating = [...actors]
    .filter((a: ActorStats) => a.avgRating && a.filmCount >= 3)
    .sort((a, b) => (b.avgRating || 0) - (a.avgRating || 0))
    .slice(0, 10);

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold gradient-text flex items-center gap-3">
            <Users className="w-8 h-8" />
            Actor Analytics
          </h1>
          <p className="text-[var(--text-muted)] mt-2">
            Deep insights into the actors in your movie collection
          </p>
        </div>
        <Link
          to="/insights"
          className="inline-flex items-center gap-2 text-[var(--primary)] hover:underline"
        >
          <BarChart3 className="w-4 h-4" />
          Back to Insights
        </Link>
      </div>

      {/* Summary Stats */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
        <AnimatedStat
          value={summary.totalActors}
          label="Unique Actors"
          icon={<Users className="w-5 h-5" />}
          color="var(--primary)"
        />
        <AnimatedStat
          value={summary.actorsWithMultipleFilms}
          label="Recurring Actors"
          icon={<Film className="w-5 h-5" />}
          color="var(--secondary)"
          suffix="+"
        />
        <AnimatedStat
          value={summary.avgFilmsPerActor}
          label="Avg Films/Actor"
          icon={<Clapperboard className="w-5 h-5" />}
          decimals={1}
          color="#f59e0b"
        />
        <AnimatedStat
          value={summary.avgRatingAcrossActors}
          label="Avg Rating"
          icon={<Star className="w-5 h-5" />}
          decimals={1}
          color="#eab308"
        />
        <AnimatedStat
          value={summary.topActorFilmCount}
          label="Most Films"
          icon={<Award className="w-5 h-5" />}
          color="#ec4899"
        />
      </div>

      {/* Charts Row */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Most Prolific Actors */}
        <GlassCard className="p-6">
          <BarChart
            data={actors.slice(0, 10).map(a => ({
              label: a.name,
              value: a.filmCount,
              rating: a.avgRating || undefined,
            }))}
            title="Most Prolific Actors"
            color="var(--primary)"
            showRating
          />
        </GlassCard>

        {/* Highest Rated Actors */}
        <GlassCard className="p-6">
          <BarChart
            data={topActorsByRating.map(a => ({
              label: a.name,
              value: a.filmCount,
              rating: a.avgRating || undefined,
            }))}
            title="Highest Rated Actors (3+ films)"
            color="#eab308"
            showRating
          />
        </GlassCard>
      </div>

      {/* Actors Table */}
      <GlassCard className="p-6">
        <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4 mb-6">
          <h3 className="text-lg font-semibold text-[var(--text)] flex items-center gap-2">
            <TrendingUp className="w-5 h-5" />
            All Actors
          </h3>

          <div className="flex items-center gap-4">
            {/* Search */}
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-[var(--text-muted)]" />
              <input
                type="text"
                placeholder="Search actors..."
                value={searchQuery}
                onChange={e => setSearchQuery(e.target.value)}
                className="pl-9 pr-4 py-2 bg-[var(--surface-hover)] rounded-lg text-sm text-[var(--text)] placeholder:text-[var(--text-muted)] border border-[var(--border)] focus:border-[var(--primary)] outline-none w-64"
              />
            </div>

            {/* Sort */}
            <select
              value={sortBy}
              onChange={e =>
                setSortBy(e.target.value as "filmCount" | "avgRating")
              }
              className="px-4 py-2 bg-[var(--surface-hover)] rounded-lg text-sm text-[var(--text)] border border-[var(--border)] focus:border-[var(--primary)] outline-none"
            >
              <option value="filmCount">Sort by Films</option>
              <option value="avgRating">Sort by Rating</option>
            </select>
          </div>
        </div>

        {/* Table */}
        <div className="space-y-2">
          {filteredActors
            .slice(0, 25)
            .map((actor: ActorStats, index: number) => {
              const placeholderImage =
                "data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNDAiIGhlaWdodD0iNDAiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyI+PHJlY3Qgd2lkdGg9IjQwIiBoZWlnaHQ9IjQwIiBmaWxsPSIjMzczZjQ3Ii8+PHRleHQgeD0iMjAiIHk9IjI1IiBmb250LWZhbWlseT0ic2Fucy1zZXJpZiIgZm9udC1zaXplPSIxNiIgZmlsbD0iIzZiN2Q4YSIgdGV4dC1hbmNob3I9Im1pZGRsZSI+8J+RpDwvdGV4dD48L3N2Zz4=";
              const imageUrl = actor.profilePath
                ? `https://image.tmdb.org/t/p/w92${actor.profilePath}`
                : placeholderImage;

              return (
                <div
                  key={index}
                  onClick={() => actor.id && navigate(`/people/${actor.id}`)}
                  className={`flex items-center gap-4 p-3 rounded-lg bg-[var(--surface)] hover:bg-[var(--surface-hover)] transition-colors ${
                    actor.id ? "cursor-pointer" : ""
                  }`}
                >
                  {/* Rank */}
                  <span className="w-8 text-center text-sm font-bold text-[var(--text-muted)]">
                    #{index + 1}
                  </span>

                  {/* Photo */}
                  <img
                    src={imageUrl}
                    alt={actor.name}
                    className="w-10 h-10 rounded-full object-cover"
                    onError={e => {
                      (e.target as HTMLImageElement).src = placeholderImage;
                    }}
                  />

                  {/* Name & Genre */}
                  <div className="flex-1 min-w-0">
                    <p className="font-medium text-[var(--text)] truncate">
                      {actor.name}
                    </p>
                    <p className="text-xs text-[var(--text-muted)]">
                      {actor.topGenre && (
                        <span className="text-[var(--primary)]">
                          {actor.topGenre}
                        </span>
                      )}
                      {actor.careerSpan && (
                        <span className="ml-2">• {actor.careerSpan}</span>
                      )}
                    </p>
                  </div>

                  {/* Stats */}
                  <div className="flex items-center gap-6">
                    <div className="text-center">
                      <p className="text-lg font-bold text-[var(--text)]">
                        {actor.filmCount}
                      </p>
                      <p className="text-xs text-[var(--text-muted)]">films</p>
                    </div>

                    {actor.avgRating && (
                      <div className="text-center">
                        <p className="text-lg font-bold text-yellow-400 flex items-center gap-1">
                          <Star className="w-4 h-4 fill-current" />
                          {actor.avgRating.toFixed(1)}
                        </p>
                        <p className="text-xs text-[var(--text-muted)]">avg</p>
                      </div>
                    )}

                    {actor.id && (
                      <ChevronRight className="w-5 h-5 text-[var(--text-muted)]" />
                    )}
                  </div>
                </div>
              );
            })}
        </div>

        {filteredActors.length > 25 && (
          <p className="text-center text-sm text-[var(--text-muted)] mt-4">
            Showing 25 of {filteredActors.length} actors
          </p>
        )}
      </GlassCard>

      {/* Fun Facts Section */}
      <GlassCard className="p-6 bg-gradient-to-r from-[var(--primary)]/10 to-[var(--secondary)]/10">
        <h3 className="text-lg font-semibold text-[var(--text)] mb-4 flex items-center gap-2">
          <Award className="w-5 h-5 text-yellow-400" />
          Actor Insights
        </h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="bg-[var(--surface)]/50 rounded-lg p-4">
            <p className="text-sm text-[var(--text-muted)]">Your #1 Actor</p>
            <p className="text-lg font-semibold text-[var(--text)]">
              {actors[0]?.name || "N/A"}
            </p>
            <p className="text-xs text-[var(--text-muted)]">
              {actors[0]?.filmCount} films in your collection
            </p>
          </div>
          <div className="bg-[var(--surface)]/50 rounded-lg p-4">
            <p className="text-sm text-[var(--text-muted)]">
              Highest Rated (3+ films)
            </p>
            <p className="text-lg font-semibold text-[var(--text)]">
              {topActorsByRating[0]?.name || "N/A"}
            </p>
            <p className="text-xs text-[var(--text-muted)]">
              ★ {topActorsByRating[0]?.avgRating?.toFixed(1)} average across{" "}
              {topActorsByRating[0]?.filmCount} films
            </p>
          </div>
          <div className="bg-[var(--surface)]/50 rounded-lg p-4">
            <p className="text-sm text-[var(--text-muted)]">Actor Diversity</p>
            <p className="text-lg font-semibold text-[var(--text)]">
              {Math.round(
                (summary.actorsWithMultipleFilms / summary.totalActors) * 100
              )}
              % Return Rate
            </p>
            <p className="text-xs text-[var(--text-muted)]">
              Actors appearing in 2+ of your films
            </p>
          </div>
        </div>
      </GlassCard>
    </div>
  );
}
