import {
  BarChart3,
  PieChart,
  Calendar,
  Film,
  Star,
  Clock,
  Users,
  TrendingUp,
  Award,
  Clapperboard,
  ChevronRight,
} from "lucide-react";
import { Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import api from "../api";

interface InsightsData {
  summary: {
    totalMovies: number;
    totalHours: number;
    avgRating: number;
    yearSpan: string;
  };
  genres: Array<{
    name: string;
    count: number;
    avgRating: number;
    percentage: number;
  }>;
  decades: Array<{ decade: string; count: number; avgRating: number }>;
  directors: Array<{ name: string; count: number; avgRating: number }>;
  actors: Array<{ name: string; count: number; avgRating: number }>;
  ratings: {
    distribution: Array<{ rating: number; count: number }>;
    average: number;
    median: number;
  };
  patterns: Array<{
    type: string;
    title: string;
    value: string;
    detail: string;
  }>;
}

function useInsights() {
  return useQuery({
    queryKey: ["insights"],
    queryFn: async () => {
      const response = await api.get("/insights");
      return response.data as InsightsData;
    },
  });
}

// Simple bar chart component
function BarChart({
  data,
  title,
  maxBars = 10,
  color = "var(--primary)",
  showRating = false,
}: {
  data: Array<{ label: string; value: number; rating?: number }>;
  title: string;
  maxBars?: number;
  color?: string;
  showRating?: boolean;
}) {
  const displayData = data.slice(0, maxBars);
  const maxValue = Math.max(...displayData.map(d => d.value), 1);

  return (
    <div className="bg-[var(--surface)] rounded-xl p-6 border border-[var(--border)]">
      <h3 className="text-lg font-semibold text-[var(--text)] mb-4">{title}</h3>
      <div className="space-y-3">
        {displayData.map((item, index) => (
          <div key={index} className="space-y-1">
            <div className="flex justify-between text-sm">
              <span className="text-[var(--text-muted)] truncate max-w-[60%]">
                {item.label}
              </span>
              <div className="flex items-center gap-3">
                {showRating && item.rating && (
                  <span className="text-yellow-400 text-xs flex items-center gap-1">
                    <Star className="w-3 h-3" />
                    {item.rating}
                  </span>
                )}
                <span className="text-[var(--text)] font-medium">
                  {item.value}
                </span>
              </div>
            </div>
            <div className="h-2 bg-[var(--bg-dark)] rounded-full overflow-hidden">
              <div
                className="h-full rounded-full transition-all duration-500"
                style={{
                  width: `${(item.value / maxValue) * 100}%`,
                  backgroundColor: color,
                }}
              />
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// Pattern card component
function PatternCard({
  pattern,
}: {
  pattern: { type: string; title: string; value: string; detail: string };
}) {
  const iconMap: Record<string, React.ReactNode> = {
    decade: <Calendar className="w-6 h-6" />,
    genre: <Film className="w-6 h-6" />,
    quality: <Star className="w-6 h-6" />,
    director: <Clapperboard className="w-6 h-6" />,
    actor: <Users className="w-6 h-6" />,
    runtime: <Clock className="w-6 h-6" />,
  };

  const colorMap: Record<string, string> = {
    decade: "text-blue-400",
    genre: "text-purple-400",
    quality: "text-yellow-400",
    director: "text-teal-400",
    actor: "text-pink-400",
    runtime: "text-orange-400",
  };

  return (
    <div className="bg-[var(--surface)] rounded-xl p-5 border border-[var(--border)] hover:border-[var(--primary)]/50 transition-colors">
      <div
        className={`${colorMap[pattern.type] || "text-[var(--primary)]"} mb-3`}
      >
        {iconMap[pattern.type] || <TrendingUp className="w-6 h-6" />}
      </div>
      <p className="text-sm text-[var(--text-muted)]">{pattern.title}</p>
      <p className="text-xl font-bold text-[var(--text)] mt-1">
        {pattern.value}
      </p>
      <p className="text-xs text-[var(--text-muted)] mt-2">{pattern.detail}</p>
    </div>
  );
}

// Rating distribution chart
function RatingDistribution({
  data,
}: {
  data: Array<{ rating: number; count: number }>;
}) {
  const maxCount = Math.max(...data.map(d => d.count), 1);

  return (
    <div className="bg-[var(--surface)] rounded-xl p-6 border border-[var(--border)]">
      <h3 className="text-lg font-semibold text-[var(--text)] mb-4 flex items-center gap-2">
        <Star className="w-5 h-5 text-yellow-400" />
        Rating Distribution
      </h3>
      <div className="flex items-end gap-1 h-40">
        {Array.from({ length: 10 }, (_, i) => i + 1).map(rating => {
          const item = data.find(d => Math.round(d.rating) === rating);
          const count = item?.count || 0;
          const height = maxCount > 0 ? (count / maxCount) * 100 : 0;

          return (
            <div
              key={rating}
              className="flex-1 flex flex-col items-center gap-1"
            >
              <span className="text-xs text-[var(--text-muted)]">{count}</span>
              <div
                className="w-full bg-gradient-to-t from-[var(--primary)] to-[var(--secondary)] rounded-t transition-all duration-500"
                style={{
                  height: `${height}%`,
                  minHeight: count > 0 ? "4px" : "0",
                }}
              />
              <span className="text-xs text-[var(--text-muted)]">{rating}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

export function InsightsPage() {
  const { data: insights, isLoading, error } = useInsights();

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]" />
      </div>
    );
  }

  if (error || !insights) {
    return (
      <div className="text-center py-20">
        <BarChart3 className="w-16 h-16 text-[var(--error)] mx-auto mb-4" />
        <h2 className="text-xl font-semibold text-[var(--text)] mb-2">
          Error loading insights
        </h2>
        <p className="text-[var(--text-muted)]">
          Make sure the API server is running.
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-8">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold gradient-text flex items-center gap-3">
          <BarChart3 className="w-8 h-8" />
          Insights
        </h1>
        <p className="text-[var(--text-muted)] mt-2">
          Deep dive into your watching patterns and preferences
        </p>
      </div>

      {/* Summary Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="bg-[var(--surface)] rounded-xl p-5 border border-[var(--border)]">
          <Film className="w-6 h-6 text-[var(--primary)] mb-2" />
          <p className="text-2xl font-bold text-[var(--text)]">
            {insights.summary.totalMovies.toLocaleString()}
          </p>
          <p className="text-sm text-[var(--text-muted)]">Movies Watched</p>
        </div>
        <div className="bg-[var(--surface)] rounded-xl p-5 border border-[var(--border)]">
          <Clock className="w-6 h-6 text-[var(--secondary)] mb-2" />
          <p className="text-2xl font-bold text-[var(--text)]">
            {insights.summary.totalHours.toLocaleString()}
          </p>
          <p className="text-sm text-[var(--text-muted)]">Hours Watched</p>
        </div>
        <div className="bg-[var(--surface)] rounded-xl p-5 border border-[var(--border)]">
          <Star className="w-6 h-6 text-yellow-400 mb-2" />
          <p className="text-2xl font-bold text-[var(--text)]">
            {insights.summary.avgRating}
          </p>
          <p className="text-sm text-[var(--text-muted)]">Average Rating</p>
        </div>
        <div className="bg-[var(--surface)] rounded-xl p-5 border border-[var(--border)]">
          <Calendar className="w-6 h-6 text-blue-400 mb-2" />
          <p className="text-2xl font-bold text-[var(--text)]">
            {insights.summary.yearSpan}
          </p>
          <p className="text-sm text-[var(--text-muted)]">Year Span</p>
        </div>
      </div>

      {/* Key Patterns */}
      <section>
        <h2 className="text-xl font-semibold text-[var(--text)] mb-4 flex items-center gap-2">
          <TrendingUp className="w-5 h-5" />
          Key Patterns
        </h2>
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
          {insights.patterns.map((pattern, index) => (
            <PatternCard key={index} pattern={pattern} />
          ))}
        </div>
      </section>

      {/* Rating Distribution */}
      {insights.ratings.distribution.length > 0 && (
        <RatingDistribution data={insights.ratings.distribution} />
      )}

      {/* Charts Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Genres */}
        <BarChart
          data={insights.genres.map(g => ({
            label: g.name,
            value: g.count,
            rating: g.avgRating,
          }))}
          title="Top Genres"
          maxBars={10}
          color="var(--primary)"
          showRating
        />

        {/* Decades */}
        <BarChart
          data={insights.decades.map(d => ({
            label: d.decade,
            value: d.count,
            rating: d.avgRating,
          }))}
          title="Movies by Decade"
          maxBars={12}
          color="var(--secondary)"
          showRating
        />

        {/* Directors */}
        <BarChart
          data={insights.directors.map(d => ({
            label: d.name,
            value: d.count,
            rating: d.avgRating,
          }))}
          title="Most Watched Directors"
          maxBars={10}
          color="#8b5cf6"
          showRating
        />

        {/* Actors */}
        <BarChart
          data={insights.actors.map(a => ({
            label: a.name,
            value: a.count,
            rating: a.avgRating,
          }))}
          title="Most Watched Actors"
          maxBars={10}
          color="#ec4899"
          showRating
        />
      </div>

      {/* Actor Analytics Promo */}
      <Link to="/actor-analytics">
        <div className="bg-gradient-to-r from-[var(--primary)]/20 to-pink-500/20 rounded-xl p-6 border border-[var(--border)] hover:border-[var(--primary)] transition-all cursor-pointer group">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              <div className="p-3 rounded-xl bg-[var(--primary)]/20">
                <Users className="w-8 h-8 text-[var(--primary)]" />
              </div>
              <div>
                <h3 className="text-lg font-semibold text-[var(--text)] group-hover:text-[var(--primary)] transition-colors">
                  Deep Actor Analytics
                </h3>
                <p className="text-sm text-[var(--text-muted)]">
                  Explore career timelines, collaborations, genre distributions,
                  and more
                </p>
              </div>
            </div>
            <ChevronRight className="w-6 h-6 text-[var(--text-muted)] group-hover:text-[var(--primary)] group-hover:translate-x-1 transition-all" />
          </div>
        </div>
      </Link>

      {/* Genre Deep Dive */}
      <section className="bg-[var(--surface)] rounded-xl p-6 border border-[var(--border)]">
        <h3 className="text-lg font-semibold text-[var(--text)] mb-4 flex items-center gap-2">
          <PieChart className="w-5 h-5" />
          Genre Breakdown
        </h3>
        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-5 gap-4">
          {insights.genres.slice(0, 10).map((genre, index) => (
            <div
              key={index}
              className="text-center p-4 bg-[var(--bg-dark)] rounded-lg"
            >
              <p className="text-2xl font-bold text-[var(--text)]">
                {genre.count}
              </p>
              <p className="text-sm text-[var(--text-muted)] truncate">
                {genre.name}
              </p>
              <p className="text-xs text-[var(--text-muted)] mt-1">
                {genre.percentage}%
              </p>
              <div className="flex items-center justify-center gap-1 mt-2 text-yellow-400 text-xs">
                <Star className="w-3 h-3" />
                {genre.avgRating}
              </div>
            </div>
          ))}
        </div>
      </section>

      {/* Fun Facts */}
      <section className="bg-gradient-to-r from-[var(--primary)]/10 to-[var(--secondary)]/10 rounded-xl p-6 border border-[var(--border)]">
        <h3 className="text-lg font-semibold text-[var(--text)] mb-4 flex items-center gap-2">
          <Award className="w-5 h-5 text-yellow-400" />
          Fun Facts
        </h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="bg-[var(--surface)]/50 rounded-lg p-4">
            <p className="text-sm text-[var(--text-muted)]">
              If you watched non-stop...
            </p>
            <p className="text-lg font-semibold text-[var(--text)]">
              {Math.round(insights.summary.totalHours / 24)} days of movies
            </p>
          </div>
          <div className="bg-[var(--surface)]/50 rounded-lg p-4">
            <p className="text-sm text-[var(--text-muted)]">
              Average movie per week
            </p>
            <p className="text-lg font-semibold text-[var(--text)]">
              ~{Math.round(insights.summary.totalMovies / 52)} movies/week (over
              a year)
            </p>
          </div>
          <div className="bg-[var(--surface)]/50 rounded-lg p-4">
            <p className="text-sm text-[var(--text-muted)]">Rating stats</p>
            <p className="text-lg font-semibold text-[var(--text)]">
              Median: {insights.ratings.median} • Avg:{" "}
              {insights.ratings.average}
            </p>
          </div>
        </div>
      </section>
    </div>
  );
}
