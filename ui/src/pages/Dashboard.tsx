import { Film, Star, Clock, TrendingUp } from "lucide-react";
import { useStats, useRecommendations } from "../hooks/useMovies";
import { MovieCard } from "../components/movies/MovieCard";
import { useNavigate } from "react-router-dom";

function StatCard({
  icon: Icon,
  label,
  value,
  subtext,
  color = "primary",
}: {
  icon: React.ElementType;
  label: string;
  value: string | number;
  subtext?: string;
  color?: "primary" | "secondary" | "green" | "yellow";
}) {
  const colors = {
    primary: "from-violet-500/20 to-violet-500/5 border-violet-500/30",
    secondary: "from-teal-500/20 to-teal-500/5 border-teal-500/30",
    green: "from-green-500/20 to-green-500/5 border-green-500/30",
    yellow: "from-yellow-500/20 to-yellow-500/5 border-yellow-500/30",
  };

  return (
    <div className={`bg-gradient-to-br ${colors[color]} border rounded-xl p-5`}>
      <div className="flex items-center gap-3">
        <div className="p-2 rounded-lg bg-[var(--surface)]">
          <Icon className="w-5 h-5 text-[var(--text)]" />
        </div>
        <div>
          <p className="text-sm text-[var(--text-muted)]">{label}</p>
          <p className="text-2xl font-bold text-[var(--text)]">{value}</p>
          {subtext && (
            <p className="text-xs text-[var(--text-muted)]">{subtext}</p>
          )}
        </div>
      </div>
    </div>
  );
}

export function Dashboard() {
  const navigate = useNavigate();
  const { data: stats, isLoading } = useStats();
  const { data: recommendations } = useRecommendations();

  return (
    <div className="space-y-8">
      {/* Welcome Header */}
      <div>
        <h1 className="text-3xl font-bold gradient-text">
          Welcome to CineScope
        </h1>
        <p className="text-[var(--text-muted)] mt-2">
          Your personal movie database and recommendation engine
        </p>
      </div>

      {/* Stats Grid */}
      {isLoading ? (
        <div className="flex justify-center py-8">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]" />
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          <StatCard
            icon={Film}
            label="Total Movies"
            value={stats?.totalMovies?.toLocaleString() || "—"}
            subtext="in your collection"
            color="primary"
          />
          <StatCard
            icon={Star}
            label="Average Rating"
            value={stats?.avgRating?.toFixed(1) || "—"}
            subtext="IMDb average"
            color="yellow"
          />
          <StatCard
            icon={Clock}
            label="Watch Time"
            value={stats?.totalHours ? `${Math.round(stats.totalHours)}h` : "—"}
            subtext="total runtime"
            color="secondary"
          />
          <StatCard
            icon={TrendingUp}
            label="Top Year"
            value={stats?.topYear || "—"}
            subtext="most movies"
            color="green"
          />
        </div>
      )}

      {/* Recommendations Section */}
      {recommendations && recommendations.length > 0 && (
        <section className="space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-xl font-semibold text-[var(--text)]">
              Recommended For You
            </h2>
            <button
              onClick={() => navigate("/recommendations")}
              className="text-sm text-[var(--primary)] hover:underline"
            >
              See all
            </button>
          </div>

          {recommendations.map((section, idx) => (
            <div key={idx} className="space-y-3">
              <h3 className="text-lg font-medium text-[var(--text-muted)]">
                {section.title}
              </h3>
              <div className="flex gap-4 overflow-x-auto pb-4 scrollbar-thin">
                {section.recommendations.slice(0, 8).map(rec => (
                  <div key={rec.movie.const} className="flex-shrink-0">
                    <MovieCard
                      movie={rec.movie}
                      size="md"
                      onClick={() => navigate(`/movies/${rec.movie.const}`)}
                    />
                  </div>
                ))}
              </div>
            </div>
          ))}
        </section>
      )}

      {/* Quick Actions */}
      <section className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <button
          onClick={() => navigate("/movies")}
          className="p-4 bg-[var(--surface)] rounded-xl border border-[var(--border)] hover:border-[var(--primary)]/50 transition-colors text-left"
        >
          <Film className="w-6 h-6 text-[var(--primary)] mb-2" />
          <p className="font-medium text-[var(--text)]">Browse Movies</p>
          <p className="text-sm text-[var(--text-muted)]">
            Explore your collection
          </p>
        </button>

        <button
          onClick={() => navigate("/people")}
          className="p-4 bg-[var(--surface)] rounded-xl border border-[var(--border)] hover:border-[var(--secondary)]/50 transition-colors text-left"
        >
          <Star className="w-6 h-6 text-[var(--secondary)] mb-2" />
          <p className="font-medium text-[var(--text)]">People</p>
          <p className="text-sm text-[var(--text-muted)]">Directors & Actors</p>
        </button>

        <button
          onClick={() => navigate("/recommendations")}
          className="p-4 bg-[var(--surface)] rounded-xl border border-[var(--border)] hover:border-yellow-500/50 transition-colors text-left"
        >
          <TrendingUp className="w-6 h-6 text-yellow-500 mb-2" />
          <p className="font-medium text-[var(--text)]">For You</p>
          <p className="text-sm text-[var(--text-muted)]">Personalized picks</p>
        </button>

        <button
          onClick={() => navigate("/insights")}
          className="p-4 bg-[var(--surface)] rounded-xl border border-[var(--border)] hover:border-green-500/50 transition-colors text-left"
        >
          <Clock className="w-6 h-6 text-green-500 mb-2" />
          <p className="font-medium text-[var(--text)]">Insights</p>
          <p className="text-sm text-[var(--text-muted)]">Stats & Trends</p>
        </button>
      </section>
    </div>
  );
}
