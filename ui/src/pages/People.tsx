import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { Search, Users, Film, Star, Filter } from "lucide-react";
import { usePeople } from "../hooks/useMovies";
import type { Person } from "../types";

export function PeoplePage() {
  const navigate = useNavigate();
  const [searchQuery, setSearchQuery] = useState("");
  const [roleFilter, setRoleFilter] = useState<
    "all" | "director" | "actor" | "writer"
  >("all");
  const {
    data: peopleData,
    isLoading,
    error,
  } = usePeople({
    search: searchQuery,
    role: roleFilter,
  });

  // Get people from paginated response
  const people = peopleData?.results || [];
  const total = peopleData?.total || 0;

  if (error) {
    return (
      <div className="text-center py-20">
        <Users className="w-16 h-16 text-[var(--error)] mx-auto mb-4" />
        <h2 className="text-xl font-semibold text-[var(--text)] mb-2">
          Error loading people
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
          <Users className="w-8 h-8" />
          People
        </h1>
        <p className="text-[var(--text-muted)] mt-2">
          Directors, actors, and writers in your collection
        </p>
      </div>

      {/* Filters */}
      <div className="flex flex-col sm:flex-row gap-4">
        {/* Search */}
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-[var(--text-muted)]" />
          <input
            type="text"
            placeholder="Search people..."
            value={searchQuery}
            onChange={e => setSearchQuery(e.target.value)}
            className="w-full pl-10 pr-4 py-3 bg-[var(--surface)] border border-[var(--border)] rounded-xl 
                     text-[var(--text)] placeholder:text-[var(--text-muted)]
                     focus:outline-none focus:ring-2 focus:ring-[var(--primary)] focus:border-transparent"
          />
        </div>

        {/* Role Filter */}
        <div className="flex items-center gap-2">
          <Filter className="w-5 h-5 text-[var(--text-muted)]" />
          {(["all", "director", "actor", "writer"] as const).map(role => (
            <button
              key={role}
              onClick={() => setRoleFilter(role)}
              className={`px-4 py-2 rounded-lg font-medium capitalize transition-colors ${
                roleFilter === role
                  ? "bg-[var(--primary)] text-white"
                  : "bg-[var(--surface)] text-[var(--text-muted)] hover:text-[var(--text)]"
              }`}
            >
              {role}
            </button>
          ))}
        </div>
      </div>

      {/* Loading State */}
      {isLoading && (
        <div className="flex items-center justify-center py-20">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]" />
        </div>
      )}

      {/* People Grid */}
      {!isLoading && (
        <>
          <p className="text-sm text-[var(--text-muted)]">
            Showing {people.length} of {total} people
          </p>

          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-4">
            {people.map((person: Person) => (
              <PersonCard
                key={person.id}
                person={person}
                onClick={() => navigate(`/people/${person.id}`)}
              />
            ))}
          </div>

          {/* Empty State */}
          {people.length === 0 && !isLoading && (
            <div className="text-center py-20">
              <Users className="w-16 h-16 text-[var(--text-muted)] mx-auto mb-4" />
              <h2 className="text-xl font-semibold text-[var(--text)] mb-2">
                No people found
              </h2>
              <p className="text-[var(--text-muted)]">
                Try adjusting your search or filters.
              </p>
            </div>
          )}
        </>
      )}
    </div>
  );
}

// Person Card Component
function PersonCard({
  person,
  onClick,
}: {
  person: Person;
  onClick?: () => void;
}) {
  const placeholderImage =
    "data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjAwIiBoZWlnaHQ9IjMwMCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48cmVjdCB3aWR0aD0iMjAwIiBoZWlnaHQ9IjMwMCIgZmlsbD0iIzM3M2Y0NyIvPjx0ZXh0IHg9IjEwMCIgeT0iMTUwIiBmb250LWZhbWlseT0ic2Fucy1zZXJpZiIgZm9udC1zaXplPSI0MCIgZmlsbD0iIzZiN2Q4YSIgdGV4dC1hbmNob3I9Im1pZGRsZSI+8J+RpDwvdGV4dD48L3N2Zz4=";

  const imageUrl = person.profilePath
    ? `https://image.tmdb.org/t/p/w185${person.profilePath}`
    : placeholderImage;

  return (
    <div className="group cursor-pointer" onClick={onClick}>
      <div className="relative aspect-[2/3] rounded-xl overflow-hidden bg-[var(--surface)] mb-3">
        <img
          src={imageUrl}
          alt={person.name}
          className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-300"
          onError={e => {
            (e.target as HTMLImageElement).src = placeholderImage;
          }}
        />

        {/* Overlay on hover */}
        <div
          className="absolute inset-0 bg-gradient-to-t from-black/80 via-transparent to-transparent 
                      opacity-0 group-hover:opacity-100 transition-opacity duration-300"
        >
          <div className="absolute bottom-3 left-3 right-3">
            {person.movieCount && (
              <div className="flex items-center gap-1 text-xs text-white/80">
                <Film className="w-3 h-3" />
                {person.movieCount} films
              </div>
            )}
            {person.avgRating && (
              <div className="flex items-center gap-1 text-xs text-yellow-400 mt-1">
                <Star className="w-3 h-3" />
                {person.avgRating.toFixed(1)} avg
              </div>
            )}
          </div>
        </div>
      </div>

      <div className="space-y-1">
        <h3 className="font-medium text-[var(--text)] truncate group-hover:text-[var(--primary)] transition-colors">
          {person.name}
        </h3>
        {person.knownFor && (
          <p className="text-xs text-[var(--text-muted)] capitalize">
            {person.knownFor}
          </p>
        )}
      </div>
    </div>
  );
}
