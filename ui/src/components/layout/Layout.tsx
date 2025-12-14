import { Link, useLocation, Outlet } from "react-router-dom";
import {
  Film,
  User,
  Sparkles,
  BarChart3,
  Search,
  Home,
  Settings,
  Users,
} from "lucide-react";
import { clsx } from "clsx";

const navItems = [
  { path: "/", icon: Home, label: "Dashboard" },
  { path: "/movies", icon: Film, label: "Movies" },
  { path: "/people", icon: User, label: "People" },
  { path: "/recommendations", icon: Sparkles, label: "For You" },
  { path: "/insights", icon: BarChart3, label: "Insights" },
  { path: "/actor-analytics", icon: Users, label: "Actor Analytics" },
];

export function Sidebar() {
  const location = useLocation();

  return (
    <aside className="fixed left-0 top-0 bottom-0 w-64 bg-[var(--surface)] border-r border-[var(--border)] flex flex-col z-50">
      {/* Logo */}
      <div className="p-6 border-b border-[var(--border)]">
        <Link to="/" className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-lg bg-gradient-to-br from-[var(--primary)] to-[var(--secondary)] flex items-center justify-center">
            <Film className="w-6 h-6 text-white" />
          </div>
          <span className="text-xl font-bold gradient-text">CineScope</span>
        </Link>
      </div>

      {/* Navigation */}
      <nav className="flex-1 p-4 space-y-1">
        {navItems.map(item => {
          const isActive =
            location.pathname === item.path ||
            (item.path !== "/" && location.pathname.startsWith(item.path));

          return (
            <Link
              key={item.path}
              to={item.path}
              className={clsx(
                "flex items-center gap-3 px-4 py-3 rounded-lg transition-colors",
                isActive
                  ? "bg-[var(--primary)]/10 text-[var(--primary)]"
                  : "text-[var(--text-muted)] hover:bg-[var(--surface-hover)] hover:text-[var(--text)]"
              )}
            >
              <item.icon className="w-5 h-5" />
              <span className="font-medium">{item.label}</span>
            </Link>
          );
        })}
      </nav>

      {/* Footer */}
      <div className="p-4 border-t border-[var(--border)]">
        <div className="text-xs text-[var(--text-muted)] text-center">
          <p>Your personal movie database</p>
          <p className="mt-1">Powered by TMDB, OMDB, Wikidata</p>
        </div>
      </div>
    </aside>
  );
}

export function Header() {
  return (
    <header className="fixed top-0 left-64 right-0 h-16 bg-[var(--surface)]/80 backdrop-blur-md border-b border-[var(--border)] flex items-center justify-between px-6 z-40">
      {/* Search */}
      <div className="flex-1 max-w-xl">
        <Link
          to="/search"
          className="flex items-center gap-2 px-4 py-2 bg-[var(--background)] border border-[var(--border)] rounded-lg text-[var(--text-muted)] hover:border-[var(--primary)]/50 transition-colors"
        >
          <Search className="w-4 h-4" />
          <span className="text-sm">Search movies, actors, directors...</span>
          <kbd className="ml-auto px-2 py-0.5 bg-[var(--surface)] rounded text-xs">
            ⌘K
          </kbd>
        </Link>
      </div>

      {/* Right side */}
      <div className="flex items-center gap-4">
        <Link
          to="/settings"
          className="p-2 rounded-lg hover:bg-[var(--surface-hover)] text-[var(--text-muted)] hover:text-[var(--text)] transition-colors"
        >
          <Settings className="w-5 h-5" />
        </Link>
      </div>
    </header>
  );
}

export function Layout() {
  return (
    <div className="min-h-screen bg-[var(--background)]">
      <Sidebar />
      <Header />
      <main className="ml-64 pt-16 p-6">
        <Outlet />
      </main>
    </div>
  );
}
