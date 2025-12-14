import { BrowserRouter, Routes, Route } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Layout } from "./components/layout/Layout";
import { Dashboard } from "./pages/Dashboard";
import { MoviesPage } from "./pages/Movies";
import { MovieDetailPage } from "./pages/MovieDetail";
import { PeoplePage } from "./pages/People";
import { PersonDetailPage } from "./pages/PersonDetailEnhanced";
import { RecommendationsPage } from "./pages/Recommendations";
import { InsightsPage } from "./pages/Insights";
import { SettingsPage } from "./pages/Settings";
import { ActorAnalyticsPage } from "./pages/ActorAnalytics";
import "./index.css";

// Create a client
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5 * 60 * 1000, // 5 minutes
      refetchOnWindowFocus: false,
    },
  },
});

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Layout />}>
            <Route index element={<Dashboard />} />
            <Route path="movies" element={<MoviesPage />} />
            <Route path="movies/:id" element={<MovieDetailPage />} />
            <Route path="people" element={<PeoplePage />} />
            <Route path="people/:id" element={<PersonDetailPage />} />
            <Route path="actor-analytics" element={<ActorAnalyticsPage />} />
            <Route path="recommendations" element={<RecommendationsPage />} />
            <Route path="insights" element={<InsightsPage />} />
            <Route path="settings" element={<SettingsPage />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  );
}

export default App;
