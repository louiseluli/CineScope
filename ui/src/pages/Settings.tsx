import { useState } from "react";
import {
  Settings,
  Bell,
  Palette,
  Database,
  RefreshCw,
  Server,
  Key,
  Save,
  CheckCircle,
  AlertCircle,
  Trash2,
  Download,
  Upload,
} from "lucide-react";

interface SettingSection {
  title: string;
  description: string;
  icon: React.ReactNode;
  children: React.ReactNode;
}

function SettingCard({ title, description, icon, children }: SettingSection) {
  return (
    <div className="bg-[var(--surface)] rounded-xl border border-[var(--border)]">
      <div className="p-5 border-b border-[var(--border)]">
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-lg bg-[var(--primary)]/20">{icon}</div>
          <div>
            <h3 className="font-semibold text-[var(--text)]">{title}</h3>
            <p className="text-sm text-[var(--text-muted)]">{description}</p>
          </div>
        </div>
      </div>
      <div className="p-5 space-y-4">{children}</div>
    </div>
  );
}

function Toggle({
  checked,
  onChange,
  label,
}: {
  checked: boolean;
  onChange: (checked: boolean) => void;
  label: string;
}) {
  return (
    <label className="flex items-center justify-between cursor-pointer">
      <span className="text-[var(--text)]">{label}</span>
      <button
        onClick={() => onChange(!checked)}
        className={`relative w-12 h-6 rounded-full transition-colors ${
          checked ? "bg-[var(--primary)]" : "bg-[var(--border)]"
        }`}
      >
        <span
          className={`absolute top-1 w-4 h-4 bg-white rounded-full transition-transform ${
            checked ? "translate-x-7" : "translate-x-1"
          }`}
        />
      </button>
    </label>
  );
}

export function SettingsPage() {
  const [apiUrl, setApiUrl] = useState("http://localhost:5001");
  const [tmdbKey, setTmdbKey] = useState("");
  const [omdbKey, setOmdbKey] = useState("");
  const [darkMode, setDarkMode] = useState(true);
  const [notifications, setNotifications] = useState(true);
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [saveStatus, setSaveStatus] = useState<
    "idle" | "saving" | "saved" | "error"
  >("idle");

  const handleSave = () => {
    setSaveStatus("saving");
    // Simulate save
    setTimeout(() => {
      setSaveStatus("saved");
      setTimeout(() => setSaveStatus("idle"), 2000);
    }, 1000);
  };

  const handleTestConnection = async () => {
    try {
      const response = await fetch(`${apiUrl}/api/stats`);
      if (response.ok) {
        alert("✅ Connection successful!");
      } else {
        alert("❌ Connection failed: Server returned an error");
      }
    } catch (error) {
      alert("❌ Connection failed: Unable to reach server");
    }
  };

  return (
    <div className="space-y-8 max-w-4xl">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold gradient-text flex items-center gap-3">
          <Settings className="w-8 h-8" />
          Settings
        </h1>
        <p className="text-[var(--text-muted)] mt-2">
          Configure your CineScope experience
        </p>
      </div>

      {/* Settings Grid */}
      <div className="space-y-6">
        {/* API Configuration */}
        <SettingCard
          title="API Configuration"
          description="Configure your backend server connection"
          icon={<Server className="w-5 h-5 text-[var(--primary)]" />}
        >
          <div className="space-y-3">
            <label className="block">
              <span className="text-sm text-[var(--text-muted)] mb-1 block">
                API URL
              </span>
              <div className="flex gap-2">
                <input
                  type="text"
                  value={apiUrl}
                  onChange={e => setApiUrl(e.target.value)}
                  className="flex-1 px-4 py-2 bg-[var(--bg-dark)] border border-[var(--border)] rounded-lg text-[var(--text)] focus:outline-none focus:border-[var(--primary)]"
                  placeholder="http://localhost:5001"
                />
                <button
                  onClick={handleTestConnection}
                  className="px-4 py-2 bg-[var(--secondary)]/20 text-[var(--secondary)] rounded-lg hover:bg-[var(--secondary)]/30 transition-colors"
                >
                  Test
                </button>
              </div>
            </label>
          </div>
        </SettingCard>

        {/* API Keys */}
        <SettingCard
          title="API Keys"
          description="External service credentials for enrichment"
          icon={<Key className="w-5 h-5 text-[var(--primary)]" />}
        >
          <div className="space-y-4">
            <label className="block">
              <span className="text-sm text-[var(--text-muted)] mb-1 block">
                TMDB API Key
              </span>
              <input
                type="password"
                value={tmdbKey}
                onChange={e => setTmdbKey(e.target.value)}
                className="w-full px-4 py-2 bg-[var(--bg-dark)] border border-[var(--border)] rounded-lg text-[var(--text)] focus:outline-none focus:border-[var(--primary)]"
                placeholder="Enter your TMDB API key"
              />
            </label>
            <label className="block">
              <span className="text-sm text-[var(--text-muted)] mb-1 block">
                OMDB API Key
              </span>
              <input
                type="password"
                value={omdbKey}
                onChange={e => setOmdbKey(e.target.value)}
                className="w-full px-4 py-2 bg-[var(--bg-dark)] border border-[var(--border)] rounded-lg text-[var(--text)] focus:outline-none focus:border-[var(--primary)]"
                placeholder="Enter your OMDB API key"
              />
            </label>
            <p className="text-xs text-[var(--text-muted)]">
              Get your API keys from{" "}
              <a
                href="https://www.themoviedb.org/settings/api"
                target="_blank"
                rel="noopener noreferrer"
                className="text-[var(--primary)] hover:underline"
              >
                TMDB
              </a>{" "}
              and{" "}
              <a
                href="https://www.omdbapi.com/apikey.aspx"
                target="_blank"
                rel="noopener noreferrer"
                className="text-[var(--primary)] hover:underline"
              >
                OMDB
              </a>
            </p>
          </div>
        </SettingCard>

        {/* Appearance */}
        <SettingCard
          title="Appearance"
          description="Customize how CineScope looks"
          icon={<Palette className="w-5 h-5 text-[var(--primary)]" />}
        >
          <Toggle checked={darkMode} onChange={setDarkMode} label="Dark Mode" />
          <div className="flex items-center justify-between">
            <span className="text-[var(--text)]">Theme Color</span>
            <div className="flex gap-2">
              {[
                "#8b5cf6",
                "#3b82f6",
                "#10b981",
                "#f59e0b",
                "#ef4444",
                "#ec4899",
              ].map(color => (
                <button
                  key={color}
                  className="w-8 h-8 rounded-full ring-2 ring-offset-2 ring-offset-[var(--bg-dark)] ring-transparent hover:ring-white/50 transition-all"
                  style={{ backgroundColor: color }}
                />
              ))}
            </div>
          </div>
        </SettingCard>

        {/* Notifications */}
        <SettingCard
          title="Notifications"
          description="Control your notification preferences"
          icon={<Bell className="w-5 h-5 text-[var(--primary)]" />}
        >
          <Toggle
            checked={notifications}
            onChange={setNotifications}
            label="Enable Notifications"
          />
          <Toggle
            checked={autoRefresh}
            onChange={setAutoRefresh}
            label="Auto-refresh data"
          />
        </SettingCard>

        {/* Data Management */}
        <SettingCard
          title="Data Management"
          description="Manage your movie collection data"
          icon={<Database className="w-5 h-5 text-[var(--primary)]" />}
        >
          <div className="grid grid-cols-2 gap-3">
            <button className="flex items-center justify-center gap-2 px-4 py-3 bg-[var(--bg-dark)] border border-[var(--border)] rounded-lg text-[var(--text)] hover:border-[var(--primary)] transition-colors">
              <Download className="w-4 h-4" />
              Export Data
            </button>
            <button className="flex items-center justify-center gap-2 px-4 py-3 bg-[var(--bg-dark)] border border-[var(--border)] rounded-lg text-[var(--text)] hover:border-[var(--primary)] transition-colors">
              <Upload className="w-4 h-4" />
              Import Data
            </button>
            <button className="flex items-center justify-center gap-2 px-4 py-3 bg-[var(--bg-dark)] border border-[var(--border)] rounded-lg text-[var(--text)] hover:border-[var(--secondary)] transition-colors">
              <RefreshCw className="w-4 h-4" />
              Sync with Source
            </button>
            <button className="flex items-center justify-center gap-2 px-4 py-3 bg-red-500/20 border border-red-500/50 rounded-lg text-red-400 hover:bg-red-500/30 transition-colors">
              <Trash2 className="w-4 h-4" />
              Clear Cache
            </button>
          </div>
        </SettingCard>
      </div>

      {/* Save Button */}
      <div className="flex items-center justify-end gap-4 pt-4 border-t border-[var(--border)]">
        {saveStatus === "saved" && (
          <span className="flex items-center gap-2 text-green-400 text-sm">
            <CheckCircle className="w-4 h-4" />
            Settings saved
          </span>
        )}
        {saveStatus === "error" && (
          <span className="flex items-center gap-2 text-red-400 text-sm">
            <AlertCircle className="w-4 h-4" />
            Error saving settings
          </span>
        )}
        <button
          onClick={handleSave}
          disabled={saveStatus === "saving"}
          className="flex items-center gap-2 px-6 py-3 bg-[var(--primary)] text-white rounded-lg font-medium hover:bg-[var(--primary-dark)] transition-colors disabled:opacity-50"
        >
          {saveStatus === "saving" ? (
            <>
              <RefreshCw className="w-4 h-4 animate-spin" />
              Saving...
            </>
          ) : (
            <>
              <Save className="w-4 h-4" />
              Save Settings
            </>
          )}
        </button>
      </div>
    </div>
  );
}
