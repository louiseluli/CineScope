/* CineScope — Research UI (fixed)
   - No [object HTMLSpanElement] in titles
   - Hide internal IDs (tconst/nconst) from UI
   - Tighter meta/header rendering
*/

(() => {
  // -------------------------------
  // State
  // -------------------------------
  const state = {
    tab: "movies",
    page: 1,
    limit: 12,
    sortMovies: "rating",
    sortPeople: "relevance",
    query: "",
    genre: "",
    yearMin: "",
    yearMax: "",
    language: "",
    watched: "",
    facets: { genres: [], years: [], languages: [] },
    pages: 1,
    total: 0,
  };

  // -------------------------------
  // Elements
  // -------------------------------
  const $ = sel => document.querySelector(sel);
  const metaLine = $("#metaLine");
  const quickStats = $("#quickStats");
  const chips = $("#chips");
  const results = $("#results");
  const pager = $("#pager");
  const pageInfo = $("#pageInfo");
  const prevPageBtn = $("#prevPageBtn");
  const nextPageBtn = $("#nextPageBtn");
  const detailsPane = $("#detailsPane");
  const analyticsPane = $("#analyticsPane");
  const adminMsg = $("#adminMsg");

  const tabMovies = $("#tab-movies");
  const tabPeople = $("#tab-people");
  const searchInput = $("#searchInput");
  const searchBtn = $("#searchBtn");

  const filtersMovies = $("#filtersMovies");
  const filtersPeople = $("#filtersPeople");

  const genreSelect = $("#genreSelect");
  const yearMinSelect = $("#yearMinSelect");
  const yearMaxSelect = $("#yearMaxSelect");
  const langSelect = $("#langSelect");
  const watchedSelect = $("#watchedSelect");
  const sortSelect = $("#sortSelect");
  const sortPeopleSelect = $("#sortPeopleSelect");

  const applyFiltersBtn = $("#applyFiltersBtn");
  const applyPeopleBtn = $("#applyPeopleBtn");

  const reloadWatchedBtn = $("#reloadWatchedBtn");
  const viewMetaBtn = $("#viewMetaBtn");

  // -------------------------------
  // Utilities
  // -------------------------------
  function qs(params) {
    const u = new URLSearchParams();
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== null && v !== "") u.set(k, String(v));
    });
    return u.toString();
  }

  function setLoading(targetEl, message = "Loading…") {
    targetEl.innerHTML = `<div class="empty">${message}</div>`;
  }

  function setError(targetEl, message = "Something went wrong.") {
    targetEl.innerHTML = `<div class="empty"><span class="warn">${message}</span></div>`;
  }

  function chip(text) {
    const span = document.createElement("span");
    span.className = "chip";
    span.textContent = text;
    return span;
  }

  function esc(s) {
    return String(s == null ? "" : s);
  }

  function join(arr, sep = " · ") {
    return (arr || []).filter(Boolean).join(sep);
  }

  function humanList(arr, max = 3) {
    if (!arr || !arr.length) return "";
    const names = arr.map(x => x.name || x.title || x).filter(Boolean);
    return names.length > max
      ? names.slice(0, max).join(", ") + "…"
      : names.join(", ");
  }

  function toYear(y) {
    return y == null || y === "" ? "" : String(y);
  }

  // -------------------------------
  // Boot: facets & meta
  // -------------------------------
  async function boot() {
    try {
      const [facets, meta] = await Promise.all([
        fetch("/facets").then(r => r.json()),
        fetch("/meta").then(r => r.json()),
      ]);
      state.facets = facets;

      // Populate selects
      fillSelect(genreSelect, ["", ...facets.genres], "Genre");
      const years = facets.years || [];
      fillSelect(yearMinSelect, ["", ...years], "Year from");
      fillSelect(yearMaxSelect, ["", ...years], "Year to");
      fillSelect(langSelect, ["", ...facets.languages], "Language");

      // Header lines — write once, don’t append to footer
      metaLine.textContent = `Titles: ${meta.counts.titles} • People: ${
        meta.counts.people
      } • Watched: ${meta.counts.watched} • Mode: ${
        meta.mode || "watched-only"
      }`;
      quickStats.innerHTML = "";
      quickStats.append(stat("Titles", meta.counts.titles));
      quickStats.append(stat("People", meta.counts.people));
      quickStats.append(stat("Watched", meta.counts.watched));
      quickStats.append(stat("Mode", meta.mode || "watched-only"));
    } catch (e) {
      console.error(e);
      adminMsg.textContent = "Failed to load facets/meta.";
    }
  }

  function fillSelect(sel, values, placeholder) {
    sel.innerHTML = "";
    for (const v of values) {
      const opt = document.createElement("option");
      opt.value = v;
      opt.textContent = v === "" ? placeholder : v;
      sel.appendChild(opt);
    }
  }

  function stat(label, value) {
    const div = document.createElement("div");
    div.className = "stat";
    div.innerHTML = `<span class="muted">${esc(label)}:</span><b>${esc(
      value
    )}</b>`;
    return div;
  }

  // -------------------------------
  // Tabs
  // -------------------------------
  function switchTab(tab) {
    state.tab = tab;
    state.page = 1;
    if (tab === "movies") {
      tabMovies.classList.add("active");
      tabMovies.setAttribute("aria-selected", "true");
      tabPeople.classList.remove("active");
      tabPeople.setAttribute("aria-selected", "false");
      filtersMovies.style.display = "";
      filtersPeople.style.display = "none";
    } else {
      tabPeople.classList.add("active");
      tabPeople.setAttribute("aria-selected", "true");
      tabMovies.classList.remove("active");
      tabMovies.setAttribute("aria-selected", "false");
      filtersMovies.style.display = "none";
      filtersPeople.style.display = "";
    }
    results.innerHTML = `<div class="empty">Use the search box or apply filters to see results.</div>`;
    detailsPane.innerHTML = `<div class="empty">Click a ${
      tab === "movies" ? "movie" : "person"
    } to see details.</div>`;
    analyticsPane.innerHTML = `<div class="empty">Similar titles, collaborators, roles & top genres will appear here.</div>`;
    chips.innerHTML = "";
  }

  tabMovies.addEventListener("click", () => switchTab("movies"));
  tabPeople.addEventListener("click", () => switchTab("people"));

  // -------------------------------
  // Searching / Filtering
  // -------------------------------
  searchBtn.addEventListener("click", () => {
    state.query = searchInput.value.trim();
    state.page = 1;
    runQuery();
  });
  searchInput.addEventListener("keydown", e => {
    if (e.key === "Enter") {
      state.query = searchInput.value.trim();
      state.page = 1;
      runQuery();
    }
  });

  applyFiltersBtn.addEventListener("click", () => {
    state.genre = genreSelect.value;
    state.yearMin = yearMinSelect.value;
    state.yearMax = yearMaxSelect.value;
    state.language = langSelect.value;
    state.watched = watchedSelect.value;
    state.sortMovies = sortSelect.value || "rating";
    state.query = "";
    state.page = 1;
    runQuery();
  });

  applyPeopleBtn.addEventListener("click", () => {
    state.sortPeople = sortPeopleSelect.value || "relevance";
    state.page = 1;
    runQuery();
  });

  prevPageBtn.addEventListener("click", () => {
    if (state.page > 1) {
      state.page -= 1;
      runQuery();
    }
  });
  nextPageBtn.addEventListener("click", () => {
    if (state.page < state.pages) {
      state.page += 1;
      runQuery();
    }
  });

  function renderChips() {
    chips.innerHTML = "";
    const arr = [];
    if (state.tab === "movies") {
      if (state.query) arr.push(chip(`Query: “${state.query}”`));
      if (state.genre) arr.push(chip(`Genre: ${state.genre}`));
      if (state.yearMin) arr.push(chip(`Year ≥ ${state.yearMin}`));
      if (state.yearMax) arr.push(chip(`Year ≤ ${state.yearMax}`));
      if (state.language) arr.push(chip(`Lang: ${state.language}`));
      if (state.watched !== "")
        arr.push(chip(state.watched === "true" ? "Watched" : "Not watched"));
      arr.push(chip(`Sort: ${state.query ? "relevance" : state.sortMovies}`));
    } else {
      if (state.query) arr.push(chip(`Query: “${state.query}”`));
      arr.push(chip(`Sort: ${state.sortPeople}`));
    }
    if (!arr.length) arr.push(chip("No filters"));
    arr.forEach(c => chips.appendChild(c));
  }

  async function runQuery() {
    renderChips();
    setLoading(results, "Searching…");

    try {
      let url = "";
      if (state.tab === "movies") {
        if (state.query) {
          url =
            "/search/movie?" +
            qs({
              query: state.query,
              page: state.page,
              limit: state.limit,
              sort_by: "relevance",
            });
        } else {
          url =
            "/filter?" +
            qs({
              genre: state.genre,
              year_min: state.yearMin,
              year_max: state.yearMax,
              language: state.language,
              watched: state.watched,
              sort_by: state.sortMovies,
              page: state.page,
              limit: state.limit,
            });
        }
      } else {
        url =
          "/search/person?" +
          qs({
            query: state.query || "",
            page: state.page,
            limit: state.limit,
            sort_by: state.sortPeople,
          });
      }

      const r = await fetch(url);
      const data = await r.json();
      const payload = {
        results: Array.isArray(data.results) ? data.results : [],
        page: data.page || state.page,
        pages: data.pages || 1,
        total: data.total || 0,
        limit: data.limit || state.limit,
      };

      state.page = payload.page;
      state.pages = payload.pages;
      state.total = payload.total;

      if (!payload.results.length) {
        results.innerHTML = `<div class="empty">No matches. Try different search or filters.</div>`;
        pager.style.display = "none";
        return;
      }

      if (state.tab === "movies") {
        renderMovieCards(payload.results);
      } else {
        renderPersonCards(payload.results);
      }

      pageInfo.textContent = `Page ${state.page}/${state.pages}`;
      pager.style.display = state.pages > 1 ? "" : "none";
    } catch (e) {
      console.error(e);
      setError(results, "Failed to fetch results.");
      pager.style.display = "none";
    }
  }

  function renderMovieCards(items) {
    results.innerHTML = "";
    items.forEach(m => {
      const card = document.createElement("div");
      card.className = "item";
      const title = esc(m.primaryTitle || m.originalTitle || "(untitled)");
      const yr = toYear(m.year);
      // FIX: Use string for pill to avoid [object HTMLSpanElement]
      const watchedHTML = m.watched ? `<span class="pill">Watched</span>` : "";
      const rating =
        m.averageRating != null ? `${m.averageRating.toFixed(1)} ★` : "—";
      const votes =
        m.numVotes != null ? `${m.numVotes.toLocaleString()} votes` : "";
      const genres = (m.genres || []).slice(0, 3).join(" · ");

      card.innerHTML = `
        <div style="flex:1">
          <div class="title">${title} ${
        yr ? `<span class="muted">(${yr})</span>` : ""
      } ${watchedHTML}</div>
          <div class="meta">${genres || "<span class='muted'>—</span>"}</div>
          <div class="meta">${rating}${votes ? " · " + votes : ""} ${
        m.originalLanguage ? " · " + m.originalLanguage.toUpperCase() : ""
      }</div>
        </div>
        <div><button class="btn">Details</button></div>
      `;
      card
        .querySelector(".btn")
        .addEventListener("click", () => loadMovieDetails(m.tconst));
      card.addEventListener("click", ev => {
        if (ev.target.tagName !== "BUTTON") loadMovieDetails(m.tconst);
      });
      results.appendChild(card);
    });
  }

  function renderPersonCards(items) {
    results.innerHTML = "";
    items.forEach(p => {
      const card = document.createElement("div");
      card.className = "item";
      const name = esc(p.name);
      const years = [toYear(p.birthYear), toYear(p.deathYear)]
        .filter(Boolean)
        .join("–");
      const prof = (p.primaryProfession || []).join(", ");
      const credits =
        p.creditsSample != null ? `${p.creditsSample} credits (sample)` : "";

      card.innerHTML = `
        <div style="flex:1">
          <div class="title">${name} ${
        years ? `<span class="muted">(${years})</span>` : ""
      }</div>
          <div class="meta">${prof || "<span class='muted'>—</span>"}</div>
          <div class="meta">${credits}</div>
        </div>
        <div><button class="btn">Details</button></div>
      `;
      card
        .querySelector(".btn")
        .addEventListener("click", () => loadPersonDetails(p.nconst));
      card.addEventListener("click", ev => {
        if (ev.target.tagName !== "BUTTON") loadPersonDetails(p.nconst);
      });
      results.appendChild(card);
    });
  }

  // -------------------------------
  // Details + Analytics (IDs hidden)
  // -------------------------------
  async function loadMovieDetails(tconst) {
    setLoading(detailsPane, "Loading movie details…");
    analyticsPane.innerHTML = `<div class="empty">Loading analytics…</div>`;
    try {
      const data = await fetch(`/movie/${encodeURIComponent(tconst)}`).then(r =>
        r.json()
      );
      const title = esc(
        data.primaryTitle || data.originalTitle || "(untitled)"
      );
      const yr = toYear(data.startYear);
      const dnames = humanList(data.directors, 5) || "—";
      const wnames = humanList(data.writers, 5) || "—";
      const cast = humanList((data.cast || []).slice(0, 10), 10) || "—";
      const crew = humanList((data.crew || []).slice(0, 8), 8) || "—";

      detailsPane.innerHTML = `
        <div class="row" style="margin-bottom:8px">
          <div class="title" style="font-size:16px">${title} ${
        yr ? `<span class="muted">(${yr})</span>` : ""
      }</div>
          ${data.watched ? `<span class="pill">Watched</span>` : ""}
        </div>
        <div class="meta">Type: ${esc(data.titleType)} · Runtime: ${
        data.runtimeMinutes || "—"
      } min · Language: ${data.originalLanguage || "—"}</div>
        <div class="meta">Genres: ${join(data.genres || [], " · ") || "—"}</div>
        <div class="meta">Rating: ${
          data.averageRating != null
            ? data.averageRating.toFixed(1) + " ★"
            : "—"
        } · Votes: ${
        data.numVotes != null ? data.numVotes.toLocaleString() : "—"
      }</div>
        <hr style="border-color:var(--border);opacity:.4;margin:12px 0" />
        <div class="meta"><b>Directors:</b> ${dnames}</div>
        <div class="meta"><b>Writers:</b> ${wnames}</div>
        <div class="meta"><b>Cast (sample):</b> ${cast}</div>
        <div class="meta"><b>Crew (sample):</b> ${crew}</div>
      `;

      const sim = (data.analytics && data.analytics.similar) || [];
      const coll = (data.analytics && data.analytics.collaborators) || [];
      analyticsPane.innerHTML = `
        <div>
          <div style="margin-bottom:8px"><b>Similar titles</b></div>
          ${sim.length ? "" : `<div class="muted">—</div>`}
          ${sim
            .slice(0, 8)
            .map(
              s => `
            <div class="item" style="margin-bottom:8px;cursor:pointer" data-t="${
              s.tconst
            }">
              <div style="flex:1">
                <div class="title">${esc(s.primaryTitle)} ${
                s.year ? `<span class="muted">(${s.year})</span>` : ""
              }</div>
                <div class="meta">${
                  (s.genres || []).slice(0, 3).join(" · ") || "—"
                }</div>
                <div class="meta">Score: ${s.score} · ${
                s.averageRating != null
                  ? s.averageRating.toFixed(1) + " ★"
                  : "—"
              } ${
                s.numVotes != null
                  ? "· " + s.numVotes.toLocaleString() + " votes"
                  : ""
              }</div>
              </div>
              <div><button class="btn">Open</button></div>
            </div>
          `
            )
            .join("")}
        </div>
        <hr style="border-color:var(--border);opacity:.4;margin:12px 0" />
        <div>
          <div style="margin-bottom:8px"><b>Frequent collaborators</b></div>
          ${coll.length ? "" : `<div class="muted">—</div>`}
          ${coll
            .slice(0, 10)
            .map(
              c => `
            <div class="item" style="margin-bottom:8px;cursor:pointer" data-n="${
              c.nconst
            }">
              <div style="flex:1">
                <div class="title">${esc(c.name || c.nconst)}</div>
                <div class="meta">Joint credits: ${c.count}</div>
              </div>
              <div><button class="btn">Open</button></div>
            </div>
          `
            )
            .join("")}
        </div>
      `;
      analyticsPane.querySelectorAll("[data-t]").forEach(node => {
        const id = node.getAttribute("data-t");
        node.addEventListener("click", ev => {
          if (ev.target.tagName !== "BUTTON") loadMovieDetails(id);
        });
        node
          .querySelector(".btn")
          .addEventListener("click", () => loadMovieDetails(id));
      });
      analyticsPane.querySelectorAll("[data-n]").forEach(node => {
        const id = node.getAttribute("data-n");
        node.addEventListener("click", ev => {
          if (ev.target.tagName !== "BUTTON") loadPersonDetails(id);
        });
        node
          .querySelector(".btn")
          .addEventListener("click", () => loadPersonDetails(id));
      });
    } catch (e) {
      console.error(e);
      setError(detailsPane, "Failed to load movie details.");
      analyticsPane.innerHTML = `<div class="empty">—</div>`;
    }
  }

  async function loadPersonDetails(nconst) {
    setLoading(detailsPane, "Loading person profile…");
    analyticsPane.innerHTML = `<div class="empty">Loading analytics…</div>`;
    try {
      const data = await fetch(`/person/${encodeURIComponent(nconst)}`).then(
        r => r.json()
      );
      const name = esc(data.name);
      const years = [toYear(data.birthYear), toYear(data.deathYear)]
        .filter(Boolean)
        .join("–");
      const prof = (data.primaryProfession || []).join(", ");

      detailsPane.innerHTML = `
        <div class="title" style="font-size:16px">${name} ${
        years ? `<span class="muted">(${years})</span>` : ""
      }</div>
        <div class="meta">Professions: ${prof || "—"}</div>
        <hr style="border-color:var(--border);opacity:.4;margin:12px 0" />
        <div><b>Selected Filmography</b></div>
        <div style="margin-top:6px">
          ${
            data.filmography && data.filmography.length
              ? data.filmography
                  .slice(0, 20)
                  .map(
                    f => `
            <div class="item" style="margin-bottom:8px;cursor:pointer" data-t="${
              f.tconst
            }">
              <div style="flex:1">
                <div class="title">${esc(f.title || f.tconst)} ${
                      f.year ? `<span class="muted">(${f.year})</span>` : ""
                    }</div>
                <div class="meta">${
                  (f.genres || []).slice(0, 3).join(" · ") || "—"
                }</div>
                <div class="meta">${esc(f.titleType || "")} ${
                      f.rating != null ? "· " + f.rating.toFixed(1) + " ★" : ""
                    } ${f.category ? "· " + esc(f.category) : ""}</div>
              </div>
              <div><button class="btn">Open</button></div>
            </div>
          `
                  )
                  .join("")
              : `<div class="muted">—</div>`
          }
        </div>
      `;

      const stats = data.stats || {};
      const roles = stats.roles || {};
      const topGenres = stats.topGenres || [];
      const topCollabs = stats.topCollaborators || [];
      const avgRating =
        stats.avgRating != null ? stats.avgRating.toFixed(2) : "—";

      analyticsPane.innerHTML = `
        <div class="row" style="flex-wrap:wrap;gap:8px">
          <div class="stat"><span class="muted">Avg rating:</span><b>${avgRating}</b></div>
          <div class="stat"><span class="muted">Roles:</span><b>${
            Object.keys(roles).length
          }</b></div>
          <div class="stat"><span class="muted">Top genres:</span><b>${
            topGenres.length
          }</b></div>
        </div>
        <hr style="border-color:var(--border);opacity:.4;margin:12px 0" />
        <div>
          <div style="margin-bottom:6px"><b>Role distribution</b></div>
          ${
            Object.keys(roles).length
              ? Object.entries(roles)
                  .map(([k, v]) => `<span class="chip">${esc(k)}: ${v}</span>`)
                  .join(" ")
              : `<div class="muted">—</div>`
          }
        </div>
        <div style="margin-top:10px">
          <div style="margin-bottom:6px"><b>Top genres</b></div>
          ${
            topGenres.length
              ? topGenres
                  .map(
                    g => `<span class="chip">${esc(g.genre)}: ${g.count}</span>`
                  )
                  .join(" ")
              : `<div class="muted">—</div>`
          }
        </div>
        <div style="margin-top:10px">
          <div style="margin-bottom:6px"><b>Frequent collaborators</b></div>
          ${
            topCollabs.length
              ? topCollabs
                  .slice(0, 12)
                  .map(
                    c => `
            <div class="item" style="margin-bottom:8px;cursor:pointer" data-n="${
              c.nconst
            }">
              <div style="flex:1">
                <div class="title">${esc(c.name || c.nconst)}</div>
                <div class="meta">Joint credits: ${c.count}</div>
              </div>
              <div><button class="btn">Open</button></div>
            </div>
          `
                  )
                  .join("")
              : `<div class="muted">—</div>`
          }
        </div>
      `;
      analyticsPane.querySelectorAll("[data-n]").forEach(node => {
        const id = node.getAttribute("data-n");
        node.addEventListener("click", ev => {
          if (ev.target.tagName !== "BUTTON") loadPersonDetails(id);
        });
        node
          .querySelector(".btn")
          .addEventListener("click", () => loadPersonDetails(id));
      });
      detailsPane.querySelectorAll("[data-t]").forEach(node => {
        const id = node.getAttribute("data-t");
        node.addEventListener("click", ev => {
          if (ev.target.tagName !== "BUTTON") loadMovieDetails(id);
        });
        node
          .querySelector(".btn")
          .addEventListener("click", () => loadMovieDetails(id));
      });
    } catch (e) {
      console.error(e);
      setError(detailsPane, "Failed to load person details.");
      analyticsPane.innerHTML = `<div class="empty">—</div>`;
    }
  }

  // -------------------------------
  // Admin utilities
  // -------------------------------
  reloadWatchedBtn.addEventListener("click", async () => {
    adminMsg.textContent = "Reloading watched CSV…";
    try {
      const res = await fetch("/admin/reload-watched", { method: "POST" }).then(
        r => r.json()
      );
      adminMsg.textContent = `Watched reloaded. Count=${
        (res.meta && res.meta.watched_count) || "—"
      } • Source=${(res.meta && res.meta.source) || "—"}`;
      if (state.tab === "movies") runQuery();
    } catch (e) {
      console.error(e);
      adminMsg.textContent = "Failed to reload watched CSV.";
    }
  });

  viewMetaBtn.addEventListener("click", async () => {
    adminMsg.textContent = "Fetching cache status…";
    try {
      const m = await fetch("/admin/cache-status").then(r => r.json());
      adminMsg.innerHTML = `
        <div class="mono">
          cache_dir: ${esc(m.cache_dir)}<br/>
          imdb_dir: ${esc(m.imdb_dir)}<br/>
          watched_source: ${esc(m.watched_source)}<br/>
          mode: ${esc(m.mode)}<br/>
          counts: titles=${(m.counts && m.counts.titles) || 0}, people=${
        (m.counts && m.counts.people) || 0
      }, watched=${(m.counts && m.counts.watched) || 0}
        </div>`;
    } catch (e) {
      console.error(e);
      adminMsg.textContent = "Failed to fetch cache status.";
    }
  });

  // -------------------------------
  // Keyboard pager
  // -------------------------------
  window.addEventListener("keydown", e => {
    if (e.key === "ArrowLeft" && state.page > 1) {
      state.page -= 1;
      runQuery();
    } else if (e.key === "ArrowRight" && state.page < state.pages) {
      state.page += 1;
      runQuery();
    }
  });

  // -------------------------------
  // Init
  // -------------------------------
  switchTab("movies");
  boot();
})();
