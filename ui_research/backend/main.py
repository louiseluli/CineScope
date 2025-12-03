#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import os
import sys
from pathlib import Path
from flask import Flask, jsonify, request, send_from_directory, redirect
from flask_cors import CORS

from data_loader import load_all_data, refresh_watched
from imdb_query import (
    search_movies, search_people, filter_movies,
    get_movie_details, get_person_details, list_facets,
)

# Resolve UI dir:  ui_research/  (parent of this backend/ folder)
UI_DIR = Path(__file__).resolve().parents[1]

def create_app() -> Flask:
    app = Flask(__name__)
    CORS(app)

    app.logger.info("📦 Loading IMDb datasets and watched list (with cache)…")
    DATA = load_all_data()
    app.logger.info("✅ Data loaded. Titles=%s People=%s Watched=%s Mode=%s",
                    DATA["meta"]["counts"]["titles"],
                    DATA["meta"]["counts"]["people"],
                    DATA["meta"]["counts"]["watched"],
                    DATA["meta"].get("mode"))

    # ---------------------------
    # Helpers
    # ---------------------------
    def _get_int(name: str, default=None):
        v = request.args.get(name, None if default is None else str(default))
        if v is None or v == "":
            return default
        try:
            return int(v)
        except Exception:
            return default

    def _get_bool(name: str):
        v = request.args.get(name)
        if v is None:
            return None
        s = str(v).strip().lower()
        if s in {"true","1","yes","y"}: return True
        if s in {"false","0","no","n"}: return False
        return None

    # ---------------------------
    # UI routes (serve index.html, css, js)
    # ---------------------------
    @app.get("/")
    def ui_index():
        index_path = UI_DIR / "index.html"
        if not index_path.exists():
            # Fallback to API meta if UI not present
            return redirect("/meta", code=302)
        return send_from_directory(str(UI_DIR), "index.html")

    @app.get("/style.css")
    def ui_style():
        return send_from_directory(str(UI_DIR), "style.css")

    @app.get("/app.js")
    def ui_app_js():
        return send_from_directory(str(UI_DIR), "app.js")

    @app.get("/favicon.ico")
    def ui_favicon():
        # Optional: serve a real icon if you add one later
        return ("", 204)

    # ---------------------------
    # Health & meta
    # ---------------------------
    @app.get("/health")
    def health():
        return jsonify({"status": "ok", "service": "cinescope-research", "version": "1.1.0"})

    @app.get("/meta")
    def meta():
        return jsonify(DATA["meta"])

    @app.get("/facets")
    def facets():
        return jsonify(list_facets(DATA))

    # ---------------------------
    # Search / filter
    # ---------------------------
    @app.get("/search/movie")
    def search_movie():
        query = request.args.get("query", "", type=str)
        page = _get_int("page", 1)
        limit = _get_int("limit", 50)
        sort_by = request.args.get("sort_by", "relevance", type=str)
        results = search_movies(DATA, query=query, limit=limit, page=page, sort_by=sort_by)
        if not results["results"]:
            return jsonify(results), 404
        return jsonify(results)

    @app.get("/search/person")
    def search_person():
        query = request.args.get("query", "", type=str)
        page = _get_int("page", 1)
        limit = _get_int("limit", 50)
        sort_by = request.args.get("sort_by", "relevance", type=str)
        results = search_people(DATA, query=query, limit=limit, page=page, sort_by=sort_by)
        if not results["results"]:
            return jsonify(results), 404
        return jsonify(results)

    @app.get("/filter")
    def filter_titles():
        genre = request.args.get("genre")
        language = request.args.get("language")
        year_min = _get_int("year_min")
        year_max = _get_int("year_max")
        watched = _get_bool("watched")
        page = _get_int("page", 1)
        limit = _get_int("limit", 50)
        sort_by = request.args.get("sort_by", "rating")
        results = filter_movies(DATA, genre=genre, year_min=year_min, year_max=year_max,
                                language=language, watched=watched,
                                sort_by=sort_by, page=page, limit=limit)
        return jsonify(results)

    # ---------------------------
    # Details
    # ---------------------------
    @app.get("/movie/<tconst>")
    def movie_details(tconst: str):
        details = get_movie_details(DATA, tconst)
        if not details:
            return jsonify({"error": f"Movie {tconst} not found"}), 404
        return jsonify(details)

    @app.get("/person/<nconst>")
    def person_details(nconst: str):
        details = get_person_details(DATA, nconst)
        if not details:
            return jsonify({"error": f"Person {nconst} not found"}), 404
        return jsonify(details)

    # ---------------------------
    # Admin / utilities
    # ---------------------------
    @app.post("/admin/reload-watched")
    def admin_reload_watched():
        body = request.get_json(silent=True) or {}
        path = body.get("path")
        if path:
            os.environ["CINESCOPE_WATCHED_PATH"] = str(path)
        meta = refresh_watched(DATA)
        return jsonify({"status": "ok", "meta": meta})

    @app.get("/admin/cache-status")
    def admin_cache_status():
        return jsonify({
            "cache_dir": DATA["meta"].get("imdb_cache"),
            "imdb_dir": DATA["meta"].get("imdb_dir"),
            "watched_source": DATA["meta"].get("watched_source"),
            "kept_title_types": DATA["meta"].get("kept_title_types"),
            "counts": DATA["meta"].get("counts"),
            "mode": DATA["meta"].get("mode"),
            "version": "1.1.0"
        })

    # Errors
    @app.errorhandler(404)
    def not_found(err): return (jsonify({"error":"Not Found","detail":str(err)}), 404)
    @app.errorhandler(400)
    def bad_request(err): return (jsonify({"error":"Bad Request","detail":str(err)}), 400)
    @app.errorhandler(500)
    def server_error(err): return (jsonify({"error":"Internal Server Error","detail":str(err)}), 500)

    app.config["DATA"] = DATA
    return app

# WSGI entrypoint
app = create_app()

if __name__ == "__main__":
    import socket
    
    # Try to find an available port starting from 8000
    base_port = int(os.environ.get("PORT", "8000"))
    port = base_port
    max_attempts = 10
    
    for attempt in range(max_attempts):
        try:
            # Test if port is available
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("0.0.0.0", port))
            sock.close()
            print(f"✅ Port {port} is available")
            break
        except OSError:
            print(f"⚠️  Port {port} is in use, trying {port + 1}...")
            port += 1
    else:
        print(f"❌ Could not find an available port after {max_attempts} attempts")
        sys.exit(1)
    
    print(f"🚀 Starting CineScope on http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, debug=True)
