#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
imdb_query.py
Query helpers that operate on the in-memory DATA built by data_loader.py.

DATA shape (from data_loader.load_all_data):
{
  "titles": { tconst: { ... } },
  "people": { nconst: { ... } },
  "indices": {
      "byGenre": { genre: set(tconst) },
      "byYear": { year: set(tconst) },
      "byLanguage": { lang: set(tconst) },
      "byName": { lower_name: [nconst, ...] }
  },
  "frames": {
      "titles_df": pd.DataFrame(index=tconst, ...),
      "people_df": pd.DataFrame(index=nconst, ...)
  },
  "meta": {...}
}
"""

from __future__ import annotations
from typing import Dict, Any, List, Tuple
import math
import re

import pandas as pd


# ---------- Utilities ----------

def _paginate(items: List[Dict[str, Any]], page: int, limit: int) -> Tuple[List[Dict[str, Any]], int, int]:
    page = max(1, int(page or 1))
    limit = max(1, int(limit or 50))
    total = len(items)
    pages = max(1, math.ceil(total / limit))
    start = (page - 1) * limit
    end = start + limit
    return items[start:end], page, pages


def _norm_query(q: str) -> str:
    return (q or "").strip().lower()


def _title_year(rec: Dict[str, Any]) -> int | None:
    y = rec.get("startYear")
    return int(y) if isinstance(y, (int, float)) and not pd.isna(y) else None


def _safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def _resolve_people_names(DATA, items):
    out = []
    seen = set()
    for p in items or []:
        nid = p.get("nconst")
        if not nid or nid in seen:
            continue
        name = (DATA["people"].get(nid) or {}).get("name") or p.get("name")
        out.append({"nconst": nid, "name": name})
        seen.add(nid)
    return out

# ---------- Facets ----------

def list_facets(DATA: Dict[str, Any]) -> Dict[str, List]:
    titles = DATA["titles"]
    genres = set()
    years = set()
    langs = set()
    for rec in titles.values():
        for g in rec.get("genres") or []:
            if g:
                genres.add(g)
        y = _title_year(rec)
        if y:
            years.add(y)
        lang = rec.get("originalLanguage")
        if lang:
            langs.add(lang)
    return {
        "genres": sorted(genres),
        "years": sorted(years),
        "languages": sorted(langs)
    }


# ---------- Movies ----------

def _movie_card(rec: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "tconst": rec["tconst"],
        "primaryTitle": rec.get("primaryTitle"),
        "originalTitle": rec.get("originalTitle"),
        "year": _title_year(rec),
        "genres": rec.get("genres") or [],
        "averageRating": _safe_float(rec.get("averageRating")),
        "numVotes": rec.get("numVotes"),
        "originalLanguage": rec.get("originalLanguage"),
        "watched": bool(rec.get("watched", False)),
    }


def search_movies(DATA: Dict[str, Any], *, query: str, limit: int = 50, page: int = 1, sort_by: str = "relevance") -> Dict[str, Any]:
    q = _norm_query(query)
    if not q:
        # mimic "no results" semantics (UI shows graceful message)
        return {"results": [], "page": 1, "pages": 1, "total": 0, "limit": limit}

    titles = DATA["titles"]
    results = []
    for rec in titles.values():
        # basic textual matching on primaryTitle / originalTitle
        txt = " ".join([
            (rec.get("primaryTitle") or ""),
            (rec.get("originalTitle") or "")
        ]).lower()
        if q in txt:
            # crude relevance heuristic: longer match & rating boost
            base = len(q) / (len(txt) + 1e-6)
            rating = _safe_float(rec.get("averageRating")) or 0.0
            score = base + (rating / 20.0)  # small boost for better-rated titles
            results.append((score, rec))

    # sort: relevance desc
    results.sort(key=lambda x: x[0], reverse=True)
    cards = [_movie_card(r[1]) for r in results]

    page_items, p, pages = _paginate(cards, page, limit)
    return {"results": page_items, "page": p, "pages": pages, "total": len(cards), "limit": limit}


def filter_movies(
    DATA: Dict[str, Any],
    *,
    genre: str | None = None,
    year_min: int | None = None,
    year_max: int | None = None,
    language: str | None = None,
    watched: bool | None = None,
    sort_by: str = "rating",
    limit: int = 50,
    page: int = 1,
) -> Dict[str, Any]:
    titles = DATA["titles"]

    # start with all
    tconsts = set(titles.keys())

    # genre filter
    if genre:
        genre_set = DATA["indices"]["byGenre"].get(genre, set())
        tconsts &= genre_set

    # language filter
    if language:
        lang_set = DATA["indices"]["byLanguage"].get(language, set())
        tconsts &= lang_set

    # year filter
    if year_min is not None or year_max is not None:
        year_min = int(year_min) if year_min is not None else -10**9
        year_max = int(year_max) if year_max is not None else 10**9
        year_match = set()
        for t in tconsts:
            y = _title_year(titles[t])
            if y is not None and (year_min <= y <= year_max):
                year_match.add(t)
        tconsts &= year_match

    # watched filter
    if watched is not None:
        tconsts = {t for t in tconsts if bool(titles[t].get("watched", False)) == bool(watched)}

    items = [_movie_card(titles[t]) for t in tconsts]

    # sorting
    s = (sort_by or "rating").lower()
    if s == "rating":
        items.sort(key=lambda m: (m["averageRating"] is None, m["averageRating"] or 0.0, m["numVotes"] or 0), reverse=True)
    elif s == "votes":
        items.sort(key=lambda m: (m["numVotes"] is None, m["numVotes"] or 0), reverse=True)
    elif s == "year":
        items.sort(key=lambda m: (m["year"] is None, m["year"] or 0), reverse=True)
    elif s == "title":
        items.sort(key=lambda m: ((m["primaryTitle"] or m["originalTitle"] or "").lower()))
    else:
        # default to rating
        items.sort(key=lambda m: (m["averageRating"] is None, m["averageRating"] or 0.0, m["numVotes"] or 0), reverse=True)

    page_items, p, pages = _paginate(items, page, limit)
    return {"results": page_items, "page": p, "pages": pages, "total": len(items), "limit": limit}


# ---------- People ----------

def _person_card(nconst: str, person: Dict[str, Any]) -> Dict[str, Any]:
    # creditsSample: number of items in sampleFilmography we built
    credits = len(person.get("sampleFilmography") or [])
    return {
        "nconst": nconst,
        "name": person.get("name"),
        "birthYear": person.get("birthYear"),
        "deathYear": person.get("deathYear"),
        "primaryProfession": person.get("primaryProfession") or [],
        "creditsSample": credits,
    }


def search_people(DATA: Dict[str, Any], *, query: str, limit: int = 50, page: int = 1, sort_by: str = "relevance") -> Dict[str, Any]:
    q = _norm_query(query)
    if not q:
        return {"results": [], "page": 1, "pages": 1, "total": 0, "limit": limit}

    people = DATA["people"]
    results: List[Tuple[float, str, Dict[str, Any]]] = []

    # simple name contains + boost by sample credits
    for nconst, person in people.items():
        name = (person.get("name") or "").lower()
        if q in name:
            credits = len(person.get("sampleFilmography") or [])
            # basic score: match length + credits boost
            score = len(q) / (len(name) + 1e-6) + (credits / 100.0)
            results.append((score, nconst, person))

    # sort
    s = (sort_by or "relevance").lower()
    if s == "credits":
        results.sort(key=lambda x: len(x[2].get("sampleFilmography") or []), reverse=True)
    elif s == "birthyear":
        results.sort(key=lambda x: ((x[2].get("birthYear") is None), x[2].get("birthYear") or 0))
    else:
        results.sort(key=lambda x: x[0], reverse=True)

    cards = [_person_card(nc, p) for _, nc, p in results]
    page_items, p, pages = _paginate(cards, page, limit)
    return {"results": page_items, "page": p, "pages": pages, "total": len(cards), "limit": limit}


# ---------- Details & Analytics ----------

def _similar_titles_basis(a: Dict[str, Any], b: Dict[str, Any]) -> float:
    # Jaccard on genres + mild year proximity + rating proximity
    a_gen = set(a.get("genres") or [])
    b_gen = set(b.get("genres") or [])
    if not a_gen and not b_gen:
        j = 0.0
    else:
        inter = len(a_gen & b_gen)
        union = len(a_gen | b_gen) or 1
        j = inter / union

    ya = _title_year(a)
    yb = _title_year(b)
    yscore = 0.0
    if ya and yb:
        dy = abs(ya - yb)
        yscore = max(0.0, 1.0 - min(dy, 20) / 20.0) * 0.3  # <=20 years ⇒ some similarity

    ra = _safe_float(a.get("averageRating")) or 0.0
    rb = _safe_float(b.get("averageRating")) or 0.0
    rscore = max(0.0, 1.0 - abs(ra - rb) / 10.0) * 0.2

    return j * 1.0 + yscore + rscore


def _movie_analytics(DATA: Dict[str, Any], tconst: str, rec: Dict[str, Any]) -> Dict[str, Any]:
    # Similar titles: compute simple score across the (watched-only) corpus, then top N
    candidates = []
    for other_id, other in DATA["titles"].items():
        if other_id == tconst:
            continue
        score = _similar_titles_basis(rec, other)
        if score > 0:
            candidates.append({
                "tconst": other_id,
                "primaryTitle": other.get("primaryTitle") or other.get("originalTitle"),
                "year": _title_year(other),
                "genres": other.get("genres") or [],
                "averageRating": _safe_float(other.get("averageRating")),
                "numVotes": other.get("numVotes"),
                "score": round(score, 3),
            })
    candidates.sort(key=lambda x: (x["score"], x["averageRating"] or 0.0, x["numVotes"] or 0), reverse=True)
    similar = candidates[:12]

    # collaborators: count shared nconst in cast + crew + directors + writers
    from collections import Counter
    c = Counter()
    def _acc(lst):
        for p in lst or []:
            nid = p.get("nconst")
            if nid:
                c[nid] += 1

    _acc(rec.get("cast"))
    _acc(rec.get("crew"))
    _acc(rec.get("directors"))
    _acc(rec.get("writers"))

    collab = []
    for nid, cnt in c.most_common(15):
        name = (DATA["people"].get(nid) or {}).get("name")
        collab.append({"nconst": nid, "name": name, "count": int(cnt)})

    return {"similar": similar, "collaborators": collab}


def get_movie_details(DATA: Dict[str, Any], tconst: str) -> Dict[str, Any] | None:
    rec = DATA["titles"].get(tconst)
    if not rec:
        return None
    details = dict(rec)  # shallow copy
    details["analytics"] = _movie_analytics(DATA, tconst, rec)
    return details


def _person_stats_and_filmography(DATA: Dict[str, Any], nconst: str, person: Dict[str, Any]) -> Dict[str, Any]:
    # Build a richer filmography and some stats from titles we have loaded.
    from collections import Counter

    # Use sampleFilmography as seed; expand with any titles that include this nconst in cast/crew/directors/writers
    filmos = []

    # From sample list
    for f in person.get("sampleFilmography") or []:
        t = DATA["titles"].get(f.get("tconst"))
        if not t:
            continue
        filmos.append({
            "tconst": f.get("tconst"),
            "title": t.get("primaryTitle") or t.get("originalTitle"),
            "year": _title_year(t),
            "titleType": t.get("titleType"),
            "genres": t.get("genres") or [],
            "rating": _safe_float(t.get("averageRating")),
            "category": f.get("category"),
        })

    # From scan of titles (only the currently loaded set — watched-only by default)
    for tid, t in DATA["titles"].items():
        listed = False
        for p in (t.get("cast") or []) + (t.get("crew") or []) + (t.get("directors") or []) + (t.get("writers") or []):
            if p.get("nconst") == nconst:
                filmos.append({
                    "tconst": tid,
                    "title": t.get("primaryTitle") or t.get("originalTitle"),
                    "year": _title_year(t),
                    "titleType": t.get("titleType"),
                    "genres": t.get("genres") or [],
                    "rating": _safe_float(t.get("averageRating")),
                    "category": p.get("category") or p.get("job"),
                })
                listed = True
        # prevent duplicates (lightweight)
        if listed:
            pass

    # Deduplicate by tconst (favor first occurrence)
    seen = set()
    uniq = []
    for f in filmos:
        if f["tconst"] in seen:
            continue
        uniq.append(f)
        seen.add(f["tconst"])

    # Stats
    role_counter = Counter()
    genre_counter = Counter()
    ratings = []
    collab_counter = Counter()

    for f in uniq:
        if f.get("category"):
            role_counter[f["category"]] += 1
        for g in f.get("genres") or []:
            genre_counter[g] += 1
        if f.get("rating") is not None:
            ratings.append(float(f["rating"]))
        # collaborators from that title
        t = DATA["titles"].get(f["tconst"])
        if t:
            for p in (t.get("cast") or []) + (t.get("crew") or []) + (t.get("directors") or []) + (t.get("writers") or []):
                nid = p.get("nconst")
                if nid and nid != nconst:
                    collab_counter[nid] += 1

    avg_rating = sum(ratings) / len(ratings) if ratings else None

    # Top collaborators, resolve names
    top_collabs = []
    for nid, cnt in collab_counter.most_common(15):
        name = (DATA["people"].get(nid) or {}).get("name")
        top_collabs.append({"nconst": nid, "name": name, "count": int(cnt)})

    top_genres = [{"genre": g, "count": int(c)} for g, c in genre_counter.most_common(10)]

    return {
        "filmography": sorted(uniq, key=lambda f: (f["year"] is None, f["year"] or 0), reverse=True),
        "stats": {
            "roles": {k: int(v) for k, v in role_counter.items()},
            "topGenres": top_genres,
            "topCollaborators": top_collabs,
            "avgRating": avg_rating
        }
    }


def get_person_details(DATA: Dict[str, Any], nconst: str) -> Dict[str, Any] | None:
    person = DATA["people"].get(nconst)
    if not person:
        return None
    out = {
        "nconst": nconst,
        "name": person.get("name"),
        "birthYear": person.get("birthYear"),
        "deathYear": person.get("deathYear"),
        "primaryProfession": person.get("primaryProfession") or [],
        "knownForTitles": person.get("knownForTitles") or [],
    }
    enrich = _person_stats_and_filmography(DATA, nconst, person)
    out.update(enrich)
    return out
