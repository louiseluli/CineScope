#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CineScope - Batch 2: Content Genome Analysis (CAST-AWARE, ENHANCED+DEBUG)
-------------------------------------------------------------------
- Robust cast detection & parsing from columns like: cast, cast_list, cast_display,
  actors, starring, performers, principalCast, top_billed_cast, credits, etc.
- Parses JSON (TMDB-style), Python dict/list literals, HTML-ish blobs, or delimited
  strings (| ; , / • · – — - + & and newline). Also handles "Name as Character"
  and "Name (Role)" forms.
- Emits rich DEBUG:
    * which columns were detected
    * non-null counts per column
    * writes sample raw values to: analysis_outputs/visualizations/batch_2/exports/cast_raw_samples.csv
- When any names are parsed, exports normalized cast table to:
    analysis_outputs/visualizations/batch_2/exports/actors_normalized.csv

Run from project root:
  PYTHONPATH=./src python scripts/batch_2_content_genome.py
"""

from __future__ import annotations

import ast
import json
import math
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# ------------------------------------------------------------------------------
# Project paths / imports
# ------------------------------------------------------------------------------
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import (  # type: ignore
    WATCHED_ONLY_DATA, VISUALIZATIONS_DIR, RATING_COLORS, GENRE_COLORS,
    DEFAULT_FIGSIZE, DEFAULT_DPI, save_figure, get_batch_output_dir,
    log_message
)
from src.core.helpers import (
    parse_genres, parse_directors, explode_genres
)

# ------------------------------------------------------------------------------
# Cast parsing (robust)
# ------------------------------------------------------------------------------

# Hard splitters (includes pipes, semicolons, commas, slashes, bullets, dots, dashes,
# plus signs, ampersands, and newlines). We also split on the word "and" separately below.
_SPLIT_RE = re.compile(r"\s*[|;,/•·\u00B7\-\u2013\u2014\+\&\n]\s*")
# " as " pattern (keep actor name, drop character)
_AS_RE = re.compile(r"\s+\bas\b\s+", flags=re.IGNORECASE)
# HTML tag stripper
_HTML_TAG_RE = re.compile(r"<[^>]+>")
# Trailing (Role) or - Role or – Role or — Role → drop the role
_ROLE_PARENS_RE = re.compile(r"\s*\([^)]*\)\s*$")
_ROLE_DASH_RE = re.compile(r"\s+[–—-]\s+.*$")

# Column discovery
_CAST_PATTERNS = [
    r"(?:^|_|\b)(cast|actors?)(?:$|_|\b)",            # cast, cast_list, imdb_cast, actors, actor_list
    r"(?:^|_|\b)starr(?:ing)?(?:$|_|\b)",             # starring, star, stars
    r"(?:^|_|\b)perform(?:er|ance|ers)?(?:$|_|\b)",   # performer(s)
    r"(?:^|_|\b)principal(?:$|_|\b).*?(cast|perform)",# principalCast / principal_performers
    r"(?:^|_|\b)top(?:$|_|\b).*?cast",                # top_billed_cast, topcast
    r"(?:^|_|\b)credits?(?:$|_|\b)",                  # credits (if it stores cast lists)
    # Common explicit names you mentioned:
    r"(?:^|_|\b)cast_list(?:$|_|\b)",
    r"(?:^|_|\b)cast_display(?:$|_|\b)",
]

def _strip_html(s: str) -> str:
    return _HTML_TAG_RE.sub("", s)

def _safe_json_load(val: str) -> Any:
    s = str(val).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    try:
        return json.loads(s)
    except Exception:
        try:
            return ast.literal_eval(s)
        except Exception:
            return None

def _normalize_name(name: str) -> str:
    n = str(name)
    n = _strip_html(n)
    n = re.sub(r"\s+", " ", n).strip()
    n = n.strip("\"'[]{}()")
    # "Name as Character" → keep "Name"
    n = _AS_RE.split(n)[0].strip()
    # drop trailing role in parentheses: "Name (Role)" -> "Name"
    n = _ROLE_PARENS_RE.sub("", n)
    # drop "Name - Role" / "Name – Role" / "Name — Role" -> "Name"
    n = _ROLE_DASH_RE.sub("", n)
    return n

def _extract_from_json_item(it: Any) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    """
    Handle TMDB/IMDB-like dicts:
      {'name':'Tom Hanks','character':'...','gender':2}
      {'original_name':'...','roles':[{'character':'...'}]}
    Returns (name, character, gender)
    """
    if not isinstance(it, dict):
        return None, None, None

    name = it.get("name") or it.get("original_name") or it.get("person") or it.get("actor") or it.get("title")
    if name is not None:
        name = _normalize_name(name)

    character = it.get("character") or it.get("role") or it.get("roles")
    if isinstance(character, list) and character:
        ch0 = character[0]
        if isinstance(ch0, dict):
            character = ch0.get("character") or ch0.get("role")
    if isinstance(character, (dict, list)):
        character = None
    if character is not None:
        character = str(character).strip()

    gender = it.get("gender")
    try:
        gender = int(gender) if gender is not None and str(gender).isdigit() else None
    except Exception:
        gender = None

    return name, character, gender

def _smart_string_split(raw: str) -> List[str]:
    """
    Splits a raw string into tokens by:
      1) hard separators (regex above), then
      2) the word 'and' when it likely joins the last two names (e.g., "A, B and C")
         or when there are no commas/pipes at all but 'and' is present.
    """
    s = _strip_html(raw).strip()
    if not s:
        return []

    # First pass: hard separators
    parts = [p for p in _SPLIT_RE.split(s) if p]
    if len(parts) > 1:
        # Now split any residual " X and Y " joints inside tokens which themselves contain no commas/pipes
        refined: List[str] = []
        for p in parts:
            if re.search(r"\band\b", p, flags=re.IGNORECASE) and not re.search(r"[|;,/•·\u00B7\-\u2013\u2014\+\&]", p):
                refined.extend([q.strip() for q in re.split(r"\s+\band\b\s+", p, flags=re.IGNORECASE) if q.strip()])
            else:
                refined.append(p)
        return refined

    # If hard splitting yielded one chunk, try a gentle "and" split
    if re.search(r"\band\b", s, flags=re.IGNORECASE):
        return [q.strip() for q in re.split(r"\s+\band\b\s+", s, flags=re.IGNORECASE) if q.strip()]

    return [s]

def _extract_names_from_cell(cell: Any) -> List[Tuple[str, Optional[str], Optional[int]]]:
    """
    Accepts:
      - JSON arrays/dicts (TMDB style) or Python literal strings of those
      - delimited strings: "A | B | C", "A, B and C", "A/B/C", bullets/lines/dashes, etc.
      - "A as X | B as Y" or "A (Role)"
    Returns list[(name, character, gender)]
    """
    out: List[Tuple[str, Optional[str], Optional[int]]] = []

    # None / NaN → nothing to parse
    if cell is None or (isinstance(cell, float) and math.isnan(cell)):
        return out

    # If it's already a list/dict, handle directly
    if isinstance(cell, (list, dict)):
        if isinstance(cell, list):
            for it in cell:
                name, character, gender = _extract_from_json_item(it)
                if name:
                    out.append((name, character, gender))
            return out
        if isinstance(cell, dict):
            for key in ("cast", "actors", "people", "credits"):
                if key in cell and isinstance(cell[key], list):
                    for it in cell[key]:
                        name, character, gender = _extract_from_json_item(it)
                        if name:
                            out.append((name, character, gender))
                    return out
            name, character, gender = _extract_from_json_item(cell)
            if name:
                out.append((name, character, gender))
            return out

    # String path (JSON-ish? delimited? HTML-ish?)
    s = str(cell).strip()
    if not s:
        return out

    # Try to parse as JSON/Python literal if it LOOKS like one
    looks_jsonish = s.startswith(("[", "{"))
    if looks_jsonish:
        obj = _safe_json_load(s)
        if isinstance(obj, list):
            for it in obj:
                name, character, gender = _extract_from_json_item(it)
                if name:
                    out.append((name, character, gender))
            if out:
                return out
            # fall through if empty to try string parsing

        elif isinstance(obj, dict):
            for key in ("cast", "actors", "people", "credits"):
                if key in obj and isinstance(obj[key], list):
                    for it in obj[key]:
                        name, character, gender = _extract_from_json_item(it)
                        if name:
                            out.append((name, character, gender))
                    if out:
                        return out
            # single credit-like dict
            name, character, gender = _extract_from_json_item(obj)
            if name:
                out.append((name, character, gender))
                return out
            # fall through if nothing

        # If JSON parse failed (obj is None or not list/dict), DO NOT return early.
        # Many exports are like: [Tom Hanks, Meg Ryan]  (no quotes)
        # Strip outer brackets and treat as a delimited list.
        stripped = s
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
            stripped = s[1:-1].strip()
        # Try splitting the stripped string; if nothing, we’ll still run the generic splitter below
        if stripped:
            tentative_parts = _smart_string_split(stripped)
            for token in tentative_parts:
                token = token.strip()
                if not token:
                    continue
                # Remove trailing commas left from CSV-ish chunks
                if token.endswith(","):
                    token = token[:-1].strip()
                # Handle "Name as Role"
                if " as " in token.lower():
                    name = _AS_RE.split(token)[0]
                    name = _normalize_name(name)
                    if name:
                        out.append((name, None, None))
                else:
                    name = _normalize_name(token)
                    if name:
                        out.append((name, None, None))
            if out:
                return out
        # If still nothing, we will fall back to generic string parsing next.

    # Generic plain-string parsing path (delimiters, "and", etc.)
    for token in _smart_string_split(_strip_html(s)):
        if not token:
            continue
        if " as " in token.lower():
            name = _AS_RE.split(token)[0]
            name = _normalize_name(name)
            if name:
                out.append((name, None, None))
        else:
            name = _normalize_name(token)
            if name:
                out.append((name, None, None))

    return out

def find_cast_cols(df: pd.DataFrame) -> list[str]:
    cols = []
    for c in df.columns:
        cl = str(c).strip()
        for pat in _CAST_PATTERNS:
            if re.search(pat, cl, flags=re.IGNORECASE):
                cols.append(c)
                break
    # de-dup while preserving order
    seen = set(); out = []
    for c in cols:
        if c not in seen:
            out.append(c); seen.add(c)
    # lightweight debug
    try:
        print(f"[DEBUG] Cast-like columns detected: {out}")
        nonnull_counts = {c: int(pd.notna(df[c]).sum()) for c in out}
        print(f"[DEBUG] Non-null counts per cast-like column: {nonnull_counts}")
    except Exception:
        pass
    return out

def _imdb_cast_fallback(df: pd.DataFrame, cache_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    Build a normalized cast table using ONLY IMDb IDs (tconst/nconst) from the local IMDb cache.
    Looks in data/processed/imdb_cache/{principals.parquet, names.parquet}.
    Returns columns: const, actor, character (None), gender (1=female, 2=male, None=unknown), source_col
    """
    try:
        cache_dir = cache_dir or (project_root / "data" / "processed" / "imdb_cache")
        principals_path = cache_dir / "principals.parquet"
        names_path = cache_dir / "names.parquet"

        if not principals_path.exists() or not names_path.exists():
            log_message(f"IMDb fallback: cache not found at {cache_dir}", level="WARNING")
            return pd.DataFrame(columns=["const", "actor", "character", "gender", "source_col"])

        # Read only the columns we need for speed
        pcols = ["tconst", "nconst", "category", "ordering"]
        ncols = ["nconst", "primaryName"]
        principals = pd.read_parquet(principals_path, columns=pcols)
        names = pd.read_parquet(names_path, columns=ncols)

        # Keep common acting categories. You can expand this list if you want cameos, archive, etc.
        acting_cats = {"actor", "actress", "self"}
        principals = principals[principals["category"].isin(acting_cats)]

        # Restrict to titles actually in our DF
        wanted = set(df["const"].astype(str).unique())
        principals = principals[principals["tconst"].isin(wanted)]

        if principals.empty:
            log_message("IMDb fallback: no principals matched your title set.", level="WARNING")
            return pd.DataFrame(columns=["const", "actor", "character", "gender", "source_col"])

        # Join to names to get display names
        merged = principals.merge(names, on="nconst", how="left")

        # Map simple gender from category (IMDb names.tsv has no gender field)
        def _g_from_cat(cat: str) -> Optional[int]:
            if cat == "actor":
                return 2  # male
            if cat == "actress":
                return 1  # female
            return None  # "self" or unknown

        merged["gender"] = merged["category"].map(_g_from_cat)
        merged["actor"] = merged["primaryName"].astype(str)
        merged["character"] = None
        merged["const"] = merged["tconst"].astype(str)
        merged["source_col"] = "imdb_cache"

        out = (merged[["const", "actor", "character", "gender", "source_col"]]
               .dropna(subset=["actor"])
               .sort_values(["const", "actor", "source_col", "gender"])
               .drop_duplicates(subset=["const", "actor"], keep="first")
               .reset_index(drop=True))

        # Helpful log
        log_message(f"IMDb fallback: cast recovered for "
                    f"{out['const'].nunique()} films; {out['actor'].nunique()} unique actors "
                    f"({len(out):,} film–actor links).")

        return out

    except Exception as e:
        log_message(f"IMDb fallback error: {e}", level="WARNING")
        return pd.DataFrame(columns=["const", "actor", "character", "gender", "source_col"])

def _imdb_nconsts_in_castlist(df: pd.DataFrame, cache_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    If a column like cast_list contains IMDb person IDs (nm1234567),
    map them to names via names.parquet and return a normalized cast table.
    """
    cache_dir = cache_dir or (project_root / "data" / "processed" / "imdb_cache")
    names_path = cache_dir / "names.parquet"
    if not names_path.exists():
        return pd.DataFrame(columns=["const","actor","character","gender","source_col"])

    # detect a cast-like column that stores nconsts
    cast_cols = [c for c in df.columns if re.search(r"(?:^|_)(cast|actors?)(?:$|_)", str(c), re.I)]
    if not cast_cols:
        return pd.DataFrame(columns=["const","actor","character","gender","source_col"])

    nm_re = re.compile(r"nm\d{5,9}")
    rows = []
    for _, r in df.iterrows():
        t = str(r.get("const"))
        for col in cast_cols:
            raw = r.get(col)
            if raw is None or (isinstance(raw, float) and math.isnan(raw)): 
                continue
            s = str(raw)
            ids = nm_re.findall(s)
            for nconst in ids:
                rows.append({"const": t, "nconst": nconst, "source_col": col})

    if not rows:
        return pd.DataFrame(columns=["const","actor","character","gender","source_col"])

    ndf = pd.DataFrame(rows).drop_duplicates()
    names = pd.read_parquet(names_path, columns=["nconst","primaryName"])
    out = (ndf.merge(names, on="nconst", how="left")
              .rename(columns={"primaryName":"actor"})
              .assign(character=None, gender=None)
              [["const","actor","character","gender","source_col"]]
              .dropna(subset=["actor"])
              .drop_duplicates())
    return out


def dump_cast_raw_samples(df: pd.DataFrame, cast_cols: List[str], export_dir: Path, max_rows: int = 200) -> None:
    """Write a CSV with a few raw, non-empty examples from each cast-like column."""
    rows = []
    for col in cast_cols:
        series = df[col].dropna()
        series = series[series.astype(str).str.strip().astype(bool)]
        for i, val in enumerate(series.head(max_rows)):
            rows.append({"column": col, "example_index": i, "raw_value": str(val)[:4000]})
    if rows:
        out = pd.DataFrame(rows)
        out_path = export_dir / "cast_raw_samples.csv"
        out.to_csv(out_path, index=False)

def build_cast_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect cast columns and build normalized table:
      columns: const, actor, character (opt), gender (TMDB: 1=female, 2=male), source_col

    Strategy:
      1) Try to parse any cast-like columns already in the dataframe (your original flow).
      2) If nothing is parsed, FALL BACK to IMDb cache (principals.parquet + names.parquet).
    """
    # ---------- Step 1: try existing cast-like columns ----------
    cast_cols = find_cast_cols(df)

    # Debug: how many non-nulls per cast-like column
    nonnull_counts = {c: int(pd.Series(df[c]).notna().sum()) for c in cast_cols if c in df.columns}
    try:
        print(f"[DEBUG] Non-null counts per cast-like column: {nonnull_counts}")
    except Exception:
        pass

    rows = []
    if cast_cols:
        # export a raw sample so we can inspect what's inside if parsing fails
        try:
            sample_out = (get_batch_output_dir(2) / "exports")
            sample_out.mkdir(parents=True, exist_ok=True)
            (df[["const"] + cast_cols].head(50)
               .to_csv(sample_out / "cast_raw_samples.csv", index=False))
        except Exception:
            pass

        for _, row in df.iterrows():
            cst = str(row.get("const"))
            for col in cast_cols:
                cell = row.get(col)
                for name, character, gender in _extract_names_from_cell(cell):
                    if not name or str(name).strip().lower() in {"nan", "none", "null", ""}:
                        continue
                    rows.append({
                        "const": cst,
                        "actor": _normalize_name(name),
                        "character": character,
                        "gender": gender,
                        "source_col": col
                    })

    if rows:
        out = pd.DataFrame(rows)
        out = (out.sort_values(["const", "actor"])
                  .drop_duplicates(subset=["const", "actor"], keep="first")
                  .reset_index(drop=True))
        return out

    # ---------- Step 2: hard fallback via IMDb cache ----------
    log_message("⚠️ No cast detected after parsing. Falling back to IMDb cache.", level="WARNING")
    imdb_out = _imdb_cast_fallback(df)
    if not imdb_out.empty:
        return imdb_out

    # ---------- Nothing found ----------
    log_message("⚠️ Cast unavailable in both inline columns and IMDb cache.", level="WARNING")
    return pd.DataFrame(columns=["const", "actor", "character", "gender", "source_col"])

IMDB_CACHE = project_root / "data" / "processed" / "imdb_cache"

def load_imdb_tables():
    paths = {
        "names": IMDB_CACHE / "names.parquet",
        "principals": IMDB_CACHE / "principals.parquet",
        "crew": IMDB_CACHE / "crew.parquet",  # if missing, we’ll derive from TSV elsewhere
    }
    missing = [k for k, p in paths.items() if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Missing IMDb cache: {missing}. Place the parquet files in {IMDB_CACHE}")
    names = pd.read_parquet(paths["names"], columns=["nconst","primaryName"])
    principals = pd.read_parquet(paths["principals"], columns=["tconst","nconst","category","ordering"])
    crew = pd.read_parquet(paths["crew"], columns=["tconst","directors","writers"])
    return names, principals, crew

def _extract_tmdb_gender(df_titles: pd.DataFrame) -> pd.DataFrame:
    """Extract gender from tmdb_cast JSON. Returns: actor name → gender (1=F, 2=M)."""
    gender_map = {}  # name → gender (1=female, 2=male, 0=unknown)
    
    if "tmdb_cast" not in df_titles.columns:
        return pd.DataFrame(columns=["actor_name", "tmdb_gender"])
    
    for cast_json in df_titles["tmdb_cast"].dropna():
        try:
            cast_list = ast.literal_eval(cast_json) if isinstance(cast_json, str) else cast_json
            if not isinstance(cast_list, list):
                continue
            for person in cast_list:
                if isinstance(person, dict):
                    name = person.get("name", "").strip()
                    gender = person.get("gender")  # 1=F, 2=M, 0=unknown
                    if name and gender is not None:
                        # Keep highest gender value if there's a conflict
                        if name not in gender_map or gender > gender_map[name]:
                            gender_map[name] = gender
        except:
            pass
    
    if not gender_map:
        return pd.DataFrame(columns=["actor_name", "tmdb_gender"])
    
    return pd.DataFrame([
        {"actor_name": name, "tmdb_gender": gender}
        for name, gender in gender_map.items()
    ])

def build_imdb_cast(df_titles: pd.DataFrame) -> pd.DataFrame:
    """Strict cast from IMDb only, enriched with TMDB gender. Returns: const, nconst, actor, gender, ordering."""
    names, principals, _ = load_imdb_tables()
    acting = principals[principals["category"].isin({"actor","actress","self"})].copy()

    # Keep only titles in our watched table
    watch_tconst = set(df_titles["const"].astype(str))
    acting = acting[acting["tconst"].isin(watch_tconst)]

    cast = (acting.merge(names, on="nconst", how="left")
                 .rename(columns={"primaryName":"actor"}))
    # Quality gates
    cast = cast[cast["actor"].notna()]
    cast = cast[~cast["actor"].str.fullmatch(r"\d{3,4}")]  # drop “1905”, etc.
    cast["const"] = cast["tconst"].astype(str)
    
    # Extract TMDB gender and merge by actor name
    tmdb_gender = _extract_tmdb_gender(df_titles)
    if not tmdb_gender.empty:
        cast = cast.merge(tmdb_gender, left_on="actor", right_on="actor_name", how="left")
        cast["gender"] = cast["tmdb_gender"]
        cast = cast.drop(columns=["tmdb_gender", "actor_name"], errors="ignore")
    else:
        cast["gender"] = None
    
    cast = cast[["const","nconst","actor","gender","ordering"]].drop_duplicates()
    return cast.reset_index(drop=True)

def build_imdb_directors_writers(df_titles: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return directors and writers tables with resolved names."""
    names, _, crew = load_imdb_tables()
    watch_tconst = set(df_titles["const"].astype(str))
    crew = crew[crew["tconst"].isin(watch_tconst)].copy()

    def _parse_ids(val):
        """Parse director/writer IDs which can be list [nm123] or comma-separated string."""
        # Check if it's a list or array-like
        if isinstance(val, (list, np.ndarray)):
            return list(val) if val is not None else []
        # Check for null/None/NaN
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return []
        # Try parsing as Python literal (list format)
        try:
            return ast.literal_eval(str(val))
        except:
            # Try comma-separated
            s = str(val).strip()
            if s:
                return s.split(",")
            return []

    rows_dir, rows_wri = [], []
    for tconst, dirs, writs in crew[["tconst", "directors", "writers"]].itertuples(index=False):
        for n in _parse_ids(dirs):
            n = str(n).strip("[] '\"")
            if n:
                rows_dir.append((tconst, n))
        for n in _parse_ids(writs):
            n = str(n).strip("[] '\"")
            if n:
                rows_wri.append((tconst, n))

    dir_df = pd.DataFrame(rows_dir, columns=["const", "nconst"]).drop_duplicates()
    wri_df = pd.DataFrame(rows_wri, columns=["const", "nconst"]).drop_duplicates()

    if not dir_df.empty:
        dir_df = dir_df.merge(names, on="nconst", how="left").rename(columns={"primaryName": "director"})
        dir_df = dir_df[dir_df["director"].notna()]
    
    if not wri_df.empty:
        wri_df = wri_df.merge(names, on="nconst", how="left").rename(columns={"primaryName": "writer"})
        wri_df = wri_df[wri_df["writer"].notna()]
    
    return dir_df.reset_index(drop=True), wri_df.reset_index(drop=True)


# -------- DDD & Quality Helpers --------

def load_ddd() -> pd.DataFrame:
    """Load DDD data with strict tconst join. Returns empty if file missing."""
    ddd_path = project_root / "data" / "processed" / "ddd" / "ddd.parquet"
    if not ddd_path.exists():
        log_message("⚠️ DDD file not found; skipping DDD integration.", level="WARNING")
        return pd.DataFrame()
    try:
        ddd = pd.read_parquet(ddd_path)
        # Normalize tconst column name if needed
        if "tconst" not in ddd.columns and "imdb_id" in ddd.columns:
            ddd = ddd.rename(columns={"imdb_id": "tconst"})
        ddd["const"] = ddd["tconst"].astype(str)
        ddd = ddd.drop(columns=[c for c in ["tconst"] if c in ddd.columns])
        log_message(f"✅ DDD loaded: {ddd.shape[0]} titles, {ddd.shape[1]-1} features")
        return ddd
    except Exception as e:
        log_message(f"⚠️ Error loading DDD: {e}", level="WARNING")
        return pd.DataFrame()


def _clean_actor_series(s: pd.Series) -> pd.Series:
    """Drop garbage actor names: all-digits, single letters, etc."""
    s = s.dropna().astype(str).str.strip()
    # Drop years/numbers (3-4 digits), single letters, and very short junk
    mask = (~s.str.fullmatch(r"\d{3,4}")) & (~s.str.fullmatch(r"[A-Za-z]")) & (s.str.len() > 1)
    return s[mask]


def _barh_pairs(fig_title: str, pairs: list, outfile: str, label_tpl: str = "{a} ↔ {b}", 
                color: str = "#b2df8a", batch_number: int = 2):
    """Generic horizontal bar chart for pairs (director–actor, writer–actor, etc.)."""
    from collections import Counter as C
    top = C(pairs).most_common(30)
    if not top:
        fig, ax = plt.subplots(figsize=(16, 12))
        ax.text(0.5, 0.5, "No links found.", ha="center", va="center", fontsize=16)
        ax.axis("off")
        ax.set_title(fig_title)
        save_figure(fig, outfile, batch_number=batch_number)
        plt.close()
        return
    labels = [label_tpl.format(a=a, b=b) for (a, b), _ in top]
    vals = [c for _, c in top]
    fig, ax = plt.subplots(figsize=(16, 12))
    ax.barh(labels[::-1], vals[::-1], color=color, edgecolor="black")
    ax.set_title(fig_title, fontweight="bold", fontsize=16, pad=20)
    ax.set_xlabel("Number of Films Together", fontsize=12)
    ax.grid(axis="x", alpha=0.3)
    plt.tight_layout()
    save_figure(fig, outfile, batch_number=batch_number)
    plt.close()


# ------------------------------------------------------------------------------
# Analysis class
# ------------------------------------------------------------------------------

class ContentGenomeAnalysis:
    def __init__(self):
        self.df: pd.DataFrame = pd.DataFrame()
        self.batch_dir = get_batch_output_dir(2)
        self.actors_df: pd.DataFrame = pd.DataFrame()
        self.directors_df: pd.DataFrame = pd.DataFrame()
        self.writers_df: pd.DataFrame = pd.DataFrame()
        self.ddd: pd.DataFrame = pd.DataFrame()

    # -------------------- Load --------------------
    def load_data(self):
        log_message("=" * 80)
        log_message("BATCH 2: Content Genome Analysis")
        log_message("Loading my watched movies for deep analysis...")
        log_message("=" * 80)

        self.df = pd.read_csv(WATCHED_ONLY_DATA)
        log_message(f"\nLoaded: {len(self.df):,} watched films")

        # Build cast, directors, writers from IMDb
        self.actors_df = build_imdb_cast(self.df)
        if self.actors_df.empty:
            log_message("❌ IMDb cast could not be built. Check imdb cache files.", level="ERROR")
        else:
            log_message(f"✅ IMDb cast: {self.actors_df['const'].nunique()} films; "
                        f"{self.actors_df['nconst'].nunique()} unique actors; "
                        f"{len(self.actors_df):,} film–actor links")

        self.directors_df, self.writers_df = build_imdb_directors_writers(self.df)
        log_message(f"✅ Directors linked: {self.directors_df['const'].nunique()} films, "
                    f"{self.directors_df['nconst'].nunique()} unique directors")
        log_message(f"✅ Writers linked: {self.writers_df['const'].nunique()} films, "
                    f"{self.writers_df['nconst'].nunique()} unique writers")

        # Load & merge DDD
        self.ddd = load_ddd()
        if not self.ddd.empty:
            before = len(self.df)
            self.df = self.df.merge(self.ddd, on="const", how="left")
            log_message(f"✅ DDD merged on tconst: {self.ddd.shape[1]-1} features, "
                        f"{self.ddd['const'].nunique()} titles")
        else:
            log_message("⚠️ DDD not merged (file missing).", level="WARNING")

        # Export strict tables for audit
        export_dir = self.batch_dir / "exports"
        export_dir.mkdir(parents=True, exist_ok=True)
        self.actors_df.to_csv(export_dir / "actors_imdb_strict.csv", index=False)
        self.directors_df.to_csv(export_dir / "directors_imdb_strict.csv", index=False)
        self.writers_df.to_csv(export_dir / "writers_imdb_strict.csv", index=False)

        return self

    # -------------------- Validation Pipeline --------------------
    def validate_pipeline(self):
        """Audit data engineering: detect gaps, garbage, and coverage issues."""
        audit_dir = self.batch_dir / "audits"
        audit_dir.mkdir(parents=True, exist_ok=True)

        # A) Titles with zero actors
        zero_cast = sorted(set(self.df["const"]) - set(self.actors_df["const"]))
        pd.DataFrame({"const": zero_cast}).to_csv(audit_dir / "titles_without_cast.csv", index=False)
        log_message(f"🔍 Titles without cast: {len(zero_cast)}")

        # B) Actors that look suspicious
        sus = self.actors_df[
            self.actors_df["actor"].str.fullmatch(r"\d{3,4}") | 
            self.actors_df["actor"].str.fullmatch(r"[A-Za-z]")
        ].copy()
        sus.to_csv(audit_dir / "suspicious_actor_tokens.csv", index=False)
        if not sus.empty:
            log_message(f"⚠️ Suspicious actor tokens: {len(sus)} (see audits/suspicious_actor_tokens.csv)")

        # C) DDD coverage
        if not self.ddd.empty:
            covered = set(self.ddd["const"])
            miss = sorted(set(self.df["const"]) - covered)
            pd.DataFrame({"const": miss}).to_csv(audit_dir / "ddd_missing_titles.csv", index=False)
            log_message(f"🔍 DDD missing: {len(miss)}/{len(self.df)} titles")

        log_message(f"✅ Audits written to {audit_dir.relative_to(project_root)}")
        return self

    # -------------------- VIZ 1 --------------------
    def viz_1_top_actors_leaderboard(self):
        log_message("\n📊 Creating Visualization 1: Top 30 Actors Leaderboard")

        if self.actors_df.empty:
            fig, ax = plt.subplots(figsize=(16, 12))
            ax.text(0.5, 0.5,
                    'No cast data detected.\nCheck exports/cast_raw_samples.csv to inspect raw values.',
                    ha='center', va='center', fontsize=16, transform=ax.transAxes)
            ax.set_title('Top 30 Actors (Cast Missing)', fontsize=16, fontweight='bold', pad=20)
            ax.axis('off')
            save_figure(fig, '01_top_actors_leaderboard.png', batch_number=2)
            plt.close()
            return self

        # Filter for male actors only (gender=2)
        males = self.actors_df[self.actors_df["gender"] == 2]
        if males.empty:
            log_message("⚠️ No male actors detected; showing all actors", level="WARNING")
            males = self.actors_df
        
        left = males.merge(self.df[["const", "imdb_rating"]], on="const", how="left")
        stats = (left.groupby("actor")
                      .agg(Count=("const", "nunique"),
                           Avg_Rating=("imdb_rating", "mean"))
                      .reset_index()
                      .sort_values(["Count", "Avg_Rating"], ascending=[False, False])
                      .head(30))

        fig, ax = plt.subplots(figsize=(16, 10))
        ax.barh(stats["actor"][::-1], stats["Count"][::-1], color="#6ab7ff", edgecolor="black")
        for y, (cnt, r) in enumerate(zip(stats["Count"][::-1], stats["Avg_Rating"][::-1])):
            ax.text(cnt + 0.3, y, f"{cnt} • {r:.2f}", va="center", fontsize=10)
        ax.set_xlabel("Number of Films")
        ax.set_title("Top 30 Actors You Watch Most (with Avg IMDb Rating)", fontweight="bold")
        plt.tight_layout()
        save_figure(fig, '01_top_actors_leaderboard.png', batch_number=2)
        plt.close()
        return self

    # -------------------- VIZ 2 --------------------
    def viz_2_top_actresses_leaderboard(self):
        log_message("📊 Creating Visualization 2: Top 30 Actresses Leaderboard")

        if self.actors_df.empty:
            fig, ax = plt.subplots(figsize=(16, 12))
            ax.text(0.5, 0.5, 'No cast data detected.',
                    ha='center', va='center', fontsize=16, transform=ax.transAxes)
            ax.set_title('Top 30 Actresses (Cast Missing)', fontsize=16, fontweight='bold', pad=20)
            ax.axis('off')
            save_figure(fig, '02_top_actresses_leaderboard.png', batch_number=2)
            plt.close()
            return self

        # Filter for female actors only (gender=1)
        females = self.actors_df[self.actors_df["gender"] == 1]
        if females.empty:
            log_message("⚠️ No female actors detected; showing all actors", level="WARNING")
            females = self.actors_df

        left = females.merge(self.df[["const", "imdb_rating"]], on="const", how="left")
        stats = (left.groupby("actor")
                      .agg(Count=("const", "nunique"),
                           Avg_Rating=("imdb_rating", "mean"))
                      .reset_index()
                      .sort_values(["Count", "Avg_Rating"], ascending=[False, False])
                      .head(30))

        fig, ax = plt.subplots(figsize=(16, 10))
        ax.barh(stats["actor"][::-1], stats["Count"][::-1], color="#ff6b9d", edgecolor="black")
        for y, (cnt, r) in enumerate(zip(stats["Count"][::-1], stats["Avg_Rating"][::-1])):
            ax.text(cnt + 0.3, y, f"{cnt} • {r:.2f}", va="center", fontsize=10)
        ax.set_xlabel("Number of Films")
        ax.set_title("Top 30 Actresses You Watch Most (with Avg IMDb Rating)", fontweight="bold")
        plt.tight_layout()
        save_figure(fig, '02_top_actresses_leaderboard.png', batch_number=2)
        plt.close()
        return self

    # -------------------- VIZ 3 --------------------
    def viz_3_actor_network_interactive(self):
        log_message("📊 Creating Visualization 3: Actor Network (Interactive HTML)")

        html_path = self.batch_dir / '03_actor_network_interactive.html'
        export_dir = self.batch_dir / "exports"
        export_dir.mkdir(parents=True, exist_ok=True)

        if self.actors_df.empty:
            html = "<h1>Actor Network</h1><p>No cast detected; nothing to render.</p>"
            html_path.write_text(html, encoding="utf-8")
            log_message(f"  ✅ HTML saved: {html_path.name}")
            return self

        # Build co-appearance counts
        pairs = Counter()
        for _, grp in self.actors_df.groupby("const"):
            actors = sorted(grp["actor"].unique())
            for i in range(len(actors)):
                for j in range(i+1, len(actors)):
                    pairs[(actors[i], actors[j])] += 1

        # Export top pairs
        top_edges = pairs.most_common(500)
        pd.DataFrame([{"actor_a": a, "actor_b": b, "co_appearances": w}
                      for (a, b), w in top_edges]).to_csv(export_dir / "actor_pairs_top500.csv", index=False)

        # Minimal HTML table
        rows = "\n".join(
            f"<tr><td>{a}</td><td>{b}</td><td>{w}</td></tr>"
            for (a, b), w in top_edges[:200]
        )
        html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Actor Collaboration Network</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 20px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; }}
    th {{ background: #f2f2f2; }}
  </style>
</head>
<body>
  <h1>🎬 Actor Collaboration Network (Top 200 links)</h1>
  <p>CSV export: <em>actor_pairs_top500.csv</em></p>
  <table>
    <tr><th>Actor A</th><th>Actor B</th><th>Co-appearances</th></tr>
    {rows}
  </table>
</body>
</html>
"""
        html_path.write_text(html, encoding="utf-8")
        log_message(f"  ✅ HTML saved: {html_path.name}")
        return self

    # -------------------- VIZ 3B --------------------
    def viz_3b_top_costar_pairs(self):
        log_message("📊 Creating Visualization 3B: Top Co-Star Pairs")
        if self.actors_df.empty:
            fig, ax = plt.subplots(figsize=(14, 10))
            ax.text(.5, .5, "Cast missing; cannot compute co-star pairs.", ha="center", va="center", fontsize=16)
            ax.axis("off")
            ax.set_title("Top Co-Star Pairs")
            save_figure(fig, '03b_top_costar_pairs.png', batch_number=2)
            plt.close()
            return self

        pairs = Counter()
        for _, grp in self.actors_df.groupby("const"):
            actors = sorted(grp["actor"].unique())
            for i in range(len(actors)):
                for j in range(i+1, len(actors)):
                    pairs[(actors[i], actors[j])] += 1

        top30 = pairs.most_common(30)
        labels = [f"{a} & {b}" for (a, b), _ in top30]
        values = [w for _, w in top30]

        fig, ax = plt.subplots(figsize=(16, 12))
        ax.barh(labels[::-1], values[::-1], color="#c5e1a5", edgecolor="black")
        ax.set_title("Top Co-Star Pairs (by Co-appearances)", fontweight="bold")
        ax.set_xlabel("Co-appearances")
        plt.tight_layout()
        save_figure(fig, '03b_top_costar_pairs.png', batch_number=2)
        plt.close()
        return self

    # -------------------- VIZ 4 --------------------
    def viz_4_genre_combination_heatmap(self):
        log_message("📊 Creating Visualization 4: Genre Combination Heatmap")
        genre_exploded = explode_genres(self.df)

        from collections import Counter as C2
        pair_counts = C2()
        by_const = genre_exploded.groupby("const")["genre"].apply(lambda s: sorted(set(s)))

        for genres in by_const:
            if len(genres) < 2:
                continue
            for i in range(len(genres)):
                for j in range(i + 1, len(genres)):
                    pair_counts[(genres[i], genres[j])] += 1

        top_genres = genre_exploded["genre"].value_counts().head(15).index.tolist()

        matrix = pd.DataFrame(0, index=top_genres, columns=top_genres, dtype=int)
        for (g1, g2), c in pair_counts.items():
            if g1 in top_genres and g2 in top_genres:
                matrix.loc[g1, g2] = c
                matrix.loc[g2, g1] = c

        fig, ax = plt.subplots(figsize=(16, 14))
        sns.heatmap(
            matrix, annot=True, fmt="d", cmap="Blues",
            cbar_kws={"label": "Number of Films"},
            linewidths=0.5, linecolor="white", ax=ax, square=True
        )
        ax.set_title(
            "Genre Combination Heatmap\n(Labels Normalized: Sci-Fi/Noir/TV Movie, etc.)",
            fontsize=16, fontweight="bold", pad=20
        )
        ax.set_xlabel("Genre", fontsize=14, fontweight="bold")
        ax.set_ylabel("Genre", fontsize=14, fontweight="bold")
        plt.tight_layout()
        save_figure(fig, "04_genre_combination_heatmap.png", batch_number=2)
        plt.close()
        return self

    # -------------------- VIZ 5 --------------------
    def viz_5_multi_genre_analysis(self):
        log_message("📊 Creating Visualization 5: Multi-Genre Analysis")

        self.df["genre_count_calc"] = self.df["genres"].apply(lambda x: len(parse_genres(x)) if pd.notna(x) else 0)
        self.df["genre_type"] = self.df["genre_count_calc"].apply(
            lambda x: "Single Genre" if x == 1 else
                      "Two Genres"   if x == 2 else
                      "3+ Genres"    if x >= 3 else "No Genre"
        )

        genre_type_stats = (self.df.groupby("genre_type")
                                .agg(Count=("const", "count"),
                                     Avg_Rating=("imdb_rating", "mean"))
                                .reset_index())

        order_all = ["Single Genre", "Two Genres", "3+ Genres", "No Genre"]
        present = [t for t in order_all if t in genre_type_stats["genre_type"].tolist()]
        genre_type_stats["genre_type"] = pd.Categorical(genre_type_stats["genre_type"], present, ordered=True)
        genre_type_stats = genre_type_stats.sort_values("genre_type")

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))

        colors_map = {'Single Genre': '#3498db', 'Two Genres': '#2ecc71', '3+ Genres': '#f39c12', 'No Genre': '#95a5a6'}
        cols = [colors_map.get(t, '#95a5a6') for t in genre_type_stats["genre_type"]]

        x_positions = np.arange(len(genre_type_stats))
        ax1.bar(x_positions, genre_type_stats["Count"].values, color=cols, edgecolor='black', linewidth=1.5, alpha=0.85)
        ax1.set_xticks(x_positions)
        ax1.set_xticklabels(genre_type_stats["genre_type"].astype(str).tolist(), rotation=0)
        ax1.set_ylabel('Number of Films', fontsize=14, fontweight='bold')
        ax1.set_title('Films by Genre Count', fontsize=16, fontweight='bold', pad=16)
        ax1.grid(True, alpha=0.3, axis='y')
        for i, count in enumerate(genre_type_stats["Count"].values):
            ax1.text(i, count, str(int(count)), ha='center', va='bottom', fontsize=11, fontweight='bold')

        ax2.bar(x_positions, genre_type_stats["Avg_Rating"].values, color=cols, edgecolor='black', linewidth=1.5, alpha=0.85)
        ax2.set_xticks(x_positions)
        ax2.set_xticklabels(genre_type_stats["genre_type"].astype(str).tolist(), rotation=0)
        ax2.set_ylabel('Average IMDb Rating', fontsize=14, fontweight='bold')
        ax2.set_title('Rating by Genre Count', fontsize=16, fontweight='bold', pad=16)
        ax2.grid(True, alpha=0.3, axis='y')
        ymin = max(0, min(5, genre_type_stats["Avg_Rating"].min() - 0.3))
        ymax = min(10, max(8, genre_type_stats["Avg_Rating"].max() + 0.3))
        ax2.set_ylim([ymin, ymax])
        for i, rating in enumerate(genre_type_stats["Avg_Rating"].values):
            ax2.text(i, rating, f'{rating:.2f}', ha='center', va='bottom', fontsize=11, fontweight='bold')

        plt.tight_layout()
        save_figure(fig, '05_multi_genre_analysis.png', batch_number=2)
        plt.close()
        return self

    # -------------------- VIZ 6 --------------------
    def viz_6_quality_by_genre_boxplot(self):
        log_message("📊 Creating Visualization 6: Quality by Genre (Box Plot)")
        gexp = explode_genres(self.df)
        top = gexp["genre"].value_counts().head(12).index.tolist()
        df_top = gexp[gexp["genre"].isin(top)].copy()
        fig, ax = plt.subplots(figsize=(16, 10))
        bp = ax.boxplot([df_top[df_top["genre"] == g]["imdb_rating"].dropna() for g in top],
                        labels=top, patch_artist=True, widths=0.6)
        for patch, genre in zip(bp["boxes"], top):
            patch.set_facecolor(GENRE_COLORS.get(genre, '#95a5a6')); patch.set_alpha(0.7)
        ax.set_xlabel('Genre', fontsize=14, fontweight='bold')
        ax.set_ylabel('IMDb Rating', fontsize=14, fontweight='bold')
        ax.set_title('Rating Distribution by Genre', fontsize=16, fontweight='bold', pad=16)
        ax.grid(True, alpha=0.3, axis='y')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        save_figure(fig, '06_quality_by_genre_boxplot.png', batch_number=2)
        plt.close()
        return self

    # -------------------- VIZ 7 --------------------
    def viz_7_genre_evolution_decades(self):
        log_message("📊 Creating Visualization 7: Genre Evolution Over Decades")
        gexp = explode_genres(self.df)
        top = gexp["genre"].value_counts().head(8).index.tolist()
        df_dec = gexp[(gexp["decade"].notna()) & (gexp["genre"].isin(top))].copy()
        pivot = df_dec.groupby(["decade", "genre"]).size().unstack(fill_value=0).sort_index()
        fig, ax = plt.subplots(figsize=(18, 10))
        pivot.plot.area(ax=ax, alpha=0.7, linewidth=2)
        ax.set_xlabel('Decade', fontsize=14, fontweight='bold')
        ax.set_ylabel('Number of Films', fontsize=14, fontweight='bold')
        ax.set_title('Genre Evolution Over Decades', fontsize=16, fontweight='bold', pad=16)
        ax.legend(title='Genre', fontsize=11, title_fontsize=12, loc='upper left')
        ax.grid(True, alpha=0.3)
        decades = [int(d) for d in pivot.index if not pd.isna(d)]
        ax.set_xticks(decades)
        ax.set_xticklabels([f"{d}s" for d in decades], rotation=45)
        plt.tight_layout()
        save_figure(fig, '07_genre_evolution_decades.png', batch_number=2)
        plt.close()
        return self

    # -------------------- VIZ 8 --------------------
    def viz_8_keywords_wordcloud(self):
        log_message("📊 Creating Visualization 8: Keywords/Themes Wordcloud")
        if "tmdb_keywords" not in self.df.columns:
            log_message("⚠️ No keyword column; using genres as proxy", level="WARNING")
            all_genres = []
            for _, r in self.df.iterrows():
                if pd.notna(r.get("genres")):
                    all_genres.extend(parse_genres(r["genres"]))
            counts = Counter(all_genres)
            fig, ax = plt.subplots(figsize=(16, 12))
            y = 0.9
            if counts:
                mx = max(counts.values())
                for i, (g, c) in enumerate(counts.most_common(20)):
                    size = 12 + (c / mx * 20)
                    color = GENRE_COLORS.get(g, '#95a5a6')
                    ax.text(0.1 + (i % 4) * 0.23, y - (i // 4) * 0.08,
                            f'{g}\n({c})',
                            fontsize=size, color=color, fontweight='bold',
                            ha='left', va='top')
            ax.set_title('Top 20 Genres (Keyword Proxy)', fontsize=16, fontweight='bold', pad=16)
            ax.axis('off')
            plt.tight_layout()
            save_figure(fig, '08_keywords_wordcloud.png', batch_number=2)
            plt.close()
            return self

        # Placeholder for real TMDB keyword parsing
        return self

    # -------------------- VIZ 9 --------------------
    def viz_9_theme_network_interactive(self):
        log_message("📊 Creating Visualization 9: Theme Network (Interactive HTML)")
        html_path = self.batch_dir / '09_theme_network_interactive.html'
        html_path.write_text("<h1>Theme Co-Occurrence Network</h1><p>Requires TMDB keywords for full graph.</p>",
                             encoding="utf-8")
        log_message(f"  ✅ HTML saved: {html_path.name}")
        return self

    # -------------------- VIZ 10 --------------------
    def viz_10_director_actor_collaboration(self):
        log_message("📊 Creating Visualization 10: Director–Actor Collaborations (IMDb-strict)")
        if self.actors_df.empty or self.directors_df.empty:
            _barh_pairs("Director–Actor Collaborations", [], '10_director_actor_collaboration.png')
            return self
        # Join on const, use resolved names
        left = (self.directors_df.merge(self.actors_df[["const", "nconst", "actor"]], on="const", how="inner")
                                .rename(columns={"nconst_x": "dir_id", "nconst_y": "act_id"}))
        pairs = [(r["director"], r["actor"]) for _, r in left.iterrows()]
        _barh_pairs("Top Director–Actor Collaborations (IMDb-strict)", pairs, '10_director_actor_collaboration.png')
        return self

    def viz_10b_writer_actor_collaboration(self):
        log_message("📊 Creating Visualization 10B: Writer–Actor Collaborations (IMDb-strict)")
        if self.actors_df.empty or self.writers_df.empty:
            _barh_pairs("Writer–Actor Collaborations", [], '10b_writer_actor_collaboration.png')
            return self
        left = (self.writers_df.merge(self.actors_df[["const", "nconst", "actor"]], on="const", how="inner")
                              .rename(columns={"nconst_x": "wri_id", "nconst_y": "act_id"}))
        pairs = [(r["writer"], r["actor"]) for _, r in left.iterrows()]
        _barh_pairs("Top Writer–Actor Collaborations (IMDb-strict)", pairs, '10b_writer_actor_collaboration.png',
                    color="#a5d6a7")
        return self


    # -------------------- VIZ 11 --------------------
    def viz_11_content_warnings_ddd(self):
        log_message("📊 Creating Visualization 11: Content Warnings (DDD)")
        dcols = [c for c in self.df.columns if c.startswith("ddd_")]
        if not dcols:
            fig, ax = plt.subplots(figsize=(16, 10))
            ax.text(0.5, 0.5, "No DDD columns present.", ha="center", va="center", fontsize=16)
            ax.axis("off")
            ax.set_title("Content Warnings (DDD)")
            save_figure(fig, '11_content_warnings_ddd.png', batch_number=2)
            plt.close()
            return self

        boolish = {}
        for c in dcols:
            s = self.df[c]
            if s.dropna().isin([0, 1, True, False, "0", "1", "true", "false"]).mean() > 0.8:
                val = (s.astype(str).str.lower().isin(["1", "true", "t", "yes"])).sum()
                boolish[c] = int(val)
        if not boolish:
            fig, ax = plt.subplots(figsize=(16, 10))
            ax.text(0.5, 0.5, "DDD present but not boolean-style warnings.", ha="center", va="center", fontsize=16)
            ax.axis("off")
            ax.set_title("Content Warnings (DDD)")
            save_figure(fig, '11_content_warnings_ddd.png', batch_number=2)
            plt.close()
            return self

        items = sorted(boolish.items(), key=lambda x: x[1], reverse=True)[:20]
        fig, ax = plt.subplots(figsize=(16, 10))
        ax.barh([k for k, _ in items][::-1], [v for _, v in items][::-1], color="#ffcc80", edgecolor="black")
        ax.set_title("Most Common Content Warnings (DDD)")
        ax.set_xlabel("Count of Films Flagged")
        plt.tight_layout()
        save_figure(fig, '11_content_warnings_ddd.png', batch_number=2)
        plt.close()
        return self

    # -------------------- VIZ 12 --------------------
    def viz_12_emotional_content_ddd(self):
        log_message("📊 Creating Visualization 12: Emotional Content (DDD)")
        fig, ax = plt.subplots(figsize=(16, 10))
        ax.text(0.5, 0.5, "Add your DDD emotional columns map when available.", ha="center", va="center", fontsize=16)
        ax.axis("off")
        ax.set_title("Emotional Content (DDD)")
        save_figure(fig, '12_emotional_content_ddd.png', batch_number=2)
        plt.close()
        return self

    # -------------------- VIZ 13 --------------------
    def viz_13_rating_distribution_by_genre_violin(self):
        log_message("📊 Creating Visualization 13: Rating Distribution by Genre (Violin)")
        gexp = explode_genres(self.df)
        top = gexp["genre"].value_counts().head(10).index.tolist()
        df_top = gexp[(gexp["genre"].isin(top)) & (gexp["imdb_rating"].notna())].copy()
        fig, ax = plt.subplots(figsize=(16, 10))
        parts = ax.violinplot([df_top[df_top["genre"] == g]["imdb_rating"].values for g in top],
                              positions=range(len(top)),
                              widths=0.7, showmeans=True, showmedians=True)
        for pc, genre in zip(parts["bodies"], top):
            pc.set_facecolor(GENRE_COLORS.get(genre, '#95a5a6')); pc.set_alpha(0.6)
        ax.set_xticks(range(len(top)))
        ax.set_xticklabels(top, rotation=45, ha='right')
        ax.set_ylabel('IMDb Rating', fontsize=14, fontweight='bold')
        ax.set_title('Rating Distribution by Genre (Violin)', fontsize=16, fontweight='bold', pad=16)
        ax.grid(True, alpha=0.3, axis='y')
        plt.tight_layout()
        save_figure(fig, '13_rating_distribution_violin.png', batch_number=2)
        plt.close()
        return self

    # -------------------- VIZ 14 --------------------
    def viz_14_actor_diversity_score(self):
        log_message("📊 Creating Visualization 14: Actor Diversity Score")
        fig, ax = plt.subplots(figsize=(16, 10))
        if self.actors_df.empty:
            ax.text(0.5, 0.5, "No cast detected; cannot compute diversity.", ha="center", va="center", fontsize=16)
            ax.axis("off")
        else:
            merged = self.actors_df.merge(self.df[["const", "decade"]], on="const", how="left")
            actors_per_decade = merged.groupby("decade")["actor"].nunique()
            films_per_decade = merged.groupby("decade")["const"].nunique()
            stat = (actors_per_decade / films_per_decade * 100.0).dropna()
            ax.plot(stat.index.astype(int), stat.values, marker="o", linewidth=3)
            ax.set_xlabel("Decade", fontsize=12, fontweight="bold")
            ax.set_ylabel("Unique Actors per 100 Films", fontsize=12, fontweight="bold")
            ax.set_title("Actor Diversity Over Time (proxy)", fontweight="bold")
            ax.grid(True, alpha=.3)
        plt.tight_layout()
        save_figure(fig, '14_actor_diversity_score.png', batch_number=2)
        plt.close()
        return self

    # -------------------- VIZ 15 --------------------
    def viz_15_genre_explorer_interactive(self):
        log_message("📊 Creating Visualization 15: Genre Explorer (Interactive HTML)")

        gexp = explode_genres(self.df)
        genre_stats = (gexp.groupby("genre")
                        .agg(Count=("const", "count"),
                             Avg_Rating=("imdb_rating", "mean"),
                             Avg_Runtime=("runtime_mins", "mean"),
                             First_Year=("year", "min"))
                        .reset_index()
                        .sort_values("Count", ascending=False))

        def fmt_float(v, nd=2):
            try:
                return "" if pd.isna(v) else f"{float(v):.{nd}f}"
            except Exception:
                return ""

        def fmt_int_or_blank(v):
            try:
                return "" if pd.isna(v) else str(int(v))
            except Exception:
                return ""

        row_html = []
        for _, r in genre_stats.iterrows():
            row_html.append(
                "<tr>"
                f"<td>{r['genre']}</td>"
                f"<td>{fmt_int_or_blank(r['Count'])}</td>"
                f"<td>{fmt_float(r['Avg_Rating'], 2)}</td>"
                f"<td>{fmt_float(r['Avg_Runtime'], 1)}</td>"
                f"<td>{fmt_int_or_blank(r['First_Year'])}</td>"
                "</tr>"
            )
        rows = "\n".join(row_html)

        html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Interactive Genre Explorer (Lite)</title>
<style>
    body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
    h1 {{ text-align: center; color: #2c3e50; }}
    table {{ border-collapse: collapse; width: 100%; background: #fff; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; }}
    th {{ background: #f2f2f2; }}
</style>
</head>
<body>
<h1>🎬 Interactive Genre Explorer</h1>
<p style="text-align:center;color:#666;">
    Quick overview of genre popularity, average rating and runtime, and earliest year.
</p>
<table>
    <tr><th>Genre</th><th>Count</th><th>Avg Rating</th><th>Avg Runtime</th><th>First Year</th></tr>
    {rows}
</table>
</body>
</html>
"""
        out = self.batch_dir / "15_genre_explorer_interactive.html"
        out.write_text(html, encoding="utf-8")
        log_message(f"  ✅ HTML saved: {out.name}")
        return self

    # -------------------- VIZ 16 --------------------
    def viz_16_gender_balance(self):
        log_message("📊 Creating Visualization 16: Cast Gender Balance")
        fig, ax = plt.subplots(figsize=(8, 8))
        if self.actors_df.empty or "gender" not in self.actors_df.columns:
            ax.text(0.5, 0.5, "Gender metadata not available.", ha="center", va="center", fontsize=14)
            ax.axis("off")
            ax.set_title("Cast Gender Balance")
            save_figure(fig, '16_cast_gender_balance.png', batch_number=2)
            plt.close()
            return self

        counts = self.actors_df["gender"].map({1: "Female", 2: "Male"}).fillna("Unknown").value_counts()
        ax.pie(counts.values, labels=counts.index, autopct='%1.1f%%', startangle=90)
        ax.set_title("Cast Gender Balance (all film–actor links)")
        ax.axis('equal')
        plt.tight_layout()
        save_figure(fig, '16_cast_gender_balance.png', batch_number=2)
        plt.close()
        return self

    # -------------------- VIZ 17 --------------------
    def viz_17_largest_casts(self):
        log_message("📊 Creating Visualization 17: Largest Cast Ensembles")
        fig, ax = plt.subplots(figsize=(16, 12))
        if self.actors_df.empty:
            ax.text(0.5, 0.5, "Cast missing; cannot compute largest ensembles.", ha="center", va="center", fontsize=16)
            ax.axis("off")
            ax.set_title("Largest Cast Ensembles")
            save_figure(fig, '17_largest_cast_ensembles.png', batch_number=2)
            plt.close()
            return self

        sizes = self.actors_df.groupby("const")["actor"].nunique().sort_values(ascending=False).head(20)
        titles = self.df.set_index("const").reindex(sizes.index)
        labels = titles.get("display_title",
                 titles.get("Title", pd.Series(index=sizes.index, dtype=str))).fillna("Untitled").tolist()

        ax.barh(labels[::-1], sizes.values[::-1], color="#90caf9", edgecolor="black")
        ax.set_title("Films with the Largest Casts (Top 20)", fontweight="bold")
        ax.set_xlabel("Unique Actors Detected")
        plt.tight_layout()
        save_figure(fig, '17_largest_ensembles.png', batch_number=2)  # filename kept friendly
        plt.close()
        return self

    # -------------------- Summary --------------------
    def export_canonical_actors_table(self):
        """Export canonical actor-film pairs table for use by other batches."""
        log_message("\n📦 Exporting canonical actors table for downstream batches...")
        
        if self.actors_df.empty:
            log_message("⚠️ Actors table empty; skipping export.", level="WARNING")
            return self
        
        # Build canonical format: actor_id, actor_name, gender, film_id, film_year, rating, genres
        canonical = self.actors_df.copy()
        
        # Rename nconst → actor_id, actor → actor_name
        canonical = canonical.rename(columns={
            "nconst": "actor_id",
            "actor": "actor_name",
            "const": "film_id"
        })
        
        # Merge with film metadata
        film_meta = self.df[["const", "imdb_rating", "year", "genres"]].copy()
        film_meta = film_meta.rename(columns={
            "const": "film_id",
            "imdb_rating": "rating",
            "year": "film_year"
        })
        
        canonical = canonical.merge(film_meta, on="film_id", how="left")
        
        # Select canonical columns in order
        canonical = canonical[[
            "actor_id", "actor_name", "gender",
            "film_id", "film_year", "rating", "genres"
        ]].drop_duplicates(subset=["actor_id", "film_id"]).reset_index(drop=True)
        
        # Sort for deterministic ordering
        canonical = canonical.sort_values(
            by=["actor_id", "film_year", "film_id"]
        ).reset_index(drop=True)
        
        # Export to parquet for flexibility
        cache_dir = project_root / "data" / "processed"
        cache_dir.mkdir(parents=True, exist_ok=True)
        
        out_path = cache_dir / "actors_master.parquet"
        canonical.to_parquet(out_path, index=False)
        
        log_message(f"✅ Exported canonical actors table: {out_path}")
        log_message(f"   Rows: {len(canonical):,} | Unique actors: {canonical['actor_id'].nunique():,}")
        log_message(f"   Columns: {', '.join(canonical.columns)}")
        
        return self

    def generate_summary_report(self):
        log_message("\n" + "=" * 80)
        log_message("BATCH 2 ANALYSIS SUMMARY")
        log_message("=" * 80)

        log_message(f"\nTotal Films Analyzed: {len(self.df):,}")

        gexp = explode_genres(self.df)
        log_message(f"\nUnique Genres: {gexp['genre'].nunique()}")
        log_message("Top 5 Genres:")
        for g, c in gexp["genre"].value_counts().head(5).items():
            log_message(f"  {g}: {c} films")

        if "genre_count_calc" in self.df.columns:
            multi_pct = (self.df["genre_count_calc"] > 1).sum() / len(self.df) * 100
            log_message(f"\nMulti-Genre Films: {multi_pct:.1f}%")

        if not self.actors_df.empty:
            log_message(f"\nActors detected: {self.actors_df['actor'].nunique()} "
                        f"across {self.actors_df['const'].nunique()} films")
            if "gender" in self.actors_df.columns:
                f_count = self.actors_df[self.actors_df["gender"] == 1]["actor"].nunique()
                m_count = self.actors_df[self.actors_df["gender"] == 2]["actor"].nunique()
                log_message(f"  (Gendered entries — female: {f_count}, male: {m_count}, "
                            f"unknown: {self.actors_df['gender'].isna().sum()})")

        log_message("\n✅ Batch 2 Analysis Complete!")
        return self

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

def main():
    print("\n" + "🎨" * 40)
    print("\n" + " " * 12 + "BATCH 2: CONTENT GENOME ANALYSIS")
    print(" " * 8 + "Deep Content Analysis with 15+ Visualizations")
    print("\n" + "🎨" * 40 + "\n")

    analysis = ContentGenomeAnalysis()
    analysis.load_data().validate_pipeline().export_canonical_actors_table()

    (analysis
        .viz_1_top_actors_leaderboard()
        .viz_2_top_actresses_leaderboard()
        .viz_3_actor_network_interactive()
        .viz_3b_top_costar_pairs()
        .viz_4_genre_combination_heatmap()
        .viz_5_multi_genre_analysis()
        .viz_6_quality_by_genre_boxplot()
        .viz_7_genre_evolution_decades()
        .viz_8_keywords_wordcloud()
        .viz_9_theme_network_interactive()
        .viz_10_director_actor_collaboration()
        .viz_10b_writer_actor_collaboration()
        .viz_11_content_warnings_ddd()
        .viz_12_emotional_content_ddd()
        .viz_13_rating_distribution_by_genre_violin()
        .viz_14_actor_diversity_score()
        .viz_15_genre_explorer_interactive()
        .viz_16_gender_balance()
        .viz_17_largest_casts()
        .generate_summary_report())

    print("\n" + "✨" * 40)
    print("\n" + " " * 6 + "BATCH 2 COMPLETE - VISUALIZATIONS CREATED!")
    print(" " * 10 + "Check analysis_outputs/visualizations/batch_2/")
    print("\n" + "✨" * 40 + "\n")
    return 0

if __name__ == "__main__":
    sys.exit(main())
