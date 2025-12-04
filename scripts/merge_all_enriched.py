#!/usr/bin/env python3
"""
CineScope - Merge ALL Enriched Data - WATCHED FILMS ONLY 
----------------------------------------------------------------
- Keeps ONLY titles from the watched ground-truth list (expected 2,289).
- Prevents row explosion by collapsing enrichment files to ONE ROW PER `const`.
- Adds decade/era + display_title, and writes the final watched-only master.

Run from project root:
  PYTHONPATH=./src python scripts/merge_all_enriched.py
"""

from __future__ import annotations
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

# ------------------------------------------------------------------------------
# Project paths
# ------------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
SRC_DIR = PROJECT_ROOT / "src"
if SRC_DIR.exists() and str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from src.core.config import (  # type: ignore
    PROCESSED_DATA_DIR, WATCHED_CSV,
    TMDB_ENRICHED, OMDB_ENRICHED, DDD_ENRICHED,
    CAST_ENRICHED, WIKIDATA_ENRICHED,
    WATCHED_ONLY_DATA
)

# ------------------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------------------

def _read_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    # normalize key column name if needed
    for k in ("const", "Const", "tconst", "imdb_id"):
        if k in df.columns:
            if k != "const":
                df = df.rename(columns={k: "const"})
            break
    if "const" not in df.columns:
        raise ValueError(f"{path.name}: no `const` column found.")
    df["const"] = df["const"].astype(str)
    return df


def _ensure_one_row_per_const(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """
    Collapse to unique const rows.
    - String columns: join unique non-null values with ' | '
    - Numeric columns: first non-null
    This guarantees 1 row / const and avoids row explosions on merge.
    """
    if df.empty:
        return df

    df = df.copy()
    df["const"] = df["const"].astype(str)

    # Quick path: if already unique
    if not df["const"].duplicated().any():
        return df

    # Build aggregation rules dynamically
    agg: Dict[str, object] = {"const": "first"}
    for col in df.columns:
        if col == "const":
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            agg[col] = lambda s: s.dropna().iloc[0] if s.dropna().size else np.nan
        else:
            agg[col] = lambda s: " | ".join(sorted(set([str(x) for x in s.dropna().astype(str) if str(x).strip()]))) or np.nan

    collapsed = df.groupby("const", as_index=False).agg(agg)

    # Report a few examples of collapsed ids
    dup_ids = df.loc[df["const"].duplicated(keep=False), "const"].unique()
    print(f"   ℹ️  {source_name}: collapsed {len(dup_ids)} duplicated consts to 1 row each")

    return collapsed


def _merge_safely(left: pd.DataFrame, right: pd.DataFrame, right_cols: List[str],
                  label: str) -> pd.DataFrame:
    """
    Merge right onto left by 'const' with guardrails:
    - Assert left has unique const
    - Assert right has unique const (after calling _ensure_one_row_per_const)
    - Check row count pre/post-merge
    """
    left = left.copy()
    if left["const"].duplicated().any():
        raise AssertionError("Ground truth table has duplicate consts; please de-dup watched list first.")

    right = right[["const"] + [c for c in right_cols if c != "const"]].copy()

    pre = len(left)
    out = left.merge(right, on="const", how="left")

    post = len(out)
    if post != pre:
        # This should never happen because right is unique by const
        raise AssertionError(
            f"Row explosion detected after merging {label}: {pre} -> {post}. "
            "Verify right table is unique by const."
        )
    print(f"   ✅ Merged {label}: {len(right_cols) - 1 if 'const' in right_cols else len(right_cols)} columns")
    return out


def _add_decade_era(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    year_col = "Year" if "Year" in df.columns else ("year" if "year" in df.columns else None)
    if year_col:
        df[year_col] = pd.to_numeric(df[year_col], errors="coerce")
        df["decade"] = (df[year_col] // 10 * 10).astype("Int64")

        def era(y: float | int | None):
            if pd.isna(y):
                return np.nan
            y = int(y)
            if y < 1927:  return "Silent Era (pre-1927)"
            if y < 1935:  return "Pre-Code (1927-1934)"
            if y < 1960:  return "Golden Age (1935-1959)"
            if y < 1980:  return "New Hollywood (1960-1979)"
            if y < 2000:  return "Blockbuster Era (1980-1999)"
            if y < 2010:  return "Digital Age (2000-2009)"
            if y < 2020:  return "Modern (2010-2019)"
            return "Current (2020+)"
        df["era"] = df[year_col].apply(era)
    return df


def _add_display_title(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if {"Original Title", "Title"} <= set(df.columns):
        def make_title(row):
            orig = row.get("Original Title")
            eng = row.get("Title")
            if pd.isna(orig) or orig == eng:
                return eng if pd.notna(eng) else "Unknown"
            return f"{orig} ({eng})"
        df["display_title"] = df.apply(make_title, axis=1)
    elif "title" in df.columns:
        df["display_title"] = df["title"]
    return df


# ------------------------------------------------------------------------------
# Main merge
# ------------------------------------------------------------------------------

def merge_all_enriched_data() -> pd.DataFrame:
    print("\n" + "=" * 80)
    print("MERGING ENRICHED DATA - WATCHED FILMS ONLY!")
    print("=" * 80 + "\n")

    # 1) Ground truth list (WATCHED)
    print("📥 Loading WATCHED movies list (GROUND TRUTH)...")
    watched = _read_csv(WATCHED_CSV)
    if "Const" in watched.columns:  # legacy
        watched = watched.rename(columns={"Const": "const"})
    # keep only distinct const
    watched = watched.drop_duplicates(subset=["const"])
    n_watched = len(watched)
    print(f"   ✅ Ground truth: {n_watched:,} WATCHED films")

    # 2) TMDB (optional)
    if TMDB_ENRICHED.exists():
        print("\n📥 TMDB enriched data...")
        tmdb = _read_csv(TMDB_ENRICHED)
        tmdb = tmdb[tmdb["const"].isin(set(watched["const"]))]
        tmdb = _ensure_one_row_per_const(tmdb, "TMDB")
        # choose only columns not already present
        tmdb_cols = ["const"] + [c for c in tmdb.columns if c != "const" and c not in watched.columns]
        watched = _merge_safely(watched, tmdb, tmdb_cols, "TMDB")

    # 3) OMDb (optional)
    if OMDB_ENRICHED.exists():
        print("\n📥 OMDB enriched data...")
        omdb = _read_csv(OMDB_ENRICHED)
        omdb = omdb[omdb["const"].isin(set(watched["const"]))]
        omdb = _ensure_one_row_per_const(omdb, "OMDb")
        omdb_cols = ["const"] + [c for c in omdb.columns if c.startswith("omdb_") and c not in watched.columns]
        if len(omdb_cols) > 1:
            watched = _merge_safely(watched, omdb[omdb_cols], omdb_cols, "OMDb")

    # 4) DDD (optional)
    if DDD_ENRICHED.exists():
        print("\n📥 DDD enriched data...")
        ddd = _read_csv(DDD_ENRICHED)
        ddd = ddd[ddd["const"].isin(set(watched["const"]))]
        ddd = _ensure_one_row_per_const(ddd, "DDD")
        ddd_cols = ["const"] + [c for c in ddd.columns if c.startswith("ddd_") and c not in watched.columns]
        if len(ddd_cols) > 1:
            watched = _merge_safely(watched, ddd[ddd_cols], ddd_cols, "DDD")

    # 5) CAST / Principals (critical; often multi-row per const)
    if CAST_ENRICHED.exists():
        print("\n📥 CAST enriched data (IMPORTANT!)...")
        cast = _read_csv(CAST_ENRICHED)
        cast = cast[cast["const"].isin(set(watched["const"]))]
        cast = _ensure_one_row_per_const(cast, "CAST")
        # merge new columns only
        cast_cols = ["const"] + [c for c in cast.columns if c != "const" and c not in watched.columns]
        if len(cast_cols) > 1:
            watched = _merge_safely(watched, cast[cast_cols], cast_cols, "CAST")

    # 6) Wikidata (optional)
    if WIKIDATA_ENRICHED.exists():
        print("\n📥 Wikidata enriched data...")
        wiki = _read_csv(WIKIDATA_ENRICHED)
        wiki = wiki[wiki["const"].isin(set(watched["const"]))]
        wiki = _ensure_one_row_per_const(wiki, "Wikidata")
        wiki_cols = ["const"] + [c for c in wiki.columns if c != "const" and c not in watched.columns]
        if len(wiki_cols) > 1:
            watched = _merge_safely(watched, wiki[wiki_cols], wiki_cols, "Wikidata")

    # 7) Sanity: still 1 row per const and still watched count?
    if watched["const"].duplicated().any():
        dups = watched.loc[watched["const"].duplicated(), "const"].head(10).tolist()
        raise AssertionError(f"Post-merge duplicates remain for const(s): {dups[:10]}")
    if len(watched) != n_watched:
        raise AssertionError(f"Row count changed {n_watched} → {len(watched)} after merges. Guard failed.")

    # 8) Enrich with decade/era & display title
    print("\n🔢 Adding decade and era columns...")
    watched = _add_decade_era(watched)
    print("   ✅ Added 'decade' and 'era'")

    print("\n📝 Creating display_title...")
    watched = _add_display_title(watched)
    print("   ✅ Created display_title")

    # 9) Save
    WATCHED_ONLY_DATA.parent.mkdir(parents=True, exist_ok=True)
    watched.to_csv(WATCHED_ONLY_DATA, index=False)
    print(f"\n💾 Saved watched-only master to: {WATCHED_ONLY_DATA}")
    print(f"   Rows: {len(watched):,}  |  Cols: {watched.shape[1]}\n")

    print("=" * 80)
    print("✅ MERGE COMPLETE - WATCHED FILMS ONLY!")
    print("=" * 80 + "\n")
    return watched


if __name__ == "__main__":
    try:
        merge_all_enriched_data()
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ ERROR: {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)
