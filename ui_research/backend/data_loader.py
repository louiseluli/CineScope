#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
data_loader.py (watched-only, low-RAM)
------------------------------------------------
Loads IMDb (from Parquet cache if present) and ONLY the titles
in my Watched CSV, plus their linked people (cast/crew/directors/writers).

Why: avoids OOM ("zsh: killed") on large IMDb corpora while giving me
everything I need to research my watched films quickly.

Switch to full-corpus mode by setting env:
    CINESCOPE_FULL=1
"""

from __future__ import annotations
import json, os, time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
import numpy as np

# ---------- Config & Paths ----------

IMDB_DIR_CANDIDATES = [
    Path(__file__).resolve().parents[2] / "data" / "raw",  # Project root / data / raw
    Path("data/raw"),  # current directory / data / raw
    Path("data/imdb"),
    Path("data/IMDB"),
    Path("/mnt/data/imdb"),
    Path("/mnt/project/imdb"),
]

IMDB_FILES = {
    "basics": ["title.basics.tsv.gz", "title.basics.tsv"],
    "ratings": ["title.ratings.tsv.gz", "title.ratings.tsv"],
    "akas": ["title.akas.tsv.gz", "title.akas.tsv"],
    "crew": ["title.crew.tsv.gz", "title.crew.tsv"],
    "principals": ["title.principals.tsv.gz", "title.principals.tsv"],
    "names": ["name.basics.tsv.gz", "name.basics.tsv"],
}

CACHE_DIR = Path(__file__).resolve().parents[2] / "data" / "processed" / "imdb_cache"
MANIFEST = CACHE_DIR / "manifest.json"

WATCHED_CANDIDATES = [
    Path(os.environ.get("CINESCOPE_WATCHED_PATH", "")) if os.environ.get("CINESCOPE_WATCHED_PATH") else None,
    Path(__file__).resolve().parents[2] / "data" / "processed" / "master_media_list.csv",  # Master list (preferred)
    Path(__file__).resolve().parents[2] / "data" / "user" / "Watched-27-Oct.csv",
    Path("data/processed/master_media_list.csv"),  # Fallback to relative
    Path("/mnt/data/Watched-27-Oct.csv"),
    Path("data/user/Watched-27-Oct.csv"),
    Path("data/Watched-27-Oct.csv"),
    Path("Watched-27-Oct.csv"),
]
WATCHED_CANDIDATES = [p for p in WATCHED_CANDIDATES if p]  # drop None

KEEP_TITLE_TYPES = {"movie", "tvMovie", "tvSeries", "tvMiniSeries", "video", "tvSpecial"}

# Default to watched-only mode to prevent OOM
WATCHED_ONLY = os.environ.get("CINESCOPE_FULL", "0") not in {"1", "true", "TRUE", "yes", "YES"}

# ---------- Utilities ----------

def _first_existing(paths: List[Path]) -> Optional[Path]:
    for p in paths:
        if p and p.exists():
            return p.resolve()
    return None

def _imdb_dir() -> Path:
    p = _first_existing(IMDB_DIR_CANDIDATES)
    if not p:
        tried = "\n  - " + "\n  - ".join(map(str, IMDB_DIR_CANDIDATES))
        raise FileNotFoundError(f"Could not find IMDb directory. Tried:{tried}")
    print(f"🎞️ IMDb directory found at: {p}")
    return p

def _find_first(base: Path, candidates: List[str]) -> Path:
    for name in candidates:
        path = base / name
        if path.exists():
            return path
    raise FileNotFoundError(f"None of {candidates} found under {base}")

def _read_tsv(path: Path, **kw) -> pd.DataFrame:
    compression = "gzip" if path.suffix == ".gz" else "infer"
    return pd.read_csv(
        path, sep="\t", compression=compression, na_values="\\N",
        low_memory=False, quoting=3, on_bad_lines="skip", **kw
    )

def _normalize_year(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("Int64")

def _shortlist(seq, n=10):
    return list(seq)[:n] if isinstance(seq, (list, tuple, pd.Series)) else []

def _now_ts() -> float:
    return time.time()

# ---------- Cache Helpers ----------

def _source_mtimes(base: Path) -> Dict[str, float]:
    mt = {}
    for key, names in IMDB_FILES.items():
        src = _find_first(base, names)
        mt[key] = src.stat().st_mtime
    return mt

def _load_manifest() -> dict:
    if MANIFEST.exists():
        try:
            return json.loads(MANIFEST.read_text())
        except Exception:
            return {}
    return {}

def _save_manifest(d: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(d, indent=2))

def _cache_paths() -> Dict[str, Path]:
    return {k: (CACHE_DIR / f"{k}.parquet") for k in IMDB_FILES.keys()}

def _cache_valid(base: Path) -> bool:
    m = _load_manifest()
    if not m or m.get("imdb_base") != str(base):
        return False
    for k, p in _cache_paths().items():
        if not p.exists():
            return False
    src_mt = _source_mtimes(base)
    return src_mt == m.get("mtimes", {})

def _write_cache(dfs: Dict[str, pd.DataFrame], base: Path) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    for k, df in dfs.items():
        df.to_parquet(_cache_paths()[k], index=False)
    _save_manifest({
        "imdb_base": str(base),
        "mtimes": _source_mtimes(base),
        "created_at": _now_ts(),
    })

def _read_cache() -> Dict[str, pd.DataFrame]:
    return {k: pd.read_parquet(p) for k, p in _cache_paths().items()}

# ---------- IMDb Load (with cache) ----------

def load_imdb() -> Dict[str, pd.DataFrame]:
    base = _imdb_dir()

    if _cache_valid(base) and not os.environ.get("CINESCOPE_FORCE_REFRESH"):
        print("⚡ Using IMDb cache from", CACHE_DIR)
        dfs = _read_cache()
    else:
        print("🛠  Building IMDb cache (first run or sources changed)…")
        basics = _read_tsv(_find_first(base, IMDB_FILES["basics"]), dtype={
            "tconst": "string", "titleType": "string",
            "primaryTitle": "string", "originalTitle": "string",
            "isAdult": "Int8", "startYear": "string", "endYear": "string",
            "runtimeMinutes": "string", "genres": "string",
        })
        basics["startYear"] = _normalize_year(basics["startYear"])
        basics["endYear"] = _normalize_year(basics["endYear"])
        basics["runtimeMinutes"] = pd.to_numeric(basics["runtimeMinutes"], errors="coerce").astype("Int64")
        basics["genres"] = basics["genres"].fillna("")
        basics = basics[basics["titleType"].isin(KEEP_TITLE_TYPES)].copy()

        ratings = _read_tsv(_find_first(base, IMDB_FILES["ratings"]), dtype={
            "tconst": "string", "averageRating": "float32", "numVotes": "int32",
        })

        crew = _read_tsv(_find_first(base, IMDB_FILES["crew"]), dtype={
            "tconst": "string", "directors": "string", "writers": "string",
        })
        crew["directors"] = crew["directors"].fillna("").map(lambda s: [] if s == "" else s.split(","))
        crew["writers"]   = crew["writers"].fillna("").map(lambda s: [] if s == "" else s.split(","))

        principals = _read_tsv(_find_first(base, IMDB_FILES["principals"]), dtype={
            "tconst": "string", "ordering": "int32", "nconst": "string",
            "category": "string", "job": "string", "characters": "string",
        })
        principals["characters"] = principals["characters"].fillna("").map(
            lambda s: [] if s == "" else [s.strip("[]").strip('"').strip("'")]
        )

        akas = _read_tsv(_find_first(base, IMDB_FILES["akas"]), dtype={
            "titleId": "string", "ordering": "int32", "title": "string",
            "region": "string", "language": "string",
            "types": "string", "attributes": "string", "isOriginalTitle": "Int8",
        })

        names = _read_tsv(_find_first(base, IMDB_FILES["names"]), dtype={
            "nconst": "string", "primaryName": "string",
            "birthYear": "string", "deathYear": "string",
            "primaryProfession": "string", "knownForTitles": "string",
        })
        names["birthYear"] = _normalize_year(names["birthYear"])
        names["deathYear"] = _normalize_year(names["deathYear"])
        names["primaryProfession"] = names["primaryProfession"].fillna("")
        names["knownForTitles"] = names["knownForTitles"].fillna("")

        dfs = {
            "basics": basics, "ratings": ratings, "crew": crew,
            "principals": principals, "akas": akas, "names": names,
        }
        _write_cache(dfs, base)
        print("✅ IMDb cache written:", CACHE_DIR)

    return dfs

# ---------- Watched List ----------

def _watched_path() -> Path:
    p = _first_existing(WATCHED_CANDIDATES)
    if not p:
        tried = "\n  - " + "\n  - ".join(map(str, WATCHED_CANDIDATES))
        raise FileNotFoundError(f"Could not find Watched CSV. Tried:{tried}")
    return p

def load_watched() -> pd.DataFrame:
    path = _watched_path()
    df = pd.read_csv(path)
    
    # Check if this is the master list (has 'is_watched' column)
    if 'is_watched' in df.columns:
        # Master list format - use 'is_watched' flag
        df = df[df['is_watched'] == 1].copy()  # Only keep watched items
        # Map 'const' column to 'tconst' if needed
        if 'const' in df.columns and 'tconst' not in df.columns:
            df['tconst'] = df['const']
        df['tconst'] = df['tconst'].astype(str)
        df = df.dropna(subset=["tconst"]).drop_duplicates(subset=["tconst"]).copy()
        df["watched"] = True
        df = df[["tconst", "watched"]]
        return df
    
    # Fallback: original Watched CSV format
    possible = ["tconst", "const", "imdb_id", "IMDb ID", "url", "URL"]
    col = next((c for c in possible if c in df.columns), None)
    if not col:
        raise ValueError("Could not find a tconst-like column in watched CSV.")
    df["tconst"] = df[col].astype(str).str.extract(r"(tt\d+)", expand=False)
    df = df.dropna(subset=["tconst"]).drop_duplicates(subset=["tconst"]).copy()
    df["watched"] = True
    extra = [c for c in df.columns if c not in {"tconst", "watched"}]
    df = df[["tconst", "watched"] + extra]
    return df

# ---------- Build Rich Records (watched-only) ----------

def _derive_original_language(akas: pd.DataFrame) -> pd.Series:
    orig = akas[akas["isOriginalTitle"] == 1][["titleId", "language"]].copy()
    orig = orig.rename(columns={"titleId": "tconst"})
    lang_map = orig.groupby("tconst")["language"].agg(lambda s: s.dropna().iloc[0] if s.notna().any() else None)
    missing = set(akas["titleId"].unique()) - set(lang_map.index)
    if missing:
        mode_lang = (
            akas[akas["titleId"].isin(missing)]
            .groupby("titleId")["language"]
            .agg(lambda s: s.dropna().mode().iloc[0] if s.dropna().size else None)
        )
        mode_lang.index = mode_lang.index.rename("tconst")
        lang_map = pd.concat([lang_map, mode_lang])
    return lang_map

def _people_lookup(names: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    people = {}
    for r in names.itertuples(index=False):
        people[r.nconst] = {
            "nconst": r.nconst,
            "name": r.primaryName,
            "birthYear": int(r.birthYear) if pd.notna(r.birthYear) else None,
            "deathYear": int(r.deathYear) if pd.notna(r.deathYear) else None,
            "primaryProfession": [] if not r.primaryProfession else str(r.primaryProfession).split(","),
            "knownForTitles": [] if not r.knownForTitles else str(r.knownForTitles).split(","),
        }
    return people

def _attach_names(ids: List[str], people: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for n in ids or []:
        p = people.get(n)
        out.append({"nconst": n, "name": (p or {}).get("name")})
    return out

def build_records(datasets: Dict[str, pd.DataFrame], watched_df: pd.DataFrame):
    basics = datasets["basics"]
    ratings = datasets["ratings"]
    crew = datasets["crew"]
    principals = datasets["principals"]
    akas = datasets["akas"]
    names = datasets["names"]

    # Limit to watched titles if in watched-only mode (default)
    if WATCHED_ONLY:
        watched_set = set(watched_df["tconst"])
        basics = basics[basics["tconst"].isin(watched_set)].copy()
        ratings = ratings[ratings["tconst"].isin(watched_set)].copy()
        crew = crew[crew["tconst"].isin(watched_set)].copy()
        principals = principals[principals["tconst"].isin(watched_set)].copy()
        akas = akas[akas["titleId"].isin(watched_set)].copy()

    orig_lang = _derive_original_language(akas)

    titles = (
        basics.merge(ratings, how="left", on="tconst")
              .merge(crew, how="left", on="tconst")
    ).merge(orig_lang.rename("originalLanguage").to_frame(), how="left",
            left_on="tconst", right_index=True) \
     .merge(watched_df[["tconst", "watched"]], how="left", on="tconst")

    titles["watched"] = titles["watched"].fillna(False)

    # Reduce names to those referenced by watched principals/directors/writers
    if WATCHED_ONLY:
        used_nconst = set()
        # from principals
        used_nconst |= set(principals["nconst"].unique().tolist())
        # from crew lists
        for col in ("directors", "writers"):
            lists = crew[col].dropna().tolist()
            for lst in lists:
                used_nconst.update(lst)
        names = names[names["nconst"].isin(used_nconst)].copy()

    people_map = _people_lookup(names)

    # Principals → per-title cast/crew (ordered)
    principals_sorted = principals.sort_values(["tconst", "ordering"])
    grouped = principals_sorted.groupby("tconst")

    def pack_principals(g: pd.DataFrame) -> Dict[str, List[Dict[str, Any]]]:
        cast, crewx = [], []
        for r in g.itertuples(index=False):
            entry = {
                "nconst": r.nconst,
                "name": people_map.get(r.nconst, {}).get("name"),
                "category": r.category,
                "job": r.job if pd.notna(r.job) else None,
                "characters": r.characters if isinstance(r.characters, list) else [],
                "ordering": int(r.ordering),
            }
            if str(r.category).lower() in {"actor", "actress", "self"}:
                cast.append(entry)
            else:
                crewx.append(entry)
        return {"cast": _shortlist(cast, 10), "crew": crewx}

    packed = grouped.apply(pack_principals).to_dict()

    title_dict: Dict[str, Dict[str, Any]] = {}
    for row in titles.itertuples(index=False):
        tconst = row.tconst
        pp = packed.get(tconst, {"cast": [], "crew": []})
        directors = _attach_names(row.directors, people_map) if isinstance(row.directors, list) else []
        writers = _attach_names(row.writers, people_map) if isinstance(row.writers, list) else []

        title_dict[tconst] = {
            "tconst": tconst,
            "titleType": row.titleType,
            "primaryTitle": row.primaryTitle,
            "originalTitle": row.originalTitle,
            "isAdult": int(row.isAdult) if pd.notna(row.isAdult) else 0,
            "startYear": int(row.startYear) if pd.notna(row.startYear) else None,
            "endYear": int(row.endYear) if pd.notna(row.endYear) else None,
            "runtimeMinutes": int(row.runtimeMinutes) if pd.notna(row.runtimeMinutes) else None,
            "genres": [] if not row.genres else str(row.genres).split(","),
            "averageRating": float(row.averageRating) if pd.notna(row.averageRating) else None,
            "numVotes": int(row.numVotes) if pd.notna(row.numVotes) else None,
            "originalLanguage": row.originalLanguage if pd.notna(row.originalLanguage) else None,
            "watched": bool(row.watched),
            "directors": directors,
            "writers": writers,
            "cast": pp["cast"],
            "crew": pp["crew"],
        }

    # Filmography (sample) per person — restricted to watched titles in watched-only mode
    filmography_map: Dict[str, List[Dict[str, Any]]] = {}
    for r in principals.itertuples(index=False):
        filmography_map.setdefault(r.nconst, []).append({
            "tconst": r.tconst,
            "category": r.category,
            "job": r.job if pd.notna(r.job) else None,
            "ordering": int(r.ordering),
        })

    people_dict: Dict[str, Dict[str, Any]] = {}
    src_names = names if WATCHED_ONLY else datasets["names"]
    for row in src_names.itertuples(index=False):
        nconst = row.nconst
        filmos = filmography_map.get(nconst, [])
        filmos_sorted = sorted(filmos, key=lambda x: x["ordering"])
        top_sample = []
        for item in _shortlist(filmos_sorted, 25):
            t = title_dict.get(item["tconst"])
            top_sample.append({
                **item,
                "title": t["primaryTitle"] if t else None,
                "year": t["startYear"] if t else None,
                "titleType": t["titleType"] if t else None,
                "genres": t["genres"] if t else None,
                "rating": t["averageRating"] if t else None,
            })

        people_dict[nconst] = {
            "nconst": nconst,
            "name": row.primaryName,
            "birthYear": int(row.birthYear) if pd.notna(row.birthYear) else None,
            "deathYear": int(row.deathYear) if pd.notna(row.deathYear) else None,
            "primaryProfession": [] if not row.primaryProfession else str(row.primaryProfession).split(","),
            "knownForTitles": [] if not row.knownForTitles else str(row.knownForTitles).split(","),
            "sampleFilmography": top_sample,
        }

    # Indices (only for loaded titles)
    indices = {"byGenre": {}, "byYear": {}, "byLanguage": {}, "byName": {}}
    for tconst, rec in title_dict.items():
        for g in rec["genres"] or []:
            indices["byGenre"].setdefault(g, set()).add(tconst)
        y = rec.get("startYear")
        if y:
            indices["byYear"].setdefault(int(y), set()).add(tconst)
        lang = rec.get("originalLanguage")
        if lang:
            indices["byLanguage"].setdefault(lang, set()).add(tconst)

    for nconst, person in people_dict.items():
        nm = (person["name"] or "").strip().lower()
        if nm:
            indices["byName"].setdefault(nm, []).append(nconst)

    frames = {
        "titles_df": pd.DataFrame.from_dict(title_dict, orient="index"),
        "people_df": pd.DataFrame.from_dict(people_dict, orient="index"),
    }

    return title_dict, people_dict, indices, frames

# ---------- Public API ----------

def load_all_data() -> Dict[str, Any]:
    imdb = load_imdb()
    watched = load_watched()
    titles, people, indices, frames = build_records(imdb, watched)
    DATA = {
        "titles": titles,
        "people": people,
        "indices": indices,
        "frames": frames,
        "meta": {
            "counts": {
                "titles": len(titles),
                "people": len(people),
                "watched": int(sum(1 for t in titles.values() if t["watched"])),
            },
            "mode": "watched-only" if WATCHED_ONLY else "full",
            "imdb_cache": str(CACHE_DIR),
            "imdb_dir": str(_imdb_dir()),
            "watched_source": str(_watched_path()),
            "kept_title_types": sorted(KEEP_TITLE_TYPES),
        }
    }
    return DATA

def refresh_watched(DATA: Dict[str, Any]) -> Dict[str, Any]:
    """
    Reload watched CSV and update flags in memory fast.
    In watched-only mode, also ensure titles_df stays aligned.
    """
    df = load_watched()
    watched_set = set(df["tconst"].tolist())

    # If we're in watched-only mode, we need to keep only watched titles
    if WATCHED_ONLY:
        # Remove titles not in watched_set
        to_drop = [t for t in list(DATA["titles"].keys()) if t not in watched_set]
        for t in to_drop:
            DATA["titles"].pop(t, None)

        # If new watched titles appeared, we need a lightweight expand:
        # Reload IMDb cache and append only those new tconsts.
        new_titles = watched_set - set(DATA["titles"].keys())
        if new_titles:
            imdb = load_imdb()
            # Filter tables
            basics = imdb["basics"][imdb["basics"]["tconst"].isin(new_titles)].copy()
            if basics.empty:
                pass
            else:
                ratings = imdb["ratings"][imdb["ratings"]["tconst"].isin(new_titles)].copy()
                crew = imdb["crew"][imdb["crew"]["tconst"].isin(new_titles)].copy()
                principals = imdb["principals"][imdb["principals"]["tconst"].isin(new_titles)].copy()
                akas = imdb["akas"][imdb["akas"]["titleId"].isin(new_titles)].copy()
                orig_lang = _derive_original_language(akas)

                # people involved
                used_nconst = set(principals["nconst"].unique().tolist())
                for col in ("directors", "writers"):
                    lists = crew[col].dropna().tolist()
                    for lst in lists:
                        used_nconst.update(lst)
                names = imdb["names"][imdb["names"]["nconst"].isin(used_nconst)].copy()
                people_map = _people_lookup(names)

                # principals packing
                principals_sorted = principals.sort_values(["tconst", "ordering"])
                grouped = principals_sorted.groupby("tconst")
                def pack_principals(g: pd.DataFrame):
                    cast, crewx = [], []
                    for r in g.itertuples(index=False):
                        entry = {
                            "nconst": r.nconst,
                            "name": people_map.get(r.nconst, {}).get("name"),
                            "category": r.category,
                            "job": r.job if pd.notna(r.job) else None,
                            "characters": r.characters if isinstance(r.characters, list) else [],
                            "ordering": int(r.ordering),
                        }
                        if str(r.category).lower() in {"actor", "actress", "self"}:
                            cast.append(entry)
                        else:
                            crewx.append(entry)
                    return {"cast": _shortlist(cast, 10), "crew": crewx}
                packed = grouped.apply(pack_principals).to_dict()

                # build & append title records
                merged = (
                    basics.merge(ratings, how="left", on="tconst")
                          .merge(crew, how="left", on="tconst")
                ).merge(orig_lang.rename("originalLanguage").to_frame(), how="left",
                        left_on="tconst", right_index=True)

                for row in merged.itertuples(index=False):
                    tconst = row.tconst
                    pp = packed.get(tconst, {"cast": [], "crew": []})
                    directors = _attach_names(row.directors, people_map) if isinstance(row.directors, list) else []
                    writers = _attach_names(row.writers, people_map) if isinstance(row.writers, list) else []
                    DATA["titles"][tconst] = {
                        "tconst": tconst,
                        "titleType": row.titleType,
                        "primaryTitle": row.primaryTitle,
                        "originalTitle": row.originalTitle,
                        "isAdult": int(row.isAdult) if pd.notna(row.isAdult) else 0,
                        "startYear": int(row.startYear) if pd.notna(row.startYear) else None,
                        "endYear": int(row.endYear) if pd.notna(row.endYear) else None,
                        "runtimeMinutes": int(row.runtimeMinutes) if pd.notna(row.runtimeMinutes) else None,
                        "genres": [] if not row.genres else str(row.genres).split(","),
                        "averageRating": float(row.averageRating) if pd.notna(row.averageRating) else None,
                        "numVotes": int(row.numVotes) if pd.notna(row.numVotes) else None,
                        "originalLanguage": (orig_lang.to_dict().get(tconst) if isinstance(orig_lang, pd.Series) else None),
                        "watched": True,
                        "directors": directors,
                        "writers": writers,
                        "cast": pp["cast"],
                        "crew": pp["crew"],
                    }

        # Rebuild indices (cheap: watched set is small)
        indices = {"byGenre": {}, "byYear": {}, "byLanguage": {}, "byName": {}}
        for tconst, rec in DATA["titles"].items():
            for g in rec.get("genres") or []:
                indices["byGenre"].setdefault(g, set()).add(tconst)
            y = rec.get("startYear")
            if y:
                indices["byYear"].setdefault(int(y), set()).add(tconst)
            lang = rec.get("originalLanguage")
            if lang:
                indices["byLanguage"].setdefault(lang, set()).add(tconst)
            # names index (approx) from cast & directors/writers
            for p in (rec.get("cast") or []) + (rec.get("crew") or []) + (rec.get("directors") or []) + (rec.get("writers") or []):
                nm = (p.get("name") or "").strip().lower()
                nid = p.get("nconst")
                if nm and nid:
                    indices["byName"].setdefault(nm, []).append(nid)

        DATA["indices"] = indices
        # frames
        DATA["frames"]["titles_df"] = pd.DataFrame.from_dict(DATA["titles"], orient="index")

    # Always update watched flag & meta counts
    watched_set = set(df["tconst"])
    for tconst, rec in DATA["titles"].items():
        rec["watched"] = tconst in watched_set
    titles_df = DATA["frames"]["titles_df"]
    titles_df["watched"] = titles_df.index.map(lambda t: t in watched_set)
    DATA["meta"]["counts"]["watched"] = int(len(watched_set & set(DATA["titles"].keys())))
    DATA["meta"]["watched_source"] = str(_watched_path())
    return {"watched_count": DATA["meta"]["counts"]["watched"], "source": DATA["meta"]["watched_source"]}
