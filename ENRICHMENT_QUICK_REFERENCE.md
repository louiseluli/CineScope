# CineScope Enrichment Pipeline - Quick Reference

## Command Cheatsheet

### Full Pipeline Execution

```bash
# Run all enrichments in sequence (production)
python scripts/enrich/00_enrich_imdb.py
python scripts/enrich/01_enrich_tmdb.py
python scripts/enrich/02_enrich_omdb.py
python scripts/enrich/03_enrich_ddd.py
python scripts/enrich/04_enrich_cast.py
python scripts/enrich/05_enrich_wikidata.py
python scripts/merge_all_enriched.py
```

### Testing (Sample 10 Items)

```bash
python scripts/enrich/00_enrich_imdb.py --limit 10
python scripts/enrich/01_enrich_tmdb.py --limit 10
python scripts/enrich/02_enrich_omdb.py --limit 10
python scripts/enrich/03_enrich_ddd.py --limit 10
python scripts/enrich/04_enrich_cast.py --limit 10
python scripts/enrich/05_enrich_wikidata.py --limit 10
python scripts/merge_all_enriched.py
```

### Individual Enrichments (Resumable)

```bash
python scripts/enrich/01_enrich_tmdb.py            # Resume or continue
python scripts/enrich/02_enrich_omdb.py            # Resume or continue
python scripts/enrich/03_enrich_ddd.py             # Resume or continue
```

### Force Re-enrichment

```bash
python scripts/enrich/01_enrich_tmdb.py --force    # Delete and re-enrich all
python scripts/enrich/02_enrich_omdb.py --force
python scripts/enrich/03_enrich_ddd.py --force
python scripts/enrich/04_enrich_cast.py --force
python scripts/enrich/05_enrich_wikidata.py --force
```

---

## Data Files

### Inputs

- **IMDb TSV files** (required): `data/raw/title.*.tsv.gz`, `name.basics.tsv.gz`
- **Watched list** (required): `data/processed/master_verification_20251013.csv` or similar

### Outputs (Sequential)

```
data/processed/
├── 00_imdb_enriched_media.csv          (IMDb base layer - ~800 MB)
├── 01_tmdb_enriched_media.csv          (TMDb columns only)
├── 02_omdb_enriched_media.csv          (OMDb columns only)
├── 03_ddd_enriched_media.csv           (DDD columns only)
├── 04_cast_enriched_media.csv          (Cast columns only)
├── 05_wikidata_enriched_media.csv      (Wikidata columns only)
└── master_cinema_data.csv              (Final merged - watched only)
```

---

## API Rate Limits & Tracking

| API      | Limit              | Tracking                      | File                  |
| -------- | ------------------ | ----------------------------- | --------------------- |
| TMDb     | 500/day            | In-memory counter             | 01_enrich_tmdb.py     |
| OMDb     | 1,000/day          | `omdb_enrichment_status.json` | 02_enrich_omdb.py     |
| DDD      | No limit published | 1-sec delays                  | 03_enrich_ddd.py      |
| Wikidata | No limit           | 1-sec delays                  | 05_enrich_wikidata.py |

---

## Column Structure by File

### 00_imdb_enriched_media.csv (Base)

- `const`, `title_from_basics`, `original_title`, `year_started`, `year_ended`
- `runtime_minutes`, `genres`, `directors`, `writers`, `top_cast`
- `imdb_rating`, `number_of_votes`, `alternative_titles`

### 01_tmdb_enriched_media.csv (TMDb only)

- `const` + 100+ columns starting with `tmdb_`
- Examples: `tmdb_id`, `tmdb_title`, `tmdb_budget`, `tmdb_vote_average`, etc.

### 02_omdb_enriched_media.csv (OMDb only)

- `const` + columns: `omdb_title`, `omdb_rated`, `omdb_plot`, `omdb_metascore`
- `omdb_imdb_rating`, `omdb_rating_rotten_tomatoes`, `omdb_rating_metacritic`

### 03_ddd_enriched_media.csv (DDD only)

- `const`, `ddd_id`, `ddd_warning_topics` (JSON), `ddd_main_warnings`
- Individual topic columns: `ddd_dog_dies`, `ddd_violence`, `ddd_animal_abuse`, etc.

### 04_cast_enriched_media.csv (Cast only)

- `const` + `imdb_cast_ids`, `imdb_cast_characters`, `imdb_director_ids`
- `tmdb_cast` (list of dicts), `tmdb_directors`, `tmdb_writers`, etc.

### 05_wikidata_enriched_media.csv (Wikidata only)

- `const` + columns starting with `wd_`
- Examples: `wd_qid`, `wd_label`, `wd_box_office`, `wd_filming_locations`

### master_cinema_data.csv (Final)

- All relevant columns from 01-05 (selective merge)
- Plus derived: `decade`, `era`, `display_title`
- Rows: 2,289 (watched films only)

---

## Troubleshooting

### Missing Input File

```
Error: Input file not found
Solution: Run the previous enrichment script first
```

### API Rate Limit Hit

```
OMDb: Run again tomorrow (tracks daily limit in omdb_enrichment_status.json)
TMDb: Automatic rate limiting (500/day)
```

### Row Explosion in Merge

```
Error: Row count changed from X to Y after merges
Solution: Verify each enrichment has unique const (deduplication)
```

### Duplicate const in Enrichment

```
Check: python -c "import pandas as pd; df = pd.read_csv('FILE.csv'); print(df['const'].duplicated().sum())"
Fix: Run with --force to re-generate
```

---

## Performance Notes

- **IMDb enrichment**: ~10-15 minutes (depends on SSD speed for TSV parsing)
- **TMDb enrichment**: ~2-3 hours (500/day limit)
- **OMDb enrichment**: ~2-3 hours (1,000/day limit, rate limited)
- **DDD enrichment**: ~30 min (1-sec rate limiting)
- **Cast enrichment**: ~1-2 hours
- **Wikidata enrichment**: ~1-2 hours (1-sec rate limiting)
- **Merge**: < 1 minute

---

## Language Style

All scripts use **first-person language**:

- Docstrings: "enriches my movies"
- Logging: "my enriched data saved to"
- Comments: "my watched films"

This creates personal ownership feel for the movie collection.

---

## Key Architectural Principles

✅ **Modular Design** - Each enrichment = single API responsibility  
✅ **Column Isolation** - No accumulation of columns across steps  
✅ **Resumable Execution** - Interrupted scripts can continue  
✅ **Deduplication** - One row per const guaranteed  
✅ **Rate Limiting** - Respects API limits and volunteer services  
✅ **First-Person Language** - Personal touch throughout  
✅ **Selective Merging** - Final output only includes chosen columns

---

## Version History

- **Initial**: Accumulated columns across enrichments (❌ architectural flaw)
- **Restructured**: API-specific columns, modular design (✅ current)
  - IMDb base layer created
  - All enrichments (01-05) updated
  - Merge script compatible
  - Consistent first-person language

---

Last Updated: $(date)  
Status: Ready for production execution
