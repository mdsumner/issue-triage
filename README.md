# issue-triage

Read-only tools for understanding a GitHub issue tracker: an offline triage kit,
and **issuescope**, a static page that puts the whole tracker in DuckDB in your
browser. Nothing here writes to the tracker.

Default target: `OSGeo/gdal` (set the repository variable `TRACKER_REPO` to
point it elsewhere).

## issuescope (the page)

- **Search by description.** Describe the feature or bug you have in mind; BM25
  ranking over every issue and PR thread (titles, bodies and comments) finds the
  open issues it belongs with and the merged PRs that set the pattern.
- **Relationships.** For any thread: parsed `fixes #N` / `duplicate of #N` links,
  every `#N` mention in both directions, similar threads from its own key terms,
  and the full conversation.
- **Live.** The data files are a daily snapshot; "Refresh from GitHub" merges in
  everything changed since, from the public API (60 calls/hour without a token).
- **Calibration.** Mark results `+` / `-`; the page reports precision on your
  marks and can fit the ranking weights to them. Marks export as CSV.
- **SQL.** Every table is queryable, with presets (abandoned fix attempts, most
  cross-referenced issues, never-answered issues, BM25 by hand, ...).
- **Agent export.** The top results, with full bodies and conversations, as one
  Markdown file.
- **From my tracker.** A picker over the open items of a downstream wish-list
  tracker (default: `wishes.json` from mdsumner/osgeo-upstream, written by its
  `scripts/wishes.py`). Choosing one runs its search words; marks are kept per
  wish (query label `tracker #N`). Set `window.ISSUESCOPE_WISHES` to another
  URL, or to `""` to hide the picker.
- **Deep links.** `#q=<words>` runs a search, `#wish=<N>` runs tracker item N,
  `#live` refreshes from GitHub on load. The address bar follows each search,
  so a result list can be shared as a link.

Tables: `items`, `postings`, `vocab`, `links`, `xrefs`, `xin`, `corpus_stats`,
`live_log`, plus `bodies` and `comments` (loaded on first use).

### Deploy

1. Settings > Pages > Source: **GitHub Actions**.
2. Actions > harvest-and-deploy > Run workflow (it also runs daily and on push).

### Run locally

```sh
pip install pandas pyarrow duckdb scikit-learn
python triage/harvest.py --repo OSGeo/gdal --out snap      # needs an authenticated gh CLI
python build/build_data.py --snap snap --out site/data
cd site && python -m http.server 8000                      # then open http://localhost:8000
```

DuckDB-wasm and its parquet extension load from the jsDelivr and
extensions.duckdb.org CDNs. For fully offline use, set
`window.ISSUESCOPE_DUCKDB` to a local copy of the duckdb-wasm `dist/` folder and
`window.ISSUESCOPE_FORMAT = "arrow"` with Arrow IPC copies of the data files.

## triage (the offline kit)

- `triage/harvest.py` - snapshot issues, PRs and comments to Parquet (REST via `gh`)
- `triage/meta_sort.py` - metadata buckets for open issues (stale, never answered,
  waiting on reporter, needs funding, ...)
- `triage/dupes.py` - duplicate candidates by TF-IDF neighbours, with recall
  calibrated against issues already closed as duplicates

```sh
python triage/meta_sort.py --snap snap --out out
python triage/dupes.py --snap snap --out out --k 10 --min-score 0.12
```

Text matching is lexical: duplicates worded differently can be missed, and every
suggestion needs a maintainer's judgement.
