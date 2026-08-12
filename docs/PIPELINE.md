# Pipeline map — motor-learning citation network

This is the authoritative map of the full pipeline: every script, the files it reads and writes, its
key knobs, and — crucially — the handoffs that are **not** wired by filename in the current code.

Each stage is a self-contained intra-script [Hamilton](https://hamilton.dagworks.io/) DAG with a
`_main()` that reads and writes hardcoded paths resolved from `constants.py`
(`RAW_DATA_PATH=data/raw`, `PROCESSED_DATA_PATH=data/processed`,
`GRAPH_LEVEL_DATA_PATH=data/graph_level_data`, `FIGURES_PATH=reports/figures`). There is **no
`dvc.yaml`** yet, so nothing but this document records inter-script lineage. Every `_main()` also
writes Graphviz by-products to `reports/figures/{script}_all_functions.png` / `{script}.png` — these
are visualization artifacts, not pipeline data, and are omitted below.

> **Provenance of this map.** Originally derived by two exploration agents on 2026-07-27 (session
> `753581ca`) and reconciled against current code on 2026-08-12. The interactive version lives in the
> DAG artifact; keep the two in sync.

## Stage order (the spine)

```
raw exports (Scopus · Web of Science · PubMed/MEDLINE · EBSCO)
  → read_scopus / read_web_of_science / get_pubmed_dataset      → *_database.parquet
  → unify_datasets                                              → unified_database.parquet
  → clean_unified_datasets                                      → clean_unified_database.parquet
  → get_references → find_missing_citations                     → updated_references.parquet
  → build_citation_network                                      → citation_network_without_layout_updated_citations.graphml
  ⋯ [implicit] community detection + rename                     → citation_network_full_low_res.graphml / citation_network.graphml
  → get_network_communities_and_stats  (frozen, Track A)
  → community_quality_metrics                                   → per-community / per-partition parquet
  ⋯ [implicit] topic modelling + rename                         → citation_network_with_topics_new.graphml
  → topic_community_analysis / build_bcp_and_cocitation / detect_missing_links / build_website
```

The `⋯ [implicit]` steps are the handoffs the code does **not** connect by filename — see
[Implicit / manual handoffs](#implicit--manual-handoffs). They are why the pipeline is not yet
one-command reproducible.

## Ingestion & references

| Script | Reads | Writes | External | Key knobs |
|---|---|---|---|---|
| `read_scopus.py` | `raw/scopus_1895_2014.csv`, `raw/scopus_2015_2025.csv` | `processed/scopus_database.parquet` | — | CSV filenames (`@parameterize` L35-36); all cols → `str` |
| `read_web_of_science.py` | `raw/wos_core_collection/savedrecs(0..25).txt`, `raw/wos_biosis_ci/savedrecs(0..11).txt`, `raw/wos_kci/savedrecs(0).txt` | `processed/wos_database.parquet` | — | file counts `range(26)`/`range(12)`; `sep="\t"`, `quoting=3` |
| `get_pubmed_dataset.py` | `raw/articles.pkl` (LOCAL branch, if present) | `processed/medline_database.parquet`, `raw/articles.pkl`, `raw/medline_articles.txt`, `raw/pubmed_results.bib` | PubMed (`pymedx`), ONLINE branch only | PubMed `query` (L46); `max_results=17000`; LOCAL/ONLINE auto-switch on `articles.pkl` |
| `unify_datasets.py` | `scopus_database.parquet`, `wos_database.parquet`, `medline_database.parquet`, `raw/ebsco_ASU_all_until_2025.csv` | `processed/unified_database.parquet`, `processed/scopus_author_ids.parquet` | — | `unified_database_schema` (strict, DOI-lowercase); per-source rename maps |
| `clean_unified_datasets.py` | `unified_database.parquet` | `processed/clean_unified_database.parquet` | — | `EXECUTE`; dedup by `doi` (ffill/bfill, drop null/dup DOI) |
| `get_references.py` | `clean_unified_database.parquet`; `references_opencitations.parquet` + error file (resume) | `processed/references_opencitations.parquet`, `processed/error_references_opencitations.parquet` | **OpenCitations** API (default); Crossref (alt) | `REFERENCES_SOURCE=OPENCITATIONS` (L27); `EXECUTE`; `sleep(0.5)` rate-limit; auto-resume |
| `find_missing_citations.py` | `references_opencitations.parquet` | `processed/updated_references.parquet`, `processed/missing_openalex_ids.csv`, `…_errors.csv` | **OpenAlex** API (2 calls) | `EXECUTE`; `sleep(0.02/0.03)`; targets rows with `cited_dois` length 0 |
| `create_file_with_references.py` | (via imported DAGs) `raw/articles.pkl`, cached `get_references`; declared `processed/updated_references.pickle` | `processed/bibtex_with_references/pubmed_results.bib` | — | ⚠ `dr.execute` is **commented out** — validates/visualizes only. Declared input `updated_references.pickle` vs the real `.parquet` — mismatch |

## Network, communities & quality

| Script | Reads | Writes | Key knobs |
|---|---|---|---|
| `build_citation_network.py` | `clean_unified_database.parquet`, `updated_references.parquet` | `processed/citation_network_without_layout_updated_citations.graphml` (saver rewrites the `citation_network` stem) | `EXECUTE`; weak-giant-component + drop degree-0; **pickle/layout/`citation_network.graphml` savers are commented out of `outputs`**; ForceAtlas2 `iterations=500` (disabled path only) |
| `get_network_communities_and_stats.py` | `graph_level/citation_network.graphml`, `clean_unified_database.parquet` | `reports/figures/…_degree_distribution.png` only (active `outputs=["filtered_citation_network"]` returns `None`). **Commented-out savers** would write `graph_level/citation_network_full_low_res.graphml` + `clean_unified_database_with_communities_low_res.parquet` | **Frozen — Track A, do not modify.** `resolutions=[0.001..0.009]`, `seed=0`, `n_iterations=10`; low-degree cut = 20th percentile; Leiden `CPMVertexPartition` |
| `community_quality_metrics.py` *(added after the original map)* | `graph_level/citation_network_full_low_res.graphml` | `graph_level/citation_network_with_community_metrics.graphml`, `community_quality_metrics/community_quality_metrics_per_community.parquet`, `…_per_partition.parquet` | per-resolution quality metrics (modularity, CPM score, surprise, conductance, internal-edge surprise, cross-seed stability); `SUBSTANTIVE_COMMUNITY_MIN_SIZE=30` |

## Temporal & keyword branches

| Script | Reads | Writes | Key knobs |
|---|---|---|---|
| `process_network_by_time_periods.py` | `processed/citation_network_without_layout_updated_citations.graphml` (the `clean_unified_database.parquet` input is passed but unused) | `graph_level/citation_network_until_<year>.graphml` (one per `year_ranges`; **without layout**) | **`year_ranges`** is the time-period matrix (alt: `(1960,1980,2000,2026)`, `(2010,2015,2020)`); degree cut `< 5`; ~48 Leiden resolutions as `cpm_communities_at_res=<r>` attrs |
| `graphml_to_parquet.py` | `graph_level/citation_network_until_2026_with_layout.graphml` | `graph_level/citation_network_until_2026_with_layout.parquet` | `GRAPHML_FILE` selects the period; keyword split `\|` |
| `keywords_analysis.py` | `graph_level/citation_network_until_2026_with_layout.parquet`, `raw/keyword_synonyms_0.99_with_transitivity.json` | (under CWD) `td-idf-until-2026/per-cluster-as-document/res-<r>-…/` TF-IDF CSVs + histograms | `NORM='l2'`, `IDF_BIAS=0.0`, `SYNONYMS_THRESHOLD=0.99`, `RESOLUTIONS=[0.0004,0.001]` |
| `find_keywords_per_cluster_noverlap.py` | `graph_level/citation_network_until_<YEAR>_with_layout.graphml`, `raw/keyword_synonyms_0.99_with_transitivity.json` | `keywords_level_data/until_<YEAR>_wordcloud_noverlap/…` TF-IDF CSVs, `wordclouds/` (SVG), `keyword_changes.txt` | `YEAR=1960`, `RESOLUTIONS=[0.005]`, `TOP_N_CLUSTERS=5`, synonyms 0.99 |

## Topics, derived networks & website

| Script | Reads | Writes | Key knobs |
|---|---|---|---|
| `topic_modeling.py` *(plain script, not Hamilton)* | `graph_level/citation_network_selected.graphml` | `data/citation_network_with_topics.graphml`, `data/{topic_info,document_topics,topic_words}.csv`, `data/bertopic_model/` | ⚠ `DATA_DIR="data"` is a plain string used with `/` — path expressions raise `TypeError` as written. SPECTER2, `MIN_TOPIC_SIZE=15`, UMAP `random_state=42`. **Adds the `topic` attribute everything downstream needs.** |
| `topic_community_analysis.py` | `graph_level/citation_network_with_topics_new.graphml` | `graph_level/topic_community/{topic_community_metrics.json, .csv, topic_disconnected_communities.csv}` | `RESOLUTION=0.005`, `TOPIC_ATTR="topic"`, `MAJOR_SHARE=0.10`, `PAIR_MIN_PAPERS=15` |
| `build_bcp_and_cocitation.py` | `graph_level/citation_network_with_topics_new.graphml` | `graph_level/bibliographic_coupling_without_layout.graphml`, `graph_level/cocitation_network_without_layout.graphml` | `EXECUTE`; layout+pickle savers disabled; ForceAtlas2 `iterations=500` |
| `detect_missing_links.py` | `graph_level/citation_network_with_topics_new.graphml` | `graph_level/missing_links/{null_model_report_res=0.005.json, candidate_missing_links_res=0.005.parquet}` | `RESOLUTION=0.005`, `N_PERMUTATIONS=200`, `TOP_K_SAVED=50000` |
| `build_website.py` *(final consumer; substantially changed since the original map)* | `graph_level/citation_network_with_topics_new.graphml` (required), `graph_level/topic_community/topic_community_metrics.json` (optional), `community_quality_metrics/{per_community,per_partition}.parquet`, vendored `website_assets/*` | `reports/website/*` and `reports/website/network_data/{nodes,clusters,communities_by_resolution,resolution_metrics,community_distributions,abstracts}.json` | `COMMUNITY_RESOLUTION=0.005`, `MIN_NAMED_GROUP_SIZE=30`, `TOP_PAPERS/AUTHORS/KEYWORDS`, `PALETTE` |

## Implicit / manual handoffs

These are the edges a future `dvc.yaml` must close. The consumed files exist on disk (DVC-tracked) but
**no committed script writes them under that name** — they came from renames, relocations, or steps run
out-of-band (e.g. on another machine, or with now-disabled savers enabled):

1. **`citation_network.graphml`** — read by `get_network_communities_and_stats`, but
   `build_citation_network`'s active output is `citation_network_without_layout_updated_citations.graphml`
   (its `citation_network.graphml` saver is commented out). Implicit rename/relocate between them.
2. **`citation_network_full_low_res.graphml`** — read by `community_quality_metrics`, but the
   `get_network_communities_and_stats` saver that writes it is commented out; the on-disk file is a
   **frozen Track-A run** artifact.
3. **`citation_network_selected.graphml`** — read by `topic_modeling`; no script writes it. A
   selected/curated graph produced upstream.
4. **`citation_network_with_topics_new.graphml`** — read by `topic_community_analysis`,
   `build_bcp_and_cocitation`, `detect_missing_links`, and `build_website` (the central artifact of the
   analysis half). `topic_modeling` writes `citation_network_with_topics.graphml` (note: **no `_new`**,
   and under `data/` due to its `DATA_DIR` bug). The `_new` file is a rename/variant run.
5. **The `…_with_layout.graphml` family** — read by `graphml_to_parquet` and
   `find_keywords_per_cluster_noverlap`. `process_network_by_time_periods` emits
   `citation_network_until_<year>.graphml` **without** layout, so a separate **ForceAtlas2 layout stage**
   sits between them and is not in the script set.

The canonical community resolution **`0.005`** is shared by `detect_missing_links`,
`topic_community_analysis`, and `build_website`; the graph must carry a `cpm_communities_at_res=0.005`
attribute (written by the Leiden sweep) for those to run.

## Excluded — not functional pipeline stages

- **`process_raw_datasets.py`** — scaffold/prototype. Node bodies are `pass` stubs; `dr.execute` is
  commented out. Declared paths are unresolved/buggy (`Path("reports"/"custom_filename.html")` is malformed).
- **`get_references_locally.py`** — empty stub. No DAG nodes, `_main` returns immediately
  (`sys.exit(_main)` passes the function, not its result). Intended to compute references from a local
  DuckDB dump; not implemented.
