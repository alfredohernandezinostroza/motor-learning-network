# Porting the Embedding-Space Analysis into the Hamilton pipeline

## Context

A colleague built a companion project, **Mariana-Embedding-Space-Analysis**, that extends this
citation-network repo with a *text-embedding* view of the same ~14,500 motor-learning papers:
each paper (title + abstract) is embedded with an LLM, clustered into topics with BERTopic, laid
out as a 2D "semantic map", and cross-analysed against the citation-graph communities to surface
pairs of communities that share a topic but do not cite each other ("citation gaps" /
undiscovered-public-knowledge candidates).

That project is a loose collection of ~21 standalone `scripts/` (argparse `main()` + module-level
helpers). This document plans porting its *analysis* core into this repo as proper **Apache
Hamilton DAG modules**, following the existing module idiom (see `dag_template.py`,
`detect_missing_links.py`, `find_missing_citations.py`), so the text-embedding view becomes a
first-class, reproducible part of the pipeline rather than a separate script bag.

## Environment map (decides what is executable here)

`pixi.toml` defines two environments:

- **`default`** — igraph, cdlib, leidenalg, `sf-hamilton`, pandas, transformers, `pytest`, dvc.
  The citation-structure DAGs run here. Pure-Python analysis nodes are unit-testable here.
- **`simple`** (`no-default-feature`) — bertopic, hdbscan, umap-learn, scikit-learn, networkx,
  matplotlib, spacy, `google-generativeai`, numpy 2 / pandas 3. The embedding stack runs here.
  **Has no `pytest`** and uses the *old* `google-generativeai` SDK (Mariana uses `google-genai`).

Consequences:
- Modules 1 (preprocess) and 5 (topic↔community) depend only on stdlib + pandas (+ igraph for 5),
  so they run and unit-test in **`default`** — fully executable by CI and by an agent with no LLM.
- Modules 3 (recluster) and 4 (layout) need umap/hdbscan/sklearn → run in **`simple`**. Their pure
  helpers (keyword cleaning, c-TF-IDF, entropy) are still unit-testable with small synthetic inputs.
- Module 2 (Gemini embed) needs a live Gemini API key → **not executed here**; only the
  cache-load + clustering nodes are exercised.

## Modules to add (`motor_learning_network/`)

| New module | Source script(s) | Executable here? | Tests |
|---|---|---|---|
| `topic_community_analysis.py` | `topic_community_analysis.py` | **Yes** (default env) | unit: entropy, union-find LCC, disconnected-pairs, integration |
| `preprocess_text_for_embeddings.py` | `preprocess_graphml_text_for_embeddings.py` | **Yes** (default env) | unit: each text-cleaning fn + record builder on synthetic nodes |
| `recluster_embeddings.py` | `recluster_gemini.py` | run needs `simple` env | unit: `_singularize`, `clean_keywords`, `c_tf_idf` on toy corpora |
| `embedding_space_layout.py` | `embedding_space_analysis.py` | run needs `simple` env | unit: `load_topic_labels`, keyword parsing, cluster-summary assembly |
| `topic_modeling_gemini.py` | `topic_modeling_new.py` | **No** (Gemini API) | unit: fingerprint determinism; clustering nodes shared with recluster |

The old `topic_modeling.py` (procedural SPECTER2) is superseded by `topic_modeling_gemini.py`;
move it to `experiments/` (repo convention, see commit `af16a9e`) once the new module lands.

### Adaptations to fit this repo (not verbatim copies)

- **Graph I/O:** Mariana ships a hand-rolled `parse_graphml`. This repo already loads graphs with
  `igraph` (`ig.Graph.Read_GraphML`, see `detect_missing_links.py`). Reuse igraph and the
  `_int_attr` helper idiom instead of porting the XML parser.
- **Community attribute:** Mariana reads a `cluster` node attr for community. Here communities are
  CPM/Leiden attrs `cpm_communities_at_res={RESOLUTION}` (see `get_network_communities_and_stats.py`,
  `detect_missing_links.py`). Parameterise the attribute name; default to the same `RESOLUTION`
  constant style already used in `detect_missing_links.py` so the two modules stay consistent.
- **Community names:** Mariana reads `webdata/communities.json` (website artefact). No website here,
  so names fall back to `f"Community {id}"`; keep an optional names-JSON input for later.
- **Paths / constants:** use `GRAPH_LEVEL_DATA_PATH`, `PROCESSED_DATA_PATH`, `FIGURES_PATH` from
  `constants.py`; add an `EMBEDDINGS_*` path constant if needed rather than hardcoding.
- **DAG shape:** `@dataloader` for inputs returning `(data, metadata)`, pure functions for
  transforms (Hamilton name-based wiring), `@datasaver` for outputs returning metadata, a `_main()`
  built from `dag_template.py`. Tests import the *node functions* directly with fixtures (never the
  driver), exactly like `test_find_missing_citations.py` / `test_bc_and_cocitation.py`.

## Testing strategy

Follow the repo's existing pattern: **unit-test pure node functions with tiny hand-built fixtures**
(igraph adjacency graphs / small DataFrames), asserting against the mathematical definition — the
way `test_bc_and_cocitation.py` checks `A@Aᵀ`. No full-DAG-on-real-data tests; no network/LLM in tests.

Per module:
- **topic_community_analysis** (highest value, all pure):
  - `normalized_entropy`: `[n]` → (0, 1); `[k,k,...]` even split → norm≈1, effective≈k.
  - `UnionFind`/LCC: a 3-node path in one topic → lcc_fraction 1.0; two disjoint edges → 0.5.
  - `find_disconnected_pairs`: construct two communities with observed=0 but expected≥threshold →
    flagged; observed just above `PAIR_MAX_RATIO*expected` → not flagged (boundary test).
  - integration: single-community topic → `community_integration is None`.
- **preprocess_text_for_embeddings**: `decode_entities`, `strip_copyright`, `normalize_headings`,
  `normalize_whitespace`, `clean_field` (placeholder detection), `build_records` (both-missing paper
  skipped; title-only kept). Golden strings for a handful of real boilerplate cases.
- **recluster_embeddings**: `_singularize`/`_sig` synonym collapsing; `clean_keywords` dedupes
  variants; `c_tf_idf` on a 2-class toy corpus returns per-class top terms matching a hand calc.
- **embedding_space_layout**: `load_topic_labels` maps ids→topics and defaults missing→-1 with
  warning; `load_topic_keywords` parses pipe-joined words/scores; cluster-summary centroid math.
- **topic_modeling_gemini**: `compute_text_fingerprint` is order-independent and busts on text/model
  change. Embedding + BERTopic fit are **not** unit-tested (API/heavy); documented as manual/`simple`-env.

### What an agent without LLM access can verify end-to-end
- `pixi run pytest motor_learning_network/tests/test_topic_community_analysis.py` and
  `..._preprocess_text_for_embeddings.py` — green in the `default` env.
- A real run of module 5 on an existing `*_with_topics_*.graphml` + embedding-space parquet
  (both DVC-tracked) reproducing `topic_community_metrics.csv` — optional, data-gated.

### What needs a human / `simple` env / API key
- Modules 3 & 4: `pixi run -e simple python -m motor_learning_network.recluster_embeddings` etc.,
  against the cached `gemini_embeddings_cache_gemini.npz` (currently only in the Mariana repo — must
  be copied in / DVC-tracked here first).
- Module 2 and the two LLM summary scripts: require `GEMINI_API_KEY` and live API calls; add
  `google-genai` (or adapt to the pinned `google-generativeai`) and run manually, then DVC the outputs.

## Sequencing / commits (each: Alfredo sole author)
1. This plan doc.
2. `topic_community_analysis.py` + tests (run green).
3. `preprocess_text_for_embeddings.py` + tests (run green).
4. `recluster_embeddings.py` + `embedding_space_layout.py` + unit tests for their pure helpers.
5. `topic_modeling_gemini.py` DAG (embed node documented as API-gated) + fingerprint test; move old
   `topic_modeling.py` to `experiments/`.
6. Follow-up (separate, not now): copy/DVC the embedding caches into this repo; wire `simple`-env
   end-to-end run; plan the LLM-summary modules.
