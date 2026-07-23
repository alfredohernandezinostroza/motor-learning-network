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

## Status (2026-07-23)

| Module | State | Verification |
|---|---|---|
| `topic_community_analysis.py` (5) | **Done, committed** | 9 unit tests green; full DAG run on `citation_network_with_topics_new` → 128 topics, 41 citation-gap pairs |
| `preprocess_text_for_embeddings.py` (1) | **Done, committed** | 11 unit tests green; full DAG run → 14,511 nodes, all embedding-ready |
| `recluster_embeddings.py` (3) | Planned | blocked: no `simple` env, no in-repo embedding cache |
| `embedding_space_layout.py` (4) | Planned | blocked: same; also low value here (feeds the excluded website) |
| `topic_modeling_gemini.py` (2) | Planned | blocked: live Gemini API |
| LLM summaries (community / bridge) | Planned | blocked: live Gemini API |

Modules 1 and 5 were the two that are both highest scientific value *and* fully verifiable in the
`default` env with no LLM. The rest are gated as below.

## Prerequisites to unblock the remaining modules (need the user / a writable env)

1. **Install the `simple` env.** `pixi install -e simple` currently fails here because
   `~/.cache/rattler` is read-only in this sandbox (`os error 30`). On the real machine this should
   just work; that env carries umap-learn, hdbscan, scikit-learn, bertopic, matplotlib.
2. **Bring the embedding cache into this repo.** `gemini_embeddings_cache_gemini.npz` (165 MB),
   `document_topics_gemini.csv`, `topic_words_gemini.csv`, and the with-topics graphml currently live
   only in `../Mariana-Embedding-Space-Analysis/Analysis/data/`. Copy them under
   `data/graph_level_data/` (or a new `data/embeddings/`) and `dvc add` them, so modules 3/4 have
   inputs and the outputs are reproducible/recoverable. Decide the canonical **node identifier**:
   module 1 emits `node_id = graph "name"` attr; the Mariana cache keys on the GraphML XML id — pick
   one and make module 2/3/4 consistent with module 1.
3. **LLM SDK.** Mariana uses `google-genai`; this repo's `simple` env pins the older
   `google-generativeai`. For module 2 and the summaries, either add `google-genai` or adapt the
   client calls to the pinned SDK. Requires `GEMINI_API_KEY` in `.env` and spends quota → run by a
   human, then `dvc add` the outputs. Do NOT run from an agent without explicit go-ahead.

## Remaining implementation (turnkey once prerequisites are met)

Each as a Hamilton DAG mirroring modules 1/5 (dataloader → pure/underscore helpers → nodes →
datasaver; heavy imports kept lazy inside nodes so the module imports cleanly in `default` for
light-helper unit tests; UI tracker left disabled). Sole author: Alfredo.

- **`recluster_embeddings.py`** (from `recluster_gemini.py`): nodes `cached_embeddings` (dataloader
  of the npz), `cluster_assignment` (UMAP-5D cosine → HDBSCAN eom, renumber by size), `cluster_keywords`
  (c-TF-IDF via sklearn + synonym collapsing), savers for `document_topics`/`topic_words`/`topic_info`.
  Testable in `default`: `_singularize`, `_sig`, `clean_keywords`, `load_synonym_map`. Env-gated:
  `c_tf_idf` (sklearn), `assign_clusters` (umap/hdbscan) — verify via a `simple`-env smoke run on the
  cache reproducing the existing `topic_info_gemini.csv` cluster count.
- **`embedding_space_layout.py`** (from `embedding_space_analysis.py`): nodes `cached_embeddings`,
  `topic_labels` (from document_topics), `layout_2d` (UMAP-2D cosine, display only), `cluster_summary`
  (+ keywords). Testable in `default`: `load_topic_labels`, `load_topic_keywords`, centroid summary.
  Env-gated: the UMAP node. Note: primarily a website feed (excluded consumer), so lower priority here.
- **`topic_modeling_gemini.py`** (from `topic_modeling_new.py`): nodes `embedding_text` (dataloader of
  module 1's ready parquet), `text_fingerprint`, `gemini_embeddings` (**API-gated** — cache-load path
  unit-tested; live embed run by a human), then reuse `recluster_embeddings`' clustering nodes; saver
  writes the `topic` attribute back onto the graphml. Unit test: `compute_text_fingerprint` is
  order-independent and busts on text/model change. Then move the old procedural
  `topic_modeling.py` (SPECTER2) to `experiments/` per repo convention (commit `af16a9e`).
- **LLM summaries** (`generate_community_summaries.py`, `generate_bridge_summaries.py`): representative-
  paper selection (centroid + MMR) is pure and unit-testable in `default` on synthetic vectors; the
  Gemini JSON calls are API-gated (human-run, cache by prompt fingerprint, then `dvc add`).

## Commits so far (each: Alfredo sole author)
1. `feat(pipelines): add topic<->community analysis DAG (from embedding-space project)`
2. `feat(pipelines): add text-preprocessing DAG for embeddings (from embedding-space project)`
   (this plan doc shipped with commit 1).
