# Next Steps: From a Citation Map to a Validated, LLM-Fused Field Map of Motor Learning

*Ideation grounded in `sources/report.md`, its cited literature (local + online), and
adjacent 2025–2026 work. Written against the current state of this repository.*

---

## 1. The strategic thesis

The single most important paper for this project is
**Camelo-Guerrero and Díaz-Rodríguez (2026), "How Much Structure Do LLMs Need?"**
Its central finding, from 100 reconstructed bibliometric corpora, is blunt:

> LLMs are **unreliable when asked to infer bibliometric structure from scratch**, but
> when *algorithms* define the clusters and the LLM only **interprets** them, the
> LLM-written descriptions can score *higher than human-written ones* on coverage,
> clustering (ARI), and graph (modularity) metrics. The best workflow is hybrid:
> **algorithms provide auditable structure; LLMs translate it into readable prose.**

A second, sharper result matters for us specifically: **the right amount of structure
depends on the relation.** Bibliographic-coupling clusters are best summarized from the
*full labeled cluster* (their `Labeled` pipeline), whereas **direct-citation clusters are
best summarized from compact, link-ranked top-*k* evidence** (their `Ranked` pipeline),
because citation structure concentrates around a few influential works.

**This repository is already the "auditable algorithmic structure" half of that hybrid.**
We have the direct-citation graph, Leiden/CPM communities at multiple resolutions,
bibliographic-coupling and co-citation graphs, SPECTER2 embeddings, and time slices. We are
not starting a new LLM system; we are adding the *interpretation and validation* layer that
the report calls "the visible gap." Everything below is organized around that gap.

---

## 2. Where the pipeline stands vs. the state of the art

| Pipeline stage | Report's best practice | This repo today | Gap |
|---|---|---|---|
| Corpus + edges | Open index (OpenAlex) | Scopus+WoS+MEDLINE+EBSCO, OpenCitations/OpenAlex reference resolution | ✅ arguably richer (multi-source, deduplicated) |
| Structural backbone | Leiden on **direct citation** (Klavans–Boyack 2017) | Leiden/CPM, direct citation, multi-resolution sweep | ✅ on the strongest single basis |
| Content representation | Citation-informed embeddings, SPECTER lineage | SPECTER2 CLS embeddings in `topic_modeling.py` | ⚠️ computed but **used in isolation** from the citation partition |
| Cluster labeling | **LLM-generated** labels + hierarchy (Zhu 2025) | corrected c-TF-IDF over keywords + BERTopic word-bags | ❌ word-bags, not readable labels; no hierarchy |
| Synthesis | Retrieval-grounded LLM survey (AutoSurvey, PaperQA) | none | ❌ missing narrative layer |
| Validation | Expert ground truth, ARI/modularity (Klavans–Boyack style) | none | ❌ **the visible gap**; map is unvalidated |
| Temporal dynamics | Emerging/fading subfields | time-sliced graphs exist, communities recomputed per slice | ⚠️ slices built but **not aligned across time** |

The three ❌ rows are where the frontier is, and where an in-house motor-learning lab
(with domain experts able to supply ground truth) has an unusual advantage.

---

## 3. Prioritized next steps

Ordered by leverage: each step's output feeds the next, and the early ones are cheap.

### Step 1 — LLM cluster interpretation on the `Ranked` pipeline *(highest leverage, do first)*

**What.** For each Leiden community, select the top-*k* representative papers by an
in-cluster importance score (in-degree within the community, or PageRank restricted to the
subgraph — citation clusters "concentrate structure around influential works"), and pass
*title + abstract of those k papers only* to an LLM that returns a short **label** and a
**one-paragraph description**. This is exactly the `Ranked` pipeline that Camelo-Guerrero
found best for direct-citation clusters — and it is cheap (k≈10–20 papers per cluster, not
the whole cluster).

**Why.** Replaces the current c-TF-IDF/BERTopic word-bags — which the report explicitly
calls "the long-standing weak link of every clustering pipeline" — with human-readable,
auditable labels, using the structure we already computed. Keeps the LLM as *interpreter*,
never *generator*, which is the whole point of the 2026 result.

**How, in this repo.** A new Hamilton module `characterize_clusters_with_llm.py` that:
loads `citation_network_full_low_res.graphml` (already has `cpm_communities_at_res=*` node
attrs); for each community computes in-cluster PageRank with igraph; builds the ranked
evidence block; calls the LLM (see Step 2 for grounding); saves a `cluster_descriptions`
parquet keyed by `(resolution, cluster_id)`. Mirror the existing
`find_keywords_per_cluster.py` structure so the two characterizations sit side by side and
can be compared. **Keep the c-TF-IDF output** — it becomes a baseline in Step 6's validation,
not dead code.

*Model note:* use a current Claude model (e.g. `claude-sonnet-5`) via the Anthropic SDK;
the task is short-context interpretation, ideal for a fast model.

### Step 2 — Reference grounding / anti-hallucination guardrail *(do together with Step 1)*

**What.** Constrain and then verify. The prompt gives the LLM only papers that are actually
in the cluster and forbids citing anything else; after generation, validate every DOI/title
the LLM emits against the cluster membership and against OpenAlex (title + year + first
author), the exact check in Camelo-Guerrero's reference-grounding metric and in
**CiteCheck (2026)**. Any unmatched citation is flagged, not silently kept.

**Why.** Automated synthesis is "where hallucinated-citation risk is most acute"
(report §Frontier; PaperQA, AutoSurvey). We already store OpenAlex IDs
(`missing_openalex_ids.csv`, reference resolution modules), so grounding is nearly free and
makes every label defensible in a published field map.

**How.** A `validate_grounding()` node reusing the existing OpenAlex plumbing; emit a
per-description `grounding_score` and a list of unverified claims.

### Step 3 — Answer the field's open question on our own corpus: does citation structure beat text alone?

**What.** We hold both signals for the *same* papers: Leiden communities (citation) and
SPECTER2 + BERTopic clusters (text). Measure their agreement (ARI, NMI, V-measure), and
cross-evaluate: compute modularity of the citation graph under the *text* partition and
silhouette in SPECTER space under the *citation* partition. This is precisely the
Camelo-Guerrero open question — "how much does the graph add beyond text?" — and
Klavans–Boyack's relation-comparison question, answerable here without new data.

**Why.** It is a genuine, publishable methodological result (the report names this as an
open problem twice), and it tells us operationally whether to trust citation clusters, text
clusters, or a fusion for the motor-learning map. See also *"A comparison of citation-based
clustering and topic modeling for science mapping"* (arXiv 2309.06160) and *"Which topics
are best represented by science maps?"* (arXiv 2406.06454) for the exact metrics and framing.

**How.** A small analysis module over the existing `citation_network_with_topics` parquet;
no new heavy compute. Also compare the **three relations we already built** — direct
citation vs. bibliographic coupling vs. co-citation — for which best matches expert ground
truth (Step 6), reproducing Klavans–Boyack 2017 on a single field.

### Step 4 — Fuse the two views instead of running them in parallel

**What.** Today `topic_modeling.py` clusters SPECTER embeddings with HDBSCAN
*independently* of the Leiden partition — "two separate maps to reconcile," which the report
warns against. Two concrete fusions, in increasing ambition:
- **(a) Attributed reduction (cheap):** feed the SPECTER embeddings *as node features* and
  cluster the graph with them jointly (e.g. run UMAP on a blend of SPECTER vectors and a
  node2vec/`igraph` structural embedding, or use SPECTER only to *label*, Leiden to
  *partition*). This is the report's "content and structure jointly represented" recipe.
- **(b) SPECTER2 with the adapter (correctness fix):** the current code takes the raw
  `specter2_base` CLS token. SPECTER2's *proximity adapter* is what actually places
  citation-linked papers together (Cohan 2020; Singh 2022 SciRepEval). Loading the adapter
  is a small change that makes the embedding citation-aware — directly the "two views of one
  embedding" idea rather than a generic BERT vector.

**Why.** This is "the seam" the report is entirely about. (b) is a low-risk correctness
improvement worth doing regardless; (a) is the research contribution.

### Step 5 — Temporal field evolution: align communities across the time slices we already built

**What.** We have `citation_network_until_{1960,1980,2000,2010,2015,2020,2026}` with
communities recomputed per slice — but *not linked across slices*. Add a matching step
(Jaccard overlap of membership between consecutive slices) to build birth / growth / merge /
split / death trajectories for each subfield, then have the LLM narrate each trajectory.
Optionally add **citation-burst detection** (Kleinberg, as in CiteSpace) to flag research
fronts.

**Why.** The report opens by defining field mapping as identifying "which are emerging and
which are fading." We are one alignment step away from that, and it is the most compelling
figure for a review paper. Grounding in dynamic-community-detection literature:
*"Exploring temporal community evolution"* (Applied Network Science 2023), *ATEM* (arXiv
2306.02221), and *"Emerging topics detection using motif-based analysis"* (Scientometrics
2025).

**How.** New module `align_communities_over_time.py` consuming the existing per-slice
graphs; output an alluvial/Sankey-ready edge list of community-to-community flows.

### Step 6 — Validation against expert ground truth *(the report's "visible gap"; the credibility anchor)*

**What.** Build a motor-learning subfield taxonomy from domain experts (this lab has them —
e.g. motor adaptation, sequence/skill learning, motor imagery, sensorimotor integration,
computational motor control, neurorehabilitation, sport/skill acquisition, development),
hand-label a stratified sample of papers, and score every partition (Leiden at each
resolution, BERTopic, the fusion from Step 4) with ARI/NMI against it — the evaluation
Klavans–Boyack (2017) ran for relation choice, which the report says "no shared benchmark"
yet does end-to-end. Also use it to **select the CPM resolution** princip(currently a raw
0.001–0.009 sweep with no selection criterion) by whichever matches expert granularity, and
to add a **stability** check (ARI across Leiden seeds).

**Why.** Without this the map is unvalidated; with it, every downstream LLM label inherits a
measured trust level, and the project has a defensible headline result. This is the step that
turns "a pipeline" into "a validated field map," which the report says nobody has published.

### Step 7 — Retrieval-grounded synthesis layer *(capstone; only after 1–3 land)*

**What.** On top of validated, labeled clusters, a PaperQA/AutoSurvey-style agent drafts the
per-subfield narrative of the motor-learning literature, every claim retrieved from and cited
to papers *in that cluster*. Reuse the Step 2 grounding check as the acceptance gate.

**Why.** This is the "narrative layer that sits on top of a structural map" (report
§Frontier). It is the deliverable a lab actually wants — a grounded, auto-drafted, human-
edited review — and it is only safe *because* Steps 1–6 make the structure auditable.

---

## 4. Recommended near-term sequence

1. **Steps 1+2 together** — LLM `Ranked` labeling with grounding. Highest leverage, ~1
   module, immediately replaces word-bag labels with citable ones.
2. **Step 3** — agreement analysis; cheap, uses existing outputs, produces a real result.
3. **Step 6 (start)** — stand up the expert taxonomy + labeled sample early; it gates
   trust for everything and needs human calendar time, so begin it in parallel.
4. **Step 5** — temporal alignment; best single figure, existing data.
5. **Step 4(b)** then **4(a)** — SPECTER2 adapter fix, then true fusion.
6. **Step 7** — synthesis capstone.

## 5. Two methodological cautions from the sources

- **Keep the LLM out of structure.** The 2026 result is that LLMs *degrade* when they invent
  organization. Never let the model merge/split/create clusters — it only labels and
  describes what Leiden produced. This is a hard design constraint, not a preference.
- **Direct citation is already the right backbone** (Klavans–Boyack 2017) — no need to switch
  primary relations. Use bibliographic coupling / co-citation as *comparison baselines* in
  Step 3/6, not as replacements.

## 6. Key sources map

- **Hybrid workflow / how much structure** — Camelo-Guerrero & Díaz-Rodríguez 2026
  (`local/arXiv-2605.24351v1`) — *the* anchor.
- **LLM hierarchical taxonomy / multi-aspect labeling** — Zhu et al. 2025
  (`local/arXiv-2509.19125v1`) — method for Step 1's hierarchy across resolutions.
- **Automated synthesis** — AutoSurvey (`local/arXiv-2406.10252v2`), PaperQA
  (`local/arXiv-2312.07559v2`) — Step 7.
- **Graph-meets-LLM survey** — Li et al. 2023 (`local/arXiv-2311.12399v4`) — Step 4 framing.
- **Relation choice / validation** — Klavans–Boyack 2017; Waltman–van Eck 2012 — Steps 3, 6.
- **Citation-informed embeddings** — SPECTER (Cohan 2020), SciRepEval (Singh 2022) — Step 4(b).
- **New (2026) grounding + comparison** — CiteCheck (arXiv 2605.27700), citation-vs-text
  clustering comparisons (arXiv 2309.06160, 2406.06454) — Steps 2, 3.
