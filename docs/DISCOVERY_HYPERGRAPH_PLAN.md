# Human-aware discovery prediction for the motor learning corpus

Implementation plan for porting Sourati & Evans (2023), *Accelerating science with
human-aware artificial intelligence*, Nature Human Behaviour 7:1682–1696
(`references/authors-material-property-hypergraph/`) onto this repository's
literature corpus.

Branch: `feature/discovery-hypergraph`
Status: **plan only — nothing implemented yet.** Written 2026-08-21.

---

## 0. Verdict up front

The method ports, but not by keeping the words "material" and "property". What is
load-bearing in the paper is not chemistry — it is a **structural triple**:

1. an **enumerable candidate pool** (≈4,000 DrugBank drugs, ≈100k inorganic compounds),
2. a small set of **valued target concepts** (3 electrochemical properties, 100–400 diseases),
3. a **dated, machine-detectable first-report event** joining one candidate to one target.

Motor learning has an analogue of all three, and it is the field's own experimental
grammar: *we gave [INTERVENTION] to [POPULATION] on [TASK] and it changed [OUTCOME]*.
Map **material → intervention/manipulation** and **property → outcome/capacity**, and the
asymmetry the paper depends on (the thing you administer vs. the thing you want) is
preserved exactly. Predicted pairs are also directly experiment-able, which the
construct-level alternatives are not.

Recommended design is **Ontology B** (§3.2): a *typed multi-class* hypergraph carrying
intervention, outcome, task, population and construct nodes alongside authors, but with
the **prediction target held binary** (intervention × outcome). Extra types act as
bridging nodes inside random walks — the same structural role author nodes play in the
paper — without diluting the evaluation.

The single biggest risk is **not** ontology. It is **corpus scale**: 36,338 papers
against their 1.5M and 27.5M. §6 treats this honestly, including the one intervention
(OpenAlex author-profile expansion) that most plausibly rescues the human-aware signal.
Phase 0 (§10) is a two-week go/no-go gate designed to kill the project cheaply if the
scale objection turns out to be fatal.

---

## 1. What the paper actually does

Condensed from the Methods (pp. 1693–1695), because the implementation must match it
closely enough that the baselines remain comparable.

**Hypergraph.** Nodes = {materials, properties, disambiguated authors}. Hyperedges =
papers, each incident to every entity mentioned in its title/abstract plus all its
authors. Author nodes vastly outnumber material nodes.

**Random walks.** Per target property: 250,000 non-lazy truncated walks. Each step (1)
picks a hyperedge containing the current node, (2) picks a node from that hyperedge.
Walks start at the property node, end after 20 steps or at a dead end. An
**α-modified sampling distribution** mixes two uniforms so that P(pick a material) = α ·
P(pick an author). Tested α ∈ {0, 1, ∞}; **α = 1 wins** — equal weight on authors and
materials. Author-only (α = 0) is markedly worst, i.e. pure networking without reading
does not predict discovery.

**Embedding.** Author nodes are *stripped from the sequences*, then skip-gram Word2Vec
(deepwalk) is trained on the remaining material/property sequences — window 8, 5 epochs
(down from 30; the deepwalk vocabulary is far smaller than natural text). Content-only
baseline = Word2Vec over raw abstracts (replicating Tshitoyan et al. 2019).

**Relevance metrics.** (a) deepwalk cosine similarity; (b) closed-form multi-step
transition probabilities via Bayes' rule over metapaths — property→author→material
(s=2) and property→author→author→material (s=3), no sampling required. Also tried
GraphSAGE with Word2Vec features as the node features (the hypergraph is featureless).

**Evaluation.** Pick a prediction year, build the hypergraph from the **preceding 5
years only** (older co-authorship is not socially live), rank the candidate pool against
the target, take **top 50 as predictions**, and score cumulative precision against pairs
actually first-published in subsequent years. Baselines: random, content-only Word2Vec,
theoretical/first-principles scores.

**Complementary ("alien") hypotheses.** Combine a *plausibility* signal (content-embedding
cosine) with an *inaccessibility* signal (shortest-path distance in the hypergraph),
each van-der-Waerden→Z-score normalized, mixed by **β ∈ [−1, 1]**. β < 0 mimics human
attention; β > 0 avoids it. Optimum for complementary-but-sound sits at **β ≈ 0.2–0.3**.

**Headline results.** ≈100% precision gain over content-only for materials; +43% for
drug repurposing; 350–400% for COVID-19 therapies. Discoverer prediction: 40% of top-50
predicted authors became actual discoverers. Wait-time to discovery rises monotonically
with β.

---

## 2. What this repository actually has

All figures below were **verified against the parquet files**, not assumed.

### 2.1 Corpus

`data/processed/clean_unified_database.parquet` — 36,338 papers.

| Field | Coverage | Note |
|---|---|---|
| `title` | 36,338 | |
| `abstract` | **34,895 usable** (>50 chars) | the extraction substrate |
| `authors` | 36,338 | numpy array of `"Last, First"` strings, **mean 4.22/paper** ⇒ ≈153k author mentions |
| `doi` | **36,338 (100%)** | universal join key → OpenAlex |
| `pubmed_id` | 20,521 (56%) | → MeSH via E-utilities |
| `year` | 36,338 | range 1895–2026 |
| `keywords` | present but **empty strings** — unusable as-is |

Source mix: Scopus 28,801 · Web of Science 3,606 · Academic Search Ultimate 3,528 ·
PubMed 403.

Year distribution is heavily recent: ≈2,000–3,200 papers/year since 2021, 1,085 in 2012.
This matters — it means a 5-year training window near the present holds ≈10,000 papers,
which is a workable hypergraph, whereas a 5-year window in 1995 does not.

### 2.2 Disambiguation signals — better than expected

The paper needed Scopus author codes and the PubMed Knowledge Graph. We have direct
equivalents already on disk:

| Source | Rows | Signal |
|---|---|---|
| `scopus_database.parquet` | 31,397 | **`Author(s) ID` — 100% coverage**, Elsevier's own disambiguated codes (`'55909479000; 55976515500'`), plus `Author full names` with IDs inline and `Affiliations` at 100% |
| `wos_database.parquet` | 37,490 | `OI` **ORCID: 23,632** · `RI` ResearcherID: 22,752 · `C1` addresses: 35,614 · `AF` full names: 25,820 · `DI` DOI: 31,035 |
| OpenAlex | via 100% DOI coverage | disambiguated author IDs, ORCIDs, institutions — `OPENALEX_API_KEY` already wired in `constants.py`, prior art in `find_missing_citations.py` |

Scopus and WoS raw row counts **exceed** the deduplicated 36,338, so the same paper is
often in both. A DOI join therefore lets WoS ORCIDs enrich Scopus-sourced records and
vice versa. This converts author disambiguation from a from-scratch problem into a
**cross-source reconciliation** problem with abundant hard anchors — much more tractable
(§4).

### 2.3 Existing assets worth reusing

- Leiden/CPM communities at 9 resolutions on the citation network, with quality and
  connectivity metrics (`community_quality_metrics.py`, `get_network_communities_and_stats.py`).
  Community co-membership is a **strong disambiguation feature** and a natural stratifier
  for evaluation.
- `detect_missing_links.py` — an established null-model-testing idiom in this repo
  (28.9× over null, z ≈ 605). §8 mirrors its structure deliberately.
- `data/raw/keyword_synonyms_0.99_with_transitivity.json` — an existing
  embedding-threshold + transitive-closure synonym-merging pattern; §5's concept
  canonicalization is the same algorithm applied to a new vocabulary.
- `topic_modeling.py`, `preprocess_text_for_embeddings.py` — text pipeline precedent.
- Full-text retrieval work on `retrieve-full-text` — **not on the critical path.** The
  paper deliberately uses titles + abstracts only, and argues its edge comes from
  *richer social* data, not more text. Abstracts suffice.

### 2.4 Environment gaps

- `gensim` and `scikit-learn` are **absent from the default pixi env** (they live only in
  the `simple` feature). Deepwalk needs `gensim`; disambiguation needs `sklearn`.
- No `sentence-transformers`; `spacy` 3.8.14 and `torch` 2.10.0+cu128 are present.
- **`nvidia-smi` fails on this host and `torch.cuda.is_available()` is `False`.** The four
  A100s are not currently visible from this environment. Not blocking (everything except
  LLM extraction is CPU-cheap — §9), but confirm before scheduling extraction.

---

## 3. Challenge 2 first: the ontology

Taking the user's harder question first, since it determines everything downstream.

### 3.1 Why "material/property" is not the thing to copy

In materials science the ontology is free: a compound is a formula, a property is
measured in named units, and their co-mention in an abstract is a real event. None of
that transfers. But it is worth being precise about *what* fails to transfer, because
three separate things are bundled in "motor learning is more abstract":

1. **No closed pool.** There is no pymatgen or DrugBank for behavioural interventions.
   Any pool must be *induced from our own corpus*, which makes pool construction part of
   the method rather than an input, and makes it a source of circularity to guard against.
2. **Naming instability.** "Contextual interference" / "random practice schedule" /
   "blocked vs. random practice" denote one construct. `PbTiO₃` never has this problem.
   Concept canonicalization becomes a first-class DAG, not a preprocessing detail — it is
   author disambiguation's twin.
3. **Co-mention ≠ finding.** "Sleep" and "consolidation" co-occur in thousands of
   abstracts; the first co-mention was probably speculative, and a large fraction of
   later ones are **null or contradictory results** (the tDCS literature is the obvious
   case). A ground truth built on raw first co-occurrence would learn to predict *what
   the field will write about*, not *what turns out to be true*. The paper flags absent
   negative knowledge as a limitation; in motor learning it is closer to a central fact.

Problem 3 is the deep one and is treated separately in §5.3.

### 3.2 Four candidate ontologies

#### Ontology A — direct transplant: intervention → outcome, two node types

- **Material →** the manipulable thing administered: *anodal tDCS over M1, sleep,
  random practice schedule, KR frequency, mirror therapy, error augmentation,
  observational practice, self-controlled practice, robot-assisted training, aerobic
  exercise, mental practice, dopaminergic agonist, dual-task load*.
- **Property →** the valued capacity sought: *retention, transfer, savings,
  consolidation, generalization to the untrained limb, offline gains, implicit
  adaptation rate, corticospinal excitability, movement variability reduction,
  resistance to interference*.

**For:** preserves the paper's asymmetry exactly; predictions are directly testable
experiments; evaluation code ports 1:1; minimal extraction surface.
**Against:** throws away the field's contingency. "Random practice improves retention"
is not a fact — it is a fact *for certain tasks, certain expertise levels, certain
retention intervals*. Contextual-interference effects famously invert with task
complexity. A two-type graph cannot represent the condition under which a claim holds.

#### Ontology B — typed multi-class hypergraph, binary prediction target ✅ **RECOMMENDED**

Node types carried in the hypergraph:

| Type | Role | Examples |
|---|---|---|
| `author` | social/cognitive accessibility | disambiguated researchers |
| `intervention` | **candidate** (the "material") | tDCS-M1-anodal, random practice schedule, sleep, KR-100% |
| `outcome` | **target** (the "property") | retention, transfer, savings, offline consolidation |
| `task` | bridging / condition | SRT, visuomotor rotation, force-field reaching, mirror tracing, dart throwing |
| `population` | bridging / condition | stroke, Parkinson's, older adults, novices, musicians |
| `construct` | bridging / theory | internal model, schema theory, OPTIMAL theory, predictive coding, use-dependent plasticity |

Hyperedge = paper, incident to all of the above that it mentions, plus its authors.
**But the predicted relation and every precision metric stay `intervention × outcome`.**

**Why this is the right call.** The paper's own justification for author nodes is that
they are *bridges the walker can cross* — the co-author who worked on sodium nitrite is
what makes the ferroelectricity inference cognitively available. `task` and `construct`
nodes play precisely that role for a behavioural science: two interventions studied on
the same task, or explained by the same theory, are near each other in the space of
imaginable hypotheses, whether or not they share authors. So contingency enters as
*graph structure* rather than as *relation arity* — no loss of the evaluation.

It also generalizes the paper's α cleanly. Instead of one scalar mixing authors against
materials, use a **per-type sampling weight vector**. α = 1 (their optimum) becomes the
special case "authors weighted equal to everything else", and the vector gives a
genuinely informative ablation grid: authors-only, concepts-only, no-task, no-construct,
uniform.

**Against:** larger extraction surface (6 types, not 2), so extraction error compounds;
and walks must traverse more type-hops to get from outcome to intervention, which costs
signal in a corpus that is already too small. Both are measurable, and §8's ablations
measure exactly them. If `task`/`construct` do not beat two-type on held-out precision,
drop them — Ontology B degrades gracefully into Ontology A.

#### Ontology C — construct-pair discovery (Swanson ABC over theory)

Predict which two *theoretical constructs* will first be linked (e.g. "predictive
coding" × "contextual interference").

**For:** matches how the field actually generates novelty — much motor learning
"discovery" is reinterpretation, not intervention testing. Genuinely closer to the
subject matter.
**Against:** fatal on all three structural preconditions. No enumerable pool, no
asymmetry (the relation is symmetric, so "which is the target?" is undefined), no crisp
dated event, and **no experimental actionability** — a predicted construct pair is a
paper someone might write, not an experiment someone can run. Precision would be
uninterpretable.
**Verdict: do not build as primary.** Worth keeping as a qualitative side-analysis once
B works, since `construct` nodes exist in B anyway.

#### Ontology D — MeSH-only, no LLM (the honest baseline)

Use NLM MeSH descriptors on the 20,521 PubMed-indexed papers as the entire vocabulary,
splitting the tree into intervention-like branches (E — Analytical/Therapeutic
Techniques; the `/therapy`, `/rehabilitation` qualifiers) and outcome-like branches
(F02 — Psychological Phenomena; G11 — Musculoskeletal & Neural Physiological Phenomena).
This is close to what the paper *actually did* for MEDLINE.

**For:** free, curated, already disambiguated, hierarchical, zero extraction error, fully
reproducible, no LLM cost or drift. Sidesteps §3.1's problems 1 and 2 entirely.
**Against:** covers only 56% of the corpus; MeSH is far too coarse for this field
(it has no descriptor for "contextual interference", "knowledge of results", or
"visuomotor rotation" — precisely the concepts that carry motor learning's content);
and indexing lags, which is poison for a first-report-date ground truth.

**Verdict: build it as a baseline and a validation anchor, not as the primary.** It is
cheap, and it gives something the plan otherwise lacks: an *independent* vocabulary
against which to check LLM-extracted entities on the same paper (§8.2). Do not skip it.

### 3.3 Should we use this paradigm at all?

Yes, with one stated limitation.

The paradigm is sound here because motor learning **does** have the structural triple,
and because its intervention→outcome claims are exactly the claims that get tested,
funded and translated. The mapping is not a stretch; it is the field's own paper
abstract, formalized.

The limitation to state in any writeup: the paradigm models discovery as **combinatorial
recombination of existing entities**. That covers a real and large share of motor
learning output, but it structurally cannot anticipate *conceptual* advances — a new
construct, a reframing, a measurement innovation. When Ontology C is dismissed above, it
is dismissed as an *evaluable prediction target*, not as a description of how the field
works. The honest framing for this project is: **"which testable intervention→outcome
hypotheses are ripe, and which are ripe but nobody is positioned to see?"** — not "what
is the next big idea in motor learning?" Overclaiming here would be the easiest way to
make a reviewer hostile.

---

## 4. Challenge 1: author disambiguation

### 4.1 Framing

Because of §2.2, this is **cross-source reconciliation with hard anchors**, not
from-scratch disambiguation. Design accordingly: a constrained clustering problem where
ORCID and Scopus IDs supply must-link and cannot-link constraints, and a learned pairwise
score fills the gaps for the ≈20% of records with no identifier.

This must be a proper DAG and must be validated, because the paper's entire result rests
on author structure. Bad author nodes ⇒ the human-aware advantage evaporates and we would
not be able to tell that from a null result about the method.

### 4.2 DAG: `disambiguate_authors.py`

```
raw_author_mentions          # explode clean db → (paper_index, position, raw_name)
scopus_author_id_anchors     # Author(s) ID ⟂ Author full names, positional align
wos_orcid_anchors            # parse OI "Name/0000-0002-..." pairs
wos_researcher_id_anchors    # parse RI
openalex_author_anchors      # fetch by DOI (cached, resumable — mirror get_full_texts.py)
        ↓
anchored_author_mentions     # mentions carrying ≥1 external identifier
normalized_author_names      # unicode fold, accent strip, particle handling
                             #   (van/de/Ó), hyphen + initial normalization
blocking_keys                # (normalized_last_name, first_initial) → candidate pairs
        ↓
pairwise_disambiguation_features
        # shared_coauthor_count (strongest single feature)
        # affiliation_string_similarity  (Scopus Affiliations, WoS C1/C3)
        # journal_overlap_count
        # citation_community_overlap     ← reuse Leiden communities
        # abstract_embedding_similarity  (author's mean paper embedding)
        # publication_year_gap
        # full_first_name_compatibility  (compatible vs contradictory)
        ↓
must_link_constraints        # identical ORCID | identical Scopus ID
cannot_link_constraints      # two *different* ORCIDs in one block
        ↓
pairwise_match_probabilities # logistic model trained on ORCID-labelled pairs
constrained_author_clusters  # threshold → connected components under constraints
        ↓
disambiguated_author_table   # canonical_author_id ↔ mentions ↔ papers
author_disambiguation_report # metrics below
```

### 4.3 Validation

- **Held-out identifier test.** Hide ORCIDs from a random 20% of anchored mentions; run
  the full pipeline; measure whether it recovers the hidden groupings. Report pairwise
  precision / recall / F1 **and B-cubed** precision/recall (pairwise alone flatters
  large clusters). The paper's integrative Scopus+PKG method reports 98.0% F1 /
  98.62% precision / 97.56% recall — that is the bar to quote against.
- **Splitting/lumping diagnostics.** Cluster-size distribution; a
  most-prolific-authors table for eyeball review; explicit inspection of high-collision
  blocks (`Zhang, Y`, `Kim, J`, `Smith, J`). A silently lumped `Zhang, Y` supernode
  would corrupt every random walk that touches it.
- **Cross-source agreement.** On DOI-joined papers present in both Scopus and WoS, do
  Scopus IDs and ORCIDs induce the same partition? Disagreement rate is a free,
  label-independent quality signal.
- **Sensitivity.** Re-run the §7 headline prediction under (a) full disambiguation,
  (b) exact-string-match authors, (c) last-name+initial only. If the discovery-prediction
  result is insensitive to this, that is important to know and to report.

---

## 5. Entities, canonicalization, and what counts as a discovery

### 5.1 DAG: `extract_science_entities.py`

Structured extraction from title + abstract over 34,895 papers, into Ontology B's six
types (minus `author`). Recommended approach: **LLM extraction with a schema-constrained
output**, run in batch.

Design constraints:
- Extract **spans plus a type label plus a normalized surface form**, and for
  intervention–outcome pairs also a **relation polarity** (`supports` / `null` /
  `contradicts` / `speculative`) and a **claim-strength** flag (tested vs. merely
  mentioned). Polarity is what defends against §3.1's problem 3, and it is nearly free
  once an LLM is already reading the abstract.
- **Two-pass vocabulary induction.** Pass 1: open extraction on a stratified sample
  (≈3,000 abstracts spanning decades and communities) → cluster → hand-curate a
  controlled vocabulary with Alfredo. Pass 2: constrained extraction over the full
  corpus against that vocabulary, with an explicit `OTHER/new` escape hatch so genuinely
  novel terms are not silently forced into existing bins. This is the step that keeps
  the pool from being a black box, and it is where domain expertise is irreplaceable.
- **Prompt/model pinned and recorded** in the output table (model id, prompt hash,
  extraction date). A drifting extractor invalidates the temporal ground truth, which is
  the one thing the whole evaluation depends on.
- Budget a **human-annotated gold set of ≈300 abstracts** for extraction P/R/F1 by type.
  Without it there is no way to state an error bar on anything downstream.

### 5.2 DAG: `canonicalize_entities.py`

Mention → canonical concept. Same algorithm the repo already uses for keyword synonyms:
embed surface forms, agglomerative-cluster at a tuned cosine threshold, take transitive
closure, then hand-review the merge list. Additions specific to this task:

- **MeSH alignment** (Ontology D): map canonical concepts to MeSH descriptors where one
  exists, on the 20,521 PubMed papers. Gives an external identifier for a subset and
  makes the vocabulary partly interoperable.
- **Explicit block-list of merges** that embeddings get wrong and that matter here —
  e.g. *retention* vs *transfer* are near-synonymous in embedding space and are the
  central distinction in the field. Curated cannot-merge pairs, versioned in the repo.
- Output a **frozen, versioned vocabulary artifact**. Every downstream DAG pins a
  vocabulary version, or the temporal experiments are not reproducible.

### 5.3 DAG: `extract_discovery_events.py` — the ground truth

This is where the port most needs to *differ* from the paper, for the reasons in §3.1.3.

A candidate `(intervention, outcome)` pair becomes a **discovery event** dated at year
*y* when:

1. it is **co-mentioned in a result-bearing relation** (not merely co-present in the
   abstract) — polarity ∈ {`supports`, `contradicts`}, claim-strength = tested;
2. it is the **first** such occurrence in the corpus; **and**
3. **sustained adoption:** the pair recurs in ≥ *k* papers from ≥ *j* distinct
   disambiguated author groups within *N* years of *y*. Start at k=3, j=2, N=5, and
   report sensitivity across the grid.

Condition 3 is the defence against speculation-as-discovery, and it is cheap. It costs
recall and it right-censors the last *N* years (pairs from 2022+ cannot yet satisfy it) —
both must be stated, and the evaluation windows in §7 are set to respect the censoring.

Also emit, as separate first-class outputs:
- `contradicted_pairs` — pairs whose polarity flips over time. Motor learning has a lot
  of these and they are scientifically interesting in their own right; they also let us
  ask whether the model predicts *contested* pairs differently from robust ones.
- `speculative_pairs` — proposed but never tested. A natural second evaluation target:
  does the model anticipate what gets *proposed* vs. what gets *established*?

---

## 6. The scale problem — honest assessment

This is the real risk and it should be stated before any effort is committed.

| | Sourati & Evans | This corpus | Ratio |
|---|---|---|---|
| Papers | 1.5M (materials) / 27.5M (MEDLINE) | 36,338 | **40× / 750× smaller** |
| Candidate pool | 4,000 drugs / ~100k compounds | ≈300–800 interventions (est.) | ~10× smaller |
| Targets evaluated | 3 properties / 100–400 diseases | ≈30–60 outcomes (est.) | comparable |
| Walks per target | 250,000 | 250,000 (affordable) | — |

Four consequences, each with a mitigation:

1. **Precision@50 is near-degenerate.** Selecting 50 from a pool of ~500 is selecting
   10% of everything. *Mitigation:* report precision@10 and @20 as headline, keep @50
   only for comparability with the paper, and add rank-based metrics that do not depend
   on pool size — **AUC, MRR, mean percentile rank of true discoveries**.
2. **Thin 5-year windows in early decades.** Measured window sizes (papers in the five
   years preceding each prediction year):

   | Prediction year | 2000 | 2005 | 2010 | 2015 | 2020 |
   |---|---|---|---|---|---|
   | Papers in window | 1,085 | 1,775 | 3,555 | 5,677 | 8,161 |

   *Mitigation:* take **prediction years 2005–2020** as the evaluation grid — about 16
   years, comparable to their 18 — but treat it as two regimes rather than one. **2010–2020
   is the well-powered core** (≥3,500 papers/window); **2005–2009 is a genuinely sparse
   regime** (1,775–3,555) that should be reported separately rather than pooled. That split
   is not just damage control: §6's closing point is that the paper's advantage is largest
   when literature is sparse, so the thin years are the *interesting* ones — provided the
   per-year error bars are shown and the two regimes are never averaged together.
3. **Truncated author profiles — the serious one.** This is a *topical* corpus. A motor
   learning researcher's work outside motor learning is invisible, so the co-authorship
   network is severed and "expert density" is systematically under-measured. That
   directly attacks the paper's core mechanism. *Mitigation:* **expand author profiles
   via OpenAlex** — for each disambiguated author, fetch their full works list and use it
   to build author–author and author–concept edges even where the paper is out of corpus.
   Free, and the ingredient most likely to make or break the human-aware advantage.
   Treated as a decision point in §11, not assumed.
4. **Few targets ⇒ wide error bars.** *Mitigation:* evaluate across every outcome with
   sufficient support and report the *distribution* of per-target precision with
   bootstrap CIs, in the style of their Fig. 2e (100 diseases), never a single number.

**The upside of small.** The paper's own finding is that human-aware AI helps *most* when
literature is sparse (up to 400% for COVID-19, where relevant prior work barely existed).
A 36k-paper field is squarely in that regime, so this is a defensible — arguably
*pointed* — test of their claim rather than an apology for it. But absolute precisions
will be lower than theirs and the honest framing is "does the human-aware advantage
survive at field scale?", which is a genuinely interesting question and a publishable
result either way.

---

## 7. DAG-by-DAG implementation plan

Ten modules in `motor_learning_network/`, following the established house pattern
(module docstring listing outputs, `Final` typed constants at top, `@dataloader` /
`@datasaver`, `_main()` with `validate_execution` + `display_all_functions` +
`visualize_execution` into `FIGURES_PATH`, `USE_TRACKER = False` by default).

Per the repo's naming convention (no cryptic abbreviations), the paper's Greek
parameters get spelled-out names throughout:
**α → `concept_to_author_sampling_ratio`**, **β → `human_inaccessibility_mixing_weight`**,
**s → `transition_path_length`**.

| # | Module | Key outputs |
|---|---|---|
| 1 | `disambiguate_authors.py` | `disambiguated_authors.parquet`, `author_disambiguation_report.parquet` |
| 2 | `extract_science_entities.py` | `entity_mentions.parquet` (span, type, polarity, model/prompt provenance) |
| 3 | `canonicalize_entities.py` | `canonical_entities.parquet`, `entity_synonym_map.json`, `controlled_vocabulary_v*.json` |
| 4 | `build_research_hypergraph.py` | `research_hypergraph.pickle`, per-year incidence snapshots |
| 5 | `extract_discovery_events.py` | `discovery_events.parquet`, `contradicted_pairs.parquet`, `speculative_pairs.parquet` |
| 6 | `hypergraph_random_walks.py` | `random_walks/prediction_year=*/target=*.parquet` |
| 7 | `hypergraph_embeddings.py` | deepwalk vectors, transition-probability matrices, optional GraphSAGE vectors |
| 8 | `predict_discoveries.py` | `discovery_predictions.parquet`, `discovery_prediction_precision.parquet` |
| 9 | `complementary_hypotheses.py` | `alien_hypotheses.parquet` swept over `human_inaccessibility_mixing_weight` |
| 10 | `validate_discovery_hypergraph.py` | null models, ablations, baselines, the summary report |

### Notable design points

**(4) Hypergraph construction.** Store as a sparse incidence matrix (papers ×
entities), not a networkx object — 36k × ~70k with ~10 entities/paper is trivially
sparse, and the walk sampler wants CSR/CSC row and column slicing, which is exactly the
two operations a hyperedge walk needs. Materialize **per-prediction-year snapshots**
containing only the preceding 5 years, matching the paper. Snapshots are small; build
them all up front.

**(6) Random walks.** Non-lazy, truncated at 20 steps, 250,000 per target, with the
per-type sampling weight vector from §3.2. Pure CSR index arithmetic, embarrassingly
parallel over targets — a multiprocessing pool over CPU cores is entirely adequate; no
GPU needed. Persist walks to parquet so §7 and §9 re-read rather than resample.

**(7) Embeddings.** Strip author nodes from sequences *before* Word2Vec, as the paper
does. Skip-gram, window 8, dimension matched to the content baseline, 5 epochs. Also
implement the closed-form transition probabilities (`transition_path_length` ∈ {2, 3});
they need no walks at all and make an excellent cheap sanity check on the walk sampler —
if deepwalk similarity and the analytic transition probability disagree wildly, the
sampler is buggy.

**(8) Prediction & evaluation.** Prediction years 2005–2020. For each target outcome:
rank the intervention pool, take top-k, score cumulative precision in subsequent years
against §5.3 discovery events. Baselines, all required:
- random,
- **content-only Word2Vec over abstracts** (the paper's key comparison; the repo's
  `preprocess_text_for_embeddings.py` already prepares this text),
- **`concept_to_author_sampling_ratio → ∞`** (concepts only, authors ignored) — isolates
  the human-aware contribution, which is the whole scientific claim,
- authors-only,
- co-occurrence frequency (a dumb popularity prior — often embarrassingly strong, and
  worth knowing).

**(9) Complementary hypotheses.** Shortest-path distance for inaccessibility, content
cosine for plausibility, van der Waerden → Z-score → weighted mix, swept over
`human_inaccessibility_mixing_weight` ∈ [−1, 1]. Reproduce their wait-time analysis:
regress observed years-to-discovery on the weight. A positive slope replicating their
Fig. 6 would be a strong independent confirmation that the pipeline is working.

---

## 8. Validation plan

Validation is a deliverable, not a phase. It mirrors `detect_missing_links.py`'s
null-model idiom.

### 8.1 Null models
- **Degree-preserving hyperedge shuffling** — rewire entity-to-paper incidence keeping
  both node degree and hyperedge size. Kills structure, keeps marginals. Report the
  observed/null precision ratio and z-score, in the same form as the missing-links result.
- **Temporal shuffling** — permute publication years, destroying the arrow of time. Should
  collapse performance to chance; if it does not, there is leakage.
- **Label permutation** — shuffle which pairs are discovery events.

### 8.2 Extraction validation
- Human-annotated gold set (≈300 abstracts): per-type precision/recall/F1.
- **MeSH cross-check** (Ontology D): on PubMed-indexed papers, do LLM-extracted entities
  agree with independently-assigned MeSH descriptors? A cheap, label-free, external
  audit of the extractor at corpus scale.
- Inter-annotator agreement on a 50-abstract subset, so the gold set's own reliability
  is quantified.

### 8.3 Leakage audits — the most likely way to get a fake result
- Every temporal snapshot must be verified to contain **no paper from the prediction
  year or later**. Assert it in code, not in a comment.
- The **controlled vocabulary is induced from the whole corpus**, including the future.
  This is a real, subtle leak: the pool of candidate interventions "knows" which concepts
  eventually mattered. Quantify it by re-inducing the vocabulary from pre-2005 text only
  and re-running one prediction year. If the gap is large, vocabulary induction must be
  made per-snapshot.
- Confirm the content-only baseline is trained on the *same* temporal window.

### 8.4 Ablations
Node types (drop `task`, drop `construct`, drop `population`, authors-only,
concepts-only) × `concept_to_author_sampling_ratio` ∈ {0, 0.5, 1, 2, ∞} × disambiguation
quality (full / string-match / initials). Their α = 1 optimum is a **prediction this
project can independently test** — a genuine replication result, and one worth reporting
whichever way it falls.

### 8.5 Face validity
Hand the top predictions for 5–10 well-understood outcomes to Alfredo and to a domain
colleague, blind to whether each pair is a model prediction, a real recent discovery, or
a random pair. Cheap, and catches classes of failure no metric will.

---

## 9. Compute

The four A100 40GB cards are **not the bottleneck** — most of this pipeline is CPU-bound
and small.

| Stage | Cost | Hardware |
|---|---|---|
| Author disambiguation | blocking + logistic model over ~153k mentions | CPU, minutes |
| **LLM entity extraction over 34,895 abstracts** | **the only real compute sink** | A100s (local 7–14B, batched) or an API |
| Concept embedding / canonicalization | ~50k short strings | 1 GPU, minutes |
| Hypergraph construction | sparse, ~36k × ~70k | CPU, seconds |
| Random walks | 250k × 20 steps × ~50 targets × 16 years | CPU multiprocessing, hours |
| Deepwalk Word2Vec | small vocabulary, 5 epochs | CPU, minutes |
| GraphSAGE (optional) | ~100k nodes | 1 GPU, minutes |

Action items: add `gensim` and `scikit-learn` to the default pixi environment; decide
local-vs-API for extraction; **confirm the GPUs are actually visible** (`nvidia-smi`
currently fails on this host — §2.4).

---

## 10. Phasing, with a real kill gate

**Phase 0 — feasibility spike (≈2 weeks). Go/no-go.**
Sample 500 abstracts. Hand-extract or LLM-extract Ontology B entities. Answer, with
numbers: (a) how large is the induced intervention pool, really? (b) can an LLM
distinguish `supports` / `null` / `speculative` at usable accuracy? (c) how many
discovery events survive the §5.3 sustained-adoption criterion per year? **If (c) yields
fewer than ~50 events/year in the 2005–2020 window, the evaluation has no statistical
power and the project should be redesigned or stopped here.** Do not skip this gate; it
costs two weeks and can save six months.

**Phase 1 — author disambiguation** (§4). Standalone value to the repo regardless of
whether the rest proceeds: it improves every author-level analysis already here.

**Phase 2 — entities + vocabulary** (§5.1–5.2), including the gold set. Requires
Alfredo's domain input for vocabulary curation; schedule that as real time, not as a
review.

**Phase 3 — hypergraph + walks + embeddings** (§7 modules 4, 6, 7). Mostly mechanical
once 1 and 2 land.

**Phase 4 — prediction + validation** (§7 modules 5, 8, 10; §8). The scientific payoff.

**Phase 5 — complementary/alien hypotheses** (§7 module 9). Only meaningful once Phase 4
shows the plain predictions beat baselines; β-tuning on top of a model that does not work
is astrology.

Phases 1 and 2 are independent and can run concurrently.

---

## 11. Open decisions for Alfredo

1. **Ontology.** Confirm B (typed nodes, binary target), or override toward A (simplest,
   fastest) or D (MeSH-only, cheapest and most reproducible). Everything downstream keys
   off this.
2. **OpenAlex author-profile expansion** (§6.3). Fetching every disambiguated author's
   full works list is the single change most likely to determine whether the human-aware
   signal exists in a topical corpus. It is free but adds a large fetch stage and an
   out-of-corpus data dependency. **My recommendation: do it** — without it we may
   measure "the method fails" when what is true is "our author graph was severed."
3. **Extraction model.** Local model on the A100s (reproducible, no per-token cost,
   pinned weights) vs. a frontier API (better extraction, especially for polarity, but
   costs money and drifts). Recommendation: local for the full pass, frontier model for
   the gold set and for adjudicating disagreements.
4. **Discovery criterion thresholds** (§5.3): k, j, N. Phase 0 should return the data
   needed to set these; they are a domain judgement, not a tuning parameter.
5. **Scope of the deliverable.** Internal method development, or aimed at a paper? If the
   latter, the §6 framing ("does human-aware discovery prediction survive at single-field
   scale?") should shape the experiment grid from the start rather than be retrofitted.

---

## References

- Sourati, J. & Evans, J. A. (2023). Accelerating science with human-aware artificial
  intelligence. *Nature Human Behaviour* **7**, 1682–1696.
  Code: https://github.com/jsourati/accelerate-discoveries
- Tshitoyan, V. et al. (2019). Unsupervised word embeddings capture latent knowledge from
  materials science literature. *Nature* **571**, 95–98. (the content-only baseline)
- Perozzi, B. et al. (2014). DeepWalk. *KDD*.
- Chitra, U. & Raphael, B. (2019). Random walks on hypergraphs with edge-dependent vertex
  weights. *ICML*.
- Swanson, D. R. (1986). Fish oil, Raynaud's syndrome, and undiscovered public knowledge.
  *Perspect. Biol. Med.* **30**, 7–18.
