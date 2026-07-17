# Next Steps v2: From a Citation Map to a Validated, LLM-Fused Field Map of Motor Learning

*Revision of `next_steps.md`. What changed: the lab has since implemented an
embedding/topic stage (SPECTER2 **and** Gemini embeddings → BERTopic) and compared the
resulting text clusters against the citation-network (CN) communities, specifically looking
for papers that are on the same topic but sit in different CN clusters and do not cite each
other. That comparison found real candidates but is so far **qualitative**. v2 (a) promotes
that idea to a first-class step with a quantitative program, and (b) re-sequences the
original plan around it. The strategic thesis of v1 is unchanged and not repeated in full
here: algorithms provide auditable structure, LLMs only interpret it.*

---

## 1. What changed since v1

| v1 assumption | Current reality |
|---|---|
| Text side = SPECTER2 CLS + BERTopic, "computed but used in isolation" | Multiple embedding models (SPECTER2, Gemini) + BERTopic topics, already **compared** against Leiden CN communities |
| Step 3 (citation vs. text agreement) not started | **Half-done**: qualitative comparison exists; quantitative metrics missing |
| Disagreement between views = methodological question ("which partition is better?") | Disagreement is also a **discovery instrument**: same-topic / different-community / unlinked pairs are candidate *missing citations* — disconnected literatures |

The reframing in the last row is the most novel piece of the whole roadmap. It is
Swanson's "undiscovered public knowledge" (the ABC model of literature-based discovery)
implemented with modern embeddings on a field-scale citation graph, and it is publishable
on its own — *if* it becomes quantitative. That is the new Step 3b, and most of the
re-sequencing exists to serve it.

---

## 2. New Step 3b — Missing-link detection, made quantitative

**Claim to establish:** pairs of papers that are textually on the same topic but live in
different citation communities and do not cite each other occur *in excess of chance*, and
the method that finds them *prospectively predicts* real (future) citations.

Ordered from cheap sanity gate to headline result:

### (i) Excess over a null model *(do first — sanity gate)*
Count same-topic / cross-community / unlinked pairs. Compare against a null distribution
obtained by (a) permuting topic labels while holding the citation graph fixed, and/or
(b) degree-preserving rewiring of the graph while holding topics fixed. Report z-score /
p-value. If the count is not in excess of chance, stop and rethink before investing in the
rest.

### (ii) Retrospective prediction on the existing time slices *(the killer test)*
We already have `citation_network_until_{2010,2015,2020,...}`. Run candidate-pair
detection on the **2015 slice only** (same topic, different community, no link — using only
pre-2015 information), then check outcomes by 2020/2026: did the pair become linked,
directly or via bibliographic coupling? Compare against matched controls:
- same-topic, *same*-community unlinked pairs;
- random unlinked pairs matched on degree and publication year.

Report **precision@k and lift** over controls. If candidates get "healed" by future
citations at a significantly higher rate, the quantitative claim is clean: *the method
prospectively predicts missing citations*. All required data already exists.

### (iii) Benchmark against structural link prediction
Score candidate non-edges with structure-only predictors (Adamic–Adar, common
bibliographic-coupling count, node2vec similarity) **and** with embedding similarity.
Compare AUC / precision on the ground truth from (ii). If embedding similarity finds true
missing links that structural predictors rank low, we have quantitatively shown that
**text adds signal beyond the graph** — the Camelo-Guerrero / Klavans–Boyack open question
(v1 Step 3), answered on our corpus.

### (iv) Graded severity instead of binary "missing"
For each candidate pair report graph distance and coupling strength. Same topic at
distance 2 (they cite common work but not each other) is a mundane miss; same topic in
different weakly-connected regions is a genuine **silo**. Stratify all reported results by
this scale.

### Guardrails (both mandatory)

- **Data-completeness confound.** An apparent missing citation may be missing *data*, not
  a missing citation. Before counting a pair as unlinked, verify the non-edge via the
  existing reference-resolution plumbing (`find_missing_citations.py`, OpenAlex /
  OpenCitations). Report edge coverage of the corpus alongside every result; otherwise the
  whole effect can be attributed to database gaps.
- **Embedding circularity.** This *inverts* v1 Step 4(b) advice for this use case.
  SPECTER2's proximity adapter is trained on citation signal — using it to find missing
  citations is circular (it co-locates cited pairs by construction). For **discovery**, use
  content-only embeddings (Gemini, or raw SPECTER2 base) as the independent signal; keep
  the citation-aware adapter for the fusion/labeling steps (Step 4). Since both embedding
  families already exist, comparing candidate sets from citation-aware vs. content-only
  embeddings is itself an informative ablation — include it.

**How, in this repo.** New Hamilton module (e.g. `detect_missing_links.py`) consuming the
per-slice GraphMLs, the BERTopic assignments, and the embeddings parquet; outputs a
candidate-pairs parquet keyed by `(doi_a, doi_b, slice)` with topic id, community ids,
embedding similarity, graph distance, coupling count, structural-predictor scores, and
(for historical slices) the future-link outcome label.

---

## 3. Revised step list

Steps keep their v1 numbers where unchanged; see `next_steps.md` for full rationale.

- **Step 1 — LLM cluster labeling (`Ranked` pipeline).** Unchanged. *Gains a second job:*
  adjudicate top-ranked Step 3b candidate pairs ("given both abstracts, is B relevant
  enough that A should plausibly cite it?") as a precision filter. Still interpreter, never
  structure-generator — the LLM ranks/filters candidates the algorithms produced; it never
  proposes pairs itself.
- **Step 2 — Reference grounding.** Unchanged, and now doubly load-bearing: the same
  OpenAlex plumbing powers Step 3b's completeness guardrail.
- **Step 3 — Citation vs. text agreement.** Partially done (qualitatively). Finish the
  quantitative half: ARI / NMI / V-measure between Leiden and BERTopic partitions, plus
  cross-metrics (citation-graph modularity under the text partition; silhouette in
  embedding space under the citation partition). Run per embedding model (SPECTER2 vs.
  Gemini) — the agreement gap between them is part of the result. This provides the global
  context in which the pair-level Step 3b result sits.
- **Step 3b — Missing-link detection *(new; see §2)*.**
- **Step 4 — Fusion.** Unchanged in content, with the circularity caveat: 4(b) (SPECTER2
  proximity adapter) remains the right call **for fusion and labeling**, but is explicitly
  the *wrong* embedding for Step 3b discovery.
- **Step 5 — Temporal alignment.** Unchanged, but **raised in priority**: Step 3b(ii) and
  temporal community alignment share the same per-slice preprocessing, so build them
  together.
- **Step 6 — Expert ground-truth validation.** Unchanged, plus a new concrete artifact:
  expert judgments on a stratified sample of Step 3b candidate pairs (not only cluster
  memberships). This doubles as the calibration set for the Step 1 LLM pair-adjudicator —
  a low-effort way to start Step 6 early.
- **Step 7 — Retrieval-grounded synthesis.** Unchanged capstone. Cross-silo candidate
  pairs from Step 3b are exactly the "bridges" a synthesis narrative should highlight.

## 4. Revised near-term sequence

1. **Step 3b(i)** — null-model excess test. Days of work; gates everything else.
2. **Step 3b(ii)** — time-slice retrospective prediction (build shared per-slice
   preprocessing with Step 5 in mind).
3. **Step 3b(iii) + Step 3 completion** — structural baselines + partition-agreement
   metrics; together they answer "does text add signal beyond the graph?" quantitatively.
4. **Steps 1+2** — LLM labeling with grounding, now including the pair-adjudication
   filter over Step 3b's top candidates.
5. **Step 6 (start)** — expert sample over both clusters and candidate pairs; needs human
   calendar time, so start in parallel with 4.
6. **Step 5** — full temporal alignment (cheap once 3b(ii)'s preprocessing exists).
7. **Step 4(b), then 4(a)** — adapter fix for the fusion/labeling track, then true fusion.
8. **Step 7** — synthesis capstone.

## 5. Methodological cautions (v1's two, plus two new)

- **Keep the LLM out of structure.** Unchanged hard constraint — it labels, describes, and
  filters what the algorithms produced; it never creates pairs, merges, or splits clusters.
- **Direct citation stays the backbone.** Unchanged; coupling / co-citation are baselines.
- **Match the embedding to the task.** Citation-aware embeddings (SPECTER2 + proximity
  adapter) for fusion and labeling; content-only embeddings (Gemini, SPECTER2 base) for
  missing-link discovery. Never use a citation-trained embedding to claim a missing
  citation.
- **No missing-link claim without the completeness check.** Every reported candidate pair
  must have its non-edge verified against OpenAlex/OpenCitations, and every aggregate
  result must be accompanied by the corpus edge-coverage figure.

## 6. Key sources map (additions to v1's list)

- **Literature-based discovery framing** — Swanson (1986) "undiscovered public knowledge" /
  ABC model — Step 3b's intellectual lineage; cite in any write-up.
- **Link prediction baselines** — Adamic–Adar; node2vec (Grover & Leskovec 2016) — Step 3b(iii).
- **Citation-vs-text comparison metrics** — arXiv 2309.06160, arXiv 2406.06454 (as in v1) —
  now serve Step 3 *and* Step 3b's framing.
- All v1 sources remain as mapped there.
