# Mapping a Scientific Field: Citation Networks Meets Large Language Models

Mapping a field of science means answering a structural question: what are its
subfields, how do they relate, which are emerging and which are fading, all inferred from
the record the field leaves behind: its publications and the citations that
link them. The state of the art now runs on two complementary signals that were,
until recently, pursued separately. Citation networks capture how the community
*organizes itself* through its referencing behavior, while large language models (LLMs)
and their embedding predecessors capture what papers *say*. The frontier of 2023–2026
is the fusion of the two, and it is worth understanding each half before the seam.

## The citation-structural tradition

The idea that citations encode the intellectual structure of science is old and
was operationalized in three moves. [Garfield (1955)](https://doi.org/10.1126/science.122.3159.108)
proposed the citation index as an instrument for science, and [Price (1965)](https://doi.org/10.1126/science.149.3683.510)
first treated the literature as a network of papers with measurable topology.
The two relations that still underpin every science map were defined in the same
decade: [Kessler (1963)](https://doi.org/10.1002/asi.5090140103) introduced
*bibliographic coupling* (two papers are related if they cite the same earlier
work), and [Small (1973)](https://doi.org/10.1002/asi.4630240406) introduced its
forward-looking dual, *co-citation*, in which two papers are related if they are later
cited together. Bibliographic coupling is fixed at publication and is stronger for
recent work; co-citation accrues over time and is stronger for established
literature. Direct citation is the third relation, and the choice among the three
is not cosmetic. [Boyack and Klavans (2010)](https://doi.org/10.1002/asi.21419)
compared all three on a large corpus and found that they carve up the same
literature differently, with direct-citation and co-citation clustering better
matching expert judgment for different granularities. Their later head-to-head,
[Klavans and Boyack (2017)](https://doi.org/10.1002/asi.23734), remains the
reference result on *which* citation relation yields the most accurate taxonomy of
science, concluding that direct citation on a large document-level graph is the
strongest single basis for a global map.

That conclusion depended on a methodological shift from journal-level and
co-citation maps to clustering the full document-level citation graph.
[Waltman and van Eck (2012)](https://doi.org/10.1002/asi.22748) built the first
publication-level classification of all of science by clustering tens of millions
of articles on their direct-citation links, and [Waltman et al. (2016)](https://doi.org/10.1371/journal.pone.0154404)
systematized the comparison of citation-relation choices for such clustering. The
enabling algorithms came from network science rather than scientometrics.
[Blondel et al. (2008)](https://doi.org/10.1088/1742-5468/2008/10/p10008) gave the
Louvain method for modularity-based community detection at scale, and
[Rosvall and Bergstrom (2008)](https://doi.org/10.1073/pnas.0706851105) gave the
information-theoretic Infomap alternative. Louvain's tendency to produce badly
connected or internally fragmented clusters was corrected by
[Traag et al. (2019)](https://doi.org/10.1038/s41598-019-41695-z), whose Leiden
algorithm guarantees well-connected communities and is now the default partitioner
for large citation graphs.

These methods reached practitioners through a small set of tools that define how
most field maps are actually produced today. [Van Eck and Waltman (2010)](https://doi.org/10.1007/s11192-009-0146-3)
released VOSviewer, still the most widely used program for constructing and
visualizing co-citation, coupling, and co-word maps; [Chen (2006)](https://doi.org/10.1002/asi.20317)
released CiteSpace, which is oriented specifically toward detecting emerging trends
and "research fronts" through citation-burst detection; and
[Aria and Cuccurullo (2017)](https://doi.org/10.1016/j.joi.2017.08.007) released
the *bibliometrix* R package, which opened scriptable, reproducible science mapping
to a broad audience. [Chen (2017)](https://doi.org/10.1515/jdis-2017-0006) reviews
the science-mapping literature these tools serve. The pipeline they encode (retrieve a corpus, build a citation or co-word network, cluster it, label the
clusters, and visualize) is the baseline that the LLM era is now modifying at almost
every step.

## Infrastructure and the science-of-science turn

Two developments moved the field from bespoke studies toward a data-rich,
model-driven science of science. First, the theory matured:
[Fortunato et al. (2018)](https://doi.org/10.1126/science.aao0185) consolidated
"the science of science" as a quantitative program, and results such as
[Wang, Song, and Barabási (2013)](https://doi.org/10.1126/science.1237825) on the
long-term citation dynamics of individual papers showed that citation trajectories
follow learnable regularities rather than noise — a premise on which predictive
field-mapping rests. Second, the data opened up. The proprietary corpora that
early maps depended on gave way to large open indexes: Microsoft Academic Graph,
introduced by [Sinha et al. (2015)](https://doi.org/10.1145/2740908.2742839), and
after its retirement its open successor [OpenAlex (Priem, Piwowar, and Orr, 2022)](https://doi.org/10.48550/arxiv.2205.01833),
which now indexes hundreds of millions of works with their citation links and is
the substrate for a growing share of mapping studies. Derived resources such as
[SciSciNet (2023)](https://doi.org/10.1038/s41597-023-02198-9) package this graph
with linked funding, patent, and author data for direct analysis. This open
infrastructure is what makes LLM-scale text processing over full fields feasible,
because a map now needs both the citation edges and the abstracts, and both are
retrievable at corpus scale.

## From Words to Representations

The first substantive contribution of modern NLP was to give every paper a dense
vector that captures its content, replacing the sparse keyword and co-word features
of classical maps. [Beltagy et al. (2019)](https://doi.org/10.18653/v1/d19-1371)
released SciBERT, a transformer pretrained on scientific text, and, most
consequentially for mapping, [Cohan et al. (2020)](https://doi.org/10.18653/v1/2020.acl-main.207)
released SPECTER, which trains document embeddings using the *citation graph itself*
as the supervision signal, so that papers close in citation space land close in
vector space. This is the pivotal idea for the fusion: SPECTER and its successor
benchmark [SciRepEval (Singh et al., 2022)](https://doi.org/10.48550/arxiv.2211.13308)
make citation structure and textual content two views of one embedding rather than
two separate maps to reconcile. [González-Márquez et al. (2024)](https://doi.org/10.1016/j.patter.2024.100968)
demonstrate the payoff at full scale, embedding roughly 21 million PubMed abstracts
into a single two-dimensional atlas of biomedical research whose regions correspond
to recognizable subfields — a content-based map of an entire domain that would have
been infeasible with co-citation methods alone.

On the clustering-and-labeling side, neural topic models displaced Latent Dirichlet
Allocation. [BERTopic (Grootendorst, 2022)](https://doi.org/10.48550/arxiv.2203.05794)
clusters documents in embedding space and labels each cluster with a class-based
TF-IDF procedure, and comparative studies on scientific corpora,
[Egger and Yu (2022)](https://doi.org/10.3389/fsoc.2022.886498) across LDA, NMF,
Top2Vec, and BERTopic, and the [BERTeley](https://doi.org/10.1016/j.nlp.2023.100044)
benchmark on scientific articles specifically, generally find embedding-based
topic models produce more coherent, better-separated themes than bag-of-words
methods on research text. At this stage the "LLM" contribution is representational:
better vectors and better clusters, but a human still reads and names the clusters.

## The Frontier: LLMs in the Loop

The genuinely new work of the last two years puts a generative LLM *inside* the
mapping loop rather than only at the embedding stage, and it splits into three
threads.

The first uses LLMs to solve the long-standing weak link of every clustering
pipeline — turning an anonymous cluster of papers or keywords into an accurate,
human-readable label and a coherent hierarchy.
[Zhu et al. (2025)](https://doi.org/10.48550/arxiv.2509.19125) generate
context-aware *hierarchical* taxonomies of a scientific corpus with an LLM,
directly targeting the tree-of-subfields that a field map is supposed to produce;
this is the most direct LLM analogue of the classical cluster-and-label step, and
it treats the taxonomy itself as the generation target rather than a post-hoc gloss.

The second thread is automated synthesis: given a field, produce the survey.
[AutoSurvey (Wang et al., 2024)](https://doi.org/10.48550/arxiv.2406.10252) and
retrieval-augmented scientific agents such as
[PaperQA (Lála et al., 2023)](https://doi.org/10.48550/arxiv.2312.07559) retrieve
relevant literature and draft grounded, cited synthesis, effectively automating the
narrative layer that sits on top of a structural map. These systems are where
hallucinated-citation risk is most acute, which is why retrieval grounding, in which every
claim is tied to a retrieved source, is their central design constraint rather than
an add-on.

The third and most integrative thread treats the citation graph and the language
model as a single system. The broader "graph-meets-LLM" program, surveyed by
[Li et al. (2023)](https://doi.org/10.48550/arxiv.2311.12399), asks how to feed
graph structure to models that natively consume text, and the scientific-LLM
landscape is catalogued by [Zhang et al. (2024)](https://doi.org/10.18653/v1/2024.emnlp-main.498).
The most pointed recent evidence on the fusion is
[Camelo-Guerrero and Díaz-Rodríguez (2026)](https://doi.org/10.48550/arxiv.2605.24351),
who ask directly *how much* citation structure an LLM needs for bibliometric tasks,
probing whether the graph adds signal beyond what the model already extracts from
text, which is precisely the open question the fusion raises. The evidence so far
suggests the two signals are complementary rather than redundant: citation edges
encode community-level relatedness that text embeddings miss, while LLMs supply the
labeling, summarization, and reasoning that citation structure alone cannot.

## Where this leaves the state of the art

A current best-practice field map is a hybrid pipeline. The corpus and its citation
edges come from an open index such as OpenAlex; the structural backbone comes from
Leiden clustering of the direct-citation graph, following the Waltman–van Eck and
Klavans–Boyack line; each paper carries a citation-informed embedding in the
SPECTER lineage so that content and structure are jointly represented; clusters are
named and organized into a hierarchy by an LLM rather than by hand; and a
retrieval-grounded LLM drafts the synthesis over the result. No single published
system yet integrates all of these into a validated, general-purpose field-mapping
tool — that integration is the visible gap. The specific open problems are the
quantitative payoff of citation structure over text alone
([Camelo-Guerrero and Díaz-Rodríguez, 2026](https://doi.org/10.48550/arxiv.2605.24351)),
the reliability and citation-faithfulness of LLM-generated labels and syntheses
([AutoSurvey](https://doi.org/10.48550/arxiv.2406.10252); [PaperQA](https://doi.org/10.48550/arxiv.2312.07559)),
and the lack of shared benchmarks that evaluate an end-to-end *map* against expert
ground truth the way [Klavans and Boyack (2017)](https://doi.org/10.1002/asi.23734)
evaluated citation-relation choices two clustering generations ago. The pieces are
mature and individually validated; the assembled, validated whole is the frontier.
