# A Deep Dive into Information Retrieval

Information retrieval is the science of finding relevant documents from a large collection given a user query. It underpins search engines, recommendation systems, and the retrieval-augmented generation pattern that powers modern AI assistants. This note surveys the key techniques from classical term-frequency methods through modern hybrid approaches.

## Classical Foundations: TF-IDF

The simplest useful retrieval model is term frequency-inverse document frequency. The intuition is that a word appearing frequently in a document but rarely across the collection is likely important for that document.

### Term Frequency

Term frequency measures how often a term appears in a document. The raw count is often dampened to prevent long documents from dominating results. A common dampening function is logarithmic: tf(t,d) = 1 + log(count(t,d)) if count > 0, else 0. This ensures that a term appearing ten times is not scored ten times higher than a term appearing once.

### Inverse Document Frequency

Inverse document frequency measures how rare a term is across the entire collection. Common words like "the" and "is" appear in almost every document and carry little discriminative power. The standard formula is idf(t) = log(N / df(t)), where N is the total number of documents and df(t) is the number of documents containing term t. Rare terms get high IDF scores, amplifying their contribution to the final ranking.

### The TF-IDF Score

The final score for a term-document pair is the product: tfidf(t,d) = tf(t,d) * idf(t). For a multi-term query, the document score is typically the sum of TF-IDF scores across all query terms. This additive model is simple to implement and surprisingly effective for many retrieval tasks, but it has known weaknesses: it cannot capture synonyms, it treats each term independently, and it is sensitive to document length.

## BM25: The Probabilistic Improvement

BM25 (Best Matching 25) is a refinement of TF-IDF based on probabilistic relevance theory. It addresses the document length bias and provides better term frequency saturation.

### The BM25 Formula

The BM25 score for a query Q and document D is:

score(Q, D) = sum over terms t in Q of: idf(t) * (tf(t,D) * (k1 + 1)) / (tf(t,D) + k1 * (1 - b + b * |D| / avgdl))

Here k1 controls term frequency saturation (typically 1.2), b controls document length normalization (typically 0.75), |D| is the document length, and avgdl is the average document length in the collection.

### Why BM25 Outperforms TF-IDF

The key improvement is term frequency saturation. In TF-IDF, doubling the term count always increases the score. In BM25, the contribution of additional occurrences diminishes as the count grows, approaching an asymptotic limit controlled by k1. This prevents keyword-stuffed documents from dominating results.

The document length normalization controlled by b is also more principled. When b = 1, documents are fully normalized by length. When b = 0, length is ignored. The default of 0.75 provides a reasonable middle ground that penalizes long documents slightly but does not eliminate their advantage when they contain genuinely more information.

### BM25 in Practice

SQLite's FTS5 extension implements BM25 natively, making it straightforward to add keyword search to any application that already uses SQLite. The bm25() function returns negative scores (more negative means more relevant), which need to be inverted or normalized when combining with other signals.

Most modern search systems still use BM25 as a baseline or as one signal in a hybrid pipeline. Its simplicity, speed, and interpretability make it hard to beat for exact keyword matching.

## Dense Retrieval: Embedding-Based Search

Dense retrieval represents both queries and documents as continuous vectors in a shared embedding space. Instead of matching on exact terms, it measures semantic similarity between the query vector and document vectors using distance metrics like cosine similarity or dot product.

### How Dense Retrieval Works

A dual-encoder model (also called a bi-encoder) produces separate embeddings for the query and each document. At query time, the system computes the query embedding and finds the nearest document embeddings using approximate nearest neighbor search. This decoupled design allows pre-computing document embeddings offline, making query-time latency independent of collection size.

### Advantages Over Sparse Methods

Dense retrieval captures semantic relationships that keyword methods miss entirely. A query for "machine learning algorithms" can retrieve a document about "neural network training procedures" because the embedding model learned that these concepts are related. This is particularly valuable for knowledge bases where users often search with natural language questions rather than precise keyword queries.

Dense methods also handle synonyms, paraphrases, and cross-lingual similarity naturally, since the embedding model maps semantically equivalent texts to nearby points regardless of the specific words used.

### Limitations of Dense Retrieval

Dense retrieval has its own weaknesses. It struggles with exact term matching: a search for a specific error code like "E0308" may miss documents containing that exact string if the embedding model does not encode character-level patterns well. It also requires more computational resources for embedding generation and vector index maintenance.

Another challenge is the cold start problem. If a document contains novel terminology not well represented in the embedding model's training data, its embedding may not accurately capture its content. This is less of a concern for general English text but matters for highly specialized domains.

## Hybrid Retrieval: Combining Sparse and Dense

The most effective modern retrieval systems combine sparse (BM25) and dense (embedding) methods. Each approach has complementary strengths: BM25 excels at exact matches and rare terms, while dense retrieval captures semantic similarity and handles paraphrases.

### Reciprocal Rank Fusion

One simple way to combine results from multiple retrieval methods is reciprocal rank fusion. For each document, the fused score is the sum of 1/(k + rank_i) across all methods, where k is a constant (typically 60) and rank_i is the document's rank in method i. This approach is robust and does not require learning combination weights.

### Learned Score Combination

A more sophisticated approach learns weights for combining normalized scores from each method. The hybrid score might be: S = w_v * sim_v + w_k * bm25_normalized + w_r * recency + w_l * link_score. The weights can be fixed based on intent (a keyword-heavy query should upweight BM25, while a conceptual question should upweight the vector signal) or learned from relevance feedback.

### Candidate Pool Construction

In a hybrid system, the initial candidate pool is constructed by taking the top-N results from each method and merging them. Typical values are N=50 from each source, yielding a combined pool of up to 100 candidates after deduplication. This pool is then scored with the full hybrid formula and the top-k results are returned.

The merge step is important for recall: a document that ranks 45th in vector search and 30th in BM25 might score highly in the hybrid ranking even though it was not a top result in either individual method. The fusion surface discovers these synergistic matches.

## Evaluation Metrics

Measuring retrieval quality requires standardized metrics that capture different aspects of system performance.

### Precision and Recall

Precision measures the fraction of retrieved documents that are relevant. Recall measures the fraction of relevant documents that are retrieved. There is usually a tradeoff between the two: returning more results improves recall but may decrease precision.

For search systems, precision at k (P@k) is more practical than overall precision. P@5 measures the fraction of the top 5 results that are relevant, which directly reflects the user experience since most users only examine the first few results.

### Mean Reciprocal Rank

Mean reciprocal rank (MRR) measures how early the first relevant result appears. For a set of queries, MRR is the average of 1/rank_of_first_relevant_result. An MRR of 1.0 means the correct answer is always the first result. MRR is particularly useful for question-answering tasks where there is one correct answer.

### Normalized Discounted Cumulative Gain

NDCG is the standard metric for evaluating ranked results with graded relevance judgments. Unlike binary relevance, it accounts for the degree of relevance: a highly relevant document ranked third is penalized less than a marginally relevant document ranked first. The discount function (typically log2(rank+1)) gives higher positions more weight, reflecting that users pay more attention to top results.

### Recall at k for Retrieval-Augmented Generation

For retrieval-augmented generation, the critical metric is whether the retrieved context contains the information needed to answer the query correctly. Recall@k measures the fraction of queries for which at least one relevant document appears in the top-k results. For a knowledge base tool, Recall@5 or Recall@10 are the most important metrics because the agent's context window limits how many chunks it can consume.

## Token Budgeting: Retrieval for LLM Agents

When the consumer of retrieved results is a language model with a finite context window, retrieval must be budget-aware. Returning twenty long documents is worse than returning five focused snippets if the model can only process a limited number of tokens.

### The Budget-First Approach

A budget-first retrieval system takes a token budget as an input parameter alongside the query. After ranking candidates, it greedily packs results into the response until the budget is exhausted. This ensures the model receives the most relevant information that fits, rather than a fixed number of results that may overflow the context.

### Progressive Disclosure with Stubs and Expansion

A two-phase approach further improves token efficiency. In the first phase, the system returns compact stubs: a title, a two-sentence summary, relevance scores, and an expansion hint. The agent examines the stubs and decides which ones to expand into full content. This search-then-expand pattern typically uses 3-5x fewer tokens than returning full chunks upfront because the agent can skip irrelevant results without paying the token cost of reading them.

### Token Estimation

Accurate token counting requires running the model's tokenizer, which is expensive. A practical approximation is to estimate tokens as characters divided by four for English text. This estimate is within 10-15 percent of the true token count for typical prose and is orders of magnitude cheaper to compute.

## Approximate Nearest Neighbor Search

Exact nearest neighbor search compares the query vector against every document vector in the collection. For small collections this is fast enough, but as the number of vectors grows into the hundreds of thousands or millions, brute-force search becomes impractical. Approximate nearest neighbor (ANN) algorithms trade a small amount of recall for dramatic speedups.

### Inverted File Index (IVF)

IVF partitions the vector space into a fixed number of Voronoi cells using k-means clustering. Each vector is assigned to the cell whose centroid is closest. At query time, only the vectors in the nearest few cells are searched, reducing the number of comparisons by a factor proportional to the number of cells divided by the number of probes.

The key parameters are the number of cells (nlist) and the number of cells to probe at query time (nprobe). More cells give finer partitioning but require more training data. More probes improve recall but increase latency. For a collection of 100,000 vectors, 256 cells with 16 probes is a reasonable starting point that typically achieves over 95 percent recall with a 10-20x speedup over brute force.

### Hierarchical Navigable Small World Graphs (HNSW)

HNSW builds a multi-layer graph where each node is a vector. The bottom layer contains all vectors, while higher layers contain progressively fewer vectors, forming a hierarchy. Search starts at the top layer and greedily navigates toward the query, dropping to lower layers as it gets closer.

HNSW typically offers better recall-latency tradeoffs than IVF for in-memory datasets. It does not require a separate training step and handles insertions well. The main drawback is memory overhead: each vector requires additional storage for the graph edges, which can add 20-50 percent to the raw embedding storage.

### Product Quantization (PQ)

Product quantization compresses vectors by splitting each vector into subvectors and quantizing each subvector independently using a learned codebook. A 384-dimensional vector might be split into 48 subvectors of 8 dimensions each, and each subvector is replaced by its nearest codebook entry. This reduces storage from 1536 bytes to roughly 48 bytes per vector, a 32x compression ratio.

The tradeoff is reduced precision in distance computation. PQ distances are approximations of the true distances, which means some relevant vectors may be missed. In practice, combining PQ with IVF (IVF-PQ) or using PQ as a re-ranking step after an initial HNSW search gives a good balance of compression, speed, and accuracy.

### Choosing an Index Strategy

The right indexing strategy depends on the collection size and hardware constraints. For a personal knowledge base with fewer than 500,000 vectors on a modern laptop, a flat index or HNSW is usually sufficient. The latency difference between brute-force search over 100,000 vectors and an indexed search is typically under 50 milliseconds, which may not justify the complexity of maintaining an index.

For larger collections or resource-constrained environments, IVF-PQ provides excellent compression with manageable recall loss. LanceDB handles index selection and maintenance internally, which simplifies the application layer.

## Document Chunking Strategies

Before documents can be embedded, they must be split into chunks of appropriate size. Chunking strategy significantly affects retrieval quality because it determines the granularity of the retrieval unit.

### Fixed-Size Chunking

The simplest approach splits text into chunks of a fixed number of characters or tokens with optional overlap. Overlap ensures that information spanning a chunk boundary is not lost. A typical configuration is 500-token chunks with 50-token overlap.

Fixed-size chunking is easy to implement and produces predictable chunk sizes, which simplifies token budget management. However, it ignores document structure: a chunk boundary might split a paragraph mid-sentence or separate a heading from its content, degrading the semantic coherence of individual chunks.

### Structure-Aware Chunking

Structure-aware chunking uses document structure to guide split points. In Markdown, natural boundaries include headings, double newlines (paragraph breaks), and block-level elements like code fences and lists. The chunker splits on these boundaries and only falls back to character-based splitting when a section exceeds the maximum chunk size.

This approach produces semantically coherent chunks that map naturally to the document's organization. A chunk containing the content under a specific heading is more likely to be self-contained and meaningful than an arbitrary 500-token window. The tradeoff is variable chunk sizes, which complicates token budget calculations.

### Heading-Aware Chunking with Context

An enhancement to structure-aware chunking is to prepend the heading hierarchy to each chunk. If a chunk contains content under "## BM25 > ### The BM25 Formula", the heading path becomes part of the chunk text. This provides the embedding model with context about where the content sits in the document's structure, which can improve retrieval for queries that reference specific sections.

The heading context also enables the system to generate better summaries and stubs for the search-then-expand pattern, since the heading path often serves as a natural title for the chunk.

### Sentence-Level Splitting as a Fallback

When a section of text exceeds the maximum chunk size even after structural splitting, the system needs a fallback. Splitting at sentence boundaries (periods, question marks, exclamation marks followed by whitespace) preserves more meaning than splitting at arbitrary character positions. The chunker searches backward from the maximum length to find the nearest sentence boundary and splits there.

Edge cases include sentences that themselves exceed the maximum length (common in academic writing with long parenthetical clauses) and text that lacks sentence boundaries (like raw data or code). For these cases, a hard character-based split is the only option, but it should be rare in well-written prose.

## Cross-Encoder Reranking

While bi-encoder models are efficient for initial retrieval because query and document embeddings are computed independently, they sacrifice precision. A cross-encoder model jointly processes the query and document together, attending to fine-grained interactions between them.

### How Cross-Encoders Differ from Bi-Encoders

A bi-encoder produces separate embeddings for the query and document, then computes similarity via dot product. The query and document never "see" each other during encoding, which limits the model's ability to capture nuanced relevance signals.

A cross-encoder concatenates the query and document as a single input and produces a relevance score directly. Because the model can attend to both texts simultaneously, it captures word-level interactions like negation, qualification, and contextual disambiguation that bi-encoders miss. The cost is that the cross-encoder must run a full forward pass for every query-document pair, making it too expensive for initial retrieval over large collections.

### The Rerank Pipeline

The standard approach is a two-stage pipeline. First, a bi-encoder or hybrid system retrieves the top 50-100 candidates. Then, a cross-encoder reranks the top 10-30 candidates to produce the final result list. This concentrates the expensive cross-encoder computation on a small number of promising candidates.

For a personal knowledge base, reranking the top 20 candidates with a small cross-encoder model takes 200-500 milliseconds on a CPU. This is acceptable for interactive use but too slow to apply to the full candidate pool. The reranker is therefore an optional refinement step, triggered when the initial retrieval confidence is low or when the user explicitly requests higher precision.

### When Reranking Helps Most

Cross-encoder reranking provides the largest improvements when the initial retrieval produces many candidates with similar scores. If the top result is clearly the best match, reranking will not change the ordering. But when several candidates are close in score and the correct answer depends on subtle phrasing differences, the cross-encoder's ability to model fine-grained interactions makes a significant difference.

Reranking is particularly valuable for question-answering queries where the answer exists in a specific passage and the difference between the correct passage and a nearby but irrelevant passage is subtle. For broad exploratory queries where multiple documents are equally valid, reranking adds less value.

## The Role of Relevance Feedback

Retrieval systems can improve over time by incorporating signals about which results users find useful.

### Implicit Feedback

In a knowledge base tool, implicit feedback comes from which stubs the agent chooses to expand. If the agent consistently expands the third result rather than the first, this suggests the ranking is suboptimal. Over time, these expansion patterns can be used to adjust the hybrid scoring weights or fine-tune the embedding model.

### Explicit Feedback

Explicit relevance judgments are more informative but harder to collect at scale. In an interactive setting, the user or agent can mark results as relevant or irrelevant. These judgments can be used to train a learning-to-rank model that optimizes the weight combination for the specific brain's content distribution.

### Cold Start and Adaptation

A new knowledge base has no feedback data, so the system must rely on sensible defaults. The intent-driven weight profiles provide a starting point: lookup queries upweight BM25, planning queries upweight recency and links, and reflection queries upweight importance. As feedback accumulates, the system can adapt these defaults to the specific user's patterns.

## Query Understanding and Intent Classification

Not all queries are alike. A factual question like "what is the BM25 formula" requires a precise lookup, while an exploratory question like "what have I learned about performance optimization" requires a broad sweep across many notes. Effective retrieval systems adapt their behavior based on the inferred query intent.

### Intent Categories

A practical set of intent categories for a personal knowledge base includes: lookup (fact-finding), planning (what to do next), reflection (what happened and why), and synthesis (combining information to create something new). Each intent maps to a different weight profile in the hybrid scoring formula.

Lookup queries benefit from upweighting BM25 and tag matching because the user is searching for specific terms or topics. Planning queries benefit from upweighting recency and link structure because the user cares about what is current and how things connect. Reflection queries benefit from upweighting importance scores because the user wants high-signal summaries rather than raw details.

### Automatic Intent Detection

Intent can be provided explicitly by the caller or inferred from query features. Questions starting with "what is" or "how does" suggest lookup intent. Questions containing temporal markers like "recently" or "this week" suggest planning or reflection intent. Questions with action verbs like "design" or "write" suggest synthesis intent.

A simple keyword-based classifier is sufficient for most cases. The fallback is an "auto" intent that uses equal weights across all signals, which produces reasonable results for ambiguous queries. The intent system is designed to be good enough rather than perfect, since even a rough intent classification meaningfully improves result relevance compared to static weights.

### Query Expansion

Another query understanding technique is query expansion, where the system augments the original query with related terms. For a query about "embedding models", the system might also search for "vector representations" and "sentence transformers". This improves recall for queries that use different terminology than the stored documents.

Query expansion can be implemented using the embedding model itself: embed the query, find the nearest terms in the vocabulary, and add them to the BM25 search. Alternatively, a thesaurus or the brain's own link structure can suggest related terms. The risk of query expansion is topic drift, where added terms pull in irrelevant results. Limiting expansion to a small number of high-confidence terms mitigates this risk.

## Indexing Pipeline Architecture

The journey from a raw Markdown file to searchable chunks involves multiple processing stages, each with its own concerns and failure modes.

### File Discovery and Change Detection

The first stage discovers which files need processing. A file watcher monitors the brain directory for changes, but it cannot catch modifications that happen while the daemon is not running. A startup scan compares the current filesystem state against the metadata store to detect offline changes.

Content hashing provides a gate that prevents redundant work. Each file's content is hashed, and the hash is compared against the stored hash from the last indexing run. Only files with changed hashes are sent through the rest of the pipeline. This is crucial for performance because the embedding stage is expensive, and re-embedding unchanged files wastes both CPU and time.

### Parsing and Structural Analysis

The parsing stage converts raw Markdown text into a structured representation. A pull parser like pulldown-cmark processes the text event by event, identifying headings, paragraphs, code blocks, links, frontmatter, and other structural elements. The parser builds a heading hierarchy that maps each content block to its position in the document's outline.

This structural information drives the chunking stage and is also stored as metadata. The heading path for each chunk enables features like hierarchical navigation and context-aware chunk display. Links extracted during parsing feed the graph expansion system.

### Embedding Generation

The embedding stage converts text chunks into dense vectors. Chunks are processed in batches to amortize the overhead of model inference. The batch size is a tradeoff between throughput and memory: larger batches are more efficient on the GPU or CPU but require more memory for the intermediate tensors.

For CPU-based inference with BGE-small, a batch size of 32 chunks typically provides good throughput without excessive memory pressure. Each batch requires tokenizing the inputs, running the forward pass, extracting CLS embeddings, and normalizing the resulting vectors. The tokenizer handles variable-length inputs by padding shorter sequences and truncating sequences that exceed the model's maximum length of 512 tokens.

### Storage and Consistency

The final stage writes the processed data to the dual-store system. Chunk metadata, links, and structural information go to SQLite in a single transaction. Embedding vectors go to LanceDB via merge_insert. The ordering matters for consistency: if the SQLite write succeeds but the LanceDB write fails, the system records this state so that it can retry the LanceDB write on the next run without re-running the entire pipeline.

This two-phase approach is not a true distributed transaction, but it provides practical consistency guarantees for a single-user system. The key invariant is that the content hash is only updated after both stores have been successfully written, so a crash at any point results in at most one redundant re-indexing of the affected file.

## Memory Tiers and Progressive Retrieval

A knowledge base that serves an AI agent must be conscious of the agent's context window. Dumping raw text into the prompt is wasteful when a compact summary would suffice. The memory tier model organizes information by token cost and recall fidelity.

### Tier 1: Raw Chunks (High Cost, High Fidelity)

Raw chunks are the full text of each section of a document. They provide complete information but consume the most tokens. A single chunk might be 200-500 tokens, and returning ten chunks could consume half of a small context window. Raw chunks are the last resort, used only when the agent needs exact wording or detailed context.

### Tier 2: Structured Metadata (Medium Cost, Medium Fidelity)

Structured metadata includes tags, backlink counts, timestamps, heading paths, and other extracted information. This metadata consumes very few tokens but provides useful signals for the agent to decide which results to explore further. A metadata stub might be 30-50 tokens, allowing the system to present many more results within the same budget.

### Tier 3: Summaries and Reflections (Low Cost, Variable Fidelity)

Summaries compress the information from multiple chunks or episodes into a compact form. A two-sentence summary might capture the key insight of a 500-token chunk in 30 tokens. Reflections go further, synthesizing patterns across multiple episodes into higher-level observations.

The fidelity of summaries depends on the summarization method. Extractive summaries (selecting key sentences) preserve exact wording but may miss important context. Abstractive summaries (generated by a language model) capture meaning more flexibly but may introduce inaccuracies. For a personal knowledge base, deterministic extractive summaries generated at ingest time provide a reliable baseline, with optional LLM-generated abstractions available as a refinement.

### The Progressive Retrieval Pattern

The agent interacts with the knowledge base through a progressive pattern. First, it issues a search with a small token budget, receiving compact stubs from Tiers 2 and 3. It examines the stubs and selects the most promising ones for expansion. The expansion call returns the full Tier 1 content for the selected chunks, consuming a larger portion of the budget.

This two-phase pattern is more efficient than returning full content upfront because the agent can skip irrelevant results without paying their token cost. In practice, agents typically expand only two or three of the top five stubs, saving 60-80 percent of the tokens that would have been spent returning all five as full chunks.

## Putting It All Together

A complete retrieval pipeline for a personal knowledge base combines all of these techniques. Documents are chunked and embedded at index time. At query time, BM25 and vector search produce initial candidates, which are fused and scored with a hybrid formula that includes recency, link structure, and other signals. Results are packed within a token budget and returned as compact stubs. The agent expands selected stubs to full content on demand.

This layered architecture means each component can be improved independently. Swapping in a better embedding model improves vector search without touching the keyword pipeline. Adding a graph expansion step surfaces linked content without changing the scoring formula. The modularity is intentional: it lets the system evolve incrementally while maintaining a stable interface for the consuming agent.

The field of information retrieval is mature but still evolving rapidly. The integration of neural methods with classical approaches has produced systems that are both more capable and more practical than either approach alone. For a local-first knowledge base, the combination of SQLite FTS5, a small embedding model, and a straightforward hybrid ranker provides excellent retrieval quality with minimal resource requirements.
