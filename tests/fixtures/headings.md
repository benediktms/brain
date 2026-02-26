# Understanding Vector Embeddings

Vector embeddings are dense numerical representations of text that capture semantic meaning. Unlike keyword-based approaches where "dog" and "canine" are completely unrelated strings, an embedding model maps both words to nearby points in a high-dimensional space because they share meaning.

## How Embedding Models Work

Transformer-based models like BERT read a sequence of tokens and produce a hidden state for each token. For sentence-level embeddings, we typically take the hidden state of the first token (the CLS token) as a summary of the entire input. The model is trained on large corpora so that semantically similar texts produce similar hidden states.

### The Role of the CLS Token

The CLS token is a special token prepended to every input sequence. During pre-training, the model learns to aggregate sentence-level information into this position. After the forward pass, extracting the CLS hidden state gives us a fixed-size vector regardless of input length.

### Fine-Tuning for Retrieval

General-purpose BERT embeddings are not optimal for similarity search out of the box. Models like BGE-small-en-v1.5 are fine-tuned on retrieval tasks using contrastive learning: the model is trained to push embeddings of relevant query-document pairs closer together and irrelevant pairs further apart.

## Similarity and Distance

Once we have two vectors, we need a way to measure how close they are. The most common metrics are cosine similarity and dot product.

### Cosine Similarity

Cosine similarity measures the angle between two vectors, ignoring their magnitude. It ranges from -1 (opposite) to 1 (identical direction). For normalized vectors (unit length), cosine similarity equals the dot product, which is cheaper to compute.

### Why Normalize to Unit Length

L2 normalization projects every vector onto the unit hypersphere. After normalization, dot product and cosine similarity are equivalent. This saves compute at query time because dot product requires one fewer operation than explicit cosine. It also means all vectors are directly comparable regardless of the quirks of the embedding model's output scale.

## Practical Considerations

### Dimensionality

BGE-small uses 384 dimensions, which is a good balance between expressiveness and efficiency. Higher dimensions capture more nuance but cost more memory and compute. For a personal knowledge base with tens of thousands of chunks, 384 dimensions are more than sufficient.

#### Storage Costs

Each 384-dimensional float32 vector occupies 1,536 bytes. For 100,000 chunks this totals roughly 150 megabytes of raw embedding data, well within laptop constraints.

#### Search Latency

With proper indexing, approximate nearest neighbor search over 100k vectors completes in under 50 milliseconds. Without an index, brute-force scan is still fast at this scale because modern CPUs handle SIMD dot products efficiently.
