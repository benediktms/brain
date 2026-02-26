# Building a Knowledge Graph from Linked Notes

A personal knowledge base becomes more powerful when notes reference each other. Links create a graph structure where each note is a node and each reference is a directed edge. This graph enables retrieval strategies that go beyond simple text matching.

## Types of Links

In a Markdown-based vault there are two common link formats. Wiki-style links use double brackets: [[headings]] refers to another note by its filename. You can also use aliased wiki-links like [[frontmatter|the LanceDB decision note]] to control the display text while pointing to a different target.

Standard Markdown links work too: [the simple explanation](simple.md) uses explicit relative paths. Both formats should be extracted and stored so the system can compute backlinks and traverse the graph at query time.

## Why Backlinks Matter

When you search for a topic and find a relevant note, the notes that link to it are often relevant too. If [[headings]] explains vector embeddings and this note links to it, then a query about embeddings might benefit from surfacing this note as well, even if the word "embedding" does not appear prominently here.

This is the intuition behind graph expansion in hybrid retrieval. After finding the top seed results by vector similarity and keyword match, the system performs a one-hop traversal of the link graph to discover transitively relevant content. The [[tasks]] note might link to this one as a dependency, creating a chain that helps the agent understand project structure.

## Limitations

Graph expansion must be bounded. In a densely linked vault, expanding beyond one hop can pull in hundreds of marginally relevant notes. The system caps expansion at a fixed number of neighbors and relies on the hybrid scoring formula to rank them alongside direct matches.
