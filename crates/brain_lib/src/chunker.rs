use crate::parser::{ParsedDocument, Section};
use crate::tokens::estimate_tokens;
use crate::utils::content_hash;

/// Current chunker algorithm version.
/// Bump this when the chunking algorithm changes to trigger re-chunking.
pub const CHUNKER_VERSION: u32 = 2;

/// Max tokens per chunk (~400 tokens, leaving headroom for BGE-small's 512 limit).
const MAX_CHUNK_TOKENS: usize = 400;

/// A chunk of text extracted from a Markdown file.
#[derive(Debug, Clone)]
pub struct Chunk {
    /// 0-indexed position within the source file.
    pub ord: usize,
    /// The chunk text content.
    pub content: String,
    /// Heading hierarchy path (e.g. "# Title > ## Section").
    pub heading_path: String,
    /// Start byte offset in the source document.
    pub byte_start: usize,
    /// End byte offset in the source document.
    pub byte_end: usize,
    /// Estimated token count.
    pub token_estimate: usize,
    /// BLAKE3 hash of the chunk content.
    pub chunk_hash: String,
}

/// Chunk a parsed document into heading-aware semantic chunks.
///
/// Primary split: heading boundaries (from the parser's sections).
/// Secondary split: paragraph boundaries within oversized sections.
/// Tertiary split: sentence boundaries for very long paragraphs.
pub fn chunk_document(doc: &ParsedDocument) -> Vec<Chunk> {
    let mut chunks = Vec::new();
    let mut ord = 0;

    for section in &doc.sections {
        if section.content.trim().is_empty() {
            continue;
        }

        let tokens = estimate_tokens(&section.content);
        if tokens <= MAX_CHUNK_TOKENS {
            chunks.push(Chunk {
                ord,
                content: section.content.clone(),
                heading_path: section.heading_path.clone(),
                byte_start: section.byte_start,
                byte_end: section.byte_end,
                token_estimate: tokens,
                chunk_hash: content_hash(&section.content),
            });
            ord += 1;
        } else {
            // Split oversized section at paragraph boundaries
            let sub_chunks = split_section(section, &mut ord);
            chunks.extend(sub_chunks);
        }
    }

    chunks
}

/// Split an oversized section at paragraph boundaries, then sentence boundaries.
fn split_section(section: &Section, ord: &mut usize) -> Vec<Chunk> {
    let mut chunks = Vec::new();
    let paragraphs: Vec<&str> = section
        .content
        .split("\n\n")
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .collect();

    let mut buf = String::new();
    let mut buf_start = section.byte_start;

    for para in &paragraphs {
        let para_tokens = estimate_tokens(para);

        // Single paragraph exceeds limit — split at sentence boundaries
        if para_tokens > MAX_CHUNK_TOKENS {
            // Flush buffer first
            if !buf.trim().is_empty() {
                let content = buf.trim().to_string();
                let tokens = estimate_tokens(&content);
                chunks.push(Chunk {
                    ord: *ord,
                    content: content.clone(),
                    heading_path: section.heading_path.clone(),
                    byte_start: buf_start,
                    byte_end: buf_start + buf.len(),
                    token_estimate: tokens,
                    chunk_hash: content_hash(&content),
                });
                *ord += 1;
                buf.clear();
            }

            for piece in split_at_sentences(para, MAX_CHUNK_TOKENS) {
                let tokens = estimate_tokens(&piece);
                chunks.push(Chunk {
                    ord: *ord,
                    content: piece.clone(),
                    heading_path: section.heading_path.clone(),
                    byte_start: buf_start,
                    byte_end: buf_start + piece.len(),
                    token_estimate: tokens,
                    chunk_hash: content_hash(&piece),
                });
                *ord += 1;
            }
            buf_start += para.len() + 2; // +2 for \n\n
            continue;
        }

        let combined_tokens = estimate_tokens(&buf) + para_tokens + 1; // +1 for separator
        if combined_tokens > MAX_CHUNK_TOKENS && !buf.trim().is_empty() {
            // Flush buffer
            let content = buf.trim().to_string();
            let tokens = estimate_tokens(&content);
            chunks.push(Chunk {
                ord: *ord,
                content: content.clone(),
                heading_path: section.heading_path.clone(),
                byte_start: buf_start,
                byte_end: buf_start + buf.len(),
                token_estimate: tokens,
                chunk_hash: content_hash(&content),
            });
            *ord += 1;
            buf.clear();
            buf_start += content.len() + 2;
        }

        if !buf.is_empty() {
            buf.push_str("\n\n");
        }
        buf.push_str(para);
    }

    // Flush remaining buffer
    if !buf.trim().is_empty() {
        let content = buf.trim().to_string();
        let tokens = estimate_tokens(&content);
        chunks.push(Chunk {
            ord: *ord,
            content: content.clone(),
            heading_path: section.heading_path.clone(),
            byte_start: buf_start,
            byte_end: section.byte_end,
            token_estimate: tokens,
            chunk_hash: content_hash(&content),
        });
        *ord += 1;
    }

    chunks
}

/// Split text at sentence boundaries (`. `, `? `, `! `) to stay within token limit.
fn split_at_sentences(text: &str, max_tokens: usize) -> Vec<String> {
    let max_chars = max_tokens * 4; // rough char estimate
    let mut pieces = Vec::new();
    let mut remaining = text;

    while estimate_tokens(remaining) > max_tokens {
        let search_end = remaining.len().min(max_chars);
        let search_region = &remaining[..search_end];
        let split_pos = search_region
            .rmatch_indices(['.', '?', '!'])
            .find(|(i, _)| *i + 1 < search_region.len())
            .map(|(i, _)| i + 1)
            .unwrap_or(search_end);

        let piece = remaining[..split_pos].trim();
        if !piece.is_empty() {
            pieces.push(piece.to_string());
        }
        remaining = remaining[split_pos..].trim_start();
    }

    if !remaining.trim().is_empty() {
        pieces.push(remaining.trim().to_string());
    }

    pieces
}

/// Simple backward-compatible chunker for plain text (used in legacy code paths).
pub fn chunk_text(text: &str) -> Vec<Chunk> {
    use crate::parser::parse_document;
    chunk_document(&parse_document(text))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_document;
    use std::path::PathBuf;

    fn fixture(name: &str) -> String {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures")
            .join(name);
        std::fs::read_to_string(path).expect("fixture should exist")
    }

    #[test]
    fn test_empty_document() {
        let doc = parse_document("");
        let chunks = chunk_document(&doc);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_single_section_small() {
        let doc = parse_document("# Title\n\nShort paragraph.");
        let chunks = chunk_document(&doc);

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].heading_path, "# Title");
        assert_eq!(chunks[0].content, "Short paragraph.");
        assert_eq!(chunks[0].ord, 0);
        assert!(chunks[0].token_estimate > 0);
        assert!(!chunks[0].chunk_hash.is_empty());
    }

    #[test]
    fn test_multiple_sections() {
        let text = "# A\n\nContent A.\n\n## B\n\nContent B.\n\n## C\n\nContent C.\n";
        let doc = parse_document(text);
        let chunks = chunk_document(&doc);

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].heading_path, "# A");
        assert_eq!(chunks[1].heading_path, "# A > ## B");
        assert_eq!(chunks[2].heading_path, "# A > ## C");

        // Ordinals are sequential
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.ord, i);
        }
    }

    #[test]
    fn test_oversized_section_splits() {
        // Create a section with >400 tokens (~1600+ chars)
        let long_para = "This is a sentence. ".repeat(100);
        let text = format!("# Big\n\n{long_para}");
        let doc = parse_document(&text);
        let chunks = chunk_document(&doc);

        assert!(
            chunks.len() > 1,
            "oversized section should be split into multiple chunks"
        );

        // All chunks should respect the token limit (with some tolerance)
        for chunk in &chunks {
            assert!(
                chunk.token_estimate <= MAX_CHUNK_TOKENS + 10,
                "chunk should be within token limit, got {}",
                chunk.token_estimate
            );
        }

        // All share the same heading_path
        for chunk in &chunks {
            assert_eq!(chunk.heading_path, "# Big");
        }
    }

    #[test]
    fn test_empty_sections_skipped() {
        let text = "# A\n## B\n## C\n\nContent C.\n";
        let doc = parse_document(text);
        let chunks = chunk_document(&doc);

        // Only section C has content
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].content, "Content C.");
    }

    #[test]
    fn test_chunk_hashes_differ() {
        let text = "# A\n\nFoo.\n\n## B\n\nBar.\n";
        let doc = parse_document(text);
        let chunks = chunk_document(&doc);

        assert_eq!(chunks.len(), 2);
        assert_ne!(chunks[0].chunk_hash, chunks[1].chunk_hash);
    }

    #[test]
    fn test_chunk_hashes_deterministic() {
        let text = "# Title\n\nSame content.\n";
        let doc1 = parse_document(text);
        let doc2 = parse_document(text);
        let chunks1 = chunk_document(&doc1);
        let chunks2 = chunk_document(&doc2);

        assert_eq!(chunks1[0].chunk_hash, chunks2[0].chunk_hash);
    }

    #[test]
    fn test_headings_fixture() {
        let text = fixture("headings.md");
        let doc = parse_document(&text);
        let chunks = chunk_document(&doc);

        assert!(!chunks.is_empty());

        // All chunks should have heading paths
        for chunk in &chunks {
            assert!(
                !chunk.heading_path.is_empty(),
                "headings fixture chunks should all have heading paths"
            );
        }
    }

    #[test]
    fn test_frontmatter_fixture() {
        let text = fixture("frontmatter.md");
        let doc = parse_document(&text);
        let chunks = chunk_document(&doc);

        assert!(!chunks.is_empty());

        // Content should not contain frontmatter delimiters
        for chunk in &chunks {
            assert!(
                !chunk.content.starts_with("---"),
                "chunk content should not contain frontmatter"
            );
        }
    }

    #[test]
    fn test_backward_compat_chunk_text() {
        let chunks = chunk_text("# Hello\n\nWorld.\n");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].content, "World.");
    }

    #[test]
    fn test_byte_offsets_within_bounds() {
        let text = fixture("simple.md");
        let doc = parse_document(&text);
        let chunks = chunk_document(&doc);

        for chunk in &chunks {
            assert!(
                chunk.byte_start <= chunk.byte_end,
                "byte_start should be <= byte_end"
            );
            assert!(
                chunk.byte_end <= text.len(),
                "byte_end should be <= text.len()"
            );
        }
    }
}
