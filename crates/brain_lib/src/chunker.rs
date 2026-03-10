use crate::parser::{ParsedDocument, Section};
use crate::tokens::estimate_tokens;
use crate::utils::content_hash;

/// Current chunker algorithm version.
/// Bump this when the chunking algorithm changes to trigger re-chunking.
pub const CHUNKER_VERSION: u32 = 2;

/// Max tokens per chunk (~400 tokens, leaving headroom for BGE-small's 512 limit).
const MAX_CHUNK_TOKENS: usize = 400;

/// A chunk of text extracted from a Markdown file.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
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
    let content = &section.content;
    let content_base = section.content_byte_start;

    // Track paragraph positions within section.content so we can compute
    // accurate byte offsets in the original document.
    let para_positions = paragraph_byte_positions(content);

    let mut buf = String::new();
    let mut buf_byte_start = para_positions
        .first()
        .map(|(_, off)| content_base + off)
        .unwrap_or(content_base);

    for (para, para_offset) in &para_positions {
        let para_byte_start = content_base + para_offset;
        let para_byte_end = para_byte_start + para.len();
        let para_tokens = estimate_tokens(para);

        // Single paragraph exceeds limit — split at sentence boundaries
        if para_tokens > MAX_CHUNK_TOKENS {
            // Flush buffer first
            if !buf.trim().is_empty() {
                let trimmed = buf.trim().to_string();
                let tokens = estimate_tokens(&trimmed);
                chunks.push(Chunk {
                    ord: *ord,
                    content: trimmed,
                    heading_path: section.heading_path.clone(),
                    byte_start: buf_byte_start,
                    byte_end: para_byte_start,
                    token_estimate: tokens,
                    chunk_hash: content_hash(buf.trim()),
                });
                *ord += 1;
                buf.clear();
            }

            // Split the oversized paragraph and track piece positions
            let mut piece_byte_start = para_byte_start;
            for piece in split_at_sentences(para, MAX_CHUNK_TOKENS) {
                let tokens = estimate_tokens(&piece);
                chunks.push(Chunk {
                    ord: *ord,
                    content: piece.clone(),
                    heading_path: section.heading_path.clone(),
                    byte_start: piece_byte_start,
                    byte_end: piece_byte_start + piece.len(),
                    token_estimate: tokens,
                    chunk_hash: content_hash(&piece),
                });
                piece_byte_start += piece.len();
                // Skip whitespace between pieces in original text
                while piece_byte_start < para_byte_end
                    && content_base + content.len() > piece_byte_start
                {
                    let offset_in_content = piece_byte_start - content_base;
                    if offset_in_content < content.len()
                        && content.as_bytes()[offset_in_content].is_ascii_whitespace()
                    {
                        piece_byte_start += 1;
                    } else {
                        break;
                    }
                }
                *ord += 1;
            }
            buf_byte_start = para_byte_end;
            continue;
        }

        let combined_tokens = estimate_tokens(&buf) + para_tokens + 1;
        if combined_tokens > MAX_CHUNK_TOKENS && !buf.trim().is_empty() {
            let trimmed = buf.trim().to_string();
            let tokens = estimate_tokens(&trimmed);
            chunks.push(Chunk {
                ord: *ord,
                content: trimmed,
                heading_path: section.heading_path.clone(),
                byte_start: buf_byte_start,
                byte_end: para_byte_start,
                token_estimate: tokens,
                chunk_hash: content_hash(buf.trim()),
            });
            *ord += 1;
            buf.clear();
            buf_byte_start = para_byte_start;
        }

        if !buf.is_empty() {
            buf.push_str("\n\n");
        }
        buf.push_str(para);
    }

    // Flush remaining buffer
    if !buf.trim().is_empty() {
        let trimmed = buf.trim().to_string();
        let tokens = estimate_tokens(&trimmed);
        chunks.push(Chunk {
            ord: *ord,
            content: trimmed,
            heading_path: section.heading_path.clone(),
            byte_start: buf_byte_start,
            byte_end: section.byte_end,
            token_estimate: tokens,
            chunk_hash: content_hash(buf.trim()),
        });
        *ord += 1;
    }

    chunks
}

/// Find byte positions of each trimmed paragraph within a content string.
///
/// Returns `(trimmed_paragraph, byte_offset_in_content)` pairs.
fn paragraph_byte_positions(content: &str) -> Vec<(&str, usize)> {
    let mut positions = Vec::new();
    let mut offset = 0;

    for raw in content.split("\n\n") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            let leading = raw.len() - raw.trim_start().len();
            positions.push((trimmed, offset + leading));
        }
        offset += raw.len() + 2; // +2 for the \n\n separator
    }

    positions
}

/// Split text at sentence boundaries (`. `, `? `, `! `) to stay within token limit.
fn split_at_sentences(text: &str, max_tokens: usize) -> Vec<String> {
    let max_bytes = max_tokens * 4;
    let mut pieces = Vec::new();
    let mut remaining = text;

    while estimate_tokens(remaining) > max_tokens {
        let search_end = snap_to_char_boundary(remaining, remaining.len().min(max_bytes));
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

/// Snap a byte index down to the nearest UTF-8 char boundary.
fn snap_to_char_boundary(s: &str, index: usize) -> usize {
    if index >= s.len() {
        return s.len();
    }
    let mut i = index;
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
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

    // ─── UTF-8 multibyte byte-offset tests ───────────────────────

    #[test]
    fn test_byte_offsets_utf8_small_sections() {
        let text = "# Emoji 🧠\n\nContent with 🧠 brain emoji.\n\n## 日本語\n\nこんにちは世界。\n";
        let doc = parse_document(text);
        let chunks = chunk_document(&doc);

        assert!(!chunks.is_empty());
        for chunk in &chunks {
            assert!(chunk.byte_end <= text.len(), "byte_end out of bounds");
            assert!(chunk.byte_start <= chunk.byte_end, "start > end");
            let slice = &text[chunk.byte_start..chunk.byte_end];
            assert!(
                std::str::from_utf8(slice.as_bytes()).is_ok(),
                "byte range should produce valid UTF-8"
            );
        }
    }

    #[test]
    fn test_byte_offsets_utf8_oversized_paragraph_split() {
        let sentence = "日本語のテスト文。";
        let long_para = sentence.repeat(100);
        let text = format!("# CJK\n\n{long_para}\n");
        let doc = parse_document(&text);
        let chunks = chunk_document(&doc);

        assert!(
            chunks.len() > 1,
            "CJK text should be split into multiple chunks"
        );
        for chunk in &chunks {
            assert!(chunk.byte_end <= text.len(), "byte_end out of bounds");
            assert!(chunk.byte_start <= chunk.byte_end, "start > end");
            let slice = &text[chunk.byte_start..chunk.byte_end];
            assert!(
                std::str::from_utf8(slice.as_bytes()).is_ok(),
                "split byte range should produce valid UTF-8, got panic at byte_start={} byte_end={}",
                chunk.byte_start,
                chunk.byte_end
            );
        }
    }

    #[test]
    fn test_byte_offsets_utf8_emoji_sentence_split() {
        let sentence = "This has an emoji 🧠 in it! ";
        let long_para = sentence.repeat(80);
        let text = format!("# Emoji split\n\n{long_para}\n");
        let doc = parse_document(&text);
        let chunks = chunk_document(&doc);

        assert!(chunks.len() > 1, "long emoji text should be split");
        for chunk in &chunks {
            assert!(chunk.byte_end <= text.len(), "byte_end out of bounds");
            let slice = &text[chunk.byte_start..chunk.byte_end];
            assert!(
                std::str::from_utf8(slice.as_bytes()).is_ok(),
                "emoji sentence split should produce valid UTF-8"
            );
        }
    }

    #[test]
    fn test_byte_offsets_utf8_accented_oversized() {
        let sentence = "Les données naïves coûtent très cher à résoudre. ";
        let long_para = sentence.repeat(50);
        let text = format!("# Résumé\n\n{long_para}\n");
        let doc = parse_document(&text);
        let chunks = chunk_document(&doc);

        assert!(chunks.len() > 1, "accented text should be split");
        for chunk in &chunks {
            assert!(chunk.byte_end <= text.len(), "byte_end out of bounds");
            let slice = &text[chunk.byte_start..chunk.byte_end];
            assert!(
                std::str::from_utf8(slice.as_bytes()).is_ok(),
                "accented split should produce valid UTF-8"
            );
        }
    }

    #[test]
    fn test_byte_offsets_content_matches_slice() {
        let text = "# Title\n\nFirst paragraph with é.\n\n## 日本\n\nSecond with 🧠.\n";
        let doc = parse_document(text);
        let chunks = chunk_document(&doc);

        for chunk in &chunks {
            let slice = &text[chunk.byte_start..chunk.byte_end];
            assert!(
                slice.contains(chunk.content.trim()),
                "byte slice should contain chunk content.\n  content: '{}'\n  slice:   '{}'",
                chunk.content.trim(),
                slice.trim()
            );
            // Verify the slice isn't wildly larger than content (heading + delimiters only)
            assert!(
                slice.len() <= chunk.content.len() + 50,
                "byte slice is suspiciously larger than content ({} vs {} bytes)",
                slice.len(),
                chunk.content.len()
            );
        }
    }

    // ─── CRLF byte-offset tests ─────────────────────────────────

    #[test]
    fn test_byte_offsets_crlf_basic() {
        let text = "# Title\r\n\r\nContent here.\r\n\r\n## Next\r\n\r\nMore content.\r\n";
        let doc = parse_document(text);
        let chunks = chunk_document(&doc);

        assert!(!chunks.is_empty());
        for chunk in &chunks {
            assert!(chunk.byte_end <= text.len(), "CRLF byte_end out of bounds");
            assert!(chunk.byte_start <= chunk.byte_end, "CRLF start > end");
            let slice = &text[chunk.byte_start..chunk.byte_end];
            assert!(
                std::str::from_utf8(slice.as_bytes()).is_ok(),
                "CRLF byte range should produce valid UTF-8"
            );
        }
    }

    #[test]
    fn test_byte_offsets_crlf_oversized() {
        let sentence = "This is a test sentence with CRLF endings. ";
        let long_para = sentence.repeat(50);
        let text = format!("# CRLF\r\n\r\n{long_para}\r\n");
        let doc = parse_document(&text);
        let chunks = chunk_document(&doc);

        assert!(chunks.len() > 1, "CRLF oversized text should be split");
        for chunk in &chunks {
            assert!(chunk.byte_end <= text.len(), "CRLF byte_end out of bounds");
            let slice = &text[chunk.byte_start..chunk.byte_end];
            assert!(
                std::str::from_utf8(slice.as_bytes()).is_ok(),
                "CRLF split byte range should produce valid UTF-8"
            );
        }
    }

    #[test]
    fn test_byte_offsets_crlf_with_utf8() {
        let text = "# Título\r\n\r\nContenido con ñ y 🎉 celebración.\r\n\r\n## Sección\r\n\r\nMás texto aquí.\r\n";
        let doc = parse_document(text);
        let chunks = chunk_document(&doc);

        for chunk in &chunks {
            assert!(chunk.byte_end <= text.len());
            let slice = &text[chunk.byte_start..chunk.byte_end];
            assert!(
                std::str::from_utf8(slice.as_bytes()).is_ok(),
                "CRLF+UTF-8 chunker byte range should produce valid UTF-8"
            );
        }
    }

    // ─── Offset non-overlap / ordering tests ─────────────────────

    #[test]
    fn test_byte_offsets_no_overlap_between_chunks() {
        let text = "# A\n\nParagraph A content.\n\n## B\n\nParagraph B content.\n\n## C\n\nParagraph C content.\n";
        let doc = parse_document(text);
        let chunks = chunk_document(&doc);

        for window in chunks.windows(2) {
            assert!(
                window[0].byte_end <= window[1].byte_start,
                "chunks should not have overlapping byte ranges: [{}, {}) vs [{}, {})",
                window[0].byte_start,
                window[0].byte_end,
                window[1].byte_start,
                window[1].byte_end,
            );
        }
    }

    #[test]
    fn test_byte_offsets_no_overlap_oversized_split() {
        let sentence = "This is sentence number one. This is sentence two. And the third. ";
        let long_para = sentence.repeat(40);
        let text = format!("# Big\n\n{long_para}\n");
        let doc = parse_document(&text);
        let chunks = chunk_document(&doc);

        assert!(chunks.len() > 1, "should produce multiple chunks");
        for window in chunks.windows(2) {
            assert!(
                window[0].byte_end <= window[1].byte_start,
                "split chunks should not overlap: [{}, {}) vs [{}, {})",
                window[0].byte_start,
                window[0].byte_end,
                window[1].byte_start,
                window[1].byte_end,
            );
        }
    }
}
