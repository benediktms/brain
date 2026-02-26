/// A chunk of text extracted from a Markdown file.
#[derive(Debug, Clone)]
pub struct Chunk {
    /// 0-indexed position within the source file.
    pub ord: usize,
    /// The chunk text content.
    pub content: String,
}

const MAX_CHUNK_CHARS: usize = 2000;

/// Split markdown `text` into chunks on paragraph and heading boundaries.
/// Chunks exceeding `MAX_CHUNK_CHARS` are split at the nearest sentence boundary.
pub fn chunk_text(text: &str) -> Vec<Chunk> {
    let raw_sections = split_on_boundaries(text);

    let mut chunks = Vec::new();
    let mut ord = 0;

    for section in raw_sections {
        let trimmed = section.trim();
        if trimmed.is_empty() {
            continue;
        }

        if trimmed.len() <= MAX_CHUNK_CHARS {
            chunks.push(Chunk {
                ord,
                content: trimmed.to_string(),
            });
            ord += 1;
        } else {
            for piece in split_long_text(trimmed, MAX_CHUNK_CHARS) {
                chunks.push(Chunk {
                    ord,
                    content: piece,
                });
                ord += 1;
            }
        }
    }

    chunks
}

/// Split on double newlines and heading lines (lines starting with `#`).
fn split_on_boundaries(text: &str) -> Vec<String> {
    let mut sections = Vec::new();
    let mut current = String::new();

    for line in text.lines() {
        if line.starts_with('#') && !current.trim().is_empty() {
            sections.push(std::mem::take(&mut current));
        }
        if !current.is_empty() || !line.is_empty() {
            if !current.is_empty() {
                current.push('\n');
            }
            current.push_str(line);
        } else if current.is_empty() && line.is_empty() && !sections.is_empty() {
            // Double newline: flush current section
        }
    }

    // Handle double-newline splits within accumulated text
    let mut result = Vec::new();
    for section in sections.into_iter().chain(std::iter::once(current)) {
        for part in section.split("\n\n") {
            let trimmed = part.trim();
            if !trimmed.is_empty() {
                result.push(trimmed.to_string());
            }
        }
    }

    result
}

/// Split text that exceeds `max_len` at sentence boundaries (`. `, `? `, `! `).
fn split_long_text(text: &str, max_len: usize) -> Vec<String> {
    let mut pieces = Vec::new();
    let mut remaining = text;

    while remaining.len() > max_len {
        let search_region = &remaining[..max_len];
        let split_pos = search_region
            .rmatch_indices(['.', '?', '!'])
            .find(|(i, _)| *i + 1 < search_region.len())
            .map(|(i, _)| i + 1)
            .unwrap_or(max_len);

        pieces.push(remaining[..split_pos].trim().to_string());
        remaining = remaining[split_pos..].trim_start();
    }

    if !remaining.trim().is_empty() {
        pieces.push(remaining.trim().to_string());
    }

    pieces
}
