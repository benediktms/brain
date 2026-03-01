use crate::parser::ParsedDocument;

/// Generate a deterministic capsule (short summary) from a parsed document.
///
/// Rule-based (no ML): title from heading_path or frontmatter, first meaningful
/// sentence, and heading outline.
pub fn generate_capsule(doc: &ParsedDocument) -> String {
    let mut parts = Vec::new();

    // Title: prefer frontmatter title, fall back to first heading
    let title = doc
        .frontmatter
        .get("title")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| {
            doc.sections
                .iter()
                .find(|s| !s.heading_path.is_empty())
                .map(|s| {
                    // Extract the leaf heading text (after last " > ")
                    s.heading_path
                        .rsplit(" > ")
                        .next()
                        .unwrap_or(&s.heading_path)
                        .trim_start_matches('#')
                        .trim()
                        .to_string()
                })
        });

    if let Some(title) = title {
        parts.push(title);
    }

    // First meaningful sentence: first non-empty content, first sentence
    if let Some(first_sentence) = first_meaningful_sentence(doc) {
        parts.push(first_sentence);
    }

    // Heading outline: list of top-level headings
    let outline = heading_outline(doc);
    if !outline.is_empty() {
        parts.push(format!("Sections: {outline}"));
    }

    parts.join(" — ")
}

/// Extract the first meaningful sentence from the document.
fn first_meaningful_sentence(doc: &ParsedDocument) -> Option<String> {
    for section in &doc.sections {
        let content = section.content.trim();
        if content.is_empty() {
            continue;
        }

        // Find first sentence boundary
        let sentence = if let Some(pos) = content.find(". ") {
            &content[..=pos]
        } else if content.ends_with('.') {
            content
        } else {
            // Take up to first newline or first 200 chars
            let end = content.find('\n').unwrap_or_else(|| content.len().min(200));
            &content[..end]
        };

        let sentence = sentence.trim();
        if !sentence.is_empty() {
            return Some(sentence.to_string());
        }
    }
    None
}

/// Build a comma-separated outline of top-level headings.
fn heading_outline(doc: &ParsedDocument) -> String {
    let headings: Vec<&str> = doc
        .sections
        .iter()
        .filter_map(|s| {
            if s.heading_path.is_empty() {
                return None;
            }
            // Only include direct headings (no " > " means top-level,
            // or one " > " means second-level which is common for ## headings)
            let depth = s.heading_path.matches(" > ").count();
            if depth > 1 {
                return None;
            }
            // Extract leaf heading text
            Some(
                s.heading_path
                    .rsplit(" > ")
                    .next()
                    .unwrap_or(&s.heading_path)
                    .trim_start_matches('#')
                    .trim(),
            )
        })
        .collect();

    headings.join(", ")
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
    fn test_capsule_with_title_and_headings() {
        let text = "# My Document\n\nThis is the first paragraph. It has details.\n\n## Section A\n\nContent A.\n\n## Section B\n\nContent B.\n";
        let doc = parse_document(text);
        let capsule = generate_capsule(&doc);

        assert!(capsule.contains("My Document"), "should contain title");
        assert!(
            capsule.contains("This is the first paragraph."),
            "should contain first sentence"
        );
        assert!(capsule.contains("Section A"), "should contain heading A");
        assert!(capsule.contains("Section B"), "should contain heading B");
    }

    #[test]
    fn test_capsule_frontmatter_title() {
        let text = "---\ntitle: Decision Note\n---\n\n# Heading\n\nFirst line of content.\n";
        let doc = parse_document(text);
        let capsule = generate_capsule(&doc);

        assert!(
            capsule.starts_with("Decision Note"),
            "should prefer frontmatter title, got: {capsule}"
        );
    }

    #[test]
    fn test_capsule_no_headings() {
        let text = "Just plain text without any headings or structure.\n";
        let doc = parse_document(text);
        let capsule = generate_capsule(&doc);

        assert!(
            capsule.contains("Just plain text"),
            "should contain the content"
        );
    }

    #[test]
    fn test_capsule_empty_document() {
        let doc = parse_document("");
        let capsule = generate_capsule(&doc);
        assert!(capsule.is_empty());
    }

    #[test]
    fn test_capsule_frontmatter_fixture() {
        let text = fixture("frontmatter.md");
        let doc = parse_document(&text);
        let capsule = generate_capsule(&doc);

        assert!(
            capsule.contains("Choosing LanceDB"),
            "should contain title from frontmatter, got: {capsule}"
        );
    }

    #[test]
    fn test_capsule_headings_fixture() {
        let text = fixture("headings.md");
        let doc = parse_document(&text);
        let capsule = generate_capsule(&doc);

        assert!(!capsule.is_empty());
        assert!(capsule.contains("Sections:"), "should have heading outline");
    }
}
