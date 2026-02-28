use std::collections::HashMap;

use pulldown_cmark::{Event, HeadingLevel, MetadataBlockKind, Options, Parser, Tag, TagEnd};

/// A parsed Markdown document with frontmatter and heading-organized sections.
#[derive(Debug, Clone)]
pub struct ParsedDocument {
    /// YAML frontmatter key-value pairs.
    pub frontmatter: HashMap<String, serde_yaml::Value>,
    /// Sections split by heading boundaries.
    pub sections: Vec<Section>,
}

/// A section of a Markdown document, defined by heading boundaries.
#[derive(Debug, Clone)]
pub struct Section {
    /// Heading hierarchy path, e.g. `"# Title > ## Section"`.
    /// Empty string for content before the first heading.
    pub heading_path: String,
    /// Body content of the section (trimmed markdown, excludes the heading line).
    pub content: String,
    /// Start byte offset in the source text (includes heading).
    pub byte_start: usize,
    /// End byte offset in the source text.
    pub byte_end: usize,
}

struct HeadingInfo {
    level: HeadingLevel,
    text: String,
    byte_start: usize,
    byte_end: usize,
}

/// Parse a Markdown document into structured sections with frontmatter.
pub fn parse_document(text: &str) -> ParsedDocument {
    let opts = Options::ENABLE_YAML_STYLE_METADATA_BLOCKS
        | Options::ENABLE_TABLES
        | Options::ENABLE_TASKLISTS
        | Options::ENABLE_STRIKETHROUGH;

    let parser = Parser::new_ext(text, opts);

    let mut frontmatter = HashMap::new();
    let mut headings = Vec::new();
    let mut in_heading = false;
    let mut in_metadata = false;
    let mut heading_text = String::new();
    let mut heading_start = 0;
    let mut heading_level = HeadingLevel::H1;
    let mut body_start = 0;

    for (event, range) in parser.into_offset_iter() {
        match &event {
            Event::Start(Tag::MetadataBlock(MetadataBlockKind::YamlStyle)) => {
                in_metadata = true;
            }
            Event::End(TagEnd::MetadataBlock(MetadataBlockKind::YamlStyle)) => {
                in_metadata = false;
                body_start = range.end;
            }
            Event::Start(Tag::Heading { level, .. }) => {
                in_heading = true;
                heading_text.clear();
                heading_start = range.start;
                heading_level = *level;
            }
            Event::End(TagEnd::Heading(_)) => {
                headings.push(HeadingInfo {
                    level: heading_level,
                    text: std::mem::take(&mut heading_text),
                    byte_start: heading_start,
                    byte_end: range.end,
                });
                in_heading = false;
            }
            Event::Text(t) | Event::Code(t) => {
                if in_metadata {
                    if let Ok(fm) = serde_yaml::from_str(t) {
                        frontmatter = fm;
                    }
                } else if in_heading {
                    heading_text.push_str(t);
                }
            }
            _ => {}
        }
    }

    let sections = build_sections(text, &headings, body_start);
    ParsedDocument {
        frontmatter,
        sections,
    }
}

fn build_sections(text: &str, headings: &[HeadingInfo], body_start: usize) -> Vec<Section> {
    let mut sections = Vec::new();
    let mut heading_stack: Vec<(HeadingLevel, &str)> = Vec::new();

    // Content between body_start and first heading
    let first_heading_start = headings.first().map(|h| h.byte_start).unwrap_or(text.len());
    let pre_content = text[body_start..first_heading_start].trim();
    if !pre_content.is_empty() {
        sections.push(Section {
            heading_path: String::new(),
            content: pre_content.to_string(),
            byte_start: body_start,
            byte_end: first_heading_start,
        });
    }

    for (i, heading) in headings.iter().enumerate() {
        // Update heading stack: pop headings at same level or deeper
        let level_num = heading_level_num(heading.level);
        while heading_stack
            .last()
            .is_some_and(|(l, _)| heading_level_num(*l) >= level_num)
        {
            heading_stack.pop();
        }
        heading_stack.push((heading.level, &heading.text));

        // Section content: from end of heading to start of next heading (or EOF)
        let content_start = heading.byte_end;
        let content_end = headings
            .get(i + 1)
            .map(|h| h.byte_start)
            .unwrap_or(text.len());
        let content = text[content_start..content_end].trim();

        sections.push(Section {
            heading_path: format_heading_path(&heading_stack),
            content: content.to_string(),
            byte_start: heading.byte_start,
            byte_end: content_end,
        });
    }

    sections
}

fn heading_level_num(level: HeadingLevel) -> u8 {
    match level {
        HeadingLevel::H1 => 1,
        HeadingLevel::H2 => 2,
        HeadingLevel::H3 => 3,
        HeadingLevel::H4 => 4,
        HeadingLevel::H5 => 5,
        HeadingLevel::H6 => 6,
    }
}

fn format_heading_path(stack: &[(HeadingLevel, &str)]) -> String {
    stack
        .iter()
        .map(|(level, text)| {
            let marker = "#".repeat(heading_level_num(*level) as usize);
            format!("{marker} {text}")
        })
        .collect::<Vec<_>>()
        .join(" > ")
}

#[cfg(test)]
mod tests {
    use super::*;
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
        assert!(doc.frontmatter.is_empty());
        assert!(doc.sections.is_empty());
    }

    #[test]
    fn test_no_headings() {
        let text = "Just some plain text.\n\nWith multiple paragraphs.\n";
        let doc = parse_document(text);

        assert!(doc.frontmatter.is_empty());
        assert_eq!(doc.sections.len(), 1);
        assert_eq!(doc.sections[0].heading_path, "");
        assert!(doc.sections[0].content.contains("plain text"));
        assert!(doc.sections[0].content.contains("multiple paragraphs"));
    }

    #[test]
    fn test_frontmatter_parsing() {
        let text = fixture("frontmatter.md");
        let doc = parse_document(&text);

        assert!(doc.frontmatter.contains_key("title"));
        assert!(doc.frontmatter.contains_key("tags"));
        assert!(doc.frontmatter.contains_key("date"));
        assert!(doc.frontmatter.contains_key("status"));

        // Verify title value
        let title = doc.frontmatter.get("title").unwrap();
        assert_eq!(
            title.as_str().unwrap(),
            "Decision: Choosing LanceDB as the Vector Store"
        );

        // Verify tags is a sequence
        let tags = doc.frontmatter.get("tags").unwrap();
        assert!(tags.is_sequence());

        // Should have heading sections after frontmatter
        assert!(!doc.sections.is_empty());
        assert!(
            doc.sections[0]
                .heading_path
                .contains("Choosing LanceDB Over Alternatives")
        );
    }

    #[test]
    fn test_frontmatter_only() {
        let text = fixture("frontmatter_only.md");
        let doc = parse_document(&text);

        assert!(!doc.frontmatter.is_empty());
        assert!(doc.sections.is_empty());
    }

    #[test]
    fn test_heading_hierarchy() {
        let text = fixture("headings.md");
        let doc = parse_document(&text);

        // Should have many sections (headings.md has nested h1/h2/h3/h4)
        assert!(doc.sections.len() > 5);

        // First section should be H1
        assert_eq!(
            doc.sections[0].heading_path,
            "# Understanding Vector Embeddings"
        );

        // Find the "The Role of the CLS Token" section — it's H3 under H2 under H1
        let cls_section = doc
            .sections
            .iter()
            .find(|s| s.heading_path.contains("The Role of the CLS Token"));
        assert!(cls_section.is_some(), "should find CLS Token section");
        let cls = cls_section.unwrap();
        assert!(
            cls.heading_path
                .contains("# Understanding Vector Embeddings"),
            "H3 path should include H1 ancestor"
        );
        assert!(
            cls.heading_path.contains("## How Embedding Models Work"),
            "H3 path should include H2 parent"
        );

        // Find an H4 section — should have 3-level path
        let h4_section = doc
            .sections
            .iter()
            .find(|s| s.heading_path.contains("#### Storage Costs"));
        assert!(h4_section.is_some(), "should find H4 section");
        let h4 = h4_section.unwrap();
        assert!(
            h4.heading_path.contains("###"),
            "H4 path should include H3 parent"
        );
    }

    #[test]
    fn test_heading_stack_resets_on_same_level() {
        let text = "# A\n\n## B\n\nContent B.\n\n## C\n\nContent C.\n";
        let doc = parse_document(text);

        // Section C should NOT have B in its path
        let c_section = doc
            .sections
            .iter()
            .find(|s| s.heading_path.contains("## C"))
            .unwrap();
        assert!(
            !c_section.heading_path.contains("## B"),
            "sibling headings should not be in path"
        );
        assert_eq!(c_section.heading_path, "# A > ## C");
    }

    #[test]
    fn test_code_blocks_not_parsed_as_headings() {
        let text = fixture("code_blocks.md");
        let doc = parse_document(&text);

        // All heading_paths should start with # (real headings)
        for section in &doc.sections {
            if !section.heading_path.is_empty() {
                assert!(
                    section.heading_path.starts_with('#'),
                    "heading_path should start with #, got: {}",
                    section.heading_path
                );
            }
        }

        // Code blocks should appear in section content
        let all_content: String = doc.sections.iter().map(|s| s.content.as_str()).collect();
        assert!(
            all_content.contains("Result<T, E>") || all_content.contains("StorageError"),
            "code block content should be preserved in sections"
        );
    }

    #[test]
    fn test_byte_offsets_valid() {
        let text = "# Title\n\nSome content.\n\n## Section\n\nMore content.\n";
        let doc = parse_document(text);

        assert_eq!(doc.sections.len(), 2);
        for section in &doc.sections {
            assert!(
                section.byte_start <= section.byte_end,
                "byte_start <= byte_end"
            );
            assert!(section.byte_end <= text.len(), "byte_end <= text.len()");
        }

        // Sections should cover the full document without gaps
        assert_eq!(doc.sections[0].byte_start, 0);
        assert_eq!(doc.sections[0].byte_end, doc.sections[1].byte_start);
        assert_eq!(doc.sections[1].byte_end, text.len());
    }

    #[test]
    fn test_byte_offsets_with_frontmatter() {
        let text = "---\ntitle: Test\n---\n\n# Heading\n\nBody text.\n";
        let doc = parse_document(text);

        assert!(!doc.frontmatter.is_empty());
        assert_eq!(doc.sections.len(), 1);

        // Section should start after frontmatter
        let section = &doc.sections[0];
        assert!(
            section.byte_start > 0,
            "section should start after frontmatter"
        );
        assert_eq!(section.byte_end, text.len());

        // The byte range should contain the heading
        let extracted = &text[section.byte_start..section.byte_end];
        assert!(extracted.contains("# Heading"));
    }

    #[test]
    fn test_wikilinks_preserved_in_content() {
        let text = fixture("wikilinks.md");
        let doc = parse_document(&text);

        let all_content: String = doc
            .sections
            .iter()
            .map(|s| s.content.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        assert!(
            all_content.contains("[["),
            "wiki-links should be preserved in content"
        );
    }

    #[test]
    fn test_content_before_first_heading() {
        let text = "Preamble text.\n\n# Heading\n\nBody.\n";
        let doc = parse_document(text);

        assert_eq!(doc.sections.len(), 2);
        assert_eq!(doc.sections[0].heading_path, "");
        assert_eq!(doc.sections[0].content, "Preamble text.");
        assert_eq!(doc.sections[1].heading_path, "# Heading");
        assert_eq!(doc.sections[1].content, "Body.");
    }

    #[test]
    fn test_empty_sections_between_headings() {
        let text = "# A\n## B\n## C\n\nContent C.\n";
        let doc = parse_document(text);

        // Section A and B should have empty content, C should have content
        let a = doc
            .sections
            .iter()
            .find(|s| s.heading_path == "# A")
            .unwrap();
        assert_eq!(a.content, "");

        let b = doc
            .sections
            .iter()
            .find(|s| s.heading_path == "# A > ## B")
            .unwrap();
        assert_eq!(b.content, "");

        let c = doc
            .sections
            .iter()
            .find(|s| s.heading_path == "# A > ## C")
            .unwrap();
        assert_eq!(c.content, "Content C.");
    }
}
