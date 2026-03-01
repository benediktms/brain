use regex::Regex;
use std::sync::LazyLock;

/// A link extracted from a Markdown document.
#[derive(Debug, Clone, PartialEq)]
pub struct Link {
    /// The target path or URL.
    pub target: String,
    /// Display text (alias for wiki-links, text for markdown links).
    pub link_text: String,
    /// Classification of the link.
    pub link_type: LinkType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkType {
    /// `[[target]]` or `[[target|display]]`
    Wiki,
    /// `[text](relative.md)` — internal relative path
    Markdown,
    /// `[text](https://...)` — external URL
    External,
}

impl LinkType {
    pub fn as_str(&self) -> &'static str {
        match self {
            LinkType::Wiki => "wiki",
            LinkType::Markdown => "markdown",
            LinkType::External => "external",
        }
    }
}

// [[target]] or [[target|display]]
static WIKI_LINK_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\[\[([^\]\|]+?)(?:\|([^\]]+?))?\]\]").unwrap());

// Matches both ![alt](url) and [text](url) — we filter out images in code.
static MD_LINK_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(!)?\[([^\]]*)\]\(([^)]+)\)").unwrap());

/// Extract all links from markdown content.
pub fn extract_links(content: &str) -> Vec<Link> {
    let mut links = Vec::new();

    // Wiki-links
    for cap in WIKI_LINK_RE.captures_iter(content) {
        let target = cap[1].trim().to_string();
        let display = cap
            .get(2)
            .map(|m| m.as_str().trim().to_string())
            .unwrap_or_else(|| target.clone());
        links.push(Link {
            target,
            link_text: display,
            link_type: LinkType::Wiki,
        });
    }

    // Markdown links — skip images (group 1 captures `!` for images)
    for cap in MD_LINK_RE.captures_iter(content) {
        if cap.get(1).is_some() {
            continue; // skip ![alt](url) images
        }
        let text = cap[2].to_string();
        let url = cap[3].trim().to_string();

        let link_type = if url.starts_with("http://") || url.starts_with("https://") {
            LinkType::External
        } else {
            LinkType::Markdown
        };

        links.push(Link {
            link_text: text,
            target: url,
            link_type,
        });
    }

    links
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
    fn test_wiki_link_simple() {
        let links = extract_links("See [[headings]] for more.");
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].target, "headings");
        assert_eq!(links[0].link_text, "headings");
        assert_eq!(links[0].link_type, LinkType::Wiki);
    }

    #[test]
    fn test_wiki_link_aliased() {
        let links = extract_links("See [[frontmatter|the decision note]] here.");
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].target, "frontmatter");
        assert_eq!(links[0].link_text, "the decision note");
        assert_eq!(links[0].link_type, LinkType::Wiki);
    }

    #[test]
    fn test_markdown_link_internal() {
        let links = extract_links("[explanation](simple.md)");
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].target, "simple.md");
        assert_eq!(links[0].link_text, "explanation");
        assert_eq!(links[0].link_type, LinkType::Markdown);
    }

    #[test]
    fn test_markdown_link_external() {
        let links = extract_links("[Rust](https://www.rust-lang.org)");
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].target, "https://www.rust-lang.org");
        assert_eq!(links[0].link_type, LinkType::External);
    }

    #[test]
    fn test_image_not_extracted() {
        let links = extract_links("![alt text](image.png)");
        assert!(links.is_empty(), "images should not be extracted as links");
    }

    #[test]
    fn test_multiple_links_mixed() {
        let text = "See [[headings]] and [simple](simple.md) and [rust](https://rust-lang.org).";
        let links = extract_links(text);
        assert_eq!(links.len(), 3);
        assert_eq!(links[0].link_type, LinkType::Wiki);
        assert_eq!(links[1].link_type, LinkType::Markdown);
        assert_eq!(links[2].link_type, LinkType::External);
    }

    #[test]
    fn test_no_links() {
        let links = extract_links("Just plain text with no links.");
        assert!(links.is_empty());
    }

    #[test]
    fn test_wikilinks_fixture() {
        let text = fixture("wikilinks.md");
        let links = extract_links(&text);

        let wiki_links: Vec<_> = links
            .iter()
            .filter(|l| l.link_type == LinkType::Wiki)
            .collect();
        assert!(
            wiki_links.len() >= 3,
            "wikilinks.md should have at least 3 wiki-links, got {}",
            wiki_links.len()
        );

        // Check specific known links from the fixture
        assert!(
            wiki_links.iter().any(|l| l.target == "headings"),
            "should find [[headings]] link"
        );
        assert!(
            wiki_links.iter().any(|l| l.target == "frontmatter"),
            "should find [[frontmatter]] link"
        );
        assert!(
            wiki_links.iter().any(|l| l.target == "tasks"),
            "should find [[tasks]] link"
        );

        // Should also have a markdown link
        let md_links: Vec<_> = links
            .iter()
            .filter(|l| l.link_type == LinkType::Markdown)
            .collect();
        assert!(
            md_links.iter().any(|l| l.target == "simple.md"),
            "should find [simple](simple.md) link"
        );
    }
}
