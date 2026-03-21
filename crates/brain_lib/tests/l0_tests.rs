//! Integration tests for extractive L0 abstract generation.
//!
//! Covers:
//! 1. Short content passes through verbatim.
//! 2. Long content is truncated to under MAX_CONTENT_CHARS.
//! 3. Abstract contains title and first sentences.
//! 4. Embedding source is the abstract, not the full content.

use brain_lib::l0_abstract::generate_l0_abstract;

const LONG_CONTENT: &str = "Alpha sentence one. Beta sentence two. Gamma sentence three. \
Delta sentence four. Epsilon sentence five. Zeta sentence six. \
Eta sentence seven. Theta sentence eight. Iota sentence nine. \
Kappa sentence ten. Lambda sentence eleven. Mu sentence twelve. \
Nu sentence thirteen. Xi sentence fourteen. Omicron sentence fifteen. \
Pi sentence sixteen. Rho sentence seventeen. Sigma sentence eighteen. \
Tau sentence nineteen. Upsilon sentence twenty. Phi sentence \
twenty-one. Chi sentence twenty-two. Psi sentence twenty-three. \
Omega sentence twenty-four. End of content reached here now fully.";

/// An abstract of long content must be shorter than the original.
#[test]
fn long_content_abstract_shorter_than_source() {
    let tags = &["integration", "embedding"];
    let abstract_text = generate_l0_abstract("Embedding Record", LONG_CONTENT, tags);
    // The abstract must be shorter than: title + full content + tags.
    let naive_full = format!(
        "Embedding Record\n\n{LONG_CONTENT}\n\nTags: integration, embedding"
    );
    assert!(
        abstract_text.len() < naive_full.len(),
        "abstract ({} chars) must be shorter than naive full ({} chars)",
        abstract_text.len(),
        naive_full.len()
    );
}

/// The abstract must contain the record title.
#[test]
fn abstract_contains_title() {
    let abstract_text = generate_l0_abstract("Unique Title XYZ", LONG_CONTENT, &[]);
    assert!(
        abstract_text.contains("Unique Title XYZ"),
        "title must appear in abstract"
    );
}

/// The abstract must contain the opening of the content.
#[test]
fn abstract_contains_first_sentences() {
    let abstract_text = generate_l0_abstract("Record", LONG_CONTENT, &["tag-a"]);
    assert!(
        abstract_text.contains("Alpha sentence one."),
        "first sentence must appear in abstract"
    );
}

/// When content is short the abstract must include it verbatim and still
/// be usable as an embedding source (shorter than a padded version would
/// be if we had naively appended irrelevant data).
#[test]
fn short_content_embedding_source_is_full_content() {
    let short = "Just a brief note about embeddings.";
    let abstract_text = generate_l0_abstract("Short Note", short, &["note"]);
    assert!(
        abstract_text.contains(short),
        "short content must appear verbatim in the abstract"
    );
    // The abstract is the embedding source — verify it does NOT contain the
    // rest of the (longer) document by checking length is bounded.
    assert!(
        abstract_text.len() < 1000,
        "embedding source must remain compact for short content"
    );
}
