use std::fmt;

/// A simple markdown table renderer with auto-sized columns.
pub struct MarkdownTable {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl MarkdownTable {
    pub fn new(headers: Vec<impl Into<String>>) -> Self {
        Self {
            headers: headers.into_iter().map(Into::into).collect(),
            rows: Vec::new(),
        }
    }

    pub fn add_row(&mut self, row: Vec<impl Into<String>>) {
        self.rows.push(row.into_iter().map(Into::into).collect());
    }

    pub fn render(&self) -> String {
        let col_count = self.headers.len();
        let mut widths: Vec<usize> = self.headers.iter().map(|h| h.len()).collect();

        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                if i < col_count {
                    widths[i] = widths[i].max(cell.len());
                }
            }
        }

        let mut out = String::new();

        // Header row
        out.push('|');
        for (i, header) in self.headers.iter().enumerate() {
            out.push_str(&format!(" {:<width$} |", header, width = widths[i]));
        }
        out.push('\n');

        // Separator row
        out.push('|');
        for w in &widths {
            out.push_str(&format!(" {} |", "-".repeat(*w)));
        }
        out.push('\n');

        // Data rows
        for row in &self.rows {
            out.push('|');
            for (i, width) in widths.iter().enumerate().take(col_count) {
                let cell = row.get(i).map(|s| s.as_str()).unwrap_or("");
                out.push_str(&format!(" {:<width$} |", cell, width = width));
            }
            out.push('\n');
        }

        out
    }
}

impl fmt::Display for MarkdownTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.render())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_table_renders_headers_and_separator() {
        let table = MarkdownTable::new(vec!["A", "BB", "CCC"]);
        let rendered = table.render();
        let lines: Vec<&str> = rendered.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], "| A | BB | CCC |");
        assert_eq!(lines[1], "| - | -- | --- |");
    }

    #[test]
    fn missing_cells_render_as_empty() {
        let mut table = MarkdownTable::new(vec!["A", "B"]);
        table.add_row(vec!["only one"]);
        let rendered = table.render();
        let lines: Vec<&str> = rendered.lines().collect();
        assert_eq!(lines[2], "| only one |   |");
    }

    #[test]
    fn single_row_alignment() {
        let mut table = MarkdownTable::new(vec!["Name", "Age"]);
        table.add_row(vec!["Alice", "30"]);
        let rendered = table.render();
        let lines: Vec<&str> = rendered.lines().collect();
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], "| Name  | Age |");
        assert_eq!(lines[1], "| ----- | --- |");
        assert_eq!(lines[2], "| Alice | 30  |");
    }

    #[test]
    fn columns_auto_size_to_longest_value() {
        let mut table = MarkdownTable::new(vec!["ID", "Title"]);
        table.add_row(vec!["1", "Short"]);
        table.add_row(vec!["2", "A much longer title here"]);
        table.add_row(vec!["3", "Med"]);
        let rendered = table.render();
        let lines: Vec<&str> = rendered.lines().collect();
        // "A much longer title here" is 24 chars, wider than "Title" (5)
        assert_eq!(lines[0], "| ID | Title                    |");
        assert_eq!(lines[1], "| -- | ------------------------ |");
        assert_eq!(lines[2], "| 1  | Short                    |");
        assert_eq!(lines[3], "| 2  | A much longer title here |");
        assert_eq!(lines[4], "| 3  | Med                      |");
    }

    #[test]
    fn special_characters_dont_break_formatting() {
        let mut table = MarkdownTable::new(vec!["Col"]);
        table.add_row(vec!["hello | world"]);
        let rendered = table.render();
        // Should still produce valid-looking rows (pipe in content is the caller's concern)
        assert!(rendered.contains("hello | world"));
    }

    #[test]
    fn display_matches_render() {
        let mut table = MarkdownTable::new(vec!["X", "Y"]);
        table.add_row(vec!["a", "b"]);
        assert_eq!(format!("{table}"), table.render());
    }
}
