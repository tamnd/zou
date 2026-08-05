//! csv request bodies.
//!
//! A csv body is a table of text, and every write on this surface
//! wants objects, so the header names the keys and each record fills
//! them. Two things about that reading are worth stating because
//! they are not obvious from the format:
//!
//! The literal `NULL` is sql null. Nothing else in a csv can say
//! null, an empty field being the empty string, so PostgREST spends
//! the word on it and a caller who wants the four letters has no way
//! to write them.
//!
//! A record shorter than the header is refused rather than padded.
//! The keys come from zipping the header with the record, so a short
//! record would quietly build an object with fewer keys than its
//! neighbours, and a body whose rows disagree about their columns is
//! not something to guess at.

/// What a csv body says: the header names, and one row of values per
/// record with none for the ones that said `NULL`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    pub header: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}

/// The word a csv field spends on sql null.
const NULL: &str = "NULL";

/// Read a csv body.
///
/// The errors are the two a caller can act on, spelled the way
/// PostgREST spells them: a body that is not csv at all, which an
/// empty one is since there is not even a header line in it, and one
/// whose records do not agree with the header.
pub fn read(input: &[u8]) -> Result<Table, String> {
    let text = String::from_utf8_lossy(input);
    let mut records = split(&text)?;
    if records.is_empty() {
        return Err(not_enough(""));
    }
    let header = records.remove(0);
    let mut rows = Vec::with_capacity(records.len());
    for record in records {
        if record.len() < header.len() {
            return Err("All lines must have same number of fields".to_string());
        }
        rows.push(
            record
                .into_iter()
                .take(header.len())
                .map(|f| if f == NULL { None } else { Some(f) })
                .collect(),
        );
    }
    Ok(Table { header, rows })
}

fn not_enough(rest: &str) -> String {
    format!("parse error (not enough input) at \"{rest}\"")
}

/// The records of a csv, unquoted. A field is quoted when the quote
/// is the first thing in it, and then a doubled quote is one quote
/// and everything else including commas and newlines is content.
fn split(text: &str) -> Result<Vec<Vec<String>>, String> {
    let chars: Vec<char> = text.chars().collect();
    let mut records: Vec<Vec<String>> = Vec::new();
    let mut record: Vec<String> = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if quoted {
            if c == '"' {
                if chars.get(i + 1) == Some(&'"') {
                    field.push('"');
                    i += 2;
                } else {
                    quoted = false;
                    i += 1;
                }
            } else {
                field.push(c);
                i += 1;
            }
            continue;
        }
        match c {
            '"' if field.is_empty() => {
                quoted = true;
                i += 1;
            }
            ',' => {
                record.push(std::mem::take(&mut field));
                i += 1;
            }
            '\n' | '\r' => {
                if c == '\r' && chars.get(i + 1) == Some(&'\n') {
                    i += 1;
                }
                record.push(std::mem::take(&mut field));
                records.push(std::mem::take(&mut record));
                i += 1;
            }
            _ => {
                field.push(c);
                i += 1;
            }
        }
    }
    // A quote nobody closed leaves the record unfinished, which is a
    // body that ran out rather than a body that is wrong.
    if quoted {
        return Err(not_enough(&field));
    }
    // A trailing newline ends the last record rather than starting an
    // empty one, so only an unterminated line is left to close here.
    if !field.is_empty() || !record.is_empty() {
        record.push(field);
        records.push(record);
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(input: &str) -> Result<Table, String> {
        read(input.as_bytes())
    }

    fn some(values: &[&str]) -> Vec<Option<String>> {
        values.iter().map(|v| Some((*v).to_string())).collect()
    }

    #[test]
    fn the_header_names_the_keys_and_the_records_fill_them() {
        let t = rows("a,b\nbar,baz").expect("two fields under two names");
        assert_eq!(t.header, ["a", "b"]);
        assert_eq!(t.rows, [some(&["bar", "baz"])]);
    }

    #[test]
    fn the_word_null_is_the_only_way_a_field_says_null() {
        let t = rows("a,b\nNULL,foo\n,bar").expect("both records fit");
        assert_eq!(
            t.rows,
            [
                vec![None, Some("foo".to_string())],
                vec![Some(String::new()), Some("bar".to_string())],
            ]
        );
    }

    #[test]
    fn a_quoted_field_holds_commas_newlines_and_quotes() {
        let t = rows("a,b\n\"x,y\",\"line\nbreak\"\n\"say \"\"hi\"\"\",z")
            .expect("quotes close and the records fit");
        assert_eq!(
            t.rows,
            [some(&["x,y", "line\nbreak"]), some(&["say \"hi\"", "z"]),]
        );
    }

    #[test]
    fn a_record_short_of_the_header_is_refused() {
        assert_eq!(
            rows("a,b\nfoo,bar\nbaz"),
            Err("All lines must have same number of fields".to_string())
        );
    }

    #[test]
    fn a_body_with_no_header_in_it_is_not_a_csv() {
        assert_eq!(
            rows(""),
            Err("parse error (not enough input) at \"\"".to_string())
        );
        assert_eq!(
            rows("a,b\n\"unclosed"),
            Err("parse error (not enough input) at \"unclosed\"".to_string())
        );
    }

    #[test]
    fn a_header_on_its_own_carries_no_rows() {
        let t = rows("a,b\n").expect("a header is a csv");
        assert_eq!(t.header, ["a", "b"]);
        assert!(t.rows.is_empty());
    }

    #[test]
    fn carriage_returns_end_a_record_the_same_way_a_newline_does() {
        let t = rows("a,b\r\n1,2\r\n").expect("crlf is a line ending");
        assert_eq!(t.header, ["a", "b"]);
        assert_eq!(t.rows, [some(&["1", "2"])]);
    }
}
