//! The layout half of the type generator.
//!
//! supabase's generator builds a rough string and hands it to prettier,
//! so the file people diff is prettier's output rather than the
//! template's. There is no prettier here, so this is the piece of it
//! that the type file actually uses: Wadler's algorithm, the one
//! prettier itself is built on, with the same fits rule.
//!
//! A group is printed on one line if what is left of the line can hold
//! it, and broken onto several if it cannot, where "what is left" means
//! the group plus whatever text follows it up to the next break. A
//! group that contains a forced break is broken, and so is every group
//! around it, which is how a wide object drags its parents open.
//!
//! Widths are counted in characters. prettier counts east asian wide
//! characters as two, so a table named in kanji would wrap a column
//! earlier there than here.

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Flat,
    Break,
}

#[derive(Clone)]
pub enum Doc {
    Text(String),
    /// A space when flat, a newline when broken.
    Line,
    /// Nothing when flat, a newline when broken.
    Soft,
    Concat(Vec<Doc>),
    Indent(usize, Box<Doc>),
    Group(Box<Doc>, bool),
    /// The first when broken, the second when flat.
    IfBreak(String, String),
}

pub fn text(s: impl Into<String>) -> Doc {
    Doc::Text(s.into())
}

pub fn concat(parts: Vec<Doc>) -> Doc {
    Doc::Concat(parts)
}

pub fn line() -> Doc {
    Doc::Line
}

pub fn soft() -> Doc {
    Doc::Soft
}

pub fn indent(width: usize, doc: Doc) -> Doc {
    Doc::Indent(width, Box::new(doc))
}

pub fn if_break(broken: &str, flat: &str) -> Doc {
    Doc::IfBreak(broken.to_string(), flat.to_string())
}

/// A group that breaks only when it has to, unless something inside it
/// has already decided to break, which it inherits.
pub fn group(doc: Doc) -> Doc {
    let forced = forces_break(&doc);
    Doc::Group(Box::new(doc), forced)
}

/// A group that breaks whatever its width. supabase's template writes a
/// newline after the opening brace of these, and prettier keeps an
/// object open when the source had it open, so they never collapse.
pub fn group_broken(doc: Doc) -> Doc {
    Doc::Group(Box::new(doc), true)
}

pub fn join(separator: Doc, items: Vec<Doc>) -> Doc {
    let mut parts = Vec::with_capacity(items.len() * 2);
    for (i, item) in items.into_iter().enumerate() {
        if i > 0 {
            parts.push(separator.clone());
        }
        parts.push(item);
    }
    Doc::Concat(parts)
}

fn forces_break(doc: &Doc) -> bool {
    match doc {
        Doc::Text(_) | Doc::Line | Doc::Soft | Doc::IfBreak(_, _) => false,
        Doc::Concat(parts) => parts.iter().any(forces_break),
        Doc::Indent(_, inner) => forces_break(inner),
        Doc::Group(_, forced) => *forced,
    }
}

fn width_of(s: &str) -> usize {
    s.chars().count()
}

/// Whether the rest of the line holds `next` printed flat. Anything
/// after it counts too, up to the first break, which is why a short
/// object followed by a long string still breaks.
///
/// Indentation is not counted, and it does not have to be: measuring
/// stops at the first newline, and everything measured is therefore on
/// a line whose indent has already been paid for.
fn fits<'a>(next: (Mode, &'a Doc), rest: &[(usize, Mode, &'a Doc)], mut room: isize) -> bool {
    let mut stack = vec![next];
    let mut behind = rest.len();
    while room >= 0 {
        let (mode, doc) = match stack.pop() {
            Some(cmd) => cmd,
            None => {
                if behind == 0 {
                    return true;
                }
                behind -= 1;
                let (_, mode, doc) = rest[behind];
                stack.push((mode, doc));
                continue;
            }
        };
        match doc {
            Doc::Text(s) => room -= width_of(s) as isize,
            Doc::Concat(parts) => stack.extend(parts.iter().rev().map(|p| (mode, p))),
            Doc::Indent(_, inner) => stack.push((mode, inner)),
            Doc::Group(inner, forced) => {
                let mode = match forced {
                    true => Mode::Break,
                    false => mode,
                };
                stack.push((mode, inner));
            }
            Doc::IfBreak(broken, flat) => {
                let chosen = match mode {
                    Mode::Break => broken,
                    Mode::Flat => flat,
                };
                room -= width_of(chosen) as isize;
            }
            Doc::Line => match mode {
                Mode::Break => return true,
                Mode::Flat => room -= 1,
            },
            Doc::Soft => {
                if mode == Mode::Break {
                    return true;
                }
            }
        }
    }
    false
}

pub fn print(doc: &Doc, width: usize) -> String {
    let mut out = String::new();
    let mut column = 0usize;
    let mut stack = vec![(0usize, Mode::Break, doc)];
    while let Some((indent, mode, doc)) = stack.pop() {
        match doc {
            Doc::Text(s) => {
                out.push_str(s);
                column += width_of(s);
            }
            Doc::Concat(parts) => stack.extend(parts.iter().rev().map(|p| (indent, mode, p))),
            Doc::Indent(extra, inner) => stack.push((indent + extra, mode, inner)),
            Doc::Group(inner, forced) => {
                let room = width as isize - column as isize;
                let flat = !forced && fits((Mode::Flat, inner), &stack, room);
                let mode = match flat {
                    true => Mode::Flat,
                    false => Mode::Break,
                };
                stack.push((indent, mode, inner));
            }
            Doc::IfBreak(broken, flat) => {
                let chosen = match mode {
                    Mode::Break => broken,
                    Mode::Flat => flat,
                };
                out.push_str(chosen);
                column += width_of(chosen);
            }
            Doc::Line | Doc::Soft => match mode {
                Mode::Break => {
                    while out.ends_with(' ') {
                        out.pop();
                    }
                    out.push('\n');
                    out.push_str(&" ".repeat(indent));
                    column = indent;
                }
                Mode::Flat => {
                    if matches!(doc, Doc::Line) {
                        out.push(' ');
                        column += 1;
                    }
                }
            },
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shape every object in the type file has: braces, a line that
    /// is a space or a newline, and members that carry a semicolon only
    /// while they are on one line.
    fn object(members: Vec<Doc>, forced: bool) -> Doc {
        let separator = concat(vec![if_break("", ";"), Doc::Line]);
        let body = concat(vec![
            text("{"),
            indent(2, concat(vec![Doc::Line, join(separator, members)])),
            Doc::Line,
            text("}"),
        ]);
        match forced {
            true => group_broken(body),
            false => group(body),
        }
    }

    #[test]
    fn a_group_that_fits_stays_on_its_line() {
        let doc = object(vec![text("a: string"), text("b: number")], false);
        assert_eq!(print(&doc, 80), "{ a: string; b: number }");
    }

    #[test]
    fn a_group_that_does_not_fit_opens_up() {
        let doc = object(vec![text("a: string"), text("b: number")], false);
        assert_eq!(print(&doc, 10), "{\n  a: string\n  b: number\n}");
    }

    /// Flat, the object is `{ a: string; b: number }`, which is twenty
    /// four characters counting the three spaces the lines turn into.
    /// One character either side of that is the whole of the decision.
    #[test]
    fn the_spaces_a_flat_group_costs_are_counted_too() {
        let doc = object(vec![text("a: string"), text("b: number")], false);
        assert_eq!(print(&doc, 24), "{ a: string; b: number }");
        assert_eq!(print(&doc, 23), "{\n  a: string\n  b: number\n}");
    }

    /// The semicolons are only there while the members share a line, so
    /// they only count while they do.
    #[test]
    fn what_a_separator_costs_depends_on_whether_it_is_there() {
        let members = vec![text("aaaa"), text("bbbb")];
        // Flat: "{ aaaa; bbbb }", fourteen characters.
        assert_eq!(print(&object(members.clone(), false), 14), "{ aaaa; bbbb }");
        assert_eq!(print(&object(members, false), 13), "{\n  aaaa\n  bbbb\n}");
    }

    /// A group decides against what is left of the line, so what was
    /// printed before it is part of the decision.
    #[test]
    fn a_group_decides_from_where_the_line_has_got_to() {
        let head = group(concat(vec![text("a"), Doc::Line, text("b")]));
        let tail = object(vec![text("x: 12345678901234567890")], false);
        let doc = concat(vec![head, tail]);
        // "a b" is three, the object flat is twenty seven.
        assert_eq!(print(&doc, 30), "a b{ x: 12345678901234567890 }");
        assert_eq!(print(&doc, 29), "a b{\n  x: 12345678901234567890\n}");
    }

    #[test]
    fn a_group_asked_to_break_breaks_at_any_width() {
        let doc = object(vec![text("a: string")], true);
        assert_eq!(print(&doc, 80), "{\n  a: string\n}");
    }

    #[test]
    fn a_broken_group_takes_the_ones_around_it_with_it() {
        let inner = object(vec![text("a: string")], true);
        let outer = object(vec![concat(vec![text("Row: "), inner])], false);
        assert_eq!(print(&outer, 80), "{\n  Row: {\n    a: string\n  }\n}");
    }

    #[test]
    fn what_follows_a_group_is_counted_before_it_is_left_flat() {
        let doc = concat(vec![
            object(vec![text("error: true")], false),
            text(" & "),
            text("\"a reason long enough that the two of them will not fit on one line\""),
        ]);
        assert_eq!(
            print(&doc, 80),
            "{\n  error: true\n} & \"a reason long enough that the two of them will not fit on one line\""
        );
    }

    /// The bar a broken union puts at the start of a line is on that
    /// line, so it is part of what the line has left for whatever comes
    /// after it.
    #[test]
    fn a_bar_written_before_a_group_is_part_of_what_the_line_holds() {
        let doc = concat(vec![
            text("x"),
            Doc::Line,
            if_break("| ", ""),
            object(vec![text("a: string")], false),
        ]);
        let doc = group_broken(doc);
        assert_eq!(print(&doc, 15), "x\n| { a: string }");
        assert_eq!(print(&doc, 14), "x\n| {\n  a: string\n}");
    }

    #[test]
    fn an_indent_is_relative_to_the_line_it_starts_on() {
        let inner = object(vec![text("a: string")], true);
        let doc = concat(vec![text("x: "), indent(4, inner)]);
        assert_eq!(print(&doc, 80), "x: {\n      a: string\n    }");
    }

    #[test]
    fn a_line_left_holding_nothing_but_spaces_is_trimmed() {
        let doc = concat(vec![
            text("a"),
            indent(2, concat(vec![Doc::Line, Doc::Line])),
            text("b"),
        ]);
        let printed = print(&group_broken(doc), 80);
        assert_eq!(printed, "a\n\n  b");
    }
}
