# Fuzz targets

Two kinds of input reach code here without anybody having looked at it
first: bytes read back off the object store, and query strings, bodies
and tokens read off the wire. Both are fuzzed, and each target says at
the top of its file what it is checking beyond the absence of a panic.
Most of the grammar targets live on a roundtrip: anything the parser
accepts renders to a canonical form, and reparsing that form gives back
the same tree.

```bash
cargo install cargo-fuzz
cargo +nightly fuzz run rest_filter
cargo +nightly fuzz list
```

`seeds/<target>/` is committed and `corpus/<target>/` is not. The seeds
are the inputs worth starting from: one per interesting shape, plus
every input that has broken the target before. CI copies the seeds into
the corpus and runs each target from them, briefly on a change and for
ten minutes a target each night, so a target that has stopped
surviving its own history fails a build rather than a search nobody
ran.

When a run does find something, `cargo fuzz` writes the input to
`artifacts/<target>/`. Fix it, then put the input in `seeds/<target>/`
under a name that says what it is, so the fix stays fixed.
