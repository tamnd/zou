//! An extension module does not link libpython, and macOS wants that
//! said out loud.
//!
//! Everywhere else the linker is happy to leave the CPython symbols
//! undefined and let the interpreter that loads the library supply
//! them. The apple linker refuses unless it is told, and this is where
//! it is told, on this crate's cdylib alone rather than through
//! workspace rustflags, since letting undefined symbols through for
//! every binary in the tree is how a missing symbol becomes a crash
//! instead of a build failure.

fn main() {
    println!("cargo::rerun-if-changed=build.rs");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo::rustc-link-arg-cdylib=-undefined");
        println!("cargo::rustc-link-arg-cdylib=dynamic_lookup");
    }
}
