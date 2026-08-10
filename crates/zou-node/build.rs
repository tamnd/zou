fn main() {
    // Node does not link its addons against anything: the symbols are
    // resolved out of the host process at load time, and this is what
    // tells the linker to let them go unresolved.
    //
    // On anything that is not unix the crate is empty, so there is
    // nothing to leave unresolved and nothing to arrange.
    #[cfg(unix)]
    napi_build::setup();
}
