//! What runs a function in a build with no engine in it.
//!
//! The alternative was for a server without an engine to serve nothing,
//! which would answer a deployed function with the same 404 as a name
//! nobody wrote, and that is a lie the caller is the least able to see
//! through. This answers the 500 a broken function answers and puts the
//! reason in the log, where the operator is.

use zou_functions::{Answer, Call, Failed, Function, Runtime};

pub struct Absent;

impl Runtime for Absent {
    fn invoke(&self, function: &Function, _call: Call) -> Result<Answer, Failed> {
        Err(Failed::Threw(format!(
            "{} needs a javascript engine and this zou was built without one, rebuild with --features zou-deno/isolate",
            function.name
        )))
    }

    fn describe(&self) -> String {
        "no javascript engine, this build has none".to_string()
    }
}
