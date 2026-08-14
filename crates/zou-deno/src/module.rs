//! Getting a function's own files into the isolate.
//!
//! A function is `index.ts`, so the loader's first job is typescript:
//! v8 has never heard of a type annotation and something has to take
//! them out before it sees the file. `deno_ast` is the same swc based
//! transpiler Deno itself uses, so what runs here is what would run
//! there rather than a second interpretation of the language.
//!
//! Only `file:` specifiers are loaded. A function that imports
//! `./_shared/util.ts` beside itself works, which is the layout every
//! example project uses. `npm:`, `jsr:` and `https:` are refused by
//! name rather than by a failure to find them, because a specifier
//! that is not supported yet should say so.

use std::rc::Rc;

use deno_core::{
    ModuleLoadResponse, ModuleLoader, ModuleResolveResponse, ModuleSource, ModuleSourceCode,
    ModuleSpecifier, ModuleType, ResolutionKind,
};
use deno_error::JsErrorBox;

/// Loads a function's modules off the disk it was deployed onto.
pub struct Disk;

impl ModuleLoader for Disk {
    fn resolve(
        &self,
        specifier: &str,
        referrer: &str,
        _kind: ResolutionKind,
    ) -> ModuleResolveResponse {
        for scheme in ["npm:", "jsr:", "node:", "http:", "https:", "data:"] {
            if specifier.starts_with(scheme) {
                return Err(JsErrorBox::type_error(format!(
                    "the {scheme} specifier in {specifier} is not supported yet"
                )));
            }
        }
        deno_core::resolve_import(specifier, referrer)
            .map_err(|e| JsErrorBox::type_error(e.to_string()))
    }

    fn load(
        &self,
        specifier: &ModuleSpecifier,
        _referrer: Option<&deno_core::ModuleLoadReferrer>,
        _options: deno_core::ModuleLoadOptions,
    ) -> ModuleLoadResponse {
        ModuleLoadResponse::Sync(read(specifier))
    }
}

fn read(specifier: &ModuleSpecifier) -> Result<ModuleSource, JsErrorBox> {
    let path = specifier
        .to_file_path()
        .map_err(|()| JsErrorBox::type_error(format!("{specifier} is not a file")))?;
    let text = std::fs::read_to_string(&path)
        .map_err(|e| JsErrorBox::generic(format!("read {}: {e}", path.display())))?;
    let media = deno_ast::MediaType::from_specifier(specifier);
    let (code, kind) = match media {
        deno_ast::MediaType::JavaScript | deno_ast::MediaType::Mjs | deno_ast::MediaType::Cjs => {
            (text, ModuleType::JavaScript)
        }
        deno_ast::MediaType::Json => (text, ModuleType::Json),
        deno_ast::MediaType::TypeScript
        | deno_ast::MediaType::Mts
        | deno_ast::MediaType::Cts
        | deno_ast::MediaType::Dts
        | deno_ast::MediaType::Dmts
        | deno_ast::MediaType::Dcts
        | deno_ast::MediaType::Jsx
        | deno_ast::MediaType::Tsx => (stripped(specifier, text, media)?, ModuleType::JavaScript),
        other => {
            return Err(JsErrorBox::type_error(format!(
                "{specifier} is a {other} and a function may not import one"
            )));
        }
    };
    Ok(ModuleSource::new(
        kind,
        ModuleSourceCode::String(code.into()),
        specifier,
        None,
    ))
}

/// The same file with the types taken out.
fn stripped(
    specifier: &ModuleSpecifier,
    text: String,
    media: deno_ast::MediaType,
) -> Result<String, JsErrorBox> {
    let parsed = deno_ast::parse_module(deno_ast::ParseParams {
        specifier: specifier.clone(),
        text: text.into(),
        media_type: media,
        capture_tokens: false,
        scope_analysis: false,
        maybe_syntax: None,
    })
    .map_err(|e| JsErrorBox::type_error(e.to_string()))?;
    let emitted = parsed
        .transpile(
            &deno_ast::TranspileOptions::default(),
            &deno_ast::TranspileModuleOptions::default(),
            &deno_ast::EmitOptions::default(),
        )
        .map_err(|e| JsErrorBox::type_error(e.to_string()))?;
    Ok(emitted.into_source().text)
}

/// The loader an isolate is built with.
pub fn loader() -> Rc<dyn ModuleLoader> {
    Rc::new(Disk)
}
