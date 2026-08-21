//! Image transforms: `/storage/v1/render/image/...`.
//!
//! storage-api does not transform anything itself. It checks the
//! request, hands the object to an imgproxy next to it, and hands back
//! what imgproxy answered, so everything a caller can see about this
//! surface is really imgproxy's behaviour reached through a query string
//! somebody else designed. zou has no second process, so what is here is
//! that behaviour written out, and it is written out from a recording
//! rather than from imgproxy's documentation.
//!
//! Four things in that recording are worth knowing before reading any
//! of this, because none of them is what you would write on your own.
//!
//! The geometry comes from a preset, not from the request. The CLI
//! starts imgproxy with `default=width:3000/height:8192`, so a side the
//! caller left out is three thousand or eight thousand rather than
//! unconstrained, and nothing is ever enlarged. That is why a width of
//! a hundred asked of a 350 by 206 image answers 100 by 206 and not the
//! 100 by 59 an aspect ratio would give you: the height was never asked
//! about, so it stayed as it was, and the width was cropped to a
//! hundred.
//!
//! Every refusal on these routes is http 400. The real status is in the
//! body, which is true of the whole storage surface and only visible
//! here: an object that is not there answers 400 carrying a 404, no key
//! at all answers 400 carrying a 403. That falls out of the serializer
//! in `storage`, so nothing in this file has to arrange it.
//!
//! A transform carries the etag of the object it was made from, not one
//! of its own. Two different transforms of one object have the same
//! etag, which is not what an etag is for and is what the reference
//! sends.
//!
//! The format is negotiated unless it is asked for. A caller whose
//! `accept` takes webp gets webp, whatever was uploaded; a caller who
//! asks for `origin` gets what was uploaded, whatever their `accept`
//! says; and `avif` is the one format that can be named outright. The
//! list really is those two: `format=jxl` is refused the way a resize
//! mode that is not one of the three is.
//!
//! What is not here: `if-none-match` on a transform, which the suite
//! does not ask about, and the animated half of imgproxy. A gif is
//! decoded to its first frame and encoded as one, which is what the
//! `image` crate does without being asked to do otherwise.

use std::io::Cursor;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{Method, StatusCode, header, request::Parts};
use axum::response::Response;
use image::{DynamicImage, ImageEncoder, ImageFormat};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::App;
use crate::object::{self, ObjectRow};
use crate::storage::{StorageError, begin, caller, done};

/// The widest and the tallest a transform can come out, which are not
/// zou's numbers: they are the default preset the reference's imgproxy
/// is started with, `default=width:3000/height:8192`. A caller who
/// names one side is asking for the other to be this, which is why the
/// side they did not name comes back unchanged rather than scaled.
const WIDEST: u32 = 3000;
const TALLEST: u32 = 8192;

/// The largest a caller can ask a side to be. Past it the number is
/// quietly held down rather than refused, which is upstream's own limit
/// and not imgproxy's: storage-api clamps before it builds the option
/// list, and the clamped number is what the answer says it did.
const BIGGEST: u32 = 2000;

/// The lowest and highest quality the schema takes. Twenty is also what
/// the client library documents, and it is the only one of the two a
/// client is likely to reach.
const WORST: i64 = 20;
const BEST: i64 = 100;

/// What a format is encoded at when the request said nothing.
///
/// Three numbers rather than one, from `IMGPROXY_FORMAT_QUALITY` in the
/// CLI's own environment for the container: `jpeg=80,avif=62,webp=80`.
/// Nothing in the suite can see them, because the size of an answer is
/// volatile and two encoders never agree on it anyway. They are here so
/// that a transform out of zou is about the size a transform out of the
/// reference is, which is the thing a caller actually notices.
const JPEG_QUALITY: u8 = 80;
const WEBP_QUALITY: u8 = 80;
const AVIF_QUALITY: u8 = 62;

/// How hard the avif encoder tries. Ten is the fastest and one is the
/// smallest, and this is the middle: an avif is already the slowest
/// thing this surface can be asked for, and a render nobody can wait
/// for is not a render.
const AVIF_SPEED: u8 = 6;

/// How the image is made to fit the box it was given.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Resize {
    /// The whole image inside the box, which leaves one side short.
    Contain,
    /// The box filled, with whatever hangs over the edges cropped off.
    /// What a request that says nothing gets.
    Cover,
    /// The box filled by stretching, aspect ratio and all.
    Fill,
}

/// Which format the answer is in, as far as the request says.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Asked {
    /// Nothing said, so the `accept` header decides.
    Negotiate,
    /// `format=origin`: the format it was uploaded in, whatever the
    /// `accept` header says.
    Origin,
    /// `format=avif`, the one format that can be named outright.
    Avif,
}

/// What the answer is encoded as, once the negotiation is over.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Out {
    Jpeg,
    Png,
    Gif,
    Webp,
    Avif,
}

impl Out {
    fn mime(self) -> &'static str {
        match self {
            Out::Jpeg => "image/jpeg",
            Out::Png => "image/png",
            Out::Gif => "image/gif",
            Out::Webp => "image/webp",
            Out::Avif => "image/avif",
        }
    }

    /// The quality this format is written at when nobody asked for one.
    fn quality(self) -> u8 {
        match self {
            Out::Avif => AVIF_QUALITY,
            Out::Webp => WEBP_QUALITY,
            _ => JPEG_QUALITY,
        }
    }

    /// What the answer is called in a cache key, which is a short word
    /// rather than a number so that a key can be read by a person
    /// looking at the store.
    fn word(self) -> &'static str {
        match self {
            Out::Jpeg => "jpeg",
            Out::Png => "png",
            Out::Gif => "gif",
            Out::Webp => "webp",
            Out::Avif => "avif",
        }
    }
}

/// Everything the request said about the transform.
#[derive(Debug)]
struct Wanted {
    /// Zero meaning the preset's, which is what a missing side is too.
    width: u32,
    height: u32,
    resize: Resize,
    /// The quality asked for, or none for the format's own default.
    quality: Option<u8>,
    format: Asked,
}

/// The transform read out of wherever it was written.
///
/// Two places write one shape: the query string of an unsigned request,
/// and the claims of a token minted by `/object/sign`. `place` is what
/// the refusals call it, `querystring` for the first and
/// `body/transform` for the second, which is fastify's own naming for
/// the two halves of a request it validates.
///
/// Only the first is recorded. A signing body carrying a transform that
/// does not validate is not something the suite asks about, so the
/// second is this file's reading of what the same schema would say
/// about the same value somewhere else.
fn wanted(place: &str, said: impl Fn(&str) -> Option<String>) -> Result<Wanted, StorageError> {
    let side = |name: &str| -> Result<u32, StorageError> {
        let Some(text) = said(name) else {
            return Ok(0);
        };
        // An empty value is the parameter not being there, which is
        // what a schema with no required fields does with one.
        if text.is_empty() {
            return Ok(0);
        }
        let Ok(number) = text.parse::<i64>() else {
            return Err(StorageError::not_valid(format!(
                "{place}/{name} must be integer"
            )));
        };
        if number < 0 {
            return Err(StorageError::not_valid(format!(
                "{place}/{name} must be >= 0"
            )));
        }
        // A side past the ceiling is held to it rather than refused,
        // quietly: a width of 2501 comes back as 2000 in the header the
        // answer carries. Nothing in the picture shows it while the
        // fixture is 350 wide, since a crop bigger than the image is a
        // crop that does nothing either way, and it is here because the
        // header said so. Recorded.
        Ok(u32::try_from(number).unwrap_or(u32::MAX).min(BIGGEST))
    };
    let width = side("width")?;
    let height = side("height")?;

    let resize = match said("resize").as_deref() {
        None | Some("") | Some("cover") => Resize::Cover,
        Some("contain") => Resize::Contain,
        Some("fill") => Resize::Fill,
        Some(_) => {
            return Err(StorageError::not_valid(format!(
                "{place}/resize must be equal to one of the allowed values"
            )));
        }
    };

    let quality = match said("quality").as_deref() {
        None | Some("") => None,
        Some(text) => {
            let Ok(number) = text.parse::<i64>() else {
                return Err(StorageError::not_valid(format!(
                    "{place}/quality must be integer"
                )));
            };
            if number < WORST {
                return Err(StorageError::not_valid(format!(
                    "{place}/quality must be >= {WORST}"
                )));
            }
            if number > BEST {
                return Err(StorageError::not_valid(format!(
                    "{place}/quality must be <= {BEST}"
                )));
            }
            Some(number as u8)
        }
    };

    let format = match said("format").as_deref() {
        None | Some("") => Asked::Negotiate,
        Some("origin") => Asked::Origin,
        Some("avif") => Asked::Avif,
        Some(_) => {
            return Err(StorageError::not_valid(format!(
                "{place}/format must be equal to one of the allowed values"
            )));
        }
    };

    Ok(Wanted {
        width,
        height,
        resize,
        quality,
        format,
    })
}

/// The transform a request carries in its query string.
fn from_query(parts: &Parts) -> Result<Wanted, StorageError> {
    wanted("querystring", |name| object::asked_for(parts, name))
}

/// The transform a signed url carries in its token.
///
/// The claim is the query string the signer was asked for, kept as a
/// string so that one parser reads both and a url signed for a
/// transform cannot mean something the same transform in a query string
/// would not.
fn from_claims(claims: &Value) -> Result<Wanted, StorageError> {
    let query = claims
        .get("transform")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    wanted("querystring", |name| one_of(&query, name))
}

/// One parameter out of a query string that is not a request's own.
fn one_of(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        (crate::rest::decode(key) == name).then(|| crate::rest::decode(value))
    })
}

/// The transform a signing request asked to have put in a token, as the
/// query string that token will carry.
///
/// Validated here rather than when the url is spent, because a url that
/// cannot work is worth refusing to the caller who asked for it rather
/// than to whoever they hand it to.
pub(crate) fn signed_transform(asked: &Value) -> Result<Option<String>, StorageError> {
    let Some(transform) = asked.get("transform") else {
        return Ok(None);
    };
    let Some(fields) = transform.as_object() else {
        return Ok(None);
    };
    let said = |name: &str| -> Option<String> {
        match fields.get(name)? {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        }
    };
    wanted("body/transform", said)?;
    // Written back out rather than kept, so that the token carries what
    // was asked for and the reading of it happens in one place. Nothing
    // is escaped on the way out because nothing that needs escaping
    // survives the line above: the two words are from closed lists and
    // the three numbers have been through `parse`.
    let mut query = String::new();
    for name in ["width", "height", "resize", "quality", "format"] {
        if let Some(value) = said(name) {
            if !query.is_empty() {
                query.push('&');
            }
            query.push_str(name);
            query.push('=');
            query.push_str(&value);
        }
    }
    Ok(Some(query))
}

/// Which door a transform came in by.
///
/// Not the same three as a download's, and the difference is recorded
/// twice. The authenticated route here has no public fallback: a
/// request with no token is refused in jose's words even when the
/// bucket is one anybody may read, where the same request to
/// `/object/authenticated/...` is answered. And the public route
/// refuses a bucket that is not public as a bucket that is not there,
/// where the object route refuses it as an object that is not there.
#[derive(Clone, Copy, PartialEq)]
enum Door {
    Authenticated,
    Public,
}

/// GET or HEAD /storage/v1/render/image/authenticated/{bucket}/{name}
pub async fn authenticated(
    State(app): State<Arc<App>>,
    Path((bucket, name)): Path<(String, String)>,
    parts: Parts,
) -> Result<Response, StorageError> {
    let wanted = from_query(&parts)?;
    let row = row_for(&app, &parts, &bucket, &name, Door::Authenticated).await?;
    answer(&app, &parts, &row, &wanted, None).await
}

/// GET or HEAD /storage/v1/render/image/public/{bucket}/{name}
pub async fn public(
    State(app): State<Arc<App>>,
    Path((bucket, name)): Path<(String, String)>,
    parts: Parts,
) -> Result<Response, StorageError> {
    let wanted = from_query(&parts)?;
    let row = row_for(&app, &parts, &bucket, &name, Door::Public).await?;
    answer(&app, &parts, &row, &wanted, None).await
}

/// GET or HEAD /storage/v1/render/image/sign/{bucket}/{name}
///
/// The transform is in the token rather than in the url, so a url
/// signed for a thumbnail cannot be edited into a url for the whole
/// image. What is in the query string here is the token and nothing
/// else.
pub async fn signed(
    State(app): State<Arc<App>>,
    Path((bucket, name)): Path<(String, String)>,
    parts: Parts,
) -> Result<Response, StorageError> {
    let Some(token) = object::asked_for(&parts, "token") else {
        return Err(StorageError::missing_property("querystring", "token"));
    };
    let claims = object::signed_claims(&app, &token, &format!("{bucket}/{name}"), "download")?;
    let wanted = from_claims(&claims)?;
    let until = claims
        .get("exp")
        .and_then(Value::as_i64)
        .map(object::http_date);

    let sess = object::superuser(&app).await?;
    let found = object::find(&sess, &bucket, &name).await;
    let Some(row) = done(sess, found).await? else {
        return Err(StorageError::no_such_key());
    };
    answer(&app, &parts, &row, &wanted, until).await
}

/// The row a transform is about, or the reason there is none.
async fn row_for(
    app: &App,
    parts: &Parts,
    bucket: &str,
    name: &str,
    door: Door,
) -> Result<ObjectRow, StorageError> {
    let sess = match door {
        // No fallback. A request with no usable token is refused here
        // rather than tried as a public read, which is the one place
        // this surface is stricter than the object one.
        Door::Authenticated => {
            let (ctx, _) = caller(app, parts)?;
            begin(app, &ctx, true).await?
        }
        Door::Public => {
            let sess = object::superuser(app).await?;
            match object::bucket_public(&sess, bucket).await {
                Err(e) => {
                    let _ = sess.rollback().await;
                    return Err(e);
                }
                // A bucket that is not public is a bucket that is not
                // there, said about the bucket. Recorded, and it is not
                // what `/object/public/...` says about the same bucket.
                Ok(None) | Ok(Some(false)) => {
                    let _ = sess.rollback().await;
                    return Err(StorageError::no_such_bucket());
                }
                Ok(Some(true)) => sess,
            }
        }
    };
    let found = object::find(&sess, bucket, name).await;
    match done(sess, found).await? {
        Some(row) => Ok(row),
        None => Err(StorageError::no_such_key()),
    }
}

/// The transformed bytes, from the cache or from the encoder.
async fn answer(
    app: &App,
    parts: &Parts,
    row: &ObjectRow,
    wanted: &Wanted,
    until: Option<String>,
) -> Result<Response, StorageError> {
    let blobs = object::blobs(app)?;
    let accept = parts
        .headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let mime = row.meta("mimetype");
    let out = chosen(wanted, mime.as_str().unwrap_or_default(), accept);

    // The store is asked before the object is, because a hit answers
    // without reading the original at all. The key has the version in
    // it, so an object that was replaced has a different one and
    // nothing has to be swept when it is.
    let key = row.render_key(&recipe(wanted, out), out.word());
    let cached = blobs
        .get(key.clone())
        .await
        .map_err(|e| StorageError::internal(e.to_string()))?;

    let bytes = match cached {
        Some(bytes) => bytes,
        None => {
            let Some(source) = blobs
                .get(row.key())
                .await
                .map_err(|e| StorageError::internal(e.to_string()))?
            else {
                // A row with no bytes behind it is the store
                // disagreeing with the table, which is the same failure
                // a download of it would be.
                return Err(StorageError::internal("Internal Server Error".to_string()));
            };
            let quality = wanted.quality.unwrap_or_else(|| out.quality());
            let plan = Plan {
                width: wanted.width,
                height: wanted.height,
                resize: wanted.resize,
                out,
                quality,
            };
            // Off the async threads. Decoding and encoding an image is
            // the one thing this server does that is measured in
            // hundreds of milliseconds rather than in reads.
            let bytes = tokio::task::spawn_blocking(move || rendered(&source, &plan))
                .await
                .map_err(|e| StorageError::internal(e.to_string()))??;
            // A cache that cannot be written is not a request that
            // failed. The bytes are in hand either way.
            if let Err(e) = blobs.put(key, bytes.clone()).await {
                log::warn!("a transform could not be cached: {e}");
            }
            bytes
        }
    };

    // The etag of the object rather than of the answer, which is
    // upstream's and is recorded: two transforms of one object carry
    // the same one.
    let etag = row.meta("eTag");
    let cache = row.served_cache();
    let cache = cache.as_str().unwrap_or(object::NO_CACHE);
    let length = bytes.len();

    let mut answer = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, out.mime())
        .header(header::CONTENT_LENGTH, length);
    if let Some(etag) = etag.as_str() {
        answer = answer.header(header::ETAG, etag);
    }
    // What was asked for, in imgproxy's own spelling. It is the only
    // thing an answer says about what it did, since the picture is
    // compared as dimensions and a format rather than as bytes.
    if let Some(said) = did(wanted) {
        answer = answer.header("x-transformations", said);
    }
    answer = match &until {
        Some(when) => answer.header(header::EXPIRES, when),
        None => answer.header(header::CACHE_CONTROL, cache),
    };
    // A head carries the headers a get would carry and none of the
    // bytes. Recorded: the reference answers one with a content type
    // and an empty body.
    let body = match parts.method == Method::HEAD {
        true => Body::empty(),
        false => Body::from(bytes),
    };
    answer
        .body(body)
        .map_err(|e| StorageError::internal(e.to_string()))
}

/// What the answer says it did, or nothing when it did nothing.
///
/// This is the option list storage-api builds for imgproxy, handed back
/// verbatim in `x-transformations`, so it is the request rather than
/// the answer: a caller who was negotiated into webp is not told so
/// here, and a caller who named `origin` is not either. Only `avif`
/// appears, because only `avif` is a format imgproxy was asked for.
///
/// The order is height, width, resizing type, quality, format, which is
/// the order the fields are read in somewhere upstream and is recorded
/// rather than chosen. The resizing type is named whenever a side was,
/// and a request with no side at all has nothing to say and says
/// nothing: no header rather than an empty one.
fn did(wanted: &Wanted) -> Option<String> {
    let mut said: Vec<String> = Vec::new();
    if wanted.height > 0 {
        said.push(format!("height:{}", wanted.height));
    }
    if wanted.width > 0 {
        said.push(format!("width:{}", wanted.width));
    }
    if wanted.width > 0 || wanted.height > 0 {
        said.push(format!(
            "resizing_type:{}",
            match wanted.resize {
                // imgproxy's names for the three, which are not the
                // three names the query takes: what a caller calls
                // cover is what imgproxy calls fill.
                Resize::Contain => "fit",
                Resize::Cover => "fill",
                Resize::Fill => "force",
            }
        ));
    }
    if let Some(quality) = wanted.quality {
        said.push(format!("quality:{quality}"));
    }
    if wanted.format == Asked::Avif {
        said.push("format:avif".to_string());
    }
    match said.is_empty() {
        true => None,
        false => Some(said.join(",")),
    }
}

/// Which format the answer comes back in.
///
/// The order is the recorded one: what the query named beats what the
/// header takes, and what the header takes beats what was uploaded. A
/// caller asking for `origin` with a browser's own `accept` gets the
/// format it was uploaded in, which is the case that says which way
/// round the last two go.
fn chosen(wanted: &Wanted, mime: &str, accept: &str) -> Out {
    match wanted.format {
        Asked::Avif => Out::Avif,
        Asked::Origin => uploaded(mime),
        Asked::Negotiate if takes_webp(accept) => Out::Webp,
        Asked::Negotiate => uploaded(mime),
    }
}

/// What an object says it is, as a format this can write back.
///
/// Read from the type the upload declared rather than from the bytes,
/// because it is read before the bytes are, and anything that is not
/// one of the four is answered as a jpeg. A transform of something the
/// decoder cannot open never gets this far.
fn uploaded(mime: &str) -> Out {
    match mime.split(';').next().unwrap_or("").trim() {
        "image/png" => Out::Png,
        "image/gif" => Out::Gif,
        "image/webp" => Out::Webp,
        "image/avif" => Out::Avif,
        _ => Out::Jpeg,
    }
}

/// Whether the caller said it takes webp.
///
/// The exact type and nothing wider. A browser sends
/// `image/webp,image/apng,image/*,*/*;q=0.8`, and the `image/*` in it
/// is not read as yes: imgproxy looks for the one string, and a client
/// that says it takes any image is not saying it takes that one.
fn takes_webp(accept: &str) -> bool {
    accept.split(',').any(|entry| {
        entry
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .eq_ignore_ascii_case("image/webp")
    })
}

/// What one transform is, short enough to put in a key.
///
/// Everything that changes the answer and nothing that does not, which
/// is why `cacheNonce` is absent: it is a parameter for getting past
/// somebody else's cache, and reading it here would mean this one never
/// answered anything twice.
fn recipe(wanted: &Wanted, out: Out) -> String {
    let spelled = format!(
        "{}x{}/{}/{}/{}",
        wanted.width,
        wanted.height,
        match wanted.resize {
            Resize::Contain => "contain",
            Resize::Cover => "cover",
            Resize::Fill => "fill",
        },
        wanted.quality.unwrap_or_else(|| out.quality()),
        out.word(),
    );
    let digest = Sha256::digest(spelled.as_bytes());
    digest
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// What one transform asks the encoder for, flat enough to hand to
/// another thread.
struct Plan {
    width: u32,
    height: u32,
    resize: Resize,
    out: Out,
    quality: u8,
}

/// The bytes of a transform, made rather than found.
fn rendered(source: &[u8], plan: &Plan) -> Result<Vec<u8>, StorageError> {
    let image = image::load_from_memory(source).map_err(|_| StorageError::not_an_image())?;
    let sized = fitted(&image, plan);
    encoded(&sized, plan)
}

/// The image at the size the request asks for.
///
/// Two steps, because a crop is not a resize. The scale is worked out
/// first and is never above one, then whatever hangs outside the box is
/// cut off around the middle. For `contain` there is never anything
/// hanging over, and for `fill` there is no scale at all: the image is
/// stretched onto the box, or onto as much of it as the image can fill
/// without being enlarged.
fn fitted(image: &DynamicImage, plan: &Plan) -> DynamicImage {
    let (width, height) = (image.width(), image.height());
    if width == 0 || height == 0 {
        return image.clone();
    }
    let box_w = match plan.width {
        0 => WIDEST,
        asked => asked,
    };
    let box_h = match plan.height {
        0 => TALLEST,
        asked => asked,
    };

    if plan.resize == Resize::Fill {
        let to_w = box_w.min(width).max(1);
        let to_h = box_h.min(height).max(1);
        return match (to_w, to_h) == (width, height) {
            true => image.clone(),
            false => image.resize_exact(to_w, to_h, image::imageops::FilterType::Lanczos3),
        };
    }

    let across = f64::from(box_w) / f64::from(width);
    let down = f64::from(box_h) / f64::from(height);
    let scale = match plan.resize {
        Resize::Contain => across.min(down),
        // Cover and the default: the smaller side decides how much of
        // the box is filled, and the rest is cropped.
        _ => across.max(down),
    }
    .min(1.0);

    let to_w = ((f64::from(width) * scale).round() as u32).max(1);
    let to_h = ((f64::from(height) * scale).round() as u32).max(1);
    let scaled = match (to_w, to_h) == (width, height) {
        true => image.clone(),
        false => image.resize_exact(to_w, to_h, image::imageops::FilterType::Lanczos3),
    };
    if plan.resize == Resize::Contain {
        return scaled;
    }

    let crop_w = box_w.min(to_w);
    let crop_h = box_h.min(to_h);
    match (crop_w, crop_h) == (to_w, to_h) {
        true => scaled,
        // Around the middle, which is imgproxy's default gravity.
        false => scaled.crop_imm((to_w - crop_w) / 2, (to_h - crop_h) / 2, crop_w, crop_h),
    }
}

/// The image written out in the format that was settled on.
fn encoded(image: &DynamicImage, plan: &Plan) -> Result<Vec<u8>, StorageError> {
    let mut out = Vec::new();
    let failed = |e: image::ImageError| StorageError::internal(e.to_string());
    match plan.out {
        Out::Jpeg => {
            let rgb = image.to_rgb8();
            image::codecs::jpeg::JpegEncoder::new_with_quality(Cursor::new(&mut out), plan.quality)
                .write_image(
                    rgb.as_raw(),
                    rgb.width(),
                    rgb.height(),
                    image::ExtendedColorType::Rgb8,
                )
                .map_err(failed)?;
        }
        Out::Png => image
            .write_to(&mut Cursor::new(&mut out), ImageFormat::Png)
            .map_err(failed)?,
        Out::Gif => image
            .write_to(&mut Cursor::new(&mut out), ImageFormat::Gif)
            .map_err(failed)?,
        Out::Webp => {
            // libwebp rather than the encoder in `image`, which writes
            // lossless only. A lossless webp of a photograph is bigger
            // than the jpeg it was made from, and a caller who asked
            // for a hundred pixels wide did not ask for that.
            let encoder = webp::Encoder::from_image(image)
                .map_err(|e| StorageError::internal(e.to_string()))?;
            out = encoder.encode(f32::from(plan.quality)).to_vec();
        }
        Out::Avif => {
            let rgba = image.to_rgba8();
            image::codecs::avif::AvifEncoder::new_with_speed_quality(
                Cursor::new(&mut out),
                AVIF_SPEED,
                plan.quality,
            )
            .write_image(
                rgba.as_raw(),
                rgba.width(),
                rgba.height(),
                image::ExtendedColorType::Rgba8,
            )
            .map_err(failed)?;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(width: u32, height: u32, resize: Resize) -> Plan {
        Plan {
            width,
            height,
            resize,
            out: Out::Jpeg,
            quality: JPEG_QUALITY,
        }
    }

    fn cat() -> DynamicImage {
        DynamicImage::new_rgb8(350, 206)
    }

    fn size(image: &DynamicImage, plan: &Plan) -> (u32, u32) {
        let out = fitted(image, plan);
        (out.width(), out.height())
    }

    #[test]
    fn a_box_the_whole_image_fits_inside_leaves_one_side_short() {
        assert_eq!(size(&cat(), &plan(100, 100, Resize::Contain)), (100, 59));
    }

    #[test]
    fn a_box_the_image_fills_is_cropped_to_the_box() {
        assert_eq!(size(&cat(), &plan(100, 100, Resize::Cover)), (100, 100));
        assert_eq!(size(&cat(), &plan(100, 100, Resize::Fill)), (100, 100));
    }

    #[test]
    fn a_side_that_was_not_asked_about_stays_as_it_was() {
        // The surprise in the recording, and the reason the preset is
        // written down: a width on its own crops rather than scales.
        assert_eq!(size(&cat(), &plan(100, 0, Resize::Cover)), (100, 206));
        assert_eq!(size(&cat(), &plan(0, 100, Resize::Cover)), (350, 100));
    }

    #[test]
    fn nothing_is_ever_enlarged() {
        assert_eq!(size(&cat(), &plan(700, 0, Resize::Cover)), (350, 206));
        assert_eq!(size(&cat(), &plan(0, 0, Resize::Cover)), (350, 206));
        assert_eq!(size(&cat(), &plan(2501, 0, Resize::Cover)), (350, 206));
        assert_eq!(size(&cat(), &plan(700, 700, Resize::Fill)), (350, 206));
    }

    #[test]
    fn a_smaller_image_is_cropped_the_same_way() {
        // The png fixture, which is the same cat at half size.
        let half = DynamicImage::new_rgb8(175, 103);
        assert_eq!(size(&half, &plan(100, 0, Resize::Cover)), (100, 103));
    }

    #[test]
    fn a_query_that_says_nothing_asks_for_the_default_of_everything() {
        let read = wanted("querystring", |_| None).unwrap();
        assert_eq!((read.width, read.height), (0, 0));
        assert!(read.resize == Resize::Cover);
        assert!(read.quality.is_none());
        assert!(read.format == Asked::Negotiate);
    }

    #[test]
    fn a_width_that_is_not_a_number_is_refused_and_a_zero_is_not() {
        let refused = wanted("querystring", |name| {
            (name == "width").then(|| "wide".to_string())
        })
        .unwrap_err();
        assert_eq!(refused.said(), "querystring/width must be integer");
        let refused = wanted("querystring", |name| {
            (name == "width").then(|| "-1".to_string())
        })
        .unwrap_err();
        assert_eq!(refused.said(), "querystring/width must be >= 0");
        assert!(
            wanted("querystring", |name| (name == "width")
                .then(|| "0".to_string()))
            .is_ok()
        );
    }

    #[test]
    fn a_side_past_the_ceiling_is_held_to_it_rather_than_refused() {
        let read = wanted("querystring", |name| {
            (name == "width").then(|| "2501".to_string())
        })
        .unwrap();
        assert_eq!(read.width, BIGGEST);
        let read = wanted("querystring", |name| {
            (name == "height").then(|| "9000".to_string())
        })
        .unwrap();
        assert_eq!(read.height, BIGGEST);
    }

    #[test]
    fn what_the_answer_says_it_did_is_what_was_asked_for() {
        let said = |width, height, resize, quality, format| {
            did(&Wanted {
                width,
                height,
                resize,
                quality,
                format,
            })
        };

        // Height before width, and the resizing type named whenever a
        // side was. All of these are recorded.
        assert_eq!(
            said(100, 100, Resize::Cover, None, Asked::Negotiate).as_deref(),
            Some("height:100,width:100,resizing_type:fill")
        );
        assert_eq!(
            said(100, 100, Resize::Contain, None, Asked::Negotiate).as_deref(),
            Some("height:100,width:100,resizing_type:fit")
        );
        assert_eq!(
            said(100, 100, Resize::Fill, None, Asked::Negotiate).as_deref(),
            Some("height:100,width:100,resizing_type:force")
        );
        assert_eq!(
            said(100, 0, Resize::Cover, Some(20), Asked::Negotiate).as_deref(),
            Some("width:100,resizing_type:fill,quality:20")
        );
        assert_eq!(
            said(100, 0, Resize::Cover, None, Asked::Avif).as_deref(),
            Some("width:100,resizing_type:fill,format:avif")
        );

        // The request rather than the answer: a caller negotiated into
        // webp is not told so, and one who asked for origin is not
        // either.
        assert_eq!(
            said(100, 0, Resize::Cover, None, Asked::Origin).as_deref(),
            Some("width:100,resizing_type:fill")
        );

        // Nothing asked for, so nothing said, and a zero is nothing
        // asked for.
        assert_eq!(said(0, 0, Resize::Cover, None, Asked::Negotiate), None);
    }

    #[test]
    fn a_quality_outside_the_two_bounds_is_refused_at_the_bound() {
        let at = |value: &'static str| {
            wanted("querystring", move |name| {
                (name == "quality").then(|| value.to_string())
            })
        };
        assert_eq!(
            at("1").unwrap_err().said(),
            "querystring/quality must be >= 20"
        );
        assert_eq!(
            at("101").unwrap_err().said(),
            "querystring/quality must be <= 100"
        );
        assert_eq!(at("20").unwrap().quality, Some(20));
        assert_eq!(at("100").unwrap().quality, Some(100));
    }

    #[test]
    fn the_two_closed_lists_are_closed() {
        let refused = wanted("querystring", |name| {
            (name == "resize").then(|| "squash".to_string())
        })
        .unwrap_err();
        assert_eq!(
            refused.said(),
            "querystring/resize must be equal to one of the allowed values"
        );
        let refused = wanted("querystring", |name| {
            (name == "format").then(|| "jxl".to_string())
        })
        .unwrap_err();
        assert_eq!(
            refused.said(),
            "querystring/format must be equal to one of the allowed values"
        );
    }

    #[test]
    fn the_format_the_query_names_beats_the_one_the_header_takes() {
        let browser = "image/webp,image/apng,image/*,*/*;q=0.8";
        let asking = |format: Asked| Wanted {
            width: 0,
            height: 0,
            resize: Resize::Cover,
            quality: None,
            format,
        };
        assert!(chosen(&asking(Asked::Negotiate), "image/jpeg", browser) == Out::Webp);
        assert!(chosen(&asking(Asked::Origin), "image/jpeg", browser) == Out::Jpeg);
        assert!(chosen(&asking(Asked::Origin), "image/png", browser) == Out::Png);
        assert!(chosen(&asking(Asked::Avif), "image/png", browser) == Out::Avif);
    }

    #[test]
    fn a_transform_hands_back_what_it_was_given_when_nobody_negotiates() {
        let plain = Wanted {
            width: 0,
            height: 0,
            resize: Resize::Cover,
            quality: None,
            format: Asked::Negotiate,
        };
        assert!(chosen(&plain, "image/png", "") == Out::Png);
        assert!(chosen(&plain, "image/jpeg", "image/jpeg") == Out::Jpeg);
        assert!(chosen(&plain, "image/gif", "*/*") == Out::Gif);
    }

    #[test]
    fn a_client_that_takes_any_image_is_not_a_client_that_takes_webp() {
        assert!(takes_webp("image/webp"));
        assert!(takes_webp("image/webp,image/apng,image/*,*/*;q=0.8"));
        assert!(takes_webp("image/avif, image/webp;q=0.9"));
        assert!(!takes_webp("image/*"));
        assert!(!takes_webp("*/*"));
        assert!(!takes_webp("image/jpeg"));
        assert!(!takes_webp(""));
    }

    #[test]
    fn a_signing_body_with_no_transform_in_it_signs_the_object_route() {
        let asked = serde_json::json!({"expiresIn": 3600});
        assert!(signed_transform(&asked).unwrap().is_none());
    }

    #[test]
    fn a_signed_transform_survives_the_round_trip_through_a_token() {
        let asked =
            serde_json::json!({"transform": {"width": 100, "height": 100, "resize": "cover"}});
        let query = signed_transform(&asked).unwrap().unwrap();
        assert_eq!(query, "width=100&height=100&resize=cover");
        let read = from_claims(&serde_json::json!({"transform": query})).unwrap();
        assert_eq!((read.width, read.height), (100, 100));
        assert!(read.resize == Resize::Cover);
    }

    #[test]
    fn a_signing_body_asking_for_something_the_query_would_refuse_is_refused() {
        let asked = serde_json::json!({"transform": {"resize": "squash"}});
        assert_eq!(
            signed_transform(&asked).unwrap_err().said(),
            "body/transform/resize must be equal to one of the allowed values"
        );
    }

    #[test]
    fn two_transforms_of_one_object_are_two_keys_and_the_same_one_twice() {
        let wanted = |width: u32| Wanted {
            width,
            height: 0,
            resize: Resize::Cover,
            quality: None,
            format: Asked::Negotiate,
        };
        let hundred = recipe(&wanted(100), Out::Jpeg);
        assert_eq!(hundred, recipe(&wanted(100), Out::Jpeg));
        assert_ne!(hundred, recipe(&wanted(200), Out::Jpeg));
        assert_ne!(hundred, recipe(&wanted(100), Out::Webp));

        // Nothing of the object is in it, so the key it goes in has to
        // put it somewhere no other object reads, which is what the
        // renders prefix is for.
        let key = crate::blob::render_key("6f1c", "1", &hundred, "jpeg");
        assert_eq!(key, format!("renders/6f1c/1.{hundred}.jpeg"));
    }
}
