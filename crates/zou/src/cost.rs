//! `zou cost <counter-file> [--card <name>] [--stored <bytes>]
//! [--commits <n>] [--tenants <n>]`: what one run would have cost on a
//! real object store, out of the ops it actually did.
//!
//! Every dollar here comes from a counter and a price card and nothing
//! else. There is no model of what a workload might do, no per tenant
//! constant somebody picked, no rate anybody extrapolated: the counts
//! are the ones `zou stats` prints, the prices are published numbers
//! with a date and a link on them, and the arithmetic between the two
//! is a multiplication. That is the whole design, and it is the reason
//! this is a subcommand of the engine rather than a spreadsheet: a
//! spreadsheet's inputs are typed in.
//!
//! What that buys is a comparison nobody has to trust. A tps says which
//! engine was faster on a box. It does not say that one of them did
//! four hundred PUTs per second and the other did nine, which on S3
//! Standard is the difference between two dollars an hour and nothing,
//! and which is most of the argument for putting a database on object
//! storage at all. The store bill is the part of the bill our design
//! decisions actually move, so it is the part that gets measured.
//!
//! The cards live in [`cards`] and each one carries the date it was
//! read and the page it was read from, because a price card without a
//! date is a rumour. `--card-file` takes your own as json, which is
//! what to use when a card here has gone stale, when the region is not
//! the one priced, or when the store is a box in a rack. The self
//! hosted card is deliberately empty and refuses to run without
//! `--box` and `--box-tb`: a box has no list price, and inventing one
//! would put a made up number in the one place this command exists to
//! keep made up numbers out of.

use std::path::Path;

use zou_store::stats::Snapshot;

pub const USAGE: &str = "usage: zou cost <counter-file> [--since <earlier-copy>] [--card <name>] [--card-file <path>] [--stored <bytes>] [--commits <n>] [--tenants <n>] [--box <usd-per-month>] [--box-tb <n>] [--egress] [--json] [--list-cards] [--export-cards <dir>]";

/// One published price list, at a date, for one storage class in one
/// region.
///
/// Everything is a rate per unit rather than a total, so nothing in
/// here depends on the size of the workload it is applied to, and a
/// card can be read and checked against the vendor's page without
/// knowing anything about the run it will be used on.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Card {
    pub name: String,
    /// The day the numbers were read off the vendor. Printed on every
    /// result, because a cost from a card that is two years old is a
    /// cost from two years ago and should say so rather than looking
    /// like today's.
    pub as_of: String,
    pub source: String,
    /// Whatever the vendor's page does not say and the arithmetic
    /// needs. Printed under the card line so it travels with the
    /// number instead of living in a comment nobody reads.
    #[serde(default)]
    pub note: String,
    /// What the vendor means by a GB when it bills one. AWS defines it
    /// as 2^30 bytes; most other pages write GB without defining it at
    /// all. Every card here uses the binary one, which is the
    /// conservative reading, and it is a field rather than a constant
    /// because the difference is seven percent of the storage line and
    /// a card that knows better should be able to say so.
    #[serde(default = "gibibyte")]
    pub gb_bytes: f64,
    #[serde(default)]
    pub storage_per_gb_month: f64,
    /// PUT, COPY, POST and LIST, the expensive class everywhere.
    #[serde(default)]
    pub class_a_per_million: f64,
    /// GET, HEAD and everything else.
    #[serde(default)]
    pub class_b_per_million: f64,
    /// Free on every card in the table, and a field anyway, because it
    /// is free by each vendor's choice rather than by nature.
    #[serde(default)]
    pub delete_per_million: f64,
    /// Bytes written, charged on top of the request. Only S3 Express
    /// One Zone does this, and it is the line that makes a store of
    /// small objects behave differently there than on Standard.
    #[serde(default)]
    pub upload_per_gb: f64,
    /// Bytes read, charged on top of the request, and not the same
    /// thing as egress: a retrieval fee is paid even when the reader is
    /// in the same region.
    #[serde(default)]
    pub retrieval_per_gb: f64,
    /// Bytes out to the internet. Off unless `--egress` asks for it,
    /// see the comment on that flag.
    #[serde(default)]
    pub egress_per_gb: f64,
    #[serde(default)]
    pub egress_free_gb_month: f64,
    /// Backblaze gives free egress up to a multiple of what is stored,
    /// which is not a constant number of GB and cannot be written as
    /// one.
    #[serde(default)]
    pub egress_free_times_storage: f64,
    /// A card with no prices in it, which is what a machine you own is
    /// until you say what it cost.
    #[serde(default)]
    pub needs_box: bool,
}

fn gibibyte() -> f64 {
    1024.0 * 1024.0 * 1024.0
}

/// The built in cards.
///
/// AWS is read from the price list API rather than the pricing page,
/// which is where the exact per request figures live: the pricing page
/// renders them in javascript and a scrape of it returns headings. The
/// others are read off the vendor's own pricing page on the date each
/// card carries.
///
/// All of them are the cheapest generally available region, us-east-1
/// and its equivalents, because that is the region a comparison
/// defaults to and a card for another one is two edits away.
pub fn cards() -> Vec<Card> {
    let card = |name: &str, as_of: &str, source: &str| Card {
        name: name.into(),
        as_of: as_of.into(),
        source: source.into(),
        note: String::new(),
        gb_bytes: gibibyte(),
        storage_per_gb_month: 0.0,
        class_a_per_million: 0.0,
        class_b_per_million: 0.0,
        delete_per_million: 0.0,
        upload_per_gb: 0.0,
        retrieval_per_gb: 0.0,
        egress_per_gb: 0.0,
        egress_free_gb_month: 0.0,
        egress_free_times_storage: 0.0,
        needs_box: false,
    };
    vec![
        Card {
            storage_per_gb_month: 0.023,
            class_a_per_million: 5.0,
            class_b_per_million: 0.4,
            egress_per_gb: 0.09,
            egress_free_gb_month: 100.0,
            note: "us-east-1, first 50 TB. DELETE is free. 5xx responses are not billed, so \
                   the throttle and server retries below are not in the request count."
                .into(),
            ..card(
                "aws-s3-standard",
                "2026-08-29",
                "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/us-east-1/index.json",
            )
        },
        Card {
            storage_per_gb_month: 0.11,
            class_a_per_million: 1.13,
            class_b_per_million: 0.03,
            upload_per_gb: 0.0032,
            retrieval_per_gb: 0.0006,
            egress_per_gb: 0.09,
            egress_free_gb_month: 100.0,
            note: "us-east-1 directory buckets. Requests are a quarter of Standard and storage \
                   is nearly five times it, and bytes are charged on top of both, so this is \
                   the card where a run of many small objects and a run of few large ones stop \
                   agreeing. Objects have a one hour minimum billing time, which nothing here \
                   models."
                .into(),
            ..card(
                "aws-s3-express",
                "2026-08-29",
                "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/us-east-1/index.json",
            )
        },
        Card {
            storage_per_gb_month: 0.015,
            class_a_per_million: 4.5,
            class_b_per_million: 0.36,
            note: "Egress is free to the internet, so --egress changes nothing on this card. \
                   DeleteObject is neither class."
                .into(),
            ..card(
                "cloudflare-r2",
                "2026-08-29",
                "https://developers.cloudflare.com/r2/pricing/",
            )
        },
        Card {
            storage_per_gb_month: 0.020,
            class_a_per_million: 5.0,
            class_b_per_million: 0.4,
            egress_per_gb: 0.12,
            note: "us-central1 regional Standard. Class A is double this in multi region and \
                   dual region buckets. The always free tier is left out, since a tenant that \
                   fits in it is not the tenant a cost model is about."
                .into(),
            ..card(
                "gcs-standard",
                "2026-08-29",
                "https://cloud.google.com/storage/pricing",
            )
        },
        Card {
            storage_per_gb_month: 6.95 / 1024.0,
            egress_per_gb: 0.01,
            egress_free_times_storage: 3.0,
            note: "6.95 per TB per month. Class A, B and C API calls are free, which is why \
                   every request line on this card is zero, and the class D calls that are not \
                   free are ones the store never makes."
                .into(),
            ..card(
                "backblaze-b2",
                "2026-08-29",
                "https://www.backblaze.com/cloud-storage/pricing",
            )
        },
        Card {
            storage_per_gb_month: 7.99 / 1024.0,
            note: "7.99 per TB per month, no request and no egress charges at all, which makes \
                   this the one card where the shape of the workload does not affect the bill \
                   and only the footprint does. The 90 day minimum retention and the monthly \
                   minimum are not modelled."
                .into(),
            ..card(
                "wasabi",
                "2026-08-29",
                "https://wasabi.com/cloud-storage-pricing",
            )
        },
        Card {
            needs_box: true,
            note: "A machine you own, amortised. There is no list price for one, so this card \
                   has no numbers until --box and --box-tb give it some, and it will not guess. \
                   Requests and bytes are free because the disk does not bill for them; what \
                   they cost is the box, which is what --box is."
                .into(),
            ..card("self-hosted-minio", "2026-08-29", "your own invoice")
        },
    ]
}

struct Args {
    path: String,
    since: Option<String>,
    card: String,
    card_file: Option<String>,
    stored: Option<u64>,
    commits: u64,
    tenants: u64,
    box_month: Option<f64>,
    box_tb: Option<f64>,
    egress: bool,
    brief: bool,
    json: bool,
}

pub fn run(argv: &[String]) -> Result<(), String> {
    if argv.iter().any(|a| a == "--list-cards") {
        for c in cards() {
            say!("{:<18} {} as of {}", c.name, c.source, c.as_of);
        }
        return Ok(());
    }
    if let Some(i) = argv.iter().position(|a| a == "--export-cards") {
        let dir = argv
            .get(i + 1)
            .ok_or_else(|| "--export-cards wants a directory".to_string())?;
        return export(Path::new(dir));
    }
    let args = parse(argv)?;
    let card = match &args.card_file {
        Some(path) => {
            let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
            serde_json::from_str::<Card>(&text).map_err(|e| format!("{path}: {e}"))?
        }
        None => cards()
            .into_iter()
            .find(|c| c.name == args.card)
            .ok_or_else(|| {
                format!(
                    "no card named {}, try --list-cards or --card-file",
                    args.card
                )
            })?,
    };
    let card = with_box(card, args.box_month, args.box_tb)?;
    let snapshot = match &args.since {
        Some(earlier) => Snapshot::read_since(Path::new(&args.path), Path::new(earlier))?,
        None => Snapshot::read(Path::new(&args.path))?,
    };
    let counts = Counts::of(&snapshot);
    let bill = Bill::of(&counts, &card, args.stored, args.egress);
    if args.json {
        say!("{}", json(&card, &counts, &bill, &args));
    } else if args.brief {
        say!("{}", brief(&card, &counts, &bill, &args));
    } else {
        say!("{}", lines(&card, &counts, &bill, &args));
    }
    Ok(())
}

/// Write the built in cards out as one json file each, in exactly the
/// shape `--card-file` reads back.
///
/// This exists so there is one set of price cards and not two. The
/// benchmark harness in tamnd/zou-bench prices its runs against the
/// same cards, and a second copy of them living over there would drift
/// from this one the first time a vendor changed a price and only one
/// side noticed. So this side owns them, that side regenerates its copy
/// from here, and a card that has gone stale goes stale once.
///
/// The self hosted card is written out with its prices empty the way it
/// is held here, since the whole point of it is that nobody has filled
/// them in yet, and a reader that wants dollars out of it has to say
/// what the box cost.
fn export(dir: &Path) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    for card in cards() {
        let path = dir.join(format!("{}.json", card.name));
        let mut text =
            serde_json::to_string_pretty(&card).map_err(|e| format!("{}: {e}", path.display()))?;
        text.push('\n');
        std::fs::write(&path, text).map_err(|e| format!("{}: {e}", path.display()))?;
        say!("{}", path.display());
    }
    Ok(())
}

fn parse(argv: &[String]) -> Result<Args, String> {
    let mut args = Args {
        path: String::new(),
        since: None,
        card: "aws-s3-standard".into(),
        card_file: None,
        stored: None,
        commits: 0,
        tenants: 1,
        box_month: None,
        box_tb: None,
        egress: false,
        brief: false,
        json: false,
    };
    let mut rest: Vec<&String> = Vec::new();
    let mut it = argv.iter();
    while let Some(arg) = it.next() {
        let mut value = |name: &str| -> Result<String, String> {
            it.next()
                .cloned()
                .ok_or_else(|| format!("{name} wants a value"))
        };
        match arg.as_str() {
            "--egress" => args.egress = true,
            "--brief" => args.brief = true,
            "--json" => args.json = true,
            "--since" => args.since = Some(value("--since")?),
            "--card" => args.card = value("--card")?,
            "--card-file" => args.card_file = Some(value("--card-file")?),
            "--stored" => args.stored = Some(number(&value("--stored")?)? as u64),
            "--commits" => args.commits = number(&value("--commits")?)? as u64,
            "--tenants" => args.tenants = number(&value("--tenants")?)?.max(1.0) as u64,
            "--box" => args.box_month = Some(number(&value("--box")?)?),
            "--box-tb" => args.box_tb = Some(number(&value("--box-tb")?)?),
            _ => rest.push(arg),
        }
    }
    match rest.as_slice() {
        [path] => args.path = (*path).clone(),
        _ => return Err(USAGE.into()),
    }
    Ok(args)
}

fn number(text: &str) -> Result<f64, String> {
    text.parse::<f64>()
        .ok()
        .filter(|n| n.is_finite() && *n >= 0.0)
        .ok_or_else(|| format!("{text} is not a number"))
}

/// A box turned into a per GB month rate, which is the only way to put
/// one next to a bucket.
///
/// The divisor is usable terabytes and not raw ones, because a MinIO
/// erasure set gives back rather less than the disks in it and charging
/// the raw figure would make a box look half again cheaper than it is.
/// Saying which of the two you are giving is the caller's job, and the
/// note printed with the result says which one was assumed.
fn with_box(mut card: Card, per_month: Option<f64>, tb: Option<f64>) -> Result<Card, String> {
    match (per_month, tb) {
        (Some(_), Some(tb)) if tb <= 0.0 => Err("--box-tb has to be more than zero".into()),
        (Some(per_month), Some(tb)) => {
            card.storage_per_gb_month =
                per_month / (tb * 1000.0 * 1000.0 * 1000.0 * 1000.0 / card.gb_bytes);
            card.needs_box = false;
            Ok(card)
        }
        (None, None) if card.needs_box => Err(format!(
            "the {} card has no prices in it, give --box <usd per month> and --box-tb <usable tb>",
            card.name
        )),
        (None, None) => Ok(card),
        _ => Err("--box and --box-tb go together".into()),
    }
}

/// The ops, grouped the way a bill groups them rather than the way the
/// store does.
struct Counts {
    class_a: u64,
    class_b: u64,
    deletes: u64,
    up: u64,
    down: u64,
    /// Requests that went out a second time. Not billed on any card
    /// here, and counted anyway, because a run that was throttled into
    /// twice the traffic is a run whose real bill is a question.
    retries: u64,
}

impl Counts {
    fn of(snapshot: &Snapshot) -> Self {
        let sum = |names: &[&str]| -> (u64, u64) {
            snapshot
                .ops
                .iter()
                .filter(|o| names.contains(&o.op))
                .fold((0, 0), |(c, b), o| (c + o.count, b + o.bytes))
        };
        // A LIST is class A everywhere, which surprises people who
        // think of it as a read. It is the op a scan of a prefix does
        // over and over and it is priced like a write.
        let (class_a, up) = sum(&["put", "put_if_match", "list"]);
        let (class_b, down) = sum(&["get", "get_range"]);
        let (deletes, _) = sum(&["delete"]);
        Counts {
            class_a,
            class_b,
            deletes,
            up,
            down,
            retries: snapshot
                .retries
                .iter()
                .filter(|r| r.kind != "exhausted")
                .map(|r| r.count)
                .sum(),
        }
    }
}

/// Every line of the bill, in dollars, kept apart rather than summed,
/// because which line dominates is the finding and a total hides it.
struct Bill {
    class_a: f64,
    class_b: f64,
    deletes: f64,
    upload: f64,
    retrieval: f64,
    egress: f64,
    storage_month: f64,
}

impl Bill {
    fn of(counts: &Counts, card: &Card, stored: Option<u64>, egress: bool) -> Self {
        let gb = |bytes: u64| bytes as f64 / card.gb_bytes;
        let stored_gb = stored.map(gb).unwrap_or(0.0);
        let per_million = |n: u64, rate: f64| n as f64 / 1_000_000.0 * rate;
        let free = card.egress_free_gb_month + card.egress_free_times_storage * stored_gb;
        Bill {
            class_a: per_million(counts.class_a, card.class_a_per_million),
            class_b: per_million(counts.class_b, card.class_b_per_million),
            deletes: per_million(counts.deletes, card.delete_per_million),
            upload: gb(counts.up) * card.upload_per_gb,
            retrieval: gb(counts.down) * card.retrieval_per_gb,
            egress: if egress {
                (gb(counts.down) - free).max(0.0) * card.egress_per_gb
            } else {
                0.0
            },
            storage_month: stored_gb * card.storage_per_gb_month,
        }
    }

    /// What the run itself cost, which is everything except the
    /// storage. Storage is a rate per month and the run was not a
    /// month, so adding the two would be adding a quantity to a rate.
    fn work(&self) -> f64 {
        self.class_a + self.class_b + self.deletes + self.upload + self.retrieval + self.egress
    }
}

fn lines(card: &Card, counts: &Counts, bill: &Bill, args: &Args) -> String {
    let gb = |bytes: u64| bytes as f64 / card.gb_bytes;
    let mut out = format!(
        "card: {}, prices as of {}, {}",
        card.name, card.as_of, card.source
    );
    if !card.note.is_empty() {
        out.push_str(&format!("\n  {}", card.note));
    }
    out.push_str(&format!(
        "\nmeasured: {} class A, {} class B, {} deletes, {} up, {} down",
        counts.class_a,
        counts.class_b,
        counts.deletes,
        bytes(counts.up),
        bytes(counts.down)
    ));
    if counts.retries > 0 {
        out.push_str(&format!(
            "\n  {} requests went out twice and are not in that count, since no card here bills \
             a 5xx",
            counts.retries
        ));
    }
    out.push_str(&format!(
        "\nrequests: {}, class A {}, class B {}",
        usd(bill.class_a + bill.class_b + bill.deletes),
        usd(bill.class_a),
        usd(bill.class_b)
    ));
    if bill.upload > 0.0 || bill.retrieval > 0.0 {
        out.push_str(&format!(
            "\nbytes: {} up, {} down, charged on top of the requests",
            usd(bill.upload),
            usd(bill.retrieval)
        ));
    }
    // Egress is off by default and says so, because the reader in every
    // scenario we run is a postgres in the same region as the bucket,
    // where the transfer is free. A cost model that charged internet
    // egress for a page read would produce a number ten times the real
    // one and would look authoritative doing it.
    out.push_str(&match (args.egress, card.egress_per_gb) {
        (_, 0.0) => "\negress: free on this card".into(),
        (true, _) => format!(
            "\negress: {} for {} out to the internet",
            usd(bill.egress),
            bytes(counts.down)
        ),
        (false, _) => "\negress: not counted, the reader is in the region of the bucket, --egress \
                       prices it as internet transfer"
            .into(),
    });
    match args.stored {
        Some(stored) => out.push_str(&format!(
            "\nstorage: {} a month for {} held",
            usd(bill.storage_month),
            bytes(stored)
        )),
        None => out.push_str(
            "\nstorage: not counted, --stored <bytes> is the footprint the run left behind",
        ),
    }
    out.push_str(&format!(
        "\nworkload: {} for the window measured",
        usd(bill.work())
    ));
    if args.stored.is_some() {
        out.push_str(&format!(", plus {} a month held", usd(bill.storage_month)));
    }
    // The three per unit figures the cost line in M1b asks for. Each
    // one is omitted rather than printed as zero when its divisor was
    // not given, because a dollars per commit of a run whose commits
    // nobody counted is a division by an assumption.
    if args.commits > 0 {
        out.push_str(&format!(
            "\nper million commits: {}",
            usd(bill.work() / args.commits as f64 * 1_000_000.0)
        ));
    }
    if counts.up > 0 {
        out.push_str(&format!(
            "\nper GB ingested: {}",
            usd(bill.work() / gb(counts.up))
        ));
    }
    if args.stored.is_some() {
        out.push_str(&format!(
            "\nper idle tenant month: {} across {} tenant{}",
            usd(bill.storage_month / args.tenants as f64),
            args.tenants,
            if args.tenants == 1 { "" } else { "s" }
        ));
    }
    out
}

/// The same bill as two lines, which is what a benchmark phase has room
/// for under its tps.
///
/// The card's name is on it and its note is not. A phase line is read in
/// a log next to twenty other phase lines and the paragraph explaining
/// what a directory bucket charges for belongs where somebody went
/// looking for it, which is the long form. What has to survive the
/// shortening is the name of the card and the split between the two
/// request classes, because those are what make the number checkable.
fn brief(card: &Card, counts: &Counts, bill: &Bill, args: &Args) -> String {
    let gb = |bytes: u64| bytes as f64 / card.gb_bytes;
    let mut out = format!(
        "cost: {} on {}, class A {}, class B {}",
        usd(bill.work()),
        card.name,
        usd(bill.class_a),
        usd(bill.class_b)
    );
    if bill.upload > 0.0 || bill.retrieval > 0.0 {
        out.push_str(&format!(", bytes {}", usd(bill.upload + bill.retrieval)));
    }
    if args.stored.is_some() {
        out.push_str(&format!(", {} a month held", usd(bill.storage_month)));
    }
    let mut per: Vec<String> = Vec::new();
    if args.commits > 0 {
        per.push(format!(
            "{} a million commits",
            usd(bill.work() / args.commits as f64 * 1_000_000.0)
        ));
    }
    if counts.up > 0 {
        per.push(format!(
            "{} a GB ingested",
            usd(bill.work() / gb(counts.up))
        ));
    }
    if args.stored.is_some() {
        per.push(format!(
            "{} an idle tenant month",
            usd(bill.storage_month / args.tenants as f64)
        ));
    }
    if !per.is_empty() {
        out.push_str(&format!("\nper: {}", per.join(", ")));
    }
    out
}

fn json(card: &Card, counts: &Counts, bill: &Bill, args: &Args) -> String {
    let gb = |bytes: u64| bytes as f64 / card.gb_bytes;
    let mut value = serde_json::json!({
        "card": {"name": card.name, "as_of": card.as_of, "source": card.source},
        "measured": {
            "class_a": counts.class_a,
            "class_b": counts.class_b,
            "deletes": counts.deletes,
            "up_bytes": counts.up,
            "down_bytes": counts.down,
            "retries": counts.retries,
            "stored_bytes": args.stored,
        },
        "usd": {
            "class_a": bill.class_a,
            "class_b": bill.class_b,
            "deletes": bill.deletes,
            "upload": bill.upload,
            "retrieval": bill.retrieval,
            "egress": bill.egress,
            "workload": bill.work(),
            "storage_per_month": bill.storage_month,
        },
    });
    let per = value["usd"].as_object_mut().expect("an object");
    if args.commits > 0 {
        per.insert(
            "per_million_commits".into(),
            (bill.work() / args.commits as f64 * 1_000_000.0).into(),
        );
    }
    if counts.up > 0 {
        per.insert(
            "per_gb_ingested".into(),
            (bill.work() / gb(counts.up)).into(),
        );
    }
    if args.stored.is_some() {
        per.insert(
            "per_idle_tenant_month".into(),
            (bill.storage_month / args.tenants as f64).into(),
        );
    }
    value.to_string()
}

/// Dollars at the scale they landed on. A store bill for one benchmark
/// phase is millionths of a dollar and the same bill for a month of a
/// fleet is thousands, and four decimal places is unreadable at one end
/// and a lie at the other, so the places follow the number.
fn usd(amount: f64) -> String {
    if amount == 0.0 {
        "$0".into()
    } else if amount >= 1.0 {
        format!("${amount:.2}")
    } else if amount >= 0.001 {
        format!("${amount:.4}")
    } else {
        format!("${amount:.7}")
    }
}

/// Binary units, the same as everywhere else in the tree, because the
/// thing being counted is pages and a page is 8 KiB.
fn bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut size = n as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit + 1 < UNITS.len() {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{size:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zou_store::stats::{ClassSnapshot, GapSnapshot, OpSnapshot, RetrySnapshot};

    fn op(name: &'static str, count: u64, bytes: u64) -> OpSnapshot {
        OpSnapshot {
            op: name,
            count,
            bytes,
            errors: 0,
            p50_us: 0,
            p95_us: 0,
            p99_us: 0,
            max_us: 0,
            by_class: vec![ClassSnapshot {
                class: "page",
                count,
                bytes,
            }],
            buckets: Vec::new(),
        }
    }

    fn snapshot(ops: Vec<OpSnapshot>) -> Snapshot {
        Snapshot {
            conflicts: 0,
            ops,
            reads: Vec::new(),
            pagesvc: Vec::new(),
            commit: Vec::new(),
            park_cause: Vec::new(),
            park_gap: GapSnapshot {
                samples: 0,
                p50_bytes: 0,
                p95_bytes: 0,
                p99_bytes: 0,
                max_bytes: 0,
            },
            retries: Vec::new(),
            packed: Vec::new(),
        }
    }

    fn args() -> Args {
        Args {
            path: String::new(),
            since: None,
            card: "aws-s3-standard".into(),
            card_file: None,
            stored: None,
            commits: 0,
            tenants: 1,
            box_month: None,
            box_tb: None,
            egress: false,
            brief: false,
            json: false,
        }
    }

    fn named(name: &str) -> Card {
        cards()
            .into_iter()
            .find(|c| c.name == name)
            .expect("a built in card")
    }

    /// A LIST is billed as a write everywhere, which is the one grouping
    /// in here somebody would get wrong by reading the op names.
    #[test]
    fn a_list_is_class_a_and_a_range_get_is_class_b() {
        let counts = Counts::of(&snapshot(vec![
            op("put", 10, 1000),
            op("put_if_match", 5, 500),
            op("list", 7, 0),
            op("get", 100, 8000),
            op("get_range", 3, 300),
            op("delete", 2, 0),
        ]));
        assert_eq!(counts.class_a, 22);
        assert_eq!(counts.class_b, 103);
        assert_eq!(counts.deletes, 2);
        assert_eq!(counts.up, 1500);
        assert_eq!(counts.down, 8300);
    }

    /// The arithmetic, against a card whose rates are public, done by
    /// hand: a million class A on S3 Standard is five dollars and a
    /// million class B is forty cents, and nothing about the run
    /// changes that.
    #[test]
    fn the_bill_is_the_counts_times_the_published_rates() {
        let counts = Counts::of(&snapshot(vec![
            op("put", 1_000_000, 0),
            op("get", 1_000_000, 0),
        ]));
        let bill = Bill::of(&counts, &named("aws-s3-standard"), None, false);
        assert!((bill.class_a - 5.0).abs() < 1e-9, "{}", bill.class_a);
        assert!((bill.class_b - 0.4).abs() < 1e-9, "{}", bill.class_b);
        assert!((bill.work() - 5.4).abs() < 1e-9, "{}", bill.work());
    }

    /// Express One Zone charges for the bytes as well as the request,
    /// which is the whole reason a run of small objects and a run of
    /// large ones do not scale together on it.
    #[test]
    fn express_charges_for_bytes_on_top_of_requests() {
        let gib = 1024 * 1024 * 1024;
        let counts = Counts::of(&snapshot(vec![op("put", 1, gib), op("get", 1, 2 * gib)]));
        let bill = Bill::of(&counts, &named("aws-s3-express"), None, false);
        assert!((bill.upload - 0.0032).abs() < 1e-9, "{}", bill.upload);
        assert!((bill.retrieval - 0.0012).abs() < 1e-9, "{}", bill.retrieval);
    }

    /// Egress stays out unless it is asked for, because every scenario
    /// this runs in reads from the region the bucket is in, where it is
    /// free, and a page read charged at internet rates would be off by
    /// an order of magnitude in the direction that looks authoritative.
    #[test]
    fn egress_is_off_until_it_is_asked_for() {
        let counts = Counts::of(&snapshot(vec![op("get", 1, 200 * 1024 * 1024 * 1024)]));
        let card = named("aws-s3-standard");
        assert_eq!(Bill::of(&counts, &card, None, false).egress, 0.0);
        // 200 GiB out, the first 100 free, at nine cents.
        let charged = Bill::of(&counts, &card, None, true).egress;
        assert!((charged - 9.0).abs() < 1e-9, "{charged}");
    }

    /// Backblaze gives free egress as a multiple of what is stored,
    /// which only has a value once the footprint is known.
    #[test]
    fn free_egress_can_depend_on_the_footprint() {
        let gib = 1024 * 1024 * 1024;
        let counts = Counts::of(&snapshot(vec![op("get", 1, 100 * gib)]));
        let card = named("backblaze-b2");
        // Ten GiB stored buys thirty free, so seventy of the hundred
        // are charged at a cent.
        let bill = Bill::of(&counts, &card, Some(10 * gib), true);
        assert!((bill.egress - 0.70).abs() < 1e-9, "{}", bill.egress);
    }

    /// A machine has no list price and this refuses to invent one.
    #[test]
    fn the_self_hosted_card_will_not_guess_what_a_box_cost() {
        let err = with_box(named("self-hosted-minio"), None, None).expect_err("a refusal");
        assert!(err.contains("--box"), "{err}");
        // 100 a month for 20 usable TB, which is 20e12 bytes over
        // 2^30 bytes to the GB, so a shade under half a cent a GB.
        let card =
            with_box(named("self-hosted-minio"), Some(100.0), Some(20.0)).expect("a priced card");
        let want = 100.0 / (20.0e12 / gibibyte());
        assert!(
            (card.storage_per_gb_month - want).abs() < 1e-12,
            "{}",
            card.storage_per_gb_month
        );
        // And the requests stay free, because the disk does not bill.
        assert_eq!(card.class_a_per_million, 0.0);
    }

    #[test]
    fn a_box_needs_both_halves_of_its_price() {
        assert!(with_box(named("wasabi"), Some(100.0), None).is_err());
        assert!(with_box(named("wasabi"), None, Some(20.0)).is_err());
        assert!(with_box(named("wasabi"), None, None).is_ok());
    }

    /// The per unit lines are the ones M1b asks for, and each is left
    /// out rather than divided by an assumption when its divisor is
    /// missing.
    #[test]
    fn a_missing_divisor_leaves_its_line_out() {
        let counts = Counts::of(&snapshot(vec![op("put", 1_000_000, 0)]));
        let card = named("aws-s3-standard");
        let bare = lines(
            &card,
            &counts,
            &Bill::of(&counts, &card, None, false),
            &args(),
        );
        assert!(!bare.contains("per million commits"), "{bare}");
        assert!(!bare.contains("per idle tenant month"), "{bare}");
        assert!(bare.contains("storage: not counted"), "{bare}");

        let mut with = args();
        with.commits = 2_000_000;
        with.stored = Some(1024 * 1024 * 1024);
        with.tenants = 4;
        let bill = Bill::of(&counts, &card, with.stored, false);
        let full = lines(&card, &counts, &bill, &with);
        // Five dollars of puts over two million commits.
        assert!(full.contains("per million commits: $2.50"), "{full}");
        // A GiB at 2.3 cents, split four ways.
        assert!(
            full.contains("per idle tenant month: $0.0057 across 4 tenants"),
            "{full}"
        );
    }

    /// A run that was throttled did twice the traffic, and the count
    /// says so even though no card here bills it.
    #[test]
    fn retries_are_reported_and_not_billed() {
        let mut snap = snapshot(vec![op("put", 100, 0)]);
        snap.retries = vec![
            RetrySnapshot {
                kind: "throttle",
                count: 40,
            },
            RetrySnapshot {
                kind: "exhausted",
                count: 3,
            },
        ];
        let counts = Counts::of(&snap);
        assert_eq!(counts.retries, 40);
        let card = named("aws-s3-standard");
        let out = lines(
            &card,
            &counts,
            &Bill::of(&counts, &card, None, false),
            &args(),
        );
        assert!(out.contains("40 requests went out twice"), "{out}");
    }

    /// A card read off a file is the way a stale table gets fixed
    /// without a release, so an unknown key in one has to be an error
    /// rather than a field quietly ignored.
    #[test]
    fn a_card_file_rejects_a_key_it_does_not_know() {
        let good = r#"{"name":"mine","as_of":"2026-08-29","source":"an invoice",
                       "storage_per_gb_month":0.01,"class_a_per_million":1.0}"#;
        let card: Card = serde_json::from_str(good).expect("a card");
        assert_eq!(card.class_b_per_million, 0.0);
        assert_eq!(card.gb_bytes, gibibyte());
        let typo = r#"{"name":"mine","as_of":"2026-08-29","source":"an invoice",
                       "class_a_per_millon":1.0}"#;
        assert!(serde_json::from_str::<Card>(typo).is_err());
    }

    /// The short form is what goes under a phase's tps, so it has to
    /// keep the card's name and the class split and drop the prose.
    #[test]
    fn the_brief_form_keeps_the_card_and_drops_the_paragraph() {
        let counts = Counts::of(&snapshot(vec![
            op("put", 1_000_000, 1024 * 1024 * 1024),
            op("get", 1_000_000, 0),
        ]));
        let card = named("aws-s3-standard");
        let mut with = args();
        with.commits = 10_000_000;
        with.stored = Some(1024 * 1024 * 1024);
        let bill = Bill::of(&counts, &card, with.stored, false);
        assert_eq!(
            brief(&card, &counts, &bill, &with),
            "cost: $5.40 on aws-s3-standard, class A $5.00, class B $0.4000, $0.0230 a month held\n\
             per: $0.5400 a million commits, $5.40 a GB ingested, $0.0230 an idle tenant month"
        );
        assert!(!brief(&card, &counts, &bill, &with).contains("us-east-1"));
    }

    #[test]
    fn dollars_read_at_the_scale_they_landed_on() {
        assert_eq!(usd(0.0), "$0");
        assert_eq!(usd(0.0000004), "$0.0000004");
        assert_eq!(usd(0.0344), "$0.0344");
        assert_eq!(usd(12.5), "$12.50");
    }

    /// Every card is a published price with a date and a link, and the
    /// one that is not says so by having no prices at all.
    #[test]
    fn every_card_is_dated_and_sourced() {
        for card in cards() {
            assert!(!card.as_of.is_empty(), "{} has no date", card.name);
            assert!(!card.source.is_empty(), "{} has no source", card.name);
            assert!(!card.note.is_empty(), "{} has no note", card.name);
            assert!(card.gb_bytes > 0.0, "{} bills in nothing", card.name);
            if !card.needs_box {
                assert!(
                    card.storage_per_gb_month > 0.0,
                    "{} stores for free",
                    card.name
                );
            }
        }
    }

    /// The export is what keeps the benchmark harness from carrying a
    /// second set of cards, so what comes out of it has to be what
    /// `--card-file` reads back, to the last field. A round trip that
    /// lost `egress_free_times_storage` would be a card that quietly
    /// billed Backblaze for egress it gives away.
    #[test]
    fn an_exported_card_reads_back_as_itself() {
        let dir = std::env::temp_dir().join(format!("zou-cost-export-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        export(&dir).expect("export");
        for want in cards() {
            let text =
                std::fs::read_to_string(dir.join(format!("{}.json", want.name))).expect("read");
            let got: Card = serde_json::from_str(&text).expect("parse");
            assert_eq!(got.name, want.name);
            assert_eq!(got.as_of, want.as_of);
            assert_eq!(got.source, want.source);
            assert_eq!(got.note, want.note);
            assert_eq!(got.gb_bytes, want.gb_bytes);
            assert_eq!(got.storage_per_gb_month, want.storage_per_gb_month);
            assert_eq!(got.class_a_per_million, want.class_a_per_million);
            assert_eq!(got.class_b_per_million, want.class_b_per_million);
            assert_eq!(got.delete_per_million, want.delete_per_million);
            assert_eq!(got.upload_per_gb, want.upload_per_gb);
            assert_eq!(got.retrieval_per_gb, want.retrieval_per_gb);
            assert_eq!(got.egress_per_gb, want.egress_per_gb);
            assert_eq!(got.egress_free_gb_month, want.egress_free_gb_month);
            assert_eq!(
                got.egress_free_times_storage,
                want.egress_free_times_storage
            );
            assert_eq!(got.needs_box, want.needs_box);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
