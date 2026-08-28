// node:punycode, RFC 3492, which is how a domain label that is not
// ascii is written as one that is.
//
// Node has deprecated this module for years and still ships it, and
// packages reach it through a dependency rather than by writing the
// import themselves, which is how two functions in the examples corpus
// come to need it. There is nothing for the host to do in any of it:
// the whole module is integer arithmetic the RFC writes out in full,
// and the parts of it below are named after the variables there so the
// two can be read side by side.

const base = 36;
const tmin = 1;
const tmax = 26;
const skew = 38;
const damp = 700;
const initialBias = 72;
// Everything below this is a basic code point, that is, ascii.
const initialN = 128;
const delimiter = "-";
const maxInt = 2147483647;

// A label that was encoded says so with this in front of it, and a
// label worth encoding has something outside ascii in it.
const encoded = /^xn--/;
const above = /[^\0-\x7f]/;
// The three dots IDNA treats as a dot, besides the dot.
const dots = /[\x2e。．｡]/g;

const reasons = {
  overflow: "Overflow: input needs wider integers to process",
  "not-basic": "Illegal input >= 0x80 (not a basic code point)",
  "invalid-input": "Invalid input",
};

function refuse(reason) {
  throw new RangeError(reasons[reason]);
}

// Run a function over each label of a domain, leaving a mail address's
// local part alone, which is what node does with the @ it finds.
function labels(domain, over) {
  const parts = String(domain).split("@");
  let local = "";
  let rest = parts[0];
  if (parts.length > 1) {
    local = parts[0] + "@";
    rest = parts.slice(1).join("@");
  }
  return local + rest.replace(dots, ".").split(".").map(over).join(".");
}

// A digit of the base 36 alphabet, or the base itself for anything that
// is not one, which every caller reads as a refusal.
function digitOf(code) {
  if (code >= 0x30 && code < 0x3a) {
    return code - 0x16;
  }
  if (code >= 0x41 && code < 0x5b) {
    return code - 0x41;
  }
  if (code >= 0x61 && code < 0x7b) {
    return code - 0x61;
  }
  return base;
}

// The other way, in the RFC's own arithmetic. The flag is the upper
// case bit, and nothing here sets it.
function letterOf(digit, flag) {
  return digit + 22 + 75 * (digit < 26) - ((flag != 0) << 5);
}

// The bias after a code point was written, which is what keeps the
// encoding short for a label whose characters are near each other.
function adapt(delta, points, first) {
  let moved = first ? Math.floor(delta / damp) : delta >> 1;
  moved += Math.floor(moved / points);
  let k = 0;
  while (moved > ((base - tmin) * tmax) >> 1) {
    moved = Math.floor(moved / (base - tmin));
    k += base;
  }
  return Math.floor(k + ((base - tmin + 1) * moved) / (moved + skew));
}

// A string as its code points, keeping a lone surrogate as it is
// rather than replacing it, which is what node keeps too.
function points(text) {
  const out = [];
  let at = 0;
  const string = String(text);
  while (at < string.length) {
    const value = string.charCodeAt(at);
    at += 1;
    if (value >= 0xd800 && value <= 0xdbff && at < string.length) {
      const low = string.charCodeAt(at);
      at += 1;
      if ((low & 0xfc00) === 0xdc00) {
        out.push(((value & 0x3ff) << 10) + (low & 0x3ff) + 0x10000);
      } else {
        out.push(value);
        at -= 1;
      }
    } else {
      out.push(value);
    }
  }
  return out;
}

function fromPoints(codes) {
  // In pieces, because a label is short but nothing stops a caller
  // handing this an array longer than the argument limit.
  let out = "";
  for (let at = 0; at < codes.length; at += 4096) {
    out += String.fromCodePoint(...codes.slice(at, at + 4096));
  }
  return out;
}

/// The punycode of one label back to the string it stands for. The
/// `xn--` is not part of it and the caller takes it off.
export function decode(input) {
  const text = String(input);
  const out = [];
  let n = initialN;
  let bias = initialBias;
  let i = 0;

  // Everything before the last delimiter came through as it was.
  let basic = text.lastIndexOf(delimiter);
  if (basic < 0) {
    basic = 0;
  }
  for (let at = 0; at < basic; at += 1) {
    if (text.charCodeAt(at) >= 0x80) {
      refuse("not-basic");
    }
    out.push(text.charCodeAt(at));
  }

  let index = basic > 0 ? basic + 1 : 0;
  while (index < text.length) {
    // Each round reads one generalised variable length integer and
    // inserts one code point at the position it names.
    const was = i;
    let w = 1;
    for (let k = base; ; k += base) {
      if (index >= text.length) {
        refuse("invalid-input");
      }
      const digit = digitOf(text.charCodeAt(index));
      index += 1;
      if (digit >= base || digit > Math.floor((maxInt - i) / w)) {
        refuse(digit >= base ? "invalid-input" : "overflow");
      }
      i += digit * w;
      const t = k <= bias ? tmin : k >= bias + tmax ? tmax : k - bias;
      if (digit < t) {
        break;
      }
      if (w > Math.floor(maxInt / (base - t))) {
        refuse("overflow");
      }
      w *= base - t;
    }
    const length = out.length + 1;
    bias = adapt(i - was, length, was === 0);
    if (Math.floor(i / length) > maxInt - n) {
      refuse("overflow");
    }
    n += Math.floor(i / length);
    i %= length;
    out.splice(i, 0, n);
    i += 1;
  }
  return fromPoints(out);
}

/// A label as punycode, without the `xn--` in front of it, which the
/// caller puts there.
export function encode(input) {
  const codes = points(input);
  const out = [];
  let n = initialN;
  let delta = 0;
  let bias = initialBias;

  for (const code of codes) {
    if (code < 0x80) {
      out.push(String.fromCharCode(code));
    }
  }
  const basic = out.length;
  let handled = basic;
  if (basic) {
    out.push(delimiter);
  }

  while (handled < codes.length) {
    // The smallest code point still to be written, which is the next
    // one the counter has to reach.
    let next = maxInt;
    for (const code of codes) {
      if (code >= n && code < next) {
        next = code;
      }
    }
    const done = handled + 1;
    if (next - n > Math.floor((maxInt - delta) / done)) {
      refuse("overflow");
    }
    delta += (next - n) * done;
    n = next;
    for (const code of codes) {
      if (code < n) {
        delta += 1;
        if (delta > maxInt) {
          refuse("overflow");
        }
      }
      if (code !== n) {
        continue;
      }
      let q = delta;
      for (let k = base; ; k += base) {
        const t = k <= bias ? tmin : k >= bias + tmax ? tmax : k - bias;
        if (q < t) {
          break;
        }
        out.push(String.fromCharCode(letterOf(t + ((q - t) % (base - t)), 0)));
        q = Math.floor((q - t) / (base - t));
      }
      out.push(String.fromCharCode(letterOf(q, 0)));
      bias = adapt(delta, done, handled === basic);
      delta = 0;
      handled += 1;
    }
    delta += 1;
    n += 1;
  }
  return out.join("");
}

/// A domain with its encoded labels read back, leaving alone the ones
/// that were never encoded.
export function toUnicode(domain) {
  return labels(domain, (label) =>
    encoded.test(label) ? decode(label.slice(4).toLowerCase()) : label,
  );
}

/// A domain with its non ascii labels encoded, leaving alone the ones
/// that are ascii already.
export function toASCII(domain) {
  return labels(domain, (label) => (above.test(label) ? "xn--" + encode(label) : label));
}

/// The pair node keeps a string's code points behind. It is here
/// because a package that wants one of them asks for this object by
/// name rather than for a top level function.
export const ucs2 = { decode: points, encode: fromPoints };

/// What node reports, and what a package that checks reads. This is the
/// last version of the standalone library node vendored.
export const version = "2.3.1";

export default { decode, encode, toASCII, toUnicode, ucs2, version };
