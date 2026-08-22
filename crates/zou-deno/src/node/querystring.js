// node:querystring, which is the older half of what URLSearchParams
// does: text to an object and back.
//
// Not written on URLSearchParams, because the two disagree about a
// plus sign in a value and about what a repeated key gives back, and a
// package that asks for this module wants node's answers.

export function escape(text) {
  return encodeURIComponent(String(text));
}

export function unescape(text) {
  try {
    return decodeURIComponent(String(text).replaceAll("+", " "));
  } catch {
    // Node hands the string back rather than throwing on a stray
    // percent, and code that parses whatever arrived depends on it.
    return String(text);
  }
}

export function parse(text, sep = "&", eq = "=", options = {}) {
  const out = Object.create(null);
  if (typeof text !== "string" || text.length === 0) {
    return out;
  }
  const limit = options.maxKeys ?? 1000;
  let seen = 0;
  for (const pair of text.split(sep)) {
    if (pair.length === 0) {
      continue;
    }
    if (limit > 0 && seen >= limit) {
      break;
    }
    seen += 1;
    const at = pair.indexOf(eq);
    const key = unescape(at === -1 ? pair : pair.slice(0, at));
    const value = at === -1 ? "" : unescape(pair.slice(at + eq.length));
    // A key twice is an array, a key once is a string, which is the
    // shape a package branches on.
    if (key in out) {
      if (Array.isArray(out[key])) {
        out[key].push(value);
      } else {
        out[key] = [out[key], value];
      }
    } else {
      out[key] = value;
    }
  }
  return out;
}

export function stringify(value, sep = "&", eq = "=") {
  if (value === null || typeof value !== "object") {
    return "";
  }
  const parts = [];
  for (const [key, held] of Object.entries(value)) {
    const name = escape(key);
    if (Array.isArray(held)) {
      for (const one of held) {
        parts.push(`${name}${eq}${escape(one)}`);
      }
    } else if (typeof held === "object" && held !== null) {
      // Node writes an empty value for anything it cannot flatten.
      parts.push(`${name}${eq}`);
    } else {
      parts.push(`${name}${eq}${escape(held)}`);
    }
  }
  return parts.join(sep);
}

export const decode = parse;
export const encode = stringify;

export default { parse, stringify, decode, encode, escape, unescape };
