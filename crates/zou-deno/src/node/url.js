// node:url. The two classes are the global ones, because a URL that a
// package parses through this module and hands to `fetch` has to be
// the same kind of object the runtime's own `fetch` takes.
//
// What is left is the file url conversions and the legacy `parse`,
// which is what code written before `URL` existed still calls.

const URLClass = globalThis.URL;
const URLSearchParamsClass = globalThis.URLSearchParams;

export function fileURLToPath(url) {
  const held = typeof url === "string" ? new URLClass(url) : url;
  // A package here is a url rather than a directory somebody unpacked,
  // so the file a package reads beside itself is a url too. What this
  // is for is `readFileSync(fileURLToPath(new URL('./x.wasm',
  // import.meta.url)))`, which is how a wasm library finds its own
  // wasm, and the reads take a url already: `Deno.readFile` of one
  // goes through the cache the modules are fetched into. So the url
  // comes back as it went in rather than as a path that would name
  // nothing on this disk.
  if (held.protocol === "https:" || held.protocol === "http:") {
    return held.href;
  }
  if (held.protocol !== "file:") {
    const wrong = new TypeError("The URL must be of scheme file");
    wrong.code = "ERR_INVALID_URL_SCHEME";
    throw wrong;
  }
  return decodeURIComponent(held.pathname);
}

export function pathToFileURL(path) {
  const made = new URLClass("file:///");
  // Every segment encoded on its own, so a slash in the path stays a
  // separator and a `#` in a name does not become a fragment.
  made.pathname = String(path)
    .split("/")
    .map((part) => encodeURIComponent(part))
    .join("/");
  return made;
}

export function urlToHttpOptions(url) {
  return {
    protocol: url.protocol,
    hostname: url.hostname,
    hash: url.hash,
    search: url.search,
    pathname: url.pathname,
    path: `${url.pathname}${url.search}`,
    href: url.href,
    port: url.port,
    auth: url.username ? `${url.username}:${url.password}` : undefined,
  };
}

export function format(url, options) {
  if (typeof url === "string") {
    return url;
  }
  if (url instanceof URLClass) {
    const held = new URLClass(url.href);
    if (options?.search === false) {
      held.search = "";
    }
    if (options?.fragment === false) {
      held.hash = "";
    }
    if (options?.auth === false) {
      held.username = "";
      held.password = "";
    }
    return held.href;
  }
  // The legacy object, put back together the way `parse` took it
  // apart.
  const auth = url.auth ? `${url.auth}@` : "";
  const host = url.host ?? `${url.hostname ?? ""}${url.port ? `:${url.port}` : ""}`;
  const search = url.search ?? (url.query ? `?${url.query}` : "");
  return `${url.protocol ?? ""}//${auth}${host}${url.pathname ?? ""}${search}${url.hash ?? ""}`;
}

/// The shape node's `url.parse` gives back, over the parser this
/// runtime has. A relative url has no base to resolve against here and
/// comes back with its parts and nothing else, which is what node does
/// with one too.
export function parse(text, parseQueryString = false) {
  let held;
  try {
    held = new URLClass(text);
  } catch {
    return {
      protocol: null,
      slashes: null,
      auth: null,
      host: null,
      port: null,
      hostname: null,
      hash: null,
      search: null,
      query: parseQueryString ? {} : null,
      pathname: text,
      path: text,
      href: text,
    };
  }
  return {
    protocol: held.protocol,
    slashes: true,
    auth: held.username ? `${held.username}:${held.password}` : null,
    host: held.host,
    port: held.port === "" ? null : held.port,
    hostname: held.hostname,
    hash: held.hash === "" ? null : held.hash,
    search: held.search === "" ? null : held.search,
    query: parseQueryString
      ? Object.fromEntries(held.searchParams)
      : held.search === ""
        ? null
        : held.search.slice(1),
    pathname: held.pathname,
    path: `${held.pathname}${held.search}`,
    href: held.href,
  };
}

export function resolve(from, to) {
  return new URLClass(to, from).href;
}

export function domainToASCII(domain) {
  try {
    return new URLClass(`http://${domain}`).hostname;
  } catch {
    return "";
  }
}

export function domainToUnicode(domain) {
  return domainToASCII(domain);
}

export { URLClass as URL, URLSearchParamsClass as URLSearchParams };

export default {
  URL: URLClass,
  URLSearchParams: URLSearchParamsClass,
  fileURLToPath,
  pathToFileURL,
  urlToHttpOptions,
  format,
  parse,
  resolve,
  domainToASCII,
  domainToUnicode,
};
