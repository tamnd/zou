// node:dns/promises, which is two different questions wearing the same
// name.
//
// `lookup` is what a program means when it says resolve a hostname: the
// host's own resolution, the one every other process on the machine
// gets, so `localhost` is the loopback, a line in `/etc/hosts` is
// honoured and a search domain is appended. It answers with an address
// and nothing else.
//
// `resolve` and the `resolveX` family are queries put on the wire for
// records of one type. That is the only way to see an MX or a TXT, and
// it is what a mail sender is doing when it asks a domain where its
// mail goes before opening a socket to it.
//
// The shapes here are node's rather than the runtime's underneath. Node
// calls an MX record's first field `priority` and a SRV record's target
// `name`, and a package written against node reads those names, so the
// translation happens here where it can be seen.

import { isIP } from "node:net";

/// Which resolver the queries below go to, when something has said.
/// Empty means the host's own, which is the ordinary case.
let servers = [];

/// The failure node's dns raises, which a package catches by `code`.
function failed(code, syscall, hostname, why) {
  const wrong = new Error(`${syscall} ${code} ${hostname}${why ? `: ${why}` : ""}`);
  wrong.code = code;
  wrong.errno = code;
  wrong.syscall = syscall;
  wrong.hostname = hostname;
  return wrong;
}

/// A resolver as node writes one, which is an address on its own or a
/// v6 address in brackets, either of them with a port after it.
function serverOf(said) {
  const written = String(said).trim();
  if (written.startsWith("[")) {
    const end = written.indexOf("]");
    if (end < 0) throw new TypeError(`${said} is not an address a resolver could be at`);
    const ipAddr = written.slice(1, end);
    const port = written.slice(end + 1).replace(/^:/, "");
    return { ipAddr, port: port === "" ? 53 : Number(port) };
  }
  // One colon is an address and a port, more than one is a v6 address
  // that was written without its brackets.
  const parts = written.split(":");
  if (parts.length === 2) return { ipAddr: parts[0], port: Number(parts[1]) };
  return { ipAddr: written, port: 53 };
}

export function getServers() {
  return servers.map((server) => (server.port === 53 ? server.ipAddr : `${server.ipAddr}:${server.port}`));
}

export function setServers(list) {
  if (!Array.isArray(list)) {
    throw new TypeError("setServers takes an array of resolvers");
  }
  servers = list.map(serverOf);
}

/// One query, with the record type node calls it by.
async function asked(hostname, kind, syscall, server) {
  const named = server ?? servers[0];
  try {
    return await Deno.resolveDns(String(hostname), kind, named ? { nameServer: named } : undefined);
  } catch (why) {
    throw failed(why.code ?? "ESERVFAIL", syscall, String(hostname), why.message);
  }
}

/// The address of a name, which is the host's own resolution and not a
/// query. An address that was handed in rather than a name is itself,
/// which is what node does with one and what saves a package that
/// resolves whatever it was configured with.
export async function lookup(hostname, options = {}) {
  const family = typeof options === "number" ? options : (options.family ?? 0);
  const all = typeof options === "object" && options !== null && options.all === true;
  const said = String(hostname);
  const literal = isIP(said);
  let found;
  if (literal !== 0) {
    found = [{ address: said, family: literal }];
  } else {
    try {
      found = await globalThis.__zouLookup(said);
    } catch (why) {
      throw failed(why.code ?? "ENOTFOUND", "getaddrinfo", said, why.message);
    }
  }
  const wanted = family === 0 ? found : found.filter((one) => one.family === family);
  if (wanted.length === 0) {
    throw failed("ENOTFOUND", "getaddrinfo", said, `no address of family ${family}`);
  }
  return all ? wanted : wanted[0];
}

/// The other half of the host's own resolution, which asks a service
/// rather than a name. There is no getnameinfo here, and a port number
/// invented for a name would be a lie a package would print.
export async function lookupService() {
  throw new TypeError(
    "node:dns lookupService asks the host what a port is called, which this runtime does not have. Ask for the records you want with resolve instead",
  );
}

export async function resolve4(hostname, options = {}) {
  ttlless(options, "resolve4");
  return await asked(hostname, "A", "queryA");
}

export async function resolve6(hostname, options = {}) {
  ttlless(options, "resolve6");
  return await asked(hostname, "AAAA", "queryAaaa");
}

export async function resolveCname(hostname) {
  return await asked(hostname, "CNAME", "queryCname");
}

export async function resolveNs(hostname) {
  return await asked(hostname, "NS", "queryNs");
}

export async function resolvePtr(hostname) {
  return await asked(hostname, "PTR", "queryPtr");
}

export async function resolveTxt(hostname) {
  return await asked(hostname, "TXT", "queryTxt");
}

/// What a domain says about where its mail goes, which is the record
/// this module was written for. Node calls the first field `priority`
/// and the wire calls it a preference.
export async function resolveMx(hostname) {
  const found = await asked(hostname, "MX", "queryMx");
  return found.map((one) => ({ priority: one.preference, exchange: one.exchange }));
}

/// Node calls the target of a SRV record its name.
export async function resolveSrv(hostname) {
  const found = await asked(hostname, "SRV", "querySrv");
  return found.map((one) => ({
    priority: one.priority,
    weight: one.weight,
    port: one.port,
    name: one.target,
  }));
}

/// One record rather than a list, which is what a zone has one of.
export async function resolveSoa(hostname) {
  const [one] = await asked(hostname, "SOA", "querySoa");
  return {
    nsname: one.mname,
    hostmaster: one.rname,
    serial: one.serial,
    refresh: one.refresh,
    retry: one.retry,
    expire: one.expire,
    minttl: one.minimum,
  };
}

/// Node puts the tag in the key rather than beside the value, so an
/// issue record arrives as `{critical: 0, issue: "..."}`.
export async function resolveCaa(hostname) {
  const found = await asked(hostname, "CAA", "queryCaa");
  return found.map((one) => ({ critical: one.critical ? 128 : 0, [one.tag]: one.value }));
}

export async function resolveNaptr() {
  throw new TypeError(
    "node:dns here asks for A, AAAA, CAA, CNAME, MX, NS, PTR, SOA, SRV and TXT records, and NAPTR is not one of them",
  );
}

export async function resolveAny() {
  throw new TypeError(
    "an ANY query is refused by most resolvers and answered by fewer, so node:dns here asks for the record type you want instead",
  );
}

export async function resolve(hostname, rrtype = "A") {
  const kind = String(rrtype).toUpperCase();
  const by = {
    A: resolve4,
    AAAA: resolve6,
    ANY: resolveAny,
    CAA: resolveCaa,
    CNAME: resolveCname,
    MX: resolveMx,
    NAPTR: resolveNaptr,
    NS: resolveNs,
    PTR: resolvePtr,
    SOA: resolveSoa,
    SRV: resolveSrv,
    TXT: resolveTxt,
  }[kind];
  if (by === undefined) {
    throw new TypeError(`${rrtype} is not a record type node:dns knows`);
  }
  return await by(hostname);
}

/// The name an address is known by, which is a PTR query on the address
/// written backwards under the zone the internet keeps for that.
export async function reverse(address) {
  const said = String(address);
  const family = isIP(said);
  if (family === 0) {
    throw failed("EINVAL", "getHostByAddr", said, "that is not an address");
  }
  return await resolvePtr(family === 4 ? backwards4(said) : backwards6(said));
}

function backwards4(address) {
  return `${address.split(".").reverse().join(".")}.in-addr.arpa`;
}

/// A v6 address one nibble at a time, backwards, which means it has to
/// be written out in full first.
function backwards6(address) {
  const halves = address.split("::");
  const groups = halves.flatMap((half) => (half === "" ? [] : half.split(":")));
  const full = halves.length === 2
    ? [
      ...halves[0].split(":").filter((group) => group !== ""),
      ...Array(8 - groups.length).fill("0"),
      ...halves[1].split(":").filter((group) => group !== ""),
    ]
    : groups;
  const nibbles = full.map((group) => group.padStart(4, "0")).join("");
  return `${nibbles.split("").reverse().join(".")}.ip6.arpa`;
}

/// The one option this cannot honour, said where it was asked for. A
/// record's time to live is not kept, so a call that wants one would
/// otherwise get addresses with no ttl on them and not notice.
function ttlless(options, name) {
  if (options !== null && typeof options === "object" && options.ttl === true) {
    throw new TypeError(
      `node:dns ${name} here does not keep how long a record lives, so ttl true has nothing to answer with`,
    );
  }
}

/// A resolver of one's own, which is the same queries against the
/// servers this was told about rather than the ones the module was.
export class Resolver {
  #servers = [];

  constructor() {
    this.cancel = () => {};
    this.setLocalAddress = () => {};
  }

  getServers() {
    return this.#servers.map((server) => (server.port === 53 ? server.ipAddr : `${server.ipAddr}:${server.port}`));
  }

  setServers(list) {
    if (!Array.isArray(list)) {
      throw new TypeError("setServers takes an array of resolvers");
    }
    this.#servers = list.map(serverOf);
  }

  async #query(hostname, kind, syscall, shape) {
    const found = await asked(hostname, kind, syscall, this.#servers[0]);
    return shape === undefined ? found : shape(found);
  }

  async resolve(hostname, rrtype = "A") {
    const kind = String(rrtype).toUpperCase();
    const by = {
      A: "resolve4",
      AAAA: "resolve6",
      ANY: "resolveAny",
      CAA: "resolveCaa",
      CNAME: "resolveCname",
      MX: "resolveMx",
      NAPTR: "resolveNaptr",
      NS: "resolveNs",
      PTR: "resolvePtr",
      SOA: "resolveSoa",
      SRV: "resolveSrv",
      TXT: "resolveTxt",
    }[kind];
    if (by === undefined) {
      throw new TypeError(`${rrtype} is not a record type node:dns knows`);
    }
    return await this[by](hostname);
  }

  async resolve4(hostname) {
    return await this.#query(hostname, "A", "queryA");
  }

  async resolve6(hostname) {
    return await this.#query(hostname, "AAAA", "queryAaaa");
  }

  async resolveCname(hostname) {
    return await this.#query(hostname, "CNAME", "queryCname");
  }

  async resolveNs(hostname) {
    return await this.#query(hostname, "NS", "queryNs");
  }

  async resolvePtr(hostname) {
    return await this.#query(hostname, "PTR", "queryPtr");
  }

  async resolveTxt(hostname) {
    return await this.#query(hostname, "TXT", "queryTxt");
  }

  async resolveMx(hostname) {
    return await this.#query(hostname, "MX", "queryMx", (found) =>
      found.map((one) => ({ priority: one.preference, exchange: one.exchange })));
  }

  async resolveSrv(hostname) {
    return await this.#query(hostname, "SRV", "querySrv", (found) =>
      found.map((one) => ({
        priority: one.priority,
        weight: one.weight,
        port: one.port,
        name: one.target,
      })));
  }

  async resolveSoa(hostname) {
    return await this.#query(hostname, "SOA", "querySoa", ([one]) => ({
      nsname: one.mname,
      hostmaster: one.rname,
      serial: one.serial,
      refresh: one.refresh,
      retry: one.retry,
      expire: one.expire,
      minttl: one.minimum,
    }));
  }

  async resolveCaa(hostname) {
    return await this.#query(hostname, "CAA", "queryCaa", (found) =>
      found.map((one) => ({ critical: one.critical ? 128 : 0, [one.tag]: one.value })));
  }

  async resolveAny() {
    return await resolveAny();
  }

  async resolveNaptr() {
    return await resolveNaptr();
  }

  async reverse(address) {
    const said = String(address);
    const family = isIP(said);
    if (family === 0) {
      throw failed("EINVAL", "getHostByAddr", said, "that is not an address");
    }
    return await this.resolvePtr(family === 4 ? backwards4(said) : backwards6(said));
  }
}

/// The flags node's lookup takes and the codes its failures carry. They
/// are constants rather than behaviour, so they are the same here.
export const ADDRCONFIG = 1024;
export const V4MAPPED = 8;
export const ALL = 16;

export const NODATA = "ENODATA";
export const FORMERR = "EFORMERR";
export const SERVFAIL = "ESERVFAIL";
export const NOTFOUND = "ENOTFOUND";
export const NOTIMP = "ENOTIMP";
export const REFUSED = "EREFUSED";
export const BADQUERY = "EBADQUERY";
export const BADNAME = "EBADNAME";
export const BADFAMILY = "EBADFAMILY";
export const BADRESP = "EBADRESP";
export const CONNREFUSED = "ECONNREFUSED";
export const TIMEOUT = "ETIMEOUT";
export const CANCELLED = "ECANCELLED";

export default {
  ADDRCONFIG,
  ALL,
  BADFAMILY,
  BADNAME,
  BADQUERY,
  BADRESP,
  CANCELLED,
  CONNREFUSED,
  FORMERR,
  NODATA,
  NOTFOUND,
  NOTIMP,
  REFUSED,
  Resolver,
  SERVFAIL,
  TIMEOUT,
  V4MAPPED,
  getServers,
  lookup,
  lookupService,
  resolve,
  resolve4,
  resolve6,
  resolveAny,
  resolveCaa,
  resolveCname,
  resolveMx,
  resolveNaptr,
  resolveNs,
  resolvePtr,
  resolveSoa,
  resolveSrv,
  resolveTxt,
  reverse,
  setServers,
};
