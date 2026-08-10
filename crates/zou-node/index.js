"use strict";

// The javascript half of the node binding.
//
// The addon moves bytes: a method, a path, headers, a body, and what
// came back. This turns that into a fetch, because a fetch is what
// supabase-js takes, and a supabase-js client built on it is talking to
// a database in this process without a socket under any of it.

const { createRequire } = require("node:module");

const native = require("./zou.node");

// supabase-js builds urls out of a base, so there has to be one even
// when nothing is listening. Nothing resolves this name and nothing
// needs to: the origin is stripped off again on the way in.
const ORIGIN = "http://zou.embedded";

/**
 * Open a project.
 *
 * `{ dir }` is a directory of objects, `{ url }` a bucket or a sqlite
 * file or a .zou file, and nothing at all is ephemeral: a store of this
 * handle's own that goes away when it closes.
 */
async function createZou(options = {}) {
  const target = options.dir ?? options.url ?? options.target ?? "";
  if (options.dir && options.url) {
    throw new Error("dir and url are two ways of saying the same thing, pick one");
  }
  const handle = await native.open({
    target: options.ephemeral ? "" : target,
    tenant: options.tenant,
    pgBin: options.pgBin ?? process.env.ZOU_PG_BIN,
    runtime: options.runtime,
    jwtSecret: options.jwtSecret,
    schemas: options.schemas,
    sharedBuffers: options.sharedBuffers,
  });
  return new Zou(handle);
}

/** One open project. */
class Zou {
  #handle;
  #port = null;

  constructor(handle) {
    this.#handle = handle;
  }

  /** The key a browser gets. */
  get anonKey() {
    return this.#handle.anonKey;
  }

  /** The key that skips row level security. Keep it on the server. */
  get serviceRoleKey() {
    return this.#handle.serviceRoleKey;
  }

  /** For psql, or for a node postgres driver, on the database directly. */
  get dsn() {
    return this.#handle.dsn;
  }

  /** The store everything durable is on. */
  get target() {
    return this.#handle.target;
  }

  /** The database inside it. */
  get tenant() {
    return this.#handle.tenant;
  }

  /**
   * Where this project is, for something that wants a url. It is the
   * port once `listen` has been called and a name that resolves nowhere
   * before that, since before that there is nothing to resolve.
   */
  get url() {
    return this.#port === null ? ORIGIN : `http://127.0.0.1:${this.#port}`;
  }

  /**
   * A supabase-js client, wired to answer in this process.
   *
   * The key is the anon one unless you ask for the other, so a test
   * that means to skip row level security has to say so in the same
   * place it would against a hosted project.
   */
  client(key = this.anonKey, options = {}) {
    const { createClient } = supabaseJs();
    return createClient(this.url, key, {
      ...options,
      auth: {
        persistSession: false,
        autoRefreshToken: false,
        ...(options.auth ?? {}),
      },
      global: {
        ...(options.global ?? {}),
        fetch: this.fetch,
      },
    });
  }

  /**
   * The same thing one level down, as a fetch.
   *
   * Bound, so it can be handed to anything that takes one without being
   * wrapped first.
   */
  fetch = async (input, init = {}) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const url = new URL(request.url, ORIGIN);
    const headers = [];
    for (const [name, value] of request.headers) {
      headers.push({ name, value });
    }
    const bytes = Buffer.from(await request.arrayBuffer());
    const answer = await this.#handle.request(
      request.method,
      url.pathname + url.search,
      headers,
      bytes.length > 0 ? bytes : undefined,
    );
    // A 204 or a 304 with a body in it is a TypeError rather than an
    // empty answer, which is a thing to know before it happens.
    const empty = answer.status === 204 || answer.status === 205 || answer.status === 304;
    return new Response(empty ? null : answer.body, {
      status: answer.status,
      headers: answer.headers.map(({ name, value }) => [name, value]),
    });
  };

  /**
   * Put the same front door on a port as well, and say which one. No
   * port, or 0, asks the kernel for one.
   */
  async listen(port = 0) {
    this.#port = await this.#handle.listen(port);
    return this.#port;
  }

  /**
   * A copy on write branch, open and ready, as a second project to
   * close like the first.
   */
  async branch(name) {
    return new Zou(await this.#handle.branch(name));
  }

  /** Whether a branch of this database would serve yet. */
  async branchable() {
    return this.#handle.branchable();
  }

  /** Push everything committed so far into the store. */
  async checkpoint() {
    await this.#handle.checkpoint();
  }

  /**
   * Stop postgres and remove the running copy. Calling it twice is
   * fine, which is what makes it safe in an `after` hook that may or
   * may not be the one that got there first.
   */
  async close() {
    await this.#handle.close();
  }
}

/**
 * supabase-js is a peer dependency rather than a dependency, so a
 * project that only wants `fetch` or `listen` does not install it and a
 * project that wants `client()` gets a sentence rather than a stack.
 */
function supabaseJs() {
  try {
    return createRequire(__filename)("@supabase/supabase-js");
  } catch {
    throw new Error(
      "client() needs @supabase/supabase-js, install it alongside zou, or use fetch() or listen() instead",
    );
  }
}

exports.createZou = createZou;
exports.Zou = Zou;
