/** How a project is opened. Everything is optional. */
export interface CreateZouOptions {
  /** A directory of objects, kept. */
  dir?: string;
  /** A bucket, a sqlite:// file, or a .zou file. */
  url?: string;
  /** Either of the two above, whichever it is. */
  target?: string;
  /** A store of this handle's own, gone on close. The default. */
  ephemeral?: boolean;
  /** The database inside the store. Defaults to local. */
  tenant?: string;
  /** The patched postgres install. Defaults to ZOU_PG_BIN. */
  pgBin?: string;
  /** Where the running copy lives. Removed on close. */
  runtime?: string;
  /** What tokens are signed with. Random when unset. */
  jwtSecret?: string;
  /** Exposed schemas, the first one the default. */
  schemas?: string[];
  /** shared_buffers for the child postmaster. */
  sharedBuffers?: string;
}

/** One open project: a store, the postgres over it, and the front door. */
export declare class Zou {
  /** The key a browser gets. */
  readonly anonKey: string;
  /** The key that skips row level security. Keep it on the server. */
  readonly serviceRoleKey: string;
  /** For psql, or a node postgres driver, on the database directly. */
  readonly dsn: string;
  /** The store everything durable is on. */
  readonly target: string;
  /** The database inside it. */
  readonly tenant: string;
  /** The port once listen has been called, and a name that resolves nowhere before that. */
  readonly url: string;

  /** A supabase-js client wired to answer in this process. */
  client(key?: string, options?: Record<string, unknown>): any;
  /** The same thing one level down, as a fetch. */
  fetch(input: Request | string | URL, init?: RequestInit): Promise<Response>;
  /** Put the same front door on a port too, and say which one. */
  listen(port?: number): Promise<number>;
  /** A copy on write branch, open and ready. */
  branch(name: string): Promise<Zou>;
  /** Whether a branch of this database would serve yet. */
  branchable(): Promise<boolean>;
  /** Push everything committed so far into the store. */
  checkpoint(): Promise<void>;
  /** Stop postgres and remove the running copy. Twice is fine. */
  close(): Promise<void>;
}

/** Open a project. */
export declare function createZou(options?: CreateZouOptions): Promise<Zou>;
