// node:http2, in the shape a client library asks about it rather than
// uses it.
//
// The reason this is here is the one node:child_process is here for.
// An http client that supports h2 imports this module at the top,
// looks at what it got, and takes the http1 path when the server did
// not negotiate anything better. A package written that way runs fine
// against a module that exists and refuses at the call, and not at all
// against an import that is refused by name, which is the difference
// between one function in the examples corpus loading and not.
//
// So there is no h2 here. `connect` and both `createServer` say what is
// true, the constants are real because a package reads them at module
// scope where a missing object is a crash before any branch is taken,
// and the classes exist so that an `instanceof` answers false rather
// than throwing. A real h2 client is a separate question from this
// module existing, see #689.

function noHttp2(what) {
  return function () {
    throw new TypeError(
      `this runtime speaks http/1.1 and http/2 over fetch only, so node:http2 ${what} cannot work here`,
    );
  };
}

/// The names a package reads off this module, which it does at the top
/// of its own file rather than inside the branch that would use them.
/// Node's list is longer. What is here is the headers, the methods, the
/// statuses and the error codes, which is what a client touches.
export const constants = {
  NGHTTP2_NO_ERROR: 0x00,
  NGHTTP2_PROTOCOL_ERROR: 0x01,
  NGHTTP2_INTERNAL_ERROR: 0x02,
  NGHTTP2_FLOW_CONTROL_ERROR: 0x03,
  NGHTTP2_SETTINGS_TIMEOUT: 0x04,
  NGHTTP2_STREAM_CLOSED: 0x05,
  NGHTTP2_FRAME_SIZE_ERROR: 0x06,
  NGHTTP2_REFUSED_STREAM: 0x07,
  NGHTTP2_CANCEL: 0x08,
  NGHTTP2_COMPRESSION_ERROR: 0x09,
  NGHTTP2_CONNECT_ERROR: 0x0a,
  NGHTTP2_ENHANCE_YOUR_CALM: 0x0b,
  NGHTTP2_INADEQUATE_SECURITY: 0x0c,
  NGHTTP2_HTTP_1_1_REQUIRED: 0x0d,

  HTTP2_HEADER_AUTHORITY: ":authority",
  HTTP2_HEADER_METHOD: ":method",
  HTTP2_HEADER_PATH: ":path",
  HTTP2_HEADER_PROTOCOL: ":protocol",
  HTTP2_HEADER_SCHEME: ":scheme",
  HTTP2_HEADER_STATUS: ":status",
  HTTP2_HEADER_ACCEPT: "accept",
  HTTP2_HEADER_ACCEPT_ENCODING: "accept-encoding",
  HTTP2_HEADER_AUTHORIZATION: "authorization",
  HTTP2_HEADER_CONNECTION: "connection",
  HTTP2_HEADER_CONTENT_ENCODING: "content-encoding",
  HTTP2_HEADER_CONTENT_LENGTH: "content-length",
  HTTP2_HEADER_CONTENT_TYPE: "content-type",
  HTTP2_HEADER_COOKIE: "cookie",
  HTTP2_HEADER_DATE: "date",
  HTTP2_HEADER_HOST: "host",
  HTTP2_HEADER_LOCATION: "location",
  HTTP2_HEADER_SET_COOKIE: "set-cookie",
  HTTP2_HEADER_TE: "te",
  HTTP2_HEADER_USER_AGENT: "user-agent",

  HTTP2_METHOD_DELETE: "DELETE",
  HTTP2_METHOD_GET: "GET",
  HTTP2_METHOD_HEAD: "HEAD",
  HTTP2_METHOD_OPTIONS: "OPTIONS",
  HTTP2_METHOD_PATCH: "PATCH",
  HTTP2_METHOD_POST: "POST",
  HTTP2_METHOD_PUT: "PUT",

  HTTP_STATUS_OK: 200,
  HTTP_STATUS_NO_CONTENT: 204,
  HTTP_STATUS_NOT_MODIFIED: 304,
  HTTP_STATUS_BAD_REQUEST: 400,
  HTTP_STATUS_UNAUTHORIZED: 401,
  HTTP_STATUS_FORBIDDEN: 403,
  HTTP_STATUS_NOT_FOUND: 404,
  HTTP_STATUS_TOO_MANY_REQUESTS: 429,
  HTTP_STATUS_INTERNAL_SERVER_ERROR: 500,
  HTTP_STATUS_BAD_GATEWAY: 502,
  HTTP_STATUS_SERVICE_UNAVAILABLE: 503,
  HTTP_STATUS_GATEWAY_TIMEOUT: 504,

  DEFAULT_SETTINGS_HEADER_TABLE_SIZE: 4096,
  DEFAULT_SETTINGS_ENABLE_PUSH: 1,
  DEFAULT_SETTINGS_MAX_CONCURRENT_STREAMS: 4294967295,
  DEFAULT_SETTINGS_INITIAL_WINDOW_SIZE: 65535,
  DEFAULT_SETTINGS_MAX_FRAME_SIZE: 16384,
  DEFAULT_SETTINGS_MAX_HEADER_LIST_SIZE: 65535,
};

/// The symbol node marks headers with that should not go into an hpack
/// table anything else can read. Nothing here compresses headers, and
/// the symbol is still the one a package puts on an object it builds.
export const sensitiveHeaders = Symbol.for("nodejs.http2.sensitiveHeaders");

/// What node would say the connection starts with, which is a package's
/// baseline before it applies its own.
export function getDefaultSettings() {
  return {
    headerTableSize: constants.DEFAULT_SETTINGS_HEADER_TABLE_SIZE,
    enablePush: true,
    initialWindowSize: constants.DEFAULT_SETTINGS_INITIAL_WINDOW_SIZE,
    maxFrameSize: constants.DEFAULT_SETTINGS_MAX_FRAME_SIZE,
    maxConcurrentStreams: constants.DEFAULT_SETTINGS_MAX_CONCURRENT_STREAMS,
    maxHeaderListSize: constants.DEFAULT_SETTINGS_MAX_HEADER_LIST_SIZE,
    maxHeaderSize: constants.DEFAULT_SETTINGS_MAX_HEADER_LIST_SIZE,
  };
}

export const connect = noHttp2("connect");
export const createServer = noHttp2("createServer");
export const createSecureServer = noHttp2("createSecureServer");
export const getPackedSettings = noHttp2("getPackedSettings");
export const getUnpackedSettings = noHttp2("getUnpackedSettings");
export const performServerHandshake = noHttp2("performServerHandshake");

// The classes a package reaches for to ask whether something it was
// handed is one of them. Making one is opening a session, so that is
// where the refusal is, and an `instanceof` against an empty class
// answers false rather than throwing, which is the answer node would
// give for anything that is not an h2 stream.
class NoSession {
  constructor() {
    throw new TypeError("this runtime has no http/2 sessions, so one cannot be made here");
  }
}

export class Http2Session extends NoSession {}
export class ClientHttp2Session extends NoSession {}
export class ServerHttp2Session extends NoSession {}
export class Http2Stream extends NoSession {}
export class ClientHttp2Stream extends NoSession {}
export class ServerHttp2Stream extends NoSession {}
export class Http2ServerRequest extends NoSession {}
export class Http2ServerResponse extends NoSession {}

export default {
  constants,
  sensitiveHeaders,
  connect,
  createServer,
  createSecureServer,
  getDefaultSettings,
  getPackedSettings,
  getUnpackedSettings,
  performServerHandshake,
  Http2Session,
  ClientHttp2Session,
  ServerHttp2Session,
  Http2Stream,
  ClientHttp2Stream,
  ServerHttp2Stream,
  Http2ServerRequest,
  Http2ServerResponse,
};
