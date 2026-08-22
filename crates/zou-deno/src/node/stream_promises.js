// node:stream/promises, the same two functions with the callback taken
// off and a promise in its place.

import { pipeline as piped, finished as done } from "node:stream";

export function pipeline(...streams) {
  return new Promise((resolve, reject) => {
    // Declared first, because the callback may be called before the
    // call it belongs to has returned.
    let last;
    last = piped(...streams, (why) => (why ? reject(why) : resolve(last)));
  });
}

export function finished(stream, options) {
  return new Promise((resolve, reject) => {
    done(stream, options ?? {}, (why) => (why ? reject(why) : resolve()));
  });
}

export default { pipeline, finished };
