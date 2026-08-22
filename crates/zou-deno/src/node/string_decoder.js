// node:string_decoder, which is one class: bytes in, text out, and a
// character split across two chunks held until the rest of it arrives.
//
// The holding is what the class is for, and `TextDecoder` already does
// it with `stream: true`, so this is that with node's method names on
// it.

import { Buffer } from "node:buffer";

class StringDecoder {
  constructor(encoding = "utf8") {
    this.encoding = String(encoding).toLowerCase();
    this.streaming = this.encoding === "utf8" || this.encoding === "utf-8";
    this.decoder = this.streaming ? new TextDecoder("utf-8") : null;
  }

  write(bytes) {
    if (typeof bytes === "string") {
      return bytes;
    }
    if (this.decoder !== null) {
      return this.decoder.decode(bytes, { stream: true });
    }
    // Every other encoding this runtime has is one byte or two per
    // character with no continuation to hold, so a chunk decodes on
    // its own.
    return Buffer.from(bytes).toString(this.encoding);
  }

  end(bytes) {
    const last = bytes === undefined ? "" : this.write(bytes);
    if (this.decoder === null) {
      return last;
    }
    // Whatever was being held, now that there is no more coming.
    return last + this.decoder.decode();
  }

  text(bytes) {
    return this.write(bytes);
  }
}

export { StringDecoder };
export default { StringDecoder };
