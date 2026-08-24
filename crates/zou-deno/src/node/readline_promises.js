// node:readline/promises, which is the same interface with `question`
// answering with a promise.
//
// It is the same class, because the callback module's `question`
// already answers with a promise when nobody handed it a callback, and
// two implementations of one line reader would be one too many.

import { Interface, createInterface } from "node:readline";

export { Interface, createInterface };

export default { Interface, createInterface };
