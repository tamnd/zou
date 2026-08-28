// node:perf_hooks, which is the web's performance timeline under the
// name node put it behind.
//
// Almost nothing here is new. `performance`, the four entry classes and
// the observer are the global ones, the same objects a function reaches
// without importing anything, because in node they are the same objects
// too: `require('perf_hooks').performance === globalThis.performance`.
// What this module is for is the import line. A package built for node
// writes `import { performance } from 'node:perf_hooks'` at the top of
// itself, and without a module under that name the import is a link
// error before the package has done anything.
//
// The three that are not here are the three that are about a node
// process rather than about timing: the event loop delay histogram, the
// histogram it hands back, and `timerify`, which records an entry of a
// type nothing here records. They throw with their own names in the
// sentence, which is this runtime's rule for what it does not have.
//
// `nodeTiming` is absent for the same reason. It is when node started,
// when v8 did and when the bootstrap finished, and a function did none
// of those: it was called on a server that was already running, and a
// row of zeroes dressed as phase times would be worse than the property
// not being there.

export const performance = globalThis.performance;
export const PerformanceEntry = globalThis.PerformanceEntry;
export const PerformanceMark = globalThis.PerformanceMark;
export const PerformanceMeasure = globalThis.PerformanceMeasure;
export const PerformanceObserver = globalThis.PerformanceObserver;
export const PerformanceObserverEntryList = globalThis.PerformanceObserverEntryList;

/// A resource that was fetched, which is an entry type nothing here
/// records: there is no document doing subresource loads in a function.
/// The name is exported because node exports it and an import of it has
/// to link, and constructing one is the illegal constructor every entry
/// class has.
export class PerformanceResourceTiming extends PerformanceEntry {}

function notAProcess(name) {
  return function () {
    throw new TypeError(`a function is not a node process with an event loop to watch, so node:perf_hooks ${name} cannot work here`);
  };
}

export const monitorEventLoopDelay = notAProcess("monitorEventLoopDelay");
export const createHistogram = notAProcess("createHistogram");
export const timerify = notAProcess("timerify");
export const eventLoopUtilization = notAProcess("eventLoopUtilization");

/// node's own numbers, which are the flags a garbage collection entry
/// carries. Nothing here records one, and the numbers are here because
/// a package that reads a constant off this object at the top of itself
/// should not get an undefined.
export const constants = Object.freeze({
  NODE_PERFORMANCE_GC_MAJOR: 4,
  NODE_PERFORMANCE_GC_MINOR: 1,
  NODE_PERFORMANCE_GC_INCREMENTAL: 8,
  NODE_PERFORMANCE_GC_WEAKCB: 16,
  NODE_PERFORMANCE_GC_FLAGS_NO: 0,
  NODE_PERFORMANCE_GC_FLAGS_CONSTRUCT_RETAINED: 2,
  NODE_PERFORMANCE_GC_FLAGS_FORCED: 4,
  NODE_PERFORMANCE_GC_FLAGS_SYNCHRONOUS_PHANTOM_PROCESSING: 8,
  NODE_PERFORMANCE_GC_FLAGS_ALL_AVAILABLE_GARBAGE: 16,
  NODE_PERFORMANCE_GC_FLAGS_ALL_EXTERNAL_MEMORY: 32,
  NODE_PERFORMANCE_GC_FLAGS_SCHEDULE_IDLE: 64,
});

export default {
  performance,
  PerformanceEntry,
  PerformanceMark,
  PerformanceMeasure,
  PerformanceObserver,
  PerformanceObserverEntryList,
  PerformanceResourceTiming,
  monitorEventLoopDelay,
  createHistogram,
  timerify,
  eventLoopUtilization,
  constants,
};
