// The suites, asked of a zou the node package started.
//
// Every other conformance job here runs a zou this harness linked in
// and pointed at a postgres somebody else brought up in a container.
// This one runs the zou a node project gets: `npm install zou`, a
// postmaster this process started over a directory, and the whole api
// answering inside it. Nothing is in a container, and the package that
// ships is the thing under test rather than a build of the same code.
//
// One suite per run, because a suite says which schemas the server has
// to expose and the first of them is the one a request that names no
// schema gets, so two suites that disagree cannot share a server.
//
//   node conformance/embedded.mjs storage --suites /tmp/zou-conformance/suites
//
// Anything after the suite name is handed to the harness as it stands.

import { spawn } from 'node:child_process'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'

import { createZou } from '../crates/zou-node/index.js'

// The same secret and the same pair every other target in a
// conformance run is configured with. A key is only the same key when
// both ends sign with the same secret, and a signature only verifies
// against the pair it was made for, so these are fixtures rather than
// credentials.
const SECRET = 'super-secret-jwt-token-with-at-least-32-characters-long'
const S3_KEY = '625729a08b95bf1b7ff351a663f3a23c'
const S3_SECRET = '850181e4652dd023b7a98c58ae0d2d34bd487ee0cc3254aed6eda37307425907'
// Where a local project says it is, which is half of what a signature
// is checked against.
const S3_REGION = 'local'

const [suite, ...rest] = process.argv.slice(2)
if (!suite) {
  console.error('usage: node conformance/embedded.mjs <suite> [harness options]')
  process.exit(2)
}

const at = rest.indexOf('--suites')
const suites = at === -1 ? process.env.ZOU_CONFORMANCE_SUITES : rest[at + 1]
if (!suites) {
  console.error('say where the suites are, with --suites or ZOU_CONFORMANCE_SUITES')
  process.exit(2)
}

// The schemas are the suite's, not a choice made here: a suite derived
// from upstream's fixtures keeps its tables where upstream keeps them.
const cases = JSON.parse(readFileSync(join(suites, suite, 'cases.json'), 'utf8'))
const schemas = cases.schemas ?? ['conformance', 'public']
// And the same for the role an unauthenticated request runs as, since
// upstream's own fixtures grant to a role of their own.
const anonRole = cases.anon_role ?? 'anon'

const zou = await createZou({
  dir: process.env.ZOU_EMBEDDED_STORE,
  jwtSecret: SECRET,
  schemas,
  anonRole,
  s3AccessKey: S3_KEY,
  s3SecretKey: S3_SECRET,
  s3Region: S3_REGION,
})
const port = await zou.listen(Number(process.env.ZOU_EMBEDDED_PORT ?? 0))
console.log(
  `zou ${zou.url} from the node package, schemas ${schemas.join(',')}, anon ${anonRole}, pg ${port}`,
)

const harness = spawn(
  process.env.ZOU_CONFORMANCE_BIN ?? 'target/debug/zou-conformance',
  [
    'check',
    '--suite',
    suite,
    '--url',
    zou.url,
    '--dsn',
    zou.dsn,
    '--jwt-secret',
    SECRET,
    '--s3-key',
    S3_KEY,
    '--s3-secret',
    S3_SECRET,
    '--name',
    'zou (node package)',
    ...rest,
  ],
  { stdio: 'inherit' },
)

const code = await new Promise((resolve) => harness.on('close', resolve))
await zou.close()
process.exit(code ?? 1)
