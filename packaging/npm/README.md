# zou-cli

The zou command line, for people who already have node.

```bash
npm install -g zou-cli
zou dev ./data
```

That is a Supabase compatible backend on http://127.0.0.1:54321, with Postgres on 5432, out of a directory.
`zou serve ./data` is the same thing bound to every interface, which is what a container or a box wants.

## What it installs

The zou binary and the patched Postgres it starts, which is what a zou release is.
npm cannot carry that as package content: four platforms of a fifty megabyte tree in one tarball is not a package anybody should download to get one of them.
So installing this package downloads the bundle for the platform it is being installed on, checks it against the sha256 published beside it, and unpacks it into the package.

It is the same tarball [install.sh](https://github.com/tamnd/zou#install) fetches and the same one the docker image is built out of, so this is a way of getting zou rather than a different zou.
The version follows the package: `zou-cli@0.2.0` downloads the binaries from the `v0.2.0` release rather than from whatever is newest.

- `ZOU_VERSION` takes a different tag.
- `ZOU_SKIP_DOWNLOAD=1` installs the package without the bundle, for an image that will mount one in later. Nothing will run until it does.
- `npm rebuild zou-cli` downloads it again, which is the fix if the bundle is missing.

## With the node binding

`zou` on npm is the [embedded binding](https://github.com/tamnd/zou/blob/main/docs/embedded.md): a Supabase project inside the node process, with supabase-js talking to it over no socket at all.
It needs the same patched Postgres, and it finds it in this package when both are installed.

```bash
npm install --save-dev zou zou-cli
```

```js
import { createFixture } from "zou";

const project = await createFixture();
const supabase = project.client();
```

No `ZOU_PG_BIN` and no path to point anywhere.
The binding looks at `pgBin`, then at `ZOU_PG_BIN`, then in `zou-cli` next door.

## Not this package

- Windows. The bundle is unix only for now, because there is no patched Postgres there yet.
- Anything that is not x64 or arm64.

Apache-2.0. The source is at [github.com/tamnd/zou](https://github.com/tamnd/zou).
