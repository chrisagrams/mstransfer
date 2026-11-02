# Getting Started

mstransfer has two halves: a **server** that receives and serves files, and a
**client** that uploads to or downloads from it. Both ship in the same
package, so you install mstransfer on every machine involved.

- [Installation](installation.md): install the CLI or library
- [Quick Start](quickstart.md): send your first file

## File formats

| Format | Extension | Description |
|--------|-----------|-------------|
| mzML | `.mzML` | XML-based, uncompressed mass spectrometry data |
| MSZ | `.msz` | mscompress's compressed, random-access container for mzML |
| MSZX | `.mszx` | tar archive bundling one or more MSZ files with metadata |

See the [mscompress concepts](https://chrisagrams.github.io/mscompress/getting-started/concepts/)
for details on MSZ and MSZX.
