# Changelog

## [0.3.0](https://github.com/chrisagrams/mstransfer/compare/v0.2.3...v0.3.0) (2026-10-28)


### Features

* add dependabot for mscompress ([5136714](https://github.com/chrisagrams/mstransfer/commit/5136714b5e60bcc5296284245e0a40df74a30d1c))
* add download client and server download routes ([f6d4aa2](https://github.com/chrisagrams/mstransfer/commit/f6d4aa2e1f24cb79348e9ec411aa7d796b6c26d4))
* add sender functions to client module ([7392348](https://github.com/chrisagrams/mstransfer/commit/7392348734a18eba40a92d74d297b93dae0f9eb0))
* implement a ThrottledCallback to reduce logging overhead ([e69302e](https://github.com/chrisagrams/mstransfer/commit/e69302e22c7117ce430e283ce9f8f5233a3c58b8))
* implement API key authentication provider ([4e6cb6c](https://github.com/chrisagrams/mstransfer/commit/4e6cb6caaafe94ddd0a29614ee29b3675871ffd4))
* implement API key authentication provider ([4742304](https://github.com/chrisagrams/mstransfer/commit/4742304235b3e891b504e205064eb701560faee8))
* implement file download / providers ([d6e4346](https://github.com/chrisagrams/mstransfer/commit/d6e4346fb923fd0d249fb2cca464d0f59e07d682))
* LRU implementation in TransferRegistry ([586aa74](https://github.com/chrisagrams/mstransfer/commit/586aa7444c2ab0dfd752a6b8a3032e0dfe6dcce1))
* refactor sender and receiver to be async first ([caa4041](https://github.com/chrisagrams/mstransfer/commit/caa40414327b4aee1712000f4c92ab73fa066215))
* refactor sender and receiver to be async first ([bdb255c](https://github.com/chrisagrams/mstransfer/commit/bdb255c30fcb5ac9e6cda53c80aa2265fd52f681))
* reusable httpx client ([548d6a1](https://github.com/chrisagrams/mstransfer/commit/548d6a1e2b42f6966fdc3859419d31f8b39e4bad))
* support include_router for embedding in parent apps ([3ef1973](https://github.com/chrisagrams/mstransfer/commit/3ef1973b1ec42dc97cd4193fa252423053ccb3c5))
* support include_router for embedding in parent apps ([758cc1c](https://github.com/chrisagrams/mstransfer/commit/758cc1cc2e28339ae4cd1984c02098c19c522a47))
* support mszx as a source format in download_file / download_batch ([5c05924](https://github.com/chrisagrams/mstransfer/commit/5c0592428e63959c08d9165e113aac4a34cecc16))
* support mszx as a source format in downloader ([9005609](https://github.com/chrisagrams/mstransfer/commit/90056093fbb2cbbec886803e52fa5b8203b1d402))


### Bug Fixes

* add py.typed marker for PEP 561 compliance ([e54041b](https://github.com/chrisagrams/mstransfer/commit/e54041bbe63988e2907af402dd28cdb2fdf3e6a6))
* add py.typed marker for PEP 561 compliance ([cab2470](https://github.com/chrisagrams/mstransfer/commit/cab2470faa4acc7fa5bfaa0ed809319356723669)), closes [#26](https://github.com/chrisagrams/mstransfer/issues/26)
* print help instead of erroring when no subcommand given ([0344f87](https://github.com/chrisagrams/mstransfer/commit/0344f87645c4a08a1b33655ce90b3f321edd3d61))
* print help instead of erroring when no subcommand given ([79d6124](https://github.com/chrisagrams/mstransfer/commit/79d6124382289e2c31779dc6d9b41568e7e46737))
* version bump to 0.2.1 instead of 0.3.0 ([21ff1c7](https://github.com/chrisagrams/mstransfer/commit/21ff1c76c09e4cdb2f88e700dc6b42a391ddaf9d))


### Code Refactoring

* derive _detect_source_format from _FORMAT_EXT inverse ([0d966d7](https://github.com/chrisagrams/mstransfer/commit/0d966d7b526ee0ffd11ce878a1bb41d81c9b30e0))
* dispatch conversions via (source, target) → converter table ([5d0c463](https://github.com/chrisagrams/mstransfer/commit/5d0c463ad1a1e80cdeb37b3459061ef4419bd153))
* update parse_target to return base URL and enhance upload handling with progress display ([63b9a89](https://github.com/chrisagrams/mstransfer/commit/63b9a890c68d2dcff289fab2bf7d87287a1d879d))
