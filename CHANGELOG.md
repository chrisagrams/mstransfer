# Changelog

## [0.3.0](https://github.com/chrisagrams/mstransfer/compare/v0.2.3...v0.3.0) (2026-10-30)


### Features

* **client:** store multi-member batch .mszx archives as msz or mzML ([#58](https://github.com/chrisagrams/mstransfer/issues/58)) ([20938cd](https://github.com/chrisagrams/mstransfer/commit/20938cd6aba11efc6ae530a3b0809e32181f3774))


### Bug Fixes

* **client:** take download size from the GET response, not a HEAD probe ([#73](https://github.com/chrisagrams/mstransfer/issues/73)) ([3bc4358](https://github.com/chrisagrams/mstransfer/commit/3bc4358a531ecb4b9347f43efa40cc3f06a4196f))
* **cli:** exit non-zero when uploads or downloads fail ([#70](https://github.com/chrisagrams/mstransfer/issues/70)) ([1581382](https://github.com/chrisagrams/mstransfer/commit/1581382561b4630fa00bbfccada305c2827cf549))
* **server:** refuse uploads that would overwrite another file ([#72](https://github.com/chrisagrams/mstransfer/issues/72)) ([18fbf40](https://github.com/chrisagrams/mstransfer/commit/18fbf400f5f404bfbacd011e555bd7d6dac7bb1b))
* **server:** store uploaded .mszx archives as-is ([#69](https://github.com/chrisagrams/mstransfer/issues/69)) ([924e5c5](https://github.com/chrisagrams/mstransfer/commit/924e5c558126dd5bac4e4785d03569331bfe1df3))


### Code Refactoring

* **client:** extract store-as logic into StorePlan ([#59](https://github.com/chrisagrams/mstransfer/issues/59)) ([31f974d](https://github.com/chrisagrams/mstransfer/commit/31f974d7545ec952e22d6b41804a7ba05f98cbb6))


### Dependencies

* bump mscompress from 1.0.13 to 1.2.0 in the mscompress group ([#44](https://github.com/chrisagrams/mstransfer/issues/44)) ([37853cb](https://github.com/chrisagrams/mstransfer/commit/37853cb61e872d46f323c38abeb9b4bbb8fbf213))
* bump rich from 14.3.3 to 15.0.0 ([#48](https://github.com/chrisagrams/mstransfer/issues/48)) ([643e842](https://github.com/chrisagrams/mstransfer/commit/643e8422660921db2f355a3483dd531d81d348bb))
* bump the production-dependencies group with 3 updates ([#46](https://github.com/chrisagrams/mstransfer/issues/46)) ([f89547c](https://github.com/chrisagrams/mstransfer/commit/f89547cfa579ec8a6cb74c522140ce23bf3e755c))
