# Changelog

## [0.3.2](https://github.com/chrisagrams/mstransfer/compare/v0.3.1...v0.3.2) (2026-11-25)


### Bug Fixes

* let file providers set the download name separately from the stored path ([#133](https://github.com/chrisagrams/mstransfer/issues/133)) ([c8107da](https://github.com/chrisagrams/mstransfer/commit/c8107da461973aee5d60fb88823ba28b04fb7f4e))

## [0.3.1](https://github.com/chrisagrams/mstransfer/compare/v0.3.0...v0.3.1) (2026-11-25)


### Bug Fixes

* allow protecting uploads and downloads separately ([#129](https://github.com/chrisagrams/mstransfer/issues/129)) ([53efab2](https://github.com/chrisagrams/mstransfer/commit/53efab26f4dc57f4344b61758bdbae5cc13461a7))


### Dependencies

* bump filelock from 4.2.12 to 4.3.0 in the production-dependencies group ([#126](https://github.com/chrisagrams/mstransfer/issues/126)) ([e334715](https://github.com/chrisagrams/mstransfer/commit/e3347157496f248cb6c4112a4b5a86ea7d053c3b))

## [0.3.0](https://github.com/chrisagrams/mstransfer/compare/v0.2.3...v0.3.0) (2026-11-23)


### Features

* **cli:** add --version/-V flag ([#93](https://github.com/chrisagrams/mstransfer/issues/93)) ([0fca3f7](https://github.com/chrisagrams/mstransfer/commit/0fca3f7736b7fd3fedabb2af16a8b0907b32ee77))
* **client:** store multi-member batch .mszx archives as msz or mzML ([#58](https://github.com/chrisagrams/mstransfer/issues/58)) ([20938cd](https://github.com/chrisagrams/mstransfer/commit/20938cd6aba11efc6ae530a3b0809e32181f3774))
* resume interrupted downloads ([#95](https://github.com/chrisagrams/mstransfer/issues/95)) ([f79d236](https://github.com/chrisagrams/mstransfer/commit/f79d23659c1d52033e61b6cd18e62ac0cbb5059e))


### Bug Fixes

* **cli:** add --api-key to download, read it from MS_API_KEY ([#84](https://github.com/chrisagrams/mstransfer/issues/84)) ([38e5045](https://github.com/chrisagrams/mstransfer/commit/38e504512b7af63dd1b68cd926fcad69ba555b3b))
* **client:** isolate and clean download part files ([#91](https://github.com/chrisagrams/mstransfer/issues/91)) ([85180ed](https://github.com/chrisagrams/mstransfer/commit/85180ed01a5ae238a97b9d82dd8d99c669232608))
* **client:** remove stale partials after a --no-resume download ([#109](https://github.com/chrisagrams/mstransfer/issues/109)) ([9baf06c](https://github.com/chrisagrams/mstransfer/commit/9baf06cef0cbdec5f1bde54e8464ae0ab77e5825))
* **client:** report a missing batch file as a per-file error ([#87](https://github.com/chrisagrams/mstransfer/issues/87)) ([b8ddedb](https://github.com/chrisagrams/mstransfer/commit/b8ddedbda843a40a6ec3691bd9f570b77caa1e19))
* **client:** return a result per request from download_batch ([#89](https://github.com/chrisagrams/mstransfer/issues/89)) ([eae38f2](https://github.com/chrisagrams/mstransfer/commit/eae38f2b72b4657931070acab6325bcb12bf81a0))
* **client:** take download size from the GET response, not a HEAD probe ([#73](https://github.com/chrisagrams/mstransfer/issues/73)) ([3bc4358](https://github.com/chrisagrams/mstransfer/commit/3bc4358a531ecb4b9347f43efa40cc3f06a4196f))
* **client:** take httpx.Timeout so explicit timeouts override client= ([#88](https://github.com/chrisagrams/mstransfer/issues/88)) ([a5f9c21](https://github.com/chrisagrams/mstransfer/commit/a5f9c21319c85ed431e07333fd0307c3a15aeff8))
* **cli:** exit 130 without a traceback on Ctrl-C ([#110](https://github.com/chrisagrams/mstransfer/issues/110)) ([0bcf4b8](https://github.com/chrisagrams/mstransfer/commit/0bcf4b8a39cd695d9bf1f1ab799b46a2a4f217ab))
* **cli:** exit non-zero when uploads or downloads fail ([#70](https://github.com/chrisagrams/mstransfer/issues/70)) ([1581382](https://github.com/chrisagrams/mstransfer/commit/1581382561b4630fa00bbfccada305c2827cf549))
* **cli:** let serve restart while the port is in TIME-WAIT ([#108](https://github.com/chrisagrams/mstransfer/issues/108)) ([cd4a0df](https://github.com/chrisagrams/mstransfer/commit/cd4a0df9533c8e59ebeea127d567a98ceb454664))
* **cli:** report skipped downloads as skipped, not downloaded ([#107](https://github.com/chrisagrams/mstransfer/issues/107)) ([14de13d](https://github.com/chrisagrams/mstransfer/commit/14de13dea24737ec1d02b744057915fd6ae8b7d9))
* **cli:** show the server's error detail when a transfer fails ([#106](https://github.com/chrisagrams/mstransfer/issues/106)) ([4fcd639](https://github.com/chrisagrams/mstransfer/commit/4fcd6394586d236ae0352134816e891be0efd903))
* remove download lock files with filelock 4 ([#104](https://github.com/chrisagrams/mstransfer/issues/104)) ([73f6253](https://github.com/chrisagrams/mstransfer/commit/73f6253d14db556a345fc0045f277e00ef1328e0))
* **server:** refuse uploads that would overwrite another file ([#72](https://github.com/chrisagrams/mstransfer/issues/72)) ([18fbf40](https://github.com/chrisagrams/mstransfer/commit/18fbf400f5f404bfbacd011e555bd7d6dac7bb1b))
* **server:** return 401, not 500, for non-ASCII API keys ([#85](https://github.com/chrisagrams/mstransfer/issues/85)) ([162da43](https://github.com/chrisagrams/mstransfer/commit/162da43e95178a8c59d72656a2e28ffcde7bdfa1))
* **server:** store uploaded .mszx archives as-is ([#69](https://github.com/chrisagrams/mstransfer/issues/69)) ([924e5c5](https://github.com/chrisagrams/mstransfer/commit/924e5c558126dd5bac4e4785d03569331bfe1df3))
* **store:** give the conversion temp file a hidden, unique name ([#74](https://github.com/chrisagrams/mstransfer/issues/74)) ([15f66c3](https://github.com/chrisagrams/mstransfer/commit/15f66c3ed961bfee128e0cbc490abe1822fca76f))
* support Windows and test it in CI ([#96](https://github.com/chrisagrams/mstransfer/issues/96)) ([784ef25](https://github.com/chrisagrams/mstransfer/commit/784ef253ad375c799b1c0cfe3566d7293cdb7bc3))


### Code Refactoring

* **client:** extract store-as logic into StorePlan ([#59](https://github.com/chrisagrams/mstransfer/issues/59)) ([31f974d](https://github.com/chrisagrams/mstransfer/commit/31f974d7545ec952e22d6b41804a7ba05f98cbb6))


### Dependencies

* bump mscompress from 1.0.13 to 1.2.0 in the mscompress group ([#44](https://github.com/chrisagrams/mstransfer/issues/44)) ([37853cb](https://github.com/chrisagrams/mstransfer/commit/37853cb61e872d46f323c38abeb9b4bbb8fbf213))
* bump pydantic from 2.14.1 to 2.14.2 in the production-dependencies group ([#76](https://github.com/chrisagrams/mstransfer/issues/76)) ([b1777ea](https://github.com/chrisagrams/mstransfer/commit/b1777ea0acbc4cbedee85a62e937ce10b51fe74a))
* bump rich from 14.3.3 to 15.0.0 ([#48](https://github.com/chrisagrams/mstransfer/issues/48)) ([643e842](https://github.com/chrisagrams/mstransfer/commit/643e8422660921db2f355a3483dd531d81d348bb))
* bump the production-dependencies group across 1 directory with 5 updates ([#120](https://github.com/chrisagrams/mstransfer/issues/120)) ([8a9a582](https://github.com/chrisagrams/mstransfer/commit/8a9a582a4d6eb574bbd168fb548b1c347ecb20d8))
* bump the production-dependencies group with 3 updates ([#46](https://github.com/chrisagrams/mstransfer/issues/46)) ([f89547c](https://github.com/chrisagrams/mstransfer/commit/f89547cfa579ec8a6cb74c522140ce23bf3e755c))
