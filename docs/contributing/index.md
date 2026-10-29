# Contributing

## Setup

```bash
git clone https://github.com/chrisagrams/mstransfer
cd mstransfer
uv sync --dev
uv run pre-commit install
```

The pre-commit hooks run ruff (lint and format check), pyright, mypy, and
pytest.

## Checks

```bash
uv run pytest                    # tests
uv run ruff check .              # lint
uv run ruff format .             # format
uv run mypy src/                 # type check (strict)
```

CI runs all of these on Python 3.10 through 3.14 for every PR into `dev`,
`stage`, and `main`.

## Docstrings

Docstrings use [Google style](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings)
(`Args:`, `Returns:`, `Raises:`) with Markdown inline code (single
backticks), since the [API Reference](../reference/index.md) renders them
with mkdocstrings. Ruff's pydocstyle rules (`D`, Google convention) check the
format; missing docstrings are not flagged.

## Branches and releases

Work branches off `dev` and PRs target `dev`. PR titles follow
[Conventional Commits](https://www.conventionalcommits.org/) (`feat:`,
`fix:`, `docs:`, …), which release-please uses to version releases and write
the changelog.

## Documentation

The docs are built with [Zensical](https://zensical.org/) from `mkdocs.yml`
and `docs/`. API reference pages are generated from docstrings with
mkdocstrings.

```bash
uvx --with-requirements docs/requirements.txt zensical serve
```

To add a page, create it under `docs/` and add it to the `nav` in
`mkdocs.yml`. Pages under an existing section directory are picked up by
`llms.txt` automatically.

Every PR into `dev` builds the site with `zensical build --strict`; pushes to
`main` deploy it to GitHub Pages.
