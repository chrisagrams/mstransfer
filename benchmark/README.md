# MSTransfer Benchmarks

Compares upload and download throughput of `mstransfer` against `sftp` and
`aws s3 cp` using real mass-spectrometry `.mzML` files, optionally under a
throttled network.

## What's here

- `download.sh` — fetches RAW files listed in `datasets.txt` from PRIDE and
  converts them to `.mzML` via a ThermoRawFileParser Docker image. Output goes
  to `data/`.
- `benchmark.sh` — uploads and/or downloads every `.mzML` in `data/` against
  each target, timing each transfer and writing one CSV per run to `results/`.
  Downloads land in `downloads/` (wiped between targets).
- `plot.py` — reads CSVs from `results/` and renders comparison plots into
  `plots/` (throughput per file, aggregate throughput, speedup vs. bandwidth).
- `Makefile` — runs the full pipeline at 40 / 100 / 1000 mbit/s and unthrottled.

## Setup

1. Install CLIs for the targets you plan to test: `mstransfer`, `sftp`,
   `aws`. Docker is required for `download.sh`. `tc` (iproute2) is required
   if you want to throttle bandwidth.
2. Configure endpoints and credentials:
   ```
   cp .env.example .env
   # then edit .env
   ```
3. Install the plotting deps (managed by `uv`):
   ```
   uv sync
   ```

## Usage

Fetch and convert datasets:
```
./download.sh
```

Run all targets, upload + download, no throttling:
```
./benchmark.sh
```

Run specific targets, throttled to 100 mbit/s:
```
./benchmark.sh -b 100 mstransfer s3
```

Upload only, or download only (download assumes files already live on the
remote — typically because upload has run first):
```
./benchmark.sh -o upload
./benchmark.sh -o download
```

Run the full sweep (40/100/1000 mbit + unthrottled) and regenerate plots:
```
make all
```

## Output

Each `benchmark.sh` invocation writes one CSV to `results/`, named by
bandwidth and timestamp (e.g. `results_100mbit_20260415_170454.csv`).
Columns: `operation,target,file,size_bytes,duration_s,throughput_mbps`,
where `operation` is `upload` or `download`.

`plot.py` consumes every CSV in `results/` to generate the figures in `plots/`.
Plots are emitted per operation (e.g. `throughput_per_file_upload.png`,
`throughput_per_file_download.png`). CSVs without an `operation` column
(from older runs) are treated as uploads.

## How throughput is measured

Throughput is always `local_mzML_bytes / wall_clock_seconds`, whether the
bytes on the wire are compressed or not. For `mstransfer` this means the
reported MB/s reflects the user-visible mzML data rate, including on-the-fly
compression during upload and decompression during download (`mstransfer`
downloads are invoked with `--store-as mzml` so the delivered file matches
the uploaded one).

## Modes

`BENCHMARK_MODE` in `.env` selects:
- `per-file` (default) — times each file individually; one CSV row per file.
- `batch` — times all files as a single upload; one CSV row per target.

## Throttling

`-b <mbit>` applies a `tc tbf` qdisc to the default outbound interface for the
duration of the run and tears it down on exit. Requires `sudo`.
