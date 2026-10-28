#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$SCRIPT_DIR/data"
DOWNLOAD_DIR="$SCRIPT_DIR/downloads"
ALL_TARGETS=(mstransfer sftp s3)
ALL_OPERATIONS=(upload download)
BANDWIDTH=""
OPERATION="both"

usage() {
    echo "Usage: $(basename "$0") [options] [target ...]"
    echo ""
    echo "Options:"
    echo "  -b, --bandwidth <mbit>     Throttle bandwidth using tc (in mbit/s)"
    echo "  -o, --operation <op>       upload, download, or both (default: both)"
    echo "  -h, --help                 Show this help message"
    echo ""
    echo "Targets: mstransfer, sftp, s3 (default: all)"
    echo ""
    echo "Examples:"
    echo "  $(basename "$0")                            # upload+download, all targets"
    echo "  $(basename "$0") -o upload mstransfer       # upload only, mstransfer"
    echo "  $(basename "$0") -o download sftp s3        # download only (assumes files exist remotely)"
    echo "  $(basename "$0") -b 100 -o both             # throttled to 100 mbit/s, both ops"
    exit 0
}

# Parse options
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) usage ;;
        -b|--bandwidth)
            BANDWIDTH="$2"
            shift 2
            ;;
        -o|--operation)
            OPERATION="$2"
            shift 2
            ;;
        *) break ;;
    esac
done

case "$OPERATION" in
    upload|download|both) ;;
    *) echo "ERROR: --operation must be upload, download, or both"; exit 1 ;;
esac

# ── Helpers ──────────────────────────────────────────────────────────────────

# Cross-platform high-resolution timestamp (seconds with decimals)
# Detect once whether date supports %N (GNU coreutils) or we need perl (macOS)
if date +%s.%N 2>/dev/null | grep -qv '\.N'; then
    now() { date +%s.%N; }
else
    now() { perl -MTime::HiRes -e 'printf "%.3f\n", Time::HiRes::time()'; }
fi

# Cross-platform file size in bytes
file_size() {
    if stat -f%z "$1" &>/dev/null; then
        stat -f%z "$1"          # macOS
    else
        stat -c%s "$1"          # Linux
    fi
}

# Compute throughput: args <size_bytes> <duration_seconds>
throughput() {
    local size_bytes=$1 duration=$2
    awk "BEGIN { printf \"%.2f\", $size_bytes / 1000000 / $duration }"
}

# Format bytes as MB with 1 decimal
fmt_mb() {
    awk "BEGIN { printf \"%.1f\", $1 / 1000000 }"
}

# ── Bandwidth throttling via tc ──────────────────────────────────────────────

NET_IFACE=""

tc_setup() {
    # Detect the default outbound interface
    NET_IFACE=$(ip route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="dev") print $(i+1); exit}')
    if [[ -z "$NET_IFACE" ]]; then
        echo "ERROR: Could not detect network interface for tc throttling."
        exit 1
    fi
    echo "Throttling bandwidth to ${BANDWIDTH}mbit on $NET_IFACE"
    sudo tc qdisc replace dev "$NET_IFACE" root tbf rate "${BANDWIDTH}mbit" burst 32kbit latency 50ms
}

tc_teardown() {
    if [[ -n "$NET_IFACE" ]]; then
        sudo tc qdisc del dev "$NET_IFACE" root 2>/dev/null || true
    fi
}

if [[ -n "$BANDWIDTH" ]]; then
    if ! command -v tc &>/dev/null; then
        echo "ERROR: 'tc' (iproute2) is required for bandwidth throttling but is not installed."
        exit 1
    fi
    tc_setup
    trap tc_teardown EXIT
fi

# ── Prerequisites ────────────────────────────────────────────────────────────

if [[ ! -f "$SCRIPT_DIR/.env" ]]; then
    echo "ERROR: .env file not found."
    echo "Copy .env.example to .env and fill in your values:"
    echo "  cp .env.example .env"
    exit 1
fi

# shellcheck source=/dev/null
source "$SCRIPT_DIR/.env"

# Map target name to required CLI command
target_cmd() {
    case "$1" in
        mstransfer) echo "mstransfer" ;;
        sftp)       echo "sftp" ;;
        s3)         echo "aws" ;;
    esac
}

FILES=("$DATA_DIR"/*.mzML)
if [[ ${#FILES[@]} -eq 0 || ! -f "${FILES[0]}" ]]; then
    echo "ERROR: No .mzML files found in $DATA_DIR"
    echo "Run download.sh first to fetch datasets."
    exit 1
fi

FILE_COUNT=${#FILES[@]}

# Export AWS credentials so the aws CLI picks them up
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION

# Select targets from CLI args, defaulting to all
if [[ $# -gt 0 ]]; then
    TARGETS=("$@")
    for t in "${TARGETS[@]}"; do
        valid=false
        for v in "${ALL_TARGETS[@]}"; do [[ "$t" == "$v" ]] && valid=true; done
        if ! $valid; then
            echo "ERROR: Unknown target '$t'. Valid targets: ${ALL_TARGETS[*]}"
            exit 1
        fi
    done
else
    TARGETS=("${ALL_TARGETS[@]}")
fi

# Select operations to run
case "$OPERATION" in
    upload)   OPERATIONS=(upload) ;;
    download) OPERATIONS=(download) ;;
    both)     OPERATIONS=("${ALL_OPERATIONS[@]}") ;;
esac

# Only check for CLIs needed by selected targets
for t in "${TARGETS[@]}"; do
    cmd=$(target_cmd "$t")
    if ! command -v "$cmd" &>/dev/null; then
        echo "ERROR: '$cmd' is required for target '$t' but is not installed or not in PATH."
        exit 1
    fi
done

MODE="${BENCHMARK_MODE:-per-file}"
RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$RESULTS_DIR"
if [[ -n "$BANDWIDTH" ]]; then
    CSV_FILE="$RESULTS_DIR/results_${BANDWIDTH}mbit_$(date +%Y%m%d_%H%M%S).csv"
else
    CSV_FILE="$RESULTS_DIR/results_$(date +%Y%m%d_%H%M%S).csv"
fi
echo "operation,target,file,size_bytes,duration_s,throughput_mbps" > "$CSV_FILE"

# ── Upload functions (single file) ───────────────────────────────────────────

upload_mstransfer() {
    local file=$1
    if [[ -n "${MSTRANSFER_API_KEY:-}" ]]; then
        mstransfer upload "$file" "$MSTRANSFER_HOST" --api-key "$MSTRANSFER_API_KEY"
    else
        mstransfer upload "$file" "$MSTRANSFER_HOST"
    fi
}

upload_sftp() {
    local file=$1
    sftp -P "$SFTP_PORT" -i "$SFTP_KEY_PATH" -b - "$SFTP_USER@$SFTP_HOST" <<< "put $file $SFTP_DEST_DIR/"
}

upload_s3() {
    local file=$1
    aws s3 cp "$file" "s3://$S3_BUCKET/${S3_PREFIX}$(basename "$file")" \
        ${S3_ENDPOINT_URL:+--endpoint-url "$S3_ENDPOINT_URL"}
}

# ── Batch upload functions (all files in one command) ────────────────────────

batch_upload_mstransfer() {
    if [[ -n "${MSTRANSFER_API_KEY:-}" ]]; then
        mstransfer upload "${FILES[@]}" "$MSTRANSFER_HOST" --api-key "$MSTRANSFER_API_KEY"
    else
        mstransfer upload "${FILES[@]}" "$MSTRANSFER_HOST"
    fi
}

batch_upload_sftp() {
    local cmds=""
    for file in "${FILES[@]}"; do
        cmds+="put $file $SFTP_DEST_DIR/"$'\n'
    done
    sftp -P "$SFTP_PORT" -i "$SFTP_KEY_PATH" -b - "$SFTP_USER@$SFTP_HOST" <<< "$cmds"
}

batch_upload_s3() {
    for file in "${FILES[@]}"; do
        aws s3 cp "$file" "s3://$S3_BUCKET/${S3_PREFIX}$(basename "$file")" \
            ${S3_ENDPOINT_URL:+--endpoint-url "$S3_ENDPOINT_URL"}
    done
}

# ── Download helpers ─────────────────────────────────────────────────────────

# Discover what mstransfer has stored (server may store as .msz or .mzML).
# Maps local basename (e.g. foo.mzML) → remote filename (e.g. foo.msz).
declare -a MSTRANSFER_REMOTE_NAMES=()

mstransfer_discover() {
    local auth=()
    if [[ -n "${MSTRANSFER_API_KEY:-}" ]]; then
        auth=(-H "Authorization: Bearer $MSTRANSFER_API_KEY")
    fi
    local json
    json=$(curl -sf "${auth[@]}" "$MSTRANSFER_HOST/files") || {
        echo "ERROR: Failed to list files from $MSTRANSFER_HOST/files" >&2
        return 1
    }
    # Extract "name" fields via python (available wherever uv is)
    mapfile -t MSTRANSFER_REMOTE_NAMES < <(
        python3 -c 'import sys,json; [print(f["name"]) for f in json.load(sys.stdin)["files"]]' <<< "$json"
    )
}

# Given a local filename (e.g. foo.mzML), find the matching remote name.
# Matches by stem since server may have changed the extension.
mstransfer_remote_for() {
    local local_name=$1
    local stem=${local_name%.*}
    for remote in "${MSTRANSFER_REMOTE_NAMES[@]}"; do
        [[ "${remote%.*}" == "$stem" ]] && { echo "$remote"; return 0; }
    done
    # Fallback to default server storage extension
    echo "${stem}.msz"
}

reset_download_dir() {
    rm -rf "$DOWNLOAD_DIR"
    mkdir -p "$DOWNLOAD_DIR"
}

# ── Download functions (single file) ─────────────────────────────────────────

download_mstransfer() {
    local file=$1
    local fname
    fname=$(basename "$file")
    local remote
    remote=$(mstransfer_remote_for "$fname")
    local url="$MSTRANSFER_HOST/files/$remote"
    if [[ -n "${MSTRANSFER_API_KEY:-}" ]]; then
        mstransfer download "$url" -o "$DOWNLOAD_DIR" --store-as mzml --force \
            --api-key "$MSTRANSFER_API_KEY"
    else
        mstransfer download "$url" -o "$DOWNLOAD_DIR" --store-as mzml --force
    fi
}

download_sftp() {
    local file=$1
    local fname
    fname=$(basename "$file")
    sftp -P "$SFTP_PORT" -i "$SFTP_KEY_PATH" -b - "$SFTP_USER@$SFTP_HOST" \
        <<< "get $SFTP_DEST_DIR/$fname $DOWNLOAD_DIR/"
}

download_s3() {
    local file=$1
    local fname
    fname=$(basename "$file")
    aws s3 cp "s3://$S3_BUCKET/${S3_PREFIX}$fname" "$DOWNLOAD_DIR/$fname" \
        ${S3_ENDPOINT_URL:+--endpoint-url "$S3_ENDPOINT_URL"}
}

# ── Batch download functions (all files in one command) ──────────────────────

batch_download_mstransfer() {
    local urls=()
    for file in "${FILES[@]}"; do
        local fname remote
        fname=$(basename "$file")
        remote=$(mstransfer_remote_for "$fname")
        urls+=("$MSTRANSFER_HOST/files/$remote")
    done
    if [[ -n "${MSTRANSFER_API_KEY:-}" ]]; then
        mstransfer download "${urls[@]}" -o "$DOWNLOAD_DIR" --store-as mzml --force \
            --api-key "$MSTRANSFER_API_KEY"
    else
        mstransfer download "${urls[@]}" -o "$DOWNLOAD_DIR" --store-as mzml --force
    fi
}

batch_download_sftp() {
    local cmds=""
    for file in "${FILES[@]}"; do
        local fname
        fname=$(basename "$file")
        cmds+="get $SFTP_DEST_DIR/$fname $DOWNLOAD_DIR/"$'\n'
    done
    sftp -P "$SFTP_PORT" -i "$SFTP_KEY_PATH" -b - "$SFTP_USER@$SFTP_HOST" <<< "$cmds"
}

batch_download_s3() {
    for file in "${FILES[@]}"; do
        local fname
        fname=$(basename "$file")
        aws s3 cp "s3://$S3_BUCKET/${S3_PREFIX}$fname" "$DOWNLOAD_DIR/$fname" \
            ${S3_ENDPOINT_URL:+--endpoint-url "$S3_ENDPOINT_URL"}
    done
}

# ── Benchmark runners ────────────────────────────────────────────────────────

run_per_file() {
    local op=$1 target=$2
    local fn="${op}_${target}"
    local total_size=0 total_duration=0

    echo "=== [$op] $target ==="

    local idx=0
    for file in "${FILES[@]}"; do
        idx=$((idx + 1))
        local fname
        fname=$(basename "$file")
        local size
        size=$(file_size "$file")
        total_size=$((total_size + size))

        local t0 t1 dur tput
        t0=$(now)
        $fn "$file"
        t1=$(now)
        dur=$(awk "BEGIN { printf \"%.1f\", $t1 - $t0 }")
        tput=$(throughput "$size" "$dur")

        printf "  [%d/%d] %-45s %8s MB  %6ss  %s MB/s\n" \
            "$idx" "$FILE_COUNT" "$fname" "$(fmt_mb "$size")" "$dur" "$tput"

        echo "$op,$target,$fname,$size,$dur,$tput" >> "$CSV_FILE"
    done

    total_duration=$(awk -F, -v op="$op" -v t="$target" \
        '$1==op && $2==t { sum+=$5 } END { printf "%.1f", sum }' "$CSV_FILE")
    local total_tput
    total_tput=$(throughput "$total_size" "$total_duration")

    printf "  Aggregate: %s MB in %ss — %s MB/s\n\n" \
        "$(fmt_mb "$total_size")" "$total_duration" "$total_tput"
}

run_batch() {
    local op=$1 target=$2
    local total_size=0

    for file in "${FILES[@]}"; do
        total_size=$((total_size + $(file_size "$file")))
    done

    echo "=== [$op] $target ==="

    local t0 t1 dur tput
    t0=$(now)
    "batch_${op}_${target}"
    t1=$(now)
    dur=$(awk "BEGIN { printf \"%.1f\", $t1 - $t0 }")
    tput=$(throughput "$total_size" "$dur")

    printf "  %s MB in %ss — %s MB/s\n\n" \
        "$(fmt_mb "$total_size")" "$dur" "$tput"

    echo "$op,$target,ALL,$total_size,$dur,$tput" >> "$CSV_FILE"
}

# ── Ensure S3 bucket exists ───────────────────────────────────────────────────

for t in "${TARGETS[@]}"; do
    if [[ "$t" == "s3" ]]; then
        if ! aws s3api head-bucket --bucket "$S3_BUCKET" \
            ${S3_ENDPOINT_URL:+--endpoint-url "$S3_ENDPOINT_URL"} 2>/dev/null; then
            echo "Creating S3 bucket: $S3_BUCKET"
            aws s3api create-bucket --bucket "$S3_BUCKET" \
                ${S3_ENDPOINT_URL:+--endpoint-url "$S3_ENDPOINT_URL"}
        fi
        break
    fi
done

# ── Main ─────────────────────────────────────────────────────────────────────

echo "Benchmark mode: $MODE"
echo "Operations: ${OPERATIONS[*]}"
echo "Targets: ${TARGETS[*]}"
echo "Files: $FILE_COUNT mzML files in $DATA_DIR"
echo "CSV output: $CSV_FILE"
echo ""

for op in "${OPERATIONS[@]}"; do
    for target in "${TARGETS[@]}"; do
        if [[ "$op" == "download" ]]; then
            reset_download_dir
            if [[ "$target" == "mstransfer" ]]; then
                mstransfer_discover
            fi
        fi
        if [[ "$MODE" == "per-file" ]]; then
            run_per_file "$op" "$target"
        else
            run_batch "$op" "$target"
        fi
    done
done

echo "Results written to $CSV_FILE"
