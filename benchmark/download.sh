#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST="$SCRIPT_DIR/datasets.txt"
DATA_DIR="$SCRIPT_DIR/data"
CONVERTER_IMAGE="thermorawfileparser:local"
DOCKERFILE="$SCRIPT_DIR/Dockerfile.thermorawfileparser"

if [[ ! -f "$MANIFEST" ]]; then
    echo "Error: datasets.txt not found at $MANIFEST"
    exit 1
fi

# Check Docker is available
if ! command -v docker &>/dev/null; then
    echo "Error: docker is not installed. Install Docker to convert RAW files to mzML."
    exit 1
fi
if ! docker info &>/dev/null; then
    echo "Error: Docker daemon is not running. Start Docker and try again."
    exit 1
fi

# Build converter image if it doesn't exist
if ! docker image inspect "$CONVERTER_IMAGE" &>/dev/null; then
    echo "Building ThermoRawFileParser Docker image..."
    docker build --platform linux/amd64 -t "$CONVERTER_IMAGE" -f "$DOCKERFILE" "$SCRIPT_DIR"
fi

# Read non-comment, non-blank lines (compatible with Bash 3 on macOS)
URLS=()
while IFS= read -r line; do
    URLS+=("$line")
done < <(grep -v '^\s*#' "$MANIFEST" | grep -v '^\s*$')

if [[ ${#URLS[@]} -eq 0 ]]; then
    echo "No URLs found in datasets.txt. Add dataset URLs and try again."
    exit 0
fi

mkdir -p "$DATA_DIR"

TOTAL=${#URLS[@]}
COUNT=0

for URL in "${URLS[@]}"; do
    COUNT=$((COUNT + 1))
    RAW_FILENAME="$(basename "$URL")"
    MZML_FILENAME="${RAW_FILENAME%.*}.mzML"
    RAW_DEST="$DATA_DIR/$RAW_FILENAME"
    MZML_DEST="$DATA_DIR/$MZML_FILENAME"

    # Skip if mzML already exists
    if [[ -f "$MZML_DEST" ]]; then
        echo "[$COUNT/$TOTAL] $MZML_FILENAME (already exists, skipping)"
        continue
    fi

    # Download RAW file
    echo "[$COUNT/$TOTAL] Downloading $RAW_FILENAME..."
    if command -v curl &>/dev/null; then
        curl -fSL --progress-bar -o "$RAW_DEST" "$URL"
    elif command -v wget &>/dev/null; then
        wget -q --show-progress -O "$RAW_DEST" "$URL"
    else
        echo "Error: neither curl nor wget found"
        exit 1
    fi

    # Convert RAW to mzML via ThermoRawFileParser Docker
    echo "[$COUNT/$TOTAL] Converting $RAW_FILENAME to mzML..."
    docker run --rm --platform linux/amd64 -v "$DATA_DIR:/data" \
        "$CONVERTER_IMAGE" \
        -i "/data/$RAW_FILENAME" -o /data/ -f 2

    # Verify conversion succeeded
    if [[ ! -f "$MZML_DEST" ]]; then
        echo "Error: conversion failed — $MZML_FILENAME not found after ThermoRawFileParser"
        exit 1
    fi

    # Clean up RAW file
    rm -f "$RAW_DEST"
    echo "[$COUNT/$TOTAL] $MZML_FILENAME ready (RAW file removed)"
done

echo ""
echo "Done. $TOTAL dataset(s) processed. mzML files in $DATA_DIR"
