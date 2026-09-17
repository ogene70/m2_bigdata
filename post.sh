#!/usr/bin/env bash
set -euo pipefail

file="${1:?Usage: ./post-file.sh <file> [bucket] [object-key]}"
bucket="${2:-data}"
key="${3:-$(basename "$file")}"
endpoint="http://localhost:9000"
auth=(--user "root:root-134" --aws-sigv4 "aws:amz:us-east-1:s3")

# Create bucket; ignore it if it already exists.
curl -fsS "${auth[@]}" -X PUT "$endpoint/$bucket" >/dev/null || true

curl -fsS "${auth[@]}" \
  --upload-file "$file" \
  "$endpoint/$bucket/$key"

echo "Uploaded: s3://$bucket/$key"