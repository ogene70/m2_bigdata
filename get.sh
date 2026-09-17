#!/usr/bin/env bash
set -euo pipefail

key="${1:?Usage: ./get-file.sh <object-key> [bucket] [destination]}"
bucket="${2:-data}"
destination="${3:-$(basename "$key")}"
endpoint="http://localhost:9000"

curl -fsS \
  --user "root:root-134" \
  --aws-sigv4 "aws:amz:us-east-1:s3" \
  "$endpoint/$bucket/$key" \
  --output "$destination"

echo "Downloaded: $destination"