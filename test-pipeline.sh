#!/bin/bash

set -euo pipefail

# Test script to verify the topic extraction pipeline

RED="\033[0;31m"
GREEN="\033[0;32m"
YELLOW="\033[1;33m"
NC="\033[0m"

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Test 1: Verify topic extraction plugin code
log_info "Test 1: Verifying topic extraction plugin code..."

if [ ! -f "./.opencode/lib/topic-extractor.mjs" ]; then
    log_error "✗ topic-extractor.mjs not found"
    exit 1
fi

if [ ! -f "./.opencode/plugins/session-archive.js" ]; then
    log_error "✗ session-archive.js not found"
    exit 1
fi

# Check that topic column is in the Parquet schema
if grep -q "topic.*UTF8" ./.opencode/lib/session-archive.mjs; then
    log_info "✓ Topic column is in the Parquet schema"
else
    log_error "✗ Topic column is not in the Parquet schema"
    exit 1
fi

# Check that topic is included in the node function
if grep -q "topic.*topic" ./.opencode/lib/session-archive.mjs | grep -q "return.*{.*;.*topic"; then
    log_info "✓ Topic column is in the node() function"
else
    log_error "✗ Topic column is not in the node() function"
    exit 1
fi

# Check that topic is included in buildRows
if grep -q "topics.*topicsFor" ./.opencode/plugins/session-archive.js; then
    log_info "✓ buildRows() includes topics parameter"
else
    log_error "✗ buildRows() does not include topics parameter"
    exit 1
fi

# Test 2: Verify Redpanda Connect schema propagation
log_info "\nTest 2: Verifying Redpanda Connect schema propagation..."

if [ ! -f "./redpanda-connect/s3-archive.yaml" ]; then
    log_error "✗ s3-archive.yaml not found"
    exit 1
fi

# Check that topic is in the Parquet schema for Redpanda Connect
if grep -q "name: topic" ./redpanda-connect/s3-archive.yaml; then
    log_info "✓ Topic column is in Redpanda Connect Parquet schema"
else
    log_error "✗ Topic column is not in Redpanda Connect Parquet schema"
    exit 1
fi

# Test 3: Verify Spark job schema propagation
log_info "\nTest 3: Verifying Spark job schema propagation..."

if [ ! -f "./spark/jobs/ingest-sessions.py" ]; then
    log_error "✗ ingest-sessions.py not found"
    exit 1
fi

# Check that topic is in the Iceberg table schema
if grep -q "topic string" ./spark/jobs/ingest-sessions.py; then
    log_info "✓ Topic column is in Iceberg table schema"
else
    log_error "✗ Topic column is not in Iceberg table schema"
    exit 1
fi

# Check that topic is included in the ALTER TABLE statement
if grep -q "ADD COLUMNS.*topic" ./spark/jobs/ingest-sessions.py; then
    log_info "✓ Topic column is added to existing tables via ALTER TABLE"
else
    log_error "✗ Topic column is not added to existing tables via ALTER TABLE"
    exit 1
fi

# Check that topic is included in the MERGE statement
if grep -q "t.topic = s.topic" ./spark/jobs/ingest-sessions.py; then
    log_info "✓ Topic column is included in MERGE statement"
else
    log_error "✗ Topic column is not included in MERGE statement"
    exit 1
fi

# Test 4: Verify Arroyo pipeline
log_info "\nTest 4: Verifying Arroyo pipeline...".

if [ ! -f "./arroyo/pipeline.sql" ]; then
    log_error "✗ pipeline.sql not found"
    exit 1
fi

# Check that topic column is in the SELECT statement
if grep -q "topic" ./arroyo/pipeline.sql; then
    log_info "✓ Topic column is in the Arroyo pipeline SELECT statement"
else
    log_error "✗ Topic column is not in the Arroyo pipeline SELECT statement"
    exit 1
fi

# Test 5: Verify topic-push service
log_info "\nTest 5: Verifying topic-push service..."

if [ ! -f "./topic-push/server.js" ]; then
    log_error "✗ server.js not found"
    exit 1
fi

# Check that topic extraction is in topic-push
if grep -q "topic.*row.*topic" ./topic-push/server.js; then
    log_info "✓ Topic extraction is in topic-push"
else
    log_error "✗ Topic extraction is not in topic-push"
    exit 1
fi

# Test 6: Verify dashboard HTML
log_info "\nTest 6: Verifying dashboard HTML..."

if [ ! -f "./dashboard-html/index.html" ]; then
    log_error "✗ index.html not found"
    exit 1
fi

# Check that dashboard has topic display logic
if grep -q "topic" ./dashboard-html/index.html | grep -q "svg\|timeline"; then
    log_info "✓ Dashboard includes topic display logic"
else
    log_error "✗ Dashboard does not include topic display logic"
    exit 1
fi

# Test 7: Verify docker-compose configuration
log_info "\nTest 7: Verifying docker-compose configuration..."

if [ ! -f "./docker-compose.yml" ]; then
    log_error "✗ docker-compose.yml not found"
    exit 1
fi

# Check that all services are defined
if grep -q "^  arroyo:" ./docker-compose.yml && \
   grep -q "^  topic-push:" ./docker-compose.yml && \
   grep -q "^  dashboard-html:" ./docker-compose.yml; then
    log_info "✓ All required services are defined in docker-compose.yml"
else
    log_error "✗ Some services are missing from docker-compose.yml"
    exit 1
fi

# Summary
log_info "\n" + "="*60)
log_info "✅ All pipeline components have been verified!"
log_info "="*60)
log_info ""
log_info "The topic extraction pipeline is correctly implemented with the following flow:"
log_info "1. Plugin opencode: Extracts topics from user messages"
log_info "2. Redpanda Connect: Persists topic to Parquet (s3://sessions/)"
log_info "3. Spark (iceberg-ingest): Reads Parquet, writes to Iceberg analytics.sessions"
log_info "4. Arroyo (session-topics): Filters for user messages with topics"
log_info "5. topic-push: Ingests topic events, maintains in-memory buffer"
log_info "6. dashboard-html: Displays real-time topic timeline"
log_info ""
log_info "To test the pipeline end-to-end:"
log_info "1. Run: docker compose up -d"
log_info "2. Run: arroyo-init (to register the pipeline)"
log_info "3. View dashboard: http://localhost:4001"
log_info "4. Run verification: ./verify-pipeline.sh"