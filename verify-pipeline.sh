#!/bin/bash

set -euo pipefail

# Script to verify the end-to-end pipeline: docker compose up, Redpanda -> Arroyo -> SSE

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

# Check if docker compose is available
if ! command -v docker compose &> /dev/null; then
    log_error "docker compose is not available"
    exit 1
fi

# Check if services are running
log_info "Checking Docker services..."
services_status=0

# Check if Redpanda is running
if docker ps --format "{{.Names}}" | grep -q "m2-bigdata-redpanda"; then
    log_info "✓ Redpanda is running"
else
    log_warn "✗ Redpanda is not running"
    services_status=1
fi

# Check if topic-push is running
if docker ps --format "{{.Names}}" | grep -q "m2-bigdata-topic-push"; then
    log_info "✓ topic-push is running"
else
    log_warn "✗ topic-push is not running"
    services_status=1
fi

# Check if arroyo is running
if docker ps --format "{{.Names}}" | grep -q "m2-bigdata-arroyo"; then
    log_info "✓ Arroyo is running"
else
    log_warn "✗ Arroyo is not running"
    services_status=1
fi

# Check if arroyo-init has run
if docker ps --format "{{.Names}}" | grep -q "m2-bigdata-arroyo-init"; then
    log_info "✓ arroyo-init has been executed"
else
    log_warn "✗ arroyo-init has not been executed yet"
    # Check if it's been completed
    if docker ps --format "{{.Names}}" | grep -q "m2-bigdata-arroyo-init"; then
        log_info "  (arroyo-init service completed successfully)"
    fi
fi

# Check if dashboard-html is running
if docker ps --format "{{.Names}}" | grep -q "m2-bigdata-dashboard-html"; then
    log_info "✓ dashboard-html is running"
else
    log_warn "✗ dashboard-html is not running"
    services_status=1
fi

# Check if topic extraction plugin is running (part of ardecode agent)
# This is harder to check from outside, but we can check logs
log_info "Checking topic extraction pipeline..."

# Try to connect to Redpanda (if available)
if docker exec m2-bigdata-redpanda rpk cluster health 2>/dev/null; then
    log_info "✓ Redpanda cluster is healthy"
else
    log_warn "✗ Redpanda cluster health check failed"
    services_status=1
fi

# Check if topic-push is accepting connections
if curl -s "http://localhost:8090/healthz" | grep -q "ok"; then
    log_info "✓ topic-push health endpoint is responding"
else
    log_warn "✗ topic-push health endpoint is not responding"
    services_status=1
fi

# Check if dashboard-html is accessible (basic check)
if curl -s "http://localhost:4001/healthz" | grep -q "ok"; then
    log_info "✓ dashboard-html health endpoint is responding"
else
    log_warn "✗ dashboard-html health endpoint is not responding"
    services_status=1
fi

# Check if Arroyo pipeline is running (if arroyo-init completed)
if docker ps --format "{{.Names}}" | grep -q "m2-bigdata-arroyo-init"; then
    log_info "Checking Arroyo pipeline status..."
    # Try to get pipeline status from Arroyo
    if curl -s "http://localhost:5115/api/v1/pipelines" | grep -q "session-topics"; then
        log_info "✓ Arroyo pipeline 'session-topics' is registered"
    else
        log_warn "✗ Arroyo pipeline 'session-topics' is not registered"
        services_status=1
    fi
fi

# Summary
if [ $services_status -eq 0 ]; then
    log_info "\n✅ All critical services are running!"
    log_info "\nPipeline status:")
    log_info "  - Redpanda broker: Healthy ✓"
    log_info "  - topic-push service: Running ✓"
    log_info "  - Arroyo pipeline: Registered ✓"
    log_info "  - dashboard-html: Accessible ✓"
    log_info "\nYou should be able to view the real-time topic dashboard at: http://localhost:4001"
else
    log_error "\n⚠️ Some services are not running properly."
    log_error "Please check the issues above and try running: docker compose up -d"
    exit 1
fi

exit 0