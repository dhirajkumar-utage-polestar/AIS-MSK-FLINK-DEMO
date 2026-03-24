#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# deploy-local.sh — Build + deploy AIS Flink Job 1 to local Docker Flink cluster
#
# Usage:
#   ./deploy-local.sh              # default: local Redpanda (no AWS auth needed)
#   ./deploy-local.sh --msk        # use real MSK (needs AWS creds + VPN/tunnel)
#   ./deploy-local.sh --build-only # just build the JAR, don't deploy
#
# Prerequisites:
#   - Maven installed (mvn)
#   - Docker compose stack running:
#       docker compose up -d         (Redpanda mode)
#       docker compose -f docker-compose.yml -f docker-compose.msk.yml up -d  (MSK mode)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

JM_HOST="${JM_HOST:-localhost}"
JM_PORT="${JM_PORT:-8081}"
JM_URL="http://${JM_HOST}:${JM_PORT}"
PARALLELISM="${FLINK_PARALLELISM:-1}"
ENTRY_CLASS="com.polestar.ais.Job1ParserValidator"
JAR_PATTERN="target/ais-flink-job1-*.jar"
MODE="local"

# ── Parse args ───────────────────────────────────────────────────────────────
BUILD_ONLY=false
for arg in "$@"; do
  case "$arg" in
    --msk)        MODE="msk" ;;
    --build-only) BUILD_ONLY=true ;;
    *)            echo "Unknown arg: $arg"; exit 1 ;;
  esac
done

# ── Step 1: Build ─────────────────────────────────────────────────────────────
echo "━━━ [1/4] Building JAR ━━━"
mvn package -DskipTests -q
JAR=$(ls ${JAR_PATTERN} 2>/dev/null | head -1)
if [[ -z "$JAR" ]]; then
  echo "ERROR: no JAR found matching ${JAR_PATTERN}"
  exit 1
fi
echo "✓ Built: ${JAR} ($(du -sh "$JAR" | cut -f1))"

[[ "$BUILD_ONLY" == "true" ]] && { echo "Build-only mode — done."; exit 0; }

# ── Step 2: Start stack if not running ───────────────────────────────────────
echo ""
echo "━━━ [2/4] Checking Flink cluster ━━━"
if ! curl -sf "${JM_URL}/overview" > /dev/null 2>&1; then
  echo "Flink not running — starting compose stack..."
  if [[ "$MODE" == "msk" ]]; then
    docker compose -f docker-compose.yml -f docker-compose.msk.yml up -d \
      jobmanager taskmanager minio minio-init
  else
    docker compose up -d jobmanager taskmanager minio minio-init redpanda redpanda-init
  fi
  echo -n "Waiting for JobManager"
  until curl -sf "${JM_URL}/overview" > /dev/null 2>&1; do
    echo -n "."
    sleep 2
  done
  echo " ready"
else
  echo "✓ Flink cluster already running"
fi

# ── Step 3: Cancel any running jobs ──────────────────────────────────────────
echo ""
echo "━━━ [3/4] Stopping existing jobs ━━━"
ACTIVE=$(curl -sf "${JM_URL}/jobs" | grep -o '"id":"[^"]*","status":"[^"]*"' | grep -E '"RUNNING"|"RESTARTING"|"CREATED"|"INITIALIZING"' | grep -o '"id":"[^"]*"' | sed 's/"id":"//;s/"//' || true)
if [[ -n "$ACTIVE" ]]; then
  for JOB_ID in $ACTIVE; do
    echo "  Cancelling job ${JOB_ID}..."
    curl -sf -X PATCH "${JM_URL}/jobs/${JOB_ID}?mode=cancel" > /dev/null
    until curl -sf "${JM_URL}/jobs/${JOB_ID}" | grep -qE '"status":"CANCELED"|"status":"FAILED"'; do
      sleep 1
    done
    echo "  ✓ Cancelled ${JOB_ID}"
  done
else
  echo "✓ No active jobs"
fi

# ── Step 4: Upload JAR + submit ───────────────────────────────────────────────
echo ""
echo "━━━ [4/4] Uploading + submitting ━━━"

UPLOAD=$(curl -sf -X POST "${JM_URL}/jars/upload" \
  -H "Expect:" \
  -F "jarfile=@${JAR}")
JAR_ID=$(echo "${UPLOAD}" | grep -o '"filename":"[^"]*"' | sed 's|"filename":"||;s|"||g;s|.*/||')
echo "✓ JAR uploaded: ${JAR_ID}"

RESPONSE=$(curl -s -X POST "${JM_URL}/jars/${JAR_ID}/run" \
  -H "Content-Type: application/json" \
  -d "{\"entryClass\": \"${ENTRY_CLASS}\", \"parallelism\": ${PARALLELISM}}")

if echo "${RESPONSE}" | grep -q '"jobid"'; then
  JOB_ID=$(echo "${RESPONSE}" | grep -o '"jobid":"[^"]*"' | sed 's/"jobid":"//;s/"//')
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "✓ Job started: ${JOB_ID}"
  echo "  Flink UI : http://${JM_HOST}:${JM_PORT}/#/job/${JOB_ID}/overview"
  if [[ "$MODE" == "local" ]]; then
    echo "  Redpanda : localhost:19092  (produce test messages)"
    echo "  MinIO UI : http://localhost:9001  (minioadmin / minioadmin)"
  fi
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
else
  echo "ERROR: job submission failed"
  echo "${RESPONSE}"
  exit 1
fi
