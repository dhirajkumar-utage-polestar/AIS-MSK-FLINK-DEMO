#!/bin/bash
# Wait for JobManager REST API to be ready, then submit the job.
set -e

JM_HOST="${JM_HOST:-jobmanager}"
JM_PORT="${JM_PORT:-8081}"
JAR="/opt/flink/usrlib/ais-flink-job1.jar"

echo "[submit] Waiting for Flink JobManager at ${JM_HOST}:${JM_PORT} ..."
until curl -sf "http://${JM_HOST}:${JM_PORT}/overview" > /dev/null 2>&1; do
  sleep 2
done

echo "[submit] JobManager ready — submitting AIS Job 1 ..."

# Upload JAR
UPLOAD=$(curl -sf -X POST \
  "http://${JM_HOST}:${JM_PORT}/jars/upload" \
  -H "Expect:" \
  -F "jarfile=@${JAR}")

JAR_ID=$(echo "${UPLOAD}" | grep -o '"filename":"[^"]*"' | sed 's|"filename":"||;s|"||g;s|.*/||')
echo "[submit] JAR uploaded: ${JAR_ID}"

# Submit job with entry class
RESPONSE=$(curl -s -X POST \
  "http://${JM_HOST}:${JM_PORT}/jars/${JAR_ID}/run" \
  -H "Content-Type: application/json" \
  -d "{
    \"entryClass\": \"com.polestar.ais.Job1ParserValidator\",
    \"parallelism\": ${FLINK_PARALLELISM:-1}
  }")

echo "[submit] Flink response: ${RESPONSE}"

if echo "${RESPONSE}" | grep -q '"jobid"'; then
  JOB_ID=$(echo "${RESPONSE}" | grep -o '"jobid":"[^"]*"' | sed 's/"jobid":"//;s/"//')
  echo "[submit] Job started: ${JOB_ID}"
  echo "[submit] Flink UI: http://${JM_HOST}:${JM_PORT}/#/job/${JOB_ID}/overview"
else
  echo "[submit] ERROR: job submission failed"
  exit 1
fi
