#!/bin/bash
# Send sample NMEA AIS messages into the local Redpanda topics.
# Run AFTER docker compose up:
#   bash docker/send-test-messages.sh

BROKER="${BROKER:-localhost:19092}"

send() {
  local topic="$1"
  local msg="$2"
  echo "$msg" | docker run --rm -i --network ais-flink-job1-java_ais-net \
    redpandadata/redpanda:v24.1.1 \
    rpk topic produce "$topic" -b redpanda:9092 --compression none 2>/dev/null
}

echo "==> Sending Type 1 (Position Report) to ais-terrestrial..."
send ais-terrestrial '!AIVDM,1,1,,A,15M67N0000G?Uf6E`FepT@0<0000,0*73'

echo "==> Sending Type 5 (Static + Voyage, 2-part) to ais-terrestrial..."
send ais-terrestrial '!AIVDM,2,1,3,B,55?MbV02>H9oL9a<0H4eN4j0T4@Dn2222220l1@A83k0Ep6888888880,0*2A'
send ais-terrestrial '!AIVDM,2,2,3,B,88888888880,2*25'

echo "==> Sending Type 18 (Class B Position) to ais-satellite..."
send ais-satellite '!AIVDM,1,1,,B,B52K>;h00Fc>jpUlNV@ikwpUoP06,0*38'

echo "==> Sending Type 24 Part A to ais-roaming..."
send ais-roaming '!AIVDM,1,1,,A,H52KMe4<9>5Ul0000000000000,2*45'

echo "==> Sending Type 24 Part B to ais-roaming..."
send ais-roaming '!AIVDM,1,1,,A,H52KMe000000004j1000000@060,0*5C'

echo ""
echo "==> Done. Check Flink UI at http://localhost:8081"
echo "==> Check parsed output topic:"
echo "    docker run --rm --network ais-flink-job1-java_ais-net redpandadata/redpanda:v24.1.1 \\"
echo "      rpk topic consume ais.ingest -b redpanda:9092 --num 10"
