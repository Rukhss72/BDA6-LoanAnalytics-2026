#!/usr/bin/env bash
set -e

PROJECT=/loan_project
RAW_DIR=$PROJECT/raw
CURATED_DIR=$PROJECT/curated
LOCAL_CSV="$1"

if [ -z "$LOCAL_CSV" ]; then
  echo "Usage: $0 /path/to/loan.csv"
  exit 1
fi

hdfs dfs -mkdir -p "$RAW_DIR" "$CURATED_DIR"
hdfs dfs -put -f "$LOCAL_CSV" "$RAW_DIR/"

echo "=== HDFS raw listing ==="
hdfs dfs -ls -h "$RAW_DIR"
echo "=== HDFS raw size ==="
hdfs dfs -du -h "$RAW_DIR"
echo "=== Row count (excluding header) ==="
hdfs dfs -cat "$RAW_DIR/loan.csv" | tail -n +2 | wc -l
