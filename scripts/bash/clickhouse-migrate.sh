#!/bin/sh

set -eu

for file in /migrations/*.sql; do
  if [ -f "$file" ]; then
    echo "Running migration: $file"
    clickhouse-client \
      --host clickhouse \
      --user=flexprice \
      --password=flexprice123 \
      --database=flexprice \
      --multiquery < "$file"
  fi
done