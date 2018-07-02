#!/bin/bash

curl -X PUT "$1/storage-registry" -H 'Content-Type: application/json' --data-binary \
  "@/etc/cs-kv-storage/storage-registry.json"

curl -X PUT "$1/_template/storage" -H 'Content-Type: application/json' --data-binary \
  "@/etc/cs-kv-storage/storage-template.json"

curl -X PUT "$1/_template/history-storage" -H 'Content-Type: application/json' --data-binary \
  "@/etc/cs-kv-storage/storage-history-template.json"

curl -X PUT "$1/storage-read-only"

curl -X PUT "$1/storage-registry/_doc/storage-read-only" -H 'Content-Type: application/json' --data-raw \
  '{"type": "ACC", "is_history_enabled": true}'

curl -X PUT "$1/storage-read-only/_doc/_bulk" -H 'Content-Type: application/json' --data-binary \
  "@/etc/cs-kv-storage/init/storage-read-only.ndjson"

curl -X PUT "$1/storage-empty"

