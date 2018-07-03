#!/bin/bash

curl -X PUT "$1/storage-registry" -H 'Content-Type: application/json' --data-binary \
  "@/etc/cs-kv-storage/storage-registry.json"

curl -X PUT "$1/_template/storage" -H 'Content-Type: application/json' --data-binary \
  "@/etc/cs-kv-storage/storage-template.json"

curl -X PUT "$1/_template/history-storage" -H 'Content-Type: application/json' --data-binary \
  "@/etc/cs-kv-storage/storage-history-template.json"

curl -X PUT "$1/storage-read-only"

curl -X PUT "$1/storage-registry/_doc/read-only" -H 'Content-Type: application/json' --data-raw \
  '{"type": "ACC", "is_history_enabled": true}'

curl -X POST "$1/storage-read-only/_doc/_bulk" -H 'Content-Type: application/json' --data-binary \
  "@/etc/cs-kv-storage/init/storage-test-data.ndjson"

curl -X PUT "$1/storage-empty"

curl -X PUT "$1/storage-registry/_doc/empty" -H 'Content-Type: application/json' --data-raw \
  '{"type": "ACC", "is_history_enabled": true}'
s
curl -X PUT "$1/storage-editable-single"

curl -X PUT "$1/storage-registry/_doc/editable-single" -H 'Content-Type: application/json' --data-raw \
  '{"type": "ACC", "is_history_enabled": true}'

curl -X POST "$1/storage-editable-single/_doc/_bulk" -H 'Content-Type: application/json' --data-binary \
  "@/etc/cs-kv-storage/init/storage-test-data.ndjson"

curl -X PUT "$1/storage-editable-multiple"

curl -X PUT "$1/storage-registry/_doc/editable-multiple" -H 'Content-Type: application/json' --data-raw \
  '{"type": "ACC", "is_history_enabled": true}'

curl -X POST "$1/storage-editable-multiple/_doc/_bulk" -H 'Content-Type: application/json' --data-binary \
  "@/etc/cs-kv-storage/init/storage-test-data.ndjson"

curl -X PUT "$1/storage-deletable-single"

curl -X PUT "$1/storage-registry/_doc/deletable-single" -H 'Content-Type: application/json' --data-raw \
  '{"type": "ACC", "is_history_enabled": true}'

curl -X POST "$1/storage-deletable-single/_doc/_bulk" -H 'Content-Type: application/json' --data-binary \
  "@/etc/cs-kv-storage/init/storage-test-data.ndjson"

curl -X PUT "$1/storage-deletable-multiple"

curl -X PUT "$1/storage-registry/_doc/deletable-multiple" -H 'Content-Type: application/json' --data-raw \
  '{"type": "ACC", "is_history_enabled": true}'

curl -X POST "$1/storage-deletable-multiple/_doc/_bulk" -H 'Content-Type: application/json' --data-binary \
  "@/etc/cs-kv-storage/init/storage-test-data.ndjson"

curl -X PUT "$1/storage-cleanable"

curl -X PUT "$1/storage-registry/_doc/cleanable" -H 'Content-Type: application/json' --data-raw \
  '{"type": "ACC", "is_history_enabled": true}'

curl -X POST "$1/storage-cleanable/_doc/_bulk" -H 'Content-Type: application/json' --data-binary \
  "@/etc/cs-kv-storage/init/storage-test-data.ndjson"

curl -X PUT "$1/storage-temp"

curl -X PUT "$1/storage-registry/_doc/temp" -H 'Content-Type: application/json' --data-raw \
  '{"type": "TEMP", "is_history_enabled": false, "ttl": 10000, "expiration_timestamp": 1530602449565}'

curl -X PUT "$1/storage-account"

curl -X PUT "$1/storage-registry/_doc/account" -H 'Content-Type: application/json' --data-raw \
  '{"type": "ACC", "is_history_enabled": true}'

curl -X PUT "$1/storage-no-index"

curl -X PUT "$1/storage-registry/_doc/not-registered" -H 'Content-Type: application/json' --data-raw \
  '{"type": "TEMP", "is_history_enabled": false, "ttl": 10000, "expiration_timestamp": 1530602449565}'
