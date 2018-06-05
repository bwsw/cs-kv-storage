#!/bin/bash

curl -X PUT "$1/storage-registry" -H 'Content-Type: application/json' --data-binary \
  "@/etc/cs-kv-storage/storage-registry.json"

curl -X PUT "$1/_template/storage-template" -H 'Content-Type: application/json' --data-binary \
  "@/etc/cs-kv-storage/storage-template.json"

curl -X PUT "$1/storage-test"

