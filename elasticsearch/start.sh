#!/bin/bash

DIR=$(dirname "$0")
REGISTRY_CODE=$(curl -s -o /dev/null -w "%{http_code}" -I $1/storage-registry/)
REGISTRY_LOCK_CODE=$(curl -s -o /dev/null -w "%{http_code}" -I $1/storage-registry-lock/)
STORAGE_DATA_TEMPLATE_CODE=$(curl -s -o /dev/null -w "%{http_code}" -I $1/_template/storage-data/)
STORAGE_HISTORY_TEMPLATE_CODE=$(curl -s -o /dev/null -w "%{http_code}" -I $1/_template/storage-history/)

if [ ! $REGISTRY_CODE -eq 200 ]
then
    curl -s -o dev/null -X PUT "$1/storage-registry" -H 'Content-Type: application/json' --data-binary \
      "@"$DIR"/storage-registry.json"
fi

if [ ! $REGISTRY_LOCK_CODE -eq 200 ]
then
    curl -s -o dev/null -X PUT "$1/storage-registry-lock" -H 'Content-Type: application/json' --data-binary \
      "@"$DIR"/storage-registry-lock.json"
fi

if [ ! STORAGE_DATA_TEMPLATE_CODE -eq 200 ]
then
    curl -s -o dev/null -X PUT "$1/_template/storage-data" -H 'Content-Type: application/json' --data-binary \
      "@"$DIR"/storage-data-template.json"
fi

if [ ! STORAGE_HISTORY_TEMPLATE_CODE -eq 200 ]
then
    curl -s -o dev/null -X PUT "$1/_template/storage-history" -H 'Content-Type: application/json' --data-binary \
      "@"$DIR"/storage-history-template.json"
fi
