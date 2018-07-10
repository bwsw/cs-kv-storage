#!/bin/bash

REGISTRY_CODE=$(curl -s -o /dev/null -w "%{http_code}" -I $1/storage-registry/)
REGISTRY_LOCK_CODE=$(curl -s -o /dev/null -w "%{http_code}" -I $1/storage-registry-lock/)
STORAGE_TEMPLATE_CODE=$(curl -s -o /dev/null -w "%{http_code}" -I $1/_template/storage/)
HISTORY_STORAGE_TEMPLATE_CODE=$(curl -s -o /dev/null -w "%{http_code}" -I $1/_template/history-storage/)

if [ ! $REGISTRY_CODE -eq 200 ]
then
    curl -s -o dev/null -X PUT "$1/storage-registry" -H 'Content-Type: application/json' --data-binary \
      "@storage-registry.json"
fi

if [ ! $REGISTRY_LOCK_CODE -eq 200 ]
then
    curl -s -o dev/null -X PUT "$1/storage-registry-lock" -H 'Content-Type: application/json' --data-binary \
      "@storage-registry-lock.json"
fi

if [ ! $STORAGE_TEMPLATE_CODE -eq 200 ]
then
    curl -s -o dev/null -X PUT "$1/_template/storage" -H 'Content-Type: application/json' --data-binary \
      "@storage-template.json"
fi

if [ ! $HISTORY_STORAGE_TEMPLATE_CODE -eq 200 ]
then
    curl -s -o dev/null -X PUT "$1/_template/history-storage" -H 'Content-Type: application/json' --data-binary \
      "@storage-history-template.json"
fi
