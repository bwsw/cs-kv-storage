#!/bin/bash

[ -z "$1" ] && echo -e "Usage:\n$0 <Elasticsearch URL>"  && exit 1

DIR=$(dirname "$0")
REGISTRY_CODE=$(curl -s -o /dev/null -w "%{http_code}" -I $1/storage-registry/)
REGISTRY_LOCK_CODE=$(curl -s -o /dev/null -w "%{http_code}" -I $1/storage-registry-lock/)
STORAGE_DATA_TEMPLATE_CODE=$(curl -s -o /dev/null -w "%{http_code}" -I $1/_template/storage-data/)
STORAGE_HISTORY_TEMPLATE_CODE=$(curl -s -o /dev/null -w "%{http_code}" -I $1/_template/storage-history/)

if [ ! $REGISTRY_CODE -eq 200 ]
then
    echo "storage-registry index to be created"
    curl -s -o /dev/null -S -X PUT "$1/storage-registry" -H 'Content-Type: application/json' --data-binary \
      "@"$DIR"/storage-registry.json" && echo "storage-registry index created"
else
    echo "storage-registry index exists"
fi

if [ ! $REGISTRY_LOCK_CODE -eq 200 ]
then
    echo "storage-registry-lock index to be created"
    curl -s -o /dev/null -S -X PUT "$1/storage-registry-lock" -H 'Content-Type: application/json' --data-binary \
      "@"$DIR"/storage-registry-lock.json" && echo "storage-registry-lock index created"
else
    echo "storage-registry-lock index exists"
fi

if [ ! $STORAGE_DATA_TEMPLATE_CODE -eq 200 ]
then
    echo "storage-data template to be created"
    curl -s -o /dev/null -S -X PUT "$1/_template/storage-data" -H 'Content-Type: application/json' --data-binary \
      "@"$DIR"/storage-data-template.json" && echo "storage-data template created"
else
    echo "storage-data template exists"
fi

if [ ! $STORAGE_HISTORY_TEMPLATE_CODE -eq 200 ]
then
    echo "storage-history template to be created"
    curl -s -o /dev/null -S -X PUT "$1/_template/storage-history" -H 'Content-Type: application/json' --data-binary \
      "@"$DIR"/storage-history-template.json" && echo "storage-history template created"
else
    echo "storage-history template exists"
fi
