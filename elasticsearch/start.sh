#!/bin/bash

[ -z "$1" ] && echo -e "Usage:\n$0 <Elasticsearch URL>"  && exit 1

check_status() {
   echo $(curl -s -o /dev/null -w "%{http_code}" -I "$1")
}

create() {
   curl -s -o /dev/null -S -X PUT "$1" -H 'Content-Type: application/json' --data-binary "$2"
}

REGISTRY_PATH="$1/storage-registry"
REGISTRY_LOCK_PATH="$1/storage-registry-lock"
DATA_TEMPLATE_PATH="$1/_template/storage-data"
HISTORY_TEMPLATE_PATH="$1/_template/storage-history"

DIR=$(dirname "$0")

REGISTRY_CODE=`check_status "${REGISTRY_PATH}"`
REGISTRY_LOCK_CODE=`check_status "${REGISTRY_LOCK_PATH}"`
DATA_TEMPLATE_CODE=`check_status "${DATA_TEMPLATE_PATH}"`
HISTORY_TEMPLATE_CODE=`check_status "${HISTORY_TEMPLATE_PATH}"`

if [ ! $REGISTRY_CODE -eq 200 ]
then
    echo "storage-registry index to be created"
    create "${REGISTRY_PATH}" "@$DIR/storage-registry.json" && echo "storage-registry index created"
else
    echo "storage-registry index exists"
fi

if [ ! $REGISTRY_LOCK_CODE -eq 200 ]
then
    echo "storage-registry-lock index to be created"
    create "${REGISTRY_LOCK_PATH}" "@$DIR/storage-registry-lock.json" && echo "storage-registry-lock index created"
else
    echo "storage-registry-lock index exists"
fi

if [ ! $DATA_TEMPLATE_CODE -eq 200 ]
then
    echo "storage-data template to be created"
    create "${DATA_TEMPLATE_PATH}" "@$DIR/storage-data-template.json" && echo "storage-data template created"
else
    echo "storage-data template exists"
fi

if [ ! $HISTORY_TEMPLATE_CODE -eq 200 ]
then
    echo "storage-history template to be created"
    create "${HISTORY_TEMPLATE_PATH}" "@$DIR/storage-history-template.json" && echo "storage-history template created"
else
    echo "storage-history template exists"
fi
