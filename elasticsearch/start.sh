#!/bin/bash

usage_and_exit() {
    echo "USAGE"
    echo -e "\t$0 [options] <Elasticsearch URL>"
    echo "OPTIONS"
    echo -e "\t-u <user:password>\n\t\tcredentials for Elasticsearch"
    echo -e "\t-i\n\t\tinteractive mode to prompt for Elasticsearch credentials"
}

TOKEN=""
while getopts u:i opt
do
    case $opt in
        i)
        echo -n "Enter <user:password>: "
        read TOKEN
        if [[ $TOKEN != *:* ]]
        then
            echo -n "Enter password for the user '${TOKEN}': "
            read PASSWORD
            TOKEN="$TOKEN:$PASSWORD"
        fi
        ;;
        u)
        [[ -z "$OPTARG" || $OPTARG != *:* ]] && usage_and_exit && exit 1
        TOKEN=$OPTARG
        ;;
    esac
done
shift $((OPTIND -1))

[ -z "$1" ] && usage_and_exit && exit 1

check_status() {
    if [ -z "$2" ]
    then
        CODE=$(curl -s -o /dev/null -w "%{http_code}" -I "$1")
    else
        CODE=$(curl -s -o /dev/null -w "%{http_code}" -u "$2" -I "$1")
    fi
    echo "$CODE"
}

create() {
    if [ -z "$3" ]
    then
        curl -s -o /dev/null -S --fail -X PUT "$1" -H 'Content-Type: application/json' --data-binary "$2"
    else
        curl -s -o /dev/null -S --fail -u "$3" -X PUT "$1" -H 'Content-Type: application/json' --data-binary "$2"
    fi
}

REGISTRY_PATH="$1/storage-registry"
REGISTRY_LOCK_PATH="$1/storage-registry-lock"
DATA_TEMPLATE_PATH="$1/_template/storage-data"
HISTORY_TEMPLATE_PATH="$1/_template/storage-history"

DIR=$(dirname "$0")

REGISTRY_CODE=`check_status "${REGISTRY_PATH}" "${TOKEN}"`
REGISTRY_LOCK_CODE=`check_status "${REGISTRY_LOCK_PATH}" "${TOKEN}"`
DATA_TEMPLATE_CODE=`check_status "${DATA_TEMPLATE_PATH}" "${TOKEN}"`
HISTORY_TEMPLATE_CODE=`check_status "${HISTORY_TEMPLATE_PATH}" "${TOKEN}"`

if [ ! $REGISTRY_CODE -eq 200 ]
then
    echo "storage-registry index to be created"
    create "${REGISTRY_PATH}" "@$DIR/storage-registry.json" "${TOKEN}" && echo "storage-registry index created"
else
    echo "storage-registry index exists"
fi

if [ ! $REGISTRY_LOCK_CODE -eq 200 ]
then
    echo "storage-registry-lock index to be created"
    create "${REGISTRY_LOCK_PATH}" "@$DIR/storage-registry-lock.json" "${TOKEN}" && \
        echo "storage-registry-lock index created"
else
    echo "storage-registry-lock index exists"
fi

if [ ! $DATA_TEMPLATE_CODE -eq 200 ]
then
    echo "storage-data template to be created"
    create "${DATA_TEMPLATE_PATH}" "@$DIR/storage-data-template.json" "${TOKEN}" && echo "storage-data template created"
else
    echo "storage-data template exists"
fi

if [ ! $HISTORY_TEMPLATE_CODE -eq 200 ]
then
    echo "storage-history template to be created"
    create "${HISTORY_TEMPLATE_PATH}" "@$DIR/storage-history-template.json" "${TOKEN}" && \
        echo "storage-history template created"
else
    echo "storage-history template exists"
fi
