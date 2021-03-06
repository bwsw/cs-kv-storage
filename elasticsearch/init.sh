# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#!/bin/bash

print_help() {
    echo "USAGE"
    echo -e "\t$0 [options] <Elasticsearch URL>"
    echo "OPTIONS"
    echo -e "\t-u <user:password>\n\t\tcredentials for Elasticsearch"
    echo -e "\t-i\n\t\tinteractive mode to prompt for Elasticsearch credentials"
    echo -e "\t-h\n\t\tprint help and exit"
}

TOKEN=""
while getopts u:ih opt
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
        [[ -z "$OPTARG" || $OPTARG != *:* ]] && print_help && exit 1
        TOKEN=$OPTARG
        ;;
        h)
        print_help && exit 0
        ;;
    esac
done
shift $((OPTIND -1))

[ -z "$1" ] && print_help && exit 1

check_status() {
    if [ -z "$2" ]
    then
        CODE=$(curl -s -o /dev/null -w "%{http_code}" "$1")
    else
        CODE=$(curl -s -o /dev/null -w "%{http_code}" -u "$2" "$1")
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
LAST_UPDATED_PIPELINE_PATH="$1/_ingest/pipeline/storage-registry-last-updated"

DIR=$(dirname "$0")

REGISTRY_CODE=`check_status "${REGISTRY_PATH}" "${TOKEN}"`
REGISTRY_LOCK_CODE=`check_status "${REGISTRY_LOCK_PATH}" "${TOKEN}"`
DATA_TEMPLATE_CODE=`check_status "${DATA_TEMPLATE_PATH}" "${TOKEN}"`
HISTORY_TEMPLATE_CODE=`check_status "${HISTORY_TEMPLATE_PATH}" "${TOKEN}"`
LAST_UPDATED_PIPELINE_CODE=`check_status "${LAST_UPDATED_PIPELINE_PATH}" "${TOKEN}"`

if [ ! $REGISTRY_CODE -eq 200 ]
then
    echo "storage-registry index to be created"
    create "${REGISTRY_PATH}" "@$DIR/storage-registry.json" "${TOKEN}" && echo "storage-registry index created" || \
        exit 1
else
    echo "storage-registry index exists"
fi

if [ ! $REGISTRY_LOCK_CODE -eq 200 ]
then
    echo "storage-registry-lock index to be created"
    create "${REGISTRY_LOCK_PATH}" "@$DIR/storage-registry-lock.json" "${TOKEN}" && \
        echo "storage-registry-lock index created" || exit 1
else
    echo "storage-registry-lock index exists"
fi

if [ ! $DATA_TEMPLATE_CODE -eq 200 ]
then
    echo "storage-data template to be created"
    create "${DATA_TEMPLATE_PATH}" "@$DIR/storage-data-template.json" "${TOKEN}" && \
        echo "storage-data template created" || exit 1
else
    echo "storage-data template exists"
fi

if [ ! $HISTORY_TEMPLATE_CODE -eq 200 ]
then
    echo "storage-history template to be created"
    create "${HISTORY_TEMPLATE_PATH}" "@$DIR/storage-history-template.json" "${TOKEN}" && \
        echo "storage-history template created" || exit 1
else
    echo "storage-history template exists"
fi

if [ ! $LAST_UPDATED_PIPELINE_CODE -eq 200 ]
then
    echo "storage-registry-last-updated pipeline to be created"
    create "${LAST_UPDATED_PIPELINE_PATH}" "@$DIR/storage-registry-last-updated-pipeline.json" "${TOKEN}" && \
        echo "storage-registry-last-updated pipeline created" || exit 1
else
    echo "storage-registry-last-updated pipeline exists"
fi
