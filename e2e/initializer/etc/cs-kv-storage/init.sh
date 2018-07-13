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

put_binary() {
    curl -s -o /dev/null -S --fail -X PUT "$1" -H 'Content-Type: application/json' --data-binary "$2" || exit 1
}

put() {
    if [ -z "$2" ]
    then
        curl -s -o /dev/null -S --fail -X PUT "$1" || exit 1
    else
        curl -s -o /dev/null -S --fail -X PUT "$1" -H 'Content-Type: application/json' --data-raw "$2" || exit 1
    fi
}

post_binary() {
    curl -s -o /dev/null -S --fail -X POST "$1" -H 'Content-Type: application/json' --data-binary "$2" || exit 1
}

echo "Test data preparation started"

put_binary "$1/storage-registry" "@/etc/cs-kv-storage/configuration/storage-registry.json"

put_binary "$1/_template/storage-data" "@/etc/cs-kv-storage/configuration/storage-data-template.json"

put_binary "$1/_template/storage-history" "@/etc/cs-kv-storage/configuration/storage-history-template.json"

put "$1/storage-data-read-only"

put "$1/storage-registry/_doc/read-only" '{"type": "ACCOUNT", "deleted": false, "history_enabled": true}'

post_binary "$1/storage-data-read-only/_doc/_bulk" "@/etc/cs-kv-storage/data/storage-test-data.ndjson"

post_binary "$1/storage-history-read-only/_doc/_bulk" "@/etc/cs-kv-storage/data/storage-test-history.ndjson"

put "$1/storage-data-empty"

put "$1/storage-registry/_doc/empty" '{"type": "ACCOUNT", "deleted": false, "history_enabled": true}'

put "$1/storage-data-editable-single"

put "$1/storage-registry/_doc/editable-single" '{"type": "ACCOUNT", "deleted": false, "history_enabled": true}'

post_binary "$1/storage-data-editable-single/_doc/_bulk" "@/etc/cs-kv-storage/data/storage-test-data.ndjson"

put "$1/storage-data-editable-multiple"

put "$1/storage-registry/_doc/editable-multiple" '{"type": "ACCOUNT", "deleted": false, "history_enabled": true}'

post_binary "$1/storage-data-editable-multiple/_doc/_bulk" "@/etc/cs-kv-storage/data/storage-test-data.ndjson"

put "$1/storage-data-deletable-single"

put "$1/storage-registry/_doc/deletable-single" '{"type": "ACCOUNT", "deleted": false, "history_enabled": true}'

put_binary "$1/storage-data-deletable-single/_doc/_bulk" "@/etc/cs-kv-storage/data/storage-test-data.ndjson"

put "$1/storage-data-deletable-multiple"

put "$1/storage-registry/_doc/deletable-multiple" '{"type": "ACCOUNT", "deleted": false, "history_enabled": true}'

put_binary "$1/storage-data-deletable-multiple/_doc/_bulk" "@/etc/cs-kv-storage/data/storage-test-data.ndjson"

put "$1/storage-data-cleanable"

put "$1/storage-registry/_doc/cleanable" '{"type": "ACCOUNT", "deleted": false, "history_enabled": true}'

post_binary "$1/storage-data-cleanable/_doc/_bulk" "@/etc/cs-kv-storage/data/storage-test-data.ndjson"

put "$1/storage-data-temp"

put "$1/storage-registry/_doc/temp" \
    '{"type": "TEMP", "deleted": false, "history_enabled": false, "ttl": 10000, "expiration_timestamp": 1010000}'

put "$1/storage-data-account"

put "$1/storage-registry/_doc/account" '{"type": "ACCOUNT", "deleted": false, "history_enabled": true}'

put "$1/storage-data-no-index"

put "$1/storage-registry/_doc/no-history" \
    '{"type": "TEMP", "deleted": false, "history_enabled": false, "ttl": 10000, "expiration_timestamp": 1530602449565}'

echo "Test data preparation finished"
