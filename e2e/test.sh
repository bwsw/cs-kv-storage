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

cd "$(dirname "$0")"

[ -z "$1" ] && echo "Application version is not specified" && exit 1

export APP_DOCKER_TAG=$1
docker-compose up -d --force-recreate --build app
docker-compose up --build initializer
docker-compose run newman run cs-kv-storage.postman_collection.json --reporters=cli
docker-compose down --volumes
