// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.bwsw.cloudstack.storage.kv.util

import com.bwsw.cloudstack.storage.kv.error.InternalError
import com.sksamuel.elastic4s.http.RequestFailure

package object elasticsearch {

  val RegistryIndex = "storage-registry"
  val DocumentType = "_doc"

  val HistoryStorageTemplateName = "storage-history"
  val DataStorageTemplateName = "storage-data"

  val DefaultError = "Elasticsearch error"

  val ScrollTimeoutUnit = "ms"

  val SecretKeyHeader = "Secret-Key"

  object StorageType {
    val Temporary = "TEMP"
    val VirtualMachine = "VM"
    val Account = "ACCOUNT"
  }

  object StorageFields {
    val Value = "value"
  }

  object HistoryFields {
    val Key = "key"
    val Value = "value"
    val Timestamp = "timestamp"
    val TimestampUnit = "ms"
    val Operation = "operation"
  }

  object RegistryFields {
    val Type = "type"
    val HistoryEnabled = "history_enabled"
    val Deleted = "deleted"
    val Account = "account"
    val Name = "name"
    val Description = "description"
    val Ttl = "ttl"
    val ExpirationTimestamp = "expiration_timestamp"
    val LastUpdated = "last_updated"
    val SecretKey = "secret_key"
  }

  object ScriptOperations {
    val NoOp = "noop"
    val Updated = "updated"
  }

  def getStorageIndex(storageUuid: String): String = s"storage-data-$storageUuid"

  def getHistoricalStorageIndex(storageUuid: String): String = s"storage-history-$storageUuid"

  def getError(requestFailure: RequestFailure): InternalError = {
    if (requestFailure.error == null)
      InternalError(DefaultError)
    else InternalError(requestFailure.error.reason)
  }

}
