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

package com.bwsw.cloudstack.storage.kv.manager

import com.bwsw.cloudstack.storage.kv.error.StorageError

import scala.concurrent.Future

/** A manager of storages */
trait KvStorageManager {

  /** Updates TTL of the given temporary storage.
    *
    * @param storage the storage UUID
    * @param ttl     TTL
    * @return an empty [[scala.concurrent.Future]] or a [[scala.concurrent.Future]] with an error
    */
  def updateTempStorageTtl(storage: String, secretKey: Array[Char], ttl: Long): Future[Either[StorageError, Unit]]

  /** Deletes the given temporary storage.
    *
    * @param storage the storage UUID
    * @return an empty [[scala.concurrent.Future]] or a [[scala.concurrent.Future]] with an error
    */
  def deleteTempStorage(storage: String, secretKey: Array[Char]): Future[Either[StorageError, Unit]]
}
