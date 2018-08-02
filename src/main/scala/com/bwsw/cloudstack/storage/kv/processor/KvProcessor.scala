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

package com.bwsw.cloudstack.storage.kv.processor

import com.bwsw.cloudstack.storage.kv.error.StorageError

import scala.concurrent.Future

/** A processor for key/value storage operations. */
trait KvProcessor {

  /** Gets the value by the given key.
    *
    * @param storage the storage UUID to retrieve the value from
    * @param key     the key
    * @return a [[scala.concurrent.Future]] with the value or an error
    */
  def get(storage: String, key: String): Future[Either[StorageError, String]]

  /** Gets values by given keys.
    *
    * @param storage the storage UUID to retrieve values from
    * @param keys    keys
    * @return a [[scala.concurrent.Future]] with the value or an error
    */
  def get(storage: String, keys: Iterable[String]): Future[Either[StorageError, Map[String, Option[String]]]]

  /** Sets the value for the given key.
    *
    * @param storage the storage UUID to put the key/value to
    * @param key     the key
    * @param value   the value
    * @return an empty [[scala.concurrent.Future]] or a [[scala.concurrent.Future]] with an error
    */
  def set(storage: String, key: String, value: String): Future[Either[StorageError, Unit]]

  /** Sets values for given keys.
    *
    * @param storage the storage UUID to put the key/value pairs to
    * @param kvs     the key/value pairs
    * @return a [[scala.concurrent.Future]] with a boolean operation status for each key or error
    */
  def set(storage: String, kvs: Map[String, String]): Future[Either[StorageError, Map[String, Boolean]]]

  /** Deletes the value of the given key.
    *
    * @param storage the storage UUID to delete the value from
    * @param key     the key
    * @return an empty [[scala.concurrent.Future]] or a [[scala.concurrent.Future]] with an error
    */
  def delete(storage: String, key: String): Future[Either[StorageError, Unit]]

  /** Deletes values of given keys.
    *
    * @param storage the storage UUID to delete values from
    * @param keys    keys
    * @return a [[scala.concurrent.Future]] with a boolean operation status for each key or error
    */
  def delete(storage: String, keys: Iterable[String]): Future[Either[StorageError, Map[String, Boolean]]]

  /** Retrieves existing keys.
    *
    * @param storage the storage UUID to retrieve keys from
    * @return a [[scala.concurrent.Future]] with a [[scala.List]] of keys or an error
    */
  def list(storage: String): Future[Either[StorageError, List[String]]]

  /** Removes all key/value pairs.
    *
    * @param storage the storage UUID to remove key/values from
    * @return an empty [[scala.concurrent.Future]] or a [[scala.concurrent.Future]] with an error
    */
  def clear(storage: String): Future[Either[StorageError, Unit]]
}
