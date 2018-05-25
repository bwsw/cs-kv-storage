package com.bwsw.kv.storage.processor

import com.bwsw.kv.storage.error.StorageError

import scala.concurrent.Future

/** A processor for key/value storage operations. */
trait KvProcessor {

  /** Gets the value by the given key.
    *
    * @param storage the storage UUID to retrieve the value from
    * @param key     the key
    * @return a [[Future]] with the value or an error
    */
  def get(storage: String, key: String): Future[Either[StorageError, String]]

  /** Gets values by given keys.
    *
    * @param storage the storage UUID to retrieve values from
    * @param keys    keys
    * @return a [[Future]] with the value or an error
    */
  def get(storage: String, keys: Iterable[String]): Future[Either[StorageError, Map[String, Option[String]]]]

  /** Sets the value for the given key.
    *
    * @param storage the storage UUID to put the key/value to
    * @param key     the key
    * @param value   the value
    * @return an empty [[Future]] or a [[Future]] with an error
    */
  def set(storage: String, key: String, value: String): Future[Either[StorageError, Unit]]

  /** Sets values for given keys.
    *
    * @param storage the storage UUID to put the key/value pairs to
    * @param kvs     the key/value pairs
    * @return a [[Future]] with a boolean operation status for each key or error
    */
  def set(storage: String, kvs: Map[String, String]): Future[Either[StorageError, Map[String, Boolean]]]

  /** Deletes the value of the given key.
    *
    * @param storage the storage UUID to delete the value from
    * @param key     the key
    * @return an empty [[Future]] or a [[Future]] with an error
    */
  def delete(storage: String, key: String): Future[Either[StorageError, Unit]]

  /** Deletes values of given keys.
    *
    * @param storage the storage UUID to delete values from
    * @param keys    keys
    * @return a [[Future]] with a boolean operation status for each key or error
    */
  def delete(storage: String, keys: Iterable[String]): Future[Either[StorageError, Map[String, Boolean]]]

  /** Retrieves existing keys.
    *
    * @param storage the storage UUID to retrieve keys from
    * @return a [[Future]] with a [[List]] of keys or an error
    */
  def list(storage: String): Future[Either[StorageError, List[String]]]

  /** Removes all key/value pairs.
    *
    * @param storage the storage UUID to remove key/values from
    * @return an empty [[Future]] or a [[Future]] with an error
    */
  def clear(storage: String): Future[Either[StorageError, Unit]]
}
