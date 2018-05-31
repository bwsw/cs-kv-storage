package com.bwsw.cloudstack.storage.kv.manager

import com.bwsw.cloudstack.storage.kv.error.StorageError

import scala.concurrent.Future

/** A manager of storages */
trait KvStorageManager {

  /** Updates TTL of the given temporary storage.
    *
    * @param storage the storage UUID
    * @param ttl     TTL
    * @return an empty [[Future]] or a [[Future]] with an error
    */
  def updateTempStorageTtl(storage: String, ttl: Long): Future[Either[StorageError, Unit]]

  /** Deletes the given temporary storage.
    *
    * @param storage the storage UUID
    * @return an empty [[Future]] or a [[Future]] with an error
    */
  def deleteTempStorage(storage: String): Future[Either[StorageError, Unit]]
}
