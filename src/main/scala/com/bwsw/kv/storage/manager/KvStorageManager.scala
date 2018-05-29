package com.bwsw.kv.storage.manager

import com.bwsw.kv.storage.error.StorageError

import scala.concurrent.Future

/** A manager of storages */
trait KvStorageManager {
  /** Updates TTL of given temporary storage
    *
    * @param storage the storage UUID
    * @param ttl     TTL
    * @return an empty [[Future]] or a [[Future]] with an error
    */
  def updateTempStorageTtl(storage: String, ttl: Long): Future[Either[StorageError, Unit]]

  /** Deletes given temporary storage
    *
    * @param storage the storage UUID
    * @return an empty [[Future]] or a [[Future]] with an error
    */
  def deleteTempStorage(storage: String): Future[Either[StorageError, Unit]]
}
