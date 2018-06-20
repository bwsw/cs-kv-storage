package com.bwsw.cloudstack.storage.kv.processor

import com.bwsw.cloudstack.storage.kv.error.StorageError
import com.bwsw.cloudstack.storage.kv.message._
import com.bwsw.cloudstack.storage.kv.message.response.GetHistoryResponse

import scala.concurrent.Future

/** A processor of storage histories. **/
trait HistoryProcessor {
  /** Saves collection a historical records into dedicated storage
    *
    * @param histories a Collection of histories
    * @return a [[Future]] with a boolean operation status for each key or error
    */
  def save(histories: List[KvHistory]): Future[Option[List[KvHistory]]]

  /** Searches for history records suitable to parameters gotten
    *
    * @param storageUuid UUID of storage to search from
    * @return a [[Future]] of records or StorageError if any error occurred
    */
  def get(storageUuid: String,
          keys: Iterable[String],
          operations: Iterable[Operation],
          start: Long,
          end: Long,
          sort: Iterable[String],
          page: Int,
          size: Int,
          scroll: String): Future[Either[StorageError, GetHistoryResponse]]
}
