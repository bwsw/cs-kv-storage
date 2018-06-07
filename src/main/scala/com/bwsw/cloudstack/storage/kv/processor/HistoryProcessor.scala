package com.bwsw.cloudstack.storage.kv.processor

import com.bwsw.cloudstack.storage.kv.error.StorageError
import com.bwsw.cloudstack.storage.kv.message.KvHistory

import scala.concurrent.Future

trait HistoryProcessor {
  /** Saves collection a historical records into dedicated storage
    *
    * @param histories a Collection of histories
    * @return a [[Future]] with a boolean operation status for each key or error
    */
  def save(histories: Vector[KvHistory]): Future[Either[StorageError, Option[List[KvHistory]]]]
}
