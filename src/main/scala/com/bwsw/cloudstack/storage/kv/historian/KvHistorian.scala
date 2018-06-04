package com.bwsw.cloudstack.storage.kv.historian

import com.bwsw.cloudstack.storage.kv.error.StorageError
import com.bwsw.cloudstack.storage.kv.message.KvHistory

import scala.concurrent.Future

trait KvHistorian {
  /** Saves historical record into dedicated storage
    *
    * @param history a history
    * @return an empty [[Future]] or a [[Future]] with an error
    */
  def save(history: KvHistory): Future[Either[StorageError, Unit]]

  /** Saves collection af historical records into dedicated storage
    *
    * @param histories a Collection of histories
    * @return a [[Future]] with a count of successful operation for each dedicated storage or error
    */
  def save(histories: Iterable[KvHistory]): Future[Either[StorageError, Map[String, Int]]]
}
