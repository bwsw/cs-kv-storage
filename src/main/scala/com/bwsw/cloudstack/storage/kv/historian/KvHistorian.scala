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
    * @return a [[Future]] with a boolean operation status for each key or error
    */
  def save(histories: Vector[KvHistory]): Future[Either[StorageError, Map[KvHistory, Boolean]]]
}
