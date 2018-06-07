package com.bwsw.cloudstack.storage.kv.processor

import com.bwsw.cloudstack.storage.kv.message.KvHistory

import scala.concurrent.Future

/** A processor of storage histories. **/
trait HistoryProcessor {
  /** Saves collection a historical records into dedicated storage
    *
    * @param histories a Collection of histories
    * @return a [[Future]] with a boolean operation status for each key or error
    */
  def save(histories: List[KvHistory]): Future[Option[List[KvHistory]]]
}
