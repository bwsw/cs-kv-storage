package com.bwsw.kv.storage.models.kvprocessor

import scala.concurrent.Future

trait KvProcessor {
  def get(storage: String, key: String): Future[String]
  def get(storage: String, keys: Iterable[String]): Future[Iterable[(String, Option[String])]]
  def set(storage: String, key: String, value: String): Future[Boolean]
  def set(storage: String, kvs: Iterable[(String, String)]): Future[Iterable[(String, Boolean)]]
  def delete(storage: String, key: String): Future[Boolean]
  def delete(storage: String, keys: Iterable[String]): Future[Iterable[(String, Boolean)]]
  def list(storage: String): Future[Iterable[(String, String)]]
  def clear(storage: String): Future[Boolean]
}
