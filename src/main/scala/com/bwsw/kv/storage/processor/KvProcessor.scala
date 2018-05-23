package com.bwsw.kv.storage.processor

import com.bwsw.kv.storage.error.StorageError

import scala.concurrent.Future

trait KvProcessor {
  def get(storage: String, key: String): Future[Either[StorageError, String]]
  def get(storage: String, keys: Iterable[String]): Future[Either[StorageError, Map[String, Option[String]]]]
  def set(storage: String, key: String, value: String): Future[Either[StorageError, Unit]]
  def set(storage: String, kvs: Iterable[(String, String)]): Future[Either[StorageError, Map[String, Boolean]]]
  def delete(storage: String, key: String): Future[Either[StorageError, Unit]]
  def delete(storage: String, keys: Iterable[String]): Future[Either[StorageError, Map[String, Boolean]]]
  def list(storage: String): Future[Either[StorageError, List[String]]]
  def clear(storage: String): Future[Either[StorageError, Unit]]
}
