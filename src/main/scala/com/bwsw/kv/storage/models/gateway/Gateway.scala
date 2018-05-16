package com.bwsw.kv.storage.models.gateway

import scala.concurrent.{ExecutionContext, Future}

trait Gateway {
  def get(storage: String, key: String)(implicit ctx: ExecutionContext): Future[Option[String]]
  def get(storage: String, keys: Iterable[String])(implicit ctx: ExecutionContext): Future[Iterable[(String, Option[String])]]
  def set(storage: String, key: String, value: String)(implicit ctx: ExecutionContext): Future[Option[String]]
  def set(storage: String, kvs: Iterable[(String, String)])(implicit ctx: ExecutionContext): Future[Iterable[(String, Option[String])]]
  def delete(storage: String, key: String)(implicit ctx: ExecutionContext): Future[Boolean]
  def delete(storage: String, keys: Iterable[String])(implicit ctx: ExecutionContext): Future[Iterable[(String, Boolean)]]
  def list(storage: String)(implicit ctx: ExecutionContext): Future[Iterable[(String, String)]]
  def clear(storage: String)(implicit ctx: ExecutionContext): Future[Boolean]
}
