package com.bwsw.kv.storage.models.gateway

trait Gateway {
  def get(storage: String, key: String): Option[String]
  def get(storage: String, keys: Iterable[String]) : Iterable[(String, Option[String])]
  def set(storage: String, key: String, value: String): Option[String]
  def set(storage: String, kvs: Iterable[(String, String)]) : Iterable[(String, Option[String])]
  def delete(storage: String, key: String): Boolean
  def delete(storage: String, keys: Iterable[String]) : Iterable[(String, Boolean)]
  def list(storage: String): Iterable[(String, String)]
  def clear(storage: String): Boolean
}
