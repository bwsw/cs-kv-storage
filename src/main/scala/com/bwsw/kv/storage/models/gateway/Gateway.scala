package com.bwsw.kv.storage.models.gateway

trait Gateway {
  def get(key: String): Option[String]
  def get(keys: Iterable[String]) : Iterable[(String, Option[String])]
  def set(key: String, value: String): Option[String]
  def set(kvs: Iterable[(String, String)]) : Iterable[(String, Option[String])]
  def delete(key: String): Boolean
  def delete(keys: Iterable[String]) : Iterable[(String, Boolean)]
  def list(): Iterable[(String, String)]
  def clear(): Boolean
}
