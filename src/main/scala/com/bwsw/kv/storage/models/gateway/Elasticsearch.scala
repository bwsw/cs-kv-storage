package com.bwsw.kv.storage.models.gateway

class Elasticsearch extends Gateway {
  /** Gets value by given key
    * @return Some(String) if value exists and None otherwise
    */
  def get(key: String): Option[String] = {
    None//TODO: Implementation
  }

  /** Gets value by given keys
    * @param keys Collection of keys
    * @return Collection of tuples key and Some(String) if value exists and None otherwise
    */
  def get(keys: Iterable[String]): Iterable[(String, Option[String])] = {
    Set(("", None))//TODO: Implementation
  }

  /** Sets value for given key
    * @return Some(String) if value successfully set and None otherwise
    */
  def set(key: String, value: String): Option[String] = {
    None//TODO: Implementation
  }

  /** Sets values by given keys
    * @param kvs Collection of tuples key and value to set
    * @return Collection of tuples key to Some(String) if value successfully set and None otherwise
    */
  def set(kvs: Iterable[(String, String)]): Iterable[(String, Option[String])] = {
    Set(("", None))//TODO: Implementation
  }

  /** Deletes value of given key
    * @param key key of value to delete
    * @return true if deletion succeed and false otherwise
    */
  def delete(key: String): Boolean = {
    true//TODO: Implementation
  }

  /** Deletes values of given keys
    * @param keys Collection of keys
    * @return Collection of tuples key to true if deletion succeed and false otherwise
    */
  def delete(keys: Iterable[String]): Iterable[(String, Boolean)] = {
    Set(("", true))//TODO: Implementation
  }

  /** Returns a list of existing keys and values
    * @return Collection of tuples key and value
    */
  def list(): Iterable[(String, String)] = {
    Set(("", ""))//TODO: Implementation
  }

  /** Clears storage
    * @return true if clearing succeed and false otherwise
    */
  def clear(): Boolean = {
    true//TODO: Implementation
  }
}
