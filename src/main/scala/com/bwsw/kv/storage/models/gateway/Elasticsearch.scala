package com.bwsw.kv.storage.models.gateway

class Elasticsearch extends Gateway {
  /** Gets value by given key
    * @param storage target storage UUID
    * @param key key of a value
    * @return Some(String) if value exists and None otherwise
    */
  def get(storage: String, key: String): Option[String] = {
    None//TODO: Implementation
  }

  /** Gets value by given keys
    * @param storage target storage UUID
    * @param keys Collection of keys
    * @return Collection of tuples key and Some(String) if value exists and None otherwise
    */
  def get(storage: String, keys: Iterable[String]): Iterable[(String, Option[String])] = {
    Set(("", None))//TODO: Implementation
  }

  /** Sets value for given key
    * @param storage target storage UUID
    * @param key key of a value
    * @param value value to set
    * @return Some(String) if value successfully set and None otherwise
    */
  def set(storage: String, key: String, value: String): Option[String] = {
    None//TODO: Implementation
  }

  /** Sets values by given keys
    * @param storage target storage UUID
    * @param kvs Collection of tuples key and value to set
    * @return Collection of tuples key to Some(String) if value successfully set and None otherwise
    */
  def set(storage: String, kvs: Iterable[(String, String)]): Iterable[(String, Option[String])] = {
    Set(("", None))//TODO: Implementation
  }

  /** Deletes value of given key
    * @param storage target storage UUID
    * @param key key of value to delete
    * @return true if deletion succeed and false otherwise
    */
  def delete(storage: String, key: String): Boolean = {
    true//TODO: Implementation
  }

  /** Deletes values of given keys
    * @param storage target storage UUID
    * @param keys Collection of keys
    * @return Collection of tuples key to true if deletion succeed and false otherwise
    */
  def delete(storage: String, keys: Iterable[String]): Iterable[(String, Boolean)] = {
    Set(("", true))//TODO: Implementation
  }

  /** Returns a list of existing keys and values
    * @param storage target storage UUID
    * @return Collection of tuples key and value
    */
  def list(storage: String): Iterable[(String, String)] = {
    Set(("", ""))//TODO: Implementation
  }

  /** Clears storage
    * @param storage target storage UUID
    * @return true if clearing succeed and false otherwise
    */
  def clear(storage: String): Boolean = {
    true//TODO: Implementation
  }
}
