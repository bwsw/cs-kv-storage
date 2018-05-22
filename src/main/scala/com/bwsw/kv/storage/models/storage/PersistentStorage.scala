package com.bwsw.kv.storage.models.storage

/** Persistent storage for a specific account
  * @param uUID Storage unique identifier
  * @param name Storage name
  * @param description Storage description
  */
case class PersistentStorage(uUID: String, name: String, description: String) extends Storage {

}
