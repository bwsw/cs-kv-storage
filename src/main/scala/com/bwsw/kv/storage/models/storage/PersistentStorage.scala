package com.bwsw.kv.storage.models.storage

import java.util.UUID

/** Persistent storage for a specific account
  * @param uUID Storage unique identifier
  * @param name Storage name
  * @param description Storage description
  */
case class PersistentStorage(uUID: UUID, name: String, description: String) extends Storage{

}
