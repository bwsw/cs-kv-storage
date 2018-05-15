package com.bwsw.kv.storage.models.storage

import java.util.UUID

/** Temporary storage with a limited lifetime
  * @param uUID Storage unique identifier
  * @param expirationTime timestamp of storage becoming inaccessible
  */
case class TemporaryStorage(uUID: UUID, expirationTime: Long) extends Storage {

}
