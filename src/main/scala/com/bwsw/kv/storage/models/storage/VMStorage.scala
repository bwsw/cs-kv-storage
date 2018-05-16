package com.bwsw.kv.storage.models.storage

/** Temporary storage for a specific Virtual Machine
  * @param uUID Storage unique identifier
  * @param ownerUUID VM unique identifier
  */
case class VMStorage(uUID: String, ownerUUID: String) extends Storage {
}
