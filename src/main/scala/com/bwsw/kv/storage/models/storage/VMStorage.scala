package com.bwsw.kv.storage.models.storage

import java.util.UUID

/** Temporary storage for a specific Virtual Machine
  * @param uUID Storage unique identifier
  * @param ownerUUID VM unique identifier
  */
case class VMStorage(uUID: UUID, ownerUUID: UUID) extends Storage {


}
