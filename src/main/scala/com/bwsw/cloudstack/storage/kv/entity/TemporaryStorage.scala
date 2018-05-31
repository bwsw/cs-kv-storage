package com.bwsw.cloudstack.storage.kv.entity

/** Temporary storage with a limited lifetime
  *
  * @param uUID Storage unique identifier
  * @param expirationTime timestamp of storage becoming inaccessible
  */
case class TemporaryStorage(uUID: String, expirationTime: Long) extends Storage {
}